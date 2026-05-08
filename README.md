# newCache

newCache 是一个用 Go 实现的分布式缓存项目。它不是单纯的本地 LRU，而是围绕多节点缓存、gRPC 节点通信、etcd 服务发现、一致性哈希路由、版本控制、墓碑删除和数据迁移逐步构建的缓存系统。

当前项目重点验证这些能力：

- 本地缓存淘汰和 TTL。
- 多节点读写路由。
- 节点上线、预热、下线状态管理。
- 数据迁移时保留 version、TTL 和 tombstone。
- 旧版本写入不覆盖新版本。
- 删除后旧写入不能复活数据。
- 真实 etcd + 多节点 + gRPC 的集成链路。

## 项目架构

```text
client
  -> Picker
  -> consistent hash ring
  -> local Group or remote gRPC node
  -> local Cache
  -> store: simple / LRU / LRU2
```

核心模块：

```text
api/proto/              gRPC proto 和生成代码
cache/                  Group、CacheEntry、version、tombstone、迁移语义
store/                  simple、LRU、LRU2 存储实现
internal/client/         gRPC client 和 etcd picker
internal/server/         gRPC server
internal/registry/       etcd 注册、租约、节点状态
internal/consistenthash/ 一致性哈希
internal/mirgration/     Scan/BatchSet 迁移管理
internal/retry-queue/    影子同步失败后的重试队列
internal/wal/            WAL 记录和 replay
internal/integration/    真实 etcd 多节点集成测试
cmd/cache-node/          节点启动示例
docs/                    设计文档
```

## 当前能力

### 本地缓存

- 支持 `simple`、`LRU`、`LRU2` 三种 store。
- `LRU2` 使用 L1/L2 两层缓存，第一次写入进入 L1，命中后晋升到 L2。
- 支持 TTL、删除、关闭、扫描和并发安全访问。
- `Scan(startKey, count)` 支持迁移时分页导出数据。

### 缓存命名空间

`cache.Group` 管理一组缓存数据：

```go
group := cache.NewGroup(name, cacheBytes, getter, workID)
```

Group 负责：

- 本地读写。
- 回源加载。
- singleflight 合并并发回源。
- 调用 picker 路由远程 owner。
- shadow ring 双写。
- retry queue 补偿失败的影子同步。

### 分布式路由

节点信息通过 etcd 注册：

```go
type NodeInfo struct {
	Addr   string     `json:"addr"`
	Status NodeStatus `json:"status"`
}
```

节点状态：

- `warming`：新节点预热中，只进入 shadow ring，不接正式读写。
- `active`：正常节点，进入 read/write/shadow 三个环。
- `draining`：准备下线，仍可读，不再接写。

Picker 会根据节点状态构建三种一致性哈希环：

```text
read ring    -> 读请求路由
write ring   -> 写请求 owner 路由
shadow ring  -> 迁移/双写路由
```

### gRPC API

定义在 `api/proto/cache.proto`：

```proto
service CacheService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Set(SetRequest) returns (SetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Scan(ScanRequest) returns (ScanResponse);
  rpc BatchSet(BatchRequest) returns (BatchResponse);
}
```

关键字段：

- `version`：防止旧写覆盖新写。
- `ttl_ms`：TTL 毫秒数。
- `from_peer`：标记来自 peer 的请求，避免循环转发。
- `tombstone`：迁移和删除时保留删除语义。
- `verison`：`DeleteRequest` 当前字段名拼写如此，属于现有 proto 兼容问题。

## 核心语义

### 写入版本

写入时必须携带 version：

```text
Set(group, key, value, version, ttl)
```

如果本地已有更高版本，旧写入会被忽略：

```text
current version = 100
incoming version = 50
incoming write ignored
```

这个语义用于处理：

- gRPC 重试。
- 迁移重复发送。
- 影子双写乱序到达。
- 删除和旧写入乱序。

### Tombstone 删除

删除不是简单物理删除，而是写入 tombstone：

```text
Delete(group, key, version)
```

tombstone 会保留删除版本。后续如果旧版本 `Set` 到达，不会把已删除数据复活。

### TTL

TTL 当前主要在内存层生效。迁移时 `Scan` 会带出剩余 TTL，`BatchSet` 在目标节点恢复 TTL。

后续如果接入 MySQL，建议语义是：

```text
ttl > 0: 只写内存和 WAL，不落 MySQL
ttl = 0: 写内存、WAL 和 MySQL
```

## 数据迁移

当前迁移基础链路：

```text
source node
  -> Scan(startKey, count)
  -> TransportEntry / CacheEntry
  -> target node BatchSet(entries)
  -> target Get(key) 命中
```

迁移会保留：

- key
- value
- version
- ttl
- tombstone

因此迁移后：

- 旧版本写入不会覆盖新版本。
- tombstone 不会被旧 Set 复活。
- 重复迁移同一批数据不会破坏正确性。

关于“迁移过程中节点又变化”的最小方案见：

[docs/migration-rebalance-simple.md](/home/david/go/src/newCache/docs/migration-rebalance-simple.md)

推荐方向是：

```text
全局迁移状态机 + 单轮迁移 + pending rebalance + version 幂等写入 + 稳定后延迟 GC
```

## WAL 和 Retry

项目里已经有 WAL 和 retry queue 的基础实现：

- WAL 用于记录 Set/Delete 以及 shadow retry 事件。
- retry queue 用于 shadow sync 失败后的异步补偿。
- replay 可以在重启后恢复未完成的本地状态和 retry 任务。

这部分还处在演进阶段，适合作为后续持久化和恢复能力的基础。

## 测试

普通单元测试：

```bash
GOCACHE=/tmp/newcache-gocache go test ./store ./cache ./internal/server ./internal/client
```

Race 测试：

```bash
GOCACHE=/tmp/newcache-gocache go test -race ./store ./cache ./internal/server ./internal/client
```

真实 etcd 集成测试：

```bash
ETCD_ENDPOINTS=http://127.0.0.1:2379 \
GOCACHE=/tmp/newcache-gocache \
go test -count=1 ./internal/integration -v
```

只跑迁移拓扑变化测试：

```bash
ETCD_ENDPOINTS=http://127.0.0.1:2379 \
GOCACHE=/tmp/newcache-gocache \
go test -count=1 ./internal/integration -run TestMigrationTopologyChangeSecondPassPreservesData -v
```

LRU2 命中率 benchmark：

```bash
GOCACHE=/tmp/newcache-gocache \
go test ./store -run '^$' -bench 'BenchmarkLRU2HitRate' -benchtime=1s -benchmem
```

LRU2 单次操作 benchmark：

```bash
GOCACHE=/tmp/newcache-gocache \
go test ./store -run '^$' -bench 'BenchmarkLRU2Operations' -benchtime=1s -benchmem
```

注意：`internal/integration` 需要真实 etcd 和本机网络权限。如果没有 etcd，测试会 skip。

## 当前测试覆盖

已覆盖：

- LRU2 Set/Get/TTL/Delete/Close。
- L1 到 L2 晋升。
- Delete 并发安全。
- Scan 排序、分页、TTL、跳过过期 key。
- version 旧写不覆盖新写。
- tombstone 不被旧 Set 复活。
- gRPC Set/Delete/Scan/BatchSet 字段传递。
- `from_peer` 防止循环转发。
- warming 节点不接正式读。
- draining 节点不接正式写。
- owner 写路由。
- shadow retry 入队。
- Scan/BatchSet 迁移后 key 可读。
- 真实 etcd 多节点启动、路由、删除、状态机和迁移。
- 迁移过程中拓扑再次变化后的二次迁移数据安全。
- LRU2 在 Zipf、冷 key 污染、扫描污染场景下的命中率 benchmark。

## 当前限制

### cmd/cache-node 入口仍需整理

`cmd/cache-node/main.go` 是示例入口，但当前存在旧 API 调用风险。当前 `Group.Set` 签名是：

```go
Set(ctx context.Context, key string, value []byte, version int64, ttl time.Duration) error
```

因此全量运行：

```bash
go test ./...
```

可能因为示例入口未同步当前 API 而失败。当前建议使用 README 中列出的包级测试命令作为验收。

### 迁移协调器尚未产品化

当前有 `Scan -> BatchSet` 的迁移基础能力，也有迁移拓扑变化测试，但完整的 rebalance coordinator 还没有落成生产代码。

后续需要把这些逻辑产品化：

- etcd 全局迁移状态。
- migration epoch。
- pending rebalance。
- 迁移任务进度。
- 稳定后 GC 非 owner 数据。

### LRU2 命中率仍有升级空间

当前 LRU2 已经比普通 LRU 更抗冷数据污染，但 benchmark 显示在“热点夹杂大量一次性冷 key”的场景下命中率较低。后续可以考虑引入 W-TinyLFU 或 SLRU。

## 可升级点

### 1. 实现 rebalance coordinator

优先级最高。

目标是把文档里的方案落成代码：

```text
stable / migrating
epoch
pending rebalance
progress checkpoint
finish 后切 active
stable 后延迟 GC
```

收益：

- 解决迁移过程中节点再次上线/下线的问题。
- 避免多轮迁移互相打断。
- 保证拓扑变化时数据可追踪、可恢复、可清理。

### 2. 把 LRU2 升级为 SLRU 或 W-TinyLFU

当前已有命中率 benchmark，可以直接对比新算法。

推荐路线：

```text
LRU2 -> SLRU -> W-TinyLFU
```

收益：

- 提升热点命中率。
- 抵抗扫描式访问污染。
- 减少 miss 后的回源、跨节点和数据库压力。

注意事项：

- 必须保留 `Store` 和 `Scanner` 接口语义。
- 不能破坏 TTL、Scan、version、tombstone 迁移行为。

### 3. 完善 WAL 持久化和恢复

当前已有 WAL 基础，后续可以强化为完整事件日志：

```text
Set/Delete -> WAL -> memory
restart -> replay WAL -> recover cache/retry queue
```

收益：

- 节点重启后恢复本地热数据。
- 恢复未完成的 shadow retry。
- 为后续 MySQL 异步落库提供事件源。

### 4. 接入 MySQL 作为旁路持久层

推荐语义：

```text
Get miss -> query MySQL -> backfill memory
Set/Delete -> memory + WAL -> async MySQL
```

MySQL 写入必须带条件：

```text
only update if incoming_version >= current_version
```

收益：

- LRU 淘汰后仍有持久后备数据。
- 节点重启不依赖缓存本身保存全部数据。
- 可支持更完整的数据恢复能力。

### 5. 增加非 owner 数据 GC

迁移完成后，旧节点可能残留不归自己的数据。后续应在 `stable` 状态下低频清理：

```text
scan local keys
ownerOf(key) != self
超过 grace period
delete local copy
```

收益：

- 控制内存占用。
- 清理迁移过程中产生的残留副本。

### 6. 修正 proto 字段拼写并做兼容迁移

当前 `DeleteRequest` 字段名是：

```proto
int64 verison = 4;
```

建议后续新增正确字段：

```proto
int64 version = 5;
```

服务端兼容读取两个字段，等客户端都升级后再考虑废弃旧字段。

### 7. 增加可观测性

建议增加指标：

- cache hit/miss。
- local/remote get latency。
- owner route hit。
- shadow sync 成功/失败。
- retry queue 长度。
- migration batch latency。
- migration copied keys。
- tombstone 数量。
- LRU eviction 数量。

收益：

- 能判断瓶颈在本地缓存、网络、etcd、迁移还是回源。
- 能用数据评估 LRU2 和 W-TinyLFU 的收益。

### 8. 整理 cmd/cache-node 和启动脚本

把示例入口更新到当前 API，并支持配置：

- node addr
- service name
- group name
- etcd endpoints
- data dir
- cache size
- store type

收益：

- 恢复 `go test ./...`。
- 更方便本地启动多节点压测。
- 降低集成测试和手工验证成本。

## 推荐下一步

建议按这个顺序推进：

1. 修正 `cmd/cache-node`，恢复 `go test ./...`。
2. 实现 rebalance coordinator 的最小版本。
3. 加非 owner GC。
4. 用现有 benchmark 对比 LRU2、SLRU、W-TinyLFU。
5. 强化 WAL replay 和 retry queue 恢复。
6. 接入 MySQL 条件更新和旁路缓存。

这个顺序的好处是：先保证分布式拓扑变化时数据安全，再优化命中率和持久化能力。
