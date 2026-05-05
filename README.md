# newCache

newCache 是一个用 Go 实现的分布式缓存项目。当前实现重点放在内存缓存、gRPC 节点通信、etcd 服务发现、一致性哈希路由、版本控制、墓碑删除、节点状态机和迁移链路上。

项目目标不是只做一个本地 LRU，而是逐步演进成一个支持多节点、可迁移、可恢复、可扩展持久化后端的缓存系统。

## 当前能力

- 本地缓存：支持 `simple`、`LRU`、`LRU2` 存储实现。
- LRU2：支持 L1/L2 两级缓存、TTL、删除、关闭、扫描和并发安全删除。
- 缓存命名空间：通过 `cache.Group` 管理一组缓存。
- singleflight：同一个 key 的并发回源会合并成一次加载。
- BloomFilter：支持空值缓存前的快速不存在判断。
- 版本控制：写入和删除都携带 version，旧版本不会覆盖新版本。
- Tombstone：删除不是简单物理删除，而是写入墓碑，防止旧写入复活。
- gRPC API：支持 `Get`、`Set`、`Delete`、`Scan`、`BatchSet`。
- etcd 注册发现：节点启动后注册到 etcd，并由 picker 拉取节点列表。
- 节点状态机：支持 `warming`、`active`、`draining`。
- 三环路由：
  - read ring：读流量路由。
  - write ring：写流量路由。
  - shadow ring：迁移/双写流量路由。
- 迁移基础：通过 `Scan` 导出数据，通过 `BatchSet` 导入目标节点。
- 集成测试：已有真实 etcd + 三节点 + gRPC 的集成测试。

## 核心语义

### 写入

写入时会携带 version：

```text
Set(group, key, value, version, ttl)
```

如果本地已有更高版本的数据，旧写入会被忽略：

```text
new version=100
old version=50
old 不会覆盖 new
```

### 删除

删除写入 tombstone：

```text
Delete(group, key, version)
```

tombstone 会保留 version。后续如果旧版本 Set 到达，不会把已删除的数据复活。

### TTL

TTL 数据只在内存层生效。后续如果接入 MySQL，建议语义是：

```text
ttl > 0: 只写内存和 AOF 留痕，不落 MySQL
ttl = 0: 写内存、AOF 和 MySQL
```

AOF 中应记录 `ExpiredAt`，恢复时跳过已过期数据，避免 TTL 数据重启后复活。

### 旁路缓存和 MySQL 规划

MySQL 适合作为 LRU 淘汰后的持久后备存储：

```text
Get
  -> 查内存
  -> miss 后查 MySQL
  -> 命中后回填内存
```

写路径建议后续演进为：

```text
Set/Delete
  -> 写内存
  -> 写 AOF
  -> 非 TTL 数据异步或同步落 MySQL
```

如果要更强持久性，AOF 应该优先于 MySQL，至少保证事件先可靠进入 AOF，再异步落库。

## 目录结构

```text
api/proto/              gRPC proto 和生成代码
cache/                  Group、CacheEntry、version、tombstone、迁移语义
store/                  simple、LRU、LRU2 存储实现
internal/client/         gRPC client 和 etcd picker
internal/server/         gRPC server
internal/registry/       etcd 注册、租约、节点状态
internal/consistenthash/ 一致性哈希
internal/mirgration/     Scan/BatchSet 迁移管理
internal/integration/    真实 etcd 三节点集成测试
cmd/cache-node/          节点启动示例
examples/                示例代码占位
```

## gRPC 接口

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

- `version`：写入和迁移时用于防止旧数据覆盖新数据。
- `verison`：DeleteRequest 当前字段名为 `verison`，注意这是 proto 中已有拼写。
- `ttl_ms`：TTL 毫秒数。
- `from_peer`：标记来自 peer 的请求，避免循环转发。
- `tombstone`：迁移和删除时保留删除语义。

## 节点状态

etcd 中节点信息使用 `registry.NodeInfo`：

```go
type NodeInfo struct {
	Addr   string     `json:"addr"`
	Status NodeStatus `json:"status"`
}
```

状态含义：

- `warming`：新节点预热中，只进 shadow ring，不接读写主流量。
- `active`：正常节点，进入 read/write/shadow 三个环。
- `draining`：准备下线，仍可读，不再接写。

## 迁移流程

当前迁移基础能力：

```text
source node
  -> Scan(startKey, count)
  -> TransportEntry / CacheEntry
  -> target node BatchSet(entries)
  -> target Get(key) 命中
```

迁移时会保留：

- key
- value
- version
- ttl
- tombstone

因此迁移后，旧版本写入仍不会覆盖新版本，tombstone 也不会被旧 Set 复活。

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

Race 版集成测试：

```bash
ETCD_ENDPOINTS=http://127.0.0.1:2379 \
GOCACHE=/tmp/newcache-gocache \
go test -race -count=1 ./internal/integration -v
```

如果没有可连接的 etcd，`internal/integration` 会 skip，不代表真实链路已经通过。

## 当前测试覆盖

已覆盖：

- LRU2 普通 Set/Get。
- TTL Set/Get。
- Delete。
- Close。
- L1 到 L2 晋升。
- Delete 并发安全。
- Scan 排序、startKey、count、TTL、跳过过期 key。
- 旧版本不覆盖新版本。
- tombstone 不复活。
- Get tombstone 返回 key 不存在。
- SetRequest/DeleteRequest/Scan/BatchSet proto 字段传递。
- FromPeer 不循环转发。
- warming 不接读。
- draining 不接写。
- owner 写路由。
- shadow retry 入队。
- Scan/BatchSet 迁移后 key 还能命中。
- 真实 etcd 三节点启动、路由、删除、状态机和迁移。

## 当前注意事项

`cmd/cache-node/main.go` 是示例入口，但当前仍存在旧 API 调用：

```text
group.Set(ctx, key, value)
```

而当前 `Group.Set` 签名是：

```go
Set(ctx context.Context, key string, value []byte, version int64, ttl time.Duration) error
```

因此直接运行：

```bash
go test ./...
```

会因为 `cmd/cache-node` 编译失败。当前验收建议使用上面的包级测试命令。后续应把 `cmd/cache-node` 更新到当前 API，再恢复 `go test ./...` 作为全量验收。

## 后续演进建议

1. 修正 `cmd/cache-node` 到当前 `Group.Set` API。
2. 增加 AOF 事件总线：
   - 每次 Set/Delete 生成 mutation event。
   - AOF consumer 写磁盘。
   - MySQL consumer 负责非 TTL 数据落库。
3. 接入 MySQL 旁路缓存：
   - 内存 miss 后查 MySQL。
   - 命中后回填内存。
   - TTL 数据不落 MySQL，只 AOF 留痕。
4. 增加 AOF replay：
   - 重启时恢复未过期 TTL 数据和永久数据。
   - 跳过已过期 TTL 数据。
5. 增加 MySQL version/tombstone 条件更新：
   - 防止异步落库乱序。
   - 防止旧 Set 覆盖 Delete。
6. 把迁移管理接入真实节点生命周期：
   - warming 拉取数据。
   - active 接入读写。
   - draining 停止写入并迁出数据。

## 提交状态

最近一次已验证的核心命令：

```bash
GOCACHE=/tmp/newcache-gocache go test ./store ./cache ./internal/server ./internal/client
GOCACHE=/tmp/newcache-gocache go test -race ./store ./cache ./internal/server ./internal/client
GOCACHE=/tmp/newcache-gocache go test -count=1 ./internal/integration -v
GOCACHE=/tmp/newcache-gocache go test -race -count=1 ./internal/integration -v
```

集成测试需要真实 etcd 和本机网络权限。
