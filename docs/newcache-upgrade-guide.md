# newCache 分层升级教学文档

本文档用于指导 `newCache` 从当前的教学版分布式缓存，逐步升级为具备正确性、一致性、扩缩容能力和工程展示价值的缓存系统。

参考项目：`Flux-KV`

重点不是照搬代码，而是学习它的设计主线：

- 节点扩缩容时如何迁移数据
- 迁移期间如何保证读写不中断
- 并发写入和删除时如何避免旧数据覆盖新数据
- 如何把缓存系统从“能跑”升级到“可解释、可验证、可演进”

---

## 总体升级路线

建议按照下面顺序推进：

1. 修复本地缓存正确性
2. 统一缓存数据模型
3. 补齐远程 API 语义
4. 明确写入策略
5. 完善失败重试
6. 服务发现升级为节点状态机
7. 拆分读环、写环、影子环
8. 增加数据迁移能力
9. 补齐工程化文档、配置、示例和测试

核心原则：

先保证单节点正确，再保证多节点写入语义正确，最后再做扩缩容迁移。

如果底层缓存还不稳定，直接做三环、迁移、双写，只会让问题更难定位。

---

## 第 0 层：修复本地缓存正确性

涉及文件：

- `store/lru2.go`
- `store/lru.go`
- `cache/cache.go`

### 要添加或修复什么

优先修复 `store/lru2.go`：

- 初始化 `LRU2Store.done`
- 初始化内部 `cache.expires`
- 修复 `LRU2Store.Set()` 新 key 不能插入的问题
- 修复 `SetWithExpiration()` 中 `ttl == 0` 时没有直接返回的问题
- 修复 `Get()` 命中 L1 后最后返回 `nil, false` 的问题
- 给 `Delete()` 加分片锁
- 让 `Close()` 支持安全关闭，避免 nil channel 或重复 close panic
- 给 `store` 包补单元测试

### 为什么这么做

本地缓存是整个系统的地基。

无论上层有 gRPC、etcd、一致性哈希、singleflight，最终每个节点都会调用本地 `Set/Get/Delete`。如果本地缓存有 bug，分布式层会把 bug 放大。

例如：

- 本地 `Set` 失败，上层会误以为远程同步失败
- 本地 TTL panic，上层表现为节点异常退出
- 本地 `Get` 明明命中却返回 miss，上层会误判为路由错误

### 如何做

#### 1. 初始化 `done`

当前 `LRU2Store` 的 `done` 没有初始化，但 `Close()` 中会 `close(s.done)`。

建议在构造函数中初始化：

```go
s := &LRU2Store{
    locks:  make([]sync.Mutex, count),
    caches: make([][2]*cache, count),
    mask:   uint32(count - 1),
    done:   make(chan struct{}),
}
```

#### 2. 初始化 `expires`

内部 `cache` 中有：

```go
expires map[string]time.Time
```

但构造函数没有初始化。调用 `SetWithExpiration()` 时会写入 `c.expires[key]`，如果 map 是 nil，会 panic。

建议：

```go
func NewCache(maxBytes int64) *cache {
    return &cache{
        maxBytes:  maxBytes,
        usedBytes: 0,
        ll:        list.New(),
        caches:    make(map[string]*list.Element),
        expires:   make(map[string]time.Time),
        onEvicted: func(key string, value Value) {
            log.Printf("delete key: %s", key)
        },
    }
}
```

#### 3. 修复 `Set()` 新 key 插入

现在 `LRU2Store.Set()` 对新 key 返回 `ErrSetErr`。这会导致普通写入无法插入。

合理行为是：

- 如果 L1 有，更新 L1
- 如果 L2 有，更新 L2
- 如果都没有，插入 L1

伪代码：

```go
if _, ok := l1.Get(key); ok {
    return l1.Set(key, value)
}
if _, ok := l2.Get(key); ok {
    return l2.Set(key, value)
}
return l1.Set(key, value)
```

#### 4. 修复 `SetWithExpiration()` 的 `ttl == 0`

现在代码里 `ttl == 0` 时调用了 `Set()`，但没有 return，后面还会继续执行 TTL 逻辑。

建议：

```go
if ttl == 0 {
    return s.Set(key, value)
}
```

内部 `cache.SetWithExpiration()` 也需要同样处理。

#### 5. 修复 `Get()` 返回值

现在 `Get()` 命中后有路径会返回 `nil, false`。

合理行为是：

- 命中 L1，无 TTL：迁移到 L2 后返回值
- 命中 L1，有 TTL：带剩余 TTL 迁移到 L2 后返回值
- 命中 L2：返回值
- 未命中：返回 `nil, false`

注意：如果从 L2 命中，不应该用 `l1.remainTTL(key)` 判断 TTL，因为 key 可能根本不在 L1。

### 不这么做的坏处

- TTL 写入可能直接 panic
- 普通 `Set` 可能永远失败
- `Get` 命中后仍返回 miss
- 并发 `Delete` 可能触发数据竞争
- 节点关闭可能 panic
- 后续分布式问题无法定位根因

### 推荐测试

新增 `store/lru2_test.go`，覆盖：

- `TestLRU2SetGet`
- `TestLRU2SetWithExpiration`
- `TestLRU2Delete`
- `TestLRU2Close`
- `TestLRU2Promotion`

运行：

```bash
go test ./store
go test -race ./store
```

---

## 第 1 层：统一缓存数据模型

涉及文件：

- `cache/byteview.go`
- `store/value.go`
- `cache/group.go`
- `api/proto/cache.proto`

### 要添加什么

当前缓存值主要是 `ByteView`，它只表达 value，不表达版本、过期时间和删除状态。

建议引入内部数据模型：

```go
type Entry struct {
    Value     ByteView
    Version   int64
    ExpireAt   time.Time
    Tombstone bool
}
```

### 为什么这么做

分布式缓存中，一个 key 的状态不只是 value。

你还需要知道：

- 这个值是不是过期了
- 这个值是不是删除标记
- 这个值的新旧版本
- 迁移时该不该覆盖目标节点已有数据

Flux-KV 的关键设计之一就是 `version + tombstone`：

- `version` 用来拒绝旧数据覆盖新数据
- `tombstone` 用来防止删除后的数据复活

### 如何做

#### 1. 写入时带版本

如果调用方没有传版本，可以用时间戳生成：

```go
if version <= 0 {
    version = time.Now().UnixNano()
}
```

本地写入时比较版本：

```go
if newVersion < oldVersion {
    return nil
}
```

#### 2. 删除时写 tombstone

不要只做物理删除。

建议：

```go
Delete(key) = Set(key, nil, tombstoneTTL, version, tombstone=true)
```

tombstone 可以设置较短 TTL，例如：

```go
60 * time.Second
```

或：

```go
5 * time.Minute
```

#### 3. 读取时识别 tombstone

如果 key 存在但 `Tombstone == true`，对外返回不存在。

### 不这么做的坏处

会出现典型的数据复活问题：

1. A 节点删除 key
2. B 节点还有旧 value
3. 迁移或重试时 B 把旧 value 推回 A
4. key 被删除后又出现

也会出现旧写覆盖新写：

1. 用户先写 `v2`
2. 网络延迟导致旧请求 `v1` 后到
3. 没有版本比较时，`v1` 覆盖 `v2`

---

## 第 2 层：补齐远程 API 语义

涉及文件：

- `api/proto/cache.proto`
- `internal/server/server.go`
- `internal/client/client.go`
- `cache/peers.go`

### 要添加什么

当前 proto 只有：

```proto
rpc Get(GetRequest) returns (GetResponse);
rpc Set(SetRequest) returns (SetResponse);
```

建议逐步升级为：

```proto
service CacheService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Set(SetRequest) returns (SetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Scan(ScanRequest) returns (ScanResponse);
  rpc BatchSet(BatchSetRequest) returns (BatchSetResponse);
}
```

### 为什么这么做

远程 API 必须表达完整缓存语义。

如果协议里没有 TTL、版本和删除，那么节点之间无法判断：

- 数据是否过期
- 数据谁更新
- 删除是否比写入更新
- 迁移时是否应该覆盖

### 如何做

#### 1. `SetRequest` 增加字段

```proto
message SetRequest {
  string group = 1;
  string key = 2;
  bytes value = 3;
  bool from_peer = 4;
  int64 ttl_ms = 5;
  int64 version = 6;
}
```

#### 2. 增加 `DeleteRequest`

```proto
message DeleteRequest {
  string group = 1;
  string key = 2;
  bool from_peer = 3;
  int64 version = 4;
}
```

#### 3. `GetResponse` 增加 `ok`

```proto
message GetResponse {
  bytes value = 1;
  bool ok = 2;
  int64 version = 3;
}
```

这样客户端能区分：

- value 为空字节
- key 不存在
- key 被 tombstone 标记

#### 4. 后续增加迁移接口

```proto
message ScanRequest {
  string group = 1;
  string start_key = 2;
  int64 count = 3;
}

message ScanResponse {
  repeated Entry entries = 1;
}

message Entry {
  string key = 1;
  bytes value = 2;
  int64 ttl_ms = 3;
  int64 version = 4;
  bool tombstone = 5;
}
```

### 不这么做的坏处

- 删除无法跨节点传播
- 迁移无法保留 TTL
- 旧数据和新数据无法比较
- 客户端无法判断 key 是不存在还是空值
- 以后做扩缩容时必须大改协议

---

## 第 3 层：明确写入策略

涉及文件：

- `cache/group.go`
- `internal/client/picker.go`
- `internal/client/client.go`

### 要添加什么

你需要明确选择一种写入策略：

1. owner 写
2. 本地写 + 广播失效
3. 本地写 + 异步复制

建议选择 owner 写。

### 为什么这么做

你已经有一致性哈希。既然一致性哈希定义了 key 的 owner，就应该让 owner 成为写入事实来源。

否则任意节点都能写同一个 key，写入顺序会混乱。

### 如何做

#### owner 写流程

1. `Set(ctx, key, value)` 收到写请求
2. 调用 `PickPeer(key)` 判断 owner
3. 如果 owner 是自己，写本地
4. 如果 owner 是远程，转发给远程 owner
5. 从 peer 收到请求时带 `FromPeer`
6. 本地写入时比较 `version`

伪代码：

```go
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
    if isPeer(ctx) {
        return g.setLocally(key, value, version)
    }

    peer, ok, isSelf := g.peers.PickPeer(key)
    if ok && !isSelf {
        return peer.Set(ctx, g.name, key, value)
    }

    return g.setLocally(key, value, version)
}
```

### 不这么做的坏处

当前模式是先写本地，再异步同步 peer。

这会导致：

- A 节点写了新值
- B 节点稍后同步旧值
- 最终旧值覆盖新值

还会导致：

- 网络失败只打日志
- retry 没生效
- 数据永久不一致

---

## 第 4 层：完善失败重试机制

涉及文件：

- `cache/group.go`

### 要添加什么

你已经有：

- `retryCh`
- `syncTask`
- `retryLoop()`
- `backoff()`

但目前缺少完整闭环。

建议添加：

- `NewGroup()` 中启动 `go g.retryLoop()`
- `syncToPeer()` 失败时调用 `enqueueRetry`
- `syncTask` 添加 `version`
- `syncTask` 添加最大重试次数
- Group 关闭时能停止 retry loop

### 为什么这么做

远程同步失败是正常情况，不是异常情况。

网络抖动、peer 重启、etcd 列表延迟、gRPC 超时都会导致短暂失败。只打日志等于默认丢数据。

### 如何做

#### 1. 扩展 `syncTask`

```go
type syncTask struct {
    key     string
    value   []byte
    version int64
    attempt int
}
```

#### 2. 失败入队

```go
if err := peer.Set(ctx, g.name, key, value); err != nil {
    g.enqueueRetry(syncTask{
        key:     key,
        value:   value,
        version: version,
        attempt: 1,
    })
}
```

#### 3. 启动 retry loop

```go
go g.retryLoop()
```

#### 4. 控制生命周期

建议给 `Group` 增加：

```go
stopCh chan struct{}
```

关闭时让 retry loop 退出。

### 不这么做的坏处

- 一次网络抖动就可能造成永久不一致
- 失败只存在日志中，系统不会自愈
- 后续做双写时，影子写失败无法追踪

---

## 第 5 层：服务发现升级为节点状态机

涉及文件：

- `internal/registry/etcd.go`
- `internal/client/picker.go`

### 要添加什么

当前 etcd 中只注册 addr。

建议注册结构化信息：

```go
type NodeInfo struct {
    Addr   string `json:"addr"`
    Status string `json:"status"`
}
```

状态：

```go
const (
    StatusWarming  = "warming"
    StatusActive   = "active"
    StatusDraining = "draining"
)
```

### 为什么这么做

节点不是只有“在线”和“离线”。

扩缩容时需要更细的生命周期：

- `warming`：新节点启动，正在迁移数据
- `active`：正常服务
- `draining`：准备下线，正在迁出数据

### 如何做

#### 1. 注册时写 warming

```go
info := NodeInfo{
    Addr:   addr,
    Status: StatusWarming,
}
```

#### 2. 迁移完成后切 active

```go
registry.UpdateStatus(StatusActive)
```

#### 3. 退出前切 draining

```go
registry.UpdateStatus(StatusDraining)
```

#### 4. 迁出完成后注销

```go
registry.Close(ctx)
```

#### 5. picker 解析 JSON

watch etcd 时不再直接读取 value 作为 addr，而是反序列化 `NodeInfo`。

### 不这么做的坏处

- 新节点刚上线就接流量，但没有数据
- 老节点刚下线，数据还没迁走
- 哈希环变化后大量 key miss
- 无法实现平滑扩缩容

---

## 第 6 层：拆分读环、写环、影子环

涉及文件：

- `internal/client/picker.go`
- `internal/consistenthash/consistenthash.go`

### 要添加什么

维护三个一致性哈希环：

```go
readRing   // active + draining
writeRing  // active
shadowRing // active + warming
```

### 为什么这么做

不同状态的节点承担不同职责：

- `warming` 数据还没准备好，不应该读
- `active` 正常读写
- `draining` 准备下线，不应该接新写，但可以兜底读
- `shadowRing` 用于迁移期间双写到未来 owner

### 如何做

#### 1. picker 结构中增加三个 ring

```go
type Picker struct {
    readRing   *consistenthash.Map
    writeRing  *consistenthash.Map
    shadowRing *consistenthash.Map
}
```

#### 2. 根据节点状态重建 ring

```go
switch status {
case StatusActive:
    readRing.Add(addr)
    writeRing.Add(addr)
    shadowRing.Add(addr)
case StatusWarming:
    shadowRing.Add(addr)
case StatusDraining:
    readRing.Add(addr)
}
```

#### 3. Get 走读环

```go
addr := readRing.Get(key)
```

#### 4. Set/Delete 走写环

```go
addr := writeRing.Get(key)
```

#### 5. 迁移期间双写 shadow owner

```go
primary := writeRing.Get(key)
shadow := shadowRing.Get(key)

if shadow != "" && shadow != primary {
    // shadow write
}
```

### 不这么做的坏处

- 新节点没数据却开始读
- 旧节点下线期间读请求失败
- 迁移期间新写只写到旧节点
- 新节点 active 后缺少迁移期间的增量写入

---

## 第 7 层：添加数据迁移能力

涉及文件：

- `api/proto/cache.proto`
- `internal/server/server.go`
- `internal/client/client.go`
- 新增 `internal/migration`

### 要添加什么

至少添加：

- `Scan(startKey, count)`
- `BatchSet(entries)`
- `MigrationManager`
- 扩容 pull
- 缩容 push

### 为什么这么做

一致性哈希只决定 key 应该归谁，不会自动移动数据。

节点变化后：

- key 的 owner 变了
- value 还在旧节点
- 如果没有迁移，新 owner 会 miss

### 如何做

#### 扩容流程

1. 新节点注册为 `warming`
2. 构建 future ring：`active + warming`
3. 找出 future ring 中应该属于新节点的 key
4. 从旧节点 `Scan`
5. 新节点 `BatchSet`
6. 迁移完成后状态切为 `active`

#### 缩容流程

1. 节点收到退出信号
2. 状态切为 `draining`
3. 停止接新写
4. 扫描本地数据
5. 根据新写环找到新 owner
6. `BatchSet` 推给新 owner
7. 推送完成后注销 etcd

#### Scan 接口注意点

`Scan` 不一定要一开始就做到全局有序，但至少要支持分页：

```go
Scan(startKey string, count int64)
```

返回：

- key
- value
- ttl
- version
- tombstone

### 不这么做的坏处

- 扩容后大量 key miss
- 缩容后数据直接丢失
- 迁移期间写入无法保证到达新 owner
- 系统只能“发现节点”，不能“平滑改变节点”

---

## 第 8 层：补齐工程化能力

涉及文件：

- `README.md`
- `config.yaml`
- `cmd/cache-node/main.go`
- `examples/three-nodes`
- `Makefile`

### 要添加什么

建议补齐：

- `README.md`
- `etc/config.yaml`
- 三节点启动 demo
- Makefile
- 压测脚本
- 指标统计
- CI

### 为什么这么做

如果这是学习项目或面试项目，别人最先看的不是你的某一行代码，而是：

- 项目解决什么问题
- 怎么启动
- 架构是否清晰
- 测试是否覆盖关键风险
- 分布式设计是否讲得通

### 如何做

#### README 推荐结构

```text
1. 项目简介
2. 架构图
3. 单机缓存设计
4. 分布式路由设计
5. 一致性策略
6. 节点扩缩容流程
7. 快速启动
8. 测试方法
9. 后续规划
```

#### config.yaml 示例

```yaml
serviceName: new-cache
groupName: scores
cacheBytes: 2097152
etcdEndpoints:
  - http://127.0.0.1:2379
grpcTimeout: 500ms
serviceTTL: 10
```

#### Makefile 示例

```makefile
test:
	go test ./...

race:
	go test -race ./...

node1:
	go run ./cmd/cache-node -port 8001 -node A

node2:
	go run ./cmd/cache-node -port 8002 -node B

node3:
	go run ./cmd/cache-node -port 8003 -node C
```

### 不这么做的坏处

- 项目能力藏在代码里，别人看不出来
- demo 难以复现
- 面试时只能口头解释
- 出问题后没有指标和测试辅助定位

---

## 推荐测试清单

### store 层测试

- 新 key 能写入
- TTL 写入不会 panic
- TTL 到期后 miss
- L1 命中晋升 L2 后仍然返回值
- Delete 并发安全
- Close 不 panic

### cache 层测试

- `singleflight` 合并回源
- 布隆过滤器挡住不存在 key
- 空值缓存生效
- owner 写转发正确
- peer 请求不会循环转发

### client/picker 层测试

- 节点加入后 ring 更新
- 节点删除后连接关闭
- active 节点进入读环、写环、影子环
- warming 节点只进入影子环
- draining 节点只进入读环

### 分布式集成测试

- 三节点启动后同一个 key 总是路由到同一 owner
- 新节点 warming 时不接读
- 新节点 active 后能读到迁移数据
- draining 节点不接新写
- Delete 后迁移不会复活旧数据

---

## 最终路线图

### 第一阶段：稳定单机缓存

目标：

- 修复 `LRU2Store`
- 补齐 store 单测
- 跑通 `go test -race ./store`

完成标志：

- 本地 `Set/Get/Delete/TTL/Close` 都稳定

### 第二阶段：明确写入一致性

目标：

- `Set/Delete` 支持 version
- Delete 改为 tombstone
- owner 写策略落地

完成标志：

- 旧版本写入不会覆盖新版本
- 删除不会被旧数据迁移复活

### 第三阶段：服务发现状态化

目标：

- etcd 注册 `NodeInfo`
- 支持 `warming/active/draining`
- picker 能根据状态构建不同 ring

完成标志：

- 新节点上线不立即接读
- 老节点下线不立即丢读

### 第四阶段：扩缩容迁移

目标：

- 增加 `Scan`
- 增加 `BatchSet`
- 实现扩容 pull 和缩容 push

完成标志：

- 新节点加入后能迁移属于自己的 key
- 节点退出前能迁出本地数据

### 第五阶段：工程展示

目标：

- README
- config
- Makefile
- 三节点 demo
- 指标和测试说明

完成标志：

- 任何人能按 README 启动和验证项目

---

## 总结

你的项目现在已经有不错的基础组件：

- gRPC 通信
- etcd 服务发现
- 一致性哈希
- `singleflight`
- 布隆过滤器
- Group 命名空间

但当前最需要提升的是：

1. 本地缓存正确性
2. 写入语义一致性
3. 删除语义
4. 节点状态机
5. 数据迁移协议
6. 工程化文档和测试

不要一开始就直接实现三环和迁移。正确顺序是：

```text
单节点正确 -> 多节点写入正确 -> 节点状态正确 -> 数据迁移正确 -> 工程展示完整
```

这样每一步都有可验证结果，系统复杂度也不会失控。
