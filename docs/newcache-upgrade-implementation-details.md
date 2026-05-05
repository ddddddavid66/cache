# newCache 分层升级具体落地实现手册

本文档是 `docs/newcache-upgrade-guide.md` 的“具体实现版”。原文档偏设计思路，这份文档重点回答：

- 具体新增哪些文件
- 每个文件里放什么代码
- 现有函数怎么改
- 改完以后怎么测试
- 每一阶段的验收标准是什么

注意：

- 本文档只是实现手册，不会覆盖 `docs/newcache-upgrade-guide.md`。
- 代码片段按阶段给出，不建议一次性全改。
- 推荐顺序是：先修 store，再加版本和 tombstone，再扩 proto，再做 owner 写和迁移。

---

## 当前项目定位

当前 `newCache` 更准确的定位是：

```text
带 etcd 服务发现、一致性哈希和 gRPC 节点通信的分布式缓存系统
```

不是严格意义上的分布式 KV 存储。

原因：

- 数据主要存在内存里
- 有 `Getter` 回源能力
- 节点重启后缓存数据默认会丢
- 现在没有 WAL、Snapshot、副本复制、Raft 或持久化版本水位

所以后续设计要围绕“分布式缓存”推进，而不是一开始就把它改成数据库。

---

## 阶段 0：本地 LRU2 正确性修复

目标：

```text
保证本地 Set/Get/Delete/TTL/Close 正确，再做分布式能力。
```

涉及文件：

- `store/lru2.go`
- `store/lru2_test.go`

### 0.1 `NewLRU2Store` 初始化 `done`

`LRU2Store.Close()` 会关闭 `s.done`，所以构造函数必须初始化它。

实现：

```go
func NewLRU2Store(count int, maxBytes1, maxBytes2 int64) *LRU2Store {
    l1Store := maxBytes1 / int64(count)
    l2Store := maxBytes2 / int64(count)

    s := &LRU2Store{
        locks:  make([]sync.Mutex, count),
        caches: make([][2]*cache, count),
        mask:   uint32(count - 1),
        done:   make(chan struct{}),
    }

    for i := 0; i < count; i++ {
        s.caches[i][0] = NewCache(l1Store)
        s.caches[i][1] = NewCache(l2Store)
    }

    s.StartCleaner(30 * time.Second)
    return s
}
```

注意：如果 `StartCleaner()` 内部已经启动 goroutine，外层就不要再写 `go s.StartCleaner(...)`。否则会多套一层 goroutine，虽然不一定错，但语义不清晰。

推荐：

```go
s.StartCleaner(30 * time.Second)
```

而不是：

```go
go s.StartCleaner(30 * time.Second)
```

### 0.2 初始化内部 `cache.expires`

当前内部 `cache` 有：

```go
expires map[string]time.Time
```

必须在构造函数里初始化。

实现：

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

### 0.3 修复 `LRU2Store.Set`

正确语义：

- L1 有 key，更新 L1
- L2 有 key，更新 L2
- 都没有，插入 L1

实现：

```go
func (s *LRU2Store) Set(key string, value Value) error {
    index := s.getIndex(key)
    l1 := s.caches[index][0]
    l2 := s.caches[index][1]

    s.locks[index].Lock()
    defer s.locks[index].Unlock()

    if _, ok := l1.Get(key); ok {
        return l1.Set(key, value)
    }
    if _, ok := l2.Get(key); ok {
        return l2.Set(key, value)
    }
    return l1.Set(key, value)
}
```

不要写成：

```go
if value, ok := l1.Get(key); ok {
    l1.Set(key, value)
}
```

这里变量名 `value` 会遮蔽外部传入的新值，导致用旧值覆盖旧值。

### 0.4 修复 `LRU2Store.SetWithExpiration`

正确语义：

- `ttl == 0` 时走普通 `Set`
- 已存在 key 时刷新值和 TTL
- 新 key 插入 L1，并写入 TTL

实现：

```go
func (s *LRU2Store) SetWithExpiration(key string, value Value, ttl time.Duration) error {
    if ttl == 0 {
        return s.Set(key, value)
    }

    index := s.getIndex(key)
    l1 := s.caches[index][0]
    l2 := s.caches[index][1]

    s.locks[index].Lock()
    defer s.locks[index].Unlock()

    if _, ok := l1.Get(key); ok {
        return l1.SetWithExpiration(key, value, ttl)
    }
    if _, ok := l2.Get(key); ok {
        return l2.SetWithExpiration(key, value, ttl)
    }
    return l1.SetWithExpiration(key, value, ttl)
}
```

### 0.5 修复内部 `cache.SetWithExpiration`

内部 `cache.SetWithExpiration()` 也要在 `ttl == 0` 时直接返回。

实现：

```go
func (c *cache) SetWithExpiration(key string, value Value, ttl time.Duration) error {
    if ttl == 0 {
        return c.Set(key, value)
    }

    ttl = randomTTL(ttl, 1*time.Minute)
    if ele, ok := c.caches[key]; ok {
        c.ll.MoveToFront(ele)
        entry := ele.Value.(*entry)
        c.usedBytes += int64(value.Len()) - int64(entry.value.Len())
        entry.value = value
        c.expires[key] = time.Now().Add(ttl)
        return nil
    }

    ele := c.ll.PushFront(&entry{key: key, value: value})
    c.usedBytes += int64(len(key)) + int64(value.Len())
    c.caches[key] = ele
    c.expires[key] = time.Now().Add(ttl)

    for c.maxBytes > 0 && c.maxBytes < c.usedBytes {
        c.removeOldest()
    }
    return nil
}
```

### 0.6 修复 `LRU2Store.Get`

正确语义：

- 命中 L1，有 TTL：带剩余 TTL 迁移到 L2，删除 L1，返回值
- 命中 L1，无 TTL：迁移到 L2，删除 L1，返回值
- 命中 L2：直接返回值
- 未命中：返回 `nil, false`

实现：

```go
func (s *LRU2Store) Get(key string) (Value, bool) {
    index := s.getIndex(key)
    l1 := s.caches[index][0]
    l2 := s.caches[index][1]

    s.locks[index].Lock()
    defer s.locks[index].Unlock()

    value, ok := l1.Get(key)
    if ok {
        if ttl := l1.remainTTL(key); ttl > 0 {
            l2.SetWithExpiration(key, value, ttl)
        } else {
            l2.Set(key, value)
        }
        l1.Delete(key)
        return value, true
    }

    value, ok = l2.Get(key)
    if ok {
        return value, true
    }

    return nil, false
}
```

### 0.7 修复 `LRU2Store.Delete` 并发安全

当前 `Delete()` 应该和 `Get/Set` 一样加分片锁。

实现：

```go
func (s *LRU2Store) Delete(key string) bool {
    index := s.getIndex(key)
    l1 := s.caches[index][0]
    l2 := s.caches[index][1]

    s.locks[index].Lock()
    defer s.locks[index].Unlock()

    deleted := false
    if l1.Delete(key) {
        deleted = true
    }
    if l2.Delete(key) {
        deleted = true
    }
    return deleted
}
```

### 0.8 `Close` 防重复关闭

如果 `Close()` 可能被调用多次，就要防止重复 `close(s.done)`。

推荐给 `LRU2Store` 加字段：

```go
closed atomic.Bool
```

结构体：

```go
type LRU2Store struct {
    locks  []sync.Mutex
    caches [][2]*cache
    mask   uint32
    done   chan struct{}
    closed atomic.Bool
}
```

实现：

```go
func (s *LRU2Store) Close() error {
    if s.closed.Swap(true) {
        return nil
    }
    close(s.done)

    for i := range s.caches {
        s.locks[i].Lock()
        s.caches[i][0].clear()
        s.caches[i][1].clear()
        s.locks[i].Unlock()
    }
    return nil
}
```

### 0.9 测试文件

新增：

```text
store/lru2_test.go
```

测试清单：

```go
func TestLRU2SetGet(t *testing.T) {}
func TestLRU2SetWithExpiration(t *testing.T) {}
func TestLRU2Delete(t *testing.T) {}
func TestLRU2Close(t *testing.T) {}
func TestLRU2Promotion(t *testing.T) {}
```

运行：

```bash
go test ./store
go test -race ./store
```

验收标准：

```text
ok newCache/store
```

---

## 阶段 1：新增缓存业务条目 `CacheEntry`

目标：

```text
让缓存值表达 value、version、expire、tombstone，而不是只有 ByteView。
```

### 1.1 不要复用 `store.entry`

你现在已有：

```go
type entry struct {
    key   string
    value Value
}
```

这个是 LRU 链表节点的内部结构，不是业务数据结构。

正确关系：

```text
LRU list.Element
  -> store.entry{key, value}
       -> cache.CacheEntry{Value, Version, ExpireAt, Tombstone}
```

也就是说：

- `store.entry`：底层 LRU 淘汰用
- `cache.CacheEntry`：上层缓存语义用

### 1.2 建议新增文件

新增：

```text
cache/cache_entry.go
```

不要放在 `store` 包里，原因：

- `CacheEntry` 依赖 `ByteView`
- `CacheEntry` 是 cache 层业务语义
- `store` 层只应该知道 `Value` 接口，不应该知道 tombstone/version

### 1.3 `CacheEntry` 实现

```go
package cache

import "time"

type CacheEntry struct {
    Value     ByteView
    Version   int64
    ExpireAt  time.Time
    Tombstone bool
}

func NewCacheEntry(value []byte, ttl time.Duration, version int64) CacheEntry {
    e := CacheEntry{
        Value:   NewByteView(value),
        Version: version,
    }
    if ttl > 0 {
        e.ExpireAt = time.Now().Add(ttl)
    }
    return e
}

func NewTombstone(version int64, ttl time.Duration) CacheEntry {
    e := CacheEntry{
        Version:   version,
        Tombstone: true,
    }
    if ttl > 0 {
        e.ExpireAt = time.Now().Add(ttl)
    }
    return e
}

func (e CacheEntry) Len() int {
    // value + version + expire unix nano + tombstone flag
    return e.Value.Len() + 8 + 8 + 1
}

func (e CacheEntry) Expired(now time.Time) bool {
    return !e.ExpireAt.IsZero() && now.After(e.ExpireAt)
}
```

注意：

```go
func (e CacheEntry) Len() int
```

这个方法让 `CacheEntry` 自动实现 `store.Value` 接口。

### 1.4 如果你已经写了 `store/cacheEntry.go`

当前如果有类似代码：

```go
type CacheEnrty struct {
    value      Value
    ExpireAt   time.Time
    version    int64
    Tombstione bool
}
```

建议调整：

- `CacheEnrty` 拼写改成 `CacheEntry`
- `Tombstione` 改成 `Tombstone`
- 字段导出：`Value`、`Version`
- 移到 `cache` 包更合适

如果暂时不移动，也至少改成：

```go
type CacheEntry struct {
    Value     Value
    ExpireAt  time.Time
    Version   int64
    Tombstone bool
}
```

否则上层 `cache.Group` 很难访问 `version` 和 `tombstone`。

---

## 阶段 2：让 `Cache` 同时支持 ByteView 和 CacheEntry

目标：

```text
不一次性破坏现有代码，先兼容旧 ByteView，再逐步迁移到 CacheEntry。
```

涉及文件：

- `cache/cache.go`

### 2.1 增加 `GetEntry`

```go
func (c *Cache) GetEntry(key string) (CacheEntry, bool) {
    if c.closed.Load() {
        return CacheEntry{}, false
    }

    v, ok := c.store.Get(key)
    if !ok {
        atomic.AddInt64(&c.misses, 1)
        return CacheEntry{}, false
    }

    switch entry := v.(type) {
    case CacheEntry:
        if entry.Tombstone {
            atomic.AddInt64(&c.hits, 1)
            return entry, true
        }
        atomic.AddInt64(&c.hits, 1)
        return entry, true
    case ByteView:
        atomic.AddInt64(&c.hits, 1)
        return CacheEntry{Value: entry}, true
    default:
        atomic.AddInt64(&c.misses, 1)
        return CacheEntry{}, false
    }
}
```

### 2.2 增加 `SetEntry`

```go
func (c *Cache) SetEntry(key string, entry CacheEntry, ttl time.Duration) error {
    if c.closed.Load() {
        return errors.New("cache is closed")
    }
    if ttl > 0 {
        return c.store.SetWithExpiration(key, entry, ttl)
    }
    return c.store.Set(key, entry)
}
```

### 2.3 保留原来的 `Get`

为了减少改动面，原来的 `Get(key) (ByteView, bool)` 继续保留，但要能识别 `CacheEntry`。

```go
func (c *Cache) Get(key string) (ByteView, bool) {
    entry, ok := c.GetEntry(key)
    if !ok || entry.Tombstone {
        return ByteView{}, false
    }
    return entry.Value, true
}
```

这样旧代码还能继续用：

```go
view, ok := g.mainCache.Get(key)
```

新代码可以用：

```go
entry, ok := g.mainCache.GetEntry(key)
```

---

## 阶段 3：版本号生成与比较

目标：

```text
所有 Set/Delete 都带版本，旧版本不能覆盖新版本。
```

涉及文件：

- `cache/version.go`

### 3.1 新增版本生成器

新增：

```text
cache/version.go
```

实现：

```go
package cache

import (
    "sync/atomic"
    "time"
)

var lastVersion int64

func nextVersion() int64 {
    for {
        now := time.Now().UnixNano()
        old := atomic.LoadInt64(&lastVersion)
        if now <= old {
            now = old + 1
        }
        if atomic.CompareAndSwapInt64(&lastVersion, old, now) {
            return now
        }
    }
}
```

为什么不用简单的 `time.Now().UnixNano()`：

- 同一纳秒内可能生成多个版本
- 单机内用 CAS 可以保证单调递增

### 3.2 在 `Group` 内比较版本

新增方法：

```go
func newerOrEqual(incoming, current int64) bool {
    return incoming >= current
}
```

写入前：

```go
old, ok := g.mainCache.GetEntry(key)
if ok && !newerOrEqual(version, old.Version) {
    return nil
}
```

### 3.3 为什么 tombstone 也要版本

Delete 也是一次写。

示例：

```text
SET user:1 version=10
DELETE user:1 version=11
旧 SET user:1 version=9 延迟到达
```

如果没有 tombstone version，旧 SET 会让 key 复活。

有 tombstone version 后：

```text
9 < 11，拒绝
```

---

## 阶段 4：改造 `Group.Set/Get/Delete`

目标：

```text
Group 层开始使用 CacheEntry，支持 version 和 tombstone。
```

涉及文件：

- `cache/group.go`
- `cache/options.go`

### 4.1 配置普通 TTL 和 tombstone TTL

当前有全局变量：

```go
var ttl = 5 * time.Minute
const deleteTomestoneTTL = 5 * time.Minute
```

建议改成 `Group` 字段，避免所有 group 共用一套硬编码。

`Group` 增加：

```go
type Group struct {
    name         string
    getter       Getter
    mainCache    *Cache
    peers        PeerPicker
    loader       *singleflight.Group
    loadSem      chan struct{}
    bloomFilter  *BloomFilter
    retryCh      chan syncTask
    ttl          time.Duration
    tombstoneTTL time.Duration
}
```

默认值：

```go
const defaultTTL = 5 * time.Minute
const defaultTombstoneTTL = 5 * time.Minute
```

`NewGroup` 中：

```go
g := &Group{
    name:         name,
    getter:       getter,
    mainCache:    NewCache(cacheBytes),
    loader:       singleflight.NewGroup(),
    loadSem:      make(chan struct{}, 100),
    retryCh:      make(chan syncTask, 1024),
    ttl:          defaultTTL,
    tombstoneTTL: defaultTombstoneTTL,
}
```

### 4.2 `options.go` 增加 tombstone TTL

```go
type cacheConfig struct {
    storeType    string
    bucketCount  int
    ttl          time.Duration
    tombstoneTTL time.Duration
    onEvicted    func(key string, value store.Value)
    bloomN       int
    bloomP       float64
    maxBytes     int64
}

func WithTombstoneTTL(d time.Duration) Option {
    return func(c *cacheConfig) {
        c.tombstoneTTL = d
    }
}
```

如果暂时不想大改 `NewGroup` 签名，可以先只用常量：

```go
const defaultTombstoneTTL = 5 * time.Minute
```

### 4.3 改造 `Get`

目标：

- tombstone 对外表现为 key 不存在
- 命中普通 entry 返回 value
- 未命中继续走 load

实现：

```go
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
    if key == "" {
        return ByteView{}, ErrKeyRequired
    }

    if g.bloomFilter != nil && !g.bloomFilter.MayExist(key) {
        _ = g.populateCache(key, *nullMaker, 30*time.Second)
        return ByteView{}, ErrKey
    }

    if entry, ok := g.mainCache.GetEntry(key); ok {
        if entry.Tombstone {
            return ByteView{}, ErrKey
        }
        return entry.Value, nil
    }

    return g.load(ctx, key)
}
```

### 4.4 新增 `populateEntry`

保留旧 `populateCache`，新增 entry 版本：

```go
func (g *Group) populateEntry(key string, entry CacheEntry, ttl time.Duration) error {
    return g.mainCache.SetEntry(key, entry, ttl)
}
```

原来的：

```go
func (g *Group) populateCache(key string, view ByteView, ttl time.Duration) error
```

可以先保留，内部转为 `CacheEntry`：

```go
func (g *Group) populateCache(key string, view ByteView, ttl time.Duration) error {
    entry := CacheEntry{
        Value:   view,
        Version: nextVersion(),
    }
    if ttl > 0 {
        entry.ExpireAt = time.Now().Add(ttl)
    }
    return g.populateEntry(key, entry, ttl)
}
```

### 4.5 改造 `Set`

保留原签名：

```go
func (g *Group) Set(ctx context.Context, key string, value []byte) error
```

内部生成版本：

```go
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
    return g.SetWithVersion(ctx, key, value, g.ttl, nextVersion())
}
```

新增：

```go
func (g *Group) SetWithVersion(ctx context.Context, key string, value []byte, ttl time.Duration, version int64) error {
    if key == "" {
        return ErrKeyRequired
    }
    if value == nil {
        return ErrValueRequired
    }
    if version <= 0 {
        version = nextVersion()
    }

    old, ok := g.mainCache.GetEntry(key)
    if ok && version < old.Version {
        return nil
    }

    entry := NewCacheEntry(value, ttl, version)
    if err := g.populateEntry(key, entry, ttl); err != nil {
        return err
    }

    if isPeer(ctx) {
        return nil
    }

    go g.syncToPeerWithVersion(key, value, ttl, version)
    return nil
}
```

### 4.6 改造 `Delete`

当前：

```go
func (g *Group) Delete(ctx context.Context, key string) bool
```

建议先保留 bool 版本，但内部写 tombstone：

```go
func (g *Group) Delete(ctx context.Context, key string) bool {
    if key == "" {
        return false
    }

    version := nextVersion()
    old, ok := g.mainCache.GetEntry(key)
    if ok && version < old.Version {
        return false
    }

    tombstone := NewTombstone(version, g.tombstoneTTL)
    if err := g.populateEntry(key, tombstone, g.tombstoneTTL); err != nil {
        return false
    }

    if !isPeer(ctx) {
        go g.syncDeleteToPeer(key, version)
    }
    return true
}
```

更推荐后续改成：

```go
func (g *Group) Delete(ctx context.Context, key string) error
```

因为 bool 不能表达远程失败、版本旧、key 为空等具体错误。

---

## 阶段 5：扩展 Peer 接口和 gRPC 协议

目标：

```text
远程节点也能接收 version、ttl、delete。
```

涉及文件：

- `cache/peers.go`
- `api/proto/cache.proto`
- `internal/client/client.go`
- `internal/server/server.go`

### 5.1 修改 `cache/peers.go`

当前：

```go
type PeerGetter interface {
    Get(ctx context.Context, group string, key string) ([]byte, error)
    Set(ctx context.Context, group string, key string, value []byte) error
}
```

建议扩展为：

```go
type PeerGetter interface {
    Get(ctx context.Context, group string, key string) ([]byte, error)
    Set(ctx context.Context, group string, key string, value []byte) error
    SetWithMeta(ctx context.Context, group string, key string, value []byte, ttl time.Duration, version int64) error
    Delete(ctx context.Context, group string, key string, version int64) error
}
```

过渡期可以保留 `Set`，让旧代码继续编译。

### 5.2 修改 proto

`api/proto/cache.proto` 改为：

```proto
syntax = "proto3";

package cachepb;

option go_package = "newCache/api/proto;cachepb";

service CacheService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Set(SetRequest) returns (SetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

message GetRequest {
  string group = 1;
  string key = 2;
}

message GetResponse {
  bytes value = 1;
  bool ok = 2;
  int64 version = 3;
}

message SetRequest {
  string group = 1;
  string key = 2;
  bytes value = 3;
  bool from_peer = 4;
  int64 ttl_ms = 5;
  int64 version = 6;
}

message SetResponse {
  bool ok = 1;
}

message DeleteRequest {
  string group = 1;
  string key = 2;
  bool from_peer = 3;
  int64 version = 4;
}

message DeleteResponse {
  bool ok = 1;
}
```

生成 pb：

```bash
protoc \
  --go_out=. \
  --go-grpc_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_opt=paths=source_relative \
  api/proto/cache.proto
```

如果没有安装插件：

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### 5.3 修改 client

`internal/client/client.go` 增加：

```go
func (c *Client) SetWithMeta(ctx context.Context, group string, key string, value []byte, ttl time.Duration, version int64) error {
    resp, err := c.cli.Set(ctx, &cachepb.SetRequest{
        Group:    group,
        Key:      key,
        Value:    value,
        FromPeer: cache.IsPeer(ctx),
        TtlMs:    ttl.Milliseconds(),
        Version:  version,
    })
    if err != nil {
        return err
    }
    if !resp.Ok {
        return fmt.Errorf("set key: %s failed", key)
    }
    return nil
}

func (c *Client) Delete(ctx context.Context, group string, key string, version int64) error {
    resp, err := c.cli.Delete(ctx, &cachepb.DeleteRequest{
        Group:    group,
        Key:      key,
        FromPeer: cache.IsPeer(ctx),
        Version:  version,
    })
    if err != nil {
        return err
    }
    if !resp.Ok {
        return fmt.Errorf("delete key: %s failed", key)
    }
    return nil
}
```

旧 `Set` 可以转调：

```go
func (c *Client) Set(ctx context.Context, group string, key string, value []byte) error {
    return c.SetWithMeta(ctx, group, key, value, 0, 0)
}
```

### 5.4 修改 server

`internal/server/server.go` 的 `Set`：

```go
func (s *Server) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
    g := cache.GetGroup(req.Group)
    if g == nil {
        return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
    }
    if req.FromPeer {
        ctx = cache.WithPeer(ctx)
    }

    ttl := time.Duration(req.TtlMs) * time.Millisecond
    if ttl <= 0 {
        ttl = 5 * time.Minute
    }

    if err := g.SetWithVersion(ctx, req.Key, req.Value, ttl, req.Version); err != nil {
        return nil, err
    }
    return &cachepb.SetResponse{Ok: true}, nil
}
```

新增 `Delete`：

```go
func (s *Server) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
    g := cache.GetGroup(req.Group)
    if g == nil {
        return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
    }
    if req.FromPeer {
        ctx = cache.WithPeer(ctx)
    }

    ok := g.DeleteWithVersion(ctx, req.Key, req.Version)
    return &cachepb.DeleteResponse{Ok: ok}, nil
}
```

为了支持上面方法，`Group` 里建议新增：

```go
func (g *Group) DeleteWithVersion(ctx context.Context, key string, version int64) bool
```

---

## 阶段 6：owner 写策略

目标：

```text
一致性哈希选出的 owner 才是 key 的写入入口。
```

涉及文件：

- `cache/group.go`
- `internal/client/picker.go`

### 6.1 当前问题

现在逻辑是：

```text
先写本地，再异步同步 peer
```

这会导致：

- 任意节点都能写同一个 key
- 写入顺序不稳定
- 旧值可能覆盖新值

### 6.2 目标流程

```text
Set(key)
  -> PickPeer(key)
  -> 如果 owner 是远程：转发给 owner
  -> 如果 owner 是自己：写本地
```

### 6.3 具体实现

在 `Group.SetWithVersion` 开头加入路由：

```go
if !isPeer(ctx) && g.peers != nil {
    if peer, ok, isSelf := g.peers.PickPeer(key); ok && !isSelf {
        return peer.SetWithMeta(ctx, g.name, key, value, ttl, version)
    }
}
```

完整顺序：

```go
func (g *Group) SetWithVersion(ctx context.Context, key string, value []byte, ttl time.Duration, version int64) error {
    if key == "" {
        return ErrKeyRequired
    }
    if value == nil {
        return ErrValueRequired
    }
    if version <= 0 {
        version = nextVersion()
    }

    // 非 peer 请求先路由到 owner
    if !isPeer(ctx) && g.peers != nil {
        if peer, ok, isSelf := g.peers.PickPeer(key); ok && !isSelf {
            return peer.SetWithMeta(ctx, g.name, key, value, ttl, version)
        }
    }

    // owner 或 peer 请求写本地
    old, ok := g.mainCache.GetEntry(key)
    if ok && version < old.Version {
        return nil
    }

    entry := NewCacheEntry(value, ttl, version)
    return g.populateEntry(key, entry, ttl)
}
```

### 6.4 Delete 也走 owner

```go
func (g *Group) DeleteWithVersion(ctx context.Context, key string, version int64) bool {
    if key == "" {
        return false
    }
    if version <= 0 {
        version = nextVersion()
    }

    if !isPeer(ctx) && g.peers != nil {
        if peer, ok, isSelf := g.peers.PickPeer(key); ok && !isSelf {
            return peer.Delete(ctx, g.name, key, version) == nil
        }
    }

    old, ok := g.mainCache.GetEntry(key)
    if ok && version < old.Version {
        return false
    }

    tombstone := NewTombstone(version, g.tombstoneTTL)
    return g.populateEntry(key, tombstone, g.tombstoneTTL) == nil
}
```

---

## 阶段 7：失败重试

目标：

```text
远程同步失败不是只打印日志，而是进入可控重试。
```

如果你采用 owner 写，普通 `Set` 不再需要“先本地后异步同步 owner”。但后面做 shadow 双写时，仍然需要重试。

### 7.1 扩展 `syncTask`

```go
type syncTask struct {
    op      string
    key     string
    value   []byte
    ttl     time.Duration
    version int64
    attempt int
}
```

op 可以是：

```go
const (
    syncSet    = "set"
    syncDelete = "delete"
)
```

### 7.2 启动 retry loop

`NewGroup` 中：

```go
go g.retryLoop()
```

### 7.3 入队

```go
func (g *Group) enqueueRetry(task syncTask) {
    select {
    case g.retryCh <- task:
    default:
        log.Printf("[cache] retry queue full, drop task: group=%s key=%s op=%s", g.name, task.key, task.op)
    }
}
```

### 7.4 重试执行

```go
func (g *Group) retryLoop() {
    for task := range g.retryCh {
        time.Sleep(backoff(task.attempt))

        var err error
        switch task.op {
        case syncSet:
            err = g.trySyncSet(task.key, task.value, task.ttl, task.version)
        case syncDelete:
            err = g.trySyncDelete(task.key, task.version)
        }

        if err == nil {
            continue
        }

        task.attempt++
        if task.attempt > 10 {
            log.Printf("[cache] retry exceeded: group=%s key=%s op=%s err=%v", g.name, task.key, task.op, err)
            continue
        }
        g.enqueueRetry(task)
    }
}
```

---

## 阶段 8：etcd 节点状态机

目标：

```text
服务发现不只发现 addr，还要知道节点是 warming、active 还是 draining。
```

涉及文件：

- `internal/registry/etcd.go`
- `internal/client/picker.go`

### 8.1 新增节点状态类型

可以新增：

```text
internal/registry/node.go
```

实现：

```go
package registry

type NodeStatus string

const (
    StatusWarming  NodeStatus = "warming"
    StatusActive   NodeStatus = "active"
    StatusDraining NodeStatus = "draining"
)

type NodeInfo struct {
    Addr   string     `json:"addr"`
    Status NodeStatus `json:"status"`
}
```

### 8.2 注册时写 JSON

`EtcdRegistry.Register` 中：

```go
info := NodeInfo{
    Addr:   r.addr,
    Status: StatusWarming,
}
data, err := json.Marshal(info)
if err != nil {
    return err
}

_, err = r.cli.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
```

### 8.3 增加状态更新方法

```go
func (r *EtcdRegistry) UpdateStatus(ctx context.Context, status NodeStatus) error {
    if r.leaseID == 0 {
        return fmt.Errorf("registry has no lease")
    }

    info := NodeInfo{
        Addr:   r.addr,
        Status: status,
    }
    data, err := json.Marshal(info)
    if err != nil {
        return err
    }

    key := ServiceKey(r.svcName, r.addr)
    _, err = r.cli.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
    return err
}
```

### 8.4 picker 解析 NodeInfo

`reload` 中：

```go
var info registry.NodeInfo
if err := json.Unmarshal(kv.Value, &info); err != nil {
    // 兼容旧格式：value 直接是 addr
    info = registry.NodeInfo{
        Addr:   string(kv.Value),
        Status: registry.StatusActive,
    }
}
```

---

## 阶段 9：读环、写环、影子环

目标：

```text
根据节点状态决定读写路由。
```

涉及文件：

- `internal/client/picker.go`

### 9.1 修改 Picker 字段

```go
type Picker struct {
    mu       sync.RWMutex
    selfAddr string

    readRing   *consistenthash.Map
    writeRing  *consistenthash.Map
    shadowRing *consistenthash.Map

    clients map[string]*Client
    nodes   map[string]registry.NodeInfo

    etcdCli clientv3.Client
    prefix  string
}
```

### 9.2 构建三个环

```go
func buildRings(nodes []registry.NodeInfo) (readRing, writeRing, shadowRing *consistenthash.Map) {
    readRing = consistenthash.NewMap(defaultReplicas, nil)
    writeRing = consistenthash.NewMap(defaultReplicas, nil)
    shadowRing = consistenthash.NewMap(defaultReplicas, nil)

    for _, n := range nodes {
        switch n.Status {
        case registry.StatusActive:
            readRing.Add(n.Addr)
            writeRing.Add(n.Addr)
            shadowRing.Add(n.Addr)
        case registry.StatusWarming:
            shadowRing.Add(n.Addr)
        case registry.StatusDraining:
            readRing.Add(n.Addr)
        }
    }
    return
}
```

### 9.3 PickPeer 区分用途

现在接口只有：

```go
PickPeer(key string)
```

建议先新增方法，不破坏旧接口：

```go
func (p *Picker) PickReadPeer(key string) (cache.PeerGetter, bool, bool)
func (p *Picker) PickWritePeer(key string) (cache.PeerGetter, bool, bool)
func (p *Picker) PickShadowPeer(key string) (cache.PeerGetter, bool, bool)
```

内部复用：

```go
func (p *Picker) pickFromRing(ring *consistenthash.Map, key string) (cache.PeerGetter, bool, bool) {
    if key == "" || ring == nil || ring.Len() == 0 {
        return nil, false, false
    }
    addr := ring.Get(key)
    if addr == p.selfAddr {
        return nil, true, true
    }
    client, ok := p.clients[addr]
    if !ok {
        return nil, false, false
    }
    return client, true, false
}
```

过渡期：

```go
func (p *Picker) PickPeer(key string) (cache.PeerGetter, bool, bool) {
    return p.PickReadPeer(key)
}
```

后面 `Set/Delete` 改用 `PickWritePeer`。

---

## 阶段 10：迁移接口 Scan 和 BatchSet

目标：

```text
节点扩缩容时可以把属于新 owner 的数据迁过去。
```

涉及文件：

- `api/proto/cache.proto`
- `cache/cache.go`
- `cache/group.go`
- `internal/server/server.go`
- `internal/client/client.go`

### 10.1 proto 增加迁移接口

```proto
service CacheService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Set(SetRequest) returns (SetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Scan(ScanRequest) returns (ScanResponse);
  rpc BatchSet(BatchSetRequest) returns (BatchSetResponse);
}

message Entry {
  string group = 1;
  string key = 2;
  bytes value = 3;
  int64 ttl_ms = 4;
  int64 version = 5;
  bool tombstone = 6;
}

message ScanRequest {
  string group = 1;
  string start_key = 2;
  int64 count = 3;
}

message ScanResponse {
  repeated Entry entries = 1;
}

message BatchSetRequest {
  repeated Entry entries = 1;
}

message BatchSetResponse {
  bool ok = 1;
}
```

### 10.2 store 层需要支持扫描

当前 `Store` 接口没有 scan。

可以新增可选接口：

```go
type Scanner interface {
    Scan(startKey string, count int) []Record
}

type Record struct {
    Key   string
    Value Value
    TTL   time.Duration
}
```

`LRU2Store` 实现：

```go
func (s *LRU2Store) Scan(startKey string, count int) []Record {
    // 第一个版本可以简单扫全量，再排序分页。
    // 先保证正确，后面再优化性能。
}
```

### 10.3 Group 暴露 Scan

```go
func (g *Group) Scan(startKey string, count int) []CacheEntryRecord {
    scanner, ok := g.mainCache.store.(store.Scanner)
    if !ok {
        return nil
    }
    records := scanner.Scan(startKey, count)
    // 类型断言 CacheEntry，转成迁移记录
}
```

### 10.4 BatchSet 处理版本

```go
func (g *Group) BatchSet(ctx context.Context, entries []CacheEntryRecord) error {
    for _, e := range entries {
        old, ok := g.mainCache.GetEntry(e.Key)
        if ok && e.Version < old.Version {
            continue
        }
        entry := CacheEntry{
            Value:     e.Value,
            Version:   e.Version,
            Tombstone: e.Tombstone,
            ExpireAt:  e.ExpireAt,
        }
        ttl := time.Until(e.ExpireAt)
        if e.ExpireAt.IsZero() {
            ttl = 0
        }
        if err := g.populateEntry(e.Key, entry, ttl); err != nil {
            return err
        }
    }
    return nil
}
```

---

## 阶段 11：测试清单和验收命令

### 11.1 store 测试

```bash
go test ./store
go test -race ./store
```

必须覆盖：

- 普通 Set/Get
- TTL Set/Get
- Delete
- Close
- L1 到 L2 晋升
- Delete 并发安全

### 11.2 cache 测试

```bash
go test ./cache
go test -race ./cache
```

新增测试：

```go
func TestGroupSetRejectsOlderVersion(t *testing.T) {}
func TestGroupDeleteWritesTombstone(t *testing.T) {}
func TestGroupGetTombstoneReturnsErrKey(t *testing.T) {}
func TestGroupTombstoneRejectsOldSet(t *testing.T) {}
```

### 11.3 proto/client/server 测试

```bash
go test ./internal/server ./internal/client
```

覆盖：

- SetRequest 携带 version
- DeleteRequest 携带 version
- FromPeer 不循环转发
- 旧版本远程写入被拒绝

### 11.4 集成测试

后面可以新增：

```text
internal/integration/
```

测试：

- 三节点启动
- 同 key 路由到同 owner
- Delete 后旧 Set 不复活
- 新节点 warming 时不接读
- draining 节点不接写

---

## 推荐实际执行顺序

### 第一步

只修 store：

- `store/lru2.go`
- `store/lru2_test.go`

验收：

```bash
go test ./store
go test -race ./store
```

### 第二步

新增：

- `cache/cache_entry.go`
- `cache/version.go`

改：

- `cache/cache.go`

验收：

```bash
go test ./cache ./store
```

### 第三步

改：

- `cache/group.go`

实现：

- `SetWithVersion`
- `DeleteWithVersion`
- tombstone 读取逻辑

验收：

```bash
go test ./cache
```

### 第四步

改 proto：

- `api/proto/cache.proto`
- regenerate pb
- `internal/client/client.go`
- `internal/server/server.go`
- `cache/peers.go`

验收：

```bash
go test ./...
```

### 第五步

做 owner 写：

- `Group.SetWithVersion`
- `Group.DeleteWithVersion`
- picker 写路由

验收：

```bash
go test ./cache ./internal/client
```

### 第六步

做节点状态机和三环：

- `internal/registry`
- `internal/client/picker.go`

验收：

```bash
go test ./internal/client ./internal/registry
```

### 第七步

做迁移：

- `Scan`
- `BatchSet`
- migration manager

验收：

```bash
go test ./...
```

---

## 最小可交付版本定义

如果你想先做一个“能讲清楚、能跑测试”的版本，建议只做到这里：

```text
1. LRU2 正确性修复
2. CacheEntry
3. Version
4. Tombstone
5. Delete 远程协议
6. owner 写
```

这一版就能说明：

- 本地缓存稳定
- 写入有版本
- 删除不会短时间复活
- 远程节点不会循环转发
- key 有明确 owner

后面的三环和迁移可以作为第二阶段。

---

## 常见坑

### 1. `CacheEntry` 不要叫 `entry`

`entry` 已经是 LRU 内部节点。建议叫：

```go
CacheEntry
```

### 2. `TombstoneTTL` 不是永久防复活

它只防止保护窗口内的旧数据复活。

如果旧数据可能在很久以后回来，需要持久化版本水位：

```text
key -> maxVersion
```

### 3. Delete 不应该直接物理删除

分布式场景下，Delete 应该是：

```text
写 tombstone + version + TTL
```

### 4. owner 写和异步同步不要混在一起

如果选择 owner 写，普通写入应该转发到 owner，而不是先写本地再同步 owner。

### 5. proto 改完必须重新生成 pb

否则 client/server 编译会失败。

### 6. `ttl == 0` 必须 return

`SetWithExpiration` 里如果 `ttl == 0` 不 return，会继续走随机 TTL 逻辑。

### 7. L2 命中不能查 L1 的 TTL

只有 L1 命中并准备晋升时，才查 L1 的 `remainTTL`。

---

## 总结

这份实现手册对应的核心落地路径是：

```text
store 正确性
  -> CacheEntry
  -> version
  -> tombstone
  -> proto Delete/ttl/version
  -> owner 写
  -> retry
  -> 节点状态机
  -> 三环
  -> Scan/BatchSet 迁移
```

每一步都应该有测试兜底，不要一次性把所有设计都塞进代码。

最推荐你现在先完成：

```text
LRU2 测试通过
CacheEntry 落地
Group Set/Delete 支持 version + tombstone
```

这三步完成后，再进入远程协议和 owner 写。
