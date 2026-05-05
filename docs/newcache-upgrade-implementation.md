# newCache 分层升级具体实现

本文档是 `newcache-upgrade-guide.md` 的配套实现文档，为每一层提供具体的代码实现。

---

## 第 0 层：修复本地缓存正确性

### 0.1 修复 `store/lru2.go`

#### 0.1.1 初始化 `done` channel

**文件:** `store/lru2.go` — `NewLRU2Store` 构造函数

当前问题：`done` 未初始化，`Close()` 中 `close(s.done)` 会 panic。

```go
func NewLRU2Store(count int, maxBytes1, maxBytes2 int64) *LRU2Store {
    if count <= 0 {
        count = 32
    }
    count = roundUpPowerOf2(count)

    s := &LRU2Store{
        locks:  make([]sync.Mutex, count),
        caches: make([][2]*cache, count),
        mask:   uint32(count - 1),
        done:   make(chan struct{}), // 修复：初始化 done channel
    }

    per1 := maxBytes1 / int64(count)
    per2 := maxBytes2 / int64(count)
    for i := 0; i < count; i++ {
        s.caches[i][0] = NewCache(per1)
        s.caches[i][1] = NewCache(per2)
    }

    s.StartCleaner(30 * time.Second)
    return s
}
```

#### 0.1.2 初始化内部 `cache.expires`

**文件:** `store/lru2.go` — `NewCache` 函数

当前问题：`expires` map 未初始化，写入时 panic。

```go
func NewCache(maxBytes int64) *cache {
    return &cache{
        maxBytes:  maxBytes,
        usedBytes: 0,
        ll:        list.New(),
        caches:    make(map[string]*list.Element),
        expires:   make(map[string]time.Time), // 修复：初始化 expires
        onEvicted: func(key string, value Value) {
            log.Printf("delete key: %s", key)
        },
    }
}
```

#### 0.1.3 修复 `Set()` 新 key 插入

**文件:** `store/lru2.go` — `LRU2Store.Set()` 方法

当前问题：现有实现对已存在的 key 会用旧值覆盖（先 Get 再 Set 旧值），且新 key 的插入逻辑虽存在但容易误读。

```go
func (s *LRU2Store) Set(key string, value Value) error {
    idx := s.getIndex(key)
    s.locks[idx].Lock()
    defer s.locks[idx].Unlock()

    l1 := s.caches[idx][0]
    l2 := s.caches[idx][1]

    // 如果 L1 有，直接更新 L1
    if _, ok := l1.Get(key); ok {
        return l1.Set(key, value)
    }
    // 如果 L2 有，直接更新 L2
    if _, ok := l2.Get(key); ok {
        return l2.Set(key, value)
    }
    // 都没有，插入 L1
    return l1.Set(key, value)
}
```

#### 0.1.4 修复 `SetWithExpiration()` 的 `ttl == 0` 问题

**文件:** `store/lru2.go` — `LRU2Store.SetWithExpiration()` 方法

当前问题：`ttl == 0` 时调用了 `Set()` 但没有 `return`，后续 TTL 逻辑继续执行。

```go
func (s *LRU2Store) SetWithExpiration(key string, value Value, ttl time.Duration) error {
    if ttl == 0 {
        return s.Set(key, value) // 修复：ttl 为 0 时直接返回
    }

    idx := s.getIndex(key)
    s.locks[idx].Lock()
    defer s.locks[idx].Unlock()

    l1 := s.caches[idx][0]
    l2 := s.caches[idx][1]

    if _, ok := l1.Get(key); ok {
        return l1.SetWithExpiration(key, value, ttl)
    }
    if _, ok := l2.Get(key); ok {
        return l2.SetWithExpiration(key, value, ttl)
    }
    return l1.SetWithExpiration(key, value, ttl)
}
```

同样修复 `store/lru.go` 中的 `LRUStore.SetWithExpiration()`：

```go
func (c *LRUStore) SetWithExpiration(key string, value Value, ttl time.Duration) error {
    if ttl == 0 {
        return c.Set(key, value) // 修复：直接返回
    }
    c.mu.Lock()
    defer c.mu.Unlock()
    // ... 后续 TTL 逻辑不变
}
```

#### 0.1.5 修复 `Get()` 返回值

**文件:** `store/lru2.go` — `LRU2Store.Get()` 方法

当前问题：命中 L1 后迁移到 L2，但 `remainTTL` 在 L1 上查询（key 已从 L1 删除），导致始终返回 0。最后可能返回 `nil, false`。

```go
func (s *LRU2Store) Get(key string) (Value, bool) {
    idx := s.getIndex(key)
    s.locks[idx].Lock()
    defer s.locks[idx].Unlock()

    l1 := s.caches[idx][0]
    l2 := s.caches[idx][1]

    // 先查 L1
    if v, ok := l1.Get(key); ok {
        // 在迁移到 L2 之前，先获取 L1 中的剩余 TTL
        remainingTTL := l1.remainTTL(key)

        // 迁移到 L2
        if remainingTTL > 0 {
            l2.SetWithExpiration(key, v, remainingTTL)
        } else {
            l2.Set(key, v)
        }
        l1.Delete(key)
        return v, true // 修复：命中后必须返回值
    }

    // 再查 L2
    if v, ok := l2.Get(key); ok {
        return v, true
    }

    return nil, false
}
```

#### 0.1.6 给 `Delete()` 加分片锁

**文件:** `store/lru2.go` — `LRU2Store.Delete()` 方法

当前问题：`Delete()` 没有获取分片锁，并发删除会导致数据竞争。

```go
func (s *LRU2Store) Delete(key string) bool {
    idx := s.getIndex(key)
    s.locks[idx].Lock()         // 修复：加分片锁
    defer s.locks[idx].Unlock() // 修复：函数结束时释放

    l1 := s.caches[idx][0]
    l2 := s.caches[idx][1]

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

#### 0.1.7 安全关闭 `Close()`

**文件:** `store/lru2.go` — `LRU2Store.Close()` 方法

当前问题：可能重复 close 导致 panic。

```go
func (s *LRU2Store) Close() error {
    // 使用 select 防止重复 close
    select {
    case <-s.done:
        return nil // 已经关闭
    default:
        close(s.done)
    }

    for i := range s.locks {
        s.locks[i].Lock()
        s.caches[i][0].clear()
        s.caches[i][1].clear()
        s.locks[i].Unlock()
    }
    return nil
}
```

#### 0.1.8 `store/lru2_test.go` 单元测试

```go
package store

import (
    "sync"
    "testing"
    "time"
)

type testValue struct{ data string }

func (v testValue) Len() int { return len(v.data) }

func TestLRU2SetGet(t *testing.T) {
    s := NewLRU2Store(4, 1024, 1024)
    defer s.Close()

    // 新 key 能写入
    if err := s.Set("k1", testValue{"v1"}); err != nil {
        t.Fatalf("Set failed: %v", err)
    }

    // 能读到
    v, ok := s.Get("k1")
    if !ok {
        t.Fatal("Get returned false for existing key")
    }
    if v.(testValue).data != "v1" {
        t.Fatalf("expected v1, got %v", v)
    }

    // 能更新
    if err := s.Set("k1", testValue{"v2"}); err != nil {
        t.Fatalf("Set update failed: %v", err)
    }
    v, ok = s.Get("k1")
    if !ok || v.(testValue).data != "v2" {
        t.Fatalf("expected v2 after update, got %v", v)
    }
}

func TestLRU2SetWithExpiration(t *testing.T) {
    s := NewLRU2Store(4, 1024, 1024)
    defer s.Close()

    // TTL 写入不 panic
    if err := s.SetWithExpiration("k1", testValue{"v1"}, 100*time.Millisecond); err != nil {
        t.Fatalf("SetWithExpiration failed: %v", err)
    }

    v, ok := s.Get("k1")
    if !ok || v.(testValue).data != "v1" {
        t.Fatal("expected to get value before expiry")
    }

    // TTL 到期后 miss
    time.Sleep(150 * time.Millisecond)
    _, ok = s.Get("k1")
    if ok {
        t.Fatal("expected miss after TTL expiry")
    }

    // ttl == 0 走普通 Set
    if err := s.SetWithExpiration("k2", testValue{"v2"}, 0); err != nil {
        t.Fatalf("SetWithExpiration with ttl=0 failed: %v", err)
    }
    v, ok = s.Get("k2")
    if !ok || v.(testValue).data != "v2" {
        t.Fatal("expected k2 to exist with ttl=0")
    }
}

func TestLRU2Delete(t *testing.T) {
    s := NewLRU2Store(4, 1024, 1024)
    defer s.Close()

    s.Set("k1", testValue{"v1"})
    if !s.Delete("k1") {
        t.Fatal("expected Delete to return true")
    }
    _, ok := s.Get("k1")
    if ok {
        t.Fatal("expected miss after Delete")
    }
    // 删除不存在的 key
    if s.Delete("nonexistent") {
        t.Fatal("expected Delete to return false for nonexistent key")
    }
}

func TestLRU2DeleteConcurrent(t *testing.T) {
    s := NewLRU2Store(4, 1024, 1024)
    defer s.Close()

    for i := 0; i < 100; i++ {
        s.Set("key", testValue{"value"})
    }

    var wg sync.WaitGroup
    for i := 0; i < 50; i++ {
        wg.Add(2)
        go func() {
            defer wg.Done()
            s.Delete("key")
        }()
        go func() {
            defer wg.Done()
            s.Set("key", testValue{"value"})
        }()
    }
    wg.Wait()
}

func TestLRU2Close(t *testing.T) {
    s := NewLRU2Store(4, 1024, 1024)
    s.Set("k1", testValue{"v1"})

    // 正常关闭不 panic
    if err := s.Close(); err != nil {
        t.Fatalf("Close failed: %v", err)
    }
    // 重复关闭不 panic
    if err := s.Close(); err != nil {
        t.Fatalf("second Close failed: %v", err)
    }
}

func TestLRU2Promotion(t *testing.T) {
    s := NewLRU2Store(4, 1024, 1024)
    defer s.Close()

    s.Set("k1", testValue{"v1"})
    // 第一次 Get 触发 L1 -> L2 晋升
    v, ok := s.Get("k1")
    if !ok || v.(testValue).data != "v1" {
        t.Fatal("first Get should return value")
    }
    // 第二次 Get 从 L2 返回
    v, ok = s.Get("k1")
    if !ok || v.(testValue).data != "v1" {
        t.Fatal("second Get should return value from L2")
    }
}
```

---

## 第 1 层：统一缓存数据模型

### 1.1 引入 `Entry` 结构

**新增文件:** `cache/entry.go`

```go
package cache

import "time"

// Entry 是缓存中存储的完整数据单元，包含版本、过期时间和删除标记。
type Entry struct {
    Value     ByteView
    Version   int64
    ExpireAt  time.Time
    Tombstone bool
}

// IsExpired 判断 entry 是否过期。
func (e *Entry) IsExpired() bool {
    if e.ExpireAt.IsZero() {
        return false
    }
    return time.Now().After(e.ExpireAt)
}

// NewEntry 创建一个普通 Entry（非 tombstone）。
func NewEntry(value ByteView, version int64, ttl time.Duration) *Entry {
    if version <= 0 {
        version = time.Now().UnixNano()
    }
    e := &Entry{
        Value:   value,
        Version: version,
    }
    if ttl > 0 {
        e.ExpireAt = time.Now().Add(ttl)
    }
    return e
}

// NewTombstone 创建一个删除标记 Entry。
func NewTombstone(version int64, tombstoneTTL time.Duration) *Entry {
    if version <= 0 {
        version = time.Now().UnixNano()
    }
    return &Entry{
        Version:   version,
        Tombstone: true,
        ExpireAt:  time.Now().Add(tombstoneTTL),
    }
}

// Len 实现 store.Value 接口。
func (e *Entry) Len() int {
    return e.Value.Len()
}
```

### 1.2 改造 `Cache` 支持版本控制

**文件:** `cache/cache.go` — 在现有 `Cache` 上增加版本感知写入

```go
// SetWithEntry 写入 Entry，比较版本拒绝旧数据覆盖新数据。
func (c *Cache) SetWithEntry(key string, entry *Entry) error {
    if c.closed.Load() {
        return nil
    }
    c.mu.Lock()
    defer c.mu.Unlock()

    // 版本比较：如果已有更新版本，拒绝写入
    if existing, ok := c.store.Get(key); ok {
        if oldEntry, ok := existing.(*Entry); ok {
            if entry.Version < oldEntry.Version {
                return nil // 拒绝旧版本覆盖新版本
            }
            // tombstone 只能被更高版本的 tombstone 或非 tombstone 覆盖
            if oldEntry.Tombstone && entry.Tombstone && entry.Version <= oldEntry.Version {
                return nil
            }
        }
    }

    ttl := time.Duration(0)
    if !entry.ExpireAt.IsZero() {
        ttl = time.Until(entry.ExpireAt)
    }
    if ttl > 0 {
        return c.store.SetWithExpiration(key, entry, ttl)
    }
    return c.store.Set(key, entry)
}

// GetEntry 获取 Entry，识别 tombstone。
func (c *Cache) GetEntry(key string) (*Entry, bool) {
    if c.closed.Load() {
        return nil, false
    }
    c.mu.RLock()
    defer c.mu.RUnlock()

    v, ok := c.store.Get(key)
    if !ok {
        c misses.Add(1)
        return nil, false
    }
    entry, ok := v.(*Entry)
    if !ok {
        return nil, false
    }
    // tombstone 视为不存在
    if entry.Tombstone {
        c.misses.Add(1)
        return nil, false
    }
    c.hits.Add(1)
    return entry, true
}
```

### 1.3 改造 `store/lru2.go` 的 `Set` 以接受 `*Entry`

由于 `Entry` 实现了 `store.Value` 接口（`Len() int`），现有的 `Set` 无需修改即可存储 `*Entry`。版本比较在上层 `Cache.SetWithEntry` 中完成。

### 1.4 删除改为写 tombstone

**文件:** `cache/group.go` — 改造 `Delete` 方法

```go
const defaultTombstoneTTL = 60 * time.Second

func (g *Group) Delete(ctx context.Context, key string) bool {
    version := time.Now().UnixNano()

    // 本地写 tombstone
    tomb := NewTombstone(version, defaultTombstoneTTL)
    g.mainCache.SetWithEntry(key, tomb)

    // 同步到 peer（owner 写策略下，由 owner 处理）
    if !isPeer(ctx) {
        go g.syncDeleteToPeer(key, version)
    }

    return true
}

func (g *Group) syncDeleteToPeer(key string, version int64) {
    if g.peers == nil {
        return
    }
    peer, ok, isSelf := g.peers.PickPeer(key)
    if !ok || isSelf {
        return
    }
    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()

    if err := peer.Delete(ctx, g.name, key, version); err != nil {
        log.Printf("[newCache] sync delete to peer failed: %v", err)
        g.enqueueRetry(syncTask{
            key:     key,
            version: version,
            attempt: 1,
            isDelete: true,
        })
    }
}
```

---

## 第 2 层：补齐远程 API 语义

### 2.1 升级 Proto 定义

**文件:** `api/proto/cache.proto`

```protobuf
syntax = "proto3";
package cachepb;
option go_package = "newCache/api/proto;cachepb";

service CacheService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Set(SetRequest) returns (SetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Scan(ScanRequest) returns (ScanResponse);
  rpc BatchSet(BatchSetRequest) returns (BatchSetResponse);
}

message GetRequest {
  string group = 1;
  string key = 2;
}

message GetResponse {
  bytes value = 1;
  bool ok = 2;
  int64 version = 3;
  bool tombstone = 4;
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

message ScanRequest {
  string group = 1;
  string start_key = 2;
  int64 count = 3;
}

message ScanResponse {
  repeated EntryMessage entries = 1;
}

message EntryMessage {
  string key = 1;
  bytes value = 2;
  int64 ttl_ms = 3;
  int64 version = 4;
  bool tombstone = 5;
}

message BatchSetRequest {
  string group = 1;
  repeated EntryMessage entries = 2;
  bool from_peer = 3;
}

message BatchSetResponse {
  int64 success_count = 1;
}
```

生成代码：

```bash
protoc --go_out=. --go-grpc_out=. api/proto/cache.proto
```

### 2.2 更新 `cache/peers.go` 接口

```go
type PeerGetter interface {
    Get(ctx context.Context, group string, key string) ([]byte, error)
    Set(ctx context.Context, group string, key string, value []byte) error
    Delete(ctx context.Context, group string, key string, version int64) error
    Scan(ctx context.Context, group string, startKey string, count int64) ([]*EntryMessage, error)
    BatchSet(ctx context.Context, group string, entries []*EntryMessage) (int64, error)
}
```

### 2.3 更新 `internal/client/client.go`

```go
func (c *Client) Delete(ctx context.Context, group string, key string, version int64) error {
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()

    _, err := c.cli.Delete(ctx, &cachepb.DeleteRequest{
        Group:    group,
        Key:      key,
        FromPeer: true,
        Version:  version,
    })
    return err
}

func (c *Client) Scan(ctx context.Context, group string, startKey string, count int64) ([]*cachepb.EntryMessage, error) {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()

    resp, err := c.cli.Scan(ctx, &cachepb.ScanRequest{
        Group:     group,
        StartKey:  startKey,
        Count:     count,
    })
    if err != nil {
        return nil, err
    }
    return resp.Entries, nil
}

func (c *Client) BatchSet(ctx context.Context, group string, entries []*cachepb.EntryMessage) (int64, error) {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()

    resp, err := c.cli.BatchSet(ctx, &cachepb.BatchSetRequest{
        Group:    group,
        Entries:  entries,
        FromPeer: true,
    })
    if err != nil {
        return 0, err
    }
    return resp.SuccessCount, nil
}
```

### 2.4 更新 `internal/server/server.go`

```go
func (s *Server) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
    g := cache.GetGroup(req.Group)
    if g == nil {
        return nil, fmt.Errorf("group %q not found", req.Group)
    }

    if req.FromPeer {
        ctx = cache.WithPeer(ctx)
    }

    g.DeleteWithVersion(ctx, req.Key, req.Version)
    return &cachepb.DeleteResponse{Ok: true}, nil
}

func (s *Server) Scan(ctx context.Context, req *cachepb.ScanRequest) (*cachepb.ScanResponse, error) {
    g := cache.GetGroup(req.Group)
    if g == nil {
        return nil, fmt.Errorf("group %q not found", req.Group)
    }

    entries := g.Scan(req.StartKey, req.Count)
    return &cachepb.ScanResponse{Entries: entries}, nil
}

func (s *Server) BatchSet(ctx context.Context, req *cachepb.BatchSetRequest) (*cachepb.BatchSetResponse, error) {
    g := cache.GetGroup(req.Group)
    if g == nil {
        return nil, fmt.Errorf("group %q not found", req.Group)
    }

    if req.FromPeer {
        ctx = cache.WithPeer(ctx)
    }

    count := g.BatchSet(ctx, req.Entries)
    return &cachepb.BatchSetResponse{SuccessCount: count}, nil
}
```

### 2.5 Group 增加 `DeleteWithVersion`、`Scan`、`BatchSet`

**文件:** `cache/group.go`

```go
// DeleteWithVersion 带版本的删除，写 tombstone。
func (g *Group) DeleteWithVersion(ctx context.Context, key string, version int64) {
    tomb := NewTombstone(version, defaultTombstoneTTL)
    g.mainCache.SetWithEntry(key, tomb)

    if !isPeer(ctx) {
        go g.syncDeleteToPeer(key, version)
    }
}

// Scan 扫描本地缓存数据，用于迁移。
func (g *Group) Scan(startKey string, count int64) []*cachepb.EntryMessage {
    // 需要在 Cache 上实现 Scan 方法，见下方 2.6
    return g.mainCache.Scan(startKey, count)
}

// BatchSet 批量写入，用于迁移。
func (g *Group) BatchSet(ctx context.Context, entries []*cachepb.EntryMessage) int64 {
    var success int64
    for _, e := range entries {
        bv := NewByteView(e.Value)
        entry := &Entry{
            Value:     bv,
            Version:   e.Version,
            Tombstone: e.Tombstone,
        }
        if e.TtlMs > 0 {
            entry.ExpireAt = time.Now().Add(time.Duration(e.TtlMs) * time.Millisecond)
        }
        if err := g.mainCache.SetWithEntry(e.Key, entry); err == nil {
            success++
        }
        // peer 同步（非 peer 请求时）
        if !isPeer(ctx) {
            go g.syncToPeer(e.Key, e.Value)
        }
    }
    return success
}
```

### 2.6 Cache 增加 `Scan` 方法

**文件:** `cache/cache.go`

```go
// Scan 扫描缓存中的 entries，用于数据迁移。
// startKey 为空时从头开始，count 为最大返回数量。
// 需要底层 store 支持迭代，这里假设 LRU2Store 提供了 ScanKeys 方法。
func (c *Cache) Scan(startKey string, count int64) []*cachepb.EntryMessage {
    if c.closed.Load() {
        return nil
    }
    c.mu.RLock()
    defer c.mu.RUnlock()

    results := make([]*cachepb.EntryMessage, 0, count)
    // 遍历 store 中的所有 key
    c.store.ScanKeys(func(key string, value Value) bool {
        if key <= startKey {
            return true // 继续，跳过 startKey 之前的
        }
        entry, ok := value.(*Entry)
        if !ok {
            return true
        }
        if entry.IsExpired() {
            return true
        }
        var ttlMs int64
        if !entry.ExpireAt.IsZero() {
            ttlMs = time.Until(entry.ExpireAt).Milliseconds()
        }
        results = append(results, &cachepb.EntryMessage{
            Key:      key,
            Value:    entry.Value.ByteSlice(),
            TtlMs:    ttlMs,
            Version:  entry.Version,
            Tombstone: entry.Tombstone,
        })
        return int64(len(results)) < count
    })
    return results
}
```

### 2.7 Store 接口扩展 `ScanKeys`

**文件:** `store/store.go`

```go
type Store interface {
    Get(key string) (Value, bool)
    Set(key string, value Value) error
    SetWithExpiration(key string, value Value, ttl time.Duration) error
    Delete(key string) bool
    Len() int
    Close() error
    ScanKeys(fn func(key string, value Value) bool) // 新增：遍历所有 key
}
```

**文件:** `store/lru2.go` — 实现 `ScanKeys`

```go
func (s *LRU2Store) ScanKeys(fn func(key string, value Value) bool) {
    for i := range s.caches {
        s.locks[i].Lock()
        // 扫描 L1
        for k, ele := range s.caches[i][0].caches {
            kv := ele.Value.(*entry)
            if !fn(kv.key, kv.value) {
                s.locks[i].Unlock()
                return
            }
        }
        // 扫描 L2
        for k, ele := range s.caches[i][1].caches {
            kv := ele.Value.(*entry)
            if !fn(kv.key, kv.value) {
                s.locks[i].Unlock()
                return
            }
        }
        s.locks[i].Unlock()
    }
}
```

---

## 第 3 层：明确写入策略（Owner 写）

### 3.1 改造 `Group.Set` 实现 owner 写

**文件:** `cache/group.go`

```go
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
    version := time.Now().UnixNano()

    // 如果是 peer 转发来的请求，直接本地写入
    if isPeer(ctx) {
        return g.setLocally(key, value, version, 0)
    }

    // 否则走 owner 写：PickPeer 判断 owner
    if g.peers != nil {
        peer, ok, isSelf := g.peers.PickPeer(key)
        if ok && !isSelf {
            // owner 是远程节点，转发
            return peer.Set(ctx, g.name, key, value)
        }
    }

    // owner 是自己，本地写入
    return g.setLocally(key, value, version, 0)
}

func (g *Group) setLocally(key string, value []byte, version int64, ttl time.Duration) error {
    bv := NewByteView(value)
    entry := NewEntry(bv, version, ttl)
    if err := g.mainCache.SetWithEntry(key, entry); err != nil {
        return err
    }
    return nil
}
```

### 3.2 改造 `Group.Get` 的 peer 转发

**文件:** `cache/group.go`

```go
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
    if key == "" {
        return ByteView{}, fmt.Errorf("key is required")
    }

    // bloom filter 快速过滤
    if g.bloomFilter != nil && !g.bloomFilter.MayExist(key) {
        return ByteView{}, fmt.Errorf("key %q not found (bloom filter)", key)
    }

    // 先查本地缓存
    if entry, ok := g.mainCache.GetEntry(key); ok {
        return entry.Value, nil
    }

    // 加载数据（singleflight 保护）
    view, err := g.load(ctx, key)
    if err != nil {
        return ByteView{}, err
    }
    return view, nil
}

func (g *Group) loadData(ctx context.Context, key string) (ByteView, error) {
    // peer 请求直接查本地
    if isPeer(ctx) {
        return g.getLocally(ctx, key)
    }

    // 非 peer 请求：先查 peer（owner），再 fallback 到本地
    if g.peers != nil {
        peer, ok, isSelf := g.peers.PickPeer(key)
        if ok && !isSelf {
            // 从远程 owner 获取
            data, err := peer.Get(ctx, g.name, key)
            if err == nil {
                return NewByteView(data), nil
            }
            log.Printf("[newCache] peer get failed, fallback to local: %v", err)
        }
    }

    return g.getLocally(ctx, key)
}
```

---

## 第 4 层：完善失败重试机制

### 4.1 扩展 `syncTask`

**文件:** `cache/group.go`

```go
type syncTask struct {
    key      string
    value    []byte
    version  int64
    attempt  int
    isDelete bool
}

const (
    maxRetryAttempts = 5
    maxRetryQueueSize = 1000
)
```

### 4.2 启动 retry loop

**文件:** `cache/group.go` — `NewGroup` 中

```go
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
    if getter == nil {
        panic("nil Getter")
    }
    if name == "" {
        panic("empty group name")
    }

    g := &Group{
        name:      name,
        getter:    getter,
        mainCache: NewCache(cacheBytes),
        loader:    singleflight.NewGroup(),
        loadSem:   make(chan struct{}, 100),
        retryCh:   make(chan syncTask, maxRetryQueueSize),
        stopCh:    make(chan struct{}),
    }

    // 启动重试循环
    go g.retryLoop()

    mu.Lock()
    groups[name] = g
    mu.Unlock()
    return g
}
```

### 4.3 `syncToPeer` 失败入队

```go
func (g *Group) syncToPeer(key string, value []byte) {
    if g.peers == nil {
        return
    }
    peer, ok, isSelf := g.peers.PickPeer(key)
    if !ok || isSelf {
        return
    }

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()

    if err := peer.Set(ctx, g.name, key, value); err != nil {
        log.Printf("[newCache] syncToPeer failed for key=%s: %v", key, err)
        g.enqueueRetry(syncTask{
            key:     key,
            value:   value,
            version: time.Now().UnixNano(),
            attempt: 1,
        })
    }
}
```

### 4.4 完整 retry loop

```go
func (g *Group) enqueueRetry(task syncTask) {
    select {
    case g.retryCh <- task:
    default:
        log.Printf("[newCache] retry queue full, dropping task for key=%s", task.key)
    }
}

func (g *Group) retryLoop() {
    for {
        select {
        case <-g.stopCh:
            return
        case task := <-g.retryCh:
            g.processRetry(task)
        }
    }
}

func (g *Group) processRetry(task syncTask) {
    if task.attempt > maxRetryAttempts {
        log.Printf("[newCache] max retry reached for key=%s, giving up", task.key)
        return
    }

    // 指数退避
    backoff := g.backoff(task.attempt)
    select {
    case <-g.stopCh:
        return
    case <-time.After(backoff):
    }

    var err error
    if task.isDelete {
        err = g.trySyncDeleteToPeer(task.key, task.version)
    } else {
        err = g.trySyncToPeer(task.key, task.value)
    }

    if err != nil {
        log.Printf("[newCache] retry attempt %d failed for key=%s: %v", task.attempt+1, task.key, err)
        task.attempt++
        g.enqueueRetry(task)
    }
}

func (g *Group) trySyncToPeer(key string, value []byte) error {
    if g.peers == nil {
        return nil
    }
    peer, ok, isSelf := g.peers.PickPeer(key)
    if !ok || isSelf {
        return nil
    }

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    return peer.Set(ctx, g.name, key, value)
}

func (g *Group) trySyncDeleteToPeer(key string, version int64) error {
    if g.peers == nil {
        return nil
    }
    peer, ok, isSelf := g.peers.PickPeer(key)
    if !ok || isSelf {
        return nil
    }

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    return peer.Delete(ctx, g.name, key, version)
}

func (g *Group) backoff(attempt int) time.Duration {
    d := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
    if d > 5*time.Second {
        d = 5 * time.Second
    }
    return d
}

// Close 停止 Group 的后台 goroutine。
func (g *Group) Close() {
    close(g.stopCh)
}
```

### 4.5 Group 结构增加 `stopCh`

```go
type Group struct {
    name        string
    getter      Getter
    mainCache   *Cache
    peers       PeerPicker
    loader      *singleflight.Group
    loadSem     chan struct{}
    bloomFilter *BloomFilter
    retryCh     chan syncTask
    stopCh      chan struct{} // 新增
}
```

---

## 第 5 层：服务发现升级为节点状态机

### 5.1 定义 `NodeInfo`

**新增文件:** `internal/registry/nodeinfo.go`

```go
package registry

import "encoding/json"

const (
    StatusWarming  = "warming"
    StatusActive   = "active"
    StatusDraining = "draining"
)

type NodeInfo struct {
    Addr   string `json:"addr"`
    Status string `json:"status"`
}

func (n NodeInfo) Marshal() ([]byte, error) {
    return json.Marshal(n)
}

func UnmarshalNodeInfo(data []byte) (*NodeInfo, error) {
    var info NodeInfo
    if err := json.Unmarshal(data, &info); err != nil {
        return nil, err
    }
    return &info, nil
}
```

### 5.2 改造 `EtcdRegistry`

**文件:** `internal/registry/etcd.go`

```go
type EtcdRegistry struct {
    cli      *clientv3.Client
    svcName  string
    addr     string
    leaseID  clientv3.LeaseID
    ttl      int64
    status   string // 新增：当前状态
}

func (r *EtcdRegistry) Register(ctx context.Context) error {
    r.status = StatusWarming // 新注册时为 warming
    return r.registerWithStatus(ctx, StatusWarming)
}

func (r *EtcdRegistry) registerWithStatus(ctx context.Context, status string) error {
    resp, err := r.cli.Grant(ctx, r.ttl)
    if err != nil {
        return err
    }
    r.leaseID = resp.ID

    info := NodeInfo{
        Addr:   r.addr,
        Status: status,
    }
    data, err := info.Marshal()
    if err != nil {
        return err
    }

    key := ServiceKey(r.svcName, r.addr)
    _, err = r.cli.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
    if err != nil {
        return err
    }

    ch, err := r.cli.KeepAlive(context.Background(), resp.ID)
    if err != nil {
        return err
    }
    go func() {
        for range ch {
        }
    }()

    r.status = status
    return nil
}

// UpdateStatus 更新节点在 etcd 中的状态。
func (r *EtcdRegistry) UpdateStatus(ctx context.Context, newStatus string) error {
    info := NodeInfo{
        Addr:   r.addr,
        Status: newStatus,
    }
    data, err := info.Marshal()
    if err != nil {
        return err
    }

    key := ServiceKey(r.svcName, r.addr)
    _, err = r.cli.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
    if err != nil {
        return err
    }

    r.status = newStatus
    log.Printf("[registry] node %s status updated to %s", r.addr, newStatus)
    return nil
}

// GetStatus 获取当前状态。
func (r *EtcdRegistry) GetStatus() string {
    return r.status
}
```

### 5.3 改造 `Picker.watch` 解析 `NodeInfo`

**文件:** `internal/client/picker.go` — 修改 `reload` 方法

```go
func (p *Picker) reload(ctx context.Context) error {
    resp, err := p.etcdCli.Get(ctx, p.prefix, clientv3.WithPrefix())
    if err != nil {
        return err
    }

    var nodes []string
    nodeStatus := make(map[string]string) // addr -> status

    for _, kv := range resp.Kvs {
        info, err := registry.UnmarshalNodeInfo(kv.Value)
        if err != nil {
            // 兼容旧格式：直接用 value 作为 addr
            addr := string(kv.Value)
            nodes = append(nodes, addr)
            nodeStatus[addr] = registry.StatusActive
            continue
        }
        nodes = append(nodes, info.Addr)
        nodeStatus[info.Addr] = info.Status
    }

    p.SetPeers(nodes, nodeStatus)
    return nil
}
```

### 5.4 改造 `Picker.SetPeers` 接受状态信息

```go
func (p *Picker) SetPeers(nodes []string, nodeStatus map[string]string) {
    p.mu.Lock()
    defer p.mu.Unlock()

    // 构建三环
    readRing := consistenthash.NewMap(defaultReplicas, nil)
    writeRing := consistenthash.NewMap(defaultReplicas, nil)
    shadowRing := consistenthash.NewMap(defaultReplicas, nil)

    activeNodes := make(map[string]bool)

    for _, addr := range nodes {
        status := nodeStatus[addr]
        switch status {
        case registry.StatusActive:
            readRing.Add(addr)
            writeRing.Add(addr)
            shadowRing.Add(addr)
            activeNodes[addr] = true
        case registry.StatusWarming:
            shadowRing.Add(addr)
        case registry.StatusDraining:
            readRing.Add(addr)
        default:
            // 未知状态按 active 处理
            readRing.Add(addr)
            writeRing.Add(addr)
            shadowRing.Add(addr)
            activeNodes[addr] = true
        }
    }

    // 更新连接
    newClients := make(map[string]*Client)
    for _, addr := range nodes {
        if addr == p.selfAddr {
            continue
        }
        if existing, ok := p.clients[addr]; ok {
            newClients[addr] = existing
        } else {
            cli, err := p.newClient(addr)
            if err != nil {
                log.Printf("[picker] failed to connect to %s: %v", addr, err)
                continue
            }
            newClients[addr] = cli
        }
    }

    // 关闭不再需要的连接
    for addr, cli := range p.clients {
        if _, ok := newClients[addr]; !ok {
            p.closeClient(cli)
        }
    }

    p.nodes = nodes
    p.clients = newClients
    p.readRing = readRing
    p.writeRing = writeRing
    p.shadowRing = shadowRing
}
```

---

## 第 6 层：拆分读环、写环、影子环

### 6.1 改造 `Picker` 结构

**文件:** `internal/client/picker.go`

```go
type Picker struct {
    mu          sync.RWMutex
    selfAddr    string
    nodes       []string
    readRing    *consistenthash.Map  // active + draining
    writeRing   *consistenthash.Map  // active
    shadowRing  *consistenthash.Map  // active + warming
    clients     map[string]*Client
    etcdCli     *clientv3.Client
    prefix      string
    newClient   func(string) (*Client, error)
    closeClient func(*Client) error
}
```

### 6.2 `PickPeer` 按场景选择环

```go
// PickPeerForRead 从读环选择节点（Get 场景）。
func (p *Picker) PickPeerForRead(key string) (PeerGetter, bool, bool) {
    p.mu.RLock()
    defer p.mu.RUnlock()

    if p.readRing == nil {
        return nil, false, false
    }

    addr := p.readRing.Get(key)
    if addr == "" {
        return nil, false, false
    }
    if addr == p.selfAddr {
        return nil, true, true
    }

    cli, ok := p.clients[addr]
    return cli, ok, false
}

// PickPeerForWrite 从写环选择节点（Set/Delete 场景）。
func (p *Picker) PickPeerForWrite(key string) (PeerGetter, bool, bool) {
    p.mu.RLock()
    defer p.mu.RUnlock()

    if p.writeRing == nil {
        return nil, false, false
    }

    addr := p.writeRing.Get(key)
    if addr == "" {
        return nil, false, false
    }
    if addr == p.selfAddr {
        return nil, true, true
    }

    cli, ok := p.clients[addr]
    return cli, ok, false
}

// PickShadowPeer 从影子环选择节点（迁移期间双写场景）。
func (p *Picker) PickShadowPeer(key string) (PeerGetter, bool, string) {
    p.mu.RLock()
    defer p.mu.RUnlock()

    if p.shadowRing == nil {
        return nil, false, ""
    }

    addr := p.shadowRing.Get(key)
    if addr == "" || addr == p.selfAddr {
        return nil, false, addr
    }

    cli, ok := p.clients[addr]
    return cli, ok, addr
}

// PickPeer 兼容旧接口，默认走读环。
func (p *Picker) PickPeer(key string) (PeerGetter, bool, bool) {
    return p.PickPeerForRead(key)
}
```

### 6.3 更新 `PeerPicker` 接口

**文件:** `cache/peers.go`

```go
type PeerPicker interface {
    PickPeer(key string) (peer PeerGetter, ok bool, isSelf bool)
    PickPeerForRead(key string) (peer PeerGetter, ok bool, isSelf bool)
    PickPeerForWrite(key string) (peer PeerGetter, ok bool, isSelf bool)
    PickShadowPeer(key string) (peer PeerGetter, ok bool, shadowAddr string)
}
```

### 6.4 Group 中使用写环进行 owner 写

```go
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
    version := time.Now().UnixNano()

    if isPeer(ctx) {
        return g.setLocally(key, value, version, 0)
    }

    if g.peers != nil {
        // 写环 PickPeer
        peer, ok, isSelf := g.peers.PickPeerForWrite(key)
        if ok && !isSelf {
            if err := peer.Set(ctx, g.name, key, value); err != nil {
                return err
            }
        }
    }

    // 本地写入
    err := g.setLocally(key, value, version, 0)

    // 影子双写（迁移期间）
    if g.peers != nil {
        shadowPeer, ok, shadowAddr := g.peers.PickShadowPeer(key)
        if ok {
            primaryAddr := ""
            if p, ok2, _ := g.peers.PickPeerForWrite(key); ok2 {
                // 获取主节点地址用于对比
                _ = p
            }
            if shadowAddr != primaryAddr {
                go func() {
                    ctx2, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
                    defer cancel()
                    shadowPeer.Set(ctx2, g.name, key, value)
                }()
            }
        }
    }

    return err
}
```

---

## 第 7 层：添加数据迁移能力

### 7.1 `MigrationManager` 结构

**新增文件:** `internal/migration/migration.go`

```go
package migration

import (
    "context"
    "log"
    "sync"
    "time"

    "newCache/internal/registry"
)

type PeerScanner interface {
    Scan(ctx context.Context, group string, startKey string, count int64) ([]*EntryMessage, error)
    BatchSet(ctx context.Context, group string, entries []*EntryMessage) (int64, error)
}

type EntryMessage struct {
    Key       string
    Value     []byte
    TtlMs     int64
    Version   int64
    Tombstone bool
}

type MigrationManager struct {
    mu       sync.Mutex
    registry *registry.EtcdRegistry
    selfAddr string
    batchSize int64
    done     chan struct{}
}

func NewMigrationManager(reg *registry.EtcdRegistry, selfAddr string) *MigrationManager {
    return &MigrationManager{
        registry:  reg,
        selfAddr:  selfAddr,
        batchSize: 100,
        done:      make(chan struct{}),
    }
}
```

### 7.2 扩容 Pull 迁移

```go
// MigrateIn 扩容时，新节点从旧节点拉取属于自己的数据。
// oldNodes 是扩容前的节点列表，newRing 是包含新节点的 future ring。
func (m *MigrationManager) MigrateIn(ctx context.Context, group string, clients map[string]PeerScanner, newRing *consistenthash.Map, oldNodes []string) error {
    log.Printf("[migration] starting pull migration for node %s", m.selfAddr)

    // 遍历旧节点，Scan 出属于自己的 key
    var wg sync.WaitGroup
    errCh := make(chan error, len(oldNodes))

    for _, oldAddr := range oldNodes {
        if oldAddr == m.selfAddr {
            continue
        }
        client, ok := clients[oldAddr]
        if !ok {
            continue
        }

        wg.Add(1)
        go func(addr string, scanner PeerScanner) {
            defer wg.Done()
            m.pullFromNode(ctx, group, scanner, newRing, addr, errCh)
        }(oldAddr, client)
    }

    wg.Wait()
    close(errCh)

    for err := range errCh {
        if err != nil {
            return err
        }
    }

    log.Printf("[migration] pull migration completed for node %s", m.selfAddr)
    return nil
}

func (m *MigrationManager) pullFromNode(ctx context.Context, group string, scanner PeerScanner, newRing *consistenthash.Map, fromAddr string, errCh chan<- error) {
    startKey := ""
    for {
        select {
        case <-m.done:
            return
        default:
        }

        entries, err := scanner.Scan(ctx, group, startKey, m.batchSize)
        if err != nil {
            errCh <- err
            return
        }
        if len(entries) == 0 {
            return
        }

        // 筛选属于自己的 key
        var myEntries []*EntryMessage
        for _, e := range entries {
            owner := newRing.Get(e.Key)
            if owner == m.selfAddr {
                myEntries = append(myEntries, e)
            }
            startKey = e.Key
        }

        // 批量写入自己
        if len(myEntries) > 0 {
            _, err := scanner.BatchSet(ctx, group, myEntries)
            if err != nil {
                log.Printf("[migration] batch set failed: %v", err)
                // 逐个重试
                for _, e := range myEntries {
                    scanner.BatchSet(ctx, group, []*EntryMessage{e})
                }
            }
        }

        if len(entries) < int(m.batchSize) {
            return // 已经扫描完
        }
    }
}
```

### 7.3 缩容 Push 迁移

```go
// MigrateOut 缩容时，当前节点将数据推送到新 owner。
// scanFn 用于扫描本地数据，newRing 是不包含自己的新写环。
func (m *MigrationManager) MigrateOut(ctx context.Context, group string, clients map[string]PeerScanner, newRing *consistenthash.Map, scanFn func(startKey string, count int64) []*EntryMessage) error {
    log.Printf("[migration] starting push migration for node %s", m.selfAddr)

    startKey := ""
    for {
        select {
        case <-m.done:
            return nil
        default:
        }

        entries := scanFn(startKey, m.batchSize)
        if len(entries) == 0 {
            break
        }

        // 按 owner 分组
        ownerEntries := make(map[string][]*EntryMessage)
        for _, e := range entries {
            owner := newRing.Get(e.Key)
            if owner != "" && owner != m.selfAddr {
                ownerEntries[owner] = append(ownerEntries[owner], e)
            }
            startKey = e.Key
        }

        // 推送到各 owner
        var wg sync.WaitGroup
        for owner, batch := range ownerEntries {
            client, ok := clients[owner]
            if !ok {
                log.Printf("[migration] no client for owner %s, skipping %d entries", owner, len(batch))
                continue
            }
            wg.Add(1)
            go func(addr string, scanner PeerScanner, batch []*EntryMessage) {
                defer wg.Done()
                _, err := scanner.BatchSet(ctx, group, batch)
                if err != nil {
                    log.Printf("[migration] push to %s failed: %v", addr, err)
                }
            }(owner, client, batch)
        }
        wg.Wait()

        if len(entries) < int(m.batchSize) {
            break
        }
    }

    log.Printf("[migration] push migration completed for node %s", m.selfAddr)
    return nil
}

// Stop 停止迁移。
func (m *MigrationManager) Stop() {
    select {
    case <-m.done:
    default:
        close(m.done)
    }
}
```

### 7.4 扩容完整流程（集成到 server 启动）

**文件:** `cmd/cache-node/main.go` — 节点启动时的迁移流程

```go
func startWithMigration(reg *registry.EtcdRegistry, picker *client.Picker, group *cache.Group) {
    ctx := context.Background()

    // 1. 注册为 warming
    // （在 Register 中已自动设为 warming）

    // 2. 等待 picker 加载当前节点列表
    time.Sleep(1 * time.Second)

    // 3. 执行 pull 迁移
    mm := migration.NewMigrationManager(reg, reg.GetAddr())
    // ... 构建 future ring 和 client 映射
    // mm.MigrateIn(ctx, groupName, clients, futureRing, oldNodes)

    // 4. 切换为 active
    reg.UpdateStatus(ctx, registry.StatusActive)

    // 5. 开始正常服务
}

func gracefulShutdown(reg *registry.EtcdRegistry, picker *client.Picker, group *cache.Group, srv *server.Server) {
    ctx := context.Background()

    // 1. 切换为 draining
    reg.UpdateStatus(ctx, registry.StatusDraining)

    // 2. 执行 push 迁移
    mm := migration.NewMigrationManager(reg, reg.GetAddr())
    // newRing := 构建不包含自己的写环
    // mm.MigrateOut(ctx, groupName, clients, newRing, scanFn)

    // 3. 关闭服务
    srv.Stop(ctx)
    picker.Close()
    group.Close()

    // 4. 注销 etcd
    reg.Close(ctx)
}
```

### 7.5 Server 增加优雅退出信号处理

**文件:** `cmd/cache-node/main.go`

```go
func main() {
    // ... 解析参数、创建组件 ...

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <-sigCh
        log.Println("received shutdown signal, starting graceful shutdown...")
        gracefulShutdown(reg, picker, group, srv)
        os.Exit(0)
    }()

    // ... 正常启动 ...
}
```

---

## 第 8 层：补齐工程化能力

### 8.1 `etc/config.yaml`

```yaml
serviceName: new-cache
groupName: scores
cacheBytes: 2097152  # 2MB
etcdEndpoints:
  - http://127.0.0.1:2379
grpcTimeout: 500ms
serviceTTL: 10
replicas: 50
tombstoneTTL: 60s
retryMaxAttempts: 5
cleanerInterval: 30s
```

### 8.2 配置加载

**新增文件:** `internal/config/config.go`

```go
package config

import (
    "os"
    "time"

    "gopkg.in/yaml.v3"
)

type Config struct {
    ServiceName      string        `yaml:"serviceName"`
    GroupName        string        `yaml:"groupName"`
    CacheBytes       int64         `yaml:"cacheBytes"`
    EtcdEndpoints    []string      `yaml:"etcdEndpoints"`
    GRPCTimeout      time.Duration `yaml:"grpcTimeout"`
    ServiceTTL       int64         `yaml:"serviceTTL"`
    Replicas         int           `yaml:"replicas"`
    TombstoneTTL     time.Duration `yaml:"tombstoneTTL"`
    RetryMaxAttempts int           `yaml:"retryMaxAttempts"`
    CleanerInterval  time.Duration `yaml:"cleanerInterval"`
}

func Load(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }

    cfg := &Config{
        ServiceName:      "new-cache",
        GroupName:        "scores",
        CacheBytes:       2 * 1024 * 1024,
        EtcdEndpoints:    []string{"http://127.0.0.1:2379"},
        GRPCTimeout:      500 * time.Millisecond,
        ServiceTTL:       10,
        Replicas:         50,
        TombstoneTTL:     60 * time.Second,
        RetryMaxAttempts: 5,
        CleanerInterval:  30 * time.Second,
    }

    if err := yaml.Unmarshal(data, cfg); err != nil {
        return nil, err
    }
    return cfg, nil
}
```

### 8.3 Makefile

```makefile
.PHONY: test race node1 node2 node3 build proto clean

test:
	go test ./...

race:
	go test -race ./...

build:
	go build -o bin/cache-node ./cmd/cache-node

proto:
	protoc --go_out=. --go-grpc_out=. api/proto/cache.proto

node1:
	go run ./cmd/cache-node -port 8001 -node A

node2:
	go run ./cmd/cache-node -port 8002 -node B

node3:
	go run ./cmd/cache-node -port 8003 -node C

bench:
	go test -bench=. -benchmem ./store/...

clean:
	rm -rf bin/
```

### 8.4 三节点 Demo 脚本

**文件:** `examples/three-nodes/main.go`

```go
package main

import (
    "fmt"
    "log"
    "os"
    "os/exec"
    "sync"
    "time"
)

func main() {
    nodes := []struct {
        port string
        name string
    }{
        {"8001", "A"},
        {"8002", "B"},
        {"8003", "C"},
    }

    var wg sync.WaitGroup
    procs := make([]*exec.Cmd, len(nodes))

    for i, n := range nodes {
        wg.Add(1)
        go func(idx int, port, name string) {
            defer wg.Done()
            cmd := exec.Command("go", "run", "./cmd/cache-node",
                "-port", port, "-node", name)
            cmd.Stdout = os.Stdout
            cmd.Stderr = os.Stderr
            procs[idx] = cmd
            if err := cmd.Start(); err != nil {
                log.Printf("failed to start node %s: %v", name, err)
            }
        }(i, n.port, n.name)
    }

    // 等待所有节点启动
    time.Sleep(3 * time.Second)
    fmt.Println("All nodes started. Press Ctrl+C to stop.")

    // 等待任一进程退出
    for _, p := range procs {
        if p != nil {
            p.Wait()
        }
    }
}
```

### 8.5 `README.md` 结构

```markdown
# newCache

分布式缓存系统，基于 Go 实现，支持一致性哈希路由、TTL 过期、节点动态扩缩容。

## 架构

┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node A  │────│ Node B  │────│ Node C  │
└────┬────┘     └────┬────┘     └────┬────┘
     │               │               │
     └───────────────┼───────────────┘
                     │
              ┌──────┴──────┐
              │    etcd     │
              └─────────────┘

## 核心特性

- LRU2 两级缓存，支持 TTL 和自动晋升
- 一致性哈希路由，虚拟节点均衡分布
- Owner 写策略，版本控制防止旧数据覆盖
- Tombstone 删除标记，防止数据复活
- 节点状态机：warming → active → draining
- 读写影子三环分离，平滑扩缩容
- gRPC 通信 + etcd 服务发现
- Singleflight 防缓存击穿
- Bloom Filter 快速过滤

## 快速启动

### 前置依赖

- Go 1.22+
- etcd

### 启动三节点

# 终端 1
make node1

# 终端 2
make node2

# 终端 3
make node3

### 运行测试

make test
make race

## 配置

参见 `etc/config.yaml`

## 目录结构

├── api/proto/          # protobuf 定义
├── cache/              # 缓存核心逻辑
├── cmd/cache-node/     # 启动入口
├── examples/           # 示例
├── internal/
│   ├── client/         # gRPC 客户端和 picker
│   ├── consistenthash/ # 一致性哈希
│   ├── migration/      # 数据迁移
│   ├── registry/       # etcd 服务注册
│   ├── server/         # gRPC 服务端
│   └── singleflight/   # 请求合并
└── store/              # 本地存储引擎

## 后续规划

- 分布式集成测试
- Prometheus 指标暴露
- 管理 API（查看节点状态、手动触发迁移）
- 多 group 支持
```

---

## 附录：测试清单

### store 层（已有 `store/lru2_test.go`）

| 测试 | 验证点 |
|------|--------|
| `TestLRU2SetGet` | 新 key 能写入和读取 |
| `TestLRU2SetWithExpiration` | TTL 写入不 panic，到期后 miss |
| `TestLRU2Delete` | 删除后 miss，删除不存在的 key 返回 false |
| `TestLRU2DeleteConcurrent` | 并发删除不 race |
| `TestLRU2Close` | 关闭不 panic，重复关闭不 panic |
| `TestLRU2Promotion` | L1 命中后晋升到 L2 |

### cache 层

| 测试 | 验证点 |
|------|--------|
| `TestSetWithEntry_VersionControl` | 旧版本不覆盖新版本 |
| `TestSetWithEntry_Tombstone` | tombstone 读取返回不存在 |
| `TestGroup_OwnerWrite` | 写请求路由到 owner |
| `TestGroup_PeerNotLoopback` | peer 请求不循环转发 |

### picker 层

| 测试 | 验证点 |
|------|--------|
| `TestThreeRings_ActiveNode` | active 节点进入三环 |
| `TestThreeRings_WarmingNode` | warming 节点只进影子环 |
| `TestThreeRings_DrainingNode` | draining 节点只进读环 |
| `TestPickPeerForWrite` | 写环路由正确 |

### 分布式集成

| 测试 | 验证点 |
|------|--------|
| `TestThreeNodes_SameKeySameOwner` | 同 key 路由到同 owner |
| `TestWarmingNode_NoRead` | warming 节点不接读 |
| `TestDrainingNode_NoWrite` | draining 节点不接新写 |
| `TestMigration_Pull` | 扩容后新节点能读到迁移数据 |
| `TestMigration_Push` | 缩容后数据迁移到新 owner |
| `TestTombstone_NoResurrection` | 删除后迁移不会复活数据 |
