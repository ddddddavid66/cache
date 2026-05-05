# newCache 升级实现 Demo 代码

这份文档只做一件事：把 `docs/newCache升级实现补充说明-后三环与迁移详解.md` 里的思路，落成一套**可以照着改的 demo 代码**。

注意：

- 这里只给实现骨架和关键代码段
- 不修改你现有代码
- 代码按“目标实现”写，和当前源码里的个别命名不完全一致

---

## 1. 先看最终目标

最终希望你把系统拆成 5 层：

```text
协议层      -> proto
客户端层    -> internal/client
服务端层    -> internal/server
路由层      -> internal/client/picker.go
业务层      -> cache
```

最核心的能力是：

```text
Set/Delete 走写环
Get 走读环
迁移期间双写影子环
迁移时保 version / ttl / tombstone
```

---

## 2. 第 5 层：proto demo

建议把协议设计成下面这样。

### `api/proto/cache.proto`

```proto
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
  int64 ttl_ms = 4;
  int64 version = 5;
  bool from_peer = 6;
}

message SetResponse {
  bool ok = 1;
}

message DeleteRequest {
  string group = 1;
  string key = 2;
  int64 version = 3;
  bool from_peer = 4;
}

message DeleteResponse {
  bool ok = 1;
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

### 为什么这么设计

- `version` 用来防止旧数据覆盖新数据
- `ttl_ms` 用来保留剩余生命周期
- `tombstone` 用来防止删除复活
- `Scan` + `BatchSet` 用来做迁移
- `from_peer` 用来防止 peer 之间循环转发

---

## 3. 业务值：CacheEntry demo

### `cache/cache_entry.go`

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
    return e.Value.Len() + 8 + 8 + 1
}

func (e CacheEntry) Expired(now time.Time) bool {
    return !e.ExpireAt.IsZero() && now.After(e.ExpireAt)
}
```

### 业务含义

- `Value`：真正的缓存值
- `Version`：写入顺序
- `ExpireAt`：过期时间
- `Tombstone`：删除墓碑

---

## 4. 路由层：三个环 demo

### `internal/registry/node.go`

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

### `internal/client/picker.go`

```go
type Picker struct {
    mu         sync.RWMutex
    selfAddr   string
    readRing   *consistenthash.Map
    writeRing  *consistenthash.Map
    shadowRing *consistenthash.Map
    clients    map[string]*Client
    nodes      map[string]registry.NodeInfo
}
```

### 三环构建规则

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

### 三个选择器

```go
func (p *Picker) PickReadPeer(key string) (cache.PeerGetter, bool, bool) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    if key == "" || p.readRing == nil || p.readRing.Len() == 0 {
        return nil, false, false
    }
    addr := p.readRing.Get(key)
    if addr == p.selfAddr {
        return nil, true, true
    }
    client, ok := p.clients[addr]
    if !ok {
        return nil, false, false
    }
    return client, true, false
}

func (p *Picker) PickWritePeer(key string) (cache.PeerGetter, bool, bool) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    if key == "" || p.writeRing == nil || p.writeRing.Len() == 0 {
        return nil, false, false
    }
    addr := p.writeRing.Get(key)
    if addr == p.selfAddr {
        return nil, true, true
    }
    client, ok := p.clients[addr]
    if !ok {
        return nil, false, false
    }
    return client, true, false
}

func (p *Picker) PickShadowPeer(key string) (cache.PeerGetter, bool, bool) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    if key == "" || p.shadowRing == nil || p.shadowRing.Len() == 0 {
        return nil, false, false
    }
    addr := p.shadowRing.Get(key)
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

### 读写含义

- `PickReadPeer`：给 `Get`
- `PickWritePeer`：给 `Set/Delete`
- `PickShadowPeer`：给迁移期间双写

---

## 5. 远程调用层：client demo

### `cache/peers.go`

```go
package cache

import (
    "context"
    "time"
)

type PeerPicker interface {
    PickPeer(key string) (peer PeerGetter, ok bool, isSelf bool)
}

type PeerGetter interface {
    Get(ctx context.Context, group string, key string) ([]byte, error)
    Set(ctx context.Context, group string, key string, value []byte, ttl time.Duration, version int64) error
    Delete(ctx context.Context, group string, key string, version int64) error
    Scan(ctx context.Context, group string, startKey string, count int64) ([]Entry, error)
    BatchSet(ctx context.Context, entries []Entry) error
}
```

### `internal/client/client.go`

```go
func (c *Client) Set(ctx context.Context, group string, key string, value []byte, ttl time.Duration, version int64) error {
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()

    resp, err := c.cli.Set(ctx, &cachepb.SetRequest{
        Group:    group,
        Key:      key,
        Value:    value,
        TtlMs:    ttl.Milliseconds(),
        Version:  version,
        FromPeer: cache.IsPeer(ctx),
    })
    if err != nil {
        return err
    }
    if !resp.Ok {
        return fmt.Errorf("set key %s failed", key)
    }
    return nil
}

func (c *Client) Delete(ctx context.Context, group string, key string, version int64) error {
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()

    resp, err := c.cli.Delete(ctx, &cachepb.DeleteRequest{
        Group:    group,
        Key:      key,
        Version:  version,
        FromPeer: cache.IsPeer(ctx),
    })
    if err != nil {
        return err
    }
    if !resp.Ok {
        return fmt.Errorf("delete key %s failed", key)
    }
    return nil
}
```

---

## 6. 服务端：server demo

### `internal/server/server.go`

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
    if ttl < 0 {
        ttl = 0
    }

    if err := g.SetWithVersion(ctx, req.Key, req.Value, ttl, req.Version); err != nil {
        return nil, err
    }
    return &cachepb.SetResponse{Ok: true}, nil
}

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

### 迁移接口

```go
func (s *Server) Scan(ctx context.Context, req *cachepb.ScanRequest) (*cachepb.ScanResponse, error) {
    g := cache.GetGroup(req.Group)
    if g == nil {
        return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
    }

    entries, err := g.Scan(req.StartKey, int(req.Count))
    if err != nil {
        return nil, err
    }
    resp := &cachepb.ScanResponse{Entries: make([]*cachepb.Entry, 0, len(entries))}
    for _, e := range entries {
        resp.Entries = append(resp.Entries, &cachepb.Entry{
            Group:     req.Group,
            Key:       e.Key,
            Value:     e.Value,
            TtlMs:     e.TTL.Milliseconds(),
            Version:   e.Version,
            Tombstone: e.Tombstone,
        })
    }
    return resp, nil
}

func (s *Server) BatchSet(ctx context.Context, req *cachepb.BatchSetRequest) (*cachepb.BatchSetResponse, error) {
    if len(req.Entries) == 0 {
        return &cachepb.BatchSetResponse{Ok: true}, nil
    }

    g := cache.GetGroup(req.Entries[0].Group)
    if g == nil {
        return nil, status.Errorf(codes.NotFound, "group %s not found", req.Entries[0].Group)
    }

    entries := make([]cache.Entry, 0, len(req.Entries))
    for _, item := range req.Entries {
        entries = append(entries, cache.Entry{
            Key:       item.Key,
            Value:     item.Value,
            TTL:       time.Duration(item.TtlMs) * time.Millisecond,
            Version:   item.Version,
            Tombstone: item.Tombstone,
        })
    }
    if err := g.BatchSet(ctx, entries); err != nil {
        return nil, err
    }
    return &cachepb.BatchSetResponse{Ok: true}, nil
}
```

---

## 7. 业务层：Group demo

这一层是最关键的。

### `cache/group.go`

#### 7.1 版本生成

```go
func nextVersion() int64 {
    return time.Now().UnixNano()
}
```

如果你需要更严格的单调递增，可以换成 Snowflake。

#### 7.2 Get

```go
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
    if key == "" {
        return ByteView{}, ErrKeyRequired
    }

    if g.bloomFilter != nil && !g.bloomFilter.MayExist(key) {
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

#### 7.3 SetWithVersion

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

    if !isPeer(ctx) && g.peers != nil {
        if peer, ok, isSelf := g.peers.PickPeer(key); ok && !isSelf {
            return peer.Set(ctx, g.name, key, value, ttl, version)
        }
    }

    old, ok := g.mainCache.GetEntry(key)
    if ok && version < old.Version {
        return nil
    }

    entry := NewCacheEntry(value, ttl, version)
    if err := g.populateEntry(key, entry); err != nil {
        return err
    }

    if !isPeer(ctx) {
        go g.syncShadowSet(key, value, ttl, version)
    }
    return nil
}
```

#### 7.4 DeleteWithVersion

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

    tombstone := NewTombstone(version, 5*time.Minute)
    if err := g.populateEntry(key, tombstone); err != nil {
        return false
    }

    if !isPeer(ctx) {
        go g.syncShadowDelete(key, version)
    }
    return true
}
```

#### 7.5 迁移回填

```go
type Entry struct {
    Key       string
    Value     []byte
    TTL       time.Duration
    Version   int64
    Tombstone bool
}

func (g *Group) BatchSet(ctx context.Context, entries []Entry) error {
    for _, item := range entries {
        old, ok := g.mainCache.GetEntry(item.Key)
        if ok && item.Version < old.Version {
            continue
        }

        var entry CacheEntry
        if item.Tombstone {
            entry = NewTombstone(item.Version, item.TTL)
        } else {
            entry = NewCacheEntry(item.Value, item.TTL, item.Version)
        }

        if err := g.populateEntry(item.Key, entry); err != nil {
            return err
        }
    }
    return nil
}
```

#### 7.6 Scan

```go
func (g *Group) Scan(startKey string, count int) ([]Entry, error) {
    scanner, ok := g.mainCache.Store().(interface {
        Scan(startKey string, count int) []Entry
    })
    if !ok {
        return nil, fmt.Errorf("store does not support scan")
    }
    return scanner.Scan(startKey, count), nil
}
```

#### 7.7 影子双写

```go
func (g *Group) syncShadowSet(key string, value []byte, ttl time.Duration, version int64) {
    if g.peers == nil {
        return
    }
    peer, ok, isSelf := g.peers.PickShadowPeer(key)
    if !ok || isSelf {
        return
    }
    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    ctx = WithPeer(ctx)
    _ = peer.Set(ctx, g.name, key, value, ttl, version)
}

func (g *Group) syncShadowDelete(key string, version int64) {
    if g.peers == nil {
        return
    }
    peer, ok, isSelf := g.peers.PickShadowPeer(key)
    if !ok || isSelf {
        return
    }
    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    ctx = WithPeer(ctx)
    _ = peer.Delete(ctx, g.name, key, version)
}
```

---

## 8. 迁移器 demo

建议新增一个迁移管理器，不要把逻辑全塞进 `Group`。

### `internal/migration/manager.go`

```go
package migration

type Manager struct {
    group     *cache.Group
    picker    *client.Picker
    batchSize int
}

func NewManager(g *cache.Group, p *client.Picker) *Manager {
    return &Manager{
        group:     g,
        picker:    p,
        batchSize:  128,
    }
}
```

### 扩容 Pull

```go
func (m *Manager) Pull(ctx context.Context, owner string, futureRing *consistenthash.Map) error {
    entries, err := m.group.Scan("", 100000)
    if err != nil {
        return err
    }

    batch := make([]cache.Entry, 0, m.batchSize)
    for _, e := range entries {
        if futureRing.Get(e.Key) != owner {
            continue
        }
        batch = append(batch, e)
        if len(batch) >= m.batchSize {
            if err := m.sendBatch(ctx, owner, batch); err != nil {
                return err
            }
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        return m.sendBatch(ctx, owner, batch)
    }
    return nil
}
```

### 缩容 Push

```go
func (m *Manager) Push(ctx context.Context, newRing *consistenthash.Map) error {
    entries, err := m.group.Scan("", 100000)
    if err != nil {
        return err
    }

    byPeer := make(map[string][]cache.Entry)
    for _, e := range entries {
        peerAddr := newRing.Get(e.Key)
        byPeer[peerAddr] = append(byPeer[peerAddr], e)
    }

    for addr, list := range byPeer {
        if addr == "" {
            continue
        }
        if err := m.sendBatch(ctx, addr, list); err != nil {
            return err
        }
    }
    return nil
}
```

### 发送批量数据

```go
func (m *Manager) sendBatch(ctx context.Context, addr string, entries []cache.Entry) error {
    peer, ok := m.picker.PickPeerByAddr(addr)
    if !ok {
        return fmt.Errorf("peer %s not found", addr)
    }
    return peer.BatchSet(ctx, entries)
}
```

---

## 9. 一个最小主流程 demo

### `cmd/cache-node/main.go`

```go
func main() {
    // 1. 启动 server
    srv, err := server.NewServer(":8001", "scores", []string{"http://127.0.0.1:2379"})
    if err != nil {
        log.Fatal(err)
    }

    // 2. 创建 group
    g := cache.NewGroup("scores", 1<<20, getter, 1)

    // 3. 注册 picker
    picker, err := client.NewPicker([]string{"http://127.0.0.1:2379"}, "scores", ":8001")
    if err != nil {
        log.Fatal(err)
    }
    g.RegisterPeers(picker)

    // 4. 启动服务
    if err := srv.Start(); err != nil {
        log.Fatal(err)
    }
}
```

---

## 10. 推荐的调用顺序

### 写入

```text
Set
  -> PickWritePeer
  -> owner 写入
  -> 写入本地 cache
  -> 影子环双写
```

### 读取

```text
Get
  -> PickReadPeer
  -> 命中本地直接返回
  -> 未命中则回源
```

### 迁移

```text
Scan
  -> 过滤属于目标 owner 的 key
  -> BatchSet
  -> version 比较
  -> tombstone 保留
  -> TTL 重新计算
```

---

## 11. 最小可验收 demo

你可以先只验证这 4 条：

1. `Set` 写入后 `Get` 能读到
2. `Delete` 后旧 `Set` 不会复活
3. `warming` 节点不进读环
4. `BatchSet` 不会覆盖更高版本

如果这 4 条成立，后三环和迁移就已经进入正确方向了。

