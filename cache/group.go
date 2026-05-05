package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"newCache/internal/singleflight"
	"newCache/store"
)

var ErrKeyRequired = fmt.Errorf("key is required")
var ErrValueRequired = fmt.Errorf("value is required")
var ErrKey = fmt.Errorf("key is not exists")
var nullMaker = &ByteView{b: []byte(nil)}
var ttl = 5 * time.Minute

const deleteTomestoneTTL = 5 * time.Minute

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// 缓存命名空间 一个group对应 一类空间 比如user product
type Group struct {
	name        string
	getter      Getter
	mainCache   *Cache
	peers       PeerPicker
	loader      *singleflight.Group
	loadSem     chan struct{} // 限流令牌 防止缓存雪崩
	bloomFilter *BloomFilter

	retryCh      chan syncTask
	closeCh      chan struct{} // 关闭 retryCh
	versionGen   *Snowflake    // version 生成
	tombstoneTTL time.Duration // 墓碑TTL
}

type syncTask struct { // owner 重试队列
	key     string
	value   []byte
	attempt int
	version int64
	ttl     time.Duration
	option  string // ?
}

const ( // option 操作
	syncSet    = "set"
	syncDelete = "delete"

	maxRetryAttempts = 10
	baseRetryDelay   = 100 * time.Millisecond
	maxRetryDelay    = 10 * time.Second

	maxRetryWorkers = 5
)

func NewGroup(name string, cacheBytes int64, getter Getter, workID int64) *Group {
	// 检查
	if getter == nil {
		panic("nil Getter")
	}
	if name == "" {
		panic("empty group name")
	}
	mu.Lock()
	defer mu.Unlock()

	g := &Group{
		name:         name,
		getter:       getter,
		mainCache:    NewCache(cacheBytes), //新建缓存
		loader:       singleflight.NewGroup(),
		loadSem:      make(chan struct{}, 100),
		retryCh:      make(chan syncTask, 1024), // TODO 真实生产要考虑持久化、限流、丢弃策略
		versionGen:   NewSnowflake(workID),
		tombstoneTTL: deleteTomestoneTTL, //TODO 后面可以改成 options
		closeCh:      make(chan struct{}),
	}
	groups[name] = g //注册全局group

	for i := 0; i < maxRetryWorkers; i++ {
		go g.retryLoop()
	}

	return g
}

func GetGroup(name string) *Group { //  获取全局group
	mu.RLock()
	defer mu.RUnlock()
	return groups[name]
}

// 注册 布隆过滤器
func (g *Group) RegisterBloomFilter(bf *BloomFilter) {
	g.bloomFilter = bf
}

// bf := NewBloomFilter(100000000, 0.001) 即可

// 注册远程节点选择器
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPerrs called more than once")
	}
	g.peers = peers
}

// group 最核心的 方法
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	//NOTE 加一个布隆过滤器
	if g.bloomFilter != nil && !g.bloomFilter.MayExist(key) {
		// NOTE  不存在的key空值缓存
		g.populateCache(key, *nullMaker, 30*time.Second)
		return ByteView{}, ErrKey
	}
	if key == "" {
		return ByteView{}, ErrKeyRequired
	}
	if entry, ok := g.mainCache.GetEntry(key); ok { //本地缓存存在
		if entry.Tombstone {
			return ByteView{}, ErrKey
		}
		return entry.Value, nil
	}

	return g.load(ctx, key) // 查远程缓存  判断是需要回源
}

func (g *Group) Set(ctx context.Context, key string, value []byte, version int64, ttl time.Duration) error {
	if key == "" {
		return ErrKeyRequired
	}
	if value == nil {
		return ErrValueRequired
	}
	if version < 0 {
		version = g.versionGen.Next()
	}
	old, ok := g.mainCache.GetEntry(key)
	if ok && version < old.Version { // 版本失效
		return nil
	}
	// 不是peer的请求 路由到 owner
	//NOTE owner 实现 还有delete
	if !isPeer(ctx) && g.peers != nil {
		if peer, ok, isSelf := g.peers.PickWritePeer(key); !isSelf && ok {
			return peer.Set(ctx, g.name, key, value, version, ttl)
		}
	}

	//ttl 是 通用的
	entry := NewCacheEntry(ByteView{b: value}, version, ttl)
	if err := g.populateEntry(key, entry); err != nil {
		return err
	}

	//本地来的 判断是不是双写 判断逻辑不是自己写
	if !isPeer(ctx) {
		go g.trySyncSet(key, value, version, ttl)
	}

	// NOTE 远程失败可降级实现
	//TODO 当前有缺陷 没有实现版本号 可能 旧的会覆盖新的 -> sync 先隐藏
	// NOTE tombstone + owner 实现强一致
	//  最终一致 延迟低但是可能得到旧值
	// owner才能写 适合支付 余额场景 强一致 延迟取决于网络
	// 广播失效  适合不能容忍长时间一致性低的

	// if isPeer(ctx) { /// 如果是从远程同步过来的，不再转发 如果没有 假设一个节点刚下线 环没有更新 A -> B B->C C->B 无线循环
	// 	return nil
	// }
	// go g.syncToPeer(key, value)

	return nil
}

func (g *Group) load(ctx context.Context, key string) (ByteView, error) {
	// singleflight 合并请求   同一个 key 同一时刻只执行一次真实加载
	view, err := g.loader.Do(key, func() (any, error) { // 返回值 fn的返回值 fn的错误 shared bool
		// 参数是  key fn 干活的函数  这个函数去调用远程节点 获取数据
		view, err := g.loadData(ctx, key)
		if err != nil {
			return nil, err
		}
		err = g.populateCache(key, view, ttl) // 回填本地缓存
		return view, err
	})
	if err != nil {
		return ByteView{}, err
	}
	return view.(ByteView), nil //类型断言
}

func (g *Group) loadData(ctx context.Context, key string) (ByteView, error) {
	//NOTE 限流
	select {
	case g.loadSem <- struct{}{}:
		defer func() { <-g.loadSem }()
	case <-ctx.Done():
		return ByteView{}, ctx.Err() // ctx 超时关闭
	}
	if isPeer(ctx) {
		return g.getLocally(ctx, key)
	}
	//真正去干活
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if g.peers != nil {
		if getter, ok, isSelf := g.peers.PickReadPeer(key); ok && !isSelf { // Peer 选择的地方
			// NOTE peer 选到自己时不走 gRPC，避免自己调用自己。
			view, err := g.getFromPeer(ctx, getter, key)
			if err == nil {
				return view, nil
			}
		}
	}
	return g.getLocally(ctx, key)
}

func (g *Group) getFromPeer(ctx context.Context, peer PeerGetter, key string) (ByteView, error) {
	byte, err := peer.Get(ctx, g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return NewByteView(byte), nil
}

func (g *Group) getLocally(ctx context.Context, key string) (ByteView, error) {
	// 本地getter回源
	byte, err := g.getter.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrKey) {
			g.populateCache(key, *nullMaker, 30*time.Second)
			return ByteView{}, err
		}
		return ByteView{}, err
	}
	return NewByteView(byte), nil
}

func (g *Group) populateCache(key string, view ByteView, ttl time.Duration) error { //回填本地数据
	entry := CacheEntry{
		Value:   view,
		Version: g.versionGen.Next(),
	}
	if ttl > 0 {
		entry.ExpiredAt = time.Now().Add(ttl)
	}
	return g.mainCache.Set(key, entry)
}

func (g *Group) populateEntry(key string, entry CacheEntry) error { //TTL 在entry里面
	return g.mainCache.Set(key, entry)
}

func (g *Group) Delete(ctx context.Context, key string) bool {
	version := g.versionGen.Next()
	return g.DeleteWithVersion(ctx, key, version)
}

func (g *Group) DeleteWithVersion(ctx context.Context, key string, version int64) bool {
	if key == "" {
		return false
	}
	if version <= 0 {
		version = g.versionGen.Next()
	}

	if !isPeer(ctx) && g.peers != nil {
		if peer, ok, isSelf := g.peers.PickWritePeer(key); ok && !isSelf {
			return peer.Delete(ctx, g.name, key, version)
		}
	}

	old, ok := g.mainCache.GetEntry(key)
	if ok && version < old.Version {
		return false
	}

	tombstone := NewTombstone(version, deleteTomestoneTTL)
	err := g.mainCache.Set(key, tombstone)
	if err != nil {
		return false // 防止主写失败
	}
	//添加影子双删除
	if !isPeer(ctx) {
		go g.trySyncDelete(key, version)
	}
	return true // 留一个 过期时间
}

func (g *Group) Close() error {
	if g == nil {
		return nil
	}
	mu.Lock()
	if groups[g.name] == g {
		delete(groups, g.name)
	}
	mu.Unlock()
	close(g.closeCh)
	// 关闭 peerpicke
	if clsoer, ok := g.peers.(interface{ Close() error }); ok {
		_ = clsoer.Close()
	}
	return g.mainCache.Close()
}

type contextKey string

const fromPeerKey contextKey = "from_peer"

func isPeer(ctx context.Context) bool {
	ok, _ := ctx.Value(fromPeerKey).(bool)
	return ok
}

func IsPeer(ctx context.Context) bool {
	return isPeer(ctx)
}

func WithPeer(ctx context.Context) context.Context {
	return context.WithValue(ctx, fromPeerKey, true)
}

// func (g *Group) syncToPeer(key string, value []byte) {
// 	if g.peers == nil {
// 		return
// 	}
// 	peer, ok, isSelf := g.peers.PickPeer(key)
// 	if !ok || isSelf {
// 		return
// 	}
// 	ctx, canel := context.WithTimeout(context.Background(), 500*time.Millisecond)
// 	defer canel()

// 	ctx = WithPeer(ctx) // 带有from_peer true认证
// 	if err := peer.Set(ctx, g.name, key, value); err != nil {
// 		// NOTE 实现放入阻塞队列重试 以及简单的指数退避
// 		log.Printf("[cache] sync set failed: group=%s key=%s err=%v", g.name, key, err)
// 	}
// }

// func (g *Group) trySyncToPeer(key string, value []byte) error {
// 	if g.peers == nil {
// 		return nil
// 	}
// 	peer, ok, isSelf := g.peers.PickPeer(key)
// 	if !ok || isSelf {
// 		return nil
// 	}
// 	// 原始 ctx 可能来自 HTTP/gRPC 请求。Set 返回后，这个 ctx 可能马上取消。
// 	ctx, canel := context.WithTimeout(context.Background(), 500*time.Millisecond)
// 	defer canel()
// 	ctx = WithPeer(ctx)

// 	return peer.Set(ctx, g.name, key, value)
// }

func (g *Group) enqueueRetry(task syncTask) {
	if task.value != nil {
		task.value = append([]byte(nil), task.value...) // 复制
	}
	select {
	case g.retryCh <- task:
	case <-g.closeCh:
		return
	default:
		log.Printf("[cache] retry queue full, drop sync task: group=%s key=%s", g.name, task.key)
	}
}

func (g *Group) retryLoop() { // NOTE 避免 因为休眠导致的关闭延时
	for {
		select {
		case task := <-g.retryCh:
			g.handleRetry(task)
		case <-g.closeCh:
			return
		}
	}
}

func (g *Group) handleRetry(task syncTask) {
	delay := backoff(task.attempt)
	timer := time.NewTimer(delay)
	select {
	case <-timer.C:
		var err error
		switch task.option {
		case syncSet:
			err = g.syncShadowSet(task.key, task.value, task.version, task.ttl)
		case syncDelete:
			err = g.syncShadowDelete(task.key, task.version)
		default:
			return
		}
		if err == nil {
			return
		}
		task.attempt++
		if task.attempt > 10 {
			log.Printf("[cache] sync retry exceeded: group=%s key=%s err=%v", g.name, task.key, err)
			return
		}
		g.enqueueRetry(task)
	case <-g.closeCh:
		timer.Stop()
		return
	}
}

// 指数回避  实现
func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := baseRetryDelay << (attempt - 1)
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	// 随机抖动 防止同一时刻所有任务并发执行
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return delay/2 + jitter
}

func (g *Group) trySyncSet(key string, value []byte, version int64, ttl time.Duration) {
	if err := g.syncShadowSet(key, value, version, ttl); err != nil {
		g.enqueueRetry(syncTask{
			key:     key,
			value:   value,
			version: version,
			ttl:     ttl,
			attempt: 1,
			option:  syncSet,
		})
	}
}

func (g *Group) trySyncDelete(key string, version int64) {
	if err := g.syncShadowDelete(key, version); err != nil {
		g.enqueueRetry(syncTask{
			key:     key,
			version: version,
			attempt: 1,
			option:  syncDelete,
		})
	}
}

// key是startkey
func (g *Group) Scan(key string, count int64) ([]*TransportEntry, error) {
	scanner, ok := g.mainCache.store.(store.Scanner)
	if !ok {
		return nil, fmt.Errorf("store :%v don support scand", g.mainCache.store)
	}
	records := scanner.Scan(key, count)
	entries := make([]*TransportEntry, 0, len(records))
	for _, record := range records {
		var cacheEntry CacheEntry
		switch v := record.Value.(type) {
		case CacheEntry:
			cacheEntry = v
		case *CacheEntry:
			if v == nil {
				continue
			}
			cacheEntry = *v
		default:
			continue
		}
		entries = append(entries, &TransportEntry{
			Group:     g.name,
			Key:       record.Key,
			Value:     cacheEntry.Value.ByteSlice(),
			TtlMs:     record.TTL.Milliseconds(),
			Tombstone: cacheEntry.Tombstone,
			Version:   cacheEntry.Version,
		})
	}
	return entries, nil
}

func (g *Group) BatchSet(ctx context.Context, entires []TransportEntry) error {
	for _, entry := range entires {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		old, ok := g.mainCache.GetEntry(entry.Key)
		if ok && old.Version > entry.Version { //版本旧
			continue
		}
		//更新ttl
		ttl := time.Duration(entry.TtlMs) * time.Millisecond
		var cacheEntry CacheEntry
		if entry.Tombstone {
			cacheEntry = NewTombstone(entry.Version, ttl)
		} else {
			cacheEntry = NewCacheEntry(NewByteView(entry.Value), entry.Version, ttl)
		}
		if err := g.populateEntry(entry.Key, cacheEntry); err != nil {
			return err
		}
	}
	return nil
}

// 影子双写 这时候 是迁移阶段 主环跟写环 是不一样的
func (g *Group) syncShadowSet(key string, value []byte, version int64, ttl time.Duration) error {
	//先判断是否需要双写
	if g.peers == nil {
		return nil
	}
	peer, ok, isSelf := g.peers.PickShadowPeer(key)
	if !ok || isSelf {
		return nil
	}
	//设置超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ctx = WithPeer(ctx)
	//开始双写  写入期间的判断 peer的set实现
	err := peer.Set(ctx, g.name, key, value, version, ttl)
	return err
}

// 影子删除  删除的时候 随着迁移到新节点 删除旧节点的数据
func (g *Group) syncShadowDelete(key string, version int64) error {
	if g.peers == nil {
		return nil
	}
	peer, ok, isSelf := g.peers.PickShadowPeer(key)
	if !ok || isSelf {
		return nil
	}
	//设置超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ctx = WithPeer(ctx)
	if ok := peer.Delete(ctx, g.name, key, version); !ok {
		return fmt.Errorf("shadowDelete err")
	}
	return nil
}
