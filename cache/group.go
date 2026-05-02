package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"newCache/internal/singleflight"
)

var ErrKeyRequired = fmt.Errorf("key is required")
var ErrValueRequired = fmt.Errorf("value is required")
var ErrKey = fmt.Errorf("key is not exists")
var nullMaker = &ByteView{b: []byte(nil)}
var ttl = 5 * time.Minute

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

	retryCh chan syncTask
}

type syncTask struct { //重试队列
	key     string
	value   []byte
	attempt int
}

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
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
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheBytes), //新建缓存
		loader:    singleflight.NewGroup(),
		loadSem:   make(chan struct{}, 100),
		retryCh:   make(chan syncTask, 1024), // TODO 真实生产要考虑持久化、限流、丢弃策略
	}
	groups[name] = g //注册全局group
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
	view, ok := g.mainCache.Get(key)
	if ok { //本地缓存存在
		return view, nil
	}
	return g.load(ctx, key) // 查远程缓存  判断是需要回源
}

func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return ErrKeyRequired
	}
	if value == nil {
		return ErrValueRequired
	}
	if err := g.populateCache(key, NewByteView(value), ttl); err != nil { // 放入本地缓存
		return err
	}
	// NOTE 远程失败可降级实现
	//TODO 当前有缺陷 没有实现版本号 可能 旧的会覆盖新的
	//  最终一致 延迟低但是可能得到旧值
	// owner才能写 适合支付 余额场景 强一致 延迟取决于网络
	// 广播失效  适合不能容忍长时间一致性低的
	if isPeer(ctx) { /// 如果是从远程同步过来的，不再转发 如果没有 假设一个节点刚下线 环没有更新 A -> B B->C C->B 无线循环
		return nil
	}
	go g.syncToPeer(key, value)

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
		if getter, ok, isSelf := g.peers.PickPeer(key); ok && !isSelf { // Peer 选择的地方
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
	if ttl == 0 {
		return g.mainCache.Set(key, view)
	}
	return g.mainCache.store.SetWithExpiration(key, view, ttl) //cache 自带锁
}

func (g *Group) Delete(ctx context.Context, key string) bool {
	if key == "" {
		return false
	}
	return g.mainCache.Delete(key)
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

func (g *Group) syncToPeer(key string, value []byte) {
	if g.peers == nil {
		return
	}
	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}
	ctx, canel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer canel()

	ctx = WithPeer(ctx) // 带有from_peer true认证
	if err := peer.Set(ctx, g.name, key, value); err != nil {
		// NOTE 实现放入阻塞队列重试 以及简单的指数退避
		log.Printf("[cache] sync set failed: group=%s key=%s err=%v", g.name, key, err)
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
	// 原始 ctx 可能来自 HTTP/gRPC 请求。Set 返回后，这个 ctx 可能马上取消。
	ctx, canel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer canel()
	ctx = WithPeer(ctx)

	return peer.Set(ctx, g.name, key, value)
}

func (g *Group) enqueueRetry(task syncTask) {
	select {
	case g.retryCh <- task:
	default:
		log.Printf("[cache] retry queue full, drop sync task: group=%s key=%s", g.name, task.key)
	}
}

func (g *Group) retryLoop() {
	for task := range g.retryCh {
		delay := backoff(task.attempt)
		timer := time.NewTimer(delay)
		<-timer.C
		err := g.trySyncToPeer(task.key, task.value)
		if err == nil {
			continue
		}
		task.attempt++
		if task.attempt > 10 {
			log.Printf("[cache] sync retry exceeded: group=%s key=%s err=%v", g.name, task.key, err)
			continue
		}
		g.enqueueRetry(task)
	}
}

// 指数回避
func backoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 100 * time.Millisecond
	case 2:
		return 300 * time.Millisecond
	case 3:
		return 1 * time.Second
	case 4:
		return 3 * time.Second
	default:
		return 5 * time.Second
	}
}
