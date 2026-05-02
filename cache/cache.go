package cache

import (
	"errors"
	"newCache/store"
	"sync"
	"sync/atomic"
)

//`Cache` 是 `Group` 和 `Store` 之间的一层包，
// 负责并发保护、命中率统计、关闭状态等。

type Cache struct {
	mu     sync.RWMutex
	store  store.Store //定义抽象的本地存储方式
	hits   int64       // 命中次数
	misses int64       // 丢失次数
	closed atomic.Bool
}

// Simple 占位
// 后续跟高级  func NewCache(maxBytes int64, opts ...Option) *Cache
func NewCache(maxBytes int64, opts ...Option) *Cache {
	cfg := &cacheConfig{ // 默认创建 LRU2
		storeType:   "LRU2",
		bucketCount: 32,
		maxBytes:    64 << 20,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return &Cache{
		store: newStore(maxBytes, *cfg),
	}
}

func newStore(maxBytes int64, cfg cacheConfig) store.Store {
	switch cfg.storeType {
	case "simple":
		return store.NewSimpleStore(maxBytes)
	case "LRU":
		return store.NewLRUStore(maxBytes, cfg.onEvicted)
	case "LRU2":
		return store.NewLRU2Store(cfg.bucketCount, maxBytes, maxBytes)
	default:
		return store.NewSimpleStore(maxBytes)
	}
}

// 操作不需要加锁 因为store 已经 加上锁了

func (c *Cache) Get(key string) (ByteView, bool) {
	if c.closed.Load() {
		return ByteView{}, false
	}

	v, ok := c.store.Get(key)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	bv, ok := v.(ByteView)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	atomic.AddInt64(&c.hits, 1)
	return bv, true
}

func (c *Cache) Set(key string, value ByteView) error {
	if c.closed.Load() {
		return errors.New("cache is closed")
	}

	return c.store.Set(key, value)
}

func (c *Cache) Delete(key string) bool {
	if c.closed.Load() {
		return false
	}

	return c.store.Delete(key)
}

func (c *Cache) Len() int {

	return c.store.Len()
}

func (c *Cache) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	return c.store.Close()
}
