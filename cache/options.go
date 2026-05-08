package cache

import (
	retryqueue "newCache/internal/retry-queue"
	"newCache/internal/wal"
	"newCache/store"
	"time"
)

type Option func(*cacheConfig)

type cacheConfig struct {
	storeType    string
	bucketCount  int
	ttl          time.Duration
	tombstoneTTL time.Duration
	onEvicted    func(key string, value store.Value) //key 被删除的时候 实现什么 这是需要我们自己去传入的
	bloomN       int
	bloomP       float64
	maxBytes     int64
}

//TODO  工厂模式 有选择 配置 Stores

func WithStoreType(t string) Option {
	return func(c *cacheConfig) {
		c.storeType = t
	}
}

func WithBucketCount(n int) Option {
	return func(c *cacheConfig) {
		c.bucketCount = n
	}
}

func WithTTL(d time.Duration) Option {
	return func(c *cacheConfig) {
		c.ttl = d
	}
}

func WithOnEvicted(fn func(string, store.Value)) Option {
	return func(c *cacheConfig) {
		c.onEvicted = fn
	}
}

func WithTombStoneTTL(d time.Duration) Option {
	return func(c *cacheConfig) {
		c.tombstoneTTL = d
	}
}

type GroupOption func(*groupOptions)

type groupOptions struct {
	retryQueue retryqueue.RetryQueue
	walWriter  *wal.Writer
	walPath    string
}

func WithRetryQueue(q retryqueue.RetryQueue) GroupOption {
	return func(cfg *groupOptions) {
		if q != nil {
			cfg.retryQueue = q
		}
	}
}

func WithWalWriter(wal *wal.Writer) GroupOption {
	return func(c *groupOptions) {
		if wal != nil {
			c.walWriter = wal
		}
	}
}

func WithWalPath(path string) GroupOption {
	return func(c *groupOptions) {
		if path != "" {
			c.walPath = path
		}
	}
}
