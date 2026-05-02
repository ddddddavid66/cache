package cache

import (
	"newCache/store"
	"time"
)

type Option func(*cacheConfig)

type cacheConfig struct {
	storeType   string
	bucketCount int
	ttl         time.Duration
	onEvicted   func(key string, value store.Value) //key 被删除的时候 实现什么 这是需要我们自己去传入的
	bloomN      int
	bloomP      float64
	maxBytes    int64
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
