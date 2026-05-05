package store

import "time"

// 它只负责 存储缓存 删除缓存
// 还有缓存是否应该被淘汰

// 先不定义LRU  用一个简单simpleMap 占位 保证不会出现错误
// 后面写 LRU 时，只要它也实现 `Store`，上层不用改。

type Store interface {
	Get(key string) (Value, bool)
	Set(key string, value Value) error
	SetWithExpiration(key string, value Value, ttl time.Duration) error
	Delete(key string) bool
	Len() int
	Close() error
}

// 可选实现接口
type Scanner interface {
	Scan(startKey string, count int64) []EntryStore
}

type EntryStore struct {
	Key   string
	Value Value
	TTL   time.Duration
}
