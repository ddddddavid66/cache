package cache

import "time"

type CacheEntry struct {
	Value     ByteView
	Version   int64
	ExpiredAt time.Time
	Tombstone bool // 保存墓碑的时候 不需要 value
}

func NewCacheEntry(value ByteView, version int64, ttl time.Duration) CacheEntry {
	e := CacheEntry{
		Value:   value,
		Version: version,
	}
	if ttl > 0 {
		e.ExpiredAt = time.Now().Add(ttl)
	}
	return e
}

func NewTombstone(version int64, ttl time.Duration) CacheEntry {
	e := CacheEntry{ // 不需要Value
		Version:   version,
		Tombstone: true,
	}
	if ttl > 0 {
		e.ExpiredAt = time.Now().Add(ttl)
	}
	return e
}

func (c CacheEntry) Len() int { // 实现Store.Value 接口
	return c.Value.Len() + 8 + 8 + 1
}

func (c CacheEntry) Expired(now time.Time) bool {
	return !c.ExpiredAt.IsZero() && now.After(c.ExpiredAt)
}
