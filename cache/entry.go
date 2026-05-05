package cache

import "time"

type TransportEntry struct {
	Group     string
	Key       string
	Value     []byte
	TtlMs     int64
	Version   int64
	Tombstone bool
}

func NewCacheTEntry(value ByteView, version int64, ttl time.Duration) TransportEntry {
	e := TransportEntry{
		Value:   value.ByteSlice(),
		Version: version,
	}
	if ttl > 0 {
		e.TtlMs = ttl.Microseconds()
	}
	return e
}

func NewTombstoneTCaches(version int64, ttl time.Duration) TransportEntry {
	e := TransportEntry{ // 不需要Value
		Version:   version,
		Tombstone: true,
	}
	if ttl > 0 {
		e.TtlMs = ttl.Microseconds()
	}
	return e
}
