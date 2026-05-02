package store

import (
	"sync"
	"time"
)

type SimpleStore struct {
	mu    sync.RWMutex
	items map[string]Value
}

func NewSimpleStore(maxBytes int64) *SimpleStore {
	return &SimpleStore{
		items: make(map[string]Value),
	}
}

func (s *SimpleStore) Get(key string) (Value, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.items[key]
	return v, ok
}

func (s *SimpleStore) Set(key string, value Value) error {
	s.mu.Lock() // 加锁
	defer s.mu.Unlock()
	s.items[key] = value
	return nil
}

func (s *SimpleStore) Delete(key string) bool {
	s.mu.Lock() // 加锁
	defer s.mu.Unlock()
	if _, ok := s.items[key]; !ok { // 查看key是否存在
		return false
	}
	delete(s.items, key)
	return true
}

func (s *SimpleStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}
func (s *SimpleStore) Close() error {
	s.mu.Lock() // 加锁
	defer s.mu.Unlock()
	s.items = nil
	return nil
}

func (s *SimpleStore) SetWithExpiration(key string, value Value, ttl time.Duration) error {
	if ttl == 0 {
		s.Set(key, value)
	}
	return nil
}
