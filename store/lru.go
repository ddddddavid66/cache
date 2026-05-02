package store

import (
	"container/list"
	"sync"
	"time"
)

// map + 双向链表
//map查找有没有key
//双向链表 调整访问顺序  淘汰末端节点

var ttl = 5 * time.Minute

type entry struct {
	key   string
	value Value
}

type LRUStore struct {
	mu        sync.Mutex //不是互斥锁 读完以后要移动链表
	maxBytes  int64
	usedBytes int64
	ll        *list.List // 双向链表 里面是 entry
	cache     map[string]*list.Element
	onEvicted func(key string, value Value) //淘汰 做统计 写日志 通知
	expires   map[string]time.Time          //TTL  过期时间
	// TODO 定时扫描清理 后面在做
}

func NewLRUStore(maxBytes int64, onEvicted func(key string, value Value)) *LRUStore {
	return &LRUStore{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		expires:   make(map[string]time.Time),
		onEvicted: onEvicted,
		usedBytes: 0,
	}
}

func (s *LRUStore) Get(key string) (Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element, ok := s.cache[key]; ok { // 找到了
		// 判断是否过期
		if exp, ok := s.expires[key]; ok && exp.Before(time.Now()) {
			s.removeElement(element)
			return nil, false
		}
		// 更新使用频率
		s.ll.MoveToFront(element)
		entry := element.Value.(*entry) //转换为entry
		return entry.value, true
	}
	return nil, false
}

func (s *LRUStore) Set(key string, value Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 判断key是否存在
	if ele, ok := s.cache[key]; ok {
		// 去除 entry  更新value  更新大小 更新使用频率
		s.ll.MoveToFront(ele)
		entry := ele.Value.(*entry)
		s.usedBytes += int64(value.Len()) - int64(entry.value.Len())
		entry.value = value
		return nil
	}
	// 更新使用频率
	ele := s.ll.PushFront(&entry{key: key, value: value})
	s.cache[key] = ele
	s.usedBytes += int64(value.Len()) + int64(len(key))
	// 删除旧的
	for s.maxBytes > 0 && s.usedBytes > s.maxBytes {
		s.removeOldEst()
	}
	return nil
}

func (s *LRUStore) SetWithExpiration(key string, value Value, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ttl == 0 {
		s.Set(key, value)
	}
	ttl = randomTTL(ttl, 1*time.Minute)
	// 判断key是否存在
	if ele, ok := s.cache[key]; ok {
		// 去除 entry  更新value  更新大小 更新使用频率
		s.ll.MoveToFront(ele)
		entry := ele.Value.(*entry)
		s.usedBytes += int64(value.Len()) - int64(entry.value.Len())
		entry.value = value
		s.expires[key] = time.Now().Add(ttl)
		return nil
	}
	// 更新使用频率
	ele := s.ll.PushFront(&entry{key: key, value: value})
	s.cache[key] = ele
	s.usedBytes += int64(value.Len()) + int64(len(key))
	s.expires[key] = time.Now().Add(ttl)
	// 删除旧的
	for s.maxBytes > 0 && s.usedBytes > s.maxBytes {
		s.removeOldEst()
	}
	return nil
}

func (s *LRUStore) removeOldEst() {
	ele := s.ll.Back()
	if ele == nil {
		return
	}
	s.removeElement(ele)
}

func (s *LRUStore) removeElement(ele *list.Element) {
	// 链表移除 减小store大小 map删除
	s.ll.Remove(ele)
	entry := ele.Value.(*entry)
	s.usedBytes -= int64(entry.value.Len()) + int64(len(entry.key))
	delete(s.cache, entry.key)
	delete(s.expires, entry.key) //删除过期map
	if s.onEvicted != nil {
		s.onEvicted(entry.key, entry.value) //写日志
	}
}

func (s *LRUStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.cache[key]; ok {
		s.removeElement(ele)
		return true
	}
	return false
}

func (s *LRUStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

func (s *LRUStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usedBytes = 0
	s.ll.Init()
	s.expires = make(map[string]time.Time)
	s.cache = make(map[string]*list.Element)
	return nil
}
