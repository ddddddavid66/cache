package store

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var ErrSetErr = fmt.Errorf("could not set key")

// 有L1 L2  分别是独立的LRU

type LRU2Store struct {
	locks  []sync.Mutex
	caches [][2]*cache
	mask   uint32        //通过这个计算key存储到哪个桶
	done   chan struct{} // 后台主动过期策略淘汰 中止
}

func NewLRU2Store(count int, maxBytes1, maxBytes2 int64) *LRU2Store {
	l1Store := int64(maxBytes1 / int64(count))
	l2Store := int64(maxBytes2 / int64(count))

	s := &LRU2Store{
		locks:  make([]sync.Mutex, count),
		caches: make([][2]*cache, count),
		mask:   uint32(count - 1),
	}
	for i := 0; i < count; i++ {
		s.caches[i][0] = NewCache(l1Store)
		s.caches[i][1] = NewCache(l2Store)
	}
	go s.StartCleaner(30 * time.Second)
	return s
}

func (s *LRU2Store) SetWithExpiration(key string, value Value, ttl time.Duration) error {
	if ttl == 0 {
		s.Set(key, value)
	}
	index := s.getIndex(key)
	l1 := s.caches[index][0]
	l2 := s.caches[index][1]
	s.locks[index].Lock() //加锁
	defer s.locks[index].Unlock()
	if value, ok := l1.Get(key); ok {
		l1.SetWithExpiration(key, value, ttl)
		return nil
	}
	if value, ok := l2.Get(key); ok {
		l2.SetWithExpiration(key, value, ttl)
		return nil
	}

	return l1.Set(key, value)

}

func (s *LRU2Store) Set(key string, value Value) error {
	index := s.getIndex(key)
	l1 := s.caches[index][0]
	l2 := s.caches[index][1]
	s.locks[index].Lock() //加锁
	defer s.locks[index].Unlock()
	if value, ok := l1.Get(key); ok {
		l1.Set(key, value)
		return nil
	}
	if value, ok := l2.Get(key); ok {
		l2.Set(key, value)
		return nil
	}
	return ErrSetErr
}

func (s *LRU2Store) Get(key string) (Value, bool) {
	index := s.getIndex(key)
	l1 := s.caches[index][0]
	l2 := s.caches[index][1]
	s.locks[index].Lock() //加锁
	defer s.locks[index].Unlock()
	value, ok := l1.Get(key)
	if !ok {
		value, ok = l2.Get(key)
		if !ok {
			return nil, false
		}
	}
	//  升级为L2
	// 判断是否有 TTL
	if ttl := l1.remainTTL(key); ttl > 0 {
		l2.SetWithExpiration(key, value, ttl)
	} else if ttl == 0 {
		l2.Set(key, value)
		l1.Delete(key)
		return l2.Get(key)
	}
	// ttl < 0
	return nil, false
}

func (s *LRU2Store) Delete(key string) bool {
	index := s.getIndex(key)
	l1 := s.caches[index][0]
	l2 := s.caches[index][1]
	if _, ok := l1.Get(key); ok {
		l1.Delete(key)
		return true
	}
	if _, ok := l2.Get(key); ok {
		l2.Delete(key)
		return true
	}
	return false
}

// TODO   桶里面可以维护计数器 原子操作加减即可
func (s *LRU2Store) Len() int {
	total := 0
	for i := range s.caches {
		s.locks[i].Lock()
		total += s.caches[i][0].ll.Len()
		total += s.caches[i][1].ll.Len()
		s.locks[i].Unlock()
	}
	return total
}

func (s *LRU2Store) Close() error {
	close(s.done) // 清除 主动删除
	for i := range s.caches {
		s.locks[i].Lock()
		s.caches[i][0].clear()
		s.caches[i][1].clear()
		s.locks[i].Unlock()
	}
	return nil
}

func (s *LRU2Store) getIndex(key string) int {
	h := fnv.New32()
	h.Write([]byte(key))
	return int(h.Sum32() & s.mask)
}

// 本质就是一个 LRU 但是不能有锁
type cache struct {
	maxBytes     int64
	usedBytes    int64
	ll           *list.List
	caches       map[string]*list.Element
	expires      map[string]time.Time //添加过期时间 ttl
	expireCount  int64                // 过期量
	onEvicted    func(key string, value Value)
	evictedCount int64 // 被淘汰的量 used > max
}

func NewCache(maxBytes int64) *cache {
	return &cache{
		maxBytes:  maxBytes,
		usedBytes: 0,
		ll:        list.New(),
		caches:    make(map[string]*list.Element, 0),
		onEvicted: func(key string, value Value) { log.Printf("delete key: %s", key) },
	}
}

func (c *cache) Get(key string) (Value, bool) {
	if ele, ok := c.caches[key]; ok { // 存在
		// 检验过期时间
		if exp, ok := c.expires[key]; ok && exp.Before(time.Now()) {
			c.removeElement(ele)
			atomic.AddInt64(&c.expireCount, 1)
			return nil, false
		}
		entry := ele.Value.(*entry)
		return entry.value, true
	}
	return nil, false
}

func (c *cache) Set(key string, value Value) error {
	if ele, ok := c.caches[key]; ok {
		c.ll.MoveToFront(ele)
		entry := ele.Value.(*entry)
		c.usedBytes += int64(value.Len()) - int64(entry.value.Len())
		entry.value = value
		return nil
	}
	ele := c.ll.PushFront(&entry{key: key, value: value})
	c.usedBytes += int64(len(key)) + int64(value.Len())
	c.caches[key] = ele
	// 淘汰
	for c.maxBytes > 0 && c.maxBytes < c.usedBytes {
		c.removeOldest()
	}
	return nil
}

func (c *cache) SetWithExpiration(key string, value Value, ttl time.Duration) error {
	if ttl == 0 {
		c.Set(key, value)
	}
	ttl = randomTTL(ttl, 1*time.Minute)
	if ele, ok := c.caches[key]; ok {
		c.ll.MoveToFront(ele)
		entry := ele.Value.(*entry)
		c.usedBytes += int64(value.Len()) - int64(entry.value.Len())
		entry.value = value
		c.expires[key] = time.Now().Add(ttl) // 刷新过期时间
		return nil
	}
	ele := c.ll.PushFront(&entry{key: key, value: value})
	c.usedBytes += int64(len(key)) + int64(value.Len())
	c.caches[key] = ele
	c.expires[key] = time.Now().Add(ttl) // 添加过期时间
	// 淘汰
	for c.maxBytes > 0 && c.maxBytes < c.usedBytes {
		c.removeOldest()
	}
	return nil
}

func (c *cache) removeOldest() {
	ele := c.ll.Back()
	if ele == nil {
		return
	}
	c.removeElement(ele)
	atomic.AddInt64(&c.evictedCount, 1)
}

func (c *cache) Delete(key string) bool {
	if ele, ok := c.caches[key]; ok {
		c.removeElement(ele)
		return true
	}
	return false
}

func (c *cache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	entry := ele.Value.(*entry)
	c.usedBytes -= int64(len(entry.key)) + int64(entry.value.Len())
	delete(c.caches, entry.key)
	delete(c.expires, entry.key) //删除过期map
}

func (c *cache) clear() {
	c.caches = make(map[string]*list.Element) //重建map
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
	c.ll.Init()
	c.evictedCount = 0
	c.expireCount = 0
}

func randomTTL(base, random time.Duration) time.Duration {
	delta := time.Duration(rand.Int63n(int64(random)*2)) - random
	return base + delta
}

// 主动清理过期key
// TODO 待优化
func (s *LRU2Store) StartCleaner(internal time.Duration) {
	go func() {
		tricker := time.NewTicker(internal)
		defer tricker.Stop()
		for {
			select {
			case <-tricker.C:
				// 干具体事情 一般封装函数
				for i := range s.caches {
					s.locks[i].Lock()
					s.caches[i][0].cleanExpired()
					s.caches[i][1].cleanExpired()
					s.locks[i].Unlock()
				}
			case <-s.done:
				return
			}
		}
	}()
}

func (c *cache) cleanExpired() {
	now := time.Now()
	for key, exp := range c.expires {
		if now.After(exp) {
			if ele, ok := c.caches[key]; ok {
				c.removeElement(ele)
				atomic.AddInt64(&c.expireCount, 1)
			}
		}
	}
}

// NOTE 细节 Until
func (c *cache) remainTTL(key string) time.Duration {
	if exp, ok := c.expires[key]; ok {
		return time.Until(exp)
	}
	return 0
}
