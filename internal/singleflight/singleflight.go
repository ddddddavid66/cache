package singleflight

import (
	"fmt"
	"sync"
)

type call struct {
	wg    sync.WaitGroup
	val   any
	err   error
	dups  int // 计算同一时刻被拦截的请求 表明是不是热点key
	panic any
}

type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func NewGroup() *Group {
	return &Group{
		m: make(map[string]*call),
	}
}

func (g *Group) Do(key string, fn func() (any, error)) (any, error) {
	val, err, _ := g.DoShared(key, fn)
	return val, err
}

func (g *Group) DoShared(key string, fn func() (any, error)) (any, error, bool) {
	g.mu.Lock()
	if call, ok := g.m[key]; ok {
		call.dups++
		g.mu.Unlock()
		call.wg.Wait()
		if call.panic != nil {
			panic(call.panic)
		}
		return call.val, call.err, true
	}
	// 没有人来
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()
	// 执行任务

	g.doCall(c, key, fn)
	return c.val, c.err, c.dups > 0
}

func (g *Group) doCall(c *call, key string, fn func() (any, error)) {
	defer func() {
		if r := recover(); r != nil {
			c.panic = r
			c.err = fmt.Errorf("singleflight: panic while loading key %q: %v", key, r)
		}

		// 删除锁
		c.wg.Done()
		g.mu.Lock()
		if g.m[key] == c {
			delete(g.m, key)
		}
		g.mu.Unlock()

		if c.panic != nil {
			panic(c.panic)
		}
	}()

	c.val, c.err = fn()
}

func (g *Group) Forget(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.m, key)
}
