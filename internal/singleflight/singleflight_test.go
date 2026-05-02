package singleflight

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoSameKeyOnlyRunsOnce(t *testing.T) {
	var calls int64
	g := NewGroup()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := g.Do("hot", func() (any, error) {
				atomic.AddInt64(&calls, 1)
				time.Sleep(50 * time.Millisecond)
				return "ok", nil
			})
			if err != nil {
				t.Errorf("Do returned error: %v", err)
				return
			}
			if v != "ok" {
				t.Errorf("Do returned %v, want ok", v)
			}
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoDifferentKeysDoNotBlockEachOther(t *testing.T) {
	var calls int64
	g := NewGroup()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := g.Do(key, func() (any, error) {
				atomic.AddInt64(&calls, 1)
				time.Sleep(50 * time.Millisecond)
				return key, nil
			})
			if err != nil {
				t.Errorf("Do returned error: %v", err)
				return
			}
			if v != key {
				t.Errorf("Do returned %v, want %s", v, key)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if calls != 100 {
		t.Fatalf("calls = %d, want 100", calls)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("different keys look serialized: elapsed=%s", elapsed)
	}
}
