package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testValue string

func (v testValue) Len() int {
	return len(v)
}

func newTestLRU2Store(t *testing.T) *LRU2Store {
	t.Helper()

	s := NewLRU2Store(2, 1<<20, 1<<20)
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

func TestLRU2SetGet(t *testing.T) {
	s := newTestLRU2Store(t)

	if err := s.Set("foo", testValue("bar")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, ok := s.Get("foo")
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got != testValue("bar") {
		t.Fatalf("Get() value = %v, want %v", got, testValue("bar"))
	}
}

func TestLRU2SetWithExpiration(t *testing.T) {
	s := newTestLRU2Store(t)

	if err := s.SetWithExpiration("foo", testValue("bar"), time.Minute); err != nil {
		t.Fatalf("SetWithExpiration() error = %v", err)
	}

	got, ok := s.Get("foo")
	if !ok {
		t.Fatal("Get() before expiration ok = false, want true")
	}
	if got != testValue("bar") {
		t.Fatalf("Get() value = %v, want %v", got, testValue("bar"))
	}

	idx := s.getIndex("foo")
	s.locks[idx].Lock()
	if _, ok := s.caches[idx][1].expires["foo"]; ok {
		s.caches[idx][1].expires["foo"] = time.Now().Add(-time.Second)
	} else {
		s.caches[idx][0].expires["foo"] = time.Now().Add(-time.Second)
	}
	s.locks[idx].Unlock()

	if got, ok := s.Get("foo"); ok {
		t.Fatalf("Get() after expiration = %v, true; want nil, false", got)
	}
}

func TestLRU2Delete(t *testing.T) {
	s := newTestLRU2Store(t)

	if err := s.Set("foo", testValue("bar")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if deleted := s.Delete("foo"); !deleted {
		t.Fatal("Delete() = false, want true")
	}
	if got, ok := s.Get("foo"); ok {
		t.Fatalf("Get() after delete = %v, true; want nil, false", got)
	}
}

func TestLRU2Close(t *testing.T) {
	s := NewLRU2Store(2, 1<<20, 1<<20)

	if err := s.Set("foo", testValue("bar")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() after Close() = %d, want 0", got)
	}
}

func TestLRU2Promotion(t *testing.T) {
	s := newTestLRU2Store(t)

	if err := s.Set("foo", testValue("bar")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	idx := s.getIndex("foo")
	if _, ok := s.caches[idx][0].Get("foo"); !ok {
		t.Fatal("key should start in L1")
	}
	if _, ok := s.caches[idx][1].Get("foo"); ok {
		t.Fatal("key should not start in L2")
	}

	got, ok := s.Get("foo")
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got != testValue("bar") {
		t.Fatalf("Get() value = %v, want %v", got, testValue("bar"))
	}

	if _, ok := s.caches[idx][0].Get("foo"); ok {
		t.Fatal("key should be removed from L1 after promotion")
	}
	if got, ok := s.caches[idx][1].Get("foo"); !ok || got != testValue("bar") {
		t.Fatalf("key should be promoted to L2, got value=%v ok=%v", got, ok)
	}
}

func TestLRU2DeleteConcurrentSafe(t *testing.T) {
	s := NewLRU2Store(1, 1<<20, 1<<20)
	t.Cleanup(func() {
		_ = s.Close()
	})

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := s.Set(key, testValue("value")); err != nil {
			t.Fatalf("Set(%q) error = %v", key, err)
		}
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := offset; i < 200; i += 2 {
				_ = s.Delete(fmt.Sprintf("key-%d", i))
			}
		}(worker)
	}
	wg.Wait()
}

func TestLRU2ScanOrdersFiltersAndLimits(t *testing.T) {
	s := newTestLRU2Store(t)

	for _, item := range []struct {
		key   string
		value testValue
	}{
		{key: "b", value: "value-b"},
		{key: "a", value: "value-a"},
		{key: "c", value: "value-c"},
	} {
		if err := s.Set(item.key, item.value); err != nil {
			t.Fatalf("Set(%q) error = %v", item.key, err)
		}
	}

	records := s.Scan("a", 2)
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2: %+v", len(records), records)
	}
	if records[0].Key != "b" || records[1].Key != "c" {
		t.Fatalf("Scan keys = [%q %q], want [b c]", records[0].Key, records[1].Key)
	}
}

func TestLRU2ScanCarriesTTLAndSkipsExpired(t *testing.T) {
	s := newTestLRU2Store(t)

	if err := s.SetWithExpiration("live", testValue("value"), time.Minute); err != nil {
		t.Fatalf("SetWithExpiration(live) error = %v", err)
	}
	if err := s.SetWithExpiration("expired", testValue("value"), time.Minute); err != nil {
		t.Fatalf("SetWithExpiration(expired) error = %v", err)
	}

	idx := s.getIndex("expired")
	s.locks[idx].Lock()
	if _, ok := s.caches[idx][0].expires["expired"]; ok {
		s.caches[idx][0].expires["expired"] = time.Now().Add(-time.Second)
	}
	if _, ok := s.caches[idx][1].expires["expired"]; ok {
		s.caches[idx][1].expires["expired"] = time.Now().Add(-time.Second)
	}
	s.locks[idx].Unlock()

	records := s.Scan("", 0)
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1: %+v", len(records), records)
	}
	if records[0].Key != "live" {
		t.Fatalf("Scan key = %q, want live", records[0].Key)
	}
	if records[0].TTL <= 0 {
		t.Fatalf("Scan TTL = %v, want positive", records[0].TTL)
	}
}
