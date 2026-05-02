package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupGet(t *testing.T) {
	loads := 0
	g := NewGroup("users", 1024, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		loads++
		return []byte("value:" + key), nil
	}))

	v, err := g.Get(context.Background(), "1001")
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "value:1001" {
		t.Fatalf("unexpected value %q", v.String())
	}

	_, _ = g.Get(context.Background(), "1001")
	if loads != 1 {
		t.Fatalf("getter called %d times", loads)
	}
}

func TestGroupGetSingleflight(t *testing.T) {
	loads := int64(0)
	g := NewGroup("users", 1024, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt64(&loads, 1)
		time.Sleep(50 * time.Millisecond)
		return []byte("ok"), nil
	}))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.Get(context.Background(), "hot")
		}()
	}
	wg.Wait()

	if loads != 1 {
		t.Fatalf("loads = %d, want 1", loads)
	}
}
