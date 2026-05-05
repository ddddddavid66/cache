package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupGet(t *testing.T) {
	loads := 0
	g := newTestGroup(t, "users", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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
	g := newTestGroup(t, "users", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
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

func TestGroupSetRejectsOlderVersion(t *testing.T) {
	g := newTestGroup(t, "version", nil)

	if err := g.Set(context.Background(), "k", []byte("new"), 20, 10*time.Minute); err != nil {
		t.Fatalf("Set(new) error = %v", err)
	}
	if err := g.Set(context.Background(), "k", []byte("old"), 10, 10*time.Minute); err != nil {
		t.Fatalf("Set(old) error = %v", err)
	}

	got, err := g.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.String() != "new" {
		t.Fatalf("Get() = %q, want %q", got.String(), "new")
	}
}

func TestGroupDeleteWritesTombstone(t *testing.T) {
	g := newTestGroup(t, "delete", nil)

	if err := g.Set(context.Background(), "k", []byte("value"), 10, 10*time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if ok := g.DeleteWithVersion(context.Background(), "k", 20); !ok {
		t.Fatal("DeleteWithVersion() = false, want true")
	}

	entry, ok := g.mainCache.GetEntry("k")
	if !ok {
		t.Fatal("mainCache.GetEntry() ok = false, want true")
	}
	if !entry.Tombstone {
		t.Fatal("entry.Tombstone = false, want true")
	}
	if entry.Version != 20 {
		t.Fatalf("entry.Version = %d, want 20", entry.Version)
	}
}

func TestGroupGetTombstoneReturnsErrKey(t *testing.T) {
	g := newTestGroup(t, "tombstone-get", nil)

	if ok := g.DeleteWithVersion(context.Background(), "k", 20); !ok {
		t.Fatal("DeleteWithVersion() = false, want true")
	}

	got, err := g.Get(context.Background(), "k")
	if !errors.Is(err, ErrKey) {
		t.Fatalf("Get() error = %v, want %v", err, ErrKey)
	}
	if got.Len() != 0 {
		t.Fatalf("Get() value length = %d, want 0", got.Len())
	}
}

func TestGroupTombstoneRejectsOldSet(t *testing.T) {
	g := newTestGroup(t, "tombstone-version", nil)

	if ok := g.DeleteWithVersion(context.Background(), "k", 20); !ok {
		t.Fatal("DeleteWithVersion() = false, want true")
	}
	if err := g.Set(context.Background(), "k", []byte("old"), 10, 10*time.Minute); err != nil {
		t.Fatalf("Set(old) error = %v", err)
	}

	entry, ok := g.mainCache.GetEntry("k")
	if !ok {
		t.Fatal("mainCache.GetEntry() ok = false, want true")
	}
	if !entry.Tombstone {
		t.Fatalf("entry.Tombstone = false, want true; value=%q", entry.Value.String())
	}
	if entry.Version != 20 {
		t.Fatalf("entry.Version = %d, want 20", entry.Version)
	}
	if _, err := g.Get(context.Background(), "k"); !errors.Is(err, ErrKey) {
		t.Fatalf("Get() error = %v, want %v", err, ErrKey)
	}
}

var testGroupSeq int64

func newTestGroup(t *testing.T, name string, getter Getter) *Group {
	t.Helper()
	if getter == nil {
		getter = GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return nil, ErrKey
		})
	}
	id := atomic.AddInt64(&testGroupSeq, 1)
	g := NewGroup(fmt.Sprintf("%s-%d", name, id), 1024, getter, id)
	t.Cleanup(func() {
		_ = g.Close()
	})
	return g
}
