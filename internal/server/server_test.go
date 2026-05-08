package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	cachepb "newCache/api/proto"
	"newCache/cache"
)

func TestServerSetFromPeerDoesNotForwardAgain(t *testing.T) {
	g, groupName := newServerTestGroup(t, "from-peer")
	picker := &countingPeerPicker{}
	g.RegisterPeers(picker)
	s := &Server{}

	resp, err := s.Set(context.Background(), &cachepb.SetRequest{
		Group:    groupName,
		Key:      "k",
		Value:    []byte("value"),
		FromPeer: true,
		Version:  100,
		TtlMs:    int64((10 * time.Minute) / time.Millisecond),
	})
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if !resp.Ok {
		t.Fatal("Set() Ok = false, want true")
	}
	if got := picker.writeCalls.Load(); got != 0 {
		t.Fatalf("PickWritePeer calls = %d, want 0", got)
	}
	if got := picker.shadowCalls.Load(); got != 0 {
		t.Fatalf("PickShadowPeer calls = %d, want 0", got)
	}
}

func TestServerRejectsOlderRemoteSetVersion(t *testing.T) {
	_, groupName := newServerTestGroup(t, "old-set")
	s := &Server{}

	if _, err := s.Set(context.Background(), &cachepb.SetRequest{
		Group:    groupName,
		Key:      "k",
		Value:    []byte("new"),
		FromPeer: true,
		Version:  100,
		TtlMs:    int64((10 * time.Minute) / time.Millisecond),
	}); err != nil {
		t.Fatalf("Set(new) error = %v", err)
	}
	if _, err := s.Set(context.Background(), &cachepb.SetRequest{
		Group:    groupName,
		Key:      "k",
		Value:    []byte("old"),
		FromPeer: true,
		Version:  50,
		TtlMs:    int64((10 * time.Minute) / time.Millisecond),
	}); err != nil {
		t.Fatalf("Set(old) error = %v", err)
	}

	resp, err := s.Get(context.Background(), &cachepb.GetRequest{Group: groupName, Key: "k"})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(resp.Value) != "new" {
		t.Fatalf("Get() = %q, want %q", string(resp.Value), "new")
	}
}

func TestServerDeleteRequestUsesVersion(t *testing.T) {
	_, groupName := newServerTestGroup(t, "delete-version")
	s := &Server{}

	if _, err := s.Set(context.Background(), &cachepb.SetRequest{
		Group:    groupName,
		Key:      "k",
		Value:    []byte("value"),
		FromPeer: true,
		Version:  100,
		TtlMs:    int64((10 * time.Minute) / time.Millisecond),
	}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	oldResp, err := s.Delete(context.Background(), &cachepb.DeleteRequest{
		Group:    groupName,
		Key:      "k",
		FromPeer: true,
		Verison:  50,
	})
	if err != nil {
		t.Fatalf("Delete(old) error = %v", err)
	}
	if oldResp.Ok {
		t.Fatal("Delete(old) Ok = true, want false")
	}

	got, err := s.Get(context.Background(), &cachepb.GetRequest{Group: groupName, Key: "k"})
	if err != nil {
		t.Fatalf("Get() after old delete error = %v", err)
	}
	if string(got.Value) != "value" {
		t.Fatalf("Get() after old delete = %q, want %q", string(got.Value), "value")
	}

	newResp, err := s.Delete(context.Background(), &cachepb.DeleteRequest{
		Group:    groupName,
		Key:      "k",
		FromPeer: true,
		Verison:  200,
	})
	if err != nil {
		t.Fatalf("Delete(new) error = %v", err)
	}
	if !newResp.Ok {
		t.Fatal("Delete(new) Ok = false, want true")
	}
	if _, err := s.Get(context.Background(), &cachepb.GetRequest{Group: groupName, Key: "k"}); !errors.Is(err, cache.ErrKey) {
		t.Fatalf("Get() after new delete error = %v, want %v", err, cache.ErrKey)
	}
}

var serverGroupSeq int64

func newServerTestGroup(t *testing.T, name string) (*cache.Group, string) {
	t.Helper()
	id := atomic.AddInt64(&serverGroupSeq, 1)
	groupName := fmt.Sprintf("%s-%d", name, id)
	g := cache.NewGroup(groupName, 1024, cache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, cache.ErrKey
	}), id, cache.WithRetryQueue(newTestRetryQueue()))
	t.Cleanup(func() {
		_ = g.Close()
	})
	return g, groupName
}

type countingPeerPicker struct {
	writeCalls  atomic.Int64
	readCalls   atomic.Int64
	shadowCalls atomic.Int64
}

func (p *countingPeerPicker) PickWritePeer(key string) (cache.PeerGetter, bool, bool) {
	p.writeCalls.Add(1)
	return failingPeer{}, true, false
}

func (p *countingPeerPicker) PickReadPeer(key string) (cache.PeerGetter, bool, bool) {
	p.readCalls.Add(1)
	return failingPeer{}, true, false
}

func (p *countingPeerPicker) PickShadowPeer(key string) (cache.PeerGetter, bool, bool) {
	p.shadowCalls.Add(1)
	return failingPeer{}, true, false
}

type failingPeer struct{}

func (failingPeer) Get(ctx context.Context, group string, key string) ([]byte, error) {
	return nil, fmt.Errorf("unexpected peer get")
}

func (failingPeer) Set(ctx context.Context, group string, key string, value []byte, version int64, ttl time.Duration) error {
	return fmt.Errorf("unexpected peer set")
}

func (failingPeer) Delete(ctx context.Context, group string, key string, version int64) bool {
	return false
}

func (failingPeer) BatchSet(ctx context.Context, entries []*cachepb.CacheEntry) (bool, error) {
	return false, fmt.Errorf("unexpected peer batch set")
}

func (failingPeer) Scan(ctx context.Context, group string, key string, count int64) ([]*cachepb.CacheEntry, error) {
	return nil, fmt.Errorf("unexpected peer scan")
}
