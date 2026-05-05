package server

import (
	"context"
	"errors"
	cachepb "newCache/api/proto"
	"newCache/cache"
	"testing"
)

func TestServerBatchSetAppliesVersionAndTombstone(t *testing.T) {
	_, groupName := newServerTestGroup(t, "batch-set")
	s := &Server{}

	resp, err := s.BatchSet(context.Background(), &cachepb.BatchRequest{Entries: []*cachepb.CacheEntry{
		{Group: groupName, Key: "k", Value: []byte("new"), Version: 100},
		{Group: groupName, Key: "k", Value: []byte("old"), Version: 50},
	}})
	if err != nil {
		t.Fatalf("BatchSet() error = %v", err)
	}
	if !resp.Ok {
		t.Fatal("BatchSet() Ok = false, want true")
	}

	got, err := s.Get(context.Background(), &cachepb.GetRequest{Group: groupName, Key: "k"})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(got.Value) != "new" {
		t.Fatalf("Get() = %q, want %q", string(got.Value), "new")
	}

	resp, err = s.BatchSet(context.Background(), &cachepb.BatchRequest{Entries: []*cachepb.CacheEntry{
		{Group: groupName, Key: "k", Version: 200, Tombstone: true},
		{Group: groupName, Key: "k", Value: []byte("resurrect-old"), Version: 150},
	}})
	if err != nil {
		t.Fatalf("BatchSet(tombstone) error = %v", err)
	}
	if !resp.Ok {
		t.Fatal("BatchSet(tombstone) Ok = false, want true")
	}
	if _, err := s.Get(context.Background(), &cachepb.GetRequest{Group: groupName, Key: "k"}); !errors.Is(err, cache.ErrKey) {
		t.Fatalf("Get() after tombstone error = %v, want %v", err, cache.ErrKey)
	}
}

func TestServerScanCarriesMigrationFields(t *testing.T) {
	_, groupName := newServerTestGroup(t, "scan")
	s := &Server{}

	if _, err := s.Set(context.Background(), &cachepb.SetRequest{
		Group:   groupName,
		Key:     "a",
		Value:   []byte("value-a"),
		Version: 10,
		TtlMs:   120000,
	}); err != nil {
		t.Fatalf("Set(a) error = %v", err)
	}
	if _, err := s.Delete(context.Background(), &cachepb.DeleteRequest{
		Group:   groupName,
		Key:     "b",
		Verison: 20,
	}); err != nil {
		t.Fatalf("Delete(b) error = %v", err)
	}

	resp, err := s.Scan(context.Background(), &cachepb.ScanRequest{Group: groupName})
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2: %+v", len(resp.Entries), resp.Entries)
	}

	byKey := make(map[string]*cachepb.CacheEntry, len(resp.Entries))
	for _, entry := range resp.Entries {
		byKey[entry.Key] = entry
	}
	if got := byKey["a"]; got == nil || got.Group != groupName || string(got.Value) != "value-a" || got.Version != 10 || got.Tombstone || got.TtlMs <= 0 {
		t.Fatalf("scan entry a = %+v, want group/value/version/ttl", got)
	}
	if got := byKey["b"]; got == nil || got.Group != groupName || got.Version != 20 || !got.Tombstone || got.TtlMs <= 0 {
		t.Fatalf("scan entry b = %+v, want tombstone/version/ttl", got)
	}
}
