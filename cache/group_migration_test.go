package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	cachepb "newCache/api/proto"
	"testing"
	"time"
)

func TestGroupSetRoutesNonPeerWriteToOwner(t *testing.T) {
	g := newTestGroup(t, "owner-write", nil)
	peer := &recordingPeer{}
	picker := &staticPeerPicker{writePeer: peer, writeOK: true}
	g.RegisterPeers(picker)

	if err := g.Set(context.Background(), "k", []byte("value"), 42, 10*time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	if peer.setCalls != 1 {
		t.Fatalf("peer Set calls = %d, want 1", peer.setCalls)
	}
	if peer.group != g.name || peer.key != "k" || !bytes.Equal(peer.value, []byte("value")) {
		t.Fatalf("peer Set got group=%q key=%q value=%q", peer.group, peer.key, string(peer.value))
	}
	if peer.version != 42 {
		t.Fatalf("peer Set version = %d, want 42", peer.version)
	}
	if peer.ttl != 10*time.Minute {
		t.Fatalf("peer Set ttl = %v, want %v", peer.ttl, 10*time.Minute)
	}
	if _, ok := g.mainCache.GetEntry("k"); ok {
		t.Fatal("non-owner Set wrote local cache, want route-only owner write")
	}
}

func TestGroupSetWritesLocallyWhenOwnerIsSelf(t *testing.T) {
	g := newTestGroup(t, "owner-self", nil)
	picker := &staticPeerPicker{writeOK: true, writeSelf: true}
	g.RegisterPeers(picker)

	if err := g.Set(context.Background(), "k", []byte("value"), 42, 10*time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	entry, ok := g.mainCache.GetEntry("k")
	if !ok {
		t.Fatal("mainCache.GetEntry() ok = false, want true")
	}
	if entry.Version != 42 || entry.Value.String() != "value" || entry.Tombstone {
		t.Fatalf("entry = {version:%d value:%q tombstone:%v}, want version 42 value value non-tombstone",
			entry.Version, entry.Value.String(), entry.Tombstone)
	}
}

func TestGroupDeleteRoutesNonPeerWriteToOwner(t *testing.T) {
	g := newTestGroup(t, "owner-delete", nil)
	peer := &recordingPeer{deleteOK: true}
	picker := &staticPeerPicker{writePeer: peer, writeOK: true}
	g.RegisterPeers(picker)

	if ok := g.DeleteWithVersion(context.Background(), "k", 77); !ok {
		t.Fatal("DeleteWithVersion() = false, want true")
	}

	if peer.deleteCalls != 1 {
		t.Fatalf("peer Delete calls = %d, want 1", peer.deleteCalls)
	}
	if peer.group != g.name || peer.key != "k" || peer.version != 77 {
		t.Fatalf("peer Delete got group=%q key=%q version=%d", peer.group, peer.key, peer.version)
	}
	if _, ok := g.mainCache.GetEntry("k"); ok {
		t.Fatal("non-owner Delete wrote local tombstone, want route-only owner write")
	}
}

func TestGroupTrySyncSetEnqueuesRetryWithCopiedValue(t *testing.T) {
	peer := &recordingPeer{setErr: errors.New("set failed")}
	g := &Group{
		name:    "retry-set",
		peers:   &staticPeerPicker{shadowPeer: peer, shadowOK: true},
		retryCh: make(chan syncTask, 1),
		closeCh: make(chan struct{}),
	}

	value := []byte("value")
	g.trySyncSet("k", value, 101, 4*time.Second)
	value[0] = 'X'

	task := readRetryTask(t, g.retryCh)
	if task.option != syncSet || task.key != "k" || task.version != 101 || task.ttl != 4*time.Second || task.attempt != 1 {
		t.Fatalf("retry task = %+v, want set key/version/ttl/attempt", task)
	}
	if string(task.value) != "value" {
		t.Fatalf("retry task value = %q, want copied %q", string(task.value), "value")
	}
	if !peer.fromPeer {
		t.Fatal("shadow Set context is not marked from peer")
	}
}

func TestGroupTrySyncDeleteEnqueuesRetry(t *testing.T) {
	peer := &recordingPeer{deleteOK: false}
	g := &Group{
		name:    "retry-delete",
		peers:   &staticPeerPicker{shadowPeer: peer, shadowOK: true},
		retryCh: make(chan syncTask, 1),
		closeCh: make(chan struct{}),
	}

	g.trySyncDelete("k", 202)

	task := readRetryTask(t, g.retryCh)
	if task.option != syncDelete || task.key != "k" || task.version != 202 || task.attempt != 1 {
		t.Fatalf("retry task = %+v, want delete key/version/attempt", task)
	}
	if !peer.fromPeer {
		t.Fatal("shadow Delete context is not marked from peer")
	}
}

func TestGroupBatchSetAppliesVersionAndTombstone(t *testing.T) {
	g := newTestGroup(t, "batch-version", nil)

	err := g.BatchSet(context.Background(), []TransportEntry{
		{Key: "k", Value: []byte("new"), Version: 100},
		{Key: "k", Value: []byte("old"), Version: 50},
	})
	if err != nil {
		t.Fatalf("BatchSet() error = %v", err)
	}

	got, err := g.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.String() != "new" {
		t.Fatalf("Get() = %q, want %q", got.String(), "new")
	}

	err = g.BatchSet(context.Background(), []TransportEntry{
		{Key: "k", Version: 200, Tombstone: true},
		{Key: "k", Value: []byte("resurrect-old"), Version: 150},
	})
	if err != nil {
		t.Fatalf("BatchSet(tombstone) error = %v", err)
	}
	if _, err := g.Get(context.Background(), "k"); !errors.Is(err, ErrKey) {
		t.Fatalf("Get() after tombstone error = %v, want %v", err, ErrKey)
	}
}

func TestGroupBatchSetPreservesTTLMilliseconds(t *testing.T) {
	g := newTestGroup(t, "batch-ttl", nil)

	if err := g.BatchSet(context.Background(), []TransportEntry{
		{Key: "k", Value: []byte("value"), Version: 1, TtlMs: int64((10 * time.Minute) / time.Millisecond)},
	}); err != nil {
		t.Fatalf("BatchSet() error = %v", err)
	}

	entry, ok := g.mainCache.GetEntry("k")
	if !ok {
		t.Fatal("mainCache.GetEntry() ok = false, want true")
	}
	remaining := time.Until(entry.ExpiredAt)
	if remaining < 8*time.Minute {
		t.Fatalf("BatchSet TTL remaining = %v, want roughly 10m", remaining)
	}
}

func TestGroupScanIncludesMigrationFields(t *testing.T) {
	g := newTestGroup(t, "scan", nil)

	if err := g.Set(context.Background(), "a", []byte("value-a"), 10, 2*time.Minute); err != nil {
		t.Fatalf("Set(a) error = %v", err)
	}
	if ok := g.DeleteWithVersion(context.Background(), "b", 20); !ok {
		t.Fatal("DeleteWithVersion(b) = false, want true")
	}

	entries, err := g.Scan("", 0)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2: %+v", len(entries), entries)
	}

	byKey := make(map[string]*TransportEntry, len(entries))
	for _, entry := range entries {
		byKey[entry.Key] = entry
	}
	if got := byKey["a"]; got == nil || got.Group != g.name || string(got.Value) != "value-a" || got.Version != 10 || got.Tombstone || got.TtlMs <= 0 {
		t.Fatalf("scan entry a = %+v, want group/value/version/ttl", got)
	}
	if got := byKey["b"]; got == nil || got.Group != g.name || got.Version != 20 || !got.Tombstone || got.TtlMs <= 0 {
		t.Fatalf("scan entry b = %+v, want tombstone/version/ttl", got)
	}
}

func TestGroupMigrationScanBatchSetKeepsKeyReadable(t *testing.T) {
	source := newTestGroup(t, "migration-source", nil)
	target := newTestGroup(t, "migration-target", nil)

	if err := source.Set(context.Background(), "live", []byte("value"), 100, 10*time.Minute); err != nil {
		t.Fatalf("source Set(live) error = %v", err)
	}
	if err := source.Set(context.Background(), "deleted", []byte("old"), 100, 10*time.Minute); err != nil {
		t.Fatalf("source Set(deleted) error = %v", err)
	}
	if ok := source.DeleteWithVersion(context.Background(), "deleted", 200); !ok {
		t.Fatal("source DeleteWithVersion(deleted) = false, want true")
	}

	scanned, err := source.Scan("", 0)
	if err != nil {
		t.Fatalf("source Scan() error = %v", err)
	}
	entries := make([]TransportEntry, 0, len(scanned))
	for _, entry := range scanned {
		entries = append(entries, *entry)
	}
	if err := target.BatchSet(context.Background(), entries); err != nil {
		t.Fatalf("target BatchSet() error = %v", err)
	}

	got, err := target.Get(context.Background(), "live")
	if err != nil {
		t.Fatalf("target Get(live) error = %v", err)
	}
	if got.String() != "value" {
		t.Fatalf("target Get(live) = %q, want %q", got.String(), "value")
	}

	if err := target.Set(context.Background(), "deleted", []byte("resurrect"), 150, 10*time.Minute); err != nil {
		t.Fatalf("target old Set(deleted) error = %v", err)
	}
	if _, err := target.Get(context.Background(), "deleted"); !errors.Is(err, ErrKey) {
		t.Fatalf("target Get(deleted) error = %v, want %v", err, ErrKey)
	}
	entry, ok := target.mainCache.GetEntry("deleted")
	if !ok {
		t.Fatal("target deleted tombstone missing after migration")
	}
	if !entry.Tombstone || entry.Version != 200 {
		t.Fatalf("target deleted entry = {tombstone:%v version:%d}, want tombstone version 200", entry.Tombstone, entry.Version)
	}
}

func readRetryTask(t *testing.T, ch <-chan syncTask) syncTask {
	t.Helper()
	select {
	case task := <-ch:
		return task
	default:
		t.Fatal("retry task was not enqueued")
		return syncTask{}
	}
}

type staticPeerPicker struct {
	writePeer  PeerGetter
	writeOK    bool
	writeSelf  bool
	shadowPeer PeerGetter
	shadowOK   bool
	shadowSelf bool
}

func (p *staticPeerPicker) PickWritePeer(key string) (PeerGetter, bool, bool) {
	return p.writePeer, p.writeOK, p.writeSelf
}

func (p *staticPeerPicker) PickReadPeer(key string) (PeerGetter, bool, bool) {
	return nil, false, false
}

func (p *staticPeerPicker) PickShadowPeer(key string) (PeerGetter, bool, bool) {
	return p.shadowPeer, p.shadowOK, p.shadowSelf
}

type recordingPeer struct {
	group       string
	key         string
	value       []byte
	version     int64
	ttl         time.Duration
	fromPeer    bool
	setCalls    int
	deleteCalls int
	deleteOK    bool
	setErr      error
}

func (p *recordingPeer) Get(ctx context.Context, group string, key string) ([]byte, error) {
	return nil, fmt.Errorf("unexpected Get")
}

func (p *recordingPeer) Set(ctx context.Context, group string, key string, value []byte, version int64, ttl time.Duration) error {
	p.setCalls++
	p.group = group
	p.key = key
	p.value = append([]byte(nil), value...)
	p.version = version
	p.ttl = ttl
	p.fromPeer = IsPeer(ctx)
	return p.setErr
}

func (p *recordingPeer) Delete(ctx context.Context, group string, key string, version int64) bool {
	p.deleteCalls++
	p.group = group
	p.key = key
	p.version = version
	p.fromPeer = IsPeer(ctx)
	return p.deleteOK
}

func (p *recordingPeer) BatchSet(ctx context.Context, entries []*cachepb.CacheEntry) (bool, error) {
	return false, fmt.Errorf("unexpected BatchSet")
}

func (p *recordingPeer) Scan(ctx context.Context, group string, key string, count int64) ([]*cachepb.CacheEntry, error) {
	return nil, fmt.Errorf("unexpected Scan")
}
