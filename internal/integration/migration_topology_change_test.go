package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cachepb "newCache/api/proto"
	"newCache/internal/consistenthash"
	"newCache/internal/registry"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMigrationTopologyChangeSecondPassPreservesData(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := fmt.Sprintf("new-cache-rebalance-it-%d", time.Now().UnixNano())
	groupName := "scores"

	etcdCli := newEtcdClient(t, endpoints)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = etcdCli.Delete(ctx, fmt.Sprintf("/cache/%s/", svcName), clientv3.WithPrefix())
		_ = etcdCli.Close()
	})

	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t), freeAddr(t)}
	a, b, c, d := addrs[0], addrs[1], addrs[2], addrs[3]

	nodeA := startNodeProcess(t, "node-a", a, svcName, groupName, endpoints)
	nodeB := startNodeProcess(t, "node-b", b, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{a, b}, registry.StatusActive)
	time.Sleep(500 * time.Millisecond)

	ringAB := integrationRing(a, b)
	ringABC := integrationRing(a, b, c)
	ringABCD := integrationRing(a, b, c, d)

	liveKey := keyForOwnerPath(t, []ownerStep{
		{ring: ringAB, owner: a},
		{ring: ringABC, owner: c},
		{ring: ringABCD, owner: d},
	}, "live")
	deletedKey := keyForOwnerPath(t, []ownerStep{
		{ring: ringAB, owner: a},
		{ring: ringABC, owner: c},
		{ring: ringABCD, owner: d},
	}, "deleted")

	clientA := newNodeClient(t, a)
	ctx := context.Background()
	if err := clientA.Set(ctx, groupName, liveKey, []byte("live-v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("Set(liveKey) through A error = %v", err)
	}
	if err := clientA.Set(ctx, groupName, deletedKey, []byte("deleted-v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("Set(deletedKey) through A error = %v", err)
	}
	if ok := clientA.Delete(ctx, groupName, deletedKey, 200); !ok {
		t.Fatal("Delete(deletedKey) through A = false, want true")
	}

	nodeC := startNodeProcess(t, "node-c", c, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{a, b, c}, registry.StatusActive)
	time.Sleep(500 * time.Millisecond)
	clientC := newNodeClient(t, c)

	entriesFromA := scanSelectedEntries(t, clientA, groupName, liveKey, deletedKey)
	ok, err := clientC.BatchSet(ctx, entriesFromA)
	if err != nil {
		t.Fatalf("C BatchSet(first pass) error = %v", err)
	}
	if !ok {
		t.Fatal("C BatchSet(first pass) ok = false, want true")
	}

	got, err := clientC.Get(ctx, groupName, liveKey)
	if err != nil {
		t.Fatalf("C Get(liveKey) after first pass error = %v", err)
	}
	if string(got) != "live-v100" {
		t.Fatalf("C Get(liveKey) = %q, want live-v100", got)
	}
	if _, err := clientC.Get(ctx, groupName, deletedKey); err == nil {
		t.Fatal("C Get(deletedKey) after first pass error = nil, want tombstone miss")
	}

	nodeD := startNodeProcess(t, "node-d", d, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, addrs, registry.StatusActive)
	time.Sleep(500 * time.Millisecond)
	clientD := newNodeClient(t, d)

	entriesFromC := scanSelectedEntries(t, clientC, groupName, liveKey, deletedKey)
	ok, err = clientD.BatchSet(ctx, entriesFromC)
	if err != nil {
		t.Fatalf("D BatchSet(second pass) error = %v", err)
	}
	if !ok {
		t.Fatal("D BatchSet(second pass) ok = false, want true")
	}

	got, err = clientD.Get(ctx, groupName, liveKey)
	if err != nil {
		t.Fatalf("D Get(liveKey) after second pass error = %v", err)
	}
	if string(got) != "live-v100" {
		t.Fatalf("D Get(liveKey) = %q, want live-v100", got)
	}

	if err := clientD.Set(ctx, groupName, liveKey, []byte("old-live-v50"), 50, 10*time.Minute); err != nil {
		t.Fatalf("D old Set(liveKey) error = %v", err)
	}
	got, err = clientD.Get(ctx, groupName, liveKey)
	if err != nil {
		t.Fatalf("D Get(liveKey) after old Set error = %v", err)
	}
	if string(got) != "live-v100" {
		t.Fatalf("D Get(liveKey) after old Set = %q, want live-v100", got)
	}

	if err := clientD.Set(ctx, groupName, deletedKey, []byte("resurrect-v150"), 150, 10*time.Minute); err != nil {
		t.Fatalf("D old Set(deletedKey) error = %v", err)
	}
	if _, err := clientD.Get(ctx, groupName, deletedKey); err == nil {
		t.Fatal("D Get(deletedKey) after old Set error = nil, want tombstone miss")
	}

	nodeD.stop(t)
	nodeC.stop(t)
	nodeB.stop(t)
	nodeA.stop(t)
}

type ownerStep struct {
	ring  *consistenthash.Map
	owner string
}

func integrationRing(addrs ...string) *consistenthash.Map {
	ring := consistenthash.NewMap(integrationReplicas, nil)
	ring.Add(addrs...)
	return ring
}

func keyForOwnerPath(t *testing.T, steps []ownerStep, label string) string {
	t.Helper()
	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("topology-%s-%d", label, i)
		matches := true
		for _, step := range steps {
			if step.ring.Get(key) != step.owner {
				matches = false
				break
			}
		}
		if matches {
			return key
		}
	}
	owners := make([]string, 0, len(steps))
	for _, step := range steps {
		owners = append(owners, step.owner)
	}
	t.Fatalf("no key found for owner path %s", strings.Join(owners, " -> "))
	return ""
}

func scanSelectedEntries(t *testing.T, cli interface {
	Scan(context.Context, string, string, int64) ([]*cachepb.CacheEntry, error)
}, groupName string, keys ...string) []*cachepb.CacheEntry {
	t.Helper()
	entries, err := cli.Scan(context.Background(), groupName, "", 0)
	if err != nil {
		t.Fatalf("Scan(%s) error = %v", groupName, err)
	}
	byKey := make(map[string]*cachepb.CacheEntry, len(entries))
	for _, entry := range entries {
		byKey[entry.Key] = entry
	}

	selected := make([]*cachepb.CacheEntry, 0, len(keys))
	for _, key := range keys {
		entry := byKey[key]
		if entry == nil {
			t.Fatalf("Scan(%s) missing key %q; scanned %d entries", groupName, key, len(entries))
		}
		selected = append(selected, entry)
	}
	return selected
}
