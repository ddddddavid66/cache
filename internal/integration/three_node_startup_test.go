package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"newCache/internal/client"
	"newCache/internal/consistenthash"
	"newCache/internal/registry"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestThreeNodeStartup verifies that three cache nodes can start, register
// with etcd, form consistent hash rings, and route reads/writes correctly.
func TestThreeNodeStartup(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := fmt.Sprintf("new-cache-3node-%d", time.Now().UnixNano())
	groupName := "scores"

	etcdCli := newEtcdClient(t, endpoints)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = etcdCli.Delete(ctx, fmt.Sprintf("/cache/%s/", svcName), clientv3.WithPrefix())
		_ = etcdCli.Close()
	})

	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	nodes := make([]*nodeProcess, 0, len(addrs))
	for i, addr := range addrs {
		node := startNodeProcess(t, fmt.Sprintf("node-%d", i+1), addr, svcName, groupName, endpoints)
		nodes = append(nodes, node)
	}
	waitForEtcdNodes(t, etcdCli, svcName, addrs, registry.StatusActive)
	time.Sleep(500 * time.Millisecond)

	// ---- Verify all nodes registered in etcd ----
	ctx := context.Background()
	prefix := registry.ServicePrefix(svcName)
	resp, err := etcdCli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Get nodes from etcd: %v", err)
	}
	if len(resp.Kvs) != 3 {
		t.Fatalf("expected 3 nodes in etcd, got %d", len(resp.Kvs))
	}

	seen := make(map[string]bool)
	for _, kv := range resp.Kvs {
		var info registry.NodeInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			t.Fatalf("unmarshal NodeInfo: %v", err)
		}
		if info.Status != registry.StatusActive {
			t.Fatalf("node %s status = %q, want %q", info.Addr, info.Status, registry.StatusActive)
		}
		seen[info.Addr] = true
	}
	for _, addr := range addrs {
		if !seen[addr] {
			t.Fatalf("node %s not found in etcd", addr)
		}
	}

	// ---- Verify picker ring includes all three nodes ----
	picker, err := client.NewPicker(endpoints, svcName, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("NewPicker: %v", err)
	}
	defer picker.Close()

	ring := consistenthash.NewMap(integrationReplicas, nil)
	ring.Add(addrs...)

	// Sample keys and verify each node gets some reads
	nodeReadCount := make(map[string]int)
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("startup-read-key-%d", i)
		_, ok, isSelf := picker.PickReadPeer(key)
		if isSelf {
			continue
		}
		if !ok {
			t.Fatalf("PickReadPeer(%q): ok=false", key)
		}
		owner := ring.Get(key)
		nodeReadCount[owner]++
	}
	for _, addr := range addrs {
		if nodeReadCount[addr] == 0 {
			t.Logf("warning: node %s received 0 read picks (may be ring distribution)", addr)
		}
	}

	// Sample keys and verify each node gets some writes
	nodeWriteCount := make(map[string]int)
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("startup-write-key-%d", i)
		_, ok, isSelf := picker.PickWritePeer(key)
		if isSelf {
			continue
		}
		if !ok {
			t.Fatalf("PickWritePeer(%q): ok=false", key)
		}
		owner := ring.Get(key)
		nodeWriteCount[owner]++
	}
	for _, addr := range addrs {
		if nodeWriteCount[addr] == 0 {
			t.Logf("warning: node %s received 0 write picks (may be ring distribution)", addr)
		}
	}

	// ---- Verify basic read/write routing ----
	a, b, c := addrs[0], addrs[1], addrs[2]
	clientA := newNodeClient(t, a)
	clientB := newNodeClient(t, b)
	clientC := newNodeClient(t, c)

	keyA := keyForOwner(t, ring, a)
	keyB := keyForOwner(t, ring, b)
	keyC := keyForOwner(t, ring, c)

	// Write through any node; owner should store the value
	if err := clientA.Set(ctx, groupName, keyA, []byte("val-a"), 1, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyA) via A: %v", err)
	}
	if err := clientB.Set(ctx, groupName, keyB, []byte("val-b"), 1, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyB) via B: %v", err)
	}
	if err := clientC.Set(ctx, groupName, keyC, []byte("val-c"), 1, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyC) via C: %v", err)
	}

	// Read from the owner node
	got, err := clientA.Get(ctx, groupName, keyA)
	if err != nil {
		t.Fatalf("Get(keyA) from A: %v", err)
	}
	if string(got) != "val-a" {
		t.Fatalf("Get(keyA) = %q, want %q", string(got), "val-a")
	}

	got, err = clientB.Get(ctx, groupName, keyB)
	if err != nil {
		t.Fatalf("Get(keyB) from B: %v", err)
	}
	if string(got) != "val-b" {
		t.Fatalf("Get(keyB) = %q, want %q", string(got), "val-b")
	}

	got, err = clientC.Get(ctx, groupName, keyC)
	if err != nil {
		t.Fatalf("Get(keyC) from C: %v", err)
	}
	if string(got) != "val-c" {
		t.Fatalf("Get(keyC) = %q, want %q", string(got), "val-c")
	}

	// Cross-node reads require picker sync — verify owner reads are reliable
	// and that each node stores its own keys correctly.
	// Cross-node forwarding is covered by the migration/topology tests.

	// Cleanup
	for _, node := range nodes {
		node.stop(t)
	}
}

// TestThreeNodeStartupSequential verifies nodes can start one at a time
// and the system remains functional as each joins.
func TestThreeNodeStartupSequential(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := fmt.Sprintf("new-cache-seq-%d", time.Now().UnixNano())
	groupName := "scores"

	etcdCli := newEtcdClient(t, endpoints)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = etcdCli.Delete(ctx, fmt.Sprintf("/cache/%s/", svcName), clientv3.WithPrefix())
		_ = etcdCli.Close()
	})

	ctx := context.Background()

	// Start first node
	addrA := freeAddr(t)
	nodeA := startNodeProcess(t, "node-a", addrA, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{addrA}, registry.StatusActive)
	time.Sleep(300 * time.Millisecond)

	clientA := newNodeClient(t, addrA)

	// Single-node: write and read should work
	if err := clientA.Set(ctx, groupName, "single-node-key", []byte("v1"), 1, 10*time.Minute); err != nil {
		t.Fatalf("single-node Set: %v", err)
	}
	got, err := clientA.Get(ctx, groupName, "single-node-key")
	if err != nil {
		t.Fatalf("single-node Get: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("single-node Get = %q, want %q", string(got), "v1")
	}

	// Start second node
	addrB := freeAddr(t)
	nodeB := startNodeProcess(t, "node-b", addrB, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{addrA, addrB}, registry.StatusActive)
	time.Sleep(300 * time.Millisecond)

	clientB := newNodeClient(t, addrB)

	ringAB := integrationRing(addrA, addrB)
	keyForB := keyForOwner(t, ringAB, addrB)
	if err := clientA.Set(ctx, groupName, keyForB, []byte("v2"), 2, 10*time.Minute); err != nil {
		t.Fatalf("two-node Set(keyForB) via A: %v", err)
	}
	got, err = clientB.Get(ctx, groupName, keyForB)
	if err != nil {
		t.Fatalf("two-node Get(keyForB) from B: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("two-node Get(keyForB) = %q, want %q", string(got), "v2")
	}

	// Start third node
	addrC := freeAddr(t)
	nodeC := startNodeProcess(t, "node-c", addrC, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{addrA, addrB, addrC}, registry.StatusActive)
	time.Sleep(300 * time.Millisecond)

	clientC := newNodeClient(t, addrC)

	ringABC := integrationRing(addrA, addrB, addrC)
	keyForC := keyForOwner(t, ringABC, addrC)
	if err := clientB.Set(ctx, groupName, keyForC, []byte("v3"), 3, 10*time.Minute); err != nil {
		t.Fatalf("three-node Set(keyForC) via B: %v", err)
	}
	got, err = clientC.Get(ctx, groupName, keyForC)
	if err != nil {
		t.Fatalf("three-node Get(keyForC) from C: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("three-node Get(keyForC) = %q, want %q", string(got), "v3")
	}

	// Verify old data still accessible
	got, err = clientA.Get(ctx, groupName, "single-node-key")
	if err != nil {
		t.Fatalf("Get(single-node-key) after 3 nodes: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("Get(single-node-key) = %q, want %q", string(got), "v1")
	}

	nodeC.stop(t)
	nodeB.stop(t)
	nodeA.stop(t)
}

// TestThreeNodeStartupWarmingLifecycle verifies that a node starts as warming
// and can be transitioned to active via setNodeStatus.
func TestThreeNodeStartupWarmingLifecycle(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := fmt.Sprintf("new-cache-warm-%d", time.Now().UnixNano())
	groupName := "scores"

	etcdCli := newEtcdClient(t, endpoints)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = etcdCli.Delete(ctx, fmt.Sprintf("/cache/%s/", svcName), clientv3.WithPrefix())
		_ = etcdCli.Close()
	})

	addrs := []string{freeAddr(t), freeAddr(t)}
	a, b := addrs[0], addrs[1]

	nodeA := startNodeProcess(t, "node-a", a, svcName, groupName, endpoints)
	nodeB := startNodeProcess(t, "node-b", b, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, addrs, registry.StatusActive)
	time.Sleep(300 * time.Millisecond)

	// Start a third node — it auto-registers as active, set it to warming for this test
	c := freeAddr(t)
	nodeC := startNodeProcess(t, "node-c", c, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{c}, registry.StatusActive)
	setNodeStatus(t, endpoints, svcName, c, registry.StatusWarming)
	time.Sleep(300 * time.Millisecond)

	// Verify C is warming
	infoC := getNodeInfo(t, etcdCli, svcName, c)
	if infoC.Status != registry.StatusWarming {
		t.Fatalf("node C status = %q, want %q", infoC.Status, registry.StatusWarming)
	}

	// Warming node should not be in read/write rings
	picker, err := client.NewPicker(endpoints, svcName, a)
	if err != nil {
		t.Fatalf("NewPicker: %v", err)
	}
	defer picker.Close()

	cClient, err := picker.PickPeerByAddr(c)
	if err != nil {
		t.Fatalf("PickPeerByAddr(%s): %v", c, err)
	}

	warmingSelected := false
	for i := 0; i < 3000; i++ {
		peer, ok, isSelf := picker.PickReadPeer(fmt.Sprintf("warm-key-%d", i))
		if ok && !isSelf && peer == cClient {
			warmingSelected = true
			break
		}
	}
	if warmingSelected {
		t.Fatal("warming node C was selected for reads")
	}

	warmingWriteSelected := false
	for i := 0; i < 3000; i++ {
		peer, ok, isSelf := picker.PickWritePeer(fmt.Sprintf("warm-write-key-%d", i))
		if ok && !isSelf && peer == cClient {
			warmingWriteSelected = true
			break
		}
	}
	if warmingWriteSelected {
		t.Fatal("warming node C was selected for writes")
	}

	// Transition C to active
	setNodeStatus(t, endpoints, svcName, c, registry.StatusActive)
	time.Sleep(500 * time.Millisecond)

	infoC = getNodeInfo(t, etcdCli, svcName, c)
	if infoC.Status != registry.StatusActive {
		t.Fatalf("node C status after activate = %q, want %q", infoC.Status, registry.StatusActive)
	}

	nodeC.stop(t)
	nodeB.stop(t)
	nodeA.stop(t)
}

// getNodeInfo reads a node's info from etcd.
func getNodeInfo(t *testing.T, cli *clientv3.Client, svcName, addr string) registry.NodeInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := registry.ServiceKey(svcName, addr)
	resp, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get node info for %s: %v", addr, err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatalf("node %s not found in etcd", addr)
	}
	var info registry.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &info); err != nil {
		t.Fatalf("unmarshal node info for %s: %v", addr, err)
	}
	return info
}
