package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	cachepb "newCache/api/proto"
	"newCache/cache"
	"newCache/internal/client"
	"newCache/internal/consistenthash"
	"newCache/internal/registry"
	"newCache/internal/server"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const integrationReplicas = 50

func TestEtcdThreeNodesRoutingStateDeleteAndMigration(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := fmt.Sprintf("new-cache-it-%d", time.Now().UnixNano())
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

	ownerRing := consistenthash.NewMap(integrationReplicas, nil)
	ownerRing.Add(addrs...)
	ownerAddr := addrs[2]
	key := keyForOwner(t, ownerRing, ownerAddr)

	writerA := newNodeClient(t, addrs[0])
	writerB := newNodeClient(t, addrs[1])
	owner := newNodeClient(t, ownerAddr)

	ctx := context.Background()
	if err := writerA.Set(ctx, groupName, key, []byte("v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("writerA Set() error = %v", err)
	}
	got, err := owner.Get(ctx, groupName, key)
	if err != nil {
		t.Fatalf("owner Get() after routed Set error = %v", err)
	}
	if string(got) != "v100" {
		t.Fatalf("owner Get() = %q, want %q", string(got), "v100")
	}

	if err := writerB.Set(ctx, groupName, key, []byte("old"), 50, 10*time.Minute); err != nil {
		t.Fatalf("writerB old Set() error = %v", err)
	}
	got, err = owner.Get(ctx, groupName, key)
	if err != nil {
		t.Fatalf("owner Get() after old Set error = %v", err)
	}
	if string(got) != "v100" {
		t.Fatalf("owner Get() after old Set = %q, want %q", string(got), "v100")
	}

	if ok := writerA.Delete(ctx, groupName, key, 200); !ok {
		t.Fatal("writerA Delete() = false, want true")
	}
	if err := writerB.Set(ctx, groupName, key, []byte("resurrect"), 150, 10*time.Minute); err != nil {
		t.Fatalf("writerB resurrect Set() error = %v", err)
	}
	if _, err := owner.Get(ctx, groupName, key); err == nil {
		t.Fatal("owner Get() after delete + old Set error = nil, want missing key")
	}

	assertWarmingNodeNotRead(t, endpoints, svcName, addrs)
	assertDrainingNodeNotWrite(t, endpoints, svcName, addrs)

	migrationKey := keyForOwner(t, ownerRing, addrs[0])
	source := newNodeClient(t, addrs[0])
	target := newNodeClient(t, addrs[1])
	if err := source.Set(ctx, groupName, migrationKey, []byte("migrated"), 300, 10*time.Minute); err != nil {
		t.Fatalf("source Set(migrationKey) error = %v", err)
	}
	entries, err := source.Scan(ctx, groupName, "", 0)
	if err != nil {
		t.Fatalf("source Scan() error = %v", err)
	}
	selected := selectEntry(t, entries, migrationKey)
	ok, err := target.BatchSet(ctx, []*cachepb.CacheEntry{selected})
	if err != nil {
		t.Fatalf("target BatchSet() error = %v", err)
	}
	if !ok {
		t.Fatal("target BatchSet() ok = false, want true")
	}
	got, err = target.Get(ctx, groupName, migrationKey)
	if err != nil {
		t.Fatalf("target Get() after migration error = %v", err)
	}
	if string(got) != "migrated" {
		t.Fatalf("target Get() after migration = %q, want %q", string(got), "migrated")
	}

	for _, node := range nodes {
		node.stop(t)
	}
}

func TestIntegrationNodeProcess(t *testing.T) {
	if os.Getenv("NEWCACHE_INTEGRATION_NODE") != "1" {
		return
	}

	endpoints := strings.Split(os.Getenv("NEWCACHE_INTEGRATION_ETCD_ENDPOINTS"), ",")
	addr := os.Getenv("NEWCACHE_INTEGRATION_ADDR")
	svcName := os.Getenv("NEWCACHE_INTEGRATION_SERVICE")
	groupName := os.Getenv("NEWCACHE_INTEGRATION_GROUP")
	if addr == "" || svcName == "" || groupName == "" || len(endpoints) == 0 {
		t.Fatal("missing integration node environment")
	}

	srv, err := server.NewServer(addr, svcName, endpoints)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	ready := make(chan error, 1)
	go func() {
		if err := srv.StartWithReady(ready); err != nil {
			fmt.Fprintf(os.Stderr, "server StartWithReady() error = %v\n", err)
			os.Exit(2)
		}
	}()
	if err := <-ready; err != nil {
		t.Fatalf("server ready error = %v", err)
	}

	group := cache.NewGroup(groupName, 4<<20, cache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, cache.ErrKey
	}), srv.WorkID())
	picker, err := client.NewPicker(endpoints, svcName, addr)
	if err != nil {
		t.Fatalf("NewPicker() error = %v", err)
	}
	group.RegisterPeers(picker)
	setNodeStatus(t, endpoints, svcName, addr, registry.StatusActive)

	fmt.Printf("READY %s\n", addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = group.Close()
	_ = srv.Stop(ctx)
}

type nodeProcess struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
}

func startNodeProcess(t *testing.T, name, addr, svcName, groupName string, endpoints []string) *nodeProcess {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIntegrationNodeProcess$", "-test.v")
	cmd.Env = append(os.Environ(),
		"NEWCACHE_INTEGRATION_NODE=1",
		"NEWCACHE_INTEGRATION_ADDR="+addr,
		"NEWCACHE_INTEGRATION_SERVICE="+svcName,
		"NEWCACHE_INTEGRATION_GROUP="+groupName,
		"NEWCACHE_INTEGRATION_ETCD_ENDPOINTS="+strings.Join(endpoints, ","),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatalf("%s StdoutPipe() error = %v", name, err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("%s start error = %v", name, err)
	}

	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "READY "+addr) {
				ready <- nil
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ready <- err
			return
		}
		ready <- fmt.Errorf("%s exited before ready", name)
	}()

	select {
	case err := <-ready:
		if err != nil {
			cancel()
			_ = cmd.Wait()
			t.Fatalf("%s readiness error = %v", name, err)
		}
	case <-time.After(10 * time.Second):
		cancel()
		_ = cmd.Wait()
		t.Fatalf("%s did not become ready", name)
	}

	node := &nodeProcess{cmd: cmd, cancel: cancel}
	t.Cleanup(func() {
		node.stop(t)
	})
	return node
}

func (n *nodeProcess) stop(t *testing.T) {
	t.Helper()
	if n == nil || n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		n.cancel()
		_ = n.cmd.Process.Kill()
		<-done
	}
	n.cmd = nil
}

func requireEtcd(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("ETCD_ENDPOINTS")
	if raw == "" {
		raw = "http://127.0.0.1:2379"
	}
	endpoints := strings.Split(raw, ",")
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Skipf("etcd unavailable at %s: %v", raw, err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := cli.Get(ctx, "newcache-integration-healthcheck"); err != nil {
		t.Skipf("etcd unavailable at %s: %v", raw, err)
	}
	return endpoints
}

func newEtcdClient(t *testing.T, endpoints []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("new etcd client error = %v", err)
	}
	return cli
}

func setNodeStatus(t *testing.T, endpoints []string, svcName, addr string, status registry.NodeStatus) {
	t.Helper()
	cli := newEtcdClient(t, endpoints)
	defer cli.Close()
	info := registry.NodeInfo{Addr: addr, Status: status}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal NodeInfo error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, registry.ServiceKey(svcName, addr), string(data)); err != nil {
		t.Fatalf("put node status %s for %s error = %v", status, addr, err)
	}
}

func waitForEtcdNodes(t *testing.T, cli *clientv3.Client, svcName string, addrs []string, status registry.NodeStatus) {
	t.Helper()
	want := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		want[addr] = struct{}{}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := cli.Get(ctx, registry.ServicePrefix(svcName), clientv3.WithPrefix())
		cancel()
		if err == nil {
			seen := make(map[string]struct{})
			for _, kv := range resp.Kvs {
				var info registry.NodeInfo
				if err := json.Unmarshal(kv.Value, &info); err != nil {
					continue
				}
				if _, ok := want[info.Addr]; ok && info.Status == status {
					seen[info.Addr] = struct{}{}
				}
			}
			if len(seen) == len(want) {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d %s nodes in etcd", len(addrs), status)
}

func assertWarmingNodeNotRead(t *testing.T, endpoints []string, svcName string, addrs []string) {
	t.Helper()
	warming := addrs[1]
	setNodeStatus(t, endpoints, svcName, warming, registry.StatusWarming)
	picker, err := client.NewPicker(endpoints, svcName, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("NewPicker() for warming assertion error = %v", err)
	}
	defer picker.Close()
	warmingPeer, err := picker.PickPeerByAddr(warming)
	if err != nil {
		t.Fatalf("PickPeerByAddr(warming) error = %v", err)
	}
	for i := 0; i < 1000; i++ {
		peer, ok, isSelf := picker.PickReadPeer(fmt.Sprintf("read-key-%d", i))
		if ok && !isSelf && peer == warmingPeer {
			t.Fatalf("warming node %s was selected for read", warming)
		}
	}
	setNodeStatus(t, endpoints, svcName, warming, registry.StatusActive)
}

func assertDrainingNodeNotWrite(t *testing.T, endpoints []string, svcName string, addrs []string) {
	t.Helper()
	draining := addrs[1]
	setNodeStatus(t, endpoints, svcName, draining, registry.StatusDraining)
	picker, err := client.NewPicker(endpoints, svcName, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("NewPicker() for draining assertion error = %v", err)
	}
	defer picker.Close()
	drainingPeer, err := picker.PickPeerByAddr(draining)
	if err != nil {
		t.Fatalf("PickPeerByAddr(draining) error = %v", err)
	}
	for i := 0; i < 1000; i++ {
		peer, ok, isSelf := picker.PickWritePeer(fmt.Sprintf("write-key-%d", i))
		if ok && !isSelf && peer == drainingPeer {
			t.Fatalf("draining node %s was selected for write", draining)
		}
	}
	setNodeStatus(t, endpoints, svcName, draining, registry.StatusActive)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free addr error = %v", err)
	}
	defer lis.Close()
	return lis.Addr().String()
}

func keyForOwner(t *testing.T, ring *consistenthash.Map, addr string) string {
	t.Helper()
	for i := 0; i < 200000; i++ {
		key := fmt.Sprintf("integration-key-%s-%d", strings.ReplaceAll(addr, ":", "-"), i)
		if ring.Get(key) == addr {
			return key
		}
	}
	t.Fatalf("no key found for owner %s", addr)
	return ""
}

func newNodeClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	cli, err := client.NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient(%s) error = %v", addr, err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
	})
	return cli
}

func selectEntry(t *testing.T, entries []*cachepb.CacheEntry, key string) *cachepb.CacheEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.Key == key {
			return entry
		}
	}
	t.Fatalf("entry for key %q not found in scan result: %+v", key, entries)
	return nil
}
