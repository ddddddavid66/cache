package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cachepb "newCache/api/proto"
	"newCache/internal/registry"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestRebalanceABScenario covers the full A/B migration flow:
//
//  1. A and B are active nodes with data.
//  2. C joins as warming; A starts epoch=1, migrates data to C.
//  3. D joins as warming while epoch=1 is in progress.
//  4. D's TryStart fails; D marks pending=true.
//  5. A finishes epoch=1, sees pending=true, startNext=true.
//  6. A starts epoch=2 with D included, migrates data to D.
//  7. epoch=2 completes, pending=false.
//  8. Version conflict: old writes rejected after migration.
//  9. Tombstone: deleted keys not resurrected by old writes.
func TestRebalanceABScenario(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := fmt.Sprintf("new-cache-ab-%d", time.Now().UnixNano())
	groupName := "scores"

	etcdCli := newEtcdClient(t, endpoints)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = etcdCli.Delete(ctx, fmt.Sprintf("/cache/%s/", svcName), clientv3.WithPrefix())
		_ = etcdCli.Close()
	})

	// ---- Phase 0: Start nodes A and B (both active) ----
	addrs := []string{freeAddr(t), freeAddr(t)}
	a, b := addrs[0], addrs[1]

	nodeA := startNodeProcess(t, "node-a", a, svcName, groupName, endpoints)
	nodeB := startNodeProcess(t, "node-b", b, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{a, b}, registry.StatusActive)
	time.Sleep(500 * time.Millisecond)

	ringAB := integrationRing(a, b)

	keyForA := keyForOwner(t, ringAB, a)
	keyForB := keyForOwner(t, ringAB, b)

	clientA := newNodeClient(t, a)
	clientB := newNodeClient(t, b)
	ctx := context.Background()

	if err := clientA.Set(ctx, groupName, keyForA, []byte("value-a-v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyForA) on A: %v", err)
	}
	if err := clientB.Set(ctx, groupName, keyForB, []byte("value-b-v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyForB) on B: %v", err)
	}

	// Write bulk keys so some will map to C in the ABC ring
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("ab-key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		cli := clientA
		if ringAB.Get(key) == b {
			cli = clientB
		}
		if err := cli.Set(ctx, groupName, key, []byte(val), int64(i+1), 10*time.Minute); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	got, err := clientA.Get(ctx, groupName, keyForA)
	if err != nil {
		t.Fatalf("Get(keyForA): %v", err)
	}
	if string(got) != "value-a-v100" {
		t.Fatalf("Get(keyForA) = %q, want %q", string(got), "value-a-v100")
	}

	// ---- Phase 1: Node C joins ----
	c := freeAddr(t)
	nodeC := startNodeProcess(t, "node-c", c, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{c}, registry.StatusActive)
	time.Sleep(300 * time.Millisecond)

	ringABC := integrationRing(a, b, c)

	// ---- Phase 2: A starts epoch=1 migration ----
	state1, ok, err := tryStartViaEtcd(t, etcdCli, svcName, a)
	if err != nil {
		t.Fatalf("A TryStart epoch=1: %v", err)
	}
	if !ok {
		t.Fatal("A TryStart epoch=1: expected ok=true")
	}

	// Migrate from A to C
	clientC := newNodeClient(t, c)
	entriesA := scanFromClient(t, clientA, groupName)
	migratedToC := batchSetToTarget(t, clientC, entriesA, ringABC, c)

	// Migrate from B to C
	entriesB := scanFromClient(t, clientB, groupName)
	migratedToC += batchSetToTarget(t, clientC, entriesB, ringABC, c)
	t.Logf("epoch=1: migrated %d entries to C", migratedToC)

	// ---- Phase 3: Node D joins, TryStart fails, marks pending ----
	d := freeAddr(t)
	nodeD := startNodeProcess(t, "node-d", d, svcName, groupName, endpoints)
	waitForEtcdNodes(t, etcdCli, svcName, []string{d}, registry.StatusActive)
	time.Sleep(300 * time.Millisecond)

	_, ok, err = tryStartViaEtcd(t, etcdCli, svcName, d)
	if err != nil {
		t.Fatalf("D TryStart: %v", err)
	}
	if ok {
		t.Fatal("D TryStart: expected ok=false while A is migrating")
	}

	if err := markPendingViaEtcd(t, etcdCli, svcName); err != nil {
		t.Fatalf("D MarkPending: %v", err)
	}

	// ---- Phase 4: A finishes epoch=1, sees pending=true ----
	startNext, err := finishViaEtcd(t, etcdCli, svcName, a, state1.Epoch)
	if err != nil {
		t.Fatalf("A Finish epoch=1: %v", err)
	}
	if !startNext {
		t.Fatal("A Finish epoch=1: expected startNext=true (pending detected)")
	}

	// ---- Phase 5: A starts epoch=2 with D included ----
	time.Sleep(300 * time.Millisecond)

	ringABCD := integrationRing(a, b, c, d)

	state2, ok, err := tryStartViaEtcd(t, etcdCli, svcName, a)
	if err != nil {
		t.Fatalf("A TryStart epoch=2: %v", err)
	}
	if !ok {
		t.Fatal("A TryStart epoch=2: expected ok=true")
	}
	if state2.Epoch != 2 {
		t.Fatalf("A epoch=2: got %d", state2.Epoch)
	}

	// Migrate from A, B, C to D
	clientD := newNodeClient(t, d)
	entriesA2 := scanFromClient(t, clientA, groupName)
	migratedToD := batchSetToTarget(t, clientD, entriesA2, ringABCD, d)

	entriesB2 := scanFromClient(t, clientB, groupName)
	migratedToD += batchSetToTarget(t, clientD, entriesB2, ringABCD, d)

	entriesC := scanFromClient(t, clientC, groupName)
	migratedToD += batchSetToTarget(t, clientD, entriesC, ringABCD, d)
	t.Logf("epoch=2: migrated %d entries to D", migratedToD)

	// ---- Phase 6: A finishes epoch=2, pending=false ----
	startNext2, err := finishViaEtcd(t, etcdCli, svcName, a, state2.Epoch)
	if err != nil {
		t.Fatalf("A Finish epoch=2: %v", err)
	}
	if startNext2 {
		t.Fatal("A Finish epoch=2: expected startNext=false")
	}

	// ---- Phase 7: Verify migrated data on D ----
	keyVerify := keyForOwnerPath(t, []ownerStep{
		{ring: ringAB, owner: a},
		{ring: ringABCD, owner: d},
	}, "verify")

	// Write to A (owner under AB ring), then migrate to D
	if err := clientA.Set(ctx, groupName, keyVerify, []byte("verify-v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyVerify) on A: %v", err)
	}

	// Scan A and migrate the verify key to D
	entriesForVerify := scanFromClient(t, clientA, groupName)
	for _, entry := range entriesForVerify {
		if entry.Key == keyVerify && ringABCD.Get(entry.Key) == d {
			if _, err := clientD.BatchSet(ctx, []*cachepb.CacheEntry{entry}); err != nil {
				t.Fatalf("BatchSet verify key to D: %v", err)
			}
		}
	}

	got, err = clientD.Get(ctx, groupName, keyVerify)
	if err != nil {
		t.Fatalf("D Get(keyVerify): %v", err)
	}
	if string(got) != "verify-v100" {
		t.Fatalf("D Get(keyVerify) = %q, want %q", string(got), "verify-v100")
	}

	// ---- Phase 8: Version conflict — old write rejected ----
	if err := clientD.Set(ctx, groupName, keyVerify, []byte("old-v50"), 50, 10*time.Minute); err != nil {
		t.Fatalf("D old Set(keyVerify): %v", err)
	}
	got, err = clientD.Get(ctx, groupName, keyVerify)
	if err != nil {
		t.Fatalf("D Get(keyVerify) after old Set: %v", err)
	}
	if string(got) != "verify-v100" {
		t.Fatalf("D Get(keyVerify) after old Set = %q, want %q (version conflict)", string(got), "verify-v100")
	}

	// ---- Phase 9: Tombstone prevents resurrection ----
	keyTomb := keyForOwnerPath(t, []ownerStep{
		{ring: ringAB, owner: a},
		{ring: ringABCD, owner: d},
	}, "tomb")

	if err := clientA.Set(ctx, groupName, keyTomb, []byte("tomb-v100"), 100, 10*time.Minute); err != nil {
		t.Fatalf("Set(keyTomb) on A: %v", err)
	}
	if ok := clientA.Delete(ctx, groupName, keyTomb, 200); !ok {
		t.Fatal("Delete(keyTomb) on A = false")
	}

	// Migrate the tombstone to D
	entriesForTomb := scanFromClient(t, clientA, groupName)
	for _, entry := range entriesForTomb {
		if entry.Key == keyTomb {
			_, _ = clientD.BatchSet(ctx, []*cachepb.CacheEntry{entry})
		}
	}

	// Old version write should not resurrect
	if err := clientD.Set(ctx, groupName, keyTomb, []byte("resurrect"), 150, 10*time.Minute); err != nil {
		t.Fatalf("D resurrect Set(keyTomb): %v", err)
	}
	if _, err := clientD.Get(ctx, groupName, keyTomb); err == nil {
		t.Fatal("D Get(keyTomb) after tombstone + old Set: expected miss, got hit")
	}

	nodeD.stop(t)
	nodeC.stop(t)
	nodeB.stop(t)
	nodeA.stop(t)
}

// ---------------------------------------------------------------------------
// Helpers: etcd-based state machine operations (mirror rebalance package)
// ---------------------------------------------------------------------------

type rebalanceState struct {
	State          string `json:"state"`
	Epoch          int64  `json:"epoch"`
	Pending        bool   `json:"pending"`
	Owner          string `json:"owner"`
	UpdateAtUnixMs int64  `json:"update_at_unix_ms"`
}

func rebalanceStateKey(svcName string) string {
	return fmt.Sprintf("/cache/%s/rebalance/state", svcName)
}

func rebalanceEpochKey(svcName string) string {
	return fmt.Sprintf("/cache/%s/rebalance/epoch", svcName)
}

func tryStartViaEtcd(t *testing.T, cli *clientv3.Client, svcName, selfAddr string) (rebalanceState, bool, error) {
	t.Helper()
	ctx := context.Background()

	stateResp, err := cli.Get(ctx, rebalanceStateKey(svcName))
	if err != nil {
		return rebalanceState{}, false, err
	}
	if len(stateResp.Kvs) == 0 {
	 initState := rebalanceState{
			State:          "stable",
			Epoch:          0,
			Pending:        false,
			UpdateAtUnixMs: time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(initState)
		_, err = cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(rebalanceStateKey(svcName)), "=", 0)).
			Then(
				clientv3.OpPut(rebalanceStateKey(svcName), string(data)),
				clientv3.OpPut(rebalanceEpochKey(svcName), "0"),
			).Commit()
		if err != nil {
			return rebalanceState{}, false, err
		}
		stateResp, err = cli.Get(ctx, rebalanceStateKey(svcName))
		if err != nil {
			return rebalanceState{}, false, err
		}
	}

	var old rebalanceState
	if err := json.Unmarshal(stateResp.Kvs[0].Value, &old); err != nil {
		return rebalanceState{}, false, err
	}
	if old.State != "stable" {
		return rebalanceState{}, false, nil
	}

	epochResp, err := cli.Get(ctx, rebalanceEpochKey(svcName))
	if err != nil {
		return rebalanceState{}, false, err
	}
	var nextEpoch int64 = 1
	if len(epochResp.Kvs) > 0 {
		fmt.Sscanf(string(epochResp.Kvs[0].Value), "%d", &nextEpoch)
		nextEpoch++
	}

	newState := rebalanceState{
		State:          "migrating",
		Epoch:          nextEpoch,
		Pending:        false,
		Owner:          selfAddr,
		UpdateAtUnixMs: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(newState)

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(rebalanceEpochKey(svcName)), "=", epochResp.Kvs[0].ModRevision)).
		Then(
			clientv3.OpPut(rebalanceStateKey(svcName), string(data)),
			clientv3.OpPut(rebalanceEpochKey(svcName), fmt.Sprintf("%d", nextEpoch)),
		).Commit()
	if err != nil {
		return rebalanceState{}, false, err
	}
	return newState, resp.Succeeded, nil
}

func markPendingViaEtcd(t *testing.T, cli *clientv3.Client, svcName string) error {
	t.Helper()
	ctx := context.Background()

	resp, err := cli.Get(ctx, rebalanceStateKey(svcName))
	if err != nil {
		return err
	}
	var state rebalanceState
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return err
	}

	state.Pending = true
	state.UpdateAtUnixMs = time.Now().UnixMilli()
	data, _ := json.Marshal(state)

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(rebalanceStateKey(svcName)), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(rebalanceStateKey(svcName), string(data))).Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("mark pending CAS failed")
	}
	return nil
}

func finishViaEtcd(t *testing.T, cli *clientv3.Client, svcName, selfAddr string, epoch int64) (startNext bool, err error) {
	t.Helper()
	ctx := context.Background()

	resp, err := cli.Get(ctx, rebalanceStateKey(svcName))
	if err != nil {
		return false, err
	}
	var state rebalanceState
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return false, err
	}

	if state.Owner != selfAddr {
		return false, fmt.Errorf("owner mismatch: %s != %s", state.Owner, selfAddr)
	}
	if state.Epoch != epoch {
		return false, fmt.Errorf("epoch mismatch: %d != %d", state.Epoch, epoch)
	}

	startNext = state.Pending
	state.State = "stable"
	state.Pending = false
	state.Owner = ""
	state.UpdateAtUnixMs = time.Now().UnixMilli()
	data, _ := json.Marshal(state)

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(rebalanceStateKey(svcName)), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(rebalanceStateKey(svcName), string(data))).Commit()
	if err != nil {
		return false, err
	}
	if !txnResp.Succeeded {
		return false, fmt.Errorf("finish CAS failed")
	}
	return startNext, nil
}

// ---------------------------------------------------------------------------
// Helpers: scan and batch-set with ring filtering
// ---------------------------------------------------------------------------

// scanFromClient scans all entries from a node for the given group.
func scanFromClient(t *testing.T, cli interface {
	Scan(context.Context, string, string, int64) ([]*cachepb.CacheEntry, error)
}, groupName string) []*cachepb.CacheEntry {
	t.Helper()
	entries, err := cli.Scan(context.Background(), groupName, "", 0)
	if err != nil {
		t.Fatalf("Scan(%s): %v", groupName, err)
	}
	return entries
}

// batchSetToTarget filters entries by ring ownership and sends them to the
// target node via BatchSet. Returns the count of entries sent.
func batchSetToTarget(t *testing.T, target interface {
	BatchSet(context.Context, []*cachepb.CacheEntry) (bool, error)
}, entries []*cachepb.CacheEntry, ring interface{ Get(string) string }, targetAddr string) int {
	t.Helper()
	ctx := context.Background()
	filtered := make([]*cachepb.CacheEntry, 0)
	for _, entry := range entries {
		if ring.Get(entry.Key) == targetAddr {
			filtered = append(filtered, entry)
		}
	}
	if len(filtered) == 0 {
		return 0
	}
	ok, err := target.BatchSet(ctx, filtered)
	if err != nil {
		t.Fatalf("BatchSet to %s: %v", targetAddr, err)
	}
	if !ok {
		t.Fatalf("BatchSet to %s: ok=false", targetAddr)
	}
	return len(filtered)
}
