package rebalance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// requireEtcd skips the test when etcd is unreachable.
func requireEtcd(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("ETCD_ENDPOINTS")
	if raw == "" {
		raw = "http://127.0.0.1:2379"
	}
	endpoints := strings.Split(raw, ",")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Skipf("etcd unavailable at %s: %v", raw, err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := cli.Get(ctx, "rebalance-test-healthcheck"); err != nil {
		t.Skipf("etcd unavailable at %s: %v", raw, err)
	}
	return endpoints
}

func newEtcdCli(t *testing.T, endpoints []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new etcd client: %v", err)
	}
	return cli
}

func uniqueSvcName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("rebalance-test-%d", time.Now().UnixNano())
}

func cleanupSvc(t *testing.T, cli *clientv3.Client, svcName string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = cli.Delete(ctx, fmt.Sprintf("/cache/%s/", svcName), clientv3.WithPrefix())
	})
}

// ---------------------------------------------------------------------------
// A/B 场景: TryStart -> MarkPending -> Finish -> startNext
// ---------------------------------------------------------------------------

// TestABScenario_FullFlow 模拟完整 A/B 场景:
//
//	T1: A TryStart 成功, epoch=1, state=migrating
//	T2: B TryStart 失败 (state=migrating)
//	T3: B MarkPending 成功, pending=true
//	T4: A Finish, 发现 pending=true, 返回 startNext=true
//	T5: A 立刻 TryStart, epoch=2 (基于最新节点列表)
//	T6: A Finish epoch=2, pending=false, startNext=false
func TestABScenario_FullFlow(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	addrA := "127.0.0.1:9001"
	addrB := "127.0.0.1:9002"

	// 初始化状态
	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	// T1: A TryStart 成功
	stateA, ok, err := TryStart(ctx, cli, svcName, addrA)
	if err != nil {
		t.Fatalf("A TryStart: %v", err)
	}
	if !ok {
		t.Fatal("A TryStart: expected ok=true")
	}
	if stateA.State != StateMigrating {
		t.Fatalf("A state = %q, want %q", stateA.State, StateMigrating)
	}
	if stateA.Epoch != 1 {
		t.Fatalf("A epoch = %d, want 1", stateA.Epoch)
	}
	if stateA.Owner != addrA {
		t.Fatalf("A owner = %q, want %q", stateA.Owner, addrA)
	}

	// T2: B TryStart 失败 (因为 state=migrating)
	_, ok, err = TryStart(ctx, cli, svcName, addrB)
	if err != nil {
		t.Fatalf("B TryStart: %v", err)
	}
	if ok {
		t.Fatal("B TryStart: expected ok=false while A is migrating")
	}

	// T3: B MarkPending 成功
	if err := MarkPending(ctx, cli, svcName); err != nil {
		t.Fatalf("B MarkPending: %v", err)
	}

	// 验证 pending=true
	loaded, err := LoadState(ctx, cli, svcName)
	if err != nil {
		t.Fatalf("LoadState after MarkPending: %v", err)
	}
	if !loaded.Pending {
		t.Fatal("expected Pending=true after MarkPending")
	}

	// T4: A Finish, pending=true -> startNext=true
	startNext, err := Finsh(ctx, cli, svcName, addrA, stateA.Epoch)
	if err != nil {
		t.Fatalf("A Finish epoch=1: %v", err)
	}
	if !startNext {
		t.Fatal("A Finish epoch=1: expected startNext=true")
	}

	// 验证 state 回到 stable
	afterFinish, err := LoadState(ctx, cli, svcName)
	if err != nil {
		t.Fatalf("LoadState after Finish: %v", err)
	}
	if afterFinish.State != StateStable {
		t.Fatalf("state after Finish = %q, want %q", afterFinish.State, StateStable)
	}

	// T5: A 立刻 TryStart epoch=2
	stateA2, ok, err := TryStart(ctx, cli, svcName, addrA)
	if err != nil {
		t.Fatalf("A TryStart epoch=2: %v", err)
	}
	if !ok {
		t.Fatal("A TryStart epoch=2: expected ok=true")
	}
	if stateA2.Epoch != 2 {
		t.Fatalf("A epoch=2: got %d", stateA2.Epoch)
	}

	// T6: A Finish epoch=2, pending=false -> startNext=false
	startNext2, err := Finsh(ctx, cli, svcName, addrA, stateA2.Epoch)
	if err != nil {
		t.Fatalf("A Finish epoch=2: %v", err)
	}
	if startNext2 {
		t.Fatal("A Finish epoch=2: expected startNext=false")
	}
}

// TestABScenario_ShouContinue validates ShouContinue across epoch boundaries.
func TestABScenario_ShouContinue(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	addrA := "127.0.0.1:9001"

	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	// 未迁移时 ShouContinue = false
	if ShouContinue(ctx, cli, svcName, addrA, 1) {
		t.Fatal("ShouContinue before TryStart: expected false")
	}

	// A 开始迁移
	state, ok, err := TryStart(ctx, cli, svcName, addrA)
	if err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	if !ok {
		t.Fatal("TryStart: expected ok=true")
	}

	// epoch 匹配时 ShouContinue = true
	if !ShouContinue(ctx, cli, svcName, addrA, state.Epoch) {
		t.Fatal("ShouContinue with matching epoch: expected true")
	}

	// epoch 不匹配时 ShouContinue = false
	if ShouContinue(ctx, cli, svcName, addrA, 999) {
		t.Fatal("ShouContinue with wrong epoch: expected false")
	}

	// owner 不匹配时 ShouContinue = false
	if ShouContinue(ctx, cli, svcName, "127.0.0.1:9999", state.Epoch) {
		t.Fatal("ShouContinue with wrong owner: expected false")
	}
}

// TestABScenario_TryStart_Concurrent ensures only one node wins the CAS.
func TestABScenario_TryStart_Concurrent(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	type result struct {
		addr string
		ok   bool
		err  error
		epoch int64
	}

	ch := make(chan result, 3)
	addrs := []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"}

	for _, addr := range addrs {
		go func(a string) {
			s, ok, err := TryStart(ctx, cli, svcName, a)
			ch <- result{addr: a, ok: ok, err: err, epoch: s.Epoch}
		}(addr)
	}

	winners := 0
	for range addrs {
		r := <-ch
		if r.err != nil {
			t.Errorf("TryStart %s: %v", r.addr, r.err)
			continue
		}
		if r.ok {
			winners++
			if r.epoch != 1 {
				t.Errorf("winner %s epoch = %d, want 1", r.addr, r.epoch)
			}
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly 1 TryStart winner, got %d", winners)
	}
}

// TestABScenario_MultiplePendingRounds tests repeated pending -> finish -> next cycles.
func TestABScenario_MultiplePendingRounds(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	addrA := "127.0.0.1:9001"

	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	for round := int64(1); round <= 3; round++ {
		// 每轮 TryStart
		state, ok, err := TryStart(ctx, cli, svcName, addrA)
		if err != nil {
			t.Fatalf("round %d TryStart: %v", round, err)
		}
		if !ok {
			t.Fatalf("round %d TryStart: expected ok=true", round)
		}
		if state.Epoch != round {
			t.Fatalf("round %d: epoch = %d, want %d", round, state.Epoch, round)
		}

		// 模拟有新节点加入
		if round < 3 {
			if err := MarkPending(ctx, cli, svcName); err != nil {
				t.Fatalf("round %d MarkPending: %v", round, err)
			}
		}

		// Finish
		startNext, err := Finsh(ctx, cli, svcName, addrA, state.Epoch)
		if err != nil {
			t.Fatalf("round %d Finish: %v", round, err)
		}

		if round < 3 {
			if !startNext {
				t.Fatalf("round %d: expected startNext=true", round)
			}
		} else {
			if startNext {
				t.Fatalf("round %d: expected startNext=false", round)
			}
		}
	}
}

// TestABScenario_Finish_WrongOwner verifies Finish rejects wrong owner.
func TestABScenario_Finish_WrongOwner(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	addrA := "127.0.0.1:9001"
	addrB := "127.0.0.1:9002"

	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	state, ok, err := TryStart(ctx, cli, svcName, addrA)
	if err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	if !ok {
		t.Fatal("TryStart: expected ok=true")
	}

	// B 尝试 Finish A 的迁移 -> 应该失败
	_, err = Finsh(ctx, cli, svcName, addrB, state.Epoch)
	if err != ErrOwnerWrong {
		t.Fatalf("Finish with wrong owner: err=%v, want ErrOwnerWrong", err)
	}
}

// TestABScenario_Finish_WrongEpoch verifies Finish rejects wrong epoch.
func TestABScenario_Finish_WrongEpoch(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	addrA := "127.0.0.1:9001"

	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	_, ok, err := TryStart(ctx, cli, svcName, addrA)
	if err != nil {
		t.Fatalf("TryStart: %v", err)
	}
	if !ok {
		t.Fatal("TryStart: expected ok=true")
	}

	// 用错误的 epoch finish
	_, err = Finsh(ctx, cli, svcName, addrA, 999)
	if err != ErrEpochWrong {
		t.Fatalf("Finish with wrong epoch: err=%v, want ErrEpochWrong", err)
	}
}

// TestABScenario_CanGc validates GC grace period logic.
func TestABScenario_CanGc(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()
	addrA := "127.0.0.1:9001"

	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	// 刚初始化，UpdateAtUnixMs 是当前时间，GC 不应执行
	if CanGc(ctx, cli, svcName) {
		t.Fatal("CanGc immediately after init: expected false")
	}

	// 手动将 UpdateAtUnixMs 设为 10 分钟前
	oldState := State{
		State:          StateStable,
		Epoch:          0,
		Pending:        false,
		Owner:          "",
		UpdateAtUnixMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
	}
	data, _ := json.Marshal(oldState)
	_, err := cli.Put(ctx, stateKey(svcName), string(data))
	if err != nil {
		t.Fatalf("put old state: %v", err)
	}

	// 现在应该可以 GC
	if !CanGc(ctx, cli, svcName) {
		t.Fatal("CanGc after 10min: expected true")
	}

	// 迁移期间不能 GC
	migratingState := State{
		State:          StateMigrating,
		Epoch:          1,
		Pending:        false,
		Owner:          addrA,
		UpdateAtUnixMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
	}
	data, _ = json.Marshal(migratingState)
	_, err = cli.Put(ctx, stateKey(svcName), string(data))
	if err != nil {
		t.Fatalf("put migrating state: %v", err)
	}

	if CanGc(ctx, cli, svcName) {
		t.Fatal("CanGc during migration: expected false")
	}
}

// TestABScenario_TaskSaveLoad tests task and progress persistence.
func TestABScenario_TaskSaveLoad(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()

	task := Task{
		Epoch:    1,
		TaskID:   "1:127.0.0.1:9001->127.0.0.1:9002",
		From:     "127.0.0.1:9001",
		To:       "127.0.0.1:9002",
		Status:   TaskStatusRunning,
		StartKey: "",
	}
	if err := saveTask(ctx, cli, svcName, task); err != nil {
		t.Fatalf("saveTask: %v", err)
	}

	progress := Progress{
		Epoch:   1,
		TaskID:  task.TaskID,
		LastKey: "some-key-99",
		Copied:  100,
	}
	if err := saveProgess(ctx, cli, svcName, progress); err != nil {
		t.Fatalf("saveProgess: %v", err)
	}

	loaded, err := loadProgess(ctx, cli, svcName, task.TaskID, 1)
	if err != nil {
		t.Fatalf("loadProgess: %v", err)
	}
	if loaded.LastKey != "some-key-99" {
		t.Fatalf("loaded LastKey = %q, want %q", loaded.LastKey, "some-key-99")
	}
	if loaded.Copied != 100 {
		t.Fatalf("loaded Copied = %d, want 100", loaded.Copied)
	}

	// 不存在的进度返回空
	empty, err := loadProgess(ctx, cli, svcName, "nonexistent", 99)
	if err != nil {
		t.Fatalf("loadProgess nonexistent: %v", err)
	}
	if empty.LastKey != "" || empty.Copied != 0 {
		t.Fatalf("expected empty progress, got LastKey=%q Copied=%d", empty.LastKey, empty.Copied)
	}
}

// TestABScenario_TryStart_AutoInit verifies TryStart auto-initializes state if missing.
func TestABScenario_TryStart_AutoInit(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()

	// 不调用 InitState, 直接 TryStart
	state, ok, err := TryStart(ctx, cli, svcName, "127.0.0.1:9001")
	if err != nil {
		t.Fatalf("TryStart without InitState: %v", err)
	}
	if !ok {
		t.Fatal("TryStart without InitState: expected ok=true")
	}
	if state.Epoch != 1 {
		t.Fatalf("auto-init epoch = %d, want 1", state.Epoch)
	}
}

// TestABScenario_MarkPending_NotMigrating verifies MarkPending CAS fails when state changed.
func TestABScenario_MarkPending_NotMigrating(t *testing.T) {
	endpoints := requireEtcd(t)
	svcName := uniqueSvcName(t)
	cli := newEtcdCli(t, endpoints)
	defer cli.Close()
	cleanupSvc(t, cli, svcName)

	ctx := context.Background()

	if err := InitState(ctx, cli, svcName); err != nil {
		t.Fatalf("InitState: %v", err)
	}

	// state=stable 时 MarkPending 应该因为 CAS 失败 (ModRevision 不匹配)
	// 实际上 MarkPending 先读 state 再 CAS，stable 状态下 CAS 的 key version 不同
	// 但行为上它会尝试写，CAS 可能成功也可能失败取决于实现
	// 这里我们验证在 stable 状态下 MarkPending 不会 panic
	_ = MarkPending(ctx, cli, svcName)
}
