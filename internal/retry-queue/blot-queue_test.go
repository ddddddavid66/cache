package retryqueue

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestDurableRetryQueueRecoversEnqueuedTaskAfterRestart(t *testing.T) {
	path := queueDBPath(t)

	q := openTestQueue(t, path, 3)
	task := testTask("group-a", "key-a", "set", 10)
	task.Attempt = 2
	task.NextRunAt = time.Now().Add(150 * time.Millisecond)

	if err := q.Enqueue(task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	q = openTestQueue(t, path, 3)
	defer closeQueue(t, q)

	got := dequeueAfterWake(t, q, time.Second)
	if got.ID != task.ID {
		t.Fatalf("dequeued ID = %q, want %q", got.ID, task.ID)
	}
	if got.Key != task.Key || got.Version != task.Version || got.Option != task.Option || got.Attempt != task.Attempt {
		t.Fatalf("dequeued task = %+v, want key=%q version=%d option=%q attempt=%d",
			got, task.Key, task.Version, task.Option, task.Attempt)
	}
}

func TestDurableRetryQueueAckPreventsRecoveryAfterRestart(t *testing.T) {
	path := queueDBPath(t)

	q := openTestQueue(t, path, 3)
	task := testTask("group-a", "key-a", "delete", 11)
	if err := q.Enqueue(task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	got := dequeueAfterWake(t, q, time.Second)
	if err := q.Ack(got.ID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	q = openTestQueue(t, path, 3)
	defer closeQueue(t, q)

	if got, ok := tryDequeueAfterWake(t, q, 100*time.Millisecond); ok {
		t.Fatalf("Dequeue() got task after Ack and restart: %+v", got)
	}
}

func TestDurableRetryQueueNackDelaysRetry(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)

	task := testTask("group-a", "key-a", "set", 12)
	if err := q.Enqueue(task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	got := dequeueAfterWake(t, q, time.Second)
	if err := q.Nack(got, errors.New("temporary failure")); err != nil {
		t.Fatalf("Nack() error = %v", err)
	}

	if got, ok := tryDequeueAfterWake(t, q, 30*time.Millisecond); ok {
		t.Fatalf("Dequeue() immediately after Nack got task: %+v", got)
	}

	got = dequeueAfterWake(t, q, time.Second)
	if got.ID != task.ID {
		t.Fatalf("dequeued ID after retry delay = %q, want %q", got.ID, task.ID)
	}
	if got.Attempt != 2 {
		t.Fatalf("dequeued attempt after Nack = %d, want 2", got.Attempt)
	}
}

func TestDurableRetryQueueMovesTaskToDLQAfterMaxAttempts(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)

	task := testTask("group-a", "key-a", "set", 13)
	task.Attempt = 1
	if err := q.Enqueue(task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := q.Nack(task, errors.New("still failing")); err != nil {
			t.Fatalf("Nack(%d) error = %v", i+1, err)
		}
		task.Attempt++
	}

	stats := q.Stats()
	if stats.PendingTasks != 0 {
		t.Fatalf("PendingTasks = %d, want 0", stats.PendingTasks)
	}
	if stats.DLQTasks != 1 {
		t.Fatalf("DLQTasks = %d, want 1", stats.DLQTasks)
	}

	dlq, err := q.ListDLQ()
	if err != nil {
		t.Fatalf("ListDLQ() error = %v", err)
	}
	if len(dlq) != 1 {
		t.Fatalf("ListDLQ() len = %d, want 1", len(dlq))
	}
	if dlq[0].ID != task.ID {
		t.Fatalf("DLQ task ID = %q, want %q", dlq[0].ID, task.ID)
	}
}

func TestDurableRetryQueueDeduplicatesByTaskID(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)

	first := testTask("group-a", "key-a", "set", 14)
	first.Value = []byte("first")
	first.Attempt = 1
	second := first
	second.Value = []byte("second")
	second.Attempt = 2

	if err := q.Enqueue(first); err != nil {
		t.Fatalf("Enqueue(first) error = %v", err)
	}
	if err := q.Enqueue(second); err != nil {
		t.Fatalf("Enqueue(second) error = %v", err)
	}

	stats := q.Stats()
	if stats.PendingTasks != 1 {
		t.Fatalf("PendingTasks = %d, want 1", stats.PendingTasks)
	}

	stored := readStoredTask(t, q, second.ID)
	if string(stored.Value) != "second" {
		t.Fatalf("stored value = %q, want %q", string(stored.Value), "second")
	}
	if stored.Attempt != 2 {
		t.Fatalf("stored attempt = %d, want 2", stored.Attempt)
	}
}

func TestDurableRetryQueueRedeliversAfterLeaseExpires(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)
	q.leaseDur = 30 * time.Millisecond

	task := testTask("group-a", "key-a", "set", 15)
	if err := q.Enqueue(task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	first := dequeueAfterWake(t, q, time.Second)
	if first.ID != task.ID {
		t.Fatalf("first Dequeue() ID = %q, want %q", first.ID, task.ID)
	}

	time.Sleep(2 * q.leaseDur)

	second := dequeueAfterWake(t, q, time.Second)
	if second.ID != task.ID {
		t.Fatalf("second Dequeue() ID = %q, want %q", second.ID, task.ID)
	}
}

func TestDurableRetryQueueCloseStopsWorkersAndClosesDB(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := q.Dequeue(context.Background())
				if errors.Is(err, ErrQueueClosed) {
					return
				}
				if err != nil {
					t.Errorf("Dequeue() error = %v", err)
					return
				}
			}
		}()
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not exit after Close()")
	}

	if err := q.db.View(func(tx *bolt.Tx) error { return nil }); err == nil {
		t.Fatal("db View() after Close() error = nil, want closed database error")
	}
}

func TestDurableRetryQueueListPending(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)

	nextRunAt := time.Now().Add(time.Hour)
	tasks := []SyncTask{
		testTask("group-a", "key-pending-1", "set", 201),
		testTask("group-a", "key-pending-2", "delete", 202),
		testTask("group-a", "key-pending-3", "set", 203),
	}
	for _, task := range tasks {
		task.NextRunAt = nextRunAt
		if err := q.Enqueue(task); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	got, err := q.ListPending(ListPendingOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListPending() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListPending() len = %d, want 2", len(got))
	}

	got, err = q.ListPending(ListPendingOptions{Option: "set"})
	if err != nil {
		t.Fatalf("ListPending(option=set) error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListPending(option=set) len = %d, want 2", len(got))
	}
	for _, task := range got {
		if task.Option != "set" {
			t.Fatalf("ListPending(option=set) got option %q", task.Option)
		}
	}

	got, err = q.ListPending(ListPendingOptions{OnlyDue: true})
	if err != nil {
		t.Fatalf("ListPending(OnlyDue) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListPending(OnlyDue) len = %d, want 0", len(got))
	}
}

func TestDurableRetryQueueRequeueDLQMovesTaskBackToPending(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)

	task := testTask("group-a", "key-dlq", "set", 301)
	task.Attempt = 3

	if err := q.MoveToDLQ(task, errors.New("permanent failure")); err != nil {
		t.Fatalf("MoveToDLQ() error = %v", err)
	}
	if err := q.RequeueDLQ(task.ID); err != nil {
		t.Fatalf("RequeueDLQ() error = %v", err)
	}

	dlq, err := q.ListDLQ()
	if err != nil {
		t.Fatalf("ListDLQ() error = %v", err)
	}
	if len(dlq) != 0 {
		t.Fatalf("ListDLQ() len = %d, want 0", len(dlq))
	}

	stored := readStoredTask(t, q, task.ID)
	if stored.Attempt != 0 {
		t.Fatalf("requeued attempt = %d, want 0", stored.Attempt)
	}
	if stored.LastErr != "" {
		t.Fatalf("requeued LastErr = %q, want empty", stored.LastErr)
	}
	if !stored.LeasedUntil.IsZero() {
		t.Fatalf("requeued LeasedUntil = %v, want zero", stored.LeasedUntil)
	}

	got := dequeueAfterWake(t, q, time.Second)
	if got.ID != task.ID {
		t.Fatalf("dequeued requeued ID = %q, want %q", got.ID, task.ID)
	}
}

func TestDurableRetryQueueStatsCountsPendingDLQAndInflight(t *testing.T) {
	q := openTestQueue(t, queueDBPath(t), 3)
	defer closeQueue(t, q)
	q.leaseDur = time.Second

	pending := testTask("group-a", "key-pending", "set", 401)
	pending.NextRunAt = time.Now().Add(time.Hour)

	inflight := testTask("group-a", "key-inflight", "set", 402)
	inflight.NextRunAt = time.Now()

	dlq := testTask("group-a", "key-stats-dlq", "set", 403)

	if err := q.Enqueue(pending); err != nil {
		t.Fatalf("Enqueue(pending) error = %v", err)
	}
	if err := q.Enqueue(inflight); err != nil {
		t.Fatalf("Enqueue(inflight) error = %v", err)
	}
	if got := dequeueAfterWake(t, q, time.Second); got.ID != inflight.ID {
		t.Fatalf("Dequeue() ID = %q, want %q", got.ID, inflight.ID)
	}
	if err := q.MoveToDLQ(dlq, errors.New("failed")); err != nil {
		t.Fatalf("MoveToDLQ() error = %v", err)
	}

	stats := q.Stats()
	if stats.PendingTasks != 2 {
		t.Fatalf("PendingTasks = %d, want 2", stats.PendingTasks)
	}
	if stats.DLQTasks != 1 {
		t.Fatalf("DLQTasks = %d, want 1", stats.DLQTasks)
	}
	if stats.InFlightTasks != 1 {
		t.Fatalf("InFlightTasks = %d, want 1", stats.InFlightTasks)
	}
}

func queueDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "retry.db")
}

func openTestQueue(t *testing.T, path string, maxAttempts int) *DurableRetryQueue {
	t.Helper()
	q, err := NewDurableRetryQueue(
		path,
		WithLogger(noopTestLogger{}),
		WithMacAttempts(maxAttempts),
		WithScanInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewDurableRetryQueue() error = %v", err)
	}
	return q
}

func closeQueue(t *testing.T, q *DurableRetryQueue) {
	t.Helper()
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func testTask(group, key, option string, version int64) SyncTask {
	return SyncTask{
		ID:        RetryTaskId(group, key, option, version),
		Key:       key,
		Value:     []byte("value"),
		Attempt:   1,
		TTL:       time.Minute,
		Version:   version,
		Option:    option,
		NextRunAt: time.Now(),
	}
}

func dequeueAfterWake(t *testing.T, q *DurableRetryQueue, timeout time.Duration) SyncTask {
	t.Helper()
	task, ok := tryDequeueAfterWake(t, q, timeout)
	if !ok {
		t.Fatalf("Dequeue() timed out after %s", timeout)
	}
	return task
}

func tryDequeueAfterWake(t *testing.T, q *DurableRetryQueue, timeout time.Duration) (SyncTask, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result := make(chan struct {
		task SyncTask
		err  error
	}, 1)

	go func() {
		task, err := q.Dequeue(ctx)
		result <- struct {
			task SyncTask
			err  error
		}{task: task, err: err}
	}()

	time.Sleep(10 * time.Millisecond)
	q.wake()

	select {
	case res := <-result:
		if res.err != nil {
			t.Fatalf("Dequeue() error = %v", res.err)
		}
		if res.task.ID == "" {
			return SyncTask{}, false
		}
		return res.task, true
	case <-time.After(timeout + 50*time.Millisecond):
		return SyncTask{}, false
	}
}

func readStoredTask(t *testing.T, q *DurableRetryQueue, id string) SyncTask {
	t.Helper()

	var task SyncTask
	err := q.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		if bucket == nil {
			t.Fatalf("%s bucket does not exist", retryTasksBucket)
		}
		data := bucket.Get([]byte(id))
		if data == nil {
			t.Fatalf("task %q does not exist in %s", id, retryTasksBucket)
		}
		if err := json.Unmarshal(data, &task); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("db.View() error = %v", err)
	}
	return task
}

type noopTestLogger struct{}

func (noopTestLogger) Info(msg string, args ...any)  {}
func (noopTestLogger) Warn(msg string, args ...any)  {}
func (noopTestLogger) Error(msg string, args ...any) {}
func (noopTestLogger) Debug(msg string, args ...any) {}
