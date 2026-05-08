package integration

import (
	"context"
	"sync"

	retryqueue "newCache/internal/retry-queue"
)

type testRetryQueue struct {
	mu     sync.Mutex
	closed bool
	ch     chan retryqueue.SyncTask
	done   chan struct{}
}

func newTestRetryQueue() *testRetryQueue {
	return &testRetryQueue{
		ch:   make(chan retryqueue.SyncTask, 16),
		done: make(chan struct{}),
	}
}

func (q *testRetryQueue) Enqueue(task retryqueue.SyncTask) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return retryqueue.ErrQueueClosed
	}
	q.ch <- task
	return nil
}

func (q *testRetryQueue) Dequeue(ctx context.Context) (retryqueue.SyncTask, error) {
	select {
	case task := <-q.ch:
		return task, nil
	case <-ctx.Done():
		return retryqueue.SyncTask{}, ctx.Err()
	case <-q.done:
		return retryqueue.SyncTask{}, retryqueue.ErrQueueClosed
	}
}

func (q *testRetryQueue) Ack(id string) error {
	return nil
}

func (q *testRetryQueue) Nack(task retryqueue.SyncTask, err error) error {
	return nil
}

func (q *testRetryQueue) MoveToDLQ(task retryqueue.SyncTask, err error) error {
	return nil
}

func (q *testRetryQueue) Close() error {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.done)
	}
	q.mu.Unlock()
	return nil
}

func (q *testRetryQueue) ListDLQ() ([]retryqueue.SyncTask, error) {
	return nil, nil
}

func (q *testRetryQueue) Stats() retryqueue.QueueStats {
	return retryqueue.QueueStats{}
}

func (q *testRetryQueue) ListPending(opts retryqueue.ListPendingOptions) ([]retryqueue.SyncTask, error) {
	return nil, nil
}

func (q *testRetryQueue) RequeueDLQ(id string) error {
	return nil
}
