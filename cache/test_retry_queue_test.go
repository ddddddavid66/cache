package cache

import (
	"context"
	"sync"

	retryqueue "newCache/internal/retry-queue"
)

type testRetryQueue struct {
	mu     sync.Mutex
	closed bool
	ch     chan syncTask
	done   chan struct{}
}

func newTestRetryQueue() *testRetryQueue {
	return &testRetryQueue{
		ch:   make(chan syncTask, 16),
		done: make(chan struct{}),
	}
}

func (q *testRetryQueue) Enqueue(task syncTask) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return retryqueue.ErrQueueClosed
	}
	q.ch <- task
	return nil
}

func (q *testRetryQueue) Dequeue(ctx context.Context) (syncTask, error) {
	select {
	case task := <-q.ch:
		return task, nil
	case <-ctx.Done():
		return syncTask{}, ctx.Err()
	case <-q.done:
		return syncTask{}, retryqueue.ErrQueueClosed
	}
}

func (q *testRetryQueue) Ack(id string) error {
	return nil
}

func (q *testRetryQueue) Nack(task syncTask, err error) error {
	return nil
}

func (q *testRetryQueue) MoveToDLQ(task syncTask, err error) error {
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

func (q *testRetryQueue) ListDLQ() ([]syncTask, error) {
	return nil, nil
}

func (q *testRetryQueue) Stats() retryqueue.QueueStats {
	return retryqueue.QueueStats{}
}

func (q *testRetryQueue) ListPending(opts retryqueue.ListPendingOptions) ([]syncTask, error) {
	return nil, nil
}

func (q *testRetryQueue) RequeueDLQ(id string) error {
	return nil
}
