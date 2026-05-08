package retryqueue

import (
	"context"
	"time"
)

// TODO 写入量大 换成 Pebble
type RetryQueue interface {
	Enqueue(task SyncTask) error
	Dequeue(ctx context.Context) (SyncTask, error)
	Ack(id string) error
	Nack(task SyncTask, err error) error
	MoveToDLQ(task SyncTask, err error) error
	Close() error
	ListDLQ() ([]SyncTask, error)
	Stats() QueueStats
	ListPending(opts ListPendingOptions) ([]SyncTask, error)
	RequeueDLQ(id string) error
}

//定义接口 后续升级为WAL 也可以升级为存储层
//group不知道底层实现

// QueueStats 队列内部统计。
type QueueStats struct {
	PendingTasks     int
	DLQTasks         int
	InFlightTasks    int           //设置了 Leasd租约的任务
	OldestPendingAge time.Duration //最老任务持续了多久
	NextDueIn        time.Duration // 距离最近一个任务执行 还需要多久
	EnqueueTotal     uint64
	DequeueTotal     uint64
	AckTotal         uint64
	NackTotal        uint64
	DLQTotal         uint64
	LastErr          string
	LastErrAt        time.Time
}

type ListPendingOptions struct {
	Limit      int
	Option     string
	OnlyDue    bool
	OnlyLeased bool
}
