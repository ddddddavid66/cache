package retryqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bucket设计
const (
	retryTasksBucket = "retry_tasks"
	retryDLQBucker   = "retry_dlq"
)

type DurableRetryQueue struct {
	db           *bolt.DB       //bbolt文件
	readyCh      chan SyncTask  // 不负责持久化  到期任务的内存投递通道
	wakeCh       chan struct{}  //任务来的时候 唤醒 调度scheduler
	closeOnce    sync.Once      //一次锁
	closeCh      chan struct{}  // 关闭信号
	wg           sync.WaitGroup // 等待 调度 退出
	maxAttempts  int
	scanInterval time.Duration // 最大等待时间
	leaseDur     time.Duration //租约时间
	logger       Logger        // 日志标准化
	metrics      Metrics
}

func NewDurableRetryQueue(path string, opts ...Option) (*DurableRetryQueue, error) {
	cfg := defalutQueueConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	// 0600 = 只有当前用户可读写
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	q := &DurableRetryQueue{
		db:           db,
		readyCh:      make(chan SyncTask),
		wakeCh:       make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
		maxAttempts:  cfg.maxAttemts,
		scanInterval: cfg.scanInterval,
		leaseDur:     cfg.leaseDul,
		logger:       cfg.logger,
		metrics:      cfg.metrics,
	}
	//创建事务 创建桶（类似 表）
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(retryTasksBucket)); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists([]byte(retryDLQBucker))
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	q.wg.Add(1)
	go q.scheduler() //调度器 唤醒

	return q, nil
}

// 入队
func (q *DurableRetryQueue) Enqueue(task SyncTask) error {
	now := time.Now()
	if task.ID == "" {
		q.logger.Error("enqueue failed",
			"task_id", task.ID,
			"error", ErrIdRequired)
		return ErrIdRequired
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	if task.NextRunAt.IsZero() {
		task.NextRunAt = now
	}
	task.UpdatedAt = now

	data, err := json.Marshal(task)
	if err != nil {
		q.logger.Error("enqueue failed",
			"task_id", task.ID,
			"error", err)
		return err
	}
	if err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		return bucket.Put([]byte(task.ID), data)
	}); err != nil {
		q.metrics.Enqueue(false)
		q.logger.Error("enqueue failed",
			"task_id", task.ID,
			"error", err)
		return err
	}
	q.wake() //唤醒 调度
	//logger
	q.logger.Debug("enqueue ok",
		"task_id", task.ID,
		"key", task.Key,
		"option", task.Option)
	q.metrics.Enqueue(true)
	return nil
}

// 阻塞 等待队列完成任务
// 返回 ErrQueueClosed 表示队列已关闭。
func (q *DurableRetryQueue) Dequeue(ctx context.Context) (SyncTask, error) {
	select {
	case task := <-q.readyCh:
		return task, nil
	case <-ctx.Done():
		return SyncTask{}, nil
	case <-q.closeCh:
		return SyncTask{}, ErrQueueClosed
	}
}

// ACK 删除任务 不删除任务会被重复执行
func (q *DurableRetryQueue) Ack(id string) error {
	if id == "" {
		q.logger.Error("ack failed",
			"task_id", id,
			"error", ErrIdRequired)
		return ErrIdRequired
	}
	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(id))
	})
	if err != nil {
		q.logger.Error("ack failed",
			"task_id", id,
			"error", err)
		return err
	}
	q.metrics.Ack()
	q.logger.Debug("ack ok", "task_id", id)
	return nil
}

// Nack attemp+++ 清空租约 计算下次执行时间 指数回避
func (q *DurableRetryQueue) Nack(task SyncTask, err error) error {
	task.Attempt++
	task.UpdatedAt = time.Now()
	task.LeasedUntil = time.Time{}

	if err != nil {
		task.LastErr = err.Error()
	}

	if task.Attempt > q.maxAttempts {
		return q.MoveToDLQ(task, err) //移动到 DLQ里面
	}
	//计算下次执行时间
	task.NextRunAt = time.Now().Add(Backoff(task.Attempt))

	json, err := json.Marshal(task)
	if err != nil {
		return err
	}
	if err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		if bucket == nil {
			return nil
		}
		return bucket.Put([]byte(task.ID), json)
	}); err != nil {
		return err
	}
	q.wake() //唤醒 调度 执行任务
	q.metrics.Nack(task.Attempt)
	q.logger.Warn("nack retry sheduled",
		"task_id", task.ID,
		"attempt", task.Attempt,
		"next_run_at", task.NextRunAt,
		"last_error", task.LastErr)
	return nil
}

// 移动到 死信队列 人工排查
// TODO 根据原因  判断是否需要 重新加入队列 或者 落盘
func (q *DurableRetryQueue) MoveToDLQ(task SyncTask, err error) error {
	task.UpdatedAt = time.Now()
	task.LeasedUntil = time.Time{}

	if err != nil {
		task.LastErr = err.Error()
	}

	json, err := json.Marshal(task)
	if err != nil {
		return err
	}
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucketTask := tx.Bucket([]byte(retryTasksBucket))
		bucketDLQ := tx.Bucket([]byte(retryDLQBucker))
		if bucketDLQ == nil || bucketTask == nil {
			return nil
		}
		if err := bucketDLQ.Put([]byte(task.ID), json); err != nil {
			return nil
		}
		return bucketTask.Delete([]byte(task.ID))
	})
	if err != nil {
		return err
	}
	q.logger.Error("task moved to dlq",
		"task_id", task.ID,
		"attempt", task.Attempt,
		"last_err", task.LastErr,
	)
	return nil
}

// 查看所有死信队列
func (q *DurableRetryQueue) ListDLQ() ([]SyncTask, error) {
	var list []SyncTask
	err := q.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryDLQBucker))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			var task SyncTask
			if err := json.Unmarshal(v, &task); err != nil {
				return nil // 跳过损坏的数据
			}
			list = append(list, task)
			return nil
		})
	})
	return list, err
}

func (q *DurableRetryQueue) RequeueDLQ(id string) error {
	if id == "" {
		return ErrIdRequired
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		bucketDLQ := tx.Bucket([]byte(retryDLQBucker))
		bucketTask := tx.Bucket([]byte(retryTasksBucket))
		if bucketDLQ == nil || bucketTask == nil {
			return nil
		}
		v := bucketDLQ.Get([]byte(id))
		if v == nil {
			return fmt.Errorf("%w : %s in DLQ", ErrTaskNotFound, id)
		}
		var task SyncTask
		if err := json.Unmarshal(v, &task); err != nil {
			return fmt.Errorf("task :%s %w", id, ErrJsonUnMarshal)
		}
		task.Attempt = 0
		task.LastErr = ""
		task.LeasedUntil = time.Time{}
		task.NextRunAt = time.Now()
		task.UpdatedAt = time.Now()

		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		if err := bucketTask.Put([]byte(id), data); err != nil {
			return err
		}
		if err := bucketDLQ.Delete([]byte(id)); err != nil {
			return err
		}
		q.wake() //立即投递
		q.logger.Info("dlq task requeued",
			"task_id", id,
		)
		return nil
	})
}

// 轻量级别的监控接口
func (q *DurableRetryQueue) Stats() QueueStats {
	now := time.Now()

	var stats QueueStats
	_ = q.db.View(func(tx *bolt.Tx) error {
		bucketTask := tx.Bucket([]byte(retryTasksBucket))
		bucketDLQ := tx.Bucket([]byte(retryDLQBucker))
		if bucketTask != nil {
			stats.PendingTasks = bucketTask.Stats().KeyN
			_ = bucketTask.ForEach(func(k, v []byte) error {
				var task SyncTask
				if err := json.Unmarshal(v, &task); err != nil {
					return nil
				}
				if !task.LeasedUntil.IsZero() && task.LeasedUntil.After(now) { //说明还在租约 并且没有过期
					stats.InFlightTasks++
				}
				if !task.CreatedAt.IsZero() {
					age := now.Sub(task.CreatedAt)
					if age > stats.OldestPendingAge {
						stats.OldestPendingAge = age
					}
				}
				return nil
			})
		}
		if bucketDLQ != nil {
			stats.DLQTasks = bucketDLQ.Stats().KeyN

		}
		return nil
	})
	return stats
}

// 未完成的任务留在 retry-tasks 这个bucket中，下次启动自动恢复
func (q *DurableRetryQueue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		close(q.closeCh)
		q.wg.Wait()

		// 释放没有投递的任务
		for {
			select {
			case task := <-q.readyCh:
				q.releaseLease(task.ID)
			default:
				goto done
			}
		}
	done:
		stats := q.Stats()
		q.logger.Info("queue closing",
			"pending_tasks", stats.PendingTasks,
		)
		err = q.db.Close()
	})
	return err
}

// 定期扫描 过期任务 并且投递到 readyCh
func (q *DurableRetryQueue) scheduler() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.scanInterval) //定时器
	defer ticker.Stop()

	for {
		select {
		case <-q.closeCh:
			return
		case <-ticker.C:
			q.dispatchDueTasks() // 投递到期任务
		case <-q.wakeCh:
			q.dispatchDueTasks()
		}
	}
}

// 扫描 retry_task  把到期的任务  投递到readyCh里
func (q *DurableRetryQueue) dispatchDueTasks() {
	now := time.Now()
	due := make([]SyncTask, 0)

	_ = q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			var task SyncTask
			if err := json.Unmarshal(v, &task); err != nil {
				return err
			}
			// 说明没有到期
			if task.NextRunAt.After(now) {
				return nil
			}
			// 还在租约内（别的 worker 正在处理）
			if !task.LeasedUntil.IsZero() && task.LeasedUntil.After(now) {
				return nil
			}

			//设置租约
			task.LeasedUntil = now.Add(q.leaseDur)
			data, err := json.Marshal(task)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(task.ID), data); err != nil {
				return err
			}
			due = append(due, task)
			return nil
		})
	})

	//事务外 投递任务到 channel  避免阻塞事务
	for _, task := range due {
		select {
		case q.readyCh <- task:
			latency := time.Since(task.NextRunAt).Seconds()
			q.metrics.DispatchLatency(latency)
		case <-q.closeCh:
			return
		default: //readyCh 满了
			_ = q.releaseLease(task.ID) //清楚租约 保证可以被再次投递
			return
		}
	}
}

// 清楚 租约
func (q *DurableRetryQueue) releaseLease(ID string) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		if bucket == nil {
			return nil
		}
		value := bucket.Get([]byte(ID))
		var task SyncTask
		if err := json.Unmarshal(value, &task); err != nil {
			return err
		}
		task.LeasedUntil = time.Time{}
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(ID), data)
	})
}

// 唤醒调度
func (q *DurableRetryQueue) wake() {
	select {
	case q.wakeCh <- struct{}{}:
	default:
	}
}

const (
	maxDelay  = 10 * time.Minute
	baseDelay = 100 * time.Millisecond
)

// 指数回避 计算下次执行时间  带随机抖动
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := baseDelay << (attempt - 1)
	if delay > maxDelay {
		delay = maxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return delay/2 + jitter
}

// 返回 哪些任务在等待、等待了多久、重试了几次。
// TODO 管理接口展示 隐藏value  计算size 和 hash
func (q *DurableRetryQueue) ListPending(opts ListPendingOptions) ([]SyncTask, error) {
	// 初始化配置
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	now := time.Now()

	var list []SyncTask
	err := q.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retryTasksBucket))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			if len(list) >= opts.Limit {
				return nil
			}
			var task SyncTask
			if err := json.Unmarshal(v, &task); err != nil {
				return nil
			}
			if opts.Option != "" && task.Option != opts.Option {
				return nil
			}
			if opts.OnlyDue && task.NextRunAt.After(now) {
				return nil
			}
			if opts.OnlyLeased && (task.LeasedUntil.IsZero() || task.LeasedUntil.Before(now)) {
				return nil
			}
			list = append(list, task)
			return nil
		})
	})
	return list, err
}
