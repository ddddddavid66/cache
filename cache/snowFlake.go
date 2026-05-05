package cache

import (
	"sync"
	"time"
)

const (
	snowflakeWorkerBits  = 10
	snowflakeSeqBits     = 12
	snowflakeMaxWorkerID = int64(-1) ^ (int64(-1) << snowflakeWorkerBits)
	snowflakeSeqMask     = int64(-1) ^ (int64(-1) << snowflakeSeqBits)

	snowflakeWorkerShift = snowflakeSeqBits
	snowflakeTimeShift   = snowflakeSeqBits + snowflakeWorkerBits

	// 自定义 epoch，减少 timestamp 占用。
	// 2026-01-01 00:00:00 UTC 毫秒。
	snowflakeEpoch = int64(1767225600000)
)

//  workerID 范围通常是 0~1023
// 同一时刻集群内不能重复
//节点重启后最好继续使用原 workerID

type Snowflake struct {
	mu       sync.Mutex
	workerID int64
	lastMs   int64
	seq      int64
}

func NewSnowflake(workerID int64) *Snowflake {
	if workerID < 0 || workerID > snowflakeMaxWorkerID {
		panic("invalid snowflake workerID")
	}
	return &Snowflake{workerID: workerID}
}

func (s *Snowflake) Next() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := currentMillis()

	for now < s.lastMs {
		// 缓存系统里 version 不能倒退，所以这里选择等待。
		//NOTE 防止时间回退
		time.Sleep(time.Duration(s.lastMs-now) * time.Millisecond)
		now = currentMillis()
	}

	if now == s.lastMs {
		s.seq = (s.seq + 1) & snowflakeSeqMask
		if s.seq == 0 {
			now = s.waitNextMillis(s.lastMs)
		}
	} else {
		s.seq = 0
	}

	s.lastMs = now

	return ((now - snowflakeEpoch) << snowflakeTimeShift) |
		(s.workerID << snowflakeWorkerShift) |
		s.seq
}

func (s *Snowflake) waitNextMillis(lastMs int64) int64 {
	now := currentMillis()
	for now <= lastMs {
		time.Sleep(time.Millisecond)
		now = currentMillis()
	}
	return now
}

func currentMillis() int64 {
	return time.Now().UnixMilli()
}
