package wal

import (
	"errors"
	"fmt"
	retryqueue "newCache/internal/retry-queue"
	"os"
	"time"
)

type ReplayConfig struct {
	SetFn             func(key string, value []byte, version int64, ttl time.Duration)
	DeleteFn          func(key string, version int64)
	ShadowRetrySet    func(key string, value []byte, version int64, ttl time.Duration)
	ShadowRetryDelete func(key string, version int64)
	Logger            retryqueue.Logger
}

func Replay(path string, cfg ReplayConfig) (int, error) {
	reader, err := NewReader(path)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	records, err := reader.ReadAll()
	if err != nil {
		return 0, err
	}

	now := time.Now().UnixNano()
	replayed := 0
	for _, record := range records {
		// 跳过已经过期的
		if record.TTL > 0 && record.ExpiredAt <= now {
			continue
		}
		switch record.Type {
		case RecordSet:
			if cfg.SetFn != nil {
				cfg.SetFn(record.Key, record.Value, record.Version, record.TTL)
			}
		case RecordDelete:
			if cfg.DeleteFn != nil {
				cfg.DeleteFn(record.Key, record.Version)
			}
		case RecordShadowRetrySet:
			if cfg.ShadowRetrySet != nil {
				cfg.ShadowRetrySet(record.Key, record.Value, record.Version, record.TTL)
			}
		case RecordShadowRetryDelete:
			if cfg.ShadowRetryDelete != nil {
				cfg.ShadowRetryDelete(record.Key, record.Version)
			}
		}
		replayed++
	}
	return replayed, nil
}

func ReplayAll(walDir, walPath string, cfg ReplayConfig) (int, error) {
	cp := NewCheckPointer(walDir, walPath, 0, 0) //禁用清理策略
	oldFiles, err := cp.ListOldFiles()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	total := 0
	// replay 旧的所有的wal
	for _, path := range oldFiles {
		n, err := Replay(path, cfg)
		if err != nil {
			return total, fmt.Errorf("replay %s: %w", path, err)
		}
		total += n
	}
	//replay 当前 所有的wal
	n, err := Replay(walPath, cfg)
	if err != nil {
		return total, fmt.Errorf("replay current wal: %w", err)
	}
	total += n
	return total, nil
}
