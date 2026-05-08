package retryqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

type SyncTask struct {
	ID          string        `json"id"`
	Key         string        `json:"key"`
	Value       []byte        `json:"value,omitempty"`
	Attempt     int           `json:"attempt"`
	TTL         time.Duration `json:"ttl"` // 现在是纳秒 （json）
	Version     int64         `json:"verison"`
	Option      string        `json:"option"`
	NextRunAt   time.Time     `json:"next_run_at"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	LastErr     string        `json:"last_err,omitempty"`
	LeasedUntil time.Time     `json:"leased_until"`
}

type TaskView struct {
	ID          string        `json"id"`
	Key         string        `json:"key"`
	Attempt     int           `json:"attempt"`
	TTL         time.Duration `json:"ttl"` // 现在是纳秒 （json）
	Version     int64         `json:"verison"`
	Option      string        `json:"option"`
	NextRunAt   time.Time     `json:"next_run_at"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	LastErr     string        `json:"last_err,omitempty"`
	LeasedUntil time.Time     `json:"leased_until"`
}

// 生成 task id
func RetryTaskId(group, key, option string, version int64) string {
	var s strings.Builder
	s.WriteString(group)
	s.WriteString(key)
	s.WriteString(option)
	s.WriteString(strconv.FormatInt(version, 10)) //10代表 十进制
	sum := sha256.Sum256([]byte(s.String()))
	return hex.EncodeToString(sum[:])
}
