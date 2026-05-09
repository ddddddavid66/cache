package rebalance

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	TaskStatusRunning = "running"
	TaskStatusDone    = "done"
	TaskBatchSize     = 256
)

// 代表一个迁移任务
type Task struct {
	Epoch          int64  `json:"epoch"`
	TaskID         string `json:"task_id"`
	From           string `json:"from"`
	To             string `json:"to"`
	Status         string `json:"status"`
	StartKey       string `json:"start_key"`
	UpdateAtUnixMs int64  `json:"update_at_unix_ms"`
}

// 表示迁移进度 迁移 worker 中途失败 / 节点重启 / 进程崩溃 还可以从头往后迁移
type Progress struct {
	Epoch          int64  `json:"epoch"`
	TaskID         string `json:"task_id"`
	LastKey        string `json:"last_key"`
	Copied         int64  `json:"copied"`
	UpdateAtUnixMs int64  `json:"update_at_unix_ms"`
}

func taskKey(svcName string, epoch int64, taskID string) string {
	return fmt.Sprintf("cache/%s/rebalance/tasks/%d/%s", svcName, epoch, taskID)
}

func progessKey(svcName string, epoch int64, taskID string) string {
	return fmt.Sprintf("cache/%s/rebalance/progress/%d/%s", svcName, epoch, taskID)
}

func taskPrefix(svcName string, epoch int64) string {
	return fmt.Sprintf("cache/%s/rebalance/tasks/%d/", svcName, epoch)
}

// saveTask 迁移任务写进去etcd
func saveTask(ctx context.Context, cli *clientv3.Client, svcName string, task Task) error {
	task.UpdateAtUnixMs = time.Now().Unix()
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = cli.Put(ctx, taskKey(svcName, task.Epoch, task.TaskID), string(data))
	return err
}

// saveProgess 保存进度到 etcd 每批 BatchSet 成功后调用
func saveProgess(ctx context.Context, cli *clientv3.Client, svcName string, progess Progress) error {
	progess.UpdateAtUnixMs = time.Now().Unix()
	data, err := json.Marshal(progess)
	if err != nil {
		return err
	}
	_, err = cli.Put(ctx, progessKey(svcName, progess.Epoch, progess.TaskID), string(data))
	return err
}

func loadProgess(ctx context.Context, cli *clientv3.Client, svcName, taskID string, epoch int64) (Progress, error) {
	resp, err := cli.Get(ctx, progessKey(svcName, epoch, taskID))
	if err != nil {
		return Progress{}, err
	}
	if len(resp.Kvs) == 0 {
		return Progress{}, nil //没有进度 从0开始
	}
	var progess Progress
	if err := json.Unmarshal(resp.Kvs[0].Value, &progess); err != nil {
		return Progress{}, err
	}
	return progess, nil
}

// 检验当前任务 是否可以 继续进行
// state = migratiing owner = self epoch 一样
func ShouContinue(ctx context.Context, cli *clientv3.Client, svcName, selfAddr string, epoch int64) bool {
	state, err := LoadState(ctx, cli, svcName)
	if err != nil {
		return false
	}
	return state.State == StateMigrating && state.Owner == selfAddr && state.Epoch == epoch
}
