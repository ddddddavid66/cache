package rebalance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrUninitialized = errors.New("rebalance state not initialized")
var ErrStateNotStable = errors.New("state is not stable")
var ErrMaekingPending = errors.New("mark pending rebalance conflict")
var ErrFinished = errors.New("finsih rebalance conflict")
var ErrEpochWrong = errors.New("not epoch changed ")
var ErrOwnerWrong = errors.New("not migration owner")

const (
	StateStable    = "stable"
	StateMigrating = "migrating"
)

type State struct {
	State          string `json:"state"`
	Epoch          int64  `json:"epoch"`   //一轮迁移 一个版本
	Pending        bool   `json:"pending"` // 节点是否有新的变化
	Owner          string `json:"owner"`   // key 归属于谁
	UpdateAtUnixMs int64  `json:"update_at_unix_ms"`
}

// etcd key 构造
func stateKey(svcName string) string {
	return fmt.Sprintf("/cache/%s/rebalance/state", svcName)
}

func epochKey(svcName string) string {
	return fmt.Sprintf("/cache/%s/rebalance/epoch", svcName)
}

// 首次部署 初始化etcd的迁移状态  确保这个事务只初始化一次
func InitState(ctx context.Context, cli *clientv3.Client, svcName string) error {
	state := State{
		State:          StateStable,
		Epoch:          0,
		Pending:        false,
		UpdateAtUnixMs: time.Now().Unix(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	//开启事务
	//NOTE Version 指的是 key被修改过多少次
	//ModVersion 指的是 最近一次key的versino是什么
	_, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(stateKey(svcName)), "=", 0)).
		Then(
			clientv3.OpPut(stateKey(svcName), string(data)),
			clientv3.OpPut(epochKey(svcName), "0"),
		).
		Commit()
	return err
}

func LoadState(ctx context.Context, cli *clientv3.Client, svcName string) (State, error) {
	resp, err := cli.Get(ctx, stateKey(svcName))
	if err != nil {
		return State{}, err
	}
	if len(resp.Kvs) == 0 {
		return State{}, ErrUninitialized
	}
	var state State
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return State{}, err
	}
	return state, nil
}

//开始迁移
//读取当前状态 看看是不是 stable  是的话才继续  读取epoch 生成nextEpoch 也就是下一个版本
// etcd事务实现 CAS乐观锁 只有state的ModRevision 没有变 才继续写入
// 事务成功 就是获取锁成  否则 就是别人已经获取成功锁了

func TryStart(ctx context.Context, cli *clientv3.Client, svcName, selfAddr string) (State, bool, error) {
	stateResp, err := cli.Get(ctx, stateKey(svcName))
	if err != nil {
		return State{}, false, err
	}
	if len(stateResp.Kvs) == 0 { //没有初始化 就初始化
		if err := InitState(ctx, cli, svcName); err != nil {
			return State{}, false, err
		}
		stateResp, err = cli.Get(ctx, stateKey(svcName))
		if err != nil {
			return State{}, false, err
		}
	}
	if len(stateResp.Kvs) == 0 {
		return State{}, false, fmt.Errorf("rebalance state not initialized")
	}

	var old State
	if err := json.Unmarshal(stateResp.Kvs[0].Value, &old); err != nil {
		return State{}, false, err
	}

	if old.State != StateStable {
		return State{}, false, ErrStateNotStable
	}

	epochResp, err := cli.Get(ctx, epochKey(svcName))
	if err != nil {
		return State{}, false, nil
	}
	var nextEpoch int64 = 1
	if len(epochResp.Kvs) > 0 {
		nextEpoch, err = strconv.ParseInt(string(epochResp.Kvs[0].Value), 10, 64)
		if err != nil {
			return State{}, false, err
		}
		nextEpoch++
	}

	state := State{
		State:          StateMigrating,
		Epoch:          nextEpoch,
		Pending:        false,
		Owner:          selfAddr,
		UpdateAtUnixMs: time.Now().Unix(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return State{}, false, err
	}

	//开启事务CAS
	resp, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(epochKey(svcName)), "=", epochResp.Kvs[0].ModRevision)).
		Then(
			clientv3.OpPut(stateKey(svcName), string(data)),
			clientv3.OpPut(epochKey(svcName), strconv.FormatInt(nextEpoch, 10)),
		).Commit()
	if err != nil {
		return State{}, false, err
	}
	return state, resp.Succeeded, nil
}

// state是 迁移  并且有新的节点变化
func MarkPending(ctx context.Context, cli *clientv3.Client, svcName string) error {
	resp, err := cli.Get(ctx, stateKey(svcName))
	if err != nil {
		return err
	}
	var state State
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return err
	}

	state.Pending = true
	state.UpdateAtUnixMs = time.Now().Unix()

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	respCas, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(epochKey(svcName)), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(stateKey(svcName), string(data))).Commit()
	if err != nil {
		return err
	}

	if !respCas.Succeeded {
		return ErrMaekingPending
	}
	return nil
}

//Finished 完成提交状态
// 自己必须是owner  并且 epoch匹配
// 有pending  就是代表 还需要下一轮

func Finsh(ctx context.Context, cli *clientv3.Client, svcName, selfAddr string, epoch int64) (bool, error) {
	resp, err := cli.Get(ctx, stateKey(svcName))
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) == 0 {
		return false, ErrUninitialized
	}

	var state State
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return false, err
	}

	//验证 owner  epoch
	if state.Owner != selfAddr {
		return false, ErrOwnerWrong
	}
	if state.Epoch != epoch {
		return false, ErrEpochWrong
	}

	startNext := state.Pending
	state.State = StateStable
	state.Pending = false
	state.Owner = ""
	state.UpdateAtUnixMs = time.Now().Unix()

	data, err := json.Marshal(state)
	if err != nil {
		return false, err
	}

	//CAS提交
	respCas, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(epochKey(svcName)), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(stateKey(svcName), string(data))).Commit()
	if err != nil {
		return false, err
	}
	if !respCas.Succeeded {
		return false, ErrFinished
	}
	return startNext, nil
}
