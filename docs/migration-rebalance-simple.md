# 迁移过程中节点变更的最简单处理方案

## 背景问题

当前项目已经有这些能力：

- 节点状态通过 etcd 注册，状态包括 `warming`、`active`、`draining`。
- `Picker` 根据节点状态构建 `readRing`、`writeRing`、`shadowRing`。
- 数据迁移通过 `Scan -> BatchSet` 完成。
- 写入和迁移都带 `version`，旧版本不会覆盖新版本。
- 删除通过 `tombstone` 保留版本，旧 `Set` 不会把删除数据复活。
- 影子双写通过 `PickShadowPeer` 写到新节点，并且已有 peer 级别限流和 retry queue。

问题发生在这个场景：

```text
A 先上线，触发迁移，并且已经迁了一部分数据。
B 后上线，按新的一致性哈希环计算，A 迁过去的那部分数据又应该归 B。
如果 A 的迁移还没结束，集群状态又变化，就可能产生半迁移数据、重复迁移数据、非 owner 节点上的残留数据。
```

最简单的解决方式是：**同一时间只允许一轮迁移生效。迁移过程中发生的新节点上线/下线不立刻打断当前迁移，只记录为 pending rebalance，等当前迁移完成后再执行下一轮。**

这个方案不需要自己再引入 Raft，因为 etcd 本身已经提供线性一致的事务能力。我们只需要把“是否允许开始迁移、迁移版本是多少、是否有待处理变更”放进 etcd，并用事务保证只有一个节点能启动迁移。

## 目标

这个方案要保证：

- 不丢数据：迁移前后都保留 `version`、`ttl`、`tombstone`。
- 不乱归属：迁移完成后才切换节点状态，不在半迁移时让新节点承担正式写流量。
- 不叠加迁移：当前迁移未完成时，新的拓扑变化只打标记，不启动第二轮迁移。
- 不明显损失性能：正常读写仍走本地内存、gRPC 和一致性哈希；迁移只在后台分批执行，并复用已有 peer 限流。
- 可恢复：迁移进度写入 etcd，迁移 worker 重启后可以继续从上次的 `start_key` 之后扫描。

## 核心规则

### 规则一：全局只有一个迁移状态

在 etcd 中维护一个全局状态：

```text
/cache/{service}/rebalance/state
```

状态只有两类：

```text
stable
migrating
```

含义：

- `stable`：没有迁移正在运行，可以根据当前节点列表计算新迁移计划。
- `migrating`：已有一轮迁移正在执行，新的节点变更只能记录，不能立刻启动新迁移。

### 规则二：每轮迁移都有单调递增版本

每次开始迁移都生成一个版本：

```text
/cache/{service}/rebalance/epoch
```

例如：

```text
epoch = 12
```

迁移任务、进度、日志都绑定这个 epoch。这样可以避免旧任务在新拓扑下继续提交状态。

### 规则三：迁移期间新拓扑变化只设置 pending

迁移过程中如果 B 上线：

```text
state = migrating
```

此时不要马上重算并执行新的迁移，只写：

```text
/cache/{service}/rebalance/pending = true
```

当前迁移完成后再检查 pending：

- `pending=false`：结束，集群回到 `stable`。
- `pending=true`：清空 pending，基于最新节点列表开启下一轮迁移。

### 规则四：迁移完成后再切 active

新节点上线时先是 `warming`，只进入 `shadowRing`，不进入正式 `writeRing`。

推荐流程：

```text
新节点注册为 warming
旧 active 节点继续承担正式读写
后台把应该迁给 warming 节点的数据复制过去
影子双写保证迁移期间的新写入也到达 warming 节点
迁移完成
把 warming 节点改成 active
下一轮 picker reload 后，正式读写才进入新节点
```

这样做的原因是：**半迁移期间不要让新节点成为正式 owner**。否则数据还没拷完，读请求就可能打到新节点并 miss。

### 规则五：残留数据延迟清理

迁移结束后，旧节点上可能还残留已经不归自己的 key。这不是立即错误，只要正式路由已经切到新 owner，残留数据不会再被写入路径使用。

清理规则：

```text
只有当 state=stable 时，才允许清理本地非 owner 数据。
清理时根据最新 writeRing 判断 owner。
如果 key 不归自己，并且超过 grace period，删除本地副本。
```

不要在迁移过程中清理。迁移过程中旧节点可能仍然是正式写 owner，也可能还需要给新节点继续补数据。

## etcd key 设计

建议增加这些 key：

```text
/cache/{service}/rebalance/state
/cache/{service}/rebalance/epoch
/cache/{service}/rebalance/tasks/{epoch}/{task_id}
/cache/{service}/rebalance/progress/{epoch}/{task_id}
```

`state` 使用 JSON 保存 `state`、`epoch`、`pending`、`owner`。不要再把这些字段拆成多个热更新 key，否则一次状态变化需要多次写 etcd，容易出现中间态。

示例内容：

```json
{
  "state": "migrating",
  "epoch": 12,
  "pending": false,
  "owner": "127.0.0.1:8001"
}
```

迁移任务：

```json
{
  "epoch": 12,
  "task_id": "12:127.0.0.1:8001->127.0.0.1:8003",
  "from": "127.0.0.1:8001",
  "to": "127.0.0.1:8003",
  "status": "running",
  "start_key": "",
  "updated_at_unix_ms": 1710000000000
}
```

进度：

```json
{
  "epoch": 12,
  "task_id": "12:127.0.0.1:8001->127.0.0.1:8003",
  "last_key": "user:10086",
  "copied": 10240,
  "updated_at_unix_ms": 1710000000000
}
```

## 详细步骤

### 1. 节点上线只注册 warming

新节点启动时继续使用当前实现：

```go
info := NodeInfo{
	Addr:   r.addr,
	Status: StatusWarming,
}
```

为什么这么做：

- `warming` 只进入 `shadowRing`，不会进入 `writeRing`。
- 正式写流量仍然只进入已有 `active` 节点。
- 新节点可以接收影子双写和迁移数据，但不会因为数据未完整而影响读写。

### 2. 尝试启动一轮迁移

任意节点监听到 etcd 节点变化后，都可以尝试启动迁移。但必须用 etcd 事务抢占全局迁移状态。

只有这个事务成功的节点才是本轮迁移 owner。

```go
package rebalance

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	StateStable    = "stable"
	StateMigrating = "migrating"
)

type State struct {
	State            string `json:"state"`
	Epoch            int64  `json:"epoch"`
	Pending          bool   `json:"pending"`
	Owner            string `json:"owner"`
	UpdatedAtUnixMs  int64  `json:"updated_at_unix_ms"`
}

func stateKey(service string) string {
	return fmt.Sprintf("/cache/%s/rebalance/state", service)
}

func epochKey(service string) string {
	return fmt.Sprintf("/cache/%s/rebalance/epoch", service)
}

func TryStart(ctx context.Context, cli *clientv3.Client, service, selfAddr string) (State, bool, error) {
	stateResp, err := cli.Get(ctx, stateKey(service))
	if err != nil {
		return State{}, false, err
	}
	if len(stateResp.Kvs) == 0 {
		if err := InitState(ctx, cli, service); err != nil {
			return State{}, false, err
		}
		stateResp, err = cli.Get(ctx, stateKey(service))
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
		return State{}, false, nil
	}

	epochResp, err := cli.Get(ctx, epochKey(service))
	if err != nil {
		return State{}, false, err
	}

	var nextEpoch int64 = 1
	if len(epochResp.Kvs) > 0 {
		_, _ = fmt.Sscanf(string(epochResp.Kvs[0].Value), "%d", &nextEpoch)
		nextEpoch++
	}

	st := State{
		State:           StateMigrating,
		Epoch:           nextEpoch,
		Pending:         false,
		Owner:           selfAddr,
		UpdatedAtUnixMs: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(st)
	if err != nil {
		return State{}, false, err
	}

	txn := cli.Txn(ctx).
		If(
			clientv3.Compare(clientv3.ModRevision(stateKey(service)), "=", stateResp.Kvs[0].ModRevision),
		).
		Then(
			clientv3.OpPut(stateKey(service), string(data)),
			clientv3.OpPut(epochKey(service), fmt.Sprintf("%d", nextEpoch)),
		)

	resp, err := txn.Commit()
	if err != nil {
		return State{}, false, err
	}
	return st, resp.Succeeded, nil
}
```

注意：第一次部署时，`state` 可能不存在。初始化时写一次：

```go
func InitState(ctx context.Context, cli *clientv3.Client, service string) error {
	st := State{
		State:           StateStable,
		Epoch:           0,
		Pending:         false,
		UpdatedAtUnixMs: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}

	_, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(stateKey(service)), "=", 0)).
		Then(
			clientv3.OpPut(stateKey(service), string(data)),
			clientv3.OpPut(epochKey(service), "0"),
		).
		Commit()
	return err
}
```

### 3. 迁移中遇到新变化只打 pending

如果节点变化时发现 `state=migrating`，不要启动迁移，只设置 pending。

```go
func MarkPending(ctx context.Context, cli *clientv3.Client, service string) error {
	resp, err := cli.Get(ctx, stateKey(service))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	var st State
	if err := json.Unmarshal(resp.Kvs[0].Value, &st); err != nil {
		return err
	}
	if st.State != StateMigrating {
		return nil
	}

	st.Pending = true
	st.UpdatedAtUnixMs = time.Now().UnixMilli()
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(stateKey(service)), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(stateKey(service), string(data))).
		Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("mark pending rebalance conflict")
	}
	return nil
}
```

为什么要比较 `ModRevision`：

- 防止多个节点同时更新 state 时互相覆盖。
- 如果比较失败，说明状态已经被别的节点更新，下一轮 watch/reload 会重新处理。

### 4. 迁移 worker 按 epoch 执行

迁移 worker 必须在开始前读取当前状态，并且确认：

```text
state=migrating
owner=self
epoch=task.epoch
```

不满足就停止当前任务。

```go
type Task struct {
	Epoch           int64  `json:"epoch"`
	TaskID          string `json:"task_id"`
	From            string `json:"from"`
	To              string `json:"to"`
	Status          string `json:"status"`
	StartKey        string `json:"start_key"`
	UpdatedAtUnixMs int64  `json:"updated_at_unix_ms"`
}

func LoadState(ctx context.Context, cli *clientv3.Client, service string) (State, error) {
	resp, err := cli.Get(ctx, stateKey(service))
	if err != nil {
		return State{}, err
	}
	if len(resp.Kvs) == 0 {
		return State{}, fmt.Errorf("rebalance state not initialized")
	}

	var st State
	if err := json.Unmarshal(resp.Kvs[0].Value, &st); err != nil {
		return State{}, err
	}
	return st, nil
}

func ShouldContinue(ctx context.Context, cli *clientv3.Client, service, selfAddr string, epoch int64) bool {
	st, err := LoadState(ctx, cli, service)
	if err != nil {
		return false
	}
	return st.State == StateMigrating && st.Owner == selfAddr && st.Epoch == epoch
}
```

迁移主体仍然复用当前 `Scan -> BatchSet`，只是要改成分批扫描，不要一次 `Scan("", 1000000)`：

```go
func RunTask(
	ctx context.Context,
	cli *clientv3.Client,
	service string,
	selfAddr string,
	task Task,
	scan func(startKey string, count int64) ([]cache.TransportEntry, error),
	sendBatch func(ctx context.Context, addr string, entries []cache.TransportEntry) error,
	saveProgress func(ctx context.Context, task Task, lastKey string, copied int64) error,
) error {
	const batchSize int64 = 256

	startKey := task.StartKey
	var copied int64

	for {
		if !ShouldContinue(ctx, cli, service, selfAddr, task.Epoch) {
			return fmt.Errorf("migration task canceled: epoch=%d task=%s", task.Epoch, task.TaskID)
		}

		entries, err := scan(startKey, batchSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}

		if err := sendBatch(ctx, task.To, entries); err != nil {
			return err
		}

		lastKey := entries[len(entries)-1].Key
		copied += int64(len(entries))

		if err := saveProgress(ctx, task, lastKey, copied); err != nil {
			return err
		}

		startKey = lastKey
	}
}
```

这里的 `scan` 和 `sendBatch` 可以直接包你当前的代码：

```go
scan := func(startKey string, count int64) ([]cache.TransportEntry, error) {
	items, err := group.Scan(startKey, count)
	if err != nil {
		return nil, err
	}

	out := make([]cache.TransportEntry, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, *item)
	}
	return out, nil
}

sendBatch := func(ctx context.Context, addr string, entries []cache.TransportEntry) error {
	return manager.SendBatch(ctx, addr, entries)
}
```

### 5. BatchSet 继续用 version 保证幂等

你当前的 `BatchSet` 已经有关键逻辑：

```go
old, ok := g.mainCache.GetEntry(entry.Key)
if ok && old.Version > entry.Version {
	continue
}
```

这保证了：

- 重试同一批数据不会产生错误。
- 旧数据不会覆盖新数据。
- 迁移和影子双写乱序到达时，较新的 version 胜出。

建议微调为：

```go
if ok && old.Version >= entry.Version {
	continue
}
```

原因：

- 相同 version 的重复迁移没有必要再次写内存。
- 对性能更友好，减少重复写和 LRU 变动。

如果同一个 version 允许覆盖相同值，这不是正确性问题，只是会多一次写入成本。

### 6. 完成迁移后提交状态

迁移 owner 完成所有任务后，用事务把状态切回 `stable`。

如果期间有 pending，则不要认为迁移彻底结束，而是立刻启动下一轮。

```go
func Finish(ctx context.Context, cli *clientv3.Client, service, selfAddr string, epoch int64) (startNext bool, err error) {
	resp, err := cli.Get(ctx, stateKey(service))
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) == 0 {
		return false, fmt.Errorf("rebalance state not initialized")
	}

	var st State
	if err := json.Unmarshal(resp.Kvs[0].Value, &st); err != nil {
		return false, err
	}
	if st.Owner != selfAddr || st.Epoch != epoch {
		return false, fmt.Errorf("not migration owner or epoch changed")
	}

	startNext = st.Pending
	st.State = StateStable
	st.Pending = false
	st.Owner = ""
	st.UpdatedAtUnixMs = time.Now().UnixMilli()

	data, err := json.Marshal(st)
	if err != nil {
		return false, err
	}

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(stateKey(service)), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(stateKey(service), string(data))).
		Commit()
	if err != nil {
		return false, err
	}
	if !txnResp.Succeeded {
		return false, fmt.Errorf("finish rebalance conflict")
	}
	return startNext, nil
}
```

调用逻辑：

```go
startNext, err := Finish(ctx, cli, service, selfAddr, epoch)
if err != nil {
	return err
}
if startNext {
	_, _, _ = TryStart(ctx, cli, service, selfAddr)
}
```

### 7. 迁移完成后再把 warming 改 active

迁移完成后，才调用现有方法：

```go
if err := registry.UpdateStatus(ctx, registry.StatusActive); err != nil {
	return err
}
```

为什么不是先 active 再迁移：

- active 会进入 `writeRing`，正式写流量会过来。
- 数据没迁完时，读写路由已经变了，会出现 miss 或旧 owner/new owner 混乱。
- 先迁移再 active，写路由切换只发生在数据准备好之后。

### 8. 稳定后清理非 owner 数据

清理只在 `stable` 状态运行：

```go
func CanGC(ctx context.Context, cli *clientv3.Client, service string) bool {
	st, err := LoadState(ctx, cli, service)
	if err != nil {
		return false
	}
	return st.State == StateStable
}
```

清理逻辑：

```go
func GCNonOwnerKeys(
	ctx context.Context,
	canGC func(context.Context) bool,
	selfAddr string,
	ownerOf func(key string) string,
	scan func(startKey string, count int64) ([]cache.TransportEntry, error),
	deleteLocal func(key string) bool,
) error {
	if !canGC(ctx) {
		return nil
	}

	const batchSize int64 = 256
	startKey := ""

	for {
		entries, err := scan(startKey, batchSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}

		for _, entry := range entries {
			if ownerOf(entry.Key) != selfAddr {
				deleteLocal(entry.Key)
			}
			startKey = entry.Key
		}
	}
}
```

实际工程里建议再加一个宽限时间，例如迁移完成 5 分钟后再开始 GC：

```text
now - state.updated_at > 5min
```

原因：

- 避免刚完成迁移时 watch 还没全部 reload。
- 避免排队中的 retry/shadow 写还没完成。
- 避免误删迁移补偿链路还可能读取的数据。

## 和当前代码的接入点

### registry

当前 `internal/registry/etcd.go` 已经有：

```go
func (r *EtcdRegistry) UpdateStatus(ctx context.Context, status NodeStatus) error
```

继续保留：

- 新节点启动：`warming`
- 迁移完成：`active`
- 下线迁出：`draining`

新增 rebalance 状态不建议塞进单个节点的 `NodeInfo`，因为它是集群级状态，不是节点状态。

### picker

当前 `internal/client/picker.go` 会 watch `/cache/{service}/workers` 并重建三环。

这个逻辑可以不改。迁移协调器只需要：

- 读取当前 workers 列表。
- 根据 active/warming/draining 计算迁移任务。
- 不让 warming 提前变 active。

### migration manager

当前 `internal/mirgration` 里有：

```go
func (m *Manger) Push(ctx context.Context, newRing *consistenthash.Map) error
func (m *Manger) Pull(ctx context.Context, owner string, futureRing *consistenthash.Map) error
func (m *Manger) SendBatch(ctx context.Context, addr string, entries []cache.TransportEntry) error
```

建议最小调整：

- `Push` 不要一次扫描 `1000000`，改成循环分批扫描。
- 每批发送前检查当前 epoch 是否仍有效。
- 每批发送后保存 `last_key`。
- `SendBatch` 继续复用，不需要改协议也能工作。

### group

当前 `Group.Set`、`DeleteWithVersion`、`BatchSet` 已经有 version/tombstone 语义，这是“不丢数据”的核心。

建议补充：

- `BatchSet` 相同 version 跳过，减少重复写。
- 本地 GC 删除最好新增一个只删本地缓存的方法，避免走 `DeleteWithVersion` 产生 tombstone 和远程同步。

例如：

```go
func (g *Group) DeleteLocalForGC(key string) bool {
	if key == "" {
		return false
	}
	return g.mainCache.Delete(key)
}
```

如果当前 `Cache` 没有公开 `Delete`，就先不做 GC，等迁移状态稳定后再补。残留数据短期存在不是正确性问题，只是内存占用问题。

## 性能保证

### 正常读写性能

正常读写路径不查 rebalance state：

```text
Get/Set/Delete
  -> Picker 一致性哈希
  -> 本地内存或 gRPC
```

不要在每次读写都访问 etcd。etcd 只参与：

- 节点注册。
- 状态 watch。
- 迁移协调。
- 迁移进度保存。

这样不会把 etcd 放到热路径上。

### 迁移吞吐

迁移按批次执行：

```text
Scan 256 条
BatchSet 256 条
保存进度
继续下一批
```

`batchSize` 建议从 256 或 512 开始，后面根据数据大小压测调整。

批次太小：

- gRPC 次数多。
- etcd 进度写多。
- 吞吐低。

批次太大：

- 单次 RPC 时间长。
- 内存峰值高。
- 失败重试成本高。

### 不阻塞业务写入

迁移过程中：

- 正式写仍然走 old owner。
- 影子写异步执行，失败进入 retry queue。
- 迁移 worker 后台分批跑。

因此业务写入不会等全量迁移完成。

### 限流

当前 `Group.withPeerLimit` 已经限制每个 peer 并发：

```go
limiter := make(chan struct{}, 2)
```

迁移也应该使用类似限制。建议：

```text
每个目标节点最多 1 到 2 个迁移 BatchSet 并发
每个源节点最多 1 个 Scan worker
```

缓存系统优先保障在线读写，迁移吞吐可以让步。

### 进度保存频率

最简单是每批保存一次进度。性能压力大时可以改成：

```text
每 N 批保存一次
或每 1 秒保存一次
```

权衡：

- 保存越频繁，重启后重复迁移越少，但 etcd 写更多。
- 保存越少，etcd 压力低，但失败后会重传更多数据。

因为 `BatchSet` 幂等，重复迁移不会丢数据，只是多消耗一点网络和 CPU。

## 为什么这个方案能处理 A/B 场景

场景：

```text
A 先上线，开始一轮迁移 epoch=12。
B 后上线，按最新环计算，有些 A 已迁移的数据应该归 B。
```

处理过程：

```text
1. A 的迁移仍然继续完成 epoch=12。
2. B 上线时发现 state=migrating，不启动新迁移，只写 pending=true。
3. epoch=12 完成后，迁移 owner 把 state 改回 stable。
4. 因为 pending=true，马上基于最新节点列表启动 epoch=13。
5. epoch=13 会把按最新环应该归 B 的数据迁到 B。
6. 等 epoch=13 完成后，B 再进入 active。
7. 稳定一段时间后，各节点清理不归自己的本地残留数据。
```

中间 A 迁过去但后来归 B 的数据怎么办：

- 如果还在旧节点上：下一轮迁移会复制给 B。
- 如果已经在某个非 owner 节点上：正式路由不会使用它，后续 GC 清掉。
- 如果迁移和影子写重复到达：`version` 保证新版本胜出，旧版本不会覆盖。
- 如果删除和写入乱序：`tombstone` 保证旧写不会复活已删除数据。

## 最小落地清单

按最小成本实现，建议分四步：

1. 新增 `internal/rebalance` 包，放 `State`、`TryStart`、`MarkPending`、`Finish`。
2. 节点 watch 到 workers 变化时：
   - 如果 `stable`，尝试 `TryStart`。
   - 如果 `migrating`，调用 `MarkPending`。
3. 改造迁移 manager：
   - 分批 `Scan`。
   - 每批前检查 epoch。
   - 每批后保存进度。
4. 迁移完成后：
   - warming 节点改 active。
   - 如有 pending，启动下一轮。
   - 延迟 GC 非 owner 本地数据。

## 不建议的做法

不要在迁移没完成时直接让新节点 active。

不要每个节点只靠本地 watch 事件顺序各自计算并立即迁移。

不要同步失败或拓扑变化时马上删除本地数据。

不要在每次 Get/Set/Delete 热路径访问 etcd。

不要为了这个问题再实现一套 Raft。当前已经依赖 etcd，使用 etcd transaction 做迁移状态机就够了。

## 总结

最简单可靠的方法是：

```text
全局迁移状态机 + 单轮迁移 + pending rebalance + version 幂等写入 + 稳定后延迟 GC
```

这个方案把复杂问题拆开：

- etcd 负责“同一时间谁能发起迁移”。
- `epoch` 负责“旧迁移任务是否还有效”。
- `warming/active` 负责“什么时候切正式流量”。
- `version/tombstone` 负责“重复迁移和乱序写不丢数据”。
- GC 负责“非 owner 残留数据最终清理”。

这样不用大改现有架构，也不会把 etcd 放进读写热路径，性能损失主要局限在后台迁移任务本身。
