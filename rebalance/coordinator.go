package rebalance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"newCache/cache"
	"newCache/internal/client"
	"newCache/internal/consistenthash"
	"newCache/internal/mirgration"
	"newCache/internal/registry"
	retryqueue "newCache/internal/retry-queue"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 迁移协调器
//监听etcd节点变化  抢占式启动迁移  分批次执行迁移任务  完成后切换节点状态 处理pendiung

var ErrMigration = errors.New("migration canceled epoch")

type Coordinator struct {
	cli      *clientv3.Client
	svcName  string
	selfAddr string
	group    *cache.Group           //复用 BatchSet 和 Scan
	manager  *mirgration.Manger     // 真正执行迁移的 工具
	registry *registry.EtcdRegistry // 更新节点状态

	mu       sync.Mutex
	running  bool               // 观察当前是否在迁移 避免重复迁移
	cancelFn context.CancelFunc // 取消迁移函数
	logger   retryqueue.Logger
	picker   *client.Picker
}

func NewCoordinator(cli *clientv3.Client, svcName, selfAddr string, group *cache.Group,
	manager *mirgration.Manger, registry *registry.EtcdRegistry, picker *client.Picker) *Coordinator {
	return &Coordinator{
		cli:      cli,
		svcName:  svcName,
		selfAddr: selfAddr,
		group:    group,
		manager:  manager,
		registry: registry,
		logger:   retryqueue.NewDefaultLogger(),
		picker:   picker,
	}
}

// 启动协调器 观察etcd
func (c *Coordinator) Start(ctx context.Context) {
	go c.watchAndBalance(ctx)
}

// worker 就是每一个集群里面的节点
func (c *Coordinator) watchAndBalance(ctx context.Context) {
	prefix := registry.ServicePrefix(c.svcName)
	for {
		watchCh := c.cli.Watch(ctx, prefix, clientv3.WithPrefix())
		for resp := range watchCh {
			if resp.Canceled {
				//说明有一个 worker 取消了 需要重新开启
				c.logger.Error("[coordinator] watch canceled", "err", resp.Err())
				break
			}
			//节点启动成功 尝试启动 迁移
			if err := c.tryRebalance(ctx); err != nil {
				c.logger.Error("[coordinator] rebalance failed", "err", err)
			}
		}
		select {
		case <-ctx.Done(): // 在这1s 期间 是可以被打断的  可以随时关闭
			return
		case <-time.After(time.Second): //如果 1 秒到了：继续往下执行
		}
	}
}

// 尝试迁移节点  stable就抢占 迁移过程中 pending
func (c *Coordinator) tryRebalance(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		// 标记pending
		c.mu.Unlock()
		return MarkPending(ctx, c.cli, c.svcName)
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	//尝试 抢占迁移
	state, ok, err := TryStart(ctx, c.cli, c.svcName, c.selfAddr)
	if err != nil {
		return err
	}
	if !ok { //没抢到  看看是不是需要 标记 pending
		currentState, err := LoadState(ctx, c.cli, c.svcName)
		if err != nil {
			return err
		}
		if currentState.State == StateMigrating {
			return MarkPending(ctx, c.cli, c.svcName)
		}
	}
	c.logger.Info("m[coordinator] started migration",
		"epoch", state.Epoch,
		"owner", c.selfAddr)

	//抢到了 执行迁移
	if err := c.runMigration(ctx, state); err != nil {
		c.logger.Error("[coordinator] migration",
			"epoch", state.Epoch,
			"failed", err)
		return err
	}
	return nil
}

// 执行一轮 迁移任务 1 读取当前的 活跃节点列表 2 计算迁移任务 3 分批次提交 4 完成提交状态
func (c *Coordinator) runMigration(ctx context.Context, state State) error {
	nodes, err := c.listActiveNodes(ctx)
	if err != nil {
		return err
	}
	//计算key达到哪里
	tasks, futureRing, hasWarmingNode := c.computeTask(state.Epoch, nodes)
	if !hasWarmingNode {
		c.logger.Info("[coordinator] no warming nodes", "epoch", state.Epoch)
		return c.finishMigration(ctx, state)
	}
	if len(tasks) == 0 {
		c.logger.Info("[coordinator] no migration tasks neede", "epoch", state.Epoch)
		return c.finishMigration(ctx, state)
	}
	for _, task := range tasks {
		if !ShouContinue(ctx, c.cli, c.svcName, c.selfAddr, state.Epoch) {
			return fmt.Errorf("%w epoch :%d", ErrMigration, state.Epoch)
		}
		if err := c.runTask(ctx, task, futureRing); err != nil {
			return fmt.Errorf("task %s: %w", task.TaskID, err)
		}
	}
	return c.finishMigration(ctx, state)
}

func (c *Coordinator) listActiveNodes(ctx context.Context) ([]registry.NodeInfo, error) {
	prefix := registry.ServicePrefix(c.svcName)
	resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	nodeList := make([]registry.NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info registry.NodeInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			c.logger.Error("[coordinator] invalid worker value",
				"key", string(kv.Key),
				"value", string(kv.Value),
				"error", err)
			continue
		}
		nodeList = append(nodeList, info)
	}
	return nodeList, nil
}

// 计算节点 应该达到哪里 创建任务
// owner不是自己 就应该生成 迁移任务
func (c *Coordinator) computeTask(epoch int64, nodes []registry.NodeInfo) ([]Task, *consistenthash.Map, bool) {
	activeAddrs := make([]string, 0)
	warmingAddrs := make([]string, 0)

	var selfIsActive bool // 粗略建造 会生成  warming -> active  的 任务
	for _, n := range nodes {
		if n.Status == registry.StatusActive {
			activeAddrs = append(activeAddrs, n.Addr)
		}
		if n.Status == registry.StatusWarming {
			warmingAddrs = append(warmingAddrs, n.Addr)
		}
		if n.Status == registry.StatusActive && n.Addr == c.selfAddr {
			selfIsActive = true
		}
	}

	if len(activeAddrs) == 0 || len(warmingAddrs) == 0 {
		return nil, nil, false
	}
	//如果 没有warming  不会迁移
	if len(activeAddrs)+len(warmingAddrs) <= 1 { // 如果只有自己一个 active 节点，不需要迁移
		if len(activeAddrs) <= 1 {
			return nil, nil, false
		}
		return nil, nil, false
	}

	var hasWarmingNode bool
	if len(warmingAddrs) != 0 {
		hasWarmingNode = true
	}

	//按照 key 计算应该 路由到哪里
	targetAddrs := append([]string{}, activeAddrs...) // 复制一份原来的切片
	targetAddrs = append(targetAddrs, warmingAddrs...)
	//创建环
	targerRing := consistenthash.NewMap(50, nil)
	targerRing.Add(targetAddrs...)

	//当前节点 只负责迁出自己的本地顺序 任务按照目标 节点拆分
	//NOTE runtask还需要过滤    targetRing.Get(key) == task.To 才发送。
	targets := make(map[string]struct{})
	for _, addr := range targetAddrs { // 只是 可能要给这些 到底能不能 还有看 runTask
		if addr != c.selfAddr {
			targets[addr] = struct{}{}
		}
	}
	tasks := make([]Task, 0, len(targets))
	for to := range targets {
		taskID := fmt.Sprintf("%d:%s->%s", epoch, c.selfAddr, to)
		tasks = append(tasks, Task{
			Epoch:    epoch,
			TaskID:   taskID,
			From:     c.selfAddr,
			To:       to,
			Status:   TaskStatusRunning,
			StartKey: "",
		})
	}
	if !selfIsActive {
		return nil, targerRing, hasWarmingNode
	}
	return tasks, targerRing, hasWarmingNode
}

func (c *Coordinator) runTask(ctx context.Context, task Task, futureRing *consistenthash.Map) error {
	if err := saveTask(ctx, c.cli, c.svcName, task); err != nil {
		// 登记 迁移任务
		return err
	}
	//登记迁移状态
	progress, err := loadProgess(ctx, c.cli, c.svcName, task.TaskID, task.Epoch)
	if err != nil {
		return err
	}

	startKey := progress.LastKey
	copied := progress.Copied

	// 过滤  顺带batchSet迁移
	for {
		if !ShouContinue(ctx, c.cli, c.svcName, c.selfAddr, task.Epoch) {
			return fmt.Errorf("%w epoch:%d", ErrMigration, task.Epoch)
		}
		entries, err := c.group.Scan(startKey, TaskBatchSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}

		filtered := make([]cache.TransportEntry, 0, len(entries))
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			if futureRing.Get(entry.Key) == task.To { //NOTE 核心过滤
				filtered = append(filtered, *entry)
			}
		}

		if len(filtered) > 0 {
			if err := c.manager.SendBatch(ctx, task.To, filtered); err != nil {
				return err
			}
			copied += int64(len(filtered))
		}
		//记录迁移进度 便于宕机恢复
		lastKey := entries[len(entries)-1].Key
		if err := saveProgess(ctx, c.cli, c.svcName, Progress{
			Epoch:   task.Epoch,
			TaskID:  task.TaskID,
			LastKey: lastKey,
			Copied:  copied,
		}); err != nil {
			return err
		}
		startKey = lastKey
	}
	// 标记任务完成
	task.Status = TaskStatusDone
	task.StartKey = ""
	if err != saveTask(ctx, c.cli, c.svcName, task) {
		return err
	}
	c.logger.Info("[coordinator] task done", "task", task.TaskID, "copied", copied)
	return nil
}

// 有pending 执行下一次迁移 没有 就切换节点切换
func (c *Coordinator) finishMigration(ctx context.Context, state State) error {
	startNext, err := Finsh(ctx, c.cli, c.svcName, c.selfAddr, state.Epoch)
	if err != nil {
		return err
	}
	//先  切换节点状态  要执行下一次迁移 还会吧状态切换掉
	if err := c.activeWarmingNodes(ctx); err != nil {
		c.logger.Error("[coordinator] activate warming nodes", "failed", err)
		// 不阻塞后续流程
	}
	if startNext { // 需要切换节点状态
		c.logger.Info("[coordinator] pending rebalance detected, starting next round")
		return c.tryRebalance(ctx)
	}
	c.logger.Info("[coordinator] migration  completed", "epoch", state.Epoch)
	return nil
}

// warming 节点 切换为 active
// NOTE  通过etcd signal key 通知 warming节点 自己 UpdateStatus
// warming 节点 有自己的 lease    UpdateStatus 会正确附加lease
// 带超时 重试    不会卡住
func (c *Coordinator) activeWarmingNodes(ctx context.Context) error {
	prefix := registry.ServicePrefix(c.svcName)
	resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	//收集所有的warming节点
	warmingAddrs := make([]string, 0)
	for _, kv := range resp.Kvs {
		var info registry.NodeInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			return err
		}
		if info.Status == registry.StatusWarming {
			warmingAddrs = append(warmingAddrs, info.Addr)
		}
	}
	if len(warmingAddrs) == 0 {
		return nil
	}
	//逐个通知他们去 激活
	var errs []error
	for _, addr := range warmingAddrs {
		if err := c.notifyActivate(ctx, addr); err != nil {
			c.logger.Error("[coordinator] activate", "node", addr, "failed", err)
			errs = append(errs, err)
			continue
		}
		c.logger.Info("[coordinator] activate", "node", addr)
	}
	if len(errs) > 0 && len(errs) == len(warmingAddrs) {
		return fmt.Errorf("failed to activate all %d warming nodes", len(warmingAddrs))
	}
	return nil
}

//如果直接写etcd
//每个节点创建的时候 都会创建租约 用租约写key 但是 coordinator 没有这个 lease，直接 Put 会覆盖掉 lease 绑定
//那么 这个节点下线的时候 key不会被自动删除

// 通过gRPC调用目标节点的激活 rpc  改proto  目标节点收到以后自己 更新状态
func (c *Coordinator) notifyActivate(ctx context.Context, addr string) error {
	// 通过picker找到 对应的gRPC
	if c.picker != nil {
		peer, err := c.picker.PickPeerByAddr(addr)
		if err != nil {
			return err
		}
		//调用RPC
		if activator, ok := peer.(interface {
			Activate(ctx context.Context) error
		}); ok {
			return activator.Activate(ctx)
		}
	}
	return errors.New("peer is nil")
}
