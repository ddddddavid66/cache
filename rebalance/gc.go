package rebalance

import (
	"context"
	"log"
	"newCache/cache"
	"newCache/internal/consistenthash"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	gcGracePeriod = 5 * time.Minute
	//为什么有呢？   刚完成迁移 picker还没有reload呢   消息队列里面的 操作没有执行 等待
	batchSize int64 = 256
)

// CanGc  判断能否执行gc
// state = Stable  并且 距离上一次迁移 过去了period
func CanGc(ctx context.Context, cli *clientv3.Client, svcName string) bool {
	state, err := LoadState(ctx, cli, svcName)
	if err != nil {
		return false
	}
	if state.State != StateStable {
		// 否则 迁移期间删除key 导致未命中 或者 数据源被删除
		return false
	}
	updatedAt := time.UnixMilli(state.UpdateAtUnixMs)
	return time.Since(updatedAt) > gcGracePeriod
}

func GCNonOwnerKeys(ctx context.Context, cli *clientv3.Client, svcName, selfAddr string, group *cache.Group, writeRing *consistenthash.Map) error {
	if !CanGc(ctx, cli, svcName) {
		return nil
	}
	startKey := ""
	gcCount := 0
	for {
		entries, err := group.Scan(startKey, batchSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			onwer := writeRing.Get(entry.Key)
			if onwer != selfAddr { //清理本地不是自己的key
				group.DeleteLocalForGC(entry.Key) //物理删除
				gcCount++
			}
			startKey = entry.Key
		}
	}
	if gcCount > 0 {
		log.Printf("[gc] cleaned %d non-owner keys", gcCount)
	}
	return nil
}
