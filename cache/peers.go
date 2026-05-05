package cache

import (
	"context"
	cachepb "newCache/api/proto"
	"time"
)

type PeerPicker interface {
	PickWritePeer(key string) (peer PeerGetter, ok bool, isSelf bool)
	PickReadPeer(key string) (peer PeerGetter, ok bool, isSelf bool)
	PickShadowPeer(key string) (peer PeerGetter, ok bool, isSelf bool)
}

type PeerGetter interface {
	Get(ctx context.Context, group string, key string) ([]byte, error)
	Delete(ctx context.Context, group string, key string, version int64) bool
	Set(ctx context.Context, group string, key string, value []byte, version int64, ttl time.Duration) error
	BatchSet(ctx context.Context, entries []*cachepb.CacheEntry) (bool, error)
	Scan(ctx context.Context, group string, key string, count int64) ([]*cachepb.CacheEntry, error)
}

//`Group` 只依赖这两个接口。
// 至于底层是 gRPC、HTTP，还是测试里的假实现，`Group` 不需要知道。
// 因为Group就是个接口 不需要 知道太多
