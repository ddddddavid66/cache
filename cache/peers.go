package cache

import "context"

type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool, isSelf bool)
}

type PeerGetter interface {
	Get(ctx context.Context, group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error // 实现远程同步 s
}

//`Group` 只依赖这两个接口。
// 至于底层是 gRPC、HTTP，还是测试里的假实现，`Group` 不需要知道。
// 因为Group就是个接口 不需要 知道太多
