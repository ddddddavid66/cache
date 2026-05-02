package cache

import "context"

// 缓存没有命中的时候 调用getter

// 这里定义了 Getter接口 然后又定义看了一个函数 的接口
// 就是为了书写的时候 方便 不会造成结构体不简洁

//比如业务的时候 直接传函数就行
// getter := cache.func(ctx context.Context, key string) ([]byte, error) {
// return []byte("value from db"), nil
/// })etterFunc(ctx)

type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}
