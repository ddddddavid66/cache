package client

import (
	"context"
	"fmt"
	cachepb "newCache/api/proto"
	"newCache/cache"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// gRPC 客户端
type Client struct {
	addr string
	conn *grpc.ClientConn
	cli  cachepb.CacheServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		addr: addr,
		conn: conn,
		cli:  cachepb.NewCacheServiceClient(conn),
	}, nil
}

//并且 向其他客户端发送请求 并且接受

func (c *Client) Get(ctx context.Context, group string, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond) // 超时防止 缓存无线等待节点
	defer cancel()
	resp, err := c.cli.Get(ctx, &cachepb.GetRequest{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Set 设置值
func (c *Client) Set(ctx context.Context, group string, key string, value []byte) error {
	resp, err := c.cli.Set(ctx, &cachepb.SetRequest{
		Group:    group,
		Key:      key,
		Value:    value,
		FromPeer: cache.IsPeer(ctx),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("set key: %s failed", key)
	}
	return nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
