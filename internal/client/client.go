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

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Set(ctx context.Context, group string, key string, value []byte, version int64, ttl time.Duration) error {
	resp, err := c.cli.Set(ctx, &cachepb.SetRequest{
		Group:    group,
		Key:      key,
		TtlMs:    ttl.Milliseconds(),
		FromPeer: cache.IsPeer(ctx),
		Value:    value,
		Version:  version,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("set key : %s failed", key)
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, group string, key string, version int64) bool {
	resp, err := c.cli.Delete(ctx, &cachepb.DeleteRequest{
		Group:    group,
		Key:      key,
		Verison:  version,
		FromPeer: cache.IsPeer(ctx),
	})
	if err != nil {
		return false
	}
	if !resp.Ok {
		return false
	}
	return true
}

func (c *Client) Scan(ctx context.Context, group string, key string, count int64) ([]*cachepb.CacheEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.cli.Scan(ctx, &cachepb.ScanRequest{
		Group:    group,
		StartKey: key,
		Count:    count,
	})
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

func (c *Client) BatchSet(ctx context.Context, entries []*cachepb.CacheEntry) (bool, error) {
	if len(entries) == 0 { //代表没有entry需要send了
		return true, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.cli.BatchSet(ctx, &cachepb.BatchRequest{
		Entries: entries,
	})
	if err != nil {
		return false, err
	}
	if !resp.Ok {
		return false, fmt.Errorf("resp error")
	}
	return true, nil
}

func (c *Client) PeerID() string {
	return c.addr
}
