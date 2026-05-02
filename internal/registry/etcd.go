package registry

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 负责etcd的逻辑 服务注册 服务发现 之类的
// 还有 watch  续租

type EtcdRegistry struct {
	cli     *clientv3.Client
	svcName string
	addr    string
	leaseID clientv3.LeaseID
	ttl     int64
}

func ServicePrefix(svcName string) string {
	return fmt.Sprintf("/cache/%s/", svcName)
}

func ServiceKey(svcName string, addr string) string {
	return ServicePrefix(svcName) + addr
}

// Endpoints is a list of URLs.
func NewEtcdRegistry(endpoints []string, svcName string, addr string) (*EtcdRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdRegistry{
		cli:     cli,
		svcName: svcName,
		addr:    addr,
		ttl:     10,
	}, nil
}

// 注册当前节点进入 etcd
func (r *EtcdRegistry) Register(ctx context.Context) error {
	key := ServiceKey(r.svcName, r.addr)
	leaseResp, err := r.cli.Grant(ctx, r.ttl)
	if err != nil {
		return err
	}
	r.leaseID = leaseResp.ID

	_, err2 := r.cli.Put(ctx, key, r.addr, clientv3.WithLease(r.leaseID))
	if err2 != nil {
		return err
	}
	keepAliveCh, err := r.cli.KeepAlive(ctx, r.leaseID)
	if err != nil {
		return err
	}
	// 创建go协程去 实现 消化响应
	go func() {
		for resp := range keepAliveCh {
			if resp == nil {
				return
			}
		}
	}()
	return nil
}

func (r *EtcdRegistry) Close(ctx context.Context) error {
	if r.leaseID != 0 {
		if _, err := r.cli.Revoke(ctx, r.leaseID); err != nil {
			_ = r.cli.Close()
			return err
		}
	}
	return r.cli.Close()
}
