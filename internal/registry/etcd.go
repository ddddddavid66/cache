package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var maxID int64 = 1023

type NodeStatus string

type NodeInfo struct {
	Addr   string     `json:"addr"`
	Status NodeStatus `json:"status"`
}

const ( // status 状态
	StatusWarming  NodeStatus = "warming" // 新节点刚启动
	StatusActive   NodeStatus = "active"
	StatusDraining NodeStatus = "draining" // 准备下限 迁出数据
)

// 负责etcd的逻辑 服务注册 服务发现 之类的
// 还有 watch  续租

type EtcdRegistry struct {
	cli     *clientv3.Client
	svcName string
	addr    string
	leaseID clientv3.LeaseID
	ttl     int64
	workID  int64 // ID 唯一的用于生成版本号
}

func ServicePrefix(svcName string) string {
	return fmt.Sprintf("/cache/%s/workers", svcName) //NOTE 区分不同节点 防止脏节点
}

func ServiceKey(svcName string, addr string) string {
	return ServicePrefix(svcName) + addr
}

func WorkIDPrefix(svcName string) string {
	return fmt.Sprintf("/cache/%s/nodes", svcName)
}

func WorkIdKey(svcName string, id int64) string {
	return fmt.Sprintf("%s%d", WorkIDPrefix(svcName), id)
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

	// 生成 WorkID
	workID, err := r.allocateWorkID(ctx, r.leaseID, maxID)
	if err != nil {
		_, e := r.cli.Revoke(ctx, r.leaseID)
		if e != nil {
			log.Println("revoke failed:", err)
		}
		return err
	}
	r.workID = workID

	info := NodeInfo{ //NOTE 添加 addr 和  status
		Addr:   r.addr,
		Status: StatusWarming,
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	_, err = r.cli.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))

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

func (r *EtcdRegistry) allocateWorkID(ctx context.Context, leaseID clientv3.LeaseID, maxID int64) (int64, error) {
	for id := int64(1); id <= maxID; id++ {
		key := WorkIdKey(r.svcName, id)
		resp, err := r.cli.Txn(ctx). // 原子性事务
						If(clientv3.Compare(clientv3.Version(key), "=", 0)).
						Then(clientv3.OpPut(key, r.addr, clientv3.WithLease(leaseID))).Commit()
		if err != nil {
			return -1, err
		}
		if resp.Succeeded {
			return id, nil
		}
	}
	return -1, fmt.Errorf("no available id")
}

func (r *EtcdRegistry) WorkID() int64 {
	return r.workID
}

func (r *EtcdRegistry) UpdateStatus(ctx context.Context, status NodeStatus) error {
	if r.leaseID == 0 {
		return fmt.Errorf("registry is nil")
	}
	info := NodeInfo{
		Addr:   r.addr,
		Status: status,
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	key := ServiceKey(r.svcName, r.addr)
	_, err = r.cli.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
	return err
}
