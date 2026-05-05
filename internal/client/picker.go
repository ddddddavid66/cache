package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"newCache/cache"
	"newCache/internal/consistenthash"
	"newCache/internal/registry"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Picker 是教学版节点选择器。
// 作用：根据 key 从节点列表中选择一个远程缓存节点。

type Picker struct {
	mu          sync.RWMutex
	selfAddr    string
	nodes       []string            // 所有的 ip:port 集合
	readRing    *consistenthash.Map // 读环 draining 状态 node准备下线
	writeRing   *consistenthash.Map //写环 正常状态
	shadowRing  *consistenthash.Map // 双写 warming状态 node准备上线
	clients     map[string]*Client  // string 是 ip : port  etcd里面存储的才包括前缀
	etcdCli     *clientv3.Client    // 比如 /cache/david-cache /   127.0.0.1:8001
	prefix      string              //  比如 /cache/david-cache /   127.0.0.1:8001
	newClient   func(string) (*Client, error)
	closeClient func(*Client) error
}

//- 正常运行时仍然使用真实实现： ai加的
// - newClient: NewClient
//- closeClient: (*Client).Close

const defaultReplicas = 50

func NewPicker(endpoints []string, svcName string, selfAddr string) (*Picker, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	p := &Picker{
		selfAddr:    selfAddr,
		nodes:       make([]string, 0),
		readRing:    consistenthash.NewMap(defaultReplicas, nil),
		writeRing:   consistenthash.NewMap(defaultReplicas, nil),
		shadowRing:  consistenthash.NewMap(defaultReplicas, nil),
		clients:     make(map[string]*Client),
		prefix:      registry.ServicePrefix(svcName),
		etcdCli:     etcdCli,
		newClient:   NewClient,
		closeClient: (*Client).Close,
	}
	if err = p.reload(context.Background()); err != nil {
		return nil, err
	}
	go p.watch()
	return p, nil
}

// 如果选中自己，返回 ok=true 且 isSelf=true，让 Group 走本地缓存或本地回源。
func (p *Picker) PickWritePeer(key string) (peer cache.PeerGetter, ok bool, isSelf bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if key == "" || p.writeRing == nil || p.writeRing.Len() == 0 {
		return nil, false, false
	}
	addr := p.writeRing.Get(key)
	if addr == p.selfAddr {
		return nil, true, true
	}
	client, ok := p.clients[addr]
	if !ok {
		return nil, false, false
	}
	return client, true, false
}
func (p *Picker) PickReadPeer(key string) (peer cache.PeerGetter, ok bool, isSelf bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if key == "" || p.readRing == nil || p.readRing.Len() == 0 {
		return nil, false, false
	}
	addr := p.readRing.Get(key)
	if addr == p.selfAddr {
		return nil, true, true
	}
	client, ok := p.clients[addr]
	if !ok {
		return nil, false, false
	}
	return client, true, false
}
func (p *Picker) PickShadowPeer(key string) (peer cache.PeerGetter, ok bool, isSelf bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if key == "" || p.shadowRing == nil || p.shadowRing.Len() == 0 {
		return nil, false, false
	}
	addr := p.shadowRing.Get(key)
	if addr == p.selfAddr {
		return nil, true, true
	}
	client, ok := p.clients[addr]
	if !ok {
		return nil, false, false
	}
	return client, true, false
}

func (p *Picker) PickPeerByAddr(addr string) (cache.PeerGetter, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if addr == "" {
		return nil, fmt.Errorf("addr is nil")
	}
	client, ok := p.clients[addr]
	if !ok {
		return nil, fmt.Errorf("the client of addr:%s error", addr)
	}
	return client, nil
}

func (p *Picker) Close() error {
	p.mu.Lock()
	clients := p.clients
	p.nodes = nil
	p.writeRing = consistenthash.NewMap(defaultReplicas, nil)
	p.readRing = consistenthash.NewMap(defaultReplicas, nil)
	p.shadowRing = consistenthash.NewMap(defaultReplicas, nil)
	p.clients = nil
	etcdCli := p.etcdCli
	closeClient := p.closeClient
	p.mu.Unlock()
	// 锁外关闭连接

	for _, client := range clients {
		_ = closePickerClient(closeClient, client)
	}
	if etcdCli == nil { // etcdCli == nil 时直接返回，方便测试构造不带 etcd 的 Picker。
		return nil
	}
	return etcdCli.Close()
}

func (p *Picker) reload(ctx context.Context) error {
	resp, err := p.etcdCli.Get(ctx, p.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	nodes := make([]registry.NodeInfo, 0, len(resp.Kvs))
	addrs := make([]string, 0, len(resp.Kvs))
	clients := make(map[string]*Client)
	oldClients := p.snapshotClients()
	for _, kv := range resp.Kvs {
		var info registry.NodeInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			info = registry.NodeInfo{
				Addr:   string(kv.Value),
				Status: registry.StatusActive,
			}
		}
		addr := info.Addr // NOTE 细节 兼容
		addrs = append(addrs, addr)
		nodes = append(nodes, info)
		if addr == p.selfAddr {
			continue
		}
		if client, ok := oldClients[addr]; ok { // 旧节点 直接添加
			clients[addr] = client
			continue
		}
		client, err := p.newClient(addr) // 新节点才创建
		if err != nil {
			continue
		}
		clients[addr] = client
	}
	sort.Strings(addrs) // 排序

	readRing, writeRing, shadowRing := p.buildRings(nodes) // 初始化环

	p.SetPeers(addrs, clients, readRing, writeRing, shadowRing)
	return nil
}

func (p *Picker) SetPeers(nodes []string, clients map[string]*Client, readRing, writeRing, shadowRing *consistenthash.Map) {
	copiedNodes := append([]string(nil), nodes...)

	copiedMap := make(map[string]*Client, len(clients))
	for addr, client := range clients {
		copiedMap[addr] = client
	}

	p.mu.Lock()
	oldClients := p.clients
	closeClient := p.closeClient
	p.clients = copiedMap
	p.nodes = copiedNodes
	p.writeRing = writeRing // reload 里面替换环
	p.readRing = readRing
	p.shadowRing = shadowRing
	p.mu.Unlock()

	for addr, oldClient := range oldClients {
		if newClient, ok := copiedMap[addr]; !ok || newClient != oldClient {
			_ = closePickerClient(closeClient, oldClient) // 关闭旧的clinet
		}
	}
}

func (p *Picker) snapshotClients() map[string]*Client { // 获取 旧节点
	p.mu.RLock()
	defer p.mu.RUnlock()

	clients := make(map[string]*Client, len(p.clients))
	for addr, client := range p.clients {
		clients[addr] = client
	}
	return clients
}

func (p *Picker) watch() { // 优化watch  打日志
	for {
		watchCh := p.etcdCli.Watch(context.Background(), p.prefix, clientv3.WithPrefix())
		for resp := range watchCh {
			if resp.Canceled {
				log.Printf("[picker] etcd watch canceled: %v", resp.Err())
				break
			}
			if err := p.reload(context.Background()); err != nil {
				log.Printf("[picker] reload peers failed: %v", err)
			}
		}
		time.Sleep(time.Second)
	}
}

func closePickerClient(closeClient func(*Client) error, client *Client) error {
	if closeClient != nil {
		return closeClient(client)
	}
	return client.Close()
}

func (p *Picker) buildRings(nodes []registry.NodeInfo) (readRing, writeRing, shadowRing *consistenthash.Map) {
	readRing = consistenthash.NewMap(defaultReplicas, nil)
	writeRing = consistenthash.NewMap(defaultReplicas, nil)
	shadowRing = consistenthash.NewMap(defaultReplicas, nil)
	for _, n := range nodes {
		switch n.Status {
		case registry.StatusActive:
			readRing.Add(n.Addr)
			writeRing.Add(n.Addr)
			shadowRing.Add(n.Addr)
		case registry.StatusWarming:
			shadowRing.Add(n.Addr)
		case registry.StatusDraining:
			readRing.Add(n.Addr)
		}
	}
	return readRing, writeRing, shadowRing
}
