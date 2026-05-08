package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"newCache/cache"
	"newCache/internal/client"
	retryqueue "newCache/internal/retry-queue"
	"newCache/internal/server"
)

func main() {
	// 解析 参数 port node
	port := flag.Int("port", 8001, "传入端口")
	node := flag.String("node", "A", "传入的哪个节点")
	host := flag.String("host", "127.0.0.1", "监听 host")
	serviceName := flag.String("service", "new-cache", "etcd 服务名")
	groupName := flag.String("group", "scores", "缓存 group 名")
	etcd := flag.String("etcd", "http://127.0.0.1:2379", "etcd endpoints, 逗号分隔")
	dataDir := flag.String("data-dir", "./data", "节点数据目录")
	flag.Parse() // 解析命令行参数 否则永远都是默认值
	// 拼接addr
	addr := fmt.Sprintf("%s:%d", *host, *port)

	etcdEndpoints := strings.Split(*etcd, ",")

	log.Printf("[node %s] starting at %s", *node, addr)

	srv, err := server.NewServer(addr, *serviceName, etcdEndpoints) // 1 创建server
	if err != nil {
		log.Fatalf("new server failed: %v", err)
	}

	ready := make(chan error, 1) //NOTE 阻塞队列 等待server启动
	go func() {
		if err := srv.StartWithReady(ready); err != nil { //3 启动server
			log.Fatalf("server start failed: %v", err)
		}
	}()
	if err := <-ready; err != nil {
		log.Fatalf("server register  failed: %v", err) //4 server 注册etcd
	}
	workID := srv.WorkID()
	log.Printf("workerID=%d", workID)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("create data dir failed: %v", err)
	}
	retryDB := filepath.Join(*dataDir, fmt.Sprintf("retry-%s.db", *node))
	retryQueue, err := retryqueue.NewDurableRetryQueue(retryDB)
	if err != nil {
		log.Fatalf("new retry queue failed: %v", err)
	}
	defer retryQueue.Close()

	group := cache.NewGroup(*groupName, 2<<20, cache.GetterFunc( // 2 创建group
		func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[node %s] load from datasource, key=%s", *node, key)
			return []byte(fmt.Sprintf("value-from-node-%s-for-%s", *node, key)), nil
		},
	), workID, cache.WithRetryQueue(retryQueue))

	picker, err := client.NewPicker(etcdEndpoints, *serviceName, addr) //
	if err != nil {
		log.Fatalf("new picker failed: %v", err)
	}

	group.RegisterPeers(picker)

	ctx := context.Background()

	key := fmt.Sprintf("key_%s", *node)
	value := []byte(fmt.Sprintf("local-value-from-node-%s", *node))

	if err := group.Set(ctx, key, value, -1, 5*time.Minute); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	v, err := group.Get(ctx, key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}

	log.Printf("[node %s] get %s = %s", *node, key, v.String())

	select {}
}
