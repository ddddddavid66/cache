package server

import (
	"context"
	"errors"
	"net"
	cachepb "newCache/api/proto"
	"newCache/cache"
	"newCache/internal/registry"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 把缓存能力暴露成为gPRC   也就是注册gRPC
// 它可以调用 cache.server 但是不应该具体实现

type Server struct {
	cachepb.UnimplementedCacheServiceServer

	addr     string
	svcName  string
	grpcSrv  *grpc.Server
	registry *registry.EtcdRegistry
}

func NewServer(addr string, svcName string, endpoints []string) (*Server, error) {
	r, err := registry.NewEtcdRegistry(endpoints, svcName, addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		svcName:  svcName,
		addr:     addr,
		registry: r,
		grpcSrv:  grpc.NewServer(),
	}, nil
}

func (s *Server) Start() error {
	return s.StartWithReady(nil)
}

// Start
// 1 监听tcp url:port 2 注册etcd服务 3 启动服务器并且循环监听
func (s *Server) StartWithReady(ready chan<- error) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		notifyReady(ready, err)
		return err
	}
	cachepb.RegisterCacheServiceServer(s.grpcSrv, s)
	if err := s.registry.Register(context.Background()); err != nil {
		notifyReady(ready, err)
		return err
	}

	notifyReady(ready, nil)

	return s.grpcSrv.Serve(lis)
}

func notifyReady(ready chan<- error, err error) {
	if ready == nil {
		return
	}
	ready <- err
	close(ready)
}

func (s *Server) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	if req.Group == "" || req.Key == "" {
		return nil, status.Errorf(codes.NotFound, "group %s or key %s not found", req.Group, req.Key)
	}
	g := cache.GetGroup(req.Group) //查询什么group
	if g == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}
	v, err := g.Get(cache.WithPeer(ctx), req.Key) // 标记一下
	if err != nil {
		return nil, err
	}
	return &cachepb.GetResponse{Value: v.ByteSlice()}, nil

}

func (s *Server) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	g := cache.GetGroup(req.Group)
	if g == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}
	if req.FromPeer {
		ctx = cache.WithPeer(ctx)
	}
	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if err := g.Set(ctx, req.Key, req.Value, req.Version, ttl); err != nil {
		return nil, err
	}
	return &cachepb.SetResponse{Ok: true}, nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.grpcSrv.GracefulStop()
	return s.registry.Close(ctx)
}

func (s *Server) WorkID() int64 {
	if s.registry == nil {
		return -1
	}
	return s.registry.WorkID()
}

func (s *Server) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	g := cache.GetGroup(req.Group)
	if g == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}
	if req.FromPeer {
		ctx = cache.WithPeer(ctx)
	}
	ok := g.DeleteWithVersion(ctx, req.Key, req.Verison)
	return &cachepb.DeleteResponse{Ok: ok}, nil
}

func (s *Server) Scan(ctx context.Context, req *cachepb.ScanRequest) (*cachepb.ScanResponse, error) {
	g := cache.GetGroup(req.Group)
	if g == nil {
		return &cachepb.ScanResponse{}, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}

	entries, err := g.Scan(req.StartKey, req.Count)
	resp := &cachepb.ScanResponse{Entries: make([]*cachepb.CacheEntry, 0, len(entries))}
	for _, entry := range entries {
		resp.Entries = append(resp.Entries, &cachepb.CacheEntry{
			Group:     entry.Group,
			Key:       entry.Key,
			Value:     entry.Value,
			TtlMs:     entry.TtlMs,
			Version:   entry.Version,
			Tombstone: entry.Tombstone,
		})
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Server) BatchSet(ctx context.Context, req *cachepb.BatchRequest) (*cachepb.BatchResponse, error) {
	if len(req.Entries) == 0 { // 成功完成
		return &cachepb.BatchResponse{Ok: true}, nil
	}
	g := cache.GetGroup(req.Entries[0].Group)
	if g == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Entries[0].Group)
	}
	entries := make([]cache.TransportEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, cache.TransportEntry{
			Group:     entry.Group,
			Key:       entry.Key,
			Value:     entry.Value,
			TtlMs:     entry.TtlMs,
			Version:   entry.Version,
			Tombstone: entry.Tombstone,
		})
	}
	if err := g.BatchSet(ctx, entries); err != nil {
		return nil, err
	}
	return &cachepb.BatchResponse{Ok: true}, nil
}

func (s *Server) Activate(ctx context.Context, req *cachepb.ActiveRequest) (*cachepb.ActiveResponse, error) {
	if s.registry == nil {
		return nil, errors.New("etcd registry not found")
	}
	if err := s.registry.UpdateStatus(ctx, registry.StatusActive); err != nil {
		return nil, status.Errorf(codes.Internal, "activate node: %v", err)
	}
	return &cachepb.ActiveResponse{Ok: true}, nil
}
