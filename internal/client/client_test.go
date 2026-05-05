package client

import (
	"context"
	"testing"
	"time"

	cachepb "newCache/api/proto"
	"newCache/cache"

	"google.golang.org/grpc"
)

func TestClientSetRequestCarriesVersionAndFromPeer(t *testing.T) {
	fake := &recordingCacheServiceClient{}
	c := &Client{cli: fake}
	ctx := cache.WithPeer(context.Background())

	if err := c.Set(ctx, "users", "k", []byte("value"), 123, 2*time.Second); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	if fake.setReq == nil {
		t.Fatal("Set() did not send request")
	}
	if fake.setReq.Version != 123 {
		t.Fatalf("SetRequest.Version = %d, want 123", fake.setReq.Version)
	}
	if !fake.setReq.FromPeer {
		t.Fatal("SetRequest.FromPeer = false, want true")
	}
	if fake.setReq.TtlMs != 2000 {
		t.Fatalf("SetRequest.TtlMs = %d, want 2000", fake.setReq.TtlMs)
	}
}

func TestClientDeleteRequestCarriesVersionAndFromPeer(t *testing.T) {
	fake := &recordingCacheServiceClient{}
	c := &Client{cli: fake}
	ctx := cache.WithPeer(context.Background())

	if ok := c.Delete(ctx, "users", "k", 456); !ok {
		t.Fatal("Delete() = false, want true")
	}

	if fake.deleteReq == nil {
		t.Fatal("Delete() did not send request")
	}
	if fake.deleteReq.Verison != 456 {
		t.Fatalf("DeleteRequest.Verison = %d, want 456", fake.deleteReq.Verison)
	}
	if !fake.deleteReq.FromPeer {
		t.Fatal("DeleteRequest.FromPeer = false, want true")
	}
}

func TestClientScanRequestCarriesGroupStartKeyAndCount(t *testing.T) {
	fake := &recordingCacheServiceClient{
		scanResp: &cachepb.ScanResponse{Entries: []*cachepb.CacheEntry{
			{Group: "users", Key: "k", Value: []byte("value"), TtlMs: 123, Version: 456, Tombstone: true},
		}},
	}
	c := &Client{cli: fake}

	entries, err := c.Scan(context.Background(), "users", "start", 10)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if fake.scanReq == nil {
		t.Fatal("Scan() did not send request")
	}
	if fake.scanReq.Group != "users" || fake.scanReq.StartKey != "start" || fake.scanReq.Count != 10 {
		t.Fatalf("ScanRequest = %+v, want group/start/count", fake.scanReq)
	}
	if len(entries) != 1 || entries[0].Version != 456 || !entries[0].Tombstone || entries[0].TtlMs != 123 {
		t.Fatalf("Scan() entries = %+v, want version/tombstone/ttl preserved", entries)
	}
}

func TestClientBatchSetRequestCarriesMigrationFields(t *testing.T) {
	fake := &recordingCacheServiceClient{}
	c := &Client{cli: fake}
	entries := []*cachepb.CacheEntry{
		{Group: "users", Key: "k", Value: []byte("value"), TtlMs: 123, Version: 456, Tombstone: true},
	}

	ok, err := c.BatchSet(context.Background(), entries)
	if err != nil {
		t.Fatalf("BatchSet() error = %v", err)
	}
	if !ok {
		t.Fatal("BatchSet() ok = false, want true")
	}
	if fake.batchReq == nil {
		t.Fatal("BatchSet() did not send request")
	}
	if len(fake.batchReq.Entries) != 1 {
		t.Fatalf("BatchRequest entries len = %d, want 1", len(fake.batchReq.Entries))
	}
	got := fake.batchReq.Entries[0]
	if got.Group != "users" || got.Key != "k" || string(got.Value) != "value" || got.TtlMs != 123 || got.Version != 456 || !got.Tombstone {
		t.Fatalf("BatchRequest entry = %+v, want migration fields preserved", got)
	}
}

type recordingCacheServiceClient struct {
	setReq    *cachepb.SetRequest
	deleteReq *cachepb.DeleteRequest
	scanReq   *cachepb.ScanRequest
	scanResp  *cachepb.ScanResponse
	batchReq  *cachepb.BatchRequest
}

func (c *recordingCacheServiceClient) Get(ctx context.Context, in *cachepb.GetRequest, opts ...grpc.CallOption) (*cachepb.GetResponse, error) {
	return &cachepb.GetResponse{Value: []byte("value")}, nil
}

func (c *recordingCacheServiceClient) Set(ctx context.Context, in *cachepb.SetRequest, opts ...grpc.CallOption) (*cachepb.SetResponse, error) {
	cp := *in
	cp.Value = append([]byte(nil), in.Value...)
	c.setReq = &cp
	return &cachepb.SetResponse{Ok: true}, nil
}

func (c *recordingCacheServiceClient) Delete(ctx context.Context, in *cachepb.DeleteRequest, opts ...grpc.CallOption) (*cachepb.DeleteResponse, error) {
	cp := *in
	c.deleteReq = &cp
	return &cachepb.DeleteResponse{Ok: true}, nil
}

func (c *recordingCacheServiceClient) Scan(ctx context.Context, in *cachepb.ScanRequest, opts ...grpc.CallOption) (*cachepb.ScanResponse, error) {
	cp := *in
	c.scanReq = &cp
	if c.scanResp != nil {
		return c.scanResp, nil
	}
	return &cachepb.ScanResponse{}, nil
}

func (c *recordingCacheServiceClient) BatchSet(ctx context.Context, in *cachepb.BatchRequest, opts ...grpc.CallOption) (*cachepb.BatchResponse, error) {
	cp := *in
	cp.Entries = append([]*cachepb.CacheEntry(nil), in.Entries...)
	c.batchReq = &cp
	return &cachepb.BatchResponse{Ok: true}, nil
}
