package mirgration

import (
	"context"
	cachepb "newCache/api/proto"
	"newCache/cache"
	"newCache/internal/client"
)

type Manger struct {
	group     *cache.Group
	picker    *client.Picker
	batchSize int
}

func NewManger(group *cache.Group, picker *client.Picker, batchSize int) *Manger {
	return &Manger{
		group:     group,
		picker:    picker,
		batchSize: 256,
	}
}

func (m *Manger) SendBatch(ctx context.Context, addr string, entries []cache.TransportEntry) error {
	peer, err := m.picker.PickPeerByAddr(addr)
	if err != nil {
		return err
	}
	//entries转换类型
	pbEntries := make([]*cachepb.CacheEntry, 0, len(entries))
	for _, entry := range entries {
		temp := &cachepb.CacheEntry{
			Group:     entry.Group,
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			TtlMs:     entry.TtlMs,
			Tombstone: entry.Tombstone,
		}
		pbEntries = append(pbEntries, temp)
	}
	ok, err := peer.BatchSet(ctx, pbEntries)
	if !ok {
		if err != nil {
			return err
		}
	}
	return nil
}
