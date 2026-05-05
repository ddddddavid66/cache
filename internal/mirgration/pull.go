package mirgration

import (
	"context"
	"newCache/cache"
	"newCache/internal/consistenthash"
)

func (m *Manger) Pull(ctx context.Context, owner string, futureRing *consistenthash.Map) error {
	entries, err := m.group.Scan("", 100000)
	if err != nil {
		return err
	}
	batch := make([]cache.TransportEntry, 0, m.batchSize)
	for _, entry := range entries {
		//判断能否pull 也就是是不是自己的key
		if futureRing.Get(entry.Key) != owner {
			continue
		}
		batch = append(batch, *entry)
		if len(batch) >= m.batchSize {
			if err := m.SendBatch(ctx, owner, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return m.SendBatch(ctx, owner, batch)
	}
	return nil
}
