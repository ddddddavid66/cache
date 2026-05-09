package mirgration

import (
	"context"
	"newCache/cache"
	"newCache/internal/consistenthash"
)

func (m *Manger) Push(ctx context.Context, newRing *consistenthash.Map, batchSize int64) error {
	startKey := ""
	for {
		entries, err := m.group.Scan(startKey, batchSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}

		byPeer := make(map[string][]cache.TransportEntry)
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			addr := newRing.Get(entry.Key)
			byPeer[addr] = append(byPeer[addr], *entry)
		}

		for addr, list := range byPeer {
			if err := m.SendBatch(ctx, addr, list); err != nil {
				return err
			}
		}

		startKey = entries[len(entries)-1].Key
	}
}
