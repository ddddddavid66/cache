package mirgration

import (
	"context"
	"newCache/cache"
	"newCache/internal/consistenthash"
)

func (m *Manger) Push(ctx context.Context, newRing *consistenthash.Map) error {
	entries, err := m.group.Scan("", 1000000)
	if err != nil {
		return err
	}
	byPeer := make(map[string][]cache.TransportEntry)
	for _, entry := range entries {
		addr := newRing.Get(entry.Key)
		byPeer[addr] = append(byPeer[addr], *entry)
	}
	for addr, list := range byPeer {
		if addr == "" {
			return nil
		}
		if err := m.SendBatch(ctx, addr, list); err != nil {
			return err
		}
	}
	return nil
}
