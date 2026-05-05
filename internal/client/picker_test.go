package client

import (
	"fmt"
	"testing"

	"newCache/internal/consistenthash"
	"newCache/internal/registry"
)

func TestPickWritePeerReturnsSelfWhenKeyBelongsToSelf(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	remote := &Client{addr: "127.0.0.1:8002"}
	readRing, writeRing, shadowRing := ringsForAddrs(p.selfAddr, remote.addr)
	p.SetPeers([]string{p.selfAddr, remote.addr}, map[string]*Client{
		remote.addr: remote,
	}, readRing, writeRing, shadowRing)

	key := keyForAddr(t, p.writeRing, p.selfAddr)
	getter, ok, isSelf := p.PickWritePeer(key)
	if !ok {
		t.Fatalf("PickWritePeer(%q) ok = false, want true", key)
	}
	if !isSelf {
		t.Fatalf("PickWritePeer(%q) isSelf = false, want true", key)
	}
	if getter != nil {
		t.Fatalf("PickWritePeer(%q) getter = %v, want nil", key, getter)
	}
}

func TestPickReadPeerReturnsClientForRemoteNode(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	remote := &Client{addr: "127.0.0.1:8002"}
	readRing, writeRing, shadowRing := ringsForAddrs(p.selfAddr, remote.addr)
	p.SetPeers([]string{p.selfAddr, remote.addr}, map[string]*Client{
		remote.addr: remote,
	}, readRing, writeRing, shadowRing)

	key := keyForAddr(t, p.readRing, remote.addr)
	getter, ok, isSelf := p.PickReadPeer(key)
	if !ok {
		t.Fatalf("PickReadPeer(%q) ok = false, want true", key)
	}
	if isSelf {
		t.Fatalf("PickReadPeer(%q) isSelf = true, want false", key)
	}
	if getter != remote {
		t.Fatalf("PickReadPeer(%q) getter = %v, want %v", key, getter, remote)
	}
}

func TestSetPeersUpdatesRingsAndClientsWhenNodeAdded(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	first := &Client{addr: "127.0.0.1:8002"}
	second := &Client{addr: "127.0.0.1:8003"}

	readRing, writeRing, shadowRing := ringsForAddrs(p.selfAddr, first.addr)
	p.SetPeers([]string{p.selfAddr, first.addr}, map[string]*Client{
		first.addr: first,
	}, readRing, writeRing, shadowRing)

	readRing, writeRing, shadowRing = ringsForAddrs(p.selfAddr, first.addr, second.addr)
	p.SetPeers([]string{p.selfAddr, first.addr, second.addr}, map[string]*Client{
		first.addr:  first,
		second.addr: second,
	}, readRing, writeRing, shadowRing)

	if p.writeRing.Len() != 3*defaultReplicas {
		t.Fatalf("writeRing.Len() = %d, want %d", p.writeRing.Len(), 3*defaultReplicas)
	}
	if p.readRing.Len() != 3*defaultReplicas {
		t.Fatalf("readRing.Len() = %d, want %d", p.readRing.Len(), 3*defaultReplicas)
	}
	if p.shadowRing.Len() != 3*defaultReplicas {
		t.Fatalf("shadowRing.Len() = %d, want %d", p.shadowRing.Len(), 3*defaultReplicas)
	}
	if len(p.clients) != 2 {
		t.Fatalf("len(clients) = %d, want 2", len(p.clients))
	}

	key := keyForAddr(t, p.writeRing, second.addr)
	getter, ok, isSelf := p.PickWritePeer(key)
	if !ok {
		t.Fatalf("PickWritePeer(%q) ok = false, want true", key)
	}
	if isSelf {
		t.Fatalf("PickWritePeer(%q) isSelf = true, want false", key)
	}
	if getter != second {
		t.Fatalf("PickWritePeer(%q) getter = %v, want %v", key, getter, second)
	}
}

func TestSetPeersCleansRingsClientsAndConnectionsWhenNodeRemoved(t *testing.T) {
	closed := make(map[*Client]int)
	p := newTestPicker("127.0.0.1:8001", func(client *Client) error {
		closed[client]++
		return nil
	})
	first := &Client{addr: "127.0.0.1:8002"}
	second := &Client{addr: "127.0.0.1:8003"}

	readRing, writeRing, shadowRing := ringsForAddrs(p.selfAddr, first.addr, second.addr)
	p.SetPeers([]string{p.selfAddr, first.addr, second.addr}, map[string]*Client{
		first.addr:  first,
		second.addr: second,
	}, readRing, writeRing, shadowRing)

	readRing, writeRing, shadowRing = ringsForAddrs(p.selfAddr, second.addr)
	p.SetPeers([]string{p.selfAddr, second.addr}, map[string]*Client{
		second.addr: second,
	}, readRing, writeRing, shadowRing)

	if p.writeRing.Len() != 2*defaultReplicas {
		t.Fatalf("writeRing.Len() = %d, want %d", p.writeRing.Len(), 2*defaultReplicas)
	}
	if _, ok := p.clients[first.addr]; ok {
		t.Fatalf("removed client %s still exists", first.addr)
	}
	if p.clients[second.addr] != second {
		t.Fatalf("remaining client = %v, want %v", p.clients[second.addr], second)
	}
	if closed[first] != 1 {
		t.Fatalf("removed client close count = %d, want 1", closed[first])
	}
	if closed[second] != 0 {
		t.Fatalf("remaining client close count = %d, want 0", closed[second])
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if got := p.writeRing.Get(key); got == first.addr {
			t.Fatalf("writeRing.Get(%q) = removed addr %s", key, first.addr)
		}
	}
}

func TestBuildRingsKeepsWarmingNodeOutOfReadAndWrite(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	warming := "127.0.0.1:8002"

	readRing, writeRing, shadowRing := p.buildRings([]registry.NodeInfo{
		{Addr: p.selfAddr, Status: registry.StatusActive},
		{Addr: warming, Status: registry.StatusWarming},
	})

	assertRingDoesNotRouteTo(t, readRing, warming)
	assertRingDoesNotRouteTo(t, writeRing, warming)
	if shadowRing.Len() != 2*defaultReplicas {
		t.Fatalf("shadowRing.Len() = %d, want %d", shadowRing.Len(), 2*defaultReplicas)
	}
}

func TestBuildRingsKeepsDrainingNodeOutOfWrite(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	draining := "127.0.0.1:8002"

	readRing, writeRing, shadowRing := p.buildRings([]registry.NodeInfo{
		{Addr: p.selfAddr, Status: registry.StatusActive},
		{Addr: draining, Status: registry.StatusDraining},
	})

	if readRing.Len() != 2*defaultReplicas {
		t.Fatalf("readRing.Len() = %d, want %d", readRing.Len(), 2*defaultReplicas)
	}
	assertRingDoesNotRouteTo(t, writeRing, draining)
	assertRingDoesNotRouteTo(t, shadowRing, draining)
}

func TestBuildRingsPutsActiveNodeInAllThreeRings(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	remote := "127.0.0.1:8002"

	readRing, writeRing, shadowRing := p.buildRings([]registry.NodeInfo{
		{Addr: p.selfAddr, Status: registry.StatusActive},
		{Addr: remote, Status: registry.StatusActive},
	})

	if readRing.Len() != 2*defaultReplicas {
		t.Fatalf("readRing.Len() = %d, want %d", readRing.Len(), 2*defaultReplicas)
	}
	if writeRing.Len() != 2*defaultReplicas {
		t.Fatalf("writeRing.Len() = %d, want %d", writeRing.Len(), 2*defaultReplicas)
	}
	if shadowRing.Len() != 2*defaultReplicas {
		t.Fatalf("shadowRing.Len() = %d, want %d", shadowRing.Len(), 2*defaultReplicas)
	}
	if key := keyForAddr(t, readRing, remote); readRing.Get(key) != remote {
		t.Fatalf("readRing did not route selected key to active remote")
	}
	if key := keyForAddr(t, writeRing, remote); writeRing.Get(key) != remote {
		t.Fatalf("writeRing did not route selected key to active remote")
	}
	if key := keyForAddr(t, shadowRing, remote); shadowRing.Get(key) != remote {
		t.Fatalf("shadowRing did not route selected key to active remote")
	}
}

func TestPickShadowPeerReturnsClientForRemoteNode(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	remote := &Client{addr: "127.0.0.1:8002"}
	readRing, writeRing, shadowRing := ringsForAddrs(p.selfAddr, remote.addr)
	p.SetPeers([]string{p.selfAddr, remote.addr}, map[string]*Client{
		remote.addr: remote,
	}, readRing, writeRing, shadowRing)

	key := keyForAddr(t, p.shadowRing, remote.addr)
	getter, ok, isSelf := p.PickShadowPeer(key)
	if !ok {
		t.Fatalf("PickShadowPeer(%q) ok = false, want true", key)
	}
	if isSelf {
		t.Fatalf("PickShadowPeer(%q) isSelf = true, want false", key)
	}
	if getter != remote {
		t.Fatalf("PickShadowPeer(%q) getter = %v, want %v", key, getter, remote)
	}
}

func newTestPicker(selfAddr string, closeClient func(*Client) error) *Picker {
	return &Picker{
		selfAddr:    selfAddr,
		clients:     make(map[string]*Client),
		closeClient: closeClient,
	}
}

func ringsForAddrs(addrs ...string) (readRing, writeRing, shadowRing *consistenthash.Map) {
	readRing = consistenthash.NewMap(defaultReplicas, nil)
	writeRing = consistenthash.NewMap(defaultReplicas, nil)
	shadowRing = consistenthash.NewMap(defaultReplicas, nil)
	readRing.Add(addrs...)
	writeRing.Add(addrs...)
	shadowRing.Add(addrs...)
	return readRing, writeRing, shadowRing
}

func keyForAddr(t *testing.T, ring *consistenthash.Map, addr string) string {
	t.Helper()

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ring.Get(key) == addr {
			return key
		}
	}
	t.Fatalf("no key found for addr %s", addr)
	return ""
}

func assertRingDoesNotRouteTo(t *testing.T, ring *consistenthash.Map, addr string) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if got := ring.Get(key); got == addr {
			t.Fatalf("ring.Get(%q) = %s, want any other node", key, addr)
		}
	}
}
