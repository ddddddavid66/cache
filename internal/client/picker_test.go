package client

import (
	"fmt"
	"testing"
)

func TestPickPeerReturnsSelfWhenKeyBelongsToSelf(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	remote := &Client{addr: "127.0.0.1:8002"}
	p.SetPeers([]string{p.selfAddr, remote.addr}, map[string]*Client{
		remote.addr: remote,
	})

	key := keyForAddr(t, p, p.selfAddr)
	getter, ok, isSelf := p.PickPeer(key)
	if !ok {
		t.Fatalf("PickPeer(%q) ok = false, want true", key)
	}
	if !isSelf {
		t.Fatalf("PickPeer(%q) isSelf = false, want true", key)
	}
	if getter != nil {
		t.Fatalf("PickPeer(%q) getter = %v, want nil", key, getter)
	}
}

func TestPickPeerReturnsClientForRemoteNode(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	remote := &Client{addr: "127.0.0.1:8002"}
	p.SetPeers([]string{p.selfAddr, remote.addr}, map[string]*Client{
		remote.addr: remote,
	})

	key := keyForAddr(t, p, remote.addr)
	getter, ok, isSelf := p.PickPeer(key)
	if !ok {
		t.Fatalf("PickPeer(%q) ok = false, want true", key)
	}
	if isSelf {
		t.Fatalf("PickPeer(%q) isSelf = true, want false", key)
	}
	if getter != remote {
		t.Fatalf("PickPeer(%q) getter = %v, want %v", key, getter, remote)
	}
}

func TestSetPeersUpdatesRingAndClientsWhenNodeAdded(t *testing.T) {
	p := newTestPicker("127.0.0.1:8001", nil)
	first := &Client{addr: "127.0.0.1:8002"}
	second := &Client{addr: "127.0.0.1:8003"}

	p.SetPeers([]string{p.selfAddr, first.addr}, map[string]*Client{
		first.addr: first,
	})
	p.SetPeers([]string{p.selfAddr, first.addr, second.addr}, map[string]*Client{
		first.addr:  first,
		second.addr: second,
	})

	if p.ring.Len() != 3*defaultReplicas {
		t.Fatalf("ring.Len() = %d, want %d", p.ring.Len(), 3*defaultReplicas)
	}
	if len(p.clients) != 2 {
		t.Fatalf("len(clients) = %d, want 2", len(p.clients))
	}

	key := keyForAddr(t, p, second.addr)
	getter, ok, isSelf := p.PickPeer(key)
	if !ok {
		t.Fatalf("PickPeer(%q) ok = false, want true", key)
	}
	if isSelf {
		t.Fatalf("PickPeer(%q) isSelf = true, want false", key)
	}
	if getter != second {
		t.Fatalf("PickPeer(%q) getter = %v, want %v", key, getter, second)
	}
}

func TestSetPeersCleansRingClientsAndConnectionsWhenNodeRemoved(t *testing.T) {
	closed := make(map[*Client]int)
	p := newTestPicker("127.0.0.1:8001", func(client *Client) error {
		closed[client]++
		return nil
	})
	first := &Client{addr: "127.0.0.1:8002"}
	second := &Client{addr: "127.0.0.1:8003"}

	p.SetPeers([]string{p.selfAddr, first.addr, second.addr}, map[string]*Client{
		first.addr:  first,
		second.addr: second,
	})
	p.SetPeers([]string{p.selfAddr, second.addr}, map[string]*Client{
		second.addr: second,
	})

	if p.ring.Len() != 2*defaultReplicas {
		t.Fatalf("ring.Len() = %d, want %d", p.ring.Len(), 2*defaultReplicas)
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
		if got := p.ring.Get(key); got == first.addr {
			t.Fatalf("ring.Get(%q) = removed addr %s", key, first.addr)
		}
	}
}

func newTestPicker(selfAddr string, closeClient func(*Client) error) *Picker {
	return &Picker{
		selfAddr:    selfAddr,
		clients:     make(map[string]*Client),
		closeClient: closeClient,
	}
}

func keyForAddr(t *testing.T, p *Picker, addr string) string {
	t.Helper()

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if p.ring.Get(key) == addr {
			return key
		}
	}
	t.Fatalf("no key found for addr %s", addr)
	return ""
}
