package consistenthash

import (
	"fmt"
	"testing"
)

func TestEmptyRingReturnsEmptyString(t *testing.T) {
	m := NewMap(3, nil)

	if got := m.Get("any-key"); got != "" {
		t.Fatalf("Get on empty ring = %q, want empty string", got)
	}
}

func TestGetReturnsSameNodeForSameKeyAfterAdd(t *testing.T) {
	m := NewMap(50, nil)
	m.Add("node-a", "node-b", "node-c")

	first := m.Get("user:1001")
	for i := 0; i < 100; i++ {
		if got := m.Get("user:1001"); got != first {
			t.Fatalf("Get returned %q, want stable node %q", got, first)
		}
	}
}

func TestAddingNodeMovesOnlySomeKeys(t *testing.T) {
	before := NewMap(50, nil)
	before.Add("node-a", "node-b", "node-c")

	after := NewMap(50, nil)
	after.Add("node-a", "node-b", "node-c", "node-d")

	const total = 10000
	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if before.Get(key) != after.Get(key) {
			moved++
		}
	}

	if moved == 0 {
		t.Fatalf("adding node moved 0 keys, want some keys moved")
	}
	if moved == total {
		t.Fatalf("adding node moved all keys, want only some keys moved")
	}
}

func TestRemoveNodeNeverReturnsRemovedNode(t *testing.T) {
	m := NewMap(50, nil)
	m.Add("node-a", "node-b", "node-c")
	m.Remove("node-b")

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if got := m.Get(key); got == "node-b" {
			t.Fatalf("Get(%q) returned removed node %q", key, got)
		}
	}
}

func TestVirtualNodesImproveDistribution(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}

	oneReplica := NewMap(1, nil)
	oneReplica.Add(nodes...)

	manyReplicas := NewMap(50, nil)
	manyReplicas.Add(nodes...)

	const total = 10000
	oneReplicaSpread := distributionSpread(oneReplica, nodes, total)
	manyReplicasSpread := distributionSpread(manyReplicas, nodes, total)

	if manyReplicasSpread >= oneReplicaSpread {
		t.Fatalf("50 replicas spread = %d, want less than 1 replica spread %d", manyReplicasSpread, oneReplicaSpread)
	}
}

func distributionSpread(m *Map, nodes []string, total int) int {
	counts := make(map[string]int, len(nodes))
	for _, node := range nodes {
		counts[node] = 0
	}

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		counts[m.Get(key)]++
	}

	min := total
	max := 0
	for _, node := range nodes {
		count := counts[node]
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}
	return max - min
}
