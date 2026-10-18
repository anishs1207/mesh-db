package cluster

import (
	"fmt"
	"reflect"
	"testing"
)

func TestOwnersAreDeterministicAndDistinct(t *testing.T) {
	a := NewRing([]string{"node-c", "node-a", "node-b", "node-a"}, 64)
	b := NewRing([]string{"node-b", "node-c", "node-a"}, 64)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		ownersA := a.Owners(key, 3)
		ownersB := b.Owners(key, 3)
		if !reflect.DeepEqual(ownersA, ownersB) {
			t.Fatalf("placement differs for %q: %v vs %v", key, ownersA, ownersB)
		}
		seen := map[string]bool{}
		for _, owner := range ownersA {
			if seen[owner] {
				t.Fatalf("duplicate owner for %q: %v", key, ownersA)
			}
			seen[owner] = true
		}
	}
}

func TestAddingNodeMovesMinorityOfKeys(t *testing.T) {
	before := NewRing([]string{"a", "b", "c"}, 256)
	after := NewRing([]string{"a", "b", "c", "d"}, 256)
	moved := 0
	const keys = 10000
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("account:%d", i)
		if before.Owners(key, 1)[0] != after.Owners(key, 1)[0] {
			moved++
		}
	}
	ratio := float64(moved) / keys
	if ratio < 0.15 || ratio > 0.35 {
		t.Fatalf("unexpected movement ratio %.3f", ratio)
	}
}

func TestReplicationFactorIsCappedByNodes(t *testing.T) {
	ring := NewRing([]string{"a", "b"}, 32)
	if owners := ring.Owners("key", 5); len(owners) != 2 {
		t.Fatalf("owners = %v", owners)
	}
}
