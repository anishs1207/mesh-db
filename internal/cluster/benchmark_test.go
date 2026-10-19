package cluster

import (
	"fmt"
	"testing"
)

func BenchmarkRingOwners100Nodes(b *testing.B) {
	nodes := make([]string, 100)
	for index := range nodes {
		nodes[index] = fmt.Sprintf("node-%03d:7000", index)
	}
	ring := NewRing(nodes, DefaultVirtualNodes)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		owners := ring.Owners(fmt.Sprintf("key-%d", index), 3)
		if len(owners) != 3 {
			b.Fatalf("owners = %d", len(owners))
		}
	}
}
