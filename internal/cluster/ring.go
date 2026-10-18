// Package cluster contains deterministic data-placement primitives.
package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
)

const DefaultVirtualNodes = 128

type point struct {
	hash uint64
	node string
}

// Ring is an immutable consistent-hash ring. A node appears at multiple
// virtual points to reduce placement skew.
type Ring struct {
	points []point
	nodes  []string
}

func NewRing(nodes []string, virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	unique := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node != "" {
			unique[node] = struct{}{}
		}
	}
	sortedNodes := make([]string, 0, len(unique))
	for node := range unique {
		sortedNodes = append(sortedNodes, node)
	}
	sort.Strings(sortedNodes)

	points := make([]point, 0, len(sortedNodes)*virtualNodes)
	for _, node := range sortedNodes {
		for replica := 0; replica < virtualNodes; replica++ {
			identity := node + "#" + strconv.Itoa(replica)
			points = append(points, point{hash: hash(identity), node: node})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash == points[j].hash {
			return points[i].node < points[j].node
		}
		return points[i].hash < points[j].hash
	})
	return &Ring{points: points, nodes: sortedNodes}
}

// Owners returns up to replicationFactor distinct owners in ring order.
func (r *Ring) Owners(key string, replicationFactor int) []string {
	if len(r.points) == 0 || replicationFactor <= 0 {
		return nil
	}
	if replicationFactor > len(r.nodes) {
		replicationFactor = len(r.nodes)
	}
	start := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= hash(key)
	})
	if start == len(r.points) {
		start = 0
	}

	owners := make([]string, 0, replicationFactor)
	seen := make(map[string]struct{}, replicationFactor)
	for offset := 0; len(owners) < replicationFactor && offset < len(r.points); offset++ {
		node := r.points[(start+offset)%len(r.points)].node
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		owners = append(owners, node)
	}
	return owners
}

func (r *Ring) Nodes() []string {
	return append([]string(nil), r.nodes...)
}

func hash(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(sum[:8])
}
