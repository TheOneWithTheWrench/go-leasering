package leasering

import (
	"fmt"
	"hash/fnv"
)

// hashNodePosition calculates the deterministic ring position for a node's vnode.
// This ensures a restarted node reclaims its exact same positions.
func hashNodePosition(nodeID string, vnodeIndex int, ringSize int) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s:%d", nodeID, vnodeIndex)))
	return int(h.Sum32() % uint32(ringSize))
}
