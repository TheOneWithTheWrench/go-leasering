package leasering

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
)

// hashNodePosition calculates the deterministic ring position for a node's vnode.
// This ensures a restarted node reclaims its exact same positions.
func hashNodePosition(nodeID string, vnodeIndex int, ringSize int) int {
	var hash = md5.Sum([]byte(fmt.Sprintf("%s:%d", nodeID, vnodeIndex)))
	var hashValue = binary.BigEndian.Uint32(hash[:4])
	return int(hashValue % uint32(ringSize))
}
