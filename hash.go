package leasering

import (
	"crypto/sha256"
	"encoding/binary"
)

// hashNodePosition calculates the deterministic ring position for a node's vnode.
// This ensures a restarted node reclaims its exact same positions.
func hashNodePosition(nodeID string, vnodeIndex int, ringSize int) int {
	var indexBytes [4]byte
	binary.BigEndian.PutUint32(indexBytes[:], uint32(vnodeIndex))

	hasher := sha256.New()
	hasher.Write([]byte(nodeID))
	hasher.Write([]byte{0})
	hasher.Write(indexBytes[:])
	hashValue := binary.BigEndian.Uint64(hasher.Sum(nil)[:8])

	return int(hashValue % uint64(ringSize))
}
