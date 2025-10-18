package leasering

import (
	"encoding/binary"
	"hash/fnv"
)

// hashNodePosition calculates the deterministic ring position for a node's vnode.
// This ensures a restarted node reclaims its exact same positions.
func hashNodePosition(nodeID string, vnodeIndex int, ringSize int) int {
	hasher := fnv.New64a()
	hasher.Write([]byte(nodeID))

	// This is like adding a semicolon between two things you want to append together
	// You want to prevent "test1, 23" and "test, 123" becoming the same hash due to a naive concatenation
	hasher.Write([]byte{0})

	var indexBytes [4]byte // A 32-bit integer is 4 bytes.
	binary.BigEndian.PutUint32(indexBytes[:], uint32(vnodeIndex))
	hasher.Write(indexBytes[:])

	hashValue := hasher.Sum64()

	return int(hashValue % uint64(ringSize))
}
