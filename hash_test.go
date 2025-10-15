package leasering

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashNodePosition(t *testing.T) {
	var (
		nodeID   = "node-1"
		ringSize = 1024
	)

	t.Run("deterministic hashing", func(t *testing.T) {
		pos1 := hashNodePosition(nodeID, 0, ringSize)
		pos2 := hashNodePosition(nodeID, 0, ringSize)
		assert.Equal(t, pos1, pos2, "same input should produce same hash")
	})

	t.Run("different vnode indices produce different positions", func(t *testing.T) {
		pos1 := hashNodePosition(nodeID, 0, ringSize)
		pos2 := hashNodePosition(nodeID, 1, ringSize)
		assert.NotEqual(t, pos1, pos2, "different vnode indices should hash differently")
	})

	t.Run("different node IDs produce different positions", func(t *testing.T) {
		pos1 := hashNodePosition("node-1", 0, ringSize)
		pos2 := hashNodePosition("node-2", 0, ringSize)
		assert.NotEqual(t, pos1, pos2, "different node IDs should hash differently")
	})

	t.Run("position is within ring size", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			pos := hashNodePosition(nodeID, i, ringSize)
			assert.GreaterOrEqual(t, pos, 0, "position should be >= 0")
			assert.Less(t, pos, ringSize, "position should be < ringSize")
		}
	})
}
