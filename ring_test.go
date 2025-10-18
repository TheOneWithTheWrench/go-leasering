package leasering

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRing(t *testing.T) {
	var (
		newRing = func() *Ring {
			return NewRing(nil, "test_ring")
		}
		newLease = func(position int, nodeID string, vnodeIdx int) *lease {
			return &lease{
				Position:  position,
				NodeID:    nodeID,
				VNodeIdx:  vnodeIdx,
				ExpiresAt: time.Now().Add(30 * time.Second),
			}
		}
	)

	t.Run("should create new ring with correct defaults", func(t *testing.T) {
		// Arrange & Act
		var sut = newRing()

		// Assert
		require.NotNil(t, sut)
		assert.Equal(t, "test_ring", sut.ringID)
		assert.NotEmpty(t, sut.nodeID, "nodeID should be generated")
		assert.NotNil(t, sut.nodes)
		assert.NotNil(t, sut.vnodes)
		assert.NotNil(t, sut.ownedPartitions)
		assert.Equal(t, 1024, sut.options.ringSize)
		assert.Equal(t, 8, sut.options.vnodeCount)
	})

	t.Run("should return empty partitions for empty ring", func(t *testing.T) {
		// Arrange
		var sut = newRing()

		// Act
		var partitions = sut.GetOwnedPartitions()

		// Assert
		assert.Empty(t, partitions)
	})

	t.Run("should rebuild from leases with single node", func(t *testing.T) {
		// Arrange
		var sut = newRing()
		var leases = []*lease{
			newLease(100, sut.nodeID, 0),
			newLease(500, sut.nodeID, 1),
		}

		// Act
		sut.rebuildFromLeases(leases)

		// Assert
		assert.Len(t, sut.vnodes, 2)
		assert.Equal(t, 100, sut.vnodes[0].Position)
		assert.Equal(t, 500, sut.vnodes[1].Position)

		// Single node owns entire ring
		var partitions = sut.GetOwnedPartitions()
		assert.Equal(t, sut.options.ringSize, len(partitions))
	})

	t.Run("should rebuild from leases with multiple nodes", func(t *testing.T) {
		// Arrange
		var sut = newRing()
		var leases = []*lease{
			newLease(100, sut.nodeID, 0),
			newLease(200, "node-2", 0),
			newLease(500, sut.nodeID, 1),
			newLease(800, "node-2", 1),
		}

		// Act
		sut.rebuildFromLeases(leases)

		// Assert
		assert.Len(t, sut.vnodes, 4)
		assert.Equal(t, 100, sut.vnodes[0].Position)
		assert.Equal(t, 200, sut.vnodes[1].Position)
		assert.Equal(t, 500, sut.vnodes[2].Position)
		assert.Equal(t, 800, sut.vnodes[3].Position)

		// sut.nodeID owns: (800, 100] and (200, 500]
		var partitions = sut.GetOwnedPartitions()
		assert.NotEmpty(t, partitions)

		// Calculate expected: (800, 1023] + [0, 100] + (200, 500]
		var expectedCount = (1024 - 800 - 1) + 100 + 1 + (500 - 200)
		assert.Equal(t, expectedCount, len(partitions))
	})

	t.Run("should calculate owned partitions correctly for wrap around case", func(t *testing.T) {
		// Arrange
		var sut = newRing()
		var leases = []*lease{
			newLease(50, "node-2", 0),
			newLease(900, sut.nodeID, 0),
		}

		// Act
		sut.rebuildFromLeases(leases)
		var partitions = sut.GetOwnedPartitions()

		// Assert
		assert.NotEmpty(t, partitions)

		// sut.nodeID at 900 owns (50, 900]
		var expectedCount = 900 - 50
		assert.Equal(t, expectedCount, len(partitions))
	})

	t.Run("should calculate owned partitions for first vnode wrapping to last", func(t *testing.T) {
		// Arrange
		var sut = newRing()
		var leases = []*lease{
			newLease(100, sut.nodeID, 0),
			newLease(900, "node-2", 0),
		}

		// Act
		sut.rebuildFromLeases(leases)
		var partitions = sut.GetOwnedPartitions()

		// Assert
		assert.NotEmpty(t, partitions)

		// sut.nodeID at 100 owns (900, 100]
		var expectedCount = (1024 - 900 - 1) + 100 + 1
		assert.Equal(t, expectedCount, len(partitions))
	})

	t.Run("should find predecessor for position between vnodes", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
				newLease(900, "node-3", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		var pred = sut.findPredecessor(600)

		// Assert
		assert.Equal(t, 500, pred)
	})

	t.Run("should find predecessor that wraps around", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
				newLease(900, "node-3", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		var pred = sut.findPredecessor(50)

		// Assert
		assert.Equal(t, 900, pred)
	})

	t.Run("should find predecessor of exact vnode position", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
				newLease(900, "node-3", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		var pred = sut.findPredecessor(500)

		// Assert
		assert.Equal(t, 100, pred)
	})

	t.Run("should return -1 for predecessor in empty ring", func(t *testing.T) {
		// Arrange
		var sut = newRing()

		// Act
		var pred = sut.findPredecessor(500)

		// Assert
		assert.Equal(t, -1, pred)
	})

	t.Run("should get my vnode positions", func(t *testing.T) {
		// Arrange
		var sut = newRing()

		// With random node-ids, there's a small chance of self-collision
		// If detected, regenerate and try again (like production code does)
		const maxAttempts = 5
		var positions []int
		for attempt := 0; attempt < maxAttempts; attempt++ {
			positions = sut.getMyVNodePositions()

			// Check if positions are unique (no self-collision)
			var seen = make(map[int]bool)
			var hasSelfCollision = false
			for _, pos := range positions {
				if seen[pos] {
					hasSelfCollision = true
					break
				}
				seen[pos] = true
			}

			if !hasSelfCollision {
				break // Success!
			}

			// Self-collision detected - regenerate node-id and retry
			if attempt < maxAttempts-1 {
				sut.regenerateNodeID()
			} else {
				t.Fatal("failed to generate collision-free node-id after max attempts")
			}
		}

		// Assert
		assert.Len(t, positions, sut.options.vnodeCount)

		// Verify all positions are within ring size
		for _, pos := range positions {
			assert.GreaterOrEqual(t, pos, 0)
			assert.Less(t, pos, sut.options.ringSize)
		}
	})

	t.Run("should get successor position between vnodes", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
				newLease(900, "node-3", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		var succ = sut.getSuccessorPosition(200)

		// Assert
		assert.Equal(t, 500, succ)
	})

	t.Run("should get successor that wraps around", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
				newLease(900, "node-3", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		var succ = sut.getSuccessorPosition(950)

		// Assert
		assert.Equal(t, 100, succ)
	})

	t.Run("should return -1 for successor in empty ring", func(t *testing.T) {
		// Arrange
		var sut = newRing()

		// Act
		var succ = sut.getSuccessorPosition(500)

		// Assert
		assert.Equal(t, -1, succ)
	})

	t.Run("should get vnode at position when it exists", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		vnode, found := sut.getVNodeAtPosition(100)

		// Assert
		require.True(t, found)
		assert.Equal(t, "node-1", vnode.NodeID)
		assert.Equal(t, 100, vnode.Position)
	})

	t.Run("should return nil for vnode at position when it does not exist", func(t *testing.T) {
		// Arrange
		var (
			sut    = newRing()
			leases = []*lease{
				newLease(100, "node-1", 0),
				newLease(500, "node-2", 0),
			}
		)

		// Act
		sut.rebuildFromLeases(leases)
		_, found := sut.getVNodeAtPosition(999)

		// Assert
		assert.False(t, found)
	})

	t.Run("should detect expired vnode", func(t *testing.T) {
		// Arrange
		var (
			now   = time.Now()
			vnode = vnode{
				NodeID:    "node-1",
				Position:  100,
				ExpiresAt: now.Add(-1 * time.Second),
			}
		)

		// Act & Assert
		assert.True(t, isExpired(vnode, now))
	})

	t.Run("should detect non-expired vnode", func(t *testing.T) {
		// Arrange
		var (
			now   = time.Now()
			vnode = vnode{
				NodeID:    "node-1",
				Position:  100,
				ExpiresAt: now.Add(30 * time.Second),
			}
		)

		// Act & Assert
		assert.False(t, isExpired(vnode, now))
	})

	t.Run("should print visual representation of ring", func(t *testing.T) {
		// Arrange
		var sut = newRing()
		var leases = []*lease{
			newLease(100, sut.nodeID, 0),
			newLease(300, "node-2", 0),
			newLease(500, sut.nodeID, 1),
			newLease(800, "node-3", 0),
		}

		// Act
		sut.rebuildFromLeases(leases)
		var output = sut.String()

		// Assert
		assert.Contains(t, output, "Ring: test_ring")
		assert.Contains(t, output, fmt.Sprintf("Node: %s", sut.nodeID))
		assert.Contains(t, output, sut.nodeID)
		assert.Contains(t, output, "node-2")
		assert.Contains(t, output, "node-3")

		t.Logf("\n%s", output)
	})
}
