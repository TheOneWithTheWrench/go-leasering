package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueries(t *testing.T) {
	var (
		newDb = func(t *testing.T) *Queries {
			var db = SetupTestDatabase(t)
			err := Migrate(db, "test_leasering")
			require.NoError(t, err)
			return NewQueries(db, "test_leasering")
		}
		newCtx = func() context.Context {
			return context.Background()
		}
		newLease = func(ringID string, position int, nodeID string, vnodeIdx int) *LeaseRecord {
			return &LeaseRecord{
				RingID:    ringID,
				Position:  position,
				NodeID:    nodeID,
				VNodeIdx:  vnodeIdx,
				ExpiresAt: time.Now().Add(30 * time.Second),
			}
		}
		newProposal = func(ringID string, predecessorPos int, newNodeID string, newVNodeIdx int, proposedPos int) *ProposalRecord {
			return &ProposalRecord{
				RingID:         ringID,
				PredecessorPos: predecessorPos,
				NewNodeID:      newNodeID,
				NewVNodeIdx:    newVNodeIdx,
				ProposedPos:    proposedPos,
				ExpiresAt:      time.Now().Add(10 * time.Second),
			}
		}
	)

	t.Run("should set and get lease", func(t *testing.T) {
		// Arrange
		var (
			sut   = newDb(t)
			ctx   = newCtx()
			lease = newLease("ring-1", 100, "node-1", 0)
		)

		// Act
		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "ring-1", retrieved.RingID)
		assert.Equal(t, 100, retrieved.Position)
		assert.Equal(t, "node-1", retrieved.NodeID)
		assert.Equal(t, 0, retrieved.VNodeIdx)
		assert.WithinDuration(t, lease.ExpiresAt, retrieved.ExpiresAt, time.Second)
	})

	t.Run("should return nil for non-existent lease", func(t *testing.T) {
		// Arrange
		var (
			sut = newDb(t)
			ctx = newCtx()
		)

		// Act
		var retrieved, err = sut.GetLease(ctx, "ring-1", 999)

		// Assert
		require.NoError(t, err)
		assert.Nil(t, retrieved)
	})

	t.Run("should list leases ordered by position", func(t *testing.T) {
		// Arrange
		var (
			sut    = newDb(t)
			ctx    = newCtx()
			leases = []*LeaseRecord{
				newLease("ring-1", 500, "node-2", 0),
				newLease("ring-1", 100, "node-1", 0),
				newLease("ring-1", 900, "node-3", 0),
			}
		)

		// Act - insert in random order
		for _, lease := range leases {
			err := sut.SetLease(ctx, lease)
			require.NoError(t, err)
		}

		var retrieved, listErr = sut.ListLeases(ctx, "ring-1")

		// Assert - should be ordered by position
		require.NoError(t, listErr)
		require.Len(t, retrieved, 3)
		assert.Equal(t, 100, retrieved[0].Position)
		assert.Equal(t, 500, retrieved[1].Position)
		assert.Equal(t, 900, retrieved[2].Position)
	})

	t.Run("should update existing lease on conflict", func(t *testing.T) {
		// Arrange
		var (
			sut    = newDb(t)
			ctx    = newCtx()
			lease1 = newLease("ring-1", 100, "node-1", 0)
			lease2 = newLease("ring-1", 100, "node-2", 1)
		)

		// Act
		err := sut.SetLease(ctx, lease1)
		require.NoError(t, err)

		err = sut.SetLease(ctx, lease2)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert - should have node-2's data
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "node-2", retrieved.NodeID)
		assert.Equal(t, 1, retrieved.VNodeIdx)
	})

	t.Run("should delete lease", func(t *testing.T) {
		// Arrange
		var (
			sut   = newDb(t)
			ctx   = newCtx()
			lease = newLease("ring-1", 100, "node-1", 0)
		)

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.DeleteLease(ctx, "ring-1", 100)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		assert.Nil(t, retrieved)
	})

	t.Run("should isolate leases by ring ID", func(t *testing.T) {
		// Arrange
		var (
			sut    = newDb(t)
			ctx    = newCtx()
			lease1 = newLease("ring-1", 100, "node-1", 0)
			lease2 = newLease("ring-2", 100, "node-2", 0)
		)

		// Act
		err := sut.SetLease(ctx, lease1)
		require.NoError(t, err)

		err = sut.SetLease(ctx, lease2)
		require.NoError(t, err)

		var ring1Leases, err1 = sut.ListLeases(ctx, "ring-1")
		var ring2Leases, err2 = sut.ListLeases(ctx, "ring-2")

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Len(t, ring1Leases, 1)
		assert.Len(t, ring2Leases, 1)
		assert.Equal(t, "node-1", ring1Leases[0].NodeID)
		assert.Equal(t, "node-2", ring2Leases[0].NodeID)
	})

	t.Run("should set and list proposals", func(t *testing.T) {
		// Arrange
		var (
			sut      = newDb(t)
			ctx      = newCtx()
			proposal = newProposal("ring-1", 100, "node-new", 0, 150)
		)

		// Act
		err := sut.SetProposal(ctx, proposal)
		require.NoError(t, err)

		var retrieved, listErr = sut.ListProposals(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, listErr)
		require.Len(t, retrieved, 1)
		assert.Equal(t, "ring-1", retrieved[0].RingID)
		assert.Equal(t, 100, retrieved[0].PredecessorPos)
		assert.Equal(t, "node-new", retrieved[0].NewNodeID)
		assert.Equal(t, 0, retrieved[0].NewVNodeIdx)
		assert.Equal(t, 150, retrieved[0].ProposedPos)
	})

	t.Run("should list multiple proposals for same predecessor", func(t *testing.T) {
		// Arrange
		var (
			sut        = newDb(t)
			ctx        = newCtx()
			proposal1  = newProposal("ring-1", 100, "node-new-1", 0, 150)
			proposal2  = newProposal("ring-1", 100, "node-new-2", 0, 160)
		)

		// Act
		err := sut.SetProposal(ctx, proposal1)
		require.NoError(t, err)

		err = sut.SetProposal(ctx, proposal2)
		require.NoError(t, err)

		var retrieved, listErr = sut.ListProposals(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, listErr)
		assert.Len(t, retrieved, 2)
	})

	t.Run("should update existing proposal on conflict", func(t *testing.T) {
		// Arrange
		var (
			sut        = newDb(t)
			ctx        = newCtx()
			proposal1  = newProposal("ring-1", 100, "node-new", 0, 150)
			proposal2  = newProposal("ring-1", 100, "node-new", 0, 160)
		)

		// Act
		err := sut.SetProposal(ctx, proposal1)
		require.NoError(t, err)

		err = sut.SetProposal(ctx, proposal2)
		require.NoError(t, err)

		var retrieved, listErr = sut.ListProposals(ctx, "ring-1", 100)

		// Assert - should have updated position
		require.NoError(t, listErr)
		require.Len(t, retrieved, 1)
		assert.Equal(t, 160, retrieved[0].ProposedPos)
	})

	t.Run("should delete proposal", func(t *testing.T) {
		// Arrange
		var (
			sut      = newDb(t)
			ctx      = newCtx()
			proposal = newProposal("ring-1", 100, "node-new", 0, 150)
		)

		err := sut.SetProposal(ctx, proposal)
		require.NoError(t, err)

		// Act
		err = sut.DeleteProposal(ctx, "ring-1", 100, "node-new", 0)
		require.NoError(t, err)

		var retrieved, listErr = sut.ListProposals(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, listErr)
		assert.Empty(t, retrieved)
	})

	t.Run("should isolate proposals by predecessor position", func(t *testing.T) {
		// Arrange
		var (
			sut        = newDb(t)
			ctx        = newCtx()
			proposal1  = newProposal("ring-1", 100, "node-new-1", 0, 150)
			proposal2  = newProposal("ring-1", 200, "node-new-2", 0, 250)
		)

		// Act
		err := sut.SetProposal(ctx, proposal1)
		require.NoError(t, err)

		err = sut.SetProposal(ctx, proposal2)
		require.NoError(t, err)

		var proposals100, err1 = sut.ListProposals(ctx, "ring-1", 100)
		var proposals200, err2 = sut.ListProposals(ctx, "ring-1", 200)

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Len(t, proposals100, 1)
		assert.Len(t, proposals200, 1)
		assert.Equal(t, "node-new-1", proposals100[0].NewNodeID)
		assert.Equal(t, "node-new-2", proposals200[0].NewNodeID)
	})

	t.Run("should list all proposals for ring with batch query", func(t *testing.T) {
		// Arrange
		var (
			sut        = newDb(t)
			ctx        = newCtx()
			proposals  = []*ProposalRecord{
				newProposal("ring-1", 100, "node-new-1", 0, 150),
				newProposal("ring-1", 100, "node-new-2", 0, 160),
				newProposal("ring-1", 200, "node-new-3", 0, 250),
				newProposal("ring-2", 300, "node-new-4", 0, 350),
			}
		)

		// Act - insert all proposals
		for _, proposal := range proposals {
			err := sut.SetProposal(ctx, proposal)
			require.NoError(t, err)
		}

		var ring1Proposals, err1 = sut.ListAllProposals(ctx, "ring-1")
		var ring2Proposals, err2 = sut.ListAllProposals(ctx, "ring-2")

		// Assert - should get all proposals for ring-1 in one query
		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Len(t, ring1Proposals, 3)
		assert.Len(t, ring2Proposals, 1)

		// Verify ring-1 proposals are isolated
		var nodeIDs = make(map[string]bool)
		for _, p := range ring1Proposals {
			assert.Equal(t, "ring-1", p.RingID)
			nodeIDs[p.NewNodeID] = true
		}
		assert.True(t, nodeIDs["node-new-1"])
		assert.True(t, nodeIDs["node-new-2"])
		assert.True(t, nodeIDs["node-new-3"])
		assert.False(t, nodeIDs["node-new-4"]) // From ring-2
	})
}
