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

	t.Run("should create expected indexes", func(t *testing.T) {
		// Arrange
		var (
			db  = SetupTestDatabase(t)
			ctx = newCtx()
		)

		// Act
		err := Migrate(db, "test_leasering")
		require.NoError(t, err)

		var indexCount int
		err = db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM pg_indexes
WHERE schemaname = current_schema()
  AND indexname IN ('test_leasering_leases_active_idx', 'test_leasering_proposals_expires_idx');`).Scan(&indexCount)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, 2, indexCount)
	})

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

	t.Run("should not list expired leases", func(t *testing.T) {
		// Arrange
		var (
			sut          = newDb(t)
			ctx          = newCtx()
			activeLease  = newLease("ring-1", 100, "node-1", 0)
			expiredLease = newLease("ring-1", 500, "node-2", 0)
		)
		expiredLease.ExpiresAt = time.Now().Add(-1 * time.Second)

		// Act
		err := sut.SetLease(ctx, activeLease)
		require.NoError(t, err)

		err = sut.SetLease(ctx, expiredLease)
		require.NoError(t, err)

		var retrieved, listErr = sut.ListLeases(ctx, "ring-1")

		// Assert
		require.NoError(t, listErr)
		require.Len(t, retrieved, 1)
		assert.Equal(t, activeLease.Position, retrieved[0].Position)
		assert.Equal(t, activeLease.NodeID, retrieved[0].NodeID)
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

	t.Run("should renew existing lease for current owner", func(t *testing.T) {
		// Arrange
		var (
			sut     = newDb(t)
			ctx     = newCtx()
			lease   = newLease("ring-1", 100, "node-1", 0)
			renewed = newLease("ring-1", 100, "node-1", 0)
		)
		renewed.ExpiresAt = lease.ExpiresAt.Add(30 * time.Second)

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.RenewLease(ctx, renewed)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "node-1", retrieved.NodeID)
		assert.Equal(t, 0, retrieved.VNodeIdx)
		assert.WithinDuration(t, renewed.ExpiresAt, retrieved.ExpiresAt, time.Second)
	})

	t.Run("should not renew lease owned by another node", func(t *testing.T) {
		// Arrange
		var (
			sut     = newDb(t)
			ctx     = newCtx()
			lease   = newLease("ring-1", 100, "node-1", 0)
			renewed = newLease("ring-1", 100, "node-2", 0)
		)
		renewed.ExpiresAt = lease.ExpiresAt.Add(30 * time.Second)

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.RenewLease(ctx, renewed)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.ErrorIs(t, err, ErrLeaseNotRenewed)
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "node-1", retrieved.NodeID)
		assert.WithinDuration(t, lease.ExpiresAt, retrieved.ExpiresAt, time.Second)
	})

	t.Run("should not renew expired lease", func(t *testing.T) {
		// Arrange
		var (
			sut     = newDb(t)
			ctx     = newCtx()
			lease   = newLease("ring-1", 100, "node-1", 0)
			renewed = newLease("ring-1", 100, "node-1", 0)
		)
		lease.ExpiresAt = time.Now().Add(-1 * time.Second)

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.RenewLease(ctx, renewed)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.ErrorIs(t, err, ErrLeaseNotRenewed)
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.WithinDuration(t, lease.ExpiresAt, retrieved.ExpiresAt, time.Second)
	})

	t.Run("should not insert missing lease during renewal", func(t *testing.T) {
		// Arrange
		var (
			sut     = newDb(t)
			ctx     = newCtx()
			renewed = newLease("ring-1", 100, "node-1", 0)
		)

		// Act
		err := sut.RenewLease(ctx, renewed)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.ErrorIs(t, err, ErrLeaseNotRenewed)
		require.NoError(t, getErr)
		assert.Nil(t, retrieved)
	})

	t.Run("should insert lease when predecessor is actively owned by accepter", func(t *testing.T) {
		// Arrange
		var (
			sut         = newDb(t)
			ctx         = newCtx()
			predecessor = newLease("ring-1", 100, "node-1", 0)
			lease       = newLease("ring-1", 150, "node-2", 0)
		)

		err := sut.SetLease(ctx, predecessor)
		require.NoError(t, err)

		// Act
		err = sut.InsertLeaseIfPredecessorOwned(ctx, lease, predecessor.Position, predecessor.NodeID)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 150)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, lease.NodeID, retrieved.NodeID)
		assert.Equal(t, lease.VNodeIdx, retrieved.VNodeIdx)
	})

	t.Run("should not insert lease when predecessor is expired", func(t *testing.T) {
		// Arrange
		var (
			sut         = newDb(t)
			ctx         = newCtx()
			predecessor = newLease("ring-1", 100, "node-1", 0)
			lease       = newLease("ring-1", 150, "node-2", 0)
		)
		predecessor.ExpiresAt = time.Now().Add(-1 * time.Second)

		err := sut.SetLease(ctx, predecessor)
		require.NoError(t, err)

		// Act
		err = sut.InsertLeaseIfPredecessorOwned(ctx, lease, predecessor.Position, predecessor.NodeID)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 150)

		// Assert
		require.ErrorIs(t, err, ErrLeaseNotInserted)
		require.NoError(t, getErr)
		assert.Nil(t, retrieved)
	})

	t.Run("should not insert lease when predecessor is owned by another node", func(t *testing.T) {
		// Arrange
		var (
			sut         = newDb(t)
			ctx         = newCtx()
			predecessor = newLease("ring-1", 100, "node-1", 0)
			lease       = newLease("ring-1", 150, "node-2", 0)
		)

		err := sut.SetLease(ctx, predecessor)
		require.NoError(t, err)

		// Act
		err = sut.InsertLeaseIfPredecessorOwned(ctx, lease, predecessor.Position, "node-3")

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 150)

		// Assert
		require.ErrorIs(t, err, ErrLeaseNotInserted)
		require.NoError(t, getErr)
		assert.Nil(t, retrieved)
	})

	t.Run("should not insert lease when predecessor is missing", func(t *testing.T) {
		// Arrange
		var (
			sut   = newDb(t)
			ctx   = newCtx()
			lease = newLease("ring-1", 150, "node-2", 0)
		)

		// Act
		err := sut.InsertLeaseIfPredecessorOwned(ctx, lease, 100, "node-1")

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 150)

		// Assert
		require.ErrorIs(t, err, ErrLeaseNotInserted)
		require.NoError(t, getErr)
		assert.Nil(t, retrieved)
	})

	t.Run("should not insert lease when proposed position is already occupied", func(t *testing.T) {
		// Arrange
		var (
			sut         = newDb(t)
			ctx         = newCtx()
			predecessor = newLease("ring-1", 100, "node-1", 0)
			existing    = newLease("ring-1", 150, "node-3", 0)
			lease       = newLease("ring-1", 150, "node-2", 0)
		)

		err := sut.SetLease(ctx, predecessor)
		require.NoError(t, err)

		err = sut.SetLease(ctx, existing)
		require.NoError(t, err)

		// Act
		err = sut.InsertLeaseIfPredecessorOwned(ctx, lease, predecessor.Position, predecessor.NodeID)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 150)

		// Assert
		require.Error(t, err)
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, existing.NodeID, retrieved.NodeID)
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

	t.Run("should delete expired lease for matching vnode", func(t *testing.T) {
		// Arrange
		var (
			sut   = newDb(t)
			ctx   = newCtx()
			lease = newLease("ring-1", 100, "node-1", 0)
		)
		lease.ExpiresAt = time.Now().Add(-1 * time.Second)

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.DeleteExpiredLease(ctx, lease)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		assert.Nil(t, retrieved)
	})

	t.Run("should not delete live lease", func(t *testing.T) {
		// Arrange
		var (
			sut   = newDb(t)
			ctx   = newCtx()
			lease = newLease("ring-1", 100, "node-1", 0)
		)

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.DeleteExpiredLease(ctx, lease)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "node-1", retrieved.NodeID)
		assert.WithinDuration(t, lease.ExpiresAt, retrieved.ExpiresAt, time.Second)
	})

	t.Run("should not delete renewed lease from stale expiry observation", func(t *testing.T) {
		// Arrange
		var (
			sut      = newDb(t)
			ctx      = newCtx()
			observed = newLease("ring-1", 100, "node-1", 0)
			renewed  = newLease("ring-1", 100, "node-1", 0)
		)
		observed.ExpiresAt = time.Now().Add(-1 * time.Second)
		renewed.ExpiresAt = time.Now().Add(30 * time.Second)

		err := sut.SetLease(ctx, renewed)
		require.NoError(t, err)

		// Act
		err = sut.DeleteExpiredLease(ctx, observed)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "node-1", retrieved.NodeID)
		assert.WithinDuration(t, renewed.ExpiresAt, retrieved.ExpiresAt, time.Second)
	})

	t.Run("should not delete expired lease for different owner", func(t *testing.T) {
		// Arrange
		var (
			sut      = newDb(t)
			ctx      = newCtx()
			lease    = newLease("ring-1", 100, "node-1", 0)
			observed = newLease("ring-1", 100, "node-2", 0)
		)
		lease.ExpiresAt = time.Now().Add(-1 * time.Second)
		observed.ExpiresAt = lease.ExpiresAt

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.DeleteExpiredLease(ctx, observed)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, "node-1", retrieved.NodeID)
	})

	t.Run("should not delete expired lease for different vnode index", func(t *testing.T) {
		// Arrange
		var (
			sut      = newDb(t)
			ctx      = newCtx()
			lease    = newLease("ring-1", 100, "node-1", 0)
			observed = newLease("ring-1", 100, "node-1", 1)
		)
		lease.ExpiresAt = time.Now().Add(-1 * time.Second)
		observed.ExpiresAt = lease.ExpiresAt

		err := sut.SetLease(ctx, lease)
		require.NoError(t, err)

		// Act
		err = sut.DeleteExpiredLease(ctx, observed)
		require.NoError(t, err)

		var retrieved, getErr = sut.GetLease(ctx, "ring-1", 100)

		// Assert
		require.NoError(t, getErr)
		require.NotNil(t, retrieved)
		assert.Equal(t, 0, retrieved.VNodeIdx)
	})

	t.Run("should not error when deleting missing expired lease", func(t *testing.T) {
		// Arrange
		var (
			sut   = newDb(t)
			ctx   = newCtx()
			lease = newLease("ring-1", 100, "node-1", 0)
		)
		lease.ExpiresAt = time.Now().Add(-1 * time.Second)

		// Act
		err := sut.DeleteExpiredLease(ctx, lease)

		// Assert
		require.NoError(t, err)
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
			sut       = newDb(t)
			ctx       = newCtx()
			proposal1 = newProposal("ring-1", 100, "node-new-1", 0, 150)
			proposal2 = newProposal("ring-1", 100, "node-new-2", 0, 160)
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
			sut       = newDb(t)
			ctx       = newCtx()
			proposal1 = newProposal("ring-1", 100, "node-new", 0, 150)
			proposal2 = newProposal("ring-1", 100, "node-new", 0, 160)
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
			sut       = newDb(t)
			ctx       = newCtx()
			proposal1 = newProposal("ring-1", 100, "node-new-1", 0, 150)
			proposal2 = newProposal("ring-1", 200, "node-new-2", 0, 250)
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

	t.Run("should list proposals for predecessor positions", func(t *testing.T) {
		// Arrange
		var (
			sut       = newDb(t)
			ctx       = newCtx()
			proposals = []*ProposalRecord{
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

		var ring1Proposals, err1 = sut.ListProposalsForPredecessors(ctx, "ring-1", []int{100, 200})
		var ring2Proposals, err2 = sut.ListProposalsForPredecessors(ctx, "ring-2", []int{300})
		var emptyProposals, err3 = sut.ListProposalsForPredecessors(ctx, "ring-1", nil)

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)
		assert.Len(t, ring1Proposals, 3)
		assert.Len(t, ring2Proposals, 1)
		assert.Empty(t, emptyProposals)

		var nodeIDs = make(map[string]bool)
		for _, p := range ring1Proposals {
			assert.Equal(t, "ring-1", p.RingID)
			assert.Contains(t, []int{100, 200}, p.PredecessorPos)
			nodeIDs[p.NewNodeID] = true
		}
		assert.True(t, nodeIDs["node-new-1"])
		assert.True(t, nodeIDs["node-new-2"])
		assert.True(t, nodeIDs["node-new-3"])
		assert.False(t, nodeIDs["node-new-4"]) // From ring-2
	})
}
