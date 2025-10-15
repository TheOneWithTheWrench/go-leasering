package leasering

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"go-leasering/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	var (
		newDb = func(t *testing.T) *sql.DB {
			return database.SetupTestDatabase(t)
		}
		newCtx = func() context.Context {
			return context.Background()
		}
		newRing = func(ringID, nodeID string) *Ring {
			// Use fast intervals for testing (1s lease TTL)
			// This gives us: renewal=333ms, refresh=500ms
			return NewRing(ringID, nodeID, WithLeaseTTL(1*time.Second))
		}
	)

	t.Run("should bootstrap single node into empty ring", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db   = newDb(t)
			ctx  = newCtx()
			ring = newRing("test_ring", "node-1")
		)

		// Act
		err := ring.Start(ctx, db)

		// Assert
		require.NoError(t, err)

		var partitions = ring.GetOwnedPartitions()
		assert.NotEmpty(t, partitions, "bootstrap node should own all partitions")

		// Verify all leases are in database
		var queries = database.NewQueries(db, "test_ring")
		var leases, listErr = queries.ListLeases(ctx, "test_ring")
		require.NoError(t, listErr)
		assert.Len(t, leases, 8, "should have 8 vnodes (default)")

		// Cleanup
		err = ring.Stop(ctx, db)
		require.NoError(t, err)
	})

	t.Run("should have second node join existing ring", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db    = newDb(t)
			ctx   = newCtx()
			ring1 = newRing("test_ring", "node-1")
			ring2 = newRing("test_ring", "node-2")
		)

		// Act - node-1 joins first
		err := ring1.Start(ctx, db)
		require.NoError(t, err)

		var partitions1Before = ring1.GetOwnedPartitions()
		assert.NotEmpty(t, partitions1Before)

		// Act - node-2 joins second
		err = ring2.Start(ctx, db)
		require.NoError(t, err)

		// Wait for node-2 to own some partitions
		assert.Eventually(t, func() bool {
			return len(ring2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "node-2 should own some partitions")

		// Node-1 should have refreshed and lost some partitions
		assert.Eventually(t, func() bool {
			return len(ring1.GetOwnedPartitions()) < len(partitions1Before)
		}, 2*time.Second, 50*time.Millisecond, "node-1 should own fewer partitions after node-2 joined")

		// Verify no partition overlap
		var (
			partitions1After = ring1.GetOwnedPartitions()
			partitions2      = ring2.GetOwnedPartitions()
			partitionSet     = make(map[int]string)
		)
		for _, p := range partitions1After {
			partitionSet[p] = "node-1"
		}
		for _, p := range partitions2 {
			if owner, exists := partitionSet[p]; exists {
				t.Fatalf("partition %d owned by both node-1 and %s", p, owner)
			}
		}

		// Cleanup
		err = ring1.Stop(ctx, db)
		require.NoError(t, err)
		err = ring2.Stop(ctx, db)
		require.NoError(t, err)
	})

	t.Run("should have three nodes join and distribute partitions", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db    = newDb(t)
			ctx   = newCtx()
			ring1 = newRing("test_ring", "node-1")
			ring2 = newRing("test_ring", "node-2")
			ring3 = newRing("test_ring", "node-3")
		)

		// Act - all nodes join sequentially
		err := ring1.Start(ctx, db)
		require.NoError(t, err)

		err = ring2.Start(ctx, db)
		require.NoError(t, err)

		err = ring3.Start(ctx, db)
		require.NoError(t, err)

		// Wait for all nodes to own partitions
		assert.Eventually(t, func() bool {
			return len(ring1.GetOwnedPartitions()) > 0 &&
				len(ring2.GetOwnedPartitions()) > 0 &&
				len(ring3.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "all nodes should own some partitions")

		// Assert - verify partition distribution
		var (
			partitions1 = ring1.GetOwnedPartitions()
			partitions2 = ring2.GetOwnedPartitions()
			partitions3 = ring3.GetOwnedPartitions()
		)

		// Verify no overlaps
		var allPartitions = make(map[int]string)
		for _, p := range partitions1 {
			allPartitions[p] = "node-1"
		}
		for _, p := range partitions2 {
			if owner, exists := allPartitions[p]; exists {
				t.Fatalf("partition %d owned by both node-1 and %s", p, owner)
			}
			allPartitions[p] = "node-2"
		}
		for _, p := range partitions3 {
			if owner, exists := allPartitions[p]; exists {
				t.Fatalf("partition %d owned by both %s and node-3", p, owner)
			}
			allPartitions[p] = "node-3"
		}

		t.Logf("Partition distribution: node-1=%d, node-2=%d, node-3=%d",
			len(partitions1), len(partitions2), len(partitions3))

		// Cleanup
		err = ring1.Stop(ctx, db)
		require.NoError(t, err)
		err = ring2.Stop(ctx, db)
		require.NoError(t, err)
		err = ring3.Stop(ctx, db)
		require.NoError(t, err)
	})

	t.Run("should renew leases continuously", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db   = newDb(t)
			ctx  = newCtx()
			ring = newRing("test_ring", "node-1")
		)

		// Act
		err := ring.Start(ctx, db)
		require.NoError(t, err)
		defer ring.Stop(ctx, db)

		// Get initial lease expiration times
		var queries = database.NewQueries(db, "test_ring")

		// Wait for initial leases to exist
		var leases1 []*database.LeaseRecord
		assert.Eventually(t, func() bool {
			var err error
			leases1, err = queries.ListLeases(ctx, "test_ring")
			return err == nil && len(leases1) == 8
		}, 2*time.Second, 50*time.Millisecond, "initial leases should be created")

		var firstExpiry = leases1[0].ExpiresAt

		// Wait for renewal to happen
		assert.Eventually(t, func() bool {
			leases2, err := queries.ListLeases(ctx, "test_ring")
			if err != nil || len(leases2) == 0 {
				return false
			}
			return leases2[0].ExpiresAt.After(firstExpiry)
		}, 2*time.Second, 50*time.Millisecond, "lease should be renewed with later expiration time")
	})

	t.Run("should cleanup expired successor leases", func(t *testing.T) {
		t.Parallel()
		// This test verifies that nodes clean up expired leases of their successors
		// Note: Cleanup is responsibility-based - each node only cleans successors of its own vnodes

		// Arrange
		var (
			db    = newDb(t)
			ctx   = newCtx()
			ring1 = newRing("test_ring", "node-1")
			ring2 = newRing("test_ring", "node-2")
		)

		// Both nodes join
		err := ring1.Start(ctx, db)
		require.NoError(t, err)
		defer ring1.Stop(ctx, db)

		err = ring2.Start(ctx, db)
		require.NoError(t, err)

		// Wait for both nodes to have partitions
		assert.Eventually(t, func() bool {
			return len(ring1.GetOwnedPartitions()) > 0 && len(ring2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "both nodes should own partitions")

		// Now manually expire node-2's leases and stop its workers (simulating a crash)
		// Don't call Stop() - that would clean up the leases
		if ring2.cancel != nil {
			ring2.cancel()
		}

		var queries = database.NewQueries(db, "test_ring")
		var leases, listErr = queries.ListLeases(ctx, "test_ring")
		require.NoError(t, listErr)

		// Expire node-2's leases
		var expiredTime = time.Now().Add(-10 * time.Second)
		for _, lease := range leases {
			if lease.NodeID == "node-2" {
				lease.ExpiresAt = expiredTime
				err = queries.SetLease(ctx, lease)
				require.NoError(t, err)
			}
		}

		// Wait for cleanup worker to remove some leases
		assert.Eventually(t, func() bool {
			finalLeases, err := queries.ListLeases(ctx, "test_ring")
			if err != nil {
				return false
			}
			node2Count := 0
			for _, lease := range finalLeases {
				if lease.NodeID == "node-2" {
					node2Count++
				}
			}
			// Some cleanup should happen (not necessarily all 8 vnodes)
			return node2Count < 8
		}, 2*time.Second, 100*time.Millisecond, "some expired successor leases should be cleaned up")
	})

	t.Run("should gracefully remove node on Stop", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db    = newDb(t)
			ctx   = newCtx()
			ring1 = newRing("test_ring", "node-1")
			ring2 = newRing("test_ring", "node-2")
		)

		// Act - both nodes join
		err := ring1.Start(ctx, db)
		require.NoError(t, err)

		err = ring2.Start(ctx, db)
		require.NoError(t, err)

		// Wait for both nodes to own partitions
		assert.Eventually(t, func() bool {
			return len(ring1.GetOwnedPartitions()) > 0 && len(ring2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "both nodes should own partitions")

		// Node-2 gracefully stops
		err = ring2.Stop(ctx, db)
		require.NoError(t, err)

		// Verify node-2's leases are removed immediately
		var queries = database.NewQueries(db, "test_ring")
		var leases, listErr = queries.ListLeases(ctx, "test_ring")
		require.NoError(t, listErr)

		for _, lease := range leases {
			assert.NotEqual(t, "node-2", lease.NodeID,
				"node-2 should have no leases after Stop")
		}

		// Cleanup
		err = ring1.Stop(ctx, db)
		require.NoError(t, err)
	})

	t.Run("should maintain consistent ring state across all nodes", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db     = newDb(t)
			ctx    = newCtx()
			rings  = make([]*Ring, 3) // Use 3 nodes instead of 5 for speed
			nodeID = func(i int) string { return fmt.Sprintf("node-%d", i) }
		)

		// Act - start 3 nodes
		for i := range 3 {
			rings[i] = newRing("test_ring", nodeID(i))
			err := rings[i].Start(ctx, db)
			require.NoError(t, err)
		}

		// Wait for all nodes to own partitions
		assert.Eventually(t, func() bool {
			for i := range 3 {
				if len(rings[i].GetOwnedPartitions()) == 0 {
					return false
				}
			}
			return true
		}, 2*time.Second, 50*time.Millisecond, "all nodes should own partitions")

		// Assert - verify partition coverage
		var allPartitions = make(map[int][]string)

		// Collect partition ownership from all nodes
		for i := range 3 {
			var partitions = rings[i].GetOwnedPartitions()
			for _, p := range partitions {
				allPartitions[p] = append(allPartitions[p], nodeID(i))
			}
		}

		// Verify each partition has exactly one owner
		for p := range 1024 {
			var owners = allPartitions[p]
			assert.Len(t, owners, 1, "partition %d should have exactly one owner, got: %v", p, owners)
		}

		// Cleanup
		for i := range 3 {
			err := rings[i].Stop(ctx, db)
			require.NoError(t, err)
		}
	})

	t.Run("should handle proposals from multiple nodes concurrently", func(t *testing.T) {
		t.Parallel()
		// Arrange
		var (
			db    = newDb(t)
			ctx   = newCtx()
			ring1 = newRing("test_ring", "node-1")
		)

		// Start first node
		err := ring1.Start(ctx, db)
		require.NoError(t, err)

		// Act - start 3 nodes concurrently
		var (
			ring2 = newRing("test_ring", "node-2")
			ring3 = newRing("test_ring", "node-3")
			ring4 = newRing("test_ring", "node-4")
		)

		var errCh = make(chan error, 3)
		go func() { errCh <- ring2.Start(ctx, db) }()
		go func() { errCh <- ring3.Start(ctx, db) }()
		go func() { errCh <- ring4.Start(ctx, db) }()

		// Verify all joined successfully
		for range 3 {
			err := <-errCh
			require.NoError(t, err, "concurrent join should succeed")
		}

		// Wait for all nodes to own partitions
		assert.Eventually(t, func() bool {
			return len(ring1.GetOwnedPartitions()) > 0 &&
				len(ring2.GetOwnedPartitions()) > 0 &&
				len(ring3.GetOwnedPartitions()) > 0 &&
				len(ring4.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "all nodes should own some partitions")

		// Cleanup
		err = ring1.Stop(ctx, db)
		require.NoError(t, err)
		err = ring2.Stop(ctx, db)
		require.NoError(t, err)
		err = ring3.Stop(ctx, db)
		require.NoError(t, err)
		err = ring4.Stop(ctx, db)
		require.NoError(t, err)
	})
}
