package leasering

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go-leasering/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	const (
		testRingID = "test_ring"
	)

	var (
		newDb = func(t *testing.T) *sql.DB {
			return database.SetupTestDatabase(t)
		}
		newCtx = func() context.Context {
			return context.Background()
		}
		newRingNode = func(db *sql.DB, ringID string) *Ring {
			// Use fast intervals for testing (1s lease TTL)
			// This gives us: renewal=333ms, refresh=500ms, joinTimeout=1.5s
			return NewRingNode(db, ringID, WithLeaseTTL(1*time.Second))
		}
	)

	t.Run("should bootstrap single node into empty ring", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node    = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		err := node.Start(ctx)
		require.NoError(t, err)

		partitions := node.GetOwnedPartitions()
		assert.NotEmpty(t, partitions, "bootstrap node should own all partitions")

		// Verify all leases are in database
		leases, listErr := queries.ListLeases(ctx, testRingID)
		require.NoError(t, listErr)
		assert.Len(t, leases, 8, "should have 8 vnodes (default)")

		// Cleanup
		err = node.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("should have second node join existing ring", func(t *testing.T) {
		t.Parallel()

		var (
			db    = newDb(t)
			ctx   = newCtx()
			node1 = newRingNode(db, testRingID)
			node2 = newRingNode(db, testRingID)
		)

		// Node-1 joins first
		err := node1.Start(ctx)
		require.NoError(t, err)

		partitions1Before := node1.GetOwnedPartitions()
		assert.NotEmpty(t, partitions1Before)

		// Node-2 joins second
		err = node2.Start(ctx)
		require.NoError(t, err)

		// Wait for node-2 to own some partitions
		assert.Eventually(t, func() bool {
			return len(node2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "node-2 should own some partitions")

		// Node-1 should have refreshed and lost some partitions
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) < len(partitions1Before)
		}, 2*time.Second, 50*time.Millisecond, "node-1 should own fewer partitions after node-2 joined")

		// Verify no partition overlap
		var (
			partitions1After = node1.GetOwnedPartitions()
			partitions2      = node2.GetOwnedPartitions()
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
		err = node1.Stop(ctx)
		require.NoError(t, err)
		err = node2.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("should have three nodes join and distribute partitions", func(t *testing.T) {
		t.Parallel()

		var (
			db    = newDb(t)
			ctx   = newCtx()
			node1 = newRingNode(db, testRingID)
			node2 = newRingNode(db, testRingID)
			node3 = newRingNode(db, testRingID)
		)

		// All nodes join sequentially
		err := node1.Start(ctx)
		require.NoError(t, err)

		err = node2.Start(ctx)
		require.NoError(t, err)

		err = node3.Start(ctx)
		require.NoError(t, err)

		// Wait for all nodes to own partitions and have consistent state (no overlaps)
		assert.Eventually(t, func() bool {
			var (
				partitions1 = node1.GetOwnedPartitions()
				partitions2 = node2.GetOwnedPartitions()
				partitions3 = node3.GetOwnedPartitions()
			)

			if len(partitions1) == 0 || len(partitions2) == 0 || len(partitions3) == 0 {
				return false
			}

			// Check no overlaps
			var seen = make(map[int]bool)
			for _, p := range partitions1 {
				if seen[p] {
					return false
				}
				seen[p] = true
			}
			for _, p := range partitions2 {
				if seen[p] {
					return false
				}
				seen[p] = true
			}
			for _, p := range partitions3 {
				if seen[p] {
					return false
				}
				seen[p] = true
			}

			return true
		}, 3*time.Second, 100*time.Millisecond, "all nodes should own partitions with no overlaps")

		// Cleanup
		err = node1.Stop(ctx)
		require.NoError(t, err)
		err = node2.Stop(ctx)
		require.NoError(t, err)
		err = node3.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("should renew leases continuously", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node    = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		err := node.Start(ctx)
		require.NoError(t, err)
		defer node.Stop(ctx)

		// Wait for initial leases to exist
		var leases1 []*database.LeaseRecord
		assert.Eventually(t, func() bool {
			var err error
			leases1, err = queries.ListLeases(ctx, testRingID)
			return err == nil && len(leases1) == 8
		}, 2*time.Second, 50*time.Millisecond, "initial leases should be created")

		firstExpiry := leases1[0].ExpiresAt

		// Wait for renewal to happen
		assert.Eventually(t, func() bool {
			leases2, err := queries.ListLeases(ctx, testRingID)
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

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node1   = newRingNode(db, testRingID)
			node2   = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		// Both nodes join
		err := node1.Start(ctx)
		require.NoError(t, err)
		defer node1.Stop(ctx)

		err = node2.Start(ctx)
		require.NoError(t, err)

		// Wait for both nodes to have partitions
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) > 0 && len(node2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "both nodes should own partitions")

		// Manually expire node-2's leases and stop its workers (simulating a crash)
		// Don't call Stop() - that would clean up the leases
		node2.simulateCrash()

		leases, listErr := queries.ListLeases(ctx, testRingID)
		require.NoError(t, listErr)

		// Expire node-2's leases
		expiredTime := time.Now().Add(-10 * time.Second)
		for _, lease := range leases {
			if lease.NodeID == node2.nodeID {
				lease.ExpiresAt = expiredTime
				err = queries.SetLease(ctx, lease)
				require.NoError(t, err)
			}
		}

		// Wait for cleanup worker to remove some leases
		assert.Eventually(t, func() bool {
			finalLeases, err := queries.ListLeases(ctx, testRingID)
			if err != nil {
				return false
			}
			node2Count := 0
			for _, lease := range finalLeases {
				if lease.NodeID == node2.nodeID {
					node2Count++
				}
			}
			// Some cleanup should happen (not necessarily all 8 vnodes)
			return node2Count < 8
		}, 2*time.Second, 100*time.Millisecond, "some expired successor leases should be cleaned up")
	})

	t.Run("should redistribute partitions after node crash", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node1   = newRingNode(db, testRingID)
			node2   = newRingNode(db, testRingID)
			node3   = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		// All three nodes join
		require.NoError(t, node1.Start(ctx))
		defer node1.Stop(ctx)
		require.NoError(t, node2.Start(ctx))
		require.NoError(t, node3.Start(ctx))
		defer node3.Stop(ctx)

		// Wait for all partitions to be claimed
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) > 0 &&
				len(node2.GetOwnedPartitions()) > 0 &&
				len(node3.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "all nodes should own partitions")

		// Node-2 crashes without cleanup
		node2.simulateCrash()

		// Expire node-2's leases to simulate them timing out
		leases, _ := queries.ListLeases(ctx, testRingID)
		expiredTime := time.Now().Add(-10 * time.Second)
		for _, lease := range leases {
			if lease.NodeID == node2.nodeID {
				lease.ExpiresAt = expiredTime
				queries.SetLease(ctx, lease)
			}
		}

		// Wait for ring1 and ring3 to take over all 1024 partitions
		assert.Eventually(t, func() bool {
			var allPartitions = make(map[int]bool)
			for _, p := range node1.GetOwnedPartitions() {
				allPartitions[p] = true
			}
			for _, p := range node3.GetOwnedPartitions() {
				allPartitions[p] = true
			}
			return len(allPartitions) == 1024
		}, 5*time.Second, 100*time.Millisecond, "remaining nodes should cover all partitions after crash")
	})

	t.Run("should bootstrap into dead ring without stealing from active nodes", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node1   = newRingNode(db, testRingID)
			node2   = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		// Node-1 joins and bootstraps the ring
		require.NoError(t, node1.Start(ctx))
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "node1 should own partitions")

		// Node-1 crashes immediately
		node1.simulateCrash()

		// Expire node-1's leases to simulate it being dead
		leases, _ := queries.ListLeases(ctx, testRingID)
		expiredTime := time.Now().Add(-10 * time.Second)
		for _, lease := range leases {
			lease.ExpiresAt = expiredTime
			queries.SetLease(ctx, lease)
		}

		// Node-2 joins into the dead ring - should bootstrap and take over
		require.NoError(t, node2.Start(ctx))
		defer node2.Stop(ctx)

		assert.Eventually(t, func() bool {
			return len(node2.GetOwnedPartitions()) == 1024
		}, 3*time.Second, 100*time.Millisecond, "node2 should bootstrap and own all partitions")
	})

	t.Run("should use proposal protocol when active node exists", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node1   = newRingNode(db, testRingID)
			node2   = newRingNode(db, testRingID)
			node3   = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		// Node-1 joins and bootstraps
		require.NoError(t, node1.Start(ctx))
		defer node1.Stop(ctx)

		// Node-1 crashes immediately
		node1.simulateCrash()

		// Expire node-1's leases
		leases, _ := queries.ListLeases(ctx, testRingID)
		expiredTime := time.Now().Add(-10 * time.Second)
		for _, lease := range leases {
			lease.ExpiresAt = expiredTime
			queries.SetLease(ctx, lease)
		}

		// Node-3 joins first and bootstraps into the dead ring
		require.NoError(t, node3.Start(ctx))
		defer node3.Stop(ctx)
		assert.Eventually(t, func() bool {
			return len(node3.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "node3 should own partitions")

		// Node-2 tries to join - should use proposal protocol, not bootstrap
		require.NoError(t, node2.Start(ctx))
		defer node2.Stop(ctx)

		// Both nodes should have partitions (not node-2 stealing everything)
		assert.Eventually(t, func() bool {
			return len(node2.GetOwnedPartitions()) > 0 && len(node3.GetOwnedPartitions()) > 0
		}, 3*time.Second, 100*time.Millisecond, "both nodes should share partitions")

		// Verify all partitions are covered with no overlap
		assert.Eventually(t, func() bool {
			var allPartitions = make(map[int]int)
			for _, p := range node2.GetOwnedPartitions() {
				allPartitions[p]++
			}
			for _, p := range node3.GetOwnedPartitions() {
				allPartitions[p]++
			}

			// Check no overlaps
			for _, count := range allPartitions {
				if count > 1 {
					return false
				}
			}

			return len(allPartitions) == 1024
		}, 3*time.Second, 100*time.Millisecond, "all partitions covered with no overlap")
	})

	t.Run("should gracefully remove node on Stop", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node1   = newRingNode(db, testRingID)
			node2   = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		// Both nodes join
		err := node1.Start(ctx)
		require.NoError(t, err)

		err = node2.Start(ctx)
		require.NoError(t, err)

		// Wait for both nodes to own partitions
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) > 0 && len(node2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "both nodes should own partitions")

		// Node-2 gracefully stops
		err = node2.Stop(ctx)
		require.NoError(t, err)

		// Verify node-2's leases are removed immediately
		leases, listErr := queries.ListLeases(ctx, testRingID)
		require.NoError(t, listErr)

		for _, lease := range leases {
			assert.NotEqual(t, node2.nodeID, lease.NodeID,
				"node2 should have no leases after Stop")
		}

		// Cleanup
		err = node1.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("should maintain consistent ring state across all nodes", func(t *testing.T) {
		t.Parallel()

		var (
			db    = newDb(t)
			ctx   = newCtx()
			nodes = make([]*Ring, 3) // Use 3 nodes instead of 5 for speed
		)

		// Start 3 nodes
		for i := range 3 {
			nodes[i] = newRingNode(db, testRingID)
			err := nodes[i].Start(ctx)
			require.NoError(t, err)
		}

		// Wait for all nodes to own partitions
		assert.Eventually(t, func() bool {
			for i := range 3 {
				if len(nodes[i].GetOwnedPartitions()) == 0 {
					return false
				}
			}
			return true
		}, 2*time.Second, 50*time.Millisecond, "all nodes should own partitions")

		// Wait for all nodes' caches to be consistent (no overlaps)
		assert.Eventually(t, func() bool {
			allPartitions := make(map[int]int) // partition -> count
			for i := range 3 {
				partitions := nodes[i].GetOwnedPartitions()
				for _, p := range partitions {
					allPartitions[p]++
				}
			}

			// Check no partition is claimed by multiple nodes
			for _, count := range allPartitions {
				if count > 1 {
					return false
				}
			}

			// Check all 1024 partitions are claimed
			return len(allPartitions) == 1024
		}, 3*time.Second, 100*time.Millisecond, "cached state should eventually be consistent")

		// Cleanup
		for i := range 3 {
			err := nodes[i].Stop(ctx)
			require.NoError(t, err)
		}
	})

	t.Run("should handle proposals from multiple nodes concurrently", func(t *testing.T) {
		t.Parallel()

		var (
			db    = newDb(t)
			ctx   = newCtx()
			node1 = newRingNode(db, testRingID)
			node2 = newRingNode(db, testRingID)
			node3 = newRingNode(db, testRingID)
			node4 = newRingNode(db, testRingID)
			errCh = make(chan error, 3)
		)

		// Start first node
		err := node1.Start(ctx)
		require.NoError(t, err)

		// Start 3 nodes concurrently
		go func() { errCh <- node2.Start(ctx) }()
		go func() { errCh <- node3.Start(ctx) }()
		go func() { errCh <- node4.Start(ctx) }()

		// Verify all joined successfully
		for range 3 {
			err := <-errCh
			require.NoError(t, err, "concurrent join should succeed")
		}

		// Wait for all nodes to own partitions
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) > 0 &&
				len(node2.GetOwnedPartitions()) > 0 &&
				len(node3.GetOwnedPartitions()) > 0 &&
				len(node4.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "all nodes should own some partitions")

		// Cleanup
		err = node1.Stop(ctx)
		require.NoError(t, err)
		err = node2.Stop(ctx)
		require.NoError(t, err)
		err = node3.Stop(ctx)
		require.NoError(t, err)
		err = node4.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("should clear partitions when heartbeat fails", func(t *testing.T) {
		t.Parallel()

		var (
			db   = newDb(t)
			ctx  = newCtx()
			node = newRingNode(db, testRingID)
		)

		// Start node and wait for it to own partitions
		err := node.Start(ctx)
		require.NoError(t, err)
		defer node.Stop(ctx)

		assert.Eventually(t, func() bool {
			return len(node.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "node should own partitions after join")

		// Close the database connection to simulate connection loss
		err = db.Close()
		require.NoError(t, err)

		// Wait for next renewal attempt to fail and partitions to be cleared
		assert.Eventually(t, func() bool {
			return len(node.GetOwnedPartitions()) == 0
		}, 2*time.Second, 50*time.Millisecond, "partitions should be cleared when heartbeat fails")
	})

	t.Run("should re-join ring after being evicted", func(t *testing.T) {
		t.Parallel()

		var (
			db      = newDb(t)
			ctx     = newCtx()
			node1   = newRingNode(db, testRingID)
			node2   = newRingNode(db, testRingID)
			queries = database.NewQueries(db, testRingID)
		)

		// Both nodes join
		err := node1.Start(ctx)
		require.NoError(t, err)
		defer node1.Stop(ctx)

		err = node2.Start(ctx)
		require.NoError(t, err)
		defer node2.Stop(ctx)

		// Wait for both nodes to have partitions
		assert.Eventually(t, func() bool {
			return len(node1.GetOwnedPartitions()) > 0 && len(node2.GetOwnedPartitions()) > 0
		}, 2*time.Second, 50*time.Millisecond, "both nodes should own partitions")

		// Delete node2's leases (simulating eviction by other nodes)
		leases, _ := queries.ListLeases(ctx, testRingID)
		for _, lease := range leases {
			if lease.NodeID == node2.nodeID {
				err = queries.DeleteLease(ctx, testRingID, lease.Position)
				require.NoError(t, err)
			}
		}

		// Node2 should detect eviction on next refresh and re-propose join
		assert.Eventually(t, func() bool {
			leases, err := queries.ListLeases(ctx, testRingID)
			if err != nil {
				return false
			}

			for _, lease := range leases {
				if lease.NodeID == node2.nodeID {
					return true
				}
			}
			return false
		}, 5*time.Second, 100*time.Millisecond, "node2 should re-join after eviction")

		// Node2 should eventually own partitions again
		assert.Eventually(t, func() bool {
			return len(node2.GetOwnedPartitions()) > 0
		}, 5*time.Second, 100*time.Millisecond, "node2 should own partitions after re-join")
	})
}
