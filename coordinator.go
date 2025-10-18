package leasering

import (
	"context"
	"fmt"
	"time"
)

// coordinator orchestrates the ring lifecycle and background workers.
type coordinator struct {
	ring       *Ring
	membership *membership
	store      *leaseStore
	options    options
	cancel     context.CancelFunc
}

// newCoordinator creates a new coordinator.
func newCoordinator(ring *Ring, m *membership, store *leaseStore, opts options) *coordinator {
	return &coordinator{
		ring:       ring,
		membership: m,
		store:      store,
		options:    opts,
	}
}

// start begins the background processes: join, lease renewal, ring refresh, and proposal acceptance.
// This will block until the node successfully joins the ring.
//
// Context handling: The caller's context is used for the join phase. Background workers run
// with a separate context.Background() to ensure they continue running independently of the
// caller's context. The workers are stopped via the internal cancel function when stop() is called.
func (c *coordinator) start(ctx context.Context) error {
	const maxRetries = 10

	var workerCtx context.Context
	workerCtx, c.cancel = context.WithCancel(context.Background())

	go c.acceptProposalsWorker(workerCtx)

	var joinSucceeded = false
	defer func() {
		if !joinSucceeded && c.cancel != nil {
			c.cancel()
		}
	}()

	for attempt := range maxRetries {
		if c.ring.hasSelfCollision() {
			if attempt >= maxRetries-1 {
				return fmt.Errorf("failed to generate collision-free node-id after %d attempts", maxRetries)
			}
			c.options.logger.Warn("self-collision detected, regenerating node-id",
				"attempt", attempt+1,
				"node_id", c.ring.nodeID)
			c.ring.regenerateNodeID()
			c.membership.nodeID = c.ring.nodeID
			continue
		}

		if err := c.membership.ProposeJoin(ctx); err != nil {
			return fmt.Errorf("failed to propose join: %w", err)
		}

		confirmed, err := c.waitForJoinConfirmation(ctx)
		if err != nil {
			return fmt.Errorf("failed to join ring: %w", err)
		}

		if confirmed {
			break
		}

		if attempt >= maxRetries-1 {
			return fmt.Errorf("failed to join ring after %d attempts", maxRetries)
		}
		c.options.logger.Warn("join timed out. This could be due to possible hash collisions, regenerating node-id and retrying",
			"attempt", attempt+1,
			"max_retries", maxRetries,
			"old_node_id", c.ring.nodeID)

		if err := c.membership.CleanupNodeData(ctx); err != nil {
			c.options.logger.Warn("failed to cleanup old node data", "error", err)
		}

		c.ring.regenerateNodeID()
		c.membership.nodeID = c.ring.nodeID
	}

	joinSucceeded = true

	// Refresh ring state to get initial partition ownership
	if err := c.membership.RefreshRingState(ctx); err != nil {
		return fmt.Errorf("failed to refresh ring state after join: %w", err)
	}

	// Log successful join
	c.options.logger.Info("successfully joined ring",
		"ring_id", c.ring.ringID,
		"node_id", c.ring.nodeID,
		"vnode_count", c.options.vnodeCount,
		"owned_partitions", len(c.ring.GetOwnedPartitions()))

	// Start remaining background workers
	go c.renewLeaseWorker(workerCtx)
	go c.refreshRingWorker(workerCtx)
	go c.cleanupExpiredLeasesWorker(workerCtx)

	return nil
}

// stop gracefully shuts down and removes this node's leases.
func (c *coordinator) stop(ctx context.Context) error {
	// Cancel background workers first
	if c.cancel != nil {
		c.cancel()
	}

	// Remove all leases
	return c.membership.Leave(ctx)
}

// waitForJoinConfirmation polls until all vnodes have active leases or timeout.
// Returns (true, nil) if all vnodes confirmed, (false, nil) if timed out, or (false, error) on error.
func (c *coordinator) waitForJoinConfirmation(ctx context.Context) (bool, error) {
	var (
		timeout   = time.After(c.options.joinTimeout)
		ticker    = time.NewTicker(100 * time.Millisecond)
		confirmed = false
	)
	defer ticker.Stop()

	for !confirmed {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timeout:
			return false, nil
		case <-ticker.C:
			var err error
			confirmed, err = c.membership.CheckJoinConfirmation(ctx)
			if err != nil {
				return false, fmt.Errorf("failed to check join confirmation: %w", err)
			}
		}
	}

	return true, nil
}

// renewLeaseWorker periodically renews this node's leases.
func (c *coordinator) renewLeaseWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.RenewLeases(ctx); err != nil {
				c.options.logger.Error("failed to renew leases", "error", err)
			}
		}
	}
}

// refreshRingWorker periodically refreshes the ring state from the database.
func (c *coordinator) refreshRingWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.RefreshRingState(ctx); err != nil {
				c.options.logger.Error("failed to refresh ring state", "error", err)
			}
		}
	}
}

// acceptProposalsWorker periodically scans for and accepts join proposals.
func (c *coordinator) acceptProposalsWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.refreshInterval)
	defer ticker.Stop()

	// Run immediately on start, then periodically
	if err := c.membership.AcceptProposals(ctx); err != nil {
		c.options.logger.Error("failed to accept proposals", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.AcceptProposals(ctx); err != nil {
				c.options.logger.Error("failed to accept proposals", "error", err)
			}
		}
	}
}

// cleanupExpiredLeasesWorker periodically checks for and removes expired leases of successors.
func (c *coordinator) cleanupExpiredLeasesWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.CleanupExpiredLeases(ctx); err != nil {
				c.options.logger.Error("failed to cleanup expired leases", "error", err)
			}
		}
	}
}
