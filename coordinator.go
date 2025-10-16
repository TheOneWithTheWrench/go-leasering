package leasering

import (
	"context"
	"fmt"
	"time"
)

// Coordinator orchestrates the ring lifecycle and background workers.
type Coordinator struct {
	ring       *Ring
	membership *Membership
	store      *LeaseStore
	options    options
	cancel     context.CancelFunc
}

// NewCoordinator creates a new Coordinator.
func NewCoordinator(ring *Ring, membership *Membership, store *LeaseStore, opts options) *Coordinator {
	return &Coordinator{
		ring:       ring,
		membership: membership,
		store:      store,
		options:    opts,
	}
}

// Start begins the background processes: join, lease renewal, ring refresh, and proposal acceptance.
// This will block until the node successfully joins the ring.
func (c *Coordinator) Start(ctx context.Context) error {
	// Phase 1: Propose join
	if err := c.membership.ProposeJoin(ctx); err != nil {
		return fmt.Errorf("failed to propose join: %w", err)
	}

	// Phase 2: Wait for confirmation
	if err := c.waitForJoinConfirmation(ctx); err != nil {
		return fmt.Errorf("failed to join ring: %w", err)
	}

	// Refresh ring state to get initial partition ownership
	if err := c.membership.RefreshRingState(ctx); err != nil {
		return fmt.Errorf("failed to refresh ring state after join: %w", err)
	}

	// Create cancellable context for background workers
	var workerCtx context.Context
	workerCtx, c.cancel = context.WithCancel(context.Background())

	// Start background workers
	go c.renewLeaseWorker(workerCtx)
	go c.refreshRingWorker(workerCtx)
	go c.acceptProposalsWorker(workerCtx)
	go c.cleanupExpiredLeasesWorker(workerCtx)

	return nil
}

// Stop gracefully shuts down and removes this node's leases.
func (c *Coordinator) Stop(ctx context.Context) error {
	// Cancel background workers first
	if c.cancel != nil {
		c.cancel()
	}

	// Remove all leases
	return c.membership.Leave(ctx)
}

// waitForJoinConfirmation polls until all vnodes have active leases or timeout.
func (c *Coordinator) waitForJoinConfirmation(ctx context.Context) error {
	var (
		timeout   = time.After(30 * time.Second)
		ticker    = time.NewTicker(1 * time.Second)
		confirmed = false
	)
	defer ticker.Stop()

	for !confirmed {
		select {
		case <-timeout:
			return fmt.Errorf("join confirmation timeout")
		case <-ticker.C:
			var err error
			confirmed, err = c.membership.CheckJoinConfirmation(ctx)
			if err != nil {
				return fmt.Errorf("failed to check join confirmation: %w", err)
			}
		}
	}

	return nil
}

// renewLeaseWorker periodically renews this node's leases.
func (c *Coordinator) renewLeaseWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.RenewLeases(ctx); err != nil {
				fmt.Printf("failed to renew leases: %v\n", err)
			}
		}
	}
}

// refreshRingWorker periodically refreshes the ring state from the database.
func (c *Coordinator) refreshRingWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.RefreshRingState(ctx); err != nil {
				fmt.Printf("failed to refresh ring state: %v\n", err)
			}
		}
	}
}

// acceptProposalsWorker periodically scans for and accepts join proposals.
func (c *Coordinator) acceptProposalsWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.AcceptProposals(ctx); err != nil {
				fmt.Printf("failed to accept proposals: %v\n", err)
			}
		}
	}
}

// cleanupExpiredLeasesWorker periodically checks for and removes expired leases of successors.
func (c *Coordinator) cleanupExpiredLeasesWorker(ctx context.Context) {
	var ticker = time.NewTicker(c.options.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.membership.CleanupExpiredLeases(ctx); err != nil {
				fmt.Printf("failed to cleanup expired leases: %v\n", err)
			}
		}
	}
}
