package leasering

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go-leasering/database"
)

// Start begins the background processes: join, lease renewal, ring refresh, and proposal acceptance.
// This will block until the node successfully joins the ring.
func (r *Ring) Start(ctx context.Context, db *sql.DB) error {
	// Validate ringID before using it in database operations
	if err := ValidateRingID(r.ringID); err != nil {
		return fmt.Errorf("invalid ringID: %w", err)
	}

	// Run migration
	if err := database.Migrate(db, r.ringID); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	var queries = database.NewQueries(db, r.ringID)

	// Phase 1: Propose join
	if err := r.proposeJoin(ctx, queries); err != nil {
		return fmt.Errorf("failed to propose join: %w", err)
	}

	// Phase 2: Wait for confirmation
	if err := r.waitForJoinConfirmation(ctx, queries); err != nil {
		return fmt.Errorf("failed to join ring: %w", err)
	}

	// Refresh ring state to get initial partition ownership
	if err := r.refreshRingState(ctx, queries); err != nil {
		return fmt.Errorf("failed to refresh ring state after join: %w", err)
	}

	// Create cancellable context for background workers
	var workerCtx context.Context
	workerCtx, r.cancel = context.WithCancel(context.Background())

	// Start background workers
	go r.renewLeaseWorker(workerCtx, db)
	go r.refreshRingWorker(workerCtx, db)
	go r.acceptProposalsWorker(workerCtx, db)
	go r.cleanupExpiredLeasesWorker(workerCtx, db)

	return nil
}

// Stop gracefully shuts down and removes this node's leases.
func (r *Ring) Stop(ctx context.Context, db *sql.DB) error {
	// Cancel background workers first
	if r.cancel != nil {
		r.cancel()
	}

	var (
		queries   = database.NewQueries(db, r.ringID)
		positions = r.getMyVNodePositions()
	)

	// Delete all our leases
	for _, position := range positions {
		if err := queries.DeleteLease(ctx, r.ringID, position); err != nil {
			return fmt.Errorf("failed to delete lease at position %d: %w", position, err)
		}
	}

	return nil
}

// waitForJoinConfirmation polls until all vnodes have active leases or timeout.
func (r *Ring) waitForJoinConfirmation(ctx context.Context, queries *database.Queries) error {
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
			confirmed, err = r.checkJoinConfirmation(ctx, queries)
			if err != nil {
				return fmt.Errorf("failed to check join confirmation: %w", err)
			}
		}
	}

	return nil
}

// renewLeaseWorker periodically renews this node's leases.
func (r *Ring) renewLeaseWorker(ctx context.Context, db *sql.DB) {
	var (
		queries = database.NewQueries(db, r.ringID)
		ticker  = time.NewTicker(r.options.renewalInterval)
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.renewLeases(ctx, queries); err != nil {
				// Log error but continue (production would use proper logging)
				fmt.Printf("failed to renew leases: %v\n", err)
			}
		}
	}
}

// refreshRingWorker periodically refreshes the ring state from the database.
func (r *Ring) refreshRingWorker(ctx context.Context, db *sql.DB) {
	var (
		queries = database.NewQueries(db, r.ringID)
		ticker  = time.NewTicker(r.options.refreshInterval)
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.refreshRingState(ctx, queries); err != nil {
				fmt.Printf("failed to refresh ring state: %v\n", err)
			}
		}
	}
}

// acceptProposalsWorker periodically scans for and accepts join proposals.
func (r *Ring) acceptProposalsWorker(ctx context.Context, db *sql.DB) {
	var (
		queries = database.NewQueries(db, r.ringID)
		ticker  = time.NewTicker(r.options.refreshInterval)
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.acceptProposals(ctx, queries); err != nil {
				fmt.Printf("failed to accept proposals: %v\n", err)
			}
		}
	}
}

// cleanupExpiredLeasesWorker periodically checks for and removes expired leases of successors.
func (r *Ring) cleanupExpiredLeasesWorker(ctx context.Context, db *sql.DB) {
	var (
		queries = database.NewQueries(db, r.ringID)
		ticker  = time.NewTicker(r.options.refreshInterval)
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.cleanupExpiredLeases(ctx, queries); err != nil {
				fmt.Printf("failed to cleanup expired leases: %v\n", err)
			}
		}
	}
}

// renewLeases renews all of this node's leases.
func (r *Ring) renewLeases(ctx context.Context, queries *database.Queries) error {
	var (
		positions = r.getMyVNodePositions()
		now       = time.Now()
		expiresAt = now.Add(r.options.leaseTTL)
	)

	for i, position := range positions {
		var lease = &database.LeaseRecord{
			RingID:    r.ringID,
			Position:  position,
			NodeID:    r.nodeID,
			VNodeIdx:  i,
			ExpiresAt: expiresAt,
		}

		if err := queries.SetLease(ctx, lease); err != nil {
			return fmt.Errorf("failed to renew lease at position %d: %w", position, err)
		}
	}

	// Update local vnodes with new expiration time
	r.updateMyVNodeExpirations(expiresAt)

	return nil
}

// updateMyVNodeExpirations updates the ExpiresAt time for all of this node's vnodes in the local state.
func (r *Ring) updateMyVNodeExpirations(expiresAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.vnodes {
		if r.vnodes[i].NodeID == r.nodeID {
			r.vnodes[i].ExpiresAt = expiresAt
		}
	}
}

// cleanupExpiredLeases removes expired leases of immediate successors.
func (r *Ring) cleanupExpiredLeases(ctx context.Context, queries *database.Queries) error {
	// Take a snapshot of my vnodes and their successors to avoid holding lock during DB operations
	type successorInfo struct {
		myVNodePos   int
		successorPos int
		successor    VNode
	}

	r.mu.RLock()
	var successors = make([]successorInfo, 0)
	for _, vnode := range r.vnodes {
		if vnode.NodeID != r.nodeID {
			continue
		}

		// Find successor position
		var successorPos = -1
		var successorIdx = -1
		for i := range r.vnodes {
			if r.vnodes[i].Position > vnode.Position {
				successorIdx = i
				successorPos = r.vnodes[i].Position
				break
			}
		}
		// Wrap around if needed
		if successorIdx == -1 && len(r.vnodes) > 0 {
			successorIdx = 0
			successorPos = r.vnodes[0].Position
		}

		if successorPos == -1 || successorPos == vnode.Position {
			continue
		}

		successors = append(successors, successorInfo{
			myVNodePos:   vnode.Position,
			successorPos: successorPos,
			successor:    r.vnodes[successorIdx],
		})
	}
	r.mu.RUnlock()

	var now = time.Now()

	// Check each successor
	for _, info := range successors {
		if isExpired(info.successor, now) {
			if err := queries.DeleteLease(ctx, r.ringID, info.successorPos); err != nil {
				return fmt.Errorf("failed to delete expired lease at position %d: %w", info.successorPos, err)
			}
		}
	}

	return nil
}
