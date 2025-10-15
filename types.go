package leasering

import (
	"context"
	"sync"
	"time"
)

// Ring represents the consistent hashing ring state.
type Ring struct {
	mu              sync.RWMutex
	nodes           map[string]*Node // Quick lookup for a node's vnodes
	vnodes          []VNode          // Sorted slice of all active vnodes for fast lookups
	ownedPartitions []int            // Cached list of partitions owned by this node
	ringID          string
	nodeID          string
	options         options
	cancel          context.CancelFunc // Cancel function for background workers
}

// Node represents a member of the ring.
type Node struct {
	ID     string
	VNodes []VNode
}

// VNode represents a virtual node's position on the ring and its lease.
type VNode struct {
	NodeID    string
	Index     int
	Position  int
	ExpiresAt time.Time
}

// Lease represents a persisted lease record in the database.
type Lease struct {
	Position  int
	NodeID    string
	VNodeIdx  int
	ExpiresAt time.Time
}

// Proposal represents a join proposal record in the database.
type Proposal struct {
	PredecessorPos int
	NewNodeID      string
	NewVNodeIdx    int
	ProposedPos    int
	ExpiresAt      time.Time
}

// options configures the Ring behavior (internal only).
type options struct {
	ringSize        int
	vnodeCount      int
	leaseTTL        time.Duration
	renewalInterval time.Duration
	refreshInterval time.Duration
	proposalTTL     time.Duration
}

// defaultOptions returns sensible defaults.
func defaultOptions() options {
	var (
		leaseTTL = 30 * time.Second
	)
	return options{
		ringSize:        1024,
		vnodeCount:      8,
		leaseTTL:        leaseTTL,
		renewalInterval: leaseTTL / 3,
		refreshInterval: leaseTTL / 2,
		proposalTTL:     10 * time.Second,
	}
}

// Option is a functional option for configuring a Ring.
type Option func(*options)

// WithLeaseTTL sets the lease time-to-live duration.
func WithLeaseTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.leaseTTL = ttl
		o.renewalInterval = ttl / 3
		o.refreshInterval = ttl / 2
	}
}

// WithVNodeCount sets the number of virtual nodes per physical node.
func WithVNodeCount(count int) Option {
	return func(o *options) {
		o.vnodeCount = count
	}
}
