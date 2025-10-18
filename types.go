package leasering

import (
	"database/sql"
	"sync"
	"time"
)

// Ring represents the consistent hashing ring state.
type Ring struct {
	mu              sync.RWMutex
	nodes           map[string]*node // Quick lookup for a node's vnodes
	vnodes          []vnode          // Sorted slice of all active vnodes for fast lookups
	ownedPartitions []int            // Cached list of partitions owned by this node
	ringID          string
	nodeID          string
	options         options
	db              *sql.DB      // Database connection for persistence
	coordinator     *coordinator // Handles lifecycle and background workers
}

// node represents a member of the ring.
type node struct {
	ID     string
	VNodes []vnode
}

// vnode represents a virtual node's position on the ring and its lease.
type vnode struct {
	NodeID    string
	Index     int
	Position  int
	ExpiresAt time.Time
}

// lease represents a persisted lease record in the database.
type lease struct {
	Position  int
	NodeID    string
	VNodeIdx  int
	ExpiresAt time.Time
}

// proposal represents a join proposal record in the database.
type proposal struct {
	PredecessorPos int
	NewNodeID      string
	NewVNodeIdx    int
	ProposedPos    int
	ExpiresAt      time.Time
}
