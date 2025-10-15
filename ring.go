package leasering

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	// ErrInvalidRingID is returned when the ringID contains invalid characters
	ErrInvalidRingID = errors.New("ringID must contain only lowercase letters, numbers, and underscores, and start with a letter")

	// validRingIDPattern validates PostgreSQL-safe identifiers
	validRingIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// NewRing creates a new Ring instance.
// The ringID must be a valid PostgreSQL identifier (lowercase letters, numbers, underscores, starting with a letter).
func NewRing(ringID, nodeID string, opts ...Option) *Ring {
	var options = defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	return &Ring{
		nodes:           make(map[string]*Node),
		vnodes:          make([]VNode, 0),
		ownedPartitions: make([]int, 0),
		ringID:          ringID,
		nodeID:          nodeID,
		options:         options,
	}
}

// ValidateRingID checks if the ringID is valid for use as a PostgreSQL identifier.
func ValidateRingID(ringID string) error {
	if ringID == "" {
		return errors.New("ringID cannot be empty")
	}

	if len(ringID) > 63 {
		return errors.New("ringID must be 63 characters or less")
	}

	if !validRingIDPattern.MatchString(ringID) {
		return ErrInvalidRingID
	}

	return nil
}

// GetOwnedPartitions returns all partition numbers this node is currently responsible for.
// Partition numbers range from 0 to ringSize-1 (default 0-1023).
// This is a hot path function and returns a cached result.
func (r *Ring) GetOwnedPartitions() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ownedPartitions
}

// rebuildFromLeases rebuilds the in-memory ring state from a list of leases.
// This recalculates owned partitions.
func (r *Ring) rebuildFromLeases(leases []*Lease) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Clear existing state
	r.nodes = make(map[string]*Node)
	r.vnodes = make([]VNode, 0, len(leases))

	// Rebuild from leases
	for _, lease := range leases {
		var vnode = VNode{
			NodeID:    lease.NodeID,
			Index:     lease.VNodeIdx,
			Position:  lease.Position,
			ExpiresAt: lease.ExpiresAt,
		}

		// Add to nodes map
		var node, exists = r.nodes[vnode.NodeID]
		if !exists {
			node = &Node{
				ID:     vnode.NodeID,
				VNodes: make([]VNode, 0),
			}
			r.nodes[vnode.NodeID] = node
		}
		node.VNodes = append(node.VNodes, vnode)

		// Add to vnodes slice
		r.vnodes = append(r.vnodes, vnode)
	}

	// Sort vnodes by position
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Position < r.vnodes[j].Position
	})

	// Recalculate owned partitions
	r.ownedPartitions = r.calculateOwnedPartitions()
}

// addVNode adds a single vnode to the ring and recalculates owned partitions.
// This is used when accepting proposals to immediately update local state.
func (r *Ring) addVNode(vnode VNode) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Add to nodes map
	var node, exists = r.nodes[vnode.NodeID]
	if !exists {
		node = &Node{
			ID:     vnode.NodeID,
			VNodes: make([]VNode, 0),
		}
		r.nodes[vnode.NodeID] = node
	}
	node.VNodes = append(node.VNodes, vnode)

	// Add to vnodes slice and keep sorted
	r.vnodes = append(r.vnodes, vnode)
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Position < r.vnodes[j].Position
	})

	// Recalculate owned partitions
	r.ownedPartitions = r.calculateOwnedPartitions()
}

// calculateOwnedPartitions computes which partitions this node owns.
// Must be called with lock held.
func (r *Ring) calculateOwnedPartitions() []int {
	var (
		ringSize   = r.options.ringSize
		partitions = make([]int, 0)
	)

	if len(r.vnodes) == 0 {
		return partitions
	}

	// Each vnode owns the range from the previous vnode's position (exclusive) to its own position (inclusive).
	for i, vnode := range r.vnodes {
		if vnode.NodeID != r.nodeID {
			continue
		}

		var (
			start int
			end   = vnode.Position
		)

		if i == 0 {
			// Wrap around: from last vnode to this one
			if len(r.vnodes) == 1 {
				// Only vnode owns entire ring
				for p := 0; p < ringSize; p++ {
					partitions = append(partitions, p)
				}
				continue
			}
			start = r.vnodes[len(r.vnodes)-1].Position
		} else {
			start = r.vnodes[i-1].Position
		}

		// Handle wrap-around case
		if start >= end {
			for p := start + 1; p < ringSize; p++ {
				partitions = append(partitions, p)
			}
			for p := 0; p <= end; p++ {
				partitions = append(partitions, p)
			}
		} else {
			// Normal case: add all positions from start (exclusive) to end (inclusive)
			for p := start + 1; p <= end; p++ {
				partitions = append(partitions, p)
			}
		}
	}

	return partitions
}

// findPredecessor returns the counter-clockwise predecessor position for a given position.
// Returns -1 if the ring is empty.
func (r *Ring) findPredecessor(position int) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return -1
	}

	// Binary search for the first vnode with position >= target
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Position >= position
	})

	// Counter-clockwise predecessor is the previous index
	if idx == 0 {
		// Wrap around to last vnode
		return r.vnodes[len(r.vnodes)-1].Position
	}
	return r.vnodes[idx-1].Position
}

// getMyVNodePositions returns all vnode positions that this node should own.
func (r *Ring) getMyVNodePositions() []int {
	var positions []int
	for i := range r.options.vnodeCount {
		pos := hashNodePosition(r.nodeID, i, r.options.ringSize)
		positions = append(positions, pos)
	}
	return positions
}

// getSuccessorPosition returns the next vnode position after the given position.
// Returns -1 if ring is empty.
func (r *Ring) getSuccessorPosition(position int) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return -1
	}

	// Binary search for the first vnode with position > target
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Position > position
	})

	// Wrap around if needed
	if idx >= len(r.vnodes) {
		return r.vnodes[0].Position
	}
	return r.vnodes[idx].Position
}

// getVNodeAtPosition returns the vnode at the given position, if it exists.
func (r *Ring) getVNodeAtPosition(position int) *VNode {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for i := range r.vnodes {
		if r.vnodes[i].Position == position {
			return &r.vnodes[i]
		}
	}
	return nil
}

// isExpired checks if a vnode's lease has expired.
func isExpired(vnode VNode, now time.Time) bool {
	return now.After(vnode.ExpiresAt)
}

// String returns a visual representation of the ring state.
func (r *Ring) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var b strings.Builder

	b.WriteString(fmt.Sprintf("Ring: %s (Node: %s)\n", r.ringID, r.nodeID))
	b.WriteString(fmt.Sprintf("Size: %d | Nodes: %d | VNodes: %d\n",
		r.options.ringSize, len(r.nodes), len(r.vnodes)))
	b.WriteString(fmt.Sprintf("Owned Partitions: %d\n", len(r.ownedPartitions)))

	if len(r.vnodes) == 0 {
		b.WriteString("\n[Empty Ring]\n")
		return b.String()
	}

	b.WriteString("\nRing Topology:\n")
	b.WriteString("┌─────────────────────────────────────────────────────────────┐\n")

	for i, vnode := range r.vnodes {
		var (
			prevPos  int
			rangeEnd = vnode.Position
		)

		if i == 0 {
			prevPos = r.vnodes[len(r.vnodes)-1].Position
		} else {
			prevPos = r.vnodes[i-1].Position
		}

		var (
			isMine = vnode.NodeID == r.nodeID
			marker = " "
			ttl    = time.Until(vnode.ExpiresAt).Round(time.Second)
		)

		if isMine {
			marker = "●"
		}

		var rangeStr string
		if prevPos >= rangeEnd {
			rangeStr = fmt.Sprintf("(%d..%d,0..%d]", prevPos, r.options.ringSize-1, rangeEnd)
		} else {
			rangeStr = fmt.Sprintf("(%d..%d]", prevPos, rangeEnd)
		}

		b.WriteString(fmt.Sprintf("│ %s @%-5d  %-15s  %-25s  ttl:%s\n",
			marker, vnode.Position, vnode.NodeID, rangeStr, ttl))
	}

	b.WriteString("└─────────────────────────────────────────────────────────────┘\n")

	// Node summary
	b.WriteString("\nNode Summary:\n")
	for nodeID, node := range r.nodes {
		var (
			vnodeCount = len(node.VNodes)
			marker     = " "
		)
		if nodeID == r.nodeID {
			marker = "●"
		}
		b.WriteString(fmt.Sprintf("  %s %-15s  vnodes: %d\n", marker, nodeID, vnodeCount))
	}

	return b.String()
}
