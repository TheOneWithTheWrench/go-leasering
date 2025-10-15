package database

import "time"

// LeaseRecord represents a lease record in the database.
type LeaseRecord struct {
	RingID    string
	Position  int
	NodeID    string
	VNodeIdx  int
	ExpiresAt time.Time
}

// ProposalRecord represents a join proposal record in the database.
type ProposalRecord struct {
	RingID         string
	PredecessorPos int
	NewNodeID      string
	NewVNodeIdx    int
	ProposedPos    int
	ExpiresAt      time.Time
}
