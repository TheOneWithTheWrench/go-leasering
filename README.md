# go-leasering

> ⚠️ **Work in Progress** - This library is  HIGHLY experimental and not production-ready. Use at your own risk.

A distributed consistent hashing ring implementation with lease-based coordination via PostgreSQL.

## Overview

`go-leasering` enables multiple nodes to coordinate partition ownership through a consistent hashing ring, using PostgreSQL as the coordination layer. Each node claims virtual nodes (vnodes) on the ring through time-limited leases, automatically distributing work across the cluster.

## Key Features

- **Consistent Hashing** - Predictable partition distribution using virtual nodes
- **Lease-Based Coordination** - Time-limited ownership with automatic renewal
- **Database-Centric** - No leader election, uses PostgreSQL for coordination
- **Graceful Scaling** - Nodes join and leave without disrupting the cluster
- **Crash Detection** - Expired leases are cleaned up by surviving nodes

## Quick Start

```go
import "go-leasering"

// Create a ring node (node ID is generated automatically)
ring := leasering.NewRingNode(
    db,                  // PostgreSQL connection
    "my_ring",           // Ring ID (must be valid PostgreSQL identifier)
    leasering.WithVNodeCount(8),
    leasering.WithLeaseTTL(30*time.Second),
)

// Join the ring
ctx := context.Background()
if err := ring.Start(ctx); err != nil {
    log.Fatal(err)
}
defer ring.Stop(ctx)

// Get owned partitions
partitions := ring.GetOwnedPartitions() // Near free to call
for _, p := range partitions {
    // Process work for partition p
}
```

## Demo CLI

A demonstration CLI is included to visualize ring behavior:

```bash
# Terminal 1
go run ./cmd/ringnode

# Terminal 2
go run ./cmd/ringnode

# Terminal 3
go run ./cmd/ringnode
```

See `cmd/ringnode/README.md` for more details.

## How It Works

1. **Join Protocol** - Nodes propose vnodes at deterministic hash positions
2. **Lease Acceptance** - Existing nodes accept proposals and grant leases
3. **Continuous Renewal** - Nodes periodically renew their leases
4. **Ring Refresh** - Nodes read all leases to maintain consistent ring view
5. **Failure Detection** - Nodes clean up expired leases of their successors

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithVNodeCount(n)` | 8 | Virtual nodes per physical node |
| `WithLeaseTTL(d)` | 30s | Lease time-to-live |
| `WithRingSize(n)` | 1024 | Total partition positions |

## Requirements

- PostgreSQL 12+
- Go 1.24+

## Future Work

This library needs significant work before production readiness:

### Think more about the API
- [ ] Document better how to use the API
- [ ] Consider what options should actually be exposed to clients

### Architecture Refactoring
- [ ] Decouple business logic from Ring struct (too monolithic)
- [ ] Separate concerns: coordination, state management, workers
- [ ] Introduce cleaner interfaces for extensibility
- [ ] Better separation between database and domain layers

### Observability & Metrics
- [ ] Add structured logging (slog integration)
- [ ] Expose Prometheus metrics (lease renewals, partition ownership, failures)
- [ ] Tracing support for distributed operations
- [ ] Health check endpoints

### Testing & Reliability
- [ ] High-load testing (hundreds of nodes)
- [ ] Chaos engineering tests (network partitions, database failures)
- [ ] Benchmark partition rebalancing performance
- [ ] Long-running stability tests
- [ ] PostgreSQL connection pool tuning and testing
