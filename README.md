# go-leasering

> ⚠️ **Work in Progress** - This library is HIGHLY experimental and not production-ready. Use at your own risk.

A distributed consistent hashing ring implementation with lease-based coordination via PostgreSQL.

## Overview

`go-leasering` enables multiple nodes to coordinate partition ownership through a consistent hashing ring, using PostgreSQL as the coordination layer. Each node claims virtual nodes (vnodes) on the ring through time-limited leases, automatically distributing work across the cluster.

The public API answers one question: "which partitions should this process own right now?" Callers periodically read `GetOwnedPartitions()` and process work for those partitions.

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

The ring has a fixed partition space of `0..1023`. Each process owns one or more virtual nodes. A vnode is a point on the ring backed by a PostgreSQL lease.

Example topology with two vnodes:

```text
Ring: example_ring
Size: 1024 | Nodes: 2 | vnodes: 2

Ring Topology:
  A @ 100    owns (900..1023,0..100]
  B @ 900    owns (100..900]
```

Ownership is defined as `(predecessor, self]`:

- The vnode at `100` owns partitions after its predecessor up to and including `100`.
- The vnode at `900` owns partitions after `100` up to and including `900`.
- Wrap-around ranges continue through `1023` and then `0`.

With virtual nodes, one physical process has multiple points on the ring. Its owned partitions are the union of the ranges owned by its vnodes.

### Join Handoff

When a new vnode joins at position `X`, two existing positions matter:

```text
Before:
  A @ 100    owns (900..1023,0..100]
  B @ 900    owns (100..900]

New vnode:
  X @ 300
```

Before `X` joins, `B` owns `(100,900]`, including `300`.

After `X` joins:

```text
After:
  A @ 100    owns (900..1023,0..100]
  X @ 300    owns (100..300]
  B @ 900    owns (300..900]
```

`A` is only the predecessor/range boundary. It is not the node giving up work.

The current owner (`B`) must accept the join because it is the node that has to stop serving `(100,300]`. To preserve at-most-one ownership, the accepting node removes the transferred partitions from its local ownership before activating the new lease for `X`. This can create a short availability gap during handoff, but avoids two nodes processing the same partition.

### Lifecycle

1. **Bootstrap** - The first active node creates leases for its vnodes.
2. **Join Proposal** - New nodes propose vnodes at deterministic hash positions.
3. **Lease Acceptance** - The current owner of each proposed position accepts and activates the new lease.
4. **Continuous Renewal** - Nodes periodically renew leases they still own.
5. **Ring Refresh** - Nodes read active leases to refresh cached ownership.
6. **Failure Detection** - Nodes periodically remove expired leases and proposals from PostgreSQL.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithVNodeCount(n)` | 8 | Virtual nodes per physical node |
| `WithLeaseTTL(d)` | 30s | Lease time-to-live. Renewal, refresh, and join timeout intervals are derived from this value. |
| `WithLogger(logger)` | no-op logger | Logger used for ring lifecycle and worker errors. Passing `nil` restores the no-op logger. |

Ring IDs must be valid PostgreSQL identifiers and short enough to leave room for generated table and index suffixes.

## Requirements

- PostgreSQL 12+
- Go 1.26.4+

## Troubleshooting

### "relation does not exist" or search_path errors

If you encounter errors like `relation "demo_ring_leases" does not exist` or search_path related issues, your PostgreSQL connection may not be configured to look in the correct schema.

**Solutions:**

1. **Add search_path to connection string** (recommended):
   ```
   postgres://user:pass@localhost:5432/db?sslmode=disable&search_path=public
   ```

2. **Set search_path at database level**:
   ```sql
   ALTER DATABASE yourdb SET search_path TO public;
   ```

3. **Set search_path for your user**:
   ```sql
   ALTER ROLE youruser SET search_path TO public;
   ```

The library creates tables in the schema determined by your PostgreSQL connection's `search_path` setting (typically `public` by default).

## Future Work

This library needs significant work before production readiness:

### Think more about the API
- [ ] Document better how to use the API
- [ ] Consider what options should actually be exposed to clients

### Observability & Metrics
- [ ] Expose Prometheus metrics (lease renewals, partition ownership, failures)
- [ ] Tracing support for distributed operations
- [ ] Health check endpoints

### Testing & Reliability
- [ ] High-load testing (hundreds of nodes)
- [ ] Chaos engineering tests (network partitions, database failures)
- [ ] Benchmark partition rebalancing performance
- [ ] Long-running stability tests
- [ ] PostgreSQL connection pool tuning and testing
