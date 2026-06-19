# Ringnode - Demo CLI for go-lease-ring

A command-line demonstration of the go-lease-ring library. Run multiple instances to see nodes join a distributed consistent hashing ring and coordinate partition ownership through PostgreSQL.

## Prerequisites

- Docker and Docker Compose (for test database)
- Go 1.24+

## Quick Start

### Step 1 - Start the test database:
```bash
make db-up
```

This starts a PostgreSQL container configured for the demo. Wait for it to be ready.

### Step 2 - Start multiple nodes in separate terminals:

**Terminal 1:**
```bash
go run ./cmd/ringnode
```

**Terminal 2:**
```bash
go run ./cmd/ringnode
```

**Terminal 3:**
```bash
go run ./cmd/ringnode
```

Watch as the nodes automatically:
- Join the ring (node IDs are auto-generated)
- Distribute partitions evenly
- Maintain leases

Press `q` in any terminal to gracefully remove that node from the ring. Press `c` or `Ctrl+C` to simulate a crash without cleanup.

### Step 3 - Stop the database when done:
```bash
make db-down
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--ring-id` | `demo_ring` | Ring identifier (all nodes must use same ring-id). Must contain only lowercase letters, numbers, and underscores, starting with a letter. |
| `--vnodes` | `8` | Number of virtual nodes per physical node |
| `--lease-ttl` | `10s` | How long leases last before expiring |
| `--db` | `postgres://testuser:testpassword@localhost:5432/leasering_test_db?sslmode=disable` | PostgreSQL connection URL |

**Note:** Node IDs are automatically generated (e.g., `node_abc123`). You don't need to specify them.

## Examples

### Custom database connection:
```bash
go run ./cmd/ringnode --db "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
```

### Fast-expiring leases for testing:
```bash
go run ./cmd/ringnode --lease-ttl 5s
```

### More virtual nodes for better distribution:
```bash
go run ./cmd/ringnode --vnodes 16
```

### Different ring:
```bash
go run ./cmd/ringnode --ring-id production_ring
```

### Simulate node failure:
Run a node normally, then press `c` or `Ctrl+C`. The process exits immediately without cleaning up leases. Other nodes will stop treating the expired leases as active and will clean them up.

## What You'll See

The CLI displays:
- Ring topology showing all vnodes and their positions
- Which vnodes belong to which nodes (marked with ●)
- Partition ranges owned by each vnode
- Lease expiration times (TTL)
- Node summary with vnode counts
- Live updates every 1 second
- Controls for graceful quit, crash, and database disconnect/reconnect

## Observing Ring Behavior

1. **Bootstrap**: Start one node - it will own all 1024 partitions
2. **Join**: Start a second node - watch partitions redistribute
3. **Scale**: Add more nodes - see balanced distribution
4. **Leave**: Press `q` - watch remaining nodes absorb partitions after graceful cleanup
5. **Crash**: Press `c` or `Ctrl+C` - watch other nodes ignore and clean up expired leases
6. **Database disconnect**: Press `d`, then `r` to reconnect and rejoin

## Building

```bash
go build -o bin/ringnode ./cmd/ringnode
./bin/ringnode --ring-id demo_ring --vnodes 8
```
