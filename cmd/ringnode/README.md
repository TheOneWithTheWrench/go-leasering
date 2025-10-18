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

Press `Ctrl+C` in any terminal to gracefully remove that node from the ring.

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
| `--crash` | `false` | Crash mode: exit immediately on Ctrl+C without cleanup |

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

### Crash mode (simulate node failure):
```bash
go run ./cmd/ringnode --crash
```
This will exit immediately on Ctrl+C without cleaning up leases, simulating a node crash. Other nodes will detect the expired leases and clean them up.

## What You'll See

The CLI displays:
- Ring topology showing all vnodes and their positions
- Which vnodes belong to which nodes (marked with ●)
- Partition ranges owned by each vnode
- Lease expiration times (TTL)
- Node summary with vnode counts
- Live updates every 5 seconds

## Observing Ring Behavior

1. **Bootstrap**: Start one node - it will own all 1024 partitions
2. **Join**: Start a second node - watch partitions redistribute
3. **Scale**: Add more nodes - see balanced distribution
4. **Leave**: Stop a node (Ctrl+C) - watch remaining nodes absorb partitions
5. **Crash**: Start a node with `--crash` flag and hit Ctrl+C - watch other nodes detect expired leases and clean them up

## Building

```bash
go build -o bin/ringnode ./cmd/ringnode
./bin/ringnode --node-id node-1
```
