# Ringnode - Demo CLI for go-lease-ring

A command-line demonstration of the go-lease-ring library. Run multiple instances to see nodes join a distributed consistent hashing ring and coordinate partition ownership through PostgreSQL.

## Prerequisites

- PostgreSQL running locally (default: `localhost:5432`)
- Database accessible with connection string

## Quick Start

### Terminal 1 - Start first node:
```bash
go run ./cmd/ringnode --node-id node-1
```

### Terminal 2 - Start second node:
```bash
go run ./cmd/ringnode --node-id node-2
```

### Terminal 3 - Start third node:
```bash
go run ./cmd/ringnode --node-id node-3
```

Watch as the nodes automatically:
- Join the ring
- Distribute partitions evenly
- Maintain leases

Press `Ctrl+C` in any terminal to gracefully remove that node from the ring.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | (required) | Unique identifier for this node |
| `--ring-id` | `demo_ring` | Ring identifier (all nodes must use same ring-id). Must contain only lowercase letters, numbers, and underscores, starting with a letter. |
| `--vnodes` | `8` | Number of virtual nodes per physical node |
| `--ring-size` | `1024` | Total number of partitions in the ring |
| `--lease-ttl` | `30s` | How long leases last before expiring |
| `--db` | `postgres://testuser:testpassword@localhost:5432/leasering_test_db?sslmode=disable` | PostgreSQL connection URL |
| `--crash` | `false` | Crash mode: exit immediately on Ctrl+C without cleanup |

## Examples

### Custom database connection:
```bash
go run ./cmd/ringnode --node-id node-1 \
  --db "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
```

### Fast-expiring leases for testing:
```bash
go run ./cmd/ringnode --node-id node-1 --lease-ttl 5s
```

### More virtual nodes for better distribution:
```bash
go run ./cmd/ringnode --node-id node-1 --vnodes 16
```

### Different ring:
```bash
go run ./cmd/ringnode --node-id node-1 --ring-id production-ring
```

### Crash mode (simulate node failure):
```bash
go run ./cmd/ringnode --node-id node-1 --crash
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
