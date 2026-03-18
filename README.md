# SupaSwarm

SupaSwarm is a Go-based distributed control plane and replicated key-value service designed for a distributed systems course project. It presents a “Supabase-inspired cluster” story, but the graded core is the original distributed machinery implemented here: membership, leader election, Lamport clocks, ordered replication, failover, and lease-based resource management.

## What The System Does

Each node runs the same Go binary and exposes:

- a public API for key-value reads and writes
- a public API for distributed lease acquisition/release
- an internal cluster API for join, heartbeat, election, append, and sync
- a web dashboard for debugging and demos

The system is intentionally small so the distributed behavior stays visible and explainable.

## Architecture

The implementation is split into four layers:

1. Node and cluster layer
- Each node has a numeric `nodeId`.
- The first node runs `cluster init`, which creates or accepts a `clusterId` and `joinToken`.
- Other nodes run `node join` with the same `clusterId` and `joinToken`.
- The leader maintains the membership table and shares it during heartbeats.

2. Coordination layer
- Nodes use Lamport clocks to attach logical time to distributed events.
- Nodes use a Bully election algorithm:
  - when heartbeats from the leader stop arriving
  - a node contacts higher-ID nodes
  - if no higher node responds, it declares itself leader
- The highest reachable node should therefore win after failure.

3. Replication layer
- All client writes go through the current leader.
- The leader creates a log entry containing:
  - log index
  - term
  - Lamport timestamp
  - operation payload
- The leader sends the entry to followers.
- The entry is committed only after a majority acknowledges it.
- Followers apply only the committed prefix of the log.
- If a follower falls behind, it requests a full sync from the leader.

4. Visibility layer
- Each node serves a dashboard at `/dashboard`.
- The dashboard polls `/api/v1/status` once per second.
- It visualizes:
  - membership
  - leader identity
  - term and Lamport time
  - log and commit progress
  - current data state
  - distributed lease ownership

## Core Data Structures

The most important state objects are:

- `Member`
  - tracks node ID, address, health, heartbeat timing, and replication progress
- `LogEntry`
  - the replicated command object for both key-value writes and lease changes
- `LeaseState`
  - the distributed resource ownership record
- `persistentState`
  - the full on-disk JSON snapshot used for restart recovery

## Request Flow

### Cluster bootstrap

1. Start the first node with `cluster init`.
2. The node sets itself as leader and stores the cluster credentials.
3. Start follower nodes with `node join`.
4. The leader validates their token and returns cluster state.

### Write path

1. A client sends `PUT /api/v1/kv/{key}` to any node.
2. If the node is not leader, it forwards the request to the leader.
3. The leader creates a `put` log entry.
4. Followers append the entry.
5. After quorum acknowledgement, the leader advances the commit index.
6. Committed entries are replayed into the key-value state machine.

### Lease path

1. A client sends `POST /api/v1/lock/acquire`.
2. The leader checks whether the lease is already held.
3. If available, the leader replicates a `lease_acquire` log entry.
4. The committed log updates the lease state across the cluster.

### Failover path

1. Followers stop receiving heartbeats from the leader.
2. After the election timeout, they start the Bully election.
3. The highest live node becomes leader.
4. The new leader truncates any uncommitted tail and continues from the committed prefix.

## Why The Consistency Model Is Simple

This project is not trying to implement full decentralized consensus or SQL transactions. The design instead uses:

- leader-based ordering
- majority commit for writes
- committed-prefix recovery after failover
- full-state catch-up for lagging followers

That keeps the implementation achievable while still demonstrating real distributed consistency behavior.

## API Overview

### Public endpoints

- `GET /health`
- `GET /api/v1/status`
- `PUT /api/v1/kv/{key}`
- `GET /api/v1/kv/{key}`
- `POST /api/v1/lock/acquire`
- `POST /api/v1/lock/release`
- `GET /dashboard`

### Internal endpoints

- `POST /internal/cluster/join`
- `POST /internal/cluster/heartbeat`
- `POST /internal/cluster/election`
- `POST /internal/cluster/coordinator`
- `POST /internal/cluster/append`
- `POST /internal/cluster/sync`

## Running Locally

### Directly with Go

Start the first node:

```bash
go run ./cmd/supaswarm cluster init \
  --node-id 1 \
  --listen-addr :8080 \
  --advertise-addr http://127.0.0.1:8080 \
  --cluster-id demo-cluster \
  --join-token demo-secret \
  --data-dir data/node-1
```

Start follower nodes in separate terminals:

```bash
go run ./cmd/supaswarm node join \
  --node-id 2 \
  --listen-addr :8081 \
  --advertise-addr http://127.0.0.1:8081 \
  --cluster-id demo-cluster \
  --join-token demo-secret \
  --manager http://127.0.0.1:8080 \
  --data-dir data/node-2
```

```bash
go run ./cmd/supaswarm node join \
  --node-id 3 \
  --listen-addr :8082 \
  --advertise-addr http://127.0.0.1:8082 \
  --cluster-id demo-cluster \
  --join-token demo-secret \
  --manager http://127.0.0.1:8080 \
  --data-dir data/node-3
```

Send a write:

```bash
go run ./cmd/supaswarm put --addr http://127.0.0.1:8081 --key project --value supaswarm
```

Read a value:

```bash
go run ./cmd/supaswarm get --addr http://127.0.0.1:8082 --key project
```

Inspect cluster status:

```bash
go run ./cmd/supaswarm cluster status --addr http://127.0.0.1:8080
```

Acquire the distributed lease:

```bash
go run ./cmd/supaswarm lock acquire --addr http://127.0.0.1:8080 --name maintenance --owner reporter --ttl 30s
```

Open the dashboard:

- [node1 dashboard](http://127.0.0.1:8080/dashboard)
- [node2 dashboard](http://127.0.0.1:8081/dashboard)
- [node3 dashboard](http://127.0.0.1:8082/dashboard)

### With Docker Compose

Build and run the 3-node cluster:

```bash
docker compose up --build
```

The demo cluster will be available at:

- [node1](http://127.0.0.1:8081/dashboard)
- [node2](http://127.0.0.1:8082/dashboard)
- [node3](http://127.0.0.1:8083/dashboard)

## Test Coverage

The automated tests cover:

- rejecting joins with an invalid token
- replication to followers
- follower rejoin and catch-up using leader sync
- leader failover using Bully election
- lease exclusivity and release

Run the tests with:

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

## Stretch Goals / Extra Wow Features

These are intentionally documented as optional follow-ups so the team can revisit them later without destabilizing the core demo:

- dashboard controls for simulated failures or partitions
- dual consistency modes such as `fast` versus `safe`
- follower reads with explicit staleness semantics
- incremental anti-entropy sync instead of full-state catch-up
- replacing the local key-value store with Postgres while keeping the same control plane
