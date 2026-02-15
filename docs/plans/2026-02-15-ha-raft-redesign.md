# HA Stack Redesign with Apache Ratis

**Date:** 2026-02-15
**Status:** Design approved

## Overview

A brand new High Availability stack for ArcadeDB, built from scratch using Apache Ratis for Raft consensus. Implemented as a standalone `ha-raft/` module, completely separate from the existing HA code in `server/`. The existing HA remains untouched; a configuration flag (`HA_IMPLEMENTATION`) switches between legacy and new.

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Consensus library | Apache Ratis | Mature, Apache 2.0, proven in Ozone/Hadoop |
| Replication unit | Physical (WAL-based) | Fast, deterministic, proven in current HA |
| Network transport | Ratis built-in gRPC | Well-tested, path of least resistance |
| Quorum modes | NONE and MAJORITY (manual config) | Explicit, validated at startup |
| Replica writes | Forwarded to leader | Transparent to all client types |
| Snapshot strategy | Full copy + incremental checksum | Full for fresh replicas, incremental for returning |
| Module approach | New standalone `ha-raft/` | Old code untouched, config flag to switch |
| 2-node failover | Manual FORCE_LEADER only | No auto-promote without majority (split-brain safety) |

## Architecture

```
+---------------------------------------------+
|              ArcadeDB Server                |
|         (loads HA plugin via config)        |
+---------------------------------------------+
|           ha-raft module                    |
|  +---------------------------------------+  |
|  |  RaftHAPlugin (ServerPlugin impl)     |  |
|  |  - lifecycle, config, cluster mgmt    |  |
|  +---------------------------------------+  |
|  |  ArcadeStateMachine (Ratis SM)        |  |
|  |  - applies WAL entries to database    |  |
|  |  - snapshot: full copy / incremental  |  |
|  +---------------------------------------+  |
|  |  RaftReplicatedDatabase (wrapper)     |  |
|  |  - intercepts commits, submits to     |  |
|  |    Raft log via Ratis client          |  |
|  |  - forwards replica writes to leader  |  |
|  +---------------------------------------+  |
|  |  Apache Ratis (gRPC transport)        |  |
|  |  - leader election, log replication   |  |
|  |  - snapshotting, membership changes   |  |
|  +---------------------------------------+  |
+---------------------------------------------+
```

## Raft Integration & State Machine

### Raft Group Setup

One Raft group manages the entire cluster. Each ArcadeDB server instance runs a single Ratis `RaftServer`. The Ratis peer list is built from the configured `HA_SERVER_LIST` at startup.

### ArcadeStateMachine

Implements Ratis's `StateMachine` interface:

- **`applyTransaction(TransactionContext)`** -- Receives committed Raft log entries containing serialized WAL changes (page diffs). Deserializes and applies them to local database files.
- **`takeSnapshot()`** -- Flushes database to disk and records the current Raft log index. Persists snapshot metadata (file list, sizes, checksums).
- **`installSnapshot(SnapshotInfo)`** -- On lagging replicas: full file copy for fresh replicas, checksum-based incremental sync for recently-offline replicas.

### Transaction Flow (Leader)

1. Client commits a transaction
2. `RaftReplicatedDatabase` intercepts the commit
3. Serializes WAL changes into a byte buffer
4. Submits as a Raft log entry via `RaftClient.io().send()`
5. Ratis replicates to followers, achieves consensus (or skips if NONE quorum)
6. `ArcadeStateMachine.applyTransaction()` applies locally
7. Commit returns to client

### Transaction Flow (Replica Write)

1. Client sends write to replica
2. `RaftReplicatedDatabase` detects this node is not the leader
3. Forwards the request to the leader via an internal `RaftClient`
4. Leader processes through normal Raft flow
5. Replica's state machine eventually applies the committed entry
6. Response returned to client

## No-Quorum (Async) Mode

When `HA_QUORUM=NONE`, the leader commits log entries locally without waiting for replica acknowledgment.

- Ratis configured with a custom `ReplicationPolicy` that commits on leader-append only
- Ratis still sends entries to replicas via AppendEntries RPC -- replicas apply them in order, just asynchronously
- Raft log on the leader maintains full ordering and durability

### Replication Gap Tracking

- Leader tracks each replica's `matchIndex` (standard Ratis behavior)
- Gap between leader's `commitIndex` and replica's `matchIndex` exposed as a metric
- Configurable `HA_REPLICATION_LAG_WARNING` threshold triggers log warnings

### Failure Behavior (No-Quorum)

- **Replica down:** Leader continues. Log entries accumulate. Replica catches up on return.
- **Replica returns:** Ratis resumes AppendEntries automatically. Snapshot resync if too far behind.
- **Leader down (2-node):** Replica does NOT auto-promote (no majority). Requires manual `FORCE_LEADER` command.

## Schema Changes & DDL Replication

### Two Log Entry Types

- **`TX_ENTRY`** -- Serialized WAL page diffs. Applied by replaying page changes.
- **`SCHEMA_ENTRY`** -- Schema change command (e.g., "create type Person"). Applied by executing DDL locally.

Schema changes are separate because they trigger side effects (file creation, index initialization) that can't be captured as page diffs.

### Schema Change Flow

1. Client issues DDL command
2. `RaftReplicatedDatabase` intercepts, serializes as `SCHEMA_ENTRY`
3. Submitted to Raft log
4. On commit, each node's state machine executes the DDL locally

### Index Operations

- `CREATE INDEX` / `REBUILD INDEX` -- Replicated as `SCHEMA_ENTRY`, each node builds locally
- Index compaction -- Local operation, not replicated

### Ordering Guarantee

Both entry types go through the same Raft log. Total ordering is preserved -- a `CREATE TYPE` always appears before any `INSERT` into that type.

## Module Structure

### Maven Dependencies

```
ha-raft/
  arcadedb-engine (compile)
  arcadedb-server (provided)            -- same pattern as protocol modules
  arcadedb-server test-jar (test)
  apache-ratis-server (compile)
  apache-ratis-grpc (compile)           -- gRPC transport
  arcadedb-network (compile)            -- for write-forwarding channel
```

### Key Classes

| Class | Responsibility |
|-------|----------------|
| `RaftHAPlugin` | `ServerPlugin` impl. Lifecycle, config validation, cluster setup |
| `RaftHAServer` | Manages Ratis `RaftServer` instance, peer list, monitoring |
| `ArcadeStateMachine` | Ratis `StateMachine` impl. Applies TX/SCHEMA entries, snapshots |
| `RaftReplicatedDatabase` | Database wrapper. Intercepts commits, submits to Raft log |
| `RaftLogEntryCodec` | Serializes/deserializes WAL changes and schema commands |
| `WriteForwarder` | On replicas, forwards client writes to leader |
| `SnapshotManager` | Full copy and incremental checksum-based snapshot logic |
| `ClusterMonitor` | Tracks replication lag, peer status, exposes metrics |

### Configuration

New settings:
- `HA_IMPLEMENTATION` -- `legacy` or `raft` (default: `legacy`)
- `HA_RAFT_PORT` -- Port for Ratis gRPC communication
- `HA_REPLICATION_LAG_WARNING` -- Log index gap threshold for warnings

Reused settings (same semantics): `HA_QUORUM`, `HA_SERVER_LIST`, `HA_CLUSTER_NAME`, `HA_SERVER_ROLE`, `HA_K8S`, `HA_K8S_DNS_SUFFIX`.

## Testing Strategy

### Unit Tests (no cluster startup)

- `RaftLogEntryCodecTest` -- Serialization round-trips for TX and SCHEMA entries
- `ArcadeStateMachineTest` -- State machine apply logic with mocked database
- `ClusterMonitorTest` -- Replication lag calculation, threshold warnings
- `WriteForwarderTest` -- Request forwarding logic, error handling
- `ConfigValidationTest` -- Quorum/cluster-size validation warnings

### Integration Tests (real multi-node clusters, in-process)

- `RaftReplication2NodesIT` -- 2-node no-quorum: basic replication, replica lag tracking
- `RaftReplication3NodesIT` -- 3-node majority quorum: standard Raft replication
- `RaftReplicationSchemaIT` -- Schema changes replicated correctly
- `RaftReplicationIndexIT` -- Index creation, rebuild, compaction across cluster
- `RaftWriteForwardingIT` -- Client writes to replica, forwarded to leader
- `RaftHotResyncIT` -- Replica catches up via Raft log replay
- `RaftFullResyncIT` -- Fresh replica receives full snapshot
- `RaftIncrementalResyncIT` -- Returning replica gets checksum-based incremental sync

### Failure Scenario Tests

- `RaftLeaderFailoverIT` -- Leader dies, new leader elected (3+ nodes, MAJORITY)
- `RaftLeaderDown2NodesIT` -- Leader dies in 2-node no-quorum, no auto-promote, FORCE_LEADER works
- `RaftReplicaFailureIT` -- Replica dies during writes, leader continues, replica catches up
- `RaftSplitBrain3NodesIT` -- Partition isolates 1 node, majority continues, minority stops
- `RaftSplitBrain5NodesIT` -- 5 nodes split 2+3, majority elects leader, rejoin heals
- `RaftRandomCrashIT` -- Random restarts during concurrent writes, consistency verified
- `RaftQuorumLostIT` -- Majority down, writes fail, cluster recovers when nodes return
- `RaftLeaderElectionRaceIT` -- Multiple candidates, exactly one leader emerges

### Test Infrastructure

- `BaseRaftHATest` extending `BaseGraphServerTest` -- multi-server startup with Ratis config
- Network partition simulation via Ratis `SimulatedRequestTimeout` or gRPC port blocking
- `assertClusterConsistency()` utility -- verifies identical data across all nodes

## Feature Parity Checklist

### Must Have

- [ ] Leader election and automatic failover (3+ nodes)
- [ ] WAL-based transaction replication
- [ ] Schema/DDL replication (types, properties, indexes, buckets)
- [ ] Quorum modes: NONE and MAJORITY
- [ ] Write forwarding from replica to leader
- [ ] Hot resync (log-based catchup)
- [ ] Full resync (snapshot-based for new/stale replicas)
- [ ] Database alignment verification (checksum-based)
- [ ] Kubernetes discovery support (HA_K8S, DNS suffix)
- [ ] Cluster info exposed via HTTP API
- [ ] Backup operations in HA mode
- [ ] Multi-database replication

### Intentionally Dropped

- Quorum modes ONE, TWO, THREE, ALL -- replaced by NONE and MAJORITY
- Custom election protocol -- replaced by Ratis
- Custom binary replication protocol -- replaced by Ratis gRPC
- Custom replication log file format -- replaced by Ratis log

### New Capabilities

- [ ] Proper Raft consensus with proven correctness guarantees
- [ ] Replication lag monitoring with configurable warning thresholds
- [ ] Manual FORCE_LEADER command for 2-node disaster recovery
- [ ] Membership change via Raft (add/remove nodes without restart)
