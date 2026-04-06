# HA Branch Comparison: `ha-redesign` vs `apache-ratis`

**Date:** 2026-04-06
**Compared against:** `main` branch

Both branches rewrite ArcadeDB's High Availability stack on top of Apache Ratis. They share the same goal but differ in architecture, scope, and maturity.

---

## 1. Module Structure

| | `ha-redesign` | `apache-ratis` |
|---|---|---|
| Location | Separate top-level module `ha-raft/` | Inside `server/` module |
| Package | `com.arcadedb.server.ha.raft` | `com.arcadedb.server.ha.ratis` |
| Server dep scope | `provided` (plugin-style) | `compile` (direct) |
| Ratis version | 3.2.0 | 3.2.1 |
| Activation | `HA_IMPLEMENTATION=raft` toggle, `ServiceLoader` plugin | Wired directly into `ArcadeDBServer` startup |
| Distribution | Shade plugin configured, ready for modular distribution | Bundled with server |

`ha-redesign` isolates the Raft subsystem as a publishable Maven artifact with `provided` scope on the server. `apache-ratis` embeds it directly in the server module.

---

## 2. Source Files

### ha-redesign (14 main classes)

| Class | LOC | Purpose |
|-------|-----|---------|
| `RaftReplicatedDatabase` | 975 | `DatabaseInternal` wrapper, intercepts `commit()` for Raft consensus |
| `RaftHAServer` | ~500 | Ratis `RaftServer`/`RaftClient` lifecycle, peer parsing, lag monitor |
| `ArcadeStateMachine` | ~350 | Ratis state machine with `SimpleStateMachineStorage`, election metrics |
| `RaftLogEntryCodec` | 267 | Encode/decode Raft log entries with LZ4 compression |
| `RaftGroupCommitter` | ~160 | Batched Raft submissions via pipelined async sends |
| `RaftHAPlugin` | 132 | `ServerPlugin` for ServiceLoader-based HA discovery |
| `SnapshotHttpHandler` | ~130 | HTTP handler serving database ZIP snapshots |
| `GetClusterHandler` | 89 | HTTP endpoint returning cluster status JSON |
| `SnapshotManager` | 94 | CRC32 checksum and file-diff utilities |
| `ClusterMonitor` | 84 | Replication lag tracking per replica |
| `HALog` | ~40 | Structured HA logging (BASIC/DETAILED/TRACE) |
| `Quorum` | ~20 | Enum: MAJORITY, ALL |
| `RaftLogEntryType` | ~20 | Enum: TX_ENTRY, SCHEMA_ENTRY, INSTALL_DATABASE_ENTRY |
| `package-info.java` | - | Package documentation |

### apache-ratis (7 main classes)

| Class | LOC | Purpose |
|-------|-----|---------|
| `ArcadeDBStateMachine` | 494 | State machine with schema apply, command forwarding via `query()` |
| `RaftLogEntry` | 409 | Integrated entry format + serialization with compression |
| `RaftHAServer` | ~600 | Server lifecycle, Quorum inner enum, K8s auto-join, dynamic membership |
| `RaftGroupCommitter` | ~150 | Batched Raft submissions |
| `SnapshotHttpHandler` | ~140 | HTTP handler serving database ZIP snapshots |
| `ClusterMonitor` | ~70 | Replication lag tracking |
| `HALog` | ~50 | Structured HA logging |

---

## 3. Key Implementation Differences

### State Machine

| Aspect | `ha-redesign` | `apache-ratis` |
|--------|---------------|----------------|
| Class name | `ArcadeStateMachine` | `ArcadeDBStateMachine` |
| Storage | `SimpleStateMachineStorage` | `SimpleStateMachineStorage` |
| DI style | Setter injection (`setServer()`, `setRaftHAServer()`) | Constructor injection |
| Election metrics | `electionCount`, `lastElectionTime`, `startTime` | Same |
| Snapshot install | `notifyInstallSnapshotFromLeader()` with HTTP download | Same approach |
| Leader skip | Skips WAL apply on leader (already committed locally) | Same |

Both state machines are functionally equivalent. The main difference is dependency injection style.

### Log Entry Format

| Aspect | `ha-redesign` | `apache-ratis` |
|--------|---------------|----------------|
| Architecture | Separate `RaftLogEntryCodec` + `RaftLogEntryType` enum | Single `RaftLogEntry` class |
| Entry types | TX_ENTRY, SCHEMA_ENTRY, INSTALL_DATABASE_ENTRY | TRANSACTION, TRANSACTION_FORWARD |
| Compression | LZ4 via `CompressionFactory` | LZ4 via `CompressionFactory` |
| Serialization | `DataInputStream`/`DataOutputStream` | `Binary` class (ArcadeDB native) |
| Write forwarding | Not in log format (uses HTTP) | TRANSACTION_FORWARD entry type with index key changes |

The biggest format difference is how write-from-replica is handled: `ha-redesign` forwards commands via HTTP to the leader, while `apache-ratis` has a dedicated `TRANSACTION_FORWARD` Raft log entry type that includes index key changes for constraint validation on the leader.

### Group Committer

Both implementations are nearly identical:
- `LinkedBlockingQueue<PendingEntry>` with a daemon flusher thread
- `submitAndWait(byte[], long timeoutMs)` blocks caller on `CompletableFuture`
- Pipelined `raftClient.async().send()` for batched entries
- `Quorum.ALL` support via Ratis Watch API
- Max batch size: 500 (configurable)

Minor difference: `apache-ratis` passes `RaftHAServer` to the constructor; `ha-redesign` passes `RaftClient`, `Quorum`, and `quorumTimeout` directly.

### Command Forwarding

| | `ha-redesign` | `apache-ratis` |
|---|---|---|
| Mechanism | HTTP POST to leader via `HttpClient` | Ratis `query()` path (state machine) |
| Auth | Cluster token via Basic auth | Cluster token via Basic auth |
| Constraint validation | Delegated to leader's normal commit path | Explicit index key changes in TRANSACTION_FORWARD |
| Complexity | Simpler (HTTP) | More integrated (Raft-native) |

### Quorum

Functionally equivalent. `ha-redesign` uses a standalone `Quorum` enum; `apache-ratis` uses an inner enum in `RaftHAServer`. Both support MAJORITY and ALL modes with Watch API enforcement.

### Ratis Configuration

Both configure nearly identical `RaftProperties`:

| Setting | `ha-redesign` | `apache-ratis` |
|---------|---------------|----------------|
| RPC timeout min/max | 2s / 5s | 1.5s / 3s |
| Request timeout | 10s | (default) |
| AppendEntries buffer | 4MB / 256 elements | 4MB / 256 elements |
| Log segment max | 64MB | 64MB |
| Write buffer | 8MB | 8MB |
| Snapshot auto-trigger | Configurable (`HA_RAFT_SNAPSHOT_THRESHOLD`) | 100,000 entries |
| Install snapshot mode | Notification (HTTP-based) | Notification (HTTP-based) |
| Leader lease | Enabled, 0.9 ratio, LINEARIZABLE | Same |

The main difference is `ha-redesign` uses slightly more conservative RPC timeouts (2-5s vs 1.5-3s) to reduce false leader elections under load.

---

## 4. Features Unique to Each Branch

### Only in `ha-redesign`

- **Modular plugin architecture** - `RaftHAPlugin` via `ServiceLoader`, `HA_IMPLEMENTATION` toggle for legacy/raft switching
- **`GetClusterHandler`** - REST endpoint at `/api/v1/cluster` returning cluster status with election metrics
- **`INSTALL_DATABASE_ENTRY`** log type - replicates `createDatabase()` calls across the cluster
- **`SnapshotManager`** utilities - CRC32 checksums and file-diff helpers for future delta sync
- **`HA_RAFT_PERSIST_STORAGE`** config - preserves Raft storage across server restarts in tests
- **Enhanced Studio cluster UI** - topology visualization, election count, uptime display
- **Comprehensive test suite** - 40 test files covering split-brain, chaos, read consistency, benchmarks

### Only in `apache-ratis`

- **`TRANSACTION_FORWARD`** entry type - Raft-native write forwarding from replicas with index key changes for constraint validation
- **Command forwarding via `query()`** - forwarded commands execute on the leader's state machine directly, with binary-serialized results returned to the follower
- **K8s auto-join** - `tryAutoJoinCluster()` via Ratis AdminApi for StatefulSet scale-up
- **Dynamic membership** - `addPeer()`, `removePeer()`, `transferLeadership()` methods
- **Command result serialization** - `serializeCommandResult()` / `deserializeCommandResult()` for forwarded command responses

---

## 5. Test Coverage

| Category | `ha-redesign` | `apache-ratis` |
|----------|---------------|----------------|
| Unit tests | 13 classes | 3 classes |
| Integration tests | 27 classes | 3 classes |
| Lines of test code | ~5,800 | ~1,500 (estimated) |
| Split-brain tests | 3-node and 5-node | None dedicated |
| Chaos/crash tests | Random crash, leader/replica crash-recover | Comprehensive IT only |
| Read consistency | Dedicated `RaftReadConsistencyIT` | None |
| Schema replication | 2 dedicated ITs | Covered in comprehensive IT |
| Index operations | 2 dedicated ITs | None |
| HTTP layer tests | 3 dedicated ITs | None |
| Snapshot resync | `RaftFullSnapshotResyncIT` | None |
| Benchmark | `RaftHAInsertBenchmark` (sync + async) | `HAInsertBenchmark` |
| E2E chaos (Toxiproxy) | 9 ITs in separate `e2e-ha/` module | Referenced but fewer |

---

## 6. Commit Activity

| | `ha-redesign` | `apache-ratis` |
|---|---|---|
| Commits ahead of main | 86 | 13 |
| Files changed | 111 (+25,299 / -380) | 94 (+8,677 / -6,711) |

`ha-redesign` has 6.6x more commits, reflecting iterative development with extensive test porting, chaos engineering, and the recent feature port from `apache-ratis`.

---

## 7. Pros and Cons

### `ha-redesign`

**Pros:**
- Clean modular boundary - HA is a pluggable module with `provided` server dependency
- `HA_IMPLEMENTATION` toggle enables safe rollout alongside legacy HA
- Comprehensive test coverage (40 files) including chaos engineering with Toxiproxy
- `INSTALL_DATABASE_ENTRY` handles `createDatabase()` replication
- Enhanced Studio UI for cluster monitoring
- Well-documented with design specs and implementation plans
- All features from `apache-ratis` now ported (group committer, compression, snapshot install, HALog, Quorum)

**Cons:**
- No Raft-native write forwarding (uses HTTP instead of `TRANSACTION_FORWARD`) - simpler but adds HTTP overhead for follower writes
- No dynamic membership API (`addPeer`/`removePeer`/`transferLeadership`)
- No K8s auto-join via Ratis AdminApi
- No command result serialization (forwarded commands go via HTTP, not Raft)
- Slightly more conservative RPC timeouts may delay leader election

### `apache-ratis`

**Pros:**
- Raft-native write forwarding with constraint validation via `TRANSACTION_FORWARD` - lower overhead for follower writes
- Dynamic membership management (`addPeer`, `removePeer`, `transferLeadership`)
- K8s auto-join via Ratis AdminApi for seamless StatefulSet scaling
- Command forwarding via Ratis `query()` - avoids HTTP layer for forwarded commands
- Fewer classes, more consolidated codebase

**Cons:**
- Tightly coupled to `server/` module - no clean extraction or swap path
- Minimal test coverage (~6 files vs 40)
- No `HA_IMPLEMENTATION` toggle - all-or-nothing replacement of legacy HA
- No `createDatabase()` replication across cluster
- No `SnapshotManager` utilities for future delta sync
- No Studio cluster monitoring UI enhancements
- No dedicated chaos engineering tests

---

## 8. Summary

After the recent feature port, `ha-redesign` now includes all the performance-critical features that were previously `apache-ratis`-only (group committer, LZ4 compression, snapshot install, HALog, Quorum enum). The remaining differences are architectural:

**`ha-redesign` is the production-ready choice:** modular architecture, comprehensive tests, Studio UI, safe rollout path via toggle. It is the clear candidate for merging to `main`.

**`apache-ratis` retains three features worth future consideration:**
1. **`TRANSACTION_FORWARD`** - Raft-native write forwarding avoids HTTP overhead for follower writes
2. **Dynamic membership** - `addPeer`/`removePeer`/`transferLeadership` for elastic clusters
3. **K8s auto-join** - automatic cluster discovery for StatefulSet scaling

These are additive features that could be ported into `ha-redesign` in future iterations without architectural changes.
