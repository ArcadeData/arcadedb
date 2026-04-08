# HA Branch Comparison: `ha-redesign` vs `apache-ratis`

**Date:** 2026-04-06 (updated after second port round)
**Compared against:** `main` branch

Both branches rewrite ArcadeDB's High Availability stack on top of Apache Ratis. They share the same goal but differ in architecture, scope, and maturity.

---

## 1. Module Structure

| | `ha-redesign`                                           | `apache-ratis` |
|---|---------------------------------------------------------|---|
| Location | Separate top-level module `ha-raft/`                    | Inside `server/` module |
| Package | `com.arcadedb.server.ha.raft`                           | `com.arcadedb.server.ha.ratis` |
| Server dep scope | `provided` (plugin-style)                               | `compile` (direct) |
| Ratis version | 3.2.2                                                   | 3.2.1 |
| Activation | `HA_IMPLEMENTATION=raft` toggle, `ServiceLoader` plugin | Wired directly into `ArcadeDBServer` startup |
| Distribution | Shade plugin configured, ready for modular distribution | Bundled with server |

`ha-redesign` isolates the Raft subsystem as a publishable Maven artifact with `provided` scope on the server. `apache-ratis` embeds it directly in the server module.

---

## 2. Source Files

### ha-redesign (14 main classes)

| Class | Purpose |
|-------|---------|
| `RaftReplicatedDatabase` | `DatabaseInternal` wrapper, intercepts `commit()` for Raft consensus |
| `RaftHAServer` | Ratis `RaftServer`/`RaftClient` lifecycle, peer parsing, lag monitor |
| `ArcadeStateMachine` | Ratis state machine with `SimpleStateMachineStorage`, election metrics |
| `RaftLogEntryCodec` | Encode/decode Raft log entries with LZ4 compression |
| `RaftGroupCommitter` | Batched Raft submissions via pipelined async sends |
| `RaftHAPlugin` | `ServerPlugin` for ServiceLoader-based HA discovery |
| `SnapshotHttpHandler` | HTTP handler serving database ZIP snapshots |
| `GetClusterHandler` | HTTP endpoint returning cluster status JSON |
| `SnapshotManager` | CRC32 checksum and file-diff utilities |
| `ClusterMonitor` | Replication lag tracking per replica |
| `HALog` | Structured HA logging (BASIC/DETAILED/TRACE) with cached level |
| `Quorum` | Enum: MAJORITY, ALL |
| `RaftLogEntryType` | Enum: TX_ENTRY, SCHEMA_ENTRY, INSTALL_DATABASE_ENTRY |
| `package-info.java` | Package documentation |

### apache-ratis (7 main classes)

| Class | Purpose |
|-------|---------|
| `ArcadeDBStateMachine` | State machine with schema apply, command forwarding via `query()` |
| `RaftLogEntry` | Integrated entry format + serialization with compression |
| `RaftHAServer` | Server lifecycle, Quorum inner enum, K8s auto-join, dynamic membership |
| `RaftGroupCommitter` | Batched Raft submissions (configurable batch size) |
| `SnapshotHttpHandler` | HTTP handler serving database ZIP snapshots |
| `ClusterMonitor` | Replication lag tracking |
| `HALog` | Structured HA logging with cached level |

---

## 3. Shared Features (Both Branches)

These features exist on both sides with equivalent implementations:

| Feature | Notes |
|---------|-------|
| Group Committer | Batched Raft writes via pipelined `async().send()`, configurable batch size |
| ALL Quorum correctness | Success only reported after ALL watch completes (race condition fixed) |
| LZ4 WAL Compression | WAL data in log entries compressed via `CompressionFactory` |
| Snapshot Install | `notifyInstallSnapshotFromLeader()` + HTTP-based ZIP download |
| Snapshot auth | `X-ArcadeDB-Cluster-Token` header with timing-safe `MessageDigest.isEqual` |
| HALog | 3 verbosity levels (BASIC/DETAILED/TRACE) with cached level, no config read on hot path |
| Quorum Enum | MAJORITY and ALL modes, ALL enforced via Ratis Watch API |
| SimpleStateMachineStorage | Replaces hand-rolled last-applied tracking |
| Election Metrics | `electionCount`, `lastElectionTime`, exposed via cluster status |
| PBKDF2 Cluster Token | 100K-iteration PBKDF2WithHmacSHA256 derivation from cluster name + root password |
| Leader Lease | `LINEARIZABLE` reads enabled with 0.9 timeout ratio |
| Configurable election timeouts | `HA_ELECTION_TIMEOUT_MIN/MAX` for WAN cluster tuning |
| Configurable Ratis tuning | Log segment size, append buffer size, write buffer all configurable |
| NIO zip-slip protection | `Path.normalize().toAbsolutePath().startsWith()` for snapshot extraction |
| WAL deletion logging | Warning logged when stale `.wal` file deletion fails |
| Dynamic membership API | `addPeer()`, `removePeer()`, `transferLeadership()`, `stepDown()`, `leaveCluster()` with REST endpoints |
| K8s auto-join | `tryAutoJoinCluster()` on startup via Ratis AdminApi, `leaveCluster()` on K8s shutdown |
| Read consistency modes | EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE with wait-for-apply notification pattern |
| 3-phase commit | Phase 1 (lock: capture WAL) -> Replication (no lock) -> Phase 2 (lock: apply locally). Leader steps down on Phase 2 failure |

---

## 4. Remaining Implementation Differences

### 4.1 Command Forwarding

| | `ha-redesign` | `apache-ratis` |
|---|---|---|
| Mechanism | HTTP POST to leader via `HttpClient` | Ratis `query()` path (state machine) |
| Auth | Cluster token via HTTP header | Cluster token via HTTP header |
| Constraint validation | Delegated to leader's normal commit path | Explicit index key changes in TRANSACTION_FORWARD |

`apache-ratis` also has a `TRANSACTION_FORWARD` Raft log entry type that forwards writes from replicas with index key changes for constraint validation. However, this is noted as having a page visibility issue and is currently unused in favor of HTTP proxy forwarding.

### 4.2 Log Entry Format

| | `ha-redesign` | `apache-ratis` |
|---|---|---|
| Architecture | Separate `RaftLogEntryCodec` + `RaftLogEntryType` enum | Single `RaftLogEntry` class |
| Entry types | TX_ENTRY, SCHEMA_ENTRY, INSTALL_DATABASE_ENTRY | TRANSACTION, TRANSACTION_FORWARD, COMMAND_FORWARD |
| Serialization | `DataInputStream`/`DataOutputStream` | `Binary` class (ArcadeDB native) |

### 4.3 Wait-for-Apply Notification (applyNotifier)

`apache-ratis` replaced polling loops (`Thread.sleep(10)`) with a proper `Object` monitor. The state machine calls `raftHAServer.notifyApplied()` after each apply, waking up blocked readers. This eliminates polling overhead for READ_YOUR_WRITES consistency.

`ha-redesign` does not have `waitForAppliedIndex` / `waitForLocalApply` methods (different forwarding approach). Worth noting if read-after-write consistency is added.

### 4.4 Ratis Configuration Defaults

| Setting | `ha-redesign` | `apache-ratis` |
|---------|---------------|----------------|
| Election timeout min (default) | 2000ms | 1500ms |
| Election timeout max (default) | 5000ms | 3000ms |
| Snapshot threshold (default) | 10,000 | 100,000 |

`ha-redesign` uses more conservative election timeouts (less likely to trigger false elections under load) and a lower snapshot threshold (more frequent log compaction).

---

## 5. Features Unique to Each Branch

### Only in `ha-redesign`

| Feature | Description |
|---------|-------------|
| Modular plugin architecture | `RaftHAPlugin` via `ServiceLoader`, `HA_IMPLEMENTATION` toggle |
| `GetClusterHandler` | REST endpoint at `/api/v1/cluster` with election metrics, uptime |
| `INSTALL_DATABASE_ENTRY` | Raft log entry type for replicating `createDatabase()` |
| `SnapshotManager` utilities | CRC32 checksums and file-diff helpers for delta sync |
| `HA_RAFT_PERSIST_STORAGE` | Preserves Raft storage across restarts in tests |
| Enhanced Studio cluster UI | Topology visualization, election count, uptime |
| Comprehensive test suite | 40 test files with split-brain, chaos, read consistency, benchmarks |
| E2E chaos tests | 9 Toxiproxy-based ITs in `e2e-ha/` module |

### Only in `apache-ratis`

| Feature | Description |
|---------|-------------|
| `TRANSACTION_FORWARD` entry type | Raft-native write forwarding with index key changes for constraint validation (currently unused due to page visibility issue) |
| Command forwarding via `query()` | Forwarded commands execute on leader's state machine (currently unused in favor of HTTP proxy) |
| BOLT + TLS support | `BOLT_SSL` config (DISABLED/OPTIONAL/REQUIRED) |

---

## 6. Test Coverage

| Category | `ha-redesign` | `apache-ratis` |
|----------|---------------|----------------|
| Unit tests | 13 classes | 3 classes |
| Integration tests | 27 classes | 3 classes |
| Test lines | ~5,800 | ~1,500 (est.) |
| Split-brain | 3-node and 5-node | None |
| Chaos/crash | Random crash, leader/replica recovery | Comprehensive IT only |
| Read consistency | Dedicated IT | None |
| Schema replication | 2 dedicated ITs | Covered in comprehensive IT |
| Snapshot resync | `RaftFullSnapshotResyncIT` | None |
| Benchmark | `RaftHAInsertBenchmark` | `HAInsertBenchmark` |
| E2E (Toxiproxy) | 9 ITs in `e2e-ha/` | Referenced |

---

## 7. Commit Activity

| | `ha-redesign` | `apache-ratis` |
|---|---|---|
| Commits ahead of main | 90 | 21 |
| Files changed | 111 (+23,988 / -380) | ~94 (+8,677 / -6,711) |

---

## 8. Future Consideration

Features from `apache-ratis` that could be added to `ha-redesign` in future iterations:

| Item | Effort | Reason |
|------|--------|--------|
| TRANSACTION_FORWARD | Large | More efficient follower writes (noted as having page visibility issues, currently unused on apache-ratis) |

---

## 9. Summary

After three rounds of porting, `ha-redesign` now includes all production-relevant features from `apache-ratis`:

- **Performance:** Group committer with batched Raft writes, LZ4 WAL compression, configurable Ratis tuning, 3-phase commit (lock released during Raft replication for concurrent write throughput)
- **Correctness:** ALL quorum race fix, snapshot-based resync for lagging replicas, NIO zip-slip protection
- **Security:** PBKDF2 cluster token derivation, timing-safe token comparison, cluster token header auth for snapshots
- **Operability:** HALog with cached verbosity levels, configurable election timeouts, WAL deletion logging, Studio cluster UI
- **Cluster Management:** Dynamic membership API (addPeer/removePeer/transferLeadership/stepDown/leaveCluster with REST endpoints), K8s auto-join discovery, multiple read consistency modes (EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE)

The only remaining `apache-ratis`-exclusive features are an experimental write-forwarding mechanism (`TRANSACTION_FORWARD`) that is currently unused due to a page visibility issue, and BOLT with TLS support.

`ha-redesign` is the production-ready choice: modular architecture, 40-file test suite with chaos engineering, safe rollout via `HA_IMPLEMENTATION` toggle, and now feature-complete with all security, performance, and cluster management features from `apache-ratis`.


## 10. Benchmark Results

ArcadeDB Raft HA Insert Benchmark

Sync:  5,000 records (batch 100/tx)  |  Async: 100,000 records (8 threads)

1 server (no HA) - embedded
-------------------------------------------------------
    Ops:        50 operations (1 thread)
    Throughput: 1,073 ops/sec
    Avg:        932 us  |  Median: 840 us
    Min:        614 us  |  P95:    1,417 us
    P99:        2,764 us  |  Max:    2,764 us

3 servers (Raft HA) - embedded on leader
-------------------------------------------------------
    Ops:        50 operations (1 thread)
    Throughput: 67 ops/sec
    Avg:        15,033 us  |  Median: 15,010 us
    Min:        10,974 us  |  P95:    21,761 us
    P99:        23,951 us  |  Max:    23,951 us

5 servers (Raft HA) - embedded on leader
-------------------------------------------------------
    Ops:        50 operations (1 thread)
    Throughput: 69 ops/sec
    Avg:        14,539 us  |  Median: 14,820 us
    Min:        9,411 us  |  P95:    20,014 us
    P99:        22,106 us  |  Max:    22,106 us

3 servers (Raft HA) - remote via follower proxy
-------------------------------------------------------
    Ops:        5,000 operations (1 thread)
    Throughput: 87 ops/sec
    Avg:        11,555 us  |  Median: 11,430 us
    Min:        3,716 us  |  P95:    15,791 us
    P99:        19,943 us  |  Max:    38,777 us

3 servers (Raft HA) - concurrent (3 threads)
-------------------------------------------------------
    Ops:        4,998 operations (3 threads)
    Throughput: 96 ops/sec
    Avg:        31,321 us  |  Median: 30,516 us
    Min:        11,922 us  |  P95:    42,913 us
    P99:        52,618 us  |  Max:    144,822 us

5 servers (Raft HA) - concurrent (5 threads)
-------------------------------------------------------
    Ops:        5,000 operations (5 threads)
    Throughput: 117 ops/sec
    Avg:        42,356 us  |  Median: 42,212 us
    Min:        10,499 us  |  P95:    57,886 us
    P99:        67,147 us  |  Max:    141,147 us

1 server (no HA) - async
-------------------------------------------------------
    Ops:        100,000 records (8 async threads, commitEvery=5000)
    Throughput: 423,500 inserts/sec
    Elapsed:    0.2 seconds

3 servers (Raft HA) - async on leader
-------------------------------------------------------
    Ops:        100,000 records (8 async threads, commitEvery=5000)
    Throughput: 278,048 inserts/sec
    Elapsed:    0.4 seconds

5 servers (Raft HA) - async on leader
-------------------------------------------------------
    Ops:        100,000 records (8 async threads, commitEvery=5000)
    Throughput: 313,998 inserts/sec
    Elapsed:    0.3 seconds
