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
| `HALog` | Structured HA logging (BASIC/DETAILED/TRACE) |
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

These features exist on both sides with similar implementations:

| Feature | Notes |
|---------|-------|
| Group Committer | Batched Raft writes via pipelined `async().send()`, configurable batch size |
| LZ4 WAL Compression | WAL data in log entries compressed via `CompressionFactory` |
| Snapshot Install | `notifyInstallSnapshotFromLeader()` + HTTP-based ZIP download |
| HALog | 3 verbosity levels (BASIC/DETAILED/TRACE), controlled by `HA_LOG_VERBOSE` |
| Quorum Enum | MAJORITY and ALL modes, ALL enforced via Ratis Watch API |
| SimpleStateMachineStorage | Replaces hand-rolled last-applied tracking |
| Election Metrics | `electionCount`, `lastElectionTime`, exposed via cluster status |
| Cluster Token | Deterministic derivation from cluster name + root password |
| Leader Lease | `LINEARIZABLE` reads enabled with 0.9 timeout ratio |
| Ratis Config Tuning | 64MB segments, 8MB write buffer, 4MB/256-element append batching |

---

## 4. Key Implementation Differences

### 4.1 Group Committer - ALL Quorum Bug Fix

`apache-ratis` fixed a subtle bug in `flushBatch()`: with `Quorum.ALL`, the original code would `complete(null)` (success) on the future BEFORE checking the ALL watch, meaning the caller could see success even if ALL quorum was not reached. The fix moves `complete(null)` AFTER the watch succeeds, with `continue` on failure branches.

**Status on ha-redesign:** Has the original (buggy) ordering. **Should be ported.**

### 4.2 Cluster Token Derivation

`apache-ratis` upgraded from `UUID.nameUUIDFromBytes()` (MD5-based) to **PBKDF2WithHmacSHA256** with 100,000 iterations for cluster token derivation. This resists brute-force attacks if the token is captured on the wire.

**Status on ha-redesign:** Still uses UUID/MD5. **Should be ported.**

### 4.3 HALog Cached Level

`apache-ratis` added a `cachedLevel` volatile field to avoid calling `GlobalConfiguration.getValueAsInteger()` on every log check. Includes a `refreshLevel()` method called at service startup. Removes the `System.out.println` that was in the original.

**Status on ha-redesign:** Reads config on every call (negligible perf impact, but easy win). Never had the `System.out.println`. **Nice to have.**

### 4.4 Wait-for-Apply Notification (applyNotifier)

`apache-ratis` replaced polling loops (`Thread.sleep(10)` in `waitForAppliedIndex` and `waitForLocalApply`) with a proper `Object` monitor (`applyNotifier`). The state machine calls `raftHAServer.notifyApplied()` after each apply, waking up blocked readers. This eliminates the 10ms polling overhead and makes READ_YOUR_WRITES consistency much more responsive.

**Status on ha-redesign:** Does not have `waitForAppliedIndex` / `waitForLocalApply` methods (different forwarding approach). **Not directly applicable** but worth noting if read-after-write consistency is added.

### 4.5 Snapshot Auth: Cluster Token Header

`apache-ratis` switched snapshot authentication from Basic auth to a dedicated `X-ArcadeDB-Cluster-Token` header with constant-time comparison (`MessageDigest.isEqual`). The `SnapshotHttpHandler` accepts both the token header and Basic auth as fallback. The state machine sends the token header instead of Basic auth credentials.

**Status on ha-redesign:** Uses Basic auth with cluster token as password. **Should be ported** - the token header approach is more secure (avoids sending root password, uses timing-safe comparison).

### 4.6 Zip Slip Protection

`apache-ratis` improved zip-slip protection in snapshot install from `getCanonicalPath().startsWith()` to `toPath().normalize().toAbsolutePath().startsWith()`. The NIO path normalization is more reliable across OS edge cases.

**Status on ha-redesign:** Uses `getCanonicalPath()`. **Should be ported** (minor but safer).

### 4.7 Configurable Election Timeouts

`apache-ratis` added `HA_ELECTION_TIMEOUT_MIN` and `HA_ELECTION_TIMEOUT_MAX` config entries, allowing operators to tune election timeouts for WAN clusters. Previously hardcoded to 1500/3000ms.

**Status on ha-redesign:** Hardcoded to 2000/5000ms. **Should be ported** - makes WAN deployments tunable.

### 4.8 Configurable Log Segment and Buffer Sizes

`apache-ratis` added `HA_LOG_SEGMENT_SIZE` and `HA_APPEND_BUFFER_SIZE` config entries for runtime tuning.

**Status on ha-redesign:** Hardcoded to 64MB / 4MB. **Nice to have.**

### 4.9 Command Forwarding Retry with NotLeaderException

`apache-ratis` improved the command forwarding retry logic in `sendCommand()` to use proper `hasCause()` chain walking instead of string matching on the exception message. Also uses a typed `CommandExecutionException` import.

**Status on ha-redesign:** Uses HTTP forwarding (different approach). **Not applicable.**

### 4.10 HTTP Handler Improvements

`apache-ratis` improved `AbstractServerHttpHandler`:
- Removed unnecessary nested braces around proxy-to-leader try block
- Uses constant-time `MessageDigest.isEqual` for cluster token validation instead of `String.equals`
- Fixed session token forwarding to use `HttpAuthSession` lookup instead of parsing stored Basic auth

**Status on ha-redesign:** Different handler structure. The timing-safe token comparison is relevant to any cluster token validation code. **Security fix worth reviewing.**

### 4.11 WAL File Deletion Logging

`apache-ratis` added a warning log when WAL file deletion fails during snapshot install instead of silently ignoring.

**Status on ha-redesign:** Silently ignores. **Should be ported** (one-liner).

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
| `TRANSACTION_FORWARD` entry type | Raft-native write forwarding with index key changes for constraint validation |
| Command forwarding via `query()` | Forwarded commands execute on leader's state machine (noted as having page visibility issue, currently unused in favor of HTTP proxy) |
| K8s auto-join | `tryAutoJoinCluster()` via Ratis AdminApi for StatefulSet scale-up |
| Dynamic membership | `addPeer()`, `removePeer()`, `transferLeadership()` |
| PBKDF2 cluster token | 100K-iteration key derivation instead of UUID/MD5 |
| applyNotifier | Wait-for-apply without polling for READ_YOUR_WRITES consistency |
| Configurable election timeouts | `HA_ELECTION_TIMEOUT_MIN/MAX` for WAN clusters |
| BOLT + TLS support | `BOLT_SSL` config (DISABLED/OPTIONAL/REQUIRED) |
| Cluster token auth header | `X-ArcadeDB-Cluster-Token` with timing-safe comparison |
| Timing-safe token validation | `MessageDigest.isEqual` instead of `String.equals` |

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
| Commits ahead of main | 86 | 21 |
| Files changed | ~111 (+25,299 / -380) | ~94 (+8,677 / -6,711) |

---

## 8. Recommended Ports to ha-redesign

Priority items from the recent `apache-ratis` updates:

### Must Port (correctness / security)

| Item | Effort | Reason |
|------|--------|--------|
| Group committer ALL quorum fix | Small | Bug: success reported before ALL watch completes |
| PBKDF2 cluster token derivation | Small | Security: MD5-based UUID is weak if token is captured |
| Timing-safe token comparison | Small | Security: prevents timing attacks on cluster token |
| Snapshot auth via cluster token header | Medium | Security: avoids sending root password in Basic auth |
| Zip-slip NIO path normalization | Trivial | Defense-in-depth: more reliable path validation |
| WAL file deletion logging | Trivial | Ops: know when cleanup fails |

### Should Port (operational)

| Item | Effort | Reason |
|------|--------|--------|
| Configurable election timeouts | Small | Needed for WAN cluster deployments |
| Configurable log segment / buffer sizes | Small | Runtime tuning for large clusters |
| HALog cached level | Trivial | Avoids repeated config reads on hot path |

### Future Consideration

| Item | Effort | Reason |
|------|--------|--------|
| Dynamic membership API | Medium | Needed for elastic clusters |
| K8s auto-join | Medium | Needed for Kubernetes deployments |
| applyNotifier pattern | Medium | Better READ_YOUR_WRITES consistency if that feature is added |
| TRANSACTION_FORWARD | Large | More efficient follower writes (noted as having page visibility issues) |

---

## 9. Summary

After the recent feature port, both branches share the same core HA capabilities (group commit, compression, snapshot install, HALog, Quorum). The branches now differ primarily in:

1. **Architecture**: `ha-redesign` is modular and plugin-based; `apache-ratis` is embedded
2. **Testing**: `ha-redesign` has 7x more test coverage including chaos engineering
3. **Security hardening**: `apache-ratis` has PBKDF2 tokens and timing-safe comparisons
4. **Operational config**: `apache-ratis` has configurable election timeouts and buffer sizes
5. **Advanced features**: `apache-ratis` has dynamic membership, K8s auto-join, and the (currently unused) TRANSACTION_FORWARD mechanism

`ha-redesign` is the production-ready choice. The "Must Port" items above should be addressed before merging to `main`.
