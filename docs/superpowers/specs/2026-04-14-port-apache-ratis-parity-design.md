# Port Plan: Bring ha-redesign to Parity with apache-ratis

## Goal

Bring `ha-redesign` to feature parity with `apache-ratis` across correctness, security, and architecture, while preserving ha-redesign's unique strengths: rich entry types (5 types + `forceSnapshot`), `SnapshotManager`, `GroupCommitter` batching, schema-entry separation, and `HA_IMPLEMENTATION` toggle.

## Execution Order

P1 (correctness) -> P3 (architecture) -> P2 (snapshot security) -> P4 (token hardening) -> P5 (forward compat) -> P6 (tests) -> P7 (polish)

Rationale: correctness fixes are safety-critical and land first. Architecture extraction (P3) happens second so that P2 and P4 changes land in their final homes rather than being added then moved.

## Not Porting

- `TRANSACTION_FORWARD` entry type / command-forwarding via Ratis `query()` - unused in apache-ratis due to page-visibility issue. Keep ha-redesign's HTTP-based leader proxy.
- `BOLT_SSL` - not HA-related.
- Class renames - `ArcadeStateMachine` stays (not renamed to `ArcadeDBStateMachine`).
- Apache-ratis's 3-entry type scheme - keep ha-redesign's 5 types + `forceSnapshot` flag.
- `SnapshotManager.java` stays in ha-redesign.

## Conventions for All Ported Code

- No em dashes anywhere.
- No inline fully-qualified names - always import.
- `final` on variables and parameters where possible.
- Single-statement `if` without braces.
- AssertJ assertions in tests (`assertThat(...).isTrue()`).
- No Claude authorship in source.

---

## Priority 1 - Correctness / Divergence Safety

### 1.1 MajorityCommittedAllFailedException + commit() Recovery

**New file:** `MajorityCommittedAllFailedException.java` extending the existing `QuorumNotReachedException`.

**RaftGroupCommitter change:** When Ratis MAJORITY succeeds but the ALL-quorum watch fails, throw `MajorityCommittedAllFailedException` instead of plain `QuorumNotReachedException`. This lets the caller distinguish "committed but not fully replicated" from "not committed at all".

**RaftReplicatedDatabase.commit() change:** Add a dedicated catch for `MajorityCommittedAllFailedException` before the existing `QuorumNotReachedException` catch. The handler calls a new `applyLocallyAfterMajorityCommit(payload)` method that:

- Acquires read lock on the proxied database.
- Calls `payload.tx.commit2ndPhase(payload.phase1)` to write local pages.
- Saves dirty schema if needed.
- On failure: logs with txId, steps down leadership.
- On step-down failure: spawns async `arcadedb-emergency-stop` thread calling `server.stop()`.
- Re-throws the original exception so the caller knows the ALL-quorum failed.

Without this, on ALL-quorum failures the leader's `lastAppliedIndex` advances (via origin-skip in `applyTransaction()`) but pages are never written, causing permanent divergence.

### 1.2 Phase-2 Failure Escalation

Enhance the existing Phase-2 catch block in `RaftReplicatedDatabase.commit()`:

- Distinguish `ConcurrentModificationException` (log as "page version conflict - may indicate a locking bug") from generic exceptions.
- Include txId in all log messages.
- On step-down failure: spawn async `arcadedb-emergency-stop` daemon thread calling `server.stop()` with "Forcing server stop to prevent leader-follower divergence" log. The async thread avoids deadlock since `commit()` holds the database read lock.

### 1.3 ReplicationException + WALVersionGapException Escalation

**New file:** `ReplicationException.java` - `RuntimeException` subclass with message and cause constructors.

**ArcadeStateMachine.applyTxEntry() change:** Replace `ignoreErrors=true` with explicit exception handling:

- Catch `WALVersionGapException`: log as SEVERE with db name and txId, re-throw wrapped in `ReplicationException` to trigger snapshot resync.
- Catch `ConcurrentModificationException`: log as WARNING ("skipping already-applied WAL entry"), continue. This is benign replay after cold restart or snapshot install.

### 1.4 Persisted Applied-Index with Atomic Write

Add to `ArcadeStateMachine`:

- `writePersistedAppliedIndex(long index)` - writes to `applied-index` file via temp file + atomic rename (`StandardCopyOption.ATOMIC_MOVE`).
- `readPersistedAppliedIndex()` - reads back, returns -1 if file missing.
- Call `writePersistedAppliedIndex()` after each successful apply in `applyTransaction()`.
- In `reinitialize()`: compare persisted index against snapshot index. If snapshot is ahead by more than `SNAPSHOT_GAP_TOLERANCE` (10), set `needsSnapshotDownload` flag and spawn 30-second watchdog on lifecycle executor. If no leader change fires within 30 seconds, trigger snapshot download directly.

### 1.5 Emergency server.stop() on Unexpected Apply Errors

In `ArcadeStateMachine.applyTransaction()`, add outer `Throwable` catch after existing exception handling:

- Log as SEVERE: "CRITICAL: Unexpected error applying Raft log entry at index %d. Shutting down to prevent state divergence."
- Spawn async `arcadedb-emergency-stop` daemon thread calling `server.stop()`. The async thread avoids deadlock since `applyTransaction()` holds Ratis internal locks.
- Return `CompletableFuture.failedFuture()`.

This prevents silent state divergence from bugs (NPE, ClassCastException, OOM, etc.) by crashing the node so it recovers via snapshot.

### 1.6 Lifecycle Executor + Hot Resync Detection

**Lifecycle executor:** Add `ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor()` (daemon thread, named `arcadedb-sm-lifecycle`) to `ArcadeStateMachine`. Used for async tasks that cannot run on the Ratis callback thread: snapshot download watchdog (1.4), emergency operations. Shut down in `close()`.

**Hot resync detection:** After updating `lastAppliedIndex` in `applyTransaction()`, track the previous value. If gap > 1 on a follower, set `catchingUp` AtomicBoolean and log at BASIC level. When applied index reaches commit index, clear flag and log "Hot resync complete". Diagnostic only - no behavioral change.

---

## Priority 3 - Architectural Refactor (Extract from RaftHAServer)

Hybrid strategy: copy from apache-ratis for classes with net-new logic, extract from ha-redesign's existing code for purely structural extractions.

### Copy from apache-ratis

| Class | Purpose |
|---|---|
| `ClusterTokenProvider` | PBKDF2 token derivation with char-array zeroing, production warnings, validation |
| `RaftClusterStatusExporter` | Self-contained cluster status/metrics JSON builder |

### Extract from ha-redesign's RaftHAServer

| Class | Purpose | What moves |
|---|---|---|
| `RaftPeerAddressResolver` | Parse `HA_SERVER_LIST`, resolve peer/HTTP addresses | Peer parsing logic + `ParsedPeerList` record |
| `RaftPropertiesBuilder` | Build Ratis `RaftProperties` from `GlobalConfiguration` | All properties setup code |
| `RaftClusterManager` | Membership ops (add/remove peer, leave, transfer leadership) | Add/remove/leave/transfer methods |
| `RaftTransactionBroker` | Transaction routing helper (encode + submit to committer) | `replicateTransaction()` and related helpers |
| `KubernetesAutoJoin` | K8s service-based peer discovery, join/leave | K8s-specific logic currently inline |

### Not changed

- `HealthMonitor` - already extracted (94 lines), working. No change needed.
- `ClusterMonitor` - already extracted (84 lines). No change needed.

### RaftHAServer after extraction

Shrinks from ~1,423 lines to ~500-600 lines. Becomes a coordinator:

- Holds `RaftServer`, `RaftClient`, `RaftGroupCommitter` lifecycle.
- Delegates to extracted classes for specific responsibilities.
- Keeps leadership state, shutdown flag, recovery lock.
- Keeps `isLeader()`, `stepDown()`, `getCommitIndex()`, `notifyApplied()` - the core coordination API.

### Side-effects

- Remove em dashes from `ArcadeStateMachine.java`, `RaftHAPlugin.java`, `RaftHAServer.java`, `SnapshotManager.java`.
- Replace inline fully-qualified names with imports (`javax.crypto.*`, `java.util.HexFormat`, etc.).
- Move `ParsedPeerList` record into `RaftPeerAddressResolver`.
- No behavioral changes in this block. All existing tests must pass unchanged.

---

## Priority 2 - Snapshot-Install Security Hardening

### 2.1 SnapshotInstaller.java (Downloader Side)

Six hardening items, all additive to the existing three-phase install logic:

1. **Real-path symlink-escape check:** After resolving each ZIP entry's target path, call `Path.toRealPath()` on the parent directory and verify it `startsWith` the temp extraction dir. Catches symlink escapes that the existing literal `..` check misses.

2. **File-level symlink rejection:** Before extracting each entry, check `Files.isSymbolicLink()` on the target. Reject with `ReplicationException` if true.

3. **`copyWithLimit` with 10 GB cap:** Replace raw `InputStream.transferTo()` with a bounded copy loop. If any single entry exceeds 10 GB, abort extraction with `ReplicationException`. Defends against zip-bombs.

4. **SSL context handling:** When `NETWORK_USE_SSL=true`, obtain `SSLSocketFactory` from `server.getHttpServer().getSSLContext()` and set it on `HttpsURLConnection` for both the snapshot download and the leader-DB-list call. If the URL is HTTPS but SSL is disabled, fail-fast with `ReplicationException`.

5. **Completion marker:** Write a `SNAPSHOT_COMPLETE_FILE` marker only after all entries are extracted and verified. On crash recovery in `recoverPendingSnapshotSwaps()`, discard any `.snapshot-new` directory that lacks this marker.

6. **Cluster token on DB-list call:** Add `X-ArcadeDB-Cluster-Token` header to the leader database-list HTTP request, not only to the snapshot download request.

### 2.2 SnapshotHttpHandler.java (Server Side)

Five hardening items:

1. **Root-only authorization:** After existing Basic auth check, verify `"root".equals(user.getName())`. Return 403 otherwise.

2. **Database-name validation:** Reject database names containing `/`, `\`, `..`, null bytes, or control characters. Return 400 with "Invalid database name".

3. **Content-Disposition sanitization:** Sanitize the database name in the `Content-Disposition` header to strip header-injection characters.

4. **Write-timeout watchdog:** Start a scheduled task when streaming begins. If the response hasn't completed within a configurable timeout (default from config, fallback 5 minutes), close the output stream and release the semaphore slot.

5. **Plain-HTTP warning:** When `NETWORK_USE_SSL` is false, log a WARNING once per server start: "Serving database snapshots over plain HTTP. Consider enabling SSL for production clusters."

### 2.3 Tests

- `SnapshotSymlinkProtectionTest` - Unit test: craft a ZIP with symlink entries and `../` paths, verify extraction is rejected.
- `SnapshotWriteTimeoutTest` - Unit test: simulate a stalled output stream, verify watchdog releases semaphore.
- `SnapshotSwapRecoveryTest` - Already exists in ha-redesign. Add new test methods (do not modify existing ones) to verify that `.snapshot-new` directories without a `SNAPSHOT_COMPLETE_FILE` marker are discarded during `recoverPendingSnapshotSwaps()`.
- `WALVersionGapSnapshotResyncTest` - Integration test: inject a WAL gap on a follower, verify snapshot resync via `ReplicationException`.

---

## Priority 4 - Cluster Token Hardening

### Wiring ClusterTokenProvider

`ClusterTokenProvider` is extracted in P3. This priority wires it into all call sites and adds validation.

### Call site migration

Replace every direct `configuration.getValueAsString(HA_CLUSTER_TOKEN)` with `ClusterTokenProvider.getToken()`:

- `ArcadeStateMachine` - token for snapshot installer authentication.
- `PostVerifyDatabaseHandler` - token sent in verification requests to peers.
- `RaftHAPlugin` - token passed to HTTP handlers at registration.
- `SnapshotInstaller` - token in `X-ArcadeDB-Cluster-Token` header.

Each call site gets the provider injected via constructor or method parameter.

### Validation and warnings

In `ClusterTokenProvider` initialization:

- **Hard error** if `HA_CLUSTER_NAME` is not set - throw `ConfigurationException`.
- **Hard error** if root password is not set - throw `ConfigurationException`.
- **WARNING log** if cluster name equals the default value ("arcadedb").
- **WARNING log** if token was auto-derived rather than explicitly set.

### Test

`ClusterTokenProviderTest.java`:

- Token derivation produces consistent results for same inputs.
- Different cluster names produce different tokens.
- Missing cluster name throws `ConfigurationException`.
- Missing root password throws `ConfigurationException`.
- `char[]` password material is zeroed after derivation.

---

## Priority 5 - RaftLogEntryType Forward Compatibility

### Problem

`RaftLogEntryType.fromId(byte id)` throws `IllegalArgumentException` for unknown type codes. During rolling upgrades, a newer node's entries kill older nodes.

### Changes

**`RaftLogEntryType.fromId()`:** Return `null` for unknown codes instead of throwing.

**`ArcadeStateMachine.applyTransaction()`:** Before the `switch` on entry type, check for `null`. If null, log at WARNING ("Unknown Raft log entry type id=%d at index %d, skipping - likely from a newer node version"), update `lastAppliedIndex`, and return success. The entry is acknowledged as applied but no database mutation occurs.

**`RaftLogEntryCodec.decode()`:** If the type byte doesn't map to a known `RaftLogEntryType`, return a `DecodedEntry` with `null` type and skip parsing the rest of the payload.

### What stays

- All 5 existing entry types keep their current byte codes.
- The `forceSnapshot` flag is unaffected.
- No new entry types added.

---

## Priority 6 - Correctness-Level Tests

Tests only, no production code changes. All under `ha-raft/src/test/java/com/arcadedb/server/ha/raft/`.

### Test support class

- **`RaftClusterStarter.java`** - Reusable helper for spinning up multi-node Raft clusters. Port from apache-ratis, adapt to ha-redesign's `BaseRaftHATest`/`BaseMiniRaftTest` conventions.

### Unit tests

- **`PostVerifyDatabaseHandlerTest.java`** - Verify-database endpoint: checksum computation, mismatch detection, error responses.
- **`RaftHAServerValidatePeerAddressTest.java`** - Peer address parsing edge cases. After P3, targets `RaftPeerAddressResolver`.
- **`ArcadeStateMachineCreateFilesTest.java`** - Schema file creation/removal during replication. Adapted from apache-ratis's `ArcadeDBStateMachineCreateFilesTest`.

### Integration tests

- **`OriginNodeSkipIT.java`** - Leader skips applying its own entries (origin-skip optimization).
- **`RaftGraphIngestionStabilityIT.java`** - High-throughput concurrent graph writes, verify no data loss.
- **`RaftHAComprehensiveIT.java`** - Multi-scenario: leader election, failover, data consistency, recovery.
- **`RaftHAServerIT.java`** - RaftHAServer lifecycle: startup, shutdown, restart, peer management.
- **`RaftReplicationIT.java`** - Core replication across document/graph/key-value models.

### Porting approach

- Copy from apache-ratis as starting point.
- Adapt class names (`ArcadeDBStateMachine` -> `ArcadeStateMachine`, `ReplicatedDatabase` -> `RaftReplicatedDatabase`).
- Adapt to ha-redesign's entry types and codec.
- Use ha-redesign's test base classes.
- Verify each test passes individually before moving to the next.

---

## Priority 7 - Optional / Nice-to-Have

### 7.1 RaftHTTP2ServersIT parity tweaks

Port resilience improvements (longer timeouts, retry logic for transient leader election races) from apache-ratis into ha-redesign's existing `RaftHTTP2ServersIT` and `RaftHTTP2ServersCreateReplicatedDatabaseIT`.

### 7.2 HAInsertBenchmark cross-check

Port apache-ratis's `HAInsertBenchmark.java` as a second benchmark alongside ha-redesign's `RaftHAInsertBenchmark.java`.

### 7.3 ForkJoinPool isolation documentation

Add a code comment in `ArcadeStateMachine` documenting whether the current scheduling of `notifyInstallSnapshotFromLeader` is safe and under what conditions it could deadlock, referencing apache-ratis's ForkJoinPool isolation approach.

---

## Summary Matrix

| # | Area | Files added | Files modified | Tests added |
|---|---|---|---|---|
| 1 | Commit-path safety | `MajorityCommittedAllFailedException`, `ReplicationException` | `RaftReplicatedDatabase`, `RaftGroupCommitter`, `ArcadeStateMachine` | `WALVersionGapSnapshotResyncTest` |
| 3 | Architecture | `ClusterTokenProvider`, `RaftClusterStatusExporter`, `RaftPeerAddressResolver`, `RaftPropertiesBuilder`, `RaftClusterManager`, `RaftTransactionBroker`, `KubernetesAutoJoin` | `RaftHAServer` (shrinks), `RaftHAPlugin`, em-dash cleanup in 4 files | - |
| 2 | Snapshot security | - | `SnapshotInstaller`, `SnapshotHttpHandler` | 4 snapshot tests |
| 4 | Cluster token | - | `ArcadeStateMachine`, `PostVerifyDatabaseHandler`, `RaftHAPlugin`, `SnapshotInstaller` | `ClusterTokenProviderTest` |
| 5 | Forward compat | - | `RaftLogEntryType`, `ArcadeStateMachine`, `RaftLogEntryCodec` | - |
| 6 | Edge-case coverage | `RaftClusterStarter` (test utility) | - | 8 tests |
| 7 | Polish | - | various | optional benchmark |

After priorities 1-5 land, ha-redesign is at or above apache-ratis across correctness, security, and architecture, while keeping its own unique strengths.
