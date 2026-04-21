# Port SnapshotInstaller Crash-Safety, Javadoc, and Test Gaps from apache-ratis

**Date**: 2026-04-14
**Status**: Draft
**Source branch**: `apache-ratis`
**Target branch**: `ha-redesign`

## Overview

Port three categories of improvements from the `apache-ratis` branch into `ha-redesign`:

1. **SnapshotInstaller crash-safety patterns** - atomic directory swap with marker files, startup recovery, retry with exponential backoff
2. **Javadoc enrichment** - design rationale and safety invariant documentation for core HA classes
3. **Test gap closure** - port tests covering genuinely missing scenarios, add new tests for new code

## Approach

Selective port with architectural integration (Approach C from brainstorming). Port the core crash-safety algorithm from apache-ratis but integrate it with ha-redesign's existing infrastructure: `SnapshotHttpHandler` for serving, `SnapshotManager` for checksum utilities, `GlobalConfiguration` for retry parameters. Javadoc written fresh against ha-redesign's actual code. Tests ported where they cover new code; new tests added where ha-redesign's architecture differs.

---

## Section 1: SnapshotInstaller - Crash-Safe Snapshot Installation

### Problem

The current `ArcadeStateMachine.installDatabaseSnapshot()` (line 231) extracts ZIP entries directly into the live database directory. If the process crashes mid-extraction, the database is left in a corrupted state with partial files and no recovery path.

### Solution

New class `SnapshotInstaller` in `com.arcadedb.server.ha.raft` implementing a three-phase atomic swap with marker files.

### Phase 1: Download

1. Create a temporary directory `<dbDir>/.snapshot-new` as a sibling inside the database directory.
2. Write a `.snapshot-pending` marker file into the database directory (`<dbDir>/.snapshot-pending`) before starting extraction. This signals that a swap is in progress.
3. Extract the snapshot ZIP into `.snapshot-new`. Existing protections carry over:
   - Zip-slip validation (path normalization check)
   - Symlink rejection (consistent with `SnapshotHttpHandler.addFileToZip()`)
4. After all entries are extracted successfully, write a `.snapshot-complete` marker file inside `.snapshot-new`. This signals the download finished intact.

### Phase 2: Atomic Swap

1. Rename the live database directory to `<dbDir>/.snapshot-backup`.
2. Rename `.snapshot-new` to the live database path.
3. Both renames are same-filesystem operations (atomic on POSIX, best-effort on Windows).
4. If the second rename fails, rename `.snapshot-backup` back to the live path to restore the original database. Throw an exception to signal the failure.

### Phase 3: Cleanup

1. Delete the `.snapshot-backup` directory recursively.
2. Delete the `.snapshot-pending` marker file.
3. Delete stale `.wal` files from the newly installed database (carried over from current code).
4. Re-open the database via `server.getDatabase(databaseName)` so it becomes visible to clients.

### Recovery on Startup

`SnapshotInstaller.recoverPendingSnapshotSwaps(Path databasesDir)` is called during `ArcadeStateMachine.initialize()`. It iterates over each subdirectory under the configured databases directory (e.g., `databases/mydb/`, `databases/otherdb/`) and checks each one for a `.snapshot-pending` marker file. Recovery logic per database directory:

| `.snapshot-pending` exists | `.snapshot-complete` in `.snapshot-new` | `.snapshot-backup` exists | Action |
|---|---|---|---|
| yes | yes | yes | Complete the swap (Phase 2 + 3) |
| yes | yes | no | Swap already completed but cleanup didn't finish; delete `.snapshot-new`, remove marker |
| yes | no | yes | Download was interrupted; delete `.snapshot-new`, rename `.snapshot-backup` to live, remove marker |
| yes | no | no | Download was interrupted before backup; delete `.snapshot-new`, remove marker |
| no | - | - | No pending swap, no-op |

### Retry with Exponential Backoff

`SnapshotInstaller.downloadWithRetry()` wraps the HTTP snapshot download. On transient HTTP failure (connection timeout, non-200 response):

- Retry up to `HA_SNAPSHOT_INSTALL_RETRIES` times (default: 3)
- Delay between attempts: `HA_SNAPSHOT_INSTALL_RETRY_BASE_MS * 2^attempt` (default base: 5000ms, so delays of 5s, 10s, 20s)
- Clean up partial `.snapshot-new` directory before each retry
- After exhausting retries, throw `IOException` with the last failure as cause

### New GlobalConfiguration Entries

| Key | Type | Default | Description |
|---|---|---|---|
| `arcadedb.ha.snapshotInstallRetries` | int | 3 | Maximum retry attempts for snapshot download |
| `arcadedb.ha.snapshotInstallRetryBaseMs` | long | 5000 | Base delay in milliseconds for exponential backoff between retries |

### Integration Points

- `ArcadeStateMachine.notifyInstallSnapshotFromLeader()` delegates to `SnapshotInstaller.install()` instead of calling `installDatabaseSnapshot()` directly
- `ArcadeStateMachine.initialize()` calls `SnapshotInstaller.recoverPendingSnapshotSwaps()` after `reinitialize()`
- The `forceSnapshot` path in `applyInstallDatabaseEntry()` also delegates to `SnapshotInstaller.install()` for consistency
- `installDatabaseSnapshot()` method is removed from `ArcadeStateMachine` (all logic moves to `SnapshotInstaller`)
- `SnapshotHttpHandler` remains unchanged - the serving side is already solid with concurrency throttling, symlink rejection, and flush suspension

### Class API

```java
public final class SnapshotInstaller {

  /**
   * Installs a database snapshot from the leader using crash-safe atomic swap.
   * Downloads the snapshot ZIP, extracts to a temp directory, and atomically
   * swaps it into the live database path.
   *
   * @param databaseName   name of the database to install
   * @param databasePath   absolute path to the live database directory
   * @param leaderHttpAddr leader's HTTP address (host:port)
   * @param clusterToken   cluster authentication token (may be null)
   * @param server         the ArcadeDB server instance for re-registering the database
   */
  public static void install(String databaseName, String databasePath,
      String leaderHttpAddr, String clusterToken, ArcadeDBServer server)
      throws IOException;

  /**
   * Scans all database directories for pending snapshot swaps and completes
   * or rolls back each one. Called during state machine initialization.
   *
   * @param databasesDir the parent directory containing all database subdirectories
   */
  public static void recoverPendingSnapshotSwaps(Path databasesDir);
}
```

---

## Section 2: Javadoc Enrichment

Javadoc is written fresh against ha-redesign's actual code. The apache-ratis branch Javadoc serves as a reference for what design decisions are worth documenting, but the content reflects ha-redesign's architecture and behavior.

### ArcadeStateMachine

**Class-level Javadoc:**
- Role as the bridge between the Raft log and ArcadeDB storage
- Entry types: `TX_ENTRY` (WAL page diffs), `SCHEMA_ENTRY` (DDL with file creation/removal and schema JSON), `INSTALL_DATABASE_ENTRY` (create or restore), `DROP_DATABASE_ENTRY`, `SECURITY_USERS_ENTRY`
- Threading model: `applyTransaction()` is called sequentially by Ratis on a single thread per group
- Idempotency: all apply methods are safe for replay after crash (file-existence guards, page-version guards)

**`applyTxEntry()` Javadoc:**
- Origin-skip optimization: on the leader, the transaction was already applied via `commit2ndPhase()` in `RaftReplicatedDatabase`, so the state machine skips it. On replicas, this is the primary application path.
- Ordering guarantee: WAL capture (Phase 1) happens before Raft replication; local apply (Phase 2) happens after Raft commit. The leader skips the state machine apply because Phase 2 already wrote the pages.
- `ignoreErrors=true` rationale: during Raft log replay on restart, entries may already be applied to database files. Page-version guards in `applyChanges()` detect and skip already-applied pages.

**`applySchemaEntry()` Javadoc:**
- Three-phase application order and why it matters:
  1. Create physical files (WAL pages reference file IDs that must already exist)
  2. Apply buffered WAL entries (index page writes that happened during DDL)
  3. Update schema JSON and reload
- Idempotency on replay: file-existence guard for Phase 1, page-version guard for Phase 2, schema reload is naturally idempotent

**`notifyInstallSnapshotFromLeader()` Javadoc:**
- When Ratis triggers this: the follower's log is too far behind the leader's compacted log, so individual log entries are no longer available
- Delegates to `SnapshotInstaller.install()` for crash-safe installation
- Runs asynchronously via `CompletableFuture.supplyAsync()` to avoid blocking the Ratis state machine thread

**`reinitialize()` Javadoc:**
- Restores `lastAppliedIndex` from Ratis `SimpleStateMachineStorage` snapshot metadata
- Called during `initialize()` and can be called again if state machine storage is reset

### RaftHAServer

**Class-level Javadoc:**
- Manages the lifecycle of Ratis `RaftServer`, `RaftClient`, and `RaftGroupCommitter`
- Owns peer configuration (parsed from `HA_SERVER_LIST`), quorum policy, and leadership state
- Provides the `HealthMonitor.HealthTarget` interface for automatic recovery from stuck Ratis states
- Thread-safety: `recoveryLock` synchronizes recovery attempts; `shutdownRequested` volatile flag prevents recovery during shutdown

**`refreshRaftClient()` Javadoc:**
- Why gRPC channels need recreation after partition heal: channels in `TRANSIENT_FAILURE` state use exponential backoff up to ~120s. Recreating the client on leader change ensures immediate connectivity.
- Passes the newly elected leader's peer ID so the fresh client routes its first write directly to the leader rather than probing peers.

**`restartRatisIfNeeded()` Javadoc:**
- Triggered by `HealthMonitor` when Ratis enters CLOSED or CLOSING state (typically after network partition)
- Reuses the existing `ArcadeStateMachine` instance because database state is persisted on disk
- Recovery lock prevents concurrent restart attempts
- State machine's `lastAppliedIndex` is preserved across restart; Ratis replays from that point

**Read consistency methods (`ensureLinearizableRead()`, `waitForLocalApply()`) Javadoc:**
- Reference to Raft paper Section 6.4 (read-only operations)
- `ensureLinearizableRead()`: uses Ratis read index protocol. Fast path validates leader lease (no network RTT). Slow path sends heartbeat to majority.
- `waitForLocalApply()`: blocks until `lastAppliedIndex >= targetIndex`. Used for READ_YOUR_WRITES consistency where the client provides a commit index bookmark.

**K8s security note:**
- When `HA_K8S` is enabled and gRPC is bound to `0.0.0.0`, any pod in the cluster can connect to the Raft port. Authentication relies on Kubernetes NetworkPolicy. Document this as a security consideration.

### RaftReplicatedDatabase

**`commit()` Javadoc:**
- Two-phase transaction flow:
  1. Phase 1 (read lock held): capture WAL bytes and bucket record deltas
  2. Replication (no lock held): submit to Raft via `RaftGroupCommitter`, wait for consensus
  3. Phase 2 (read lock held): apply pages locally via `commit2ndPhase()`
- Why the lock is released during replication: allows concurrent transactions to proceed through Phase 1 while waiting for Raft consensus, improving throughput
- Phase 2 failure handling: if local apply fails after Raft has committed, the entry is already in the log. Other replicas will apply it. The leader logs SEVERE and should step down rather than diverge from the committed log.

---

## Section 3: Test Gap Closure

### Tests Ported from apache-ratis (4 files)

#### 1. SnapshotSwapRecoveryTest (unit test)

Tests the `SnapshotInstaller.recoverPendingSnapshotSwaps()` recovery logic using synthetic filesystem state (no actual HTTP downloads or Raft clusters).

Test scenarios matching the recovery truth table from Section 1:
- **`testRecoveryCompletesInterruptedSwap()`**: `.snapshot-pending` + `.snapshot-complete` + `.snapshot-backup` all present. Verifies recovery completes the swap: live directory contains snapshot data, backup and markers are cleaned up.
- **`testRecoveryRollsBackIncompleteDownload()`**: `.snapshot-pending` + `.snapshot-backup` present but no `.snapshot-complete`. Verifies recovery restores the backup to live and cleans up.
- **`testRecoveryCleansUpOrphanedNewDir()`**: `.snapshot-pending` + `.snapshot-new` present but no backup and no complete marker. Verifies `.snapshot-new` is deleted and marker removed.
- **`testNoMarkerNoAction()`**: Clean database directory with no markers. Verifies no changes.
- **`testMultipleDatabasesRecoveredIndependently()`**: Two databases with different pending states. Verifies each is recovered independently.

#### 2. RaftReplicaFailureIT (integration test)

Port from apache-ratis, adapted to ha-redesign's `BaseRaftHATest` framework:
- **Replica dies during snapshot installation**: Start 3-node cluster, isolate a replica long enough for log compaction, kill it during snapshot download, restart. Verify it recovers via `recoverPendingSnapshotSwaps()` and eventually catches up.
- **Replica restarts with corrupted database files**: Corrupt a data file on a stopped replica, restart. Verify the replica detects the issue and triggers a full snapshot resync.

#### 3. RaftHAServerAddressParsingTest (unit test)

Port from apache-ratis, covering edge cases in `RaftHAServer.parsePeerList()`:
- Malformed entries (empty string, extra colons, non-numeric ports)
- Single-node cluster
- IPv6 addresses
- Duplicate host:port combinations
- Missing/blank hostnames

#### 4. HAConfigDefaultsTest (unit test)

Port from apache-ratis, adapted to include new config entries:
- All `HA_*` entries in `GlobalConfiguration` have non-null defaults
- Default types match declared types
- New `HA_SNAPSHOT_INSTALL_RETRIES` and `HA_SNAPSHOT_INSTALL_RETRY_BASE_MS` have correct defaults (3 and 5000)

### New Tests (2 files)

#### 5. SnapshotInstallerRetryTest (unit test)

Tests the exponential backoff retry logic in isolation (mocked HTTP responses or a local test HTTP server):
- **`testSuccessOnFirstAttempt()`**: HTTP 200 on first try, no retries needed
- **`testSuccessAfterTransientFailure()`**: HTTP 503 then 200. Verifies retry happened and installation succeeded.
- **`testExhaustedRetriesThrows()`**: All attempts return HTTP 500. Verifies `IOException` is thrown after `HA_SNAPSHOT_INSTALL_RETRIES` attempts.
- **`testBackoffTimingIncreases()`**: Verify delay doubles between attempts (5s, 10s, 20s with default config)
- **`testPartialDownloadCleanedUpBeforeRetry()`**: First attempt creates partial `.snapshot-new`, second attempt starts clean

#### 6. SnapshotInstallerIntegrationIT (integration test)

End-to-end test using `BaseRaftHATest` 3-node cluster:
- **`testFollowerInstallsSnapshotViaCrashSafeFlow()`**: Write enough data to trigger log compaction on leader. Add a lagging follower. Verify it receives snapshot via `SnapshotInstaller`, database is usable, marker files are cleaned up.
- **`testSnapshotInstallSurvivesProcessRestart()`**: Same setup but kill the follower mid-installation (using a latch or similar mechanism). Restart and verify recovery completes the installation.

### Tests Not Ported (with rationale)

| apache-ratis test | Reason not ported |
|---|---|
| `RaftHAComprehensiveIT` | ha-redesign covers these scenarios across multiple focused tests |
| `RaftReplicationIT` | Covered by `RaftReplication2NodesIT` and `RaftReplication3NodesIT` |
| `ArcadeDBStateMachineCreateFilesTest` | ha-redesign handles file creation via `SCHEMA_ENTRY`; existing `ArcadeStateMachineTest` covers it |
| `RaftHAPluginTest` | Plugin lifecycle tested via `RaftHAServerTest` |

---

## File Change Summary

### New Files
| File | Type | Description |
|---|---|---|
| `ha-raft/src/main/java/.../SnapshotInstaller.java` | Production | Crash-safe snapshot installation with atomic swap and retry |
| `ha-raft/src/test/java/.../SnapshotSwapRecoveryTest.java` | Unit test | Recovery logic for all marker/backup states |
| `ha-raft/src/test/java/.../SnapshotInstallerRetryTest.java` | Unit test | Exponential backoff retry logic |
| `ha-raft/src/test/java/.../SnapshotInstallerIntegrationIT.java` | Integration test | End-to-end snapshot install with crash-safety |
| `ha-raft/src/test/java/.../RaftReplicaFailureIT.java` | Integration test | Replica failure during snapshot install |
| `ha-raft/src/test/java/.../RaftHAServerAddressParsingTest.java` | Unit test | Peer address parsing edge cases |
| `ha-raft/src/test/java/.../HAConfigDefaultsTest.java` | Unit test | GlobalConfiguration HA defaults validation |

### Modified Files
| File | Change |
|---|---|
| `ArcadeStateMachine.java` | Delegate snapshot install to `SnapshotInstaller`, call `recoverPendingSnapshotSwaps()` in `initialize()`, add Javadoc to class and key methods, remove `installDatabaseSnapshot()` |
| `RaftHAServer.java` | Add Javadoc to class and key methods |
| `RaftReplicatedDatabase.java` | Add Javadoc to `commit()` flow |
| `GlobalConfiguration.java` | Add `HA_SNAPSHOT_INSTALL_RETRIES` and `HA_SNAPSHOT_INSTALL_RETRY_BASE_MS` entries |

### Unchanged Files
| File | Reason |
|---|---|
| `SnapshotHttpHandler.java` | Serving side already has concurrency throttling, symlink rejection, flush suspension |
| `SnapshotManager.java` | Checksum utilities remain building blocks for future incremental sync |

---

## Dependencies and Risks

**No new external dependencies.** All functionality uses JDK standard library (`java.nio.file`, `java.net`, `java.util.zip`).

**Risks:**
- **Windows filesystem atomicity**: `Files.move()` with `ATOMIC_MOVE` is not guaranteed on all Windows filesystems. The swap uses `REPLACE_EXISTING` as fallback but documents this limitation.
- **Disk space**: During the swap, both the old database and new snapshot coexist temporarily, requiring roughly 2x the database size in free disk space. This is inherent to the atomic swap pattern and matches apache-ratis behavior.
- **Race with database access**: The database must be closed before the swap and re-opened after. This is already handled by the current code path (`db.close()` / `server.removeDatabase()` / `server.getDatabase()`).
