# Issue #4740 - HA: leader phase-2 commit failure → follower-ahead divergence, non-converging snapshot resync, node self-halt

## Problem

Under concurrent write load on a 3-node Raft HA cluster, two bugs compound to take a node fully offline:

1. **Leader phase-2 commit failure leaves pages behind**: When the leader's local `commit2ndPhase` throws after Raft has already committed the entry (followers applied it), the leader steps down but its pages stay at version N. When it becomes a follower and replays the log, it hits `WALVersionGapException` (version N+1 expected, page at N) on every subsequent entry.

2. **Diverged-state unexpected errors trigger fatal halt**: After WAL gaps leave the state inconsistent, subsequent `applyTransaction` calls throw unexpected exceptions (NPE, ClassCastException, etc.) from operating on corrupted in-memory state. These reach the `catch (Throwable)` branch in `applyTransaction`, set `haltedAfterCriticalError`, and shut the server down. The node cannot auto-recover — manual data-dir wipe required.

## Root Cause

**Fix 1 - Leader self-reconciliation** (`RaftReplicatedDatabase`): After `commit2ndPhase` fails, call `applyChanges` with the existing WAL payload to bring leader pages from version N to N+1 before stepping down. Page-version guards make this idempotent — already-applied pages are skipped, un-applied ones are written. When the ex-leader replays as a follower, there is no version gap.

**Fix 2 - Diverged-state safety** (`ArcadeStateMachine`): Add `stateDivergedSinceWalGap` flag. When a `WALVersionGapException` is detected (state known-diverged), wrap unexpected `Throwable`s in `applyWithRetry` as `ReplicationException` (recoverable resync) instead of letting them reach the fatal halt path. Also trigger an immediate snapshot download on first WAL gap detection instead of waiting for the HealthMonitor's periodic check.

## Changes

### `ArcadeStateMachine.java`
- Add `stateDivergedSinceWalGap` AtomicBoolean field
- In `applyTxEntry` WAL gap handler: set flag + schedule immediate `triggerSnapshotDownload`
- In `applyWithRetry`: catch `Throwable` when diverged → wrap as `ReplicationException`
- Reset flag in `triggerSnapshotDownload` and `notifyInstallSnapshotFromLeader` success paths

### `RaftReplicatedDatabase.java`
- In `commit()` phase-2 failure catch: call `applyChanges` to reconcile pages before step-down
- Same in `applyLocallyAfterMajorityCommit` failure path
- Add `TEST_PHASE2_FAILURE_HOOK` for IT test

### Tests
- `ArcadeStateMachineApplyRetryTest`: two new unit tests for diverged-state wrapping

## Test Results

- `ArcadeStateMachineApplyRetryTest`: 8/8 PASS (6 existing + 2 new regression guards)
- All ha-raft unit tests (48 total): 48/48 PASS
- Full project build (`mvn clean install -DskipTests`): SUCCESS

## Files Changed

- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineApplyRetryTest.java`
- `docs/4740-ha-leader-phase2-commit-divergence.md` (this file)
