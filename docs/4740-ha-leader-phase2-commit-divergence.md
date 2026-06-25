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
- Replace the diverged flag with a per-database `divergedDatabases` set (scoped so a gap in one DB never masks a bug while applying another, healthy DB - review item)
- In `applyTxEntry` WAL gap handler: add DB to the set + schedule immediate `triggerSnapshotDownload`
- In `applyWithRetry` (now takes a `databaseName` arg; old 2-arg form kept as a delegating overload): rethrow JVM `Error` and `ReplicationException` unchanged; wrap other Throwables as `ReplicationException` only when the DB is diverged; bounded-escalation counter so a node that can never resync eventually re-halts loudly
- `clearDivergedState()` resets the set + counter from `triggerSnapshotDownload` and `notifyInstallSnapshotFromLeader` success paths

### `RaftReplicatedDatabase.java`
- `reconcileLeaderPagesAfterPhase2Failure()` replays the committed WAL on the leader before step-down (idempotent via page-version guards)
- Called from both `commit()` and `applyLocallyAfterMajorityCommit` phase-2 failure paths
- Added `TEST_PHASE2_COMMIT_FAULT` hook for the regression IT

### Tests
- `ArcadeStateMachineApplyRetryTest`: 4 new unit tests (diverged-DB wrap, healthy-DB rethrow, JVM Error rethrow, ReplicationException no-double-wrap)
- `Issue4740Phase2ReconcileIT`: 3-node IT injecting a phase-2 fault, asserting no WAL gap and cluster convergence (Fix 1 coverage)

## Test Results

- `ArcadeStateMachineApplyRetryTest`: 10/10 PASS (6 existing + 4 new regression guards)
- All ha-raft unit tests: PASS
- Full project build (`mvn clean install -DskipTests`): SUCCESS

## Files Changed

- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineApplyRetryTest.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue4740Phase2ReconcileIT.java`
- `docs/4740-ha-leader-phase2-commit-divergence.md` (this file)

## Review cycles

### Cycle 1 - HEAD 51c69fff

**Gemini** (3 items, all applied):
1. Rethrow JVM `Error` instead of wrapping as recoverable
2. Pass `ree` not `null` to the logger
3. Add regression test for Error rethrow

**Claude** (6 items):
1. `stateDivergedSinceWalGap` server-global → scoped per-database (applied)
2. Flag sticky if resync never succeeds → bounded escalation counter that re-halts a stuck node (applied)
3. Javadoc note that reconcile runs under the read lock (applied)
4. Fix 1 had no automated test → added `Issue4740Phase2ReconcileIT` (applied)
5. Doc/code mismatch on the test hook name → reconciled; hook is `TEST_PHASE2_COMMIT_FAULT` (applied)
6. Minor test comment about default-retries fallback (applied)

### Cycle 2 - HEAD 654f43d5c

Gemini did not re-review (its cycle-1 items were already resolved). Claude re-reviewed, confirmed cycle-1 items addressed, and raised 6 follow-ups (all applied):
1. Detailed javadoc was attached to the 2-arg overload (referenced a non-existent `databaseName` param) → moved onto the 3-arg method; short doc on the overload
2. `markStateDiverged`/`clearDivergedState` were interleaved in the field block → moved to the private-method group near `triggerSnapshotDownload`
3. Comment said "compareAndSet" for a `Set.add()` → reworded
4. Escalation counter is JVM-wide while the diverged set is per-DB → added a comment noting the global scope is deliberate
5. Best-effort reconcile used `ignoreErrors=false` → switched to `true` so it applies every page it can; gapped pages resync via Fix 2
6. Untested Fix-2 internals → added `boundedEscalationRePropagatesAfterThresholdExceeded` and `clearDivergedStateResetsSetAndCounter` (with `isDatabaseDiverged`/`divergedSwallowedErrorCount` test accessors)

`ArcadeStateMachineApplyRetryTest` now 12/12; ha-raft unit suite 102/102; `Issue4740Phase2ReconcileIT` green; full build SUCCESS.

### Cycle 3 - HEAD b0cd5b21a

Claude re-reviewed and posted no further items (clean). Gemini did not re-review. CI green (build-and-package, claude-review, CodeQL all pass; the Meterian failure is a pre-existing dependency vuln unrelated to this change).

Remaining signal: Codacy reported a persistent high-severity ErrorProne finding (the broad `catch (Throwable)` in `applyWithRetry`). Resolved by narrowing to `catch (RuntimeException)`: a `Runnable` can only throw `RuntimeException` or `Error`, so JVM `Error`s now propagate to the fatal halt path by simply not being caught - identical semantics, no broad catch, and the explicit `instanceof Error` rethrow is no longer needed. All tests still pass (the OOM-rethrow guard now verifies the Error propagates uncaught).

## Final state

`clean-approval` (HEAD `9a9b74239`).

- Claude re-reviewed each push; the final two cycles returned no further items.
- Gemini's only review was cycle 0; all of its items were addressed in cycle 1 and it did not re-review.
- Codacy: `success` (the high-severity ErrorProne finding and the earlier CodeStyle minors are all cleared).
- CI: `build-and-package`, `claude-review`, `CodeQL`, all language analyzers and e2e suites green. The `Meterian client scan` failure is a pre-existing dependency vulnerability on the default branch, unrelated to this change.

PR: https://github.com/ArcadeData/arcadedb/pull/4742

Merge remains the developer's decision; this workflow does not merge.
