# Issue #4754 — HA: follower never rejoins after snapshot install

## Root Cause

`StateMachineUpdater.reload()` (Ratis 3.2.2, line 230) asserts:

```java
Preconditions.assertTrue(stateMachine.getLifeCycleState() == LifeCycle.State.PAUSED);
```

This throws `IllegalStateException` because `ArcadeStateMachine` never transitions its
`LifeCycle` state — `BaseStateMachine.pause()` is a no-op, and `ArcadeStateMachine` neither
calls `getLifeCycle().transition(STARTING)` in `initialize()` nor overrides `pause()` to
transition to `PAUSED`. So `getLifeCycleState()` always returned `NEW`.

After #4749 fixed the earlier crash in `notifyInstallSnapshotFromLeader`, the install now
completes successfully and returns. Ratis then calls `pause()` (no-op, lifecycle stays `NEW`)
and `state.reloadStateMachine()`, which signals the `StateMachineUpdater` to enter `RELOAD`
mode. The updater calls `reload()`, which checks `getLifeCycleState() == PAUSED` — but the
lifecycle is still `NEW` — and throws `IllegalStateException`. The `StateMachineUpdater` thread
dies, the Raft division closes, and the follower permanently rejects `AppendEntries` as
`ServerNotReadyException: current state is CLOSED`.

Additionally, after the install `notifyInstallSnapshotFromLeader` returned `firstTermIndexInLog`
(the first log entry AFTER the snapshot) instead of the snapshot's own TermIndex, and it never
updated `SimpleStateMachineStorage` with the installed snapshot metadata. So after `reinitialize()`
was called by `reload()`, `getLatestSnapshot()` would return null and cause a `NullPointerException`.

## Changes

### `ArcadeStateMachine.java`

1. **`initialize()`**: transition lifecycle `NEW → STARTING → RUNNING` after `super.initialize()`.

2. **`pause()` (new override)**: transition lifecycle `RUNNING → PAUSING → PAUSED` with
   a guard so it is idempotent if called in an unexpected state.

3. **`reinitialize()`**: after restoring `lastAppliedIndex` from storage, if lifecycle is
   `PAUSED` transition `PAUSED → STARTING → RUNNING` so that after `reload()` calls
   `reinitialize()`, the node is back to `RUNNING`.

4. **`notifyInstallSnapshotFromLeader()`**:
   - Compute the correct snapshot `TermIndex` as `(term, firstTermIndexInLog.getIndex()-1)`.
   - Write an empty marker file in `SimpleStateMachineStorage`'s snapshot directory and call
     `storage.updateLatestSnapshot(...)` so `getLatestSnapshot()` is non-null after
     `reinitialize()`.
   - Update `lastAppliedIndex`, `updateLastAppliedTermIndex`, `writePersistedAppliedIndex`.
   - Return the correct `TermIndex` (not `firstTermIndexInLog`).

### New test: `ArcadeStateMachineLifecycleTest.java`

Unit test for the lifecycle contract that `StateMachineUpdater.reload()` relies on.

### Updated test: `RaftFullSnapshotResyncIT.java`

Now enabled - tests the full 3-node path: snapshot install completes → follower rejoins quorum.

## Test Results

| Test | Outcome |
|------|---------|
| `ArcadeStateMachineLifecycleTest` (3 tests) | PASS |
| `ArcadeStateMachineTest` (12 tests) | PASS |
| `ArcadeStateMachineApplyRetryTest` (12 tests) | PASS |
| Full ha-raft unit suite (151 tests) | PASS |

## Impact

Followers now correctly rejoin the Raft quorum after a full snapshot resync. No behavior
change for the normal (non-snapshot) replication path.
