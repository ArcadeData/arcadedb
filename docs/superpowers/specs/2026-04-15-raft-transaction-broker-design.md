# RaftTransactionBroker Port Design

**Date:** 2026-04-15
**Branch:** ha-redesign
**Source:** apache-ratis branch `RaftTransactionBroker` concept (adapted, not literal port)

## Summary

Port three improvements from the apache-ratis branch's `RaftTransactionBroker` into ha-redesign:

1. **Broker facade** - centralize all Raft entry submission behind a single `RaftTransactionBroker` class
2. **Timeout cancellation** - prevent phantom commits when a client times out before the flusher dispatches the entry
3. **Encapsulation** - `RaftGroupCommitter` becomes package-private, only accessible through the broker

## Motivation

Currently, Raft entry submission is spread across `RaftReplicatedDatabase` (TX_ENTRY, SCHEMA_ENTRY, INSTALL_DATABASE_ENTRY, DROP_DATABASE_ENTRY) and `RaftHAPlugin` (SECURITY_USERS_ENTRY). Each call site manually encodes via `RaftLogEntryCodec`, obtains the `RaftGroupCommitter` from `RaftHAServer`, and calls `submitAndWait()`. This creates:

- **Scattered encoding logic** - five call sites encode entries independently
- **Phantom commit risk** - if a client thread times out while its entry is still in the queue, the flusher can pick it up and send it to Raft after the caller has already thrown `QuorumNotReachedException`. The entry gets committed cluster-wide but the leader never calls `commit2ndPhase()`, leaving the leader's local database diverged.
- **Leaky abstraction** - `RaftGroupCommitter` is public and accessed directly from multiple classes

## Design

### 1. RaftTransactionBroker - Class Structure

**Package:** `com.arcadedb.server.ha.raft`

**Owns:** `RaftGroupCommitter` (creates it in constructor, stops it in `stop()`)

**Public API (typed methods):**

```java
public class RaftTransactionBroker {

  RaftTransactionBroker(RaftClient raftClient, Quorum quorum, long quorumTimeout);
  RaftTransactionBroker(RaftClient raftClient, Quorum quorum, long quorumTimeout, int maxBatchSize);

  // Transaction replication (called from RaftReplicatedDatabase.commit())
  void replicateTransaction(String dbName, byte[] walData,
      Map<Integer,Integer> bucketDeltas, long timeoutMs);

  // Schema changes (called from RaftReplicatedDatabase.recordFileChanges())
  void replicateSchema(String dbName, String schemaJson,
      Map<Integer,String> filesToAdd, Map<Integer,String> filesToRemove,
      List<byte[]> walEntries, List<Map<Integer,Integer>> bucketDeltas, long timeoutMs);

  // Database lifecycle
  void replicateInstallDatabase(String dbName, boolean forceSnapshot, long timeoutMs);
  void replicateDropDatabase(String dbName, long timeoutMs);

  // Security
  void replicateSecurityUsers(String usersJson, long timeoutMs);

  // Lifecycle
  void stop();
}
```

Each method:
1. Validates arguments
2. Encodes via `RaftLogEntryCodec`
3. Submits to `RaftGroupCommitter.submitAndWait()`
4. Handles timeout with cancellation semantics (see below)

### 2. Timeout Cancellation Protocol

**New inner class:** `CancellablePendingEntry` replaces `PendingEntry` inside `RaftGroupCommitter`.

**State machine** (AtomicInteger with three values):

```
PENDING (0) --> DISPATCHED (1)    // flusher wins CAS before sending to Raft
PENDING (0) --> CANCELLED (2)     // client wins CAS after timeout
```

**Flow:**

1. **Broker** calls `submitAndWait()`, which creates `CancellablePendingEntry`, adds to queue, calls `future.get(timeoutMs)`.
2. **On timeout**, `submitAndWait()` attempts CAS `PENDING -> CANCELLED`:
   - **CAS succeeds:** Entry was still in queue, never sent to Raft. Throws `QuorumNotReachedException`.
   - **CAS fails** (state is `DISPATCHED`): Flusher already picked it up. Must wait for the Raft result with a bounded grace period (`quorumTimeout`) since the entry is in-flight. This prevents phantom commits.
3. **Flusher** drains queue, for each entry attempts CAS `PENDING -> DISPATCHED`:
   - **CAS succeeds:** Send to Raft normally.
   - **CAS fails** (state is `CANCELLED`): Skip entry, complete future with `QuorumNotReachedException("cancelled before dispatch")`.

**Correctness property:** Once DISPATCHED, the entry will be sent to Raft and its result reported back to the caller. The caller never gets a timeout error for an entry that was actually committed.

**Grace period:** When the broker's timeout fires but CAS fails (entry already DISPATCHED), `submitAndWait()` waits up to `quorumTimeout` more for the actual Raft result. If that also times out, it throws - but at that point Raft itself timed out, not just the queue wait.

### 3. Wiring Changes

**`RaftHAServer`:**
- `RaftGroupCommitter` field replaced with `RaftTransactionBroker`
- `getGroupCommitter()` replaced with `getTransactionBroker()`
- Constructor creates `RaftTransactionBroker` (which internally creates `RaftGroupCommitter`)
- `stop()` calls `broker.stop()` (which calls `committer.stop()`)

**`RaftReplicatedDatabase`:**
- `commit()` (line 274): `raft.getGroupCommitter().submitAndWait(entry.toByteArray(), ...)` becomes `raft.getTransactionBroker().replicateTransaction(getName(), walData, bucketDeltas, timeout)`
- `recordFileChanges()` (line 1070): becomes `raft.getTransactionBroker().replicateSchema(...)`
- `createInReplicas()` (lines 1135/1149): becomes `raft.getTransactionBroker().replicateInstallDatabase(...)`
- `dropInReplicas()` (line 1165): becomes `raft.getTransactionBroker().replicateDropDatabase(...)`
- No more direct `RaftLogEntryCodec` usage in this class - encoding moves into the broker

**`RaftHAPlugin`:**
- `replicateSecurityUsers()` (line 118): becomes `raftHAServer.getTransactionBroker().replicateSecurityUsers(usersJson, timeout)`
- No more direct `RaftLogEntryCodec` usage here

**`RaftGroupCommitter`:**
- `PendingEntry` replaced with `CancellablePendingEntry` (AtomicInteger state field)
- `submitAndWait()` signature unchanged (broker calls it)
- `flushBatch()` adds CAS `PENDING -> DISPATCHED` check before dispatch
- Class visibility changes from `public` to package-private (only broker accesses it)
- On timeout in `submitAndWait()`: CAS `PENDING -> CANCELLED`, if fails wait grace period

**No changes to:** `RaftLogEntryCodec`, `ArcadeStateMachine`, `RaftLogEntryType`

### 4. Testing Strategy

**Unit tests:**

1. **`RaftTransactionBrokerTest`** - broker in isolation with mock `RaftGroupCommitter`:
   - Each typed method encodes the correct entry type (verify via `RaftLogEntryCodec.decode()`)
   - Timeout propagation to committer

2. **`CancellablePendingEntryTest`** - state machine transitions:
   - PENDING -> CANCELLED: CAS succeeds, entry skipped
   - PENDING -> DISPATCHED: CAS succeeds, entry sent
   - CANCELLED then DISPATCHED attempt: CAS fails, entry skipped
   - DISPATCHED then CANCELLED attempt: CAS fails, caller waits for Raft result

3. **`RaftGroupCommitterCancellationTest`** - flusher respects cancellation:
   - Entry cancelled before flush: flusher skips it, future completes with error
   - Entry dispatched normally: existing behavior unchanged

**Integration tests:**

4. **Existing `RaftReplicationIT` and `RaftUserManagement3NodesIT`** pass unchanged (end-to-end behavior unaffected)

5. **New `RaftTransactionBrokerIT`** - 3-node cluster:
   - Submit transaction through broker, verify committed on all nodes
   - Submit security users entry through broker, verify propagated

No new integration test for cancellation - timing-dependent races are better covered by unit tests.

## Files Changed

| File | Change |
|------|--------|
| `ha-raft/.../RaftTransactionBroker.java` | NEW - broker facade |
| `ha-raft/.../RaftGroupCommitter.java` | MODIFY - CancellablePendingEntry, package-private, CAS in flush |
| `ha-raft/.../RaftHAServer.java` | MODIFY - broker replaces committer field |
| `ha-raft/.../RaftReplicatedDatabase.java` | MODIFY - call broker instead of committer |
| `ha-raft/.../RaftHAPlugin.java` | MODIFY - call broker instead of committer |
| `ha-raft/.../RaftTransactionBrokerTest.java` | NEW - unit test |
| `ha-raft/.../CancellablePendingEntryTest.java` | NEW - unit test |
| `ha-raft/.../RaftGroupCommitterCancellationTest.java` | NEW - unit test |
| `ha-raft/.../RaftTransactionBrokerIT.java` | NEW - integration test |

## Out of Scope

- Changing `RaftLogEntryCodec` or entry types
- Changing `ArcadeStateMachine` apply logic
- Changing the origin-skip mechanism
- Changing quorum handling (MAJORITY/ALL)
- Any new Raft entry types
