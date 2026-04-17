# 3-Phase Commit Port from `apache-ratis` to `ha-redesign`

**Date:** 2026-04-08
**Branch:** ha-redesign
**Scope:** `RaftReplicatedDatabase.commit()` refactor only

## Problem

The current `commit()` in `RaftReplicatedDatabase` holds the database read lock for the entire duration of Raft replication (WAL capture + gRPC round-trip + local apply). Under high concurrent write load, this blocks other transactions from progressing while one transaction waits for Raft quorum acknowledgment.

The `apache-ratis` branch solves this by splitting commit into three phases, releasing the lock during the network round-trip.

## Design

### Data Carrier

A private record carries state between the three phases:

```java
private record ReplicationPayload(
    TransactionContext tx,
    TransactionContext.TransactionPhase1 phase1,
    byte[] walData,
    Map<Integer, Integer> bucketDeltas
) {}
```

### Leader Commit Flow

**Phase 1** (read lock):
- Call `commit1stPhase(true)` to capture WAL bytes and delta
- If phase1 is null (read-only tx), reset and return
- Extract `walData` and `bucketDeltas` into `ReplicationPayload`
- Save schema if dirty
- Release read lock via `executeInReadLock()` return

**Replication** (no lock held):
- Encode entry via `RaftLogEntryCodec.encodeTxEntry(getName(), walData, bucketDeltas)`
- Submit to group committer: `raft.getGroupCommitter().submitAndWait(entry, timeout)`
- On failure: rollback, throw

**Phase 2** (read lock):
- Re-fetch `DatabaseContext` thread-local (may have changed between lock acquisitions)
- Call `commit2ndPhase(phase1)` to apply pages locally
- Save schema if dirty
- On failure:
  1. Log SEVERE with database name and error
  2. Call `raftHAServer.stepDown()` so a follower with correct state becomes leader
  3. Throw the exception (node self-heals on restart via Raft log replay)

### Follower Path

Unchanged. Followers reject writes with `ServerIsNotTheLeaderException` in the `command()` forwarding path. They never reach the commit replication code.

### Schema-Buffering Path

The existing schema-buffering block (triggered when `proxied.getFileManager().getRecordedChanges() != null`) stays as-is. It commits locally without Raft replication because the WAL data gets embedded in the `SCHEMA_ENTRY` that `recordFileChanges()` sends after the callback.

### Error Handling Summary

| Phase | Failure action |
|-------|---------------|
| Phase 1 | rollback, throw (same as today) |
| Replication | rollback, throw (same as today) |
| Phase 2 | log SEVERE, step down, throw (new) |

Phase 2 failures are expected to be extremely rare since it's a local page write. The step-down strategy ensures no stale reads from a leader that missed a committed write.

### Thread-Local Safety

- `DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath())` must be re-fetched in Phase 2 since the thread-local could have been modified by other operations between Phase 1 lock release and Phase 2 lock acquisition.
- `current.popIfNotLastTransaction()` must be called in Phase 1's finally block (after capturing the payload) and again in Phase 2's finally block (after commit2ndPhase). This matches the existing cleanup pattern.
- On read-only transactions (phase1 == null), `tx.reset()` and `popIfNotLastTransaction()` happen in Phase 1 and we return early - no Phase 2 needed.

## Files Changed

| File | Change |
|------|--------|
| `ha-raft/src/main/java/.../RaftReplicatedDatabase.java` | Refactor `commit()` into 3-phase pattern, add `ReplicationPayload` record, add Phase 2 step-down recovery |

No other files are modified.

## Testing

### Verification approach
- All existing HA tests must pass (they exercise the commit path)
- Targeted tests to add:
  1. Unit test confirming that the read lock is NOT held during Raft submission (mock the group committer to track lock state)
  2. Integration test with concurrent writers to verify improved throughput

### Existing test coverage
- `RaftReplication2NodesIT`, `RaftReplication3NodesIT` - basic replication
- `RaftHTTPGraphConcurrentIT` - concurrent graph operations
- `RaftReplicationWriteAgainstReplicaIT` - follower write forwarding
- `RaftLeaderCrashAndRecoverIT` - crash recovery
- `RaftReplicationChangeSchemaIT` - schema change replication
- E2E tests in `e2e-ha/` - chaos scenarios

## Out of Scope

- `ReplicationCallback` event system - not needed, `ClusterMonitor` covers lag monitoring
- `TRANSACTION_FORWARD` Raft entry type - unused on `apache-ratis`, has known page visibility bug
- `COMMAND_FORWARD` entry type - unused on `apache-ratis`
- BOLT+TLS support - unrelated to commit path
