# #5728 - a schema change made while another thread holds the recording session never replicates

## Issue

`Bolt5002RoutingTableIT.neo4jSchemeRoutesReadsAndWrites` intermittently leaves one follower without the
`Bolt5002Route` vertex type. Two independent signals in CI run 30706875826: a 60 s poll never saw server 1
receive the type, and the teardown comparator reported `DatabaseAreNotIdentical: Types: DB1 6 <> DB2 5`.
It passes in isolation and fails under a loaded runner, so it is load- or timing-sensitive.

## Root cause

Not the `#5492` / `#5655` wrong-database-instance family. The Bolt write path resolves the replicated
wrapper correctly end to end:

```
BoltNetworkExecutor.handleRun                  -> ServerDatabase.command("opencypher", ...)
  -> RaftReplicatedDatabase.command            -> LocalDatabase.command
    -> OpenCypherQueryEngine.executionDatabase -> database.getWrappedDatabaseInstance()   (the Raft wrapper)
```

The defect is one level down, in how a DDL step reaches Raft.

Every schema mutation funnels through `LocalSchema.recordFileChanges`
(`engine/src/main/java/com/arcadedb/schema/LocalSchema.java:2352`), which calls
`database.getWrappedDatabaseInstance().recordFileChanges(callback)`. Under HA that lands on
`RaftReplicatedDatabase.recordFileChanges`, whose first act is to open a *file-manager recording session*
so it can capture the created files, the serialized schema and any WAL buffered inside the callback, and
ship them as one `SCHEMA_ENTRY`.

```java
// RaftReplicatedDatabase.java:1434 (before the fix)
final boolean alreadyRecording = !proxied.getFileManager().startRecordingChanges();
if (alreadyRecording)
  return proxied.recordFileChanges(callback);   // <-- runs the DDL LOCALLY, replicates nothing
```

`FileManager.recordedChanges` is a plain instance field - one session per database, no owner, no
re-entrancy counter (`engine/src/main/java/com/arcadedb/engine/FileManager.java:147`). So
`startRecordingChanges()` returns `false` in two completely different situations that the code above
cannot tell apart:

1. **Re-entrant, same thread.** `LocalSchema.createVertexType` nests: the outer callback creates the type
   and an inner `LocalDocumentType.recordFileChanges` creates its buckets. The inner call must not open a
   second session - the outer one is already recording, and it replicates everything on the way out.
   This is the case the branch was written for, and it is correct.

2. **Contended, different thread.** Another thread is between `startRecordingChanges()` and
   `stopRecordingChanges()` - a concurrent DDL, or an LSM index compaction inside
   `runWithCompactionReplication`. There is no outer session *on this thread* to carry the changes, so the
   DDL is applied on the leader alone and **nothing is proposed to Raft**. Nothing throws.

Case 2 is the bug. The window is wide open on purpose: the owning session releases the database write
lock when its callback returns (line 1452) but holds the recording session until after
`replicateSchema()` (line 1489) has made a full Raft round trip. A second thread that arrives during that
Raft proposal finds the write lock free and the session busy - exactly the state a loaded CI runner
produces and an idle laptop does not.

The same hazard on the compaction side was already found and fixed as **#4063**:
`runWithCompactionReplication` (line 1525) used to have an identical "local-only fallback" and now
*defers* instead, returning `false` so the async scheduler retries. `recordFileChanges` never got the
matching treatment, and it cannot simply defer because the DDL is synchronous user-visible work.

`FileManager.startRecordingChanges()` was also a non-atomic check-then-set on a non-volatile field, so two
arriving threads could both be told they had started a session; the second would replace `recordedChanges`
with a fresh list and the first would lose its recorded file creations.

## Fix

`ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`

- Distinguish the two cases by asking this database's own `FileManager` whether the active session belongs
  to the calling thread (`isRecordingChangesOnCurrentThread()`). When it does, nesting is safe - delegate
  to `proxied` as before. The static `isSchemaCommitThread` thread-local is deliberately *not* the signal:
  it is shared across every `RaftReplicatedDatabase`, so it would also answer yes for a thread nested in a
  *different* database's session, for which this database has replicated nothing.
- `recordFileChanges` now saves and restores that thread-local instead of removing it, so a session opened
  on another database from inside an outer callback cannot clear the outer frame's mark and send its
  remaining commits as separate `TX_ENTRY`s (the #4083 ordering hazard). `runWithCompactionReplication`
  keeps the bare `remove()` - it defers rather than nesting, so it can never be the inner frame.
- When the session belongs to another thread, `acquireRecordingSession()` waits for it to be released
  instead of silently skipping replication, then opens our own and replicates normally. On expiry it
  throws `TimeoutException` rather than diverge - unlike a compaction, a schema change cannot be deferred
  and rescheduled. The bound reuses `HA_QUORUM_TIMEOUT`, the same setting the sibling
  `waitForActiveRecordingSession()` on the commit path already uses; no new configuration key.
- Leadership is re-checked after the wait. Claiming the session can take seconds, and running the callback
  on a node that became a follower meanwhile would apply the schema change there and propose nothing - the
  same divergence from the other end.

`engine/src/main/java/com/arcadedb/engine/FileManager.java`

- Make `startRecordingChanges()` / `stopRecordingChanges()` an atomic pair (`synchronized`, `volatile`
  field) so the waiting thread and a compaction thread cannot both win the session. The check-then-set was
  previously unsynchronized on a non-volatile field, so two arriving threads could both be told they had
  started; the second would replace `recordedChanges` and the first would lose its recorded file
  creations.
- Record the session's owning thread and expose `isRecordingChangesOnCurrentThread()`, so "a session is
  open" and "*my* session is open" stop being the same question.

## The cost of waiting

`TypeBuilder.create` wraps the whole operation in `executeInWriteLock`, so the poll can run while the
caller holds the database write lock, stalling writers for as long as it waits. This is a deliberate
trade: an availability pause bounded by `HA_QUORUM_TIMEOUT`, in place of a leader that silently stops
replicating its schema. The bound is also what stops the stall becoming permanent should the session's
owner ever need a lock the waiter holds. A successful wait logs how long it blocked, at `HALog.BASIC`, so
an operator seeing writes pause can find the reason rather than infer it.

For the same reason, the contending owner is in practice a compaction rather than another DDL: the DDL
entry points that take the write lock around the whole operation serialize on it before ever reaching
`acquireRecordingSession()`.

## Verification

`Issue5728SchemaChangeDuringRecordingSessionIT` (ha-raft). It plants the contended state rather than
racing for it: the test thread takes the recording session directly from the leader's `FileManager` -
the same technique `RaftIndexCompactionReplicationIT.compactionDefersWhenRecordingSessionActive` uses for
the #4063 contract - creates a vertex type from a second thread, and releases the session shortly after.

- Before the fix: the DDL returns immediately down the local-only path, the followers never see the type.
- After the fix: the DDL blocks until the session is released, then replicates.

A second test pins the other half of the contract: a session that is never released must make the schema
change throw `TimeoutException`, and leave the type absent on the leader too. Both tests hold the session
on a thread *other* than the one running the DDL - a same-thread holder is the legitimate re-entrant
nesting the fix still allows through, so the wait, and therefore the timeout, would never be reached.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5743

### Review cycles

| Cycle | Head | Outcome |
|---|---|---|
| 1 | `4befacf` | LGTM. Asked for a distinct message on the interrupted wait, flagged that `isSchemaCommitThread.remove()` does not restore a prior value, and noted the timeout branch was uncovered. All three applied. |
| 2 | `266b9a3` | LGTM. Found that the new method's Javadoc had been inserted between `waitForActiveRecordingSession()`'s Javadoc and its declaration, orphaning it - fixed. Argued the two-signal guard still did not fully close the cross-database hole and recommended tracking the session's owner thread instead - adopted. |
| 3 | `1cc8bfa` | LGTM, no blockers. Noted the wait widens the window for losing leadership mid-DDL, and that the polled FINE log evaluates its argument unguarded - both applied. Declined the `wait()`/`notifyAll()` and `stopRecordingChanges()` owner-assertion suggestions: the reviewer called both pre-existing style, and the polling form is deliberately consistent with the sibling `waitForActiveRecordingSession()`. |
| 4 | `5838497` | LGTM, no blockers. Caught the same Javadoc-displacement mistake a second time (the new `schemaChangesNeedTheLeader()` helper had been inserted between `acquireRecordingSession()`'s Javadoc and its declaration) and, more importantly, established that the wait can run while the caller holds the database write lock - which contradicted a claim in that Javadoc. Both corrected, and the stall is now logged. Declined lowering `HA_QUORUM_TIMEOUT` for the timeout test: it would trade 10 s of runtime for a tighter quorum bound on a loaded CI runner, and `@Tag("slow")` is a no-op for ITs. |

Adopting the owner-thread field in cycle 2 exposed a flaw in the timeout test added in cycle 1: it held
the session on the same thread that ran the DDL, which is by definition the re-entrant nesting case, so
the wait it meant to exercise was never reached. Both tests now hold the session on a separate thread.

### Final state

`max-cycles-reached` - four cycles run, every review LGTM, no blocking finding outstanding. Merge is the
developer's call.
