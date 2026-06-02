# Fix #4458: WAL page-version gap on TS shard page-0 under compaction + concurrent appends in Raft HA

## Root cause

`TimeSeriesShard.appendSamples()` held `compactionLock.readLock()` for the entire
begin-write-commit lifecycle. Under Raft HA, `commit()` calls
`RaftReplicatedDatabase.waitForActiveRecordingSession()`, which spins until the compaction
recording session ends. The session ends only after Phase 4c (which needs `compactionLock.writeLock()`),
but Phase 4c cannot acquire the write lock while this thread holds the read lock.

Deadlock:
- Append: holds readLock, waiting for recording session to end
- Phase 4c: waiting for writeLock (blocked by append's readLock)
- Recording session: ends only after Phase 4c completes

`waitForActiveRecordingSession()` has a `HA_QUORUM_TIMEOUT` escape hatch. When that fires:
- Append ships TX_ENTRY (page-0 at V+1) immediately
- Phase 4c eventually gets writeLock, commits, replicateSchema() ships SCHEMA_ENTRY (page-0 at V + clear at V+1)
- Follower receives TX_ENTRY[V+1] before SCHEMA_ENTRY[V, V+1] → WALVersionGapException → snapshot resync

## Fix

Release `compactionLock.readLock()` BEFORE calling `db.commit()`. This eliminates the deadlock:
Phase 4c can always acquire writeLock, complete, and trigger `replicateSchema()` promptly.
`waitForActiveRecordingSession()` then sees the session end quickly and does not time out.

If Phase 4c commits its page-0 clear between the readLock release and our commit, we receive
a `ConcurrentModificationException` (MVCC conflict) and transparently retry on the freshly
cleared page. The retry succeeds because the recording session has ended by this point, so
`waitForActiveRecordingSession()` returns immediately and the TX_ENTRY ships in the correct
Raft log order (after the SCHEMA_ENTRY).

### Files changed

- `engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesShard.java`
  - `appendSamples()` refactored to serialize via `appendLock` first, then on Raft HA leaders
    release `compactionLock.readLock()` before `commit()` (with a CME retry loop) to eliminate
    the deadlock; standalone mode holds the read lock through commit as before
  - Phase 0 `compactInternal()`: CME retry loop for in-flight append commits
  - `TEST_PRE_PHASE4C_HOOK` added for deterministic HA testing
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
  - `TEST_WAL_GAP_COUNTER` added for deterministic testing

### Tests added

- `engine/src/test/java/com/arcadedb/engine/timeseries/Issue4458AppendCompactionRaceTest.java`
  - Embedded DB: verifies CME-retry preserves all data when compaction races concurrent appends
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue4458TsWalVersionGapIT.java`
  - 2-node Raft HA: uses TEST_PRE_PHASE4C_HOOK to deterministically race an append against
    Phase 4c, verifies no WAL gap on the follower

## Verification plan

1. Compile engine + ha-raft modules
2. Run engine test: `mvn test -pl engine -Dtest=Issue4458AppendCompactionRaceTest`
3. Run HA test: `mvn test -pl ha-raft -Dtest=Issue4458TsWalVersionGapIT`
4. Run existing regression: `mvn test -pl grpcw -Dtest=TimeSeriesGrpcHaConcurrentInsertIT`
5. Run related TS suite: `mvn test -pl engine -Dtest="TimeSeries*"`
