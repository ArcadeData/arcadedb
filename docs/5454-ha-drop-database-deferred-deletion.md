# Issue #5454 - drop database deletes synchronously inside the Raft apply

## Symptom

`drop database` performed the full physical deletion of the database on the Ratis apply thread.
Because `ArcadeStateMachine` multiplexes every database onto one sequential apply loop, a slow drop
of database A stalled the apply of committed entries for databases B and C on every node, and could
outlive the caller's `arcadedb.ha.quorumTimeout` budget, producing:

```
ReplicationDispatchedTimeoutException:
  Group commit entry failed: TimeoutException (entry was dispatched to Raft; outcome unknown)
```

for a drop that in fact committed and will be applied everywhere. A follower busy inside a long drop
also stops applying, so its `commitIndex - appliedIndex` lag grows and `isReadyForTraffic` flaps.

## Root cause

`ArcadeStateMachine.applyDropDatabaseEntry` called `db.getEmbedded().drop()` inline:

```java
final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
db.getEmbedded().drop();        // close + FileUtils.deleteRecursively, on the apply thread
server.removeDatabase(databaseName);
```

`LocalDatabase.drop()` is `closeInternal(true)` followed by `FileUtils.deleteRecursively(databasePath)`.
The recursive delete is one `unlink` syscall per file (buckets, indexes, WAL segments), unbounded in
the size of the database, and it ran with the apply loop held. `lastAppliedIndex` only advances after
apply returns, so nothing else on the state machine progressed meanwhile.

## Fix

Split the drop into a bounded synchronous part and an unbounded background part.

1. **Engine** - `LocalDatabase.closeForDrop()` exposes the close half of `drop()` (drop-time close
   semantics: no index flush, no file sync) without removing the files. `drop()` is now
   `closeForDrop()` + the recursive delete, so its behaviour is unchanged.

2. **ha-raft** - new `DeferredDatabaseDeleter`:
   - `stageForDeletion(dir)` renames `databases/<name>` to the sibling reserved directory
     `databases/.dropped-<name>-<nanos>` with `ATOMIC_MOVE`. The rename is O(1) and is the only file
     work left on the apply thread.
   - `deleteInBackground(staged)` queues the recursive delete on a dedicated single-thread daemon
     executor. It is deliberately a separate call so the caller can make it outside the lock it holds
     across the staging step: on a saturated queue the delete runs on the calling thread, and that
     must not widen the lock hold.
   - If the rename fails (cross-device, or a platform holding handles) `stageForDeletion` falls back
     to deleting inline, so the outcome is never worse than before the fix.
   - `sweepOrphanedStagingDirectories(databasesDir)` enqueues any `.dropped-*` left behind by a crash
     or by an executor shutdown.

3. **ha-raft** - `applyDropDatabaseEntry` now closes the database, deregisters it and stages the
   rename while holding `server.getDatabasesLock()`, mirroring `SnapshotInstaller.swapAndReopen`, so
   no concurrent `getDatabase` can reopen the directory between the close and the rename. The
   background delete runs outside the lock and outside the apply loop.

4. `ArcadeStateMachine.initialize` runs the orphan sweep next to
   `SnapshotInstaller.recoverPendingSnapshotSwaps`; `close()` shuts the deleter down.

### Why a reserved-prefix rename is safe

`ArcadeDBServer.RESERVED_DATABASE_PREFIX` is `"."`, and every consumer of the databases directory
already skips dot-prefixed entries: `loadDatabases`, `SnapshotInstaller.recoverPendingSnapshotSwaps`,
`DatabaseReconciler`, `GetClusterHandler`, `PostBootstrapStateHandler`. So a staged directory is
invisible to the registry, to the cluster view, and to a restart.

`create database` with the just-dropped name also stops colliding: `ArcadeDBServer.createDatabase`
rejects on `DatabaseFactory.exists()`, which checks `databases/<name>/schema.json`, and that path no
longer exists once the rename returns - previously it existed for the whole duration of the delete.

### Idempotent replay

Crash after the rename but before `writePersistedAppliedIndexDroppingDatabase`: on restart the
directory is dot-prefixed, `loadDatabases` skips it, so `server.existsDatabase(name)` is false and the
replayed entry takes the existing early-return branch. The staged directory is removed by the sweep.
Crash before the rename: the directory still carries its real name, is reloaded, and the replay drops
it again.

## Tests

- `engine` `LocalDatabaseCloseForDropTest` - `closeForDrop()` closes without deleting; the files are
  still openable afterwards; `drop()` still deletes; `closeForDrop()` inside a transaction throws.
- `ha-raft` `DeferredDatabaseDeleterTest` - the rename is synchronous and the physical delete is not;
  the staged name is reserved; repeated drops of the same name get distinct staging directories; the
  inline fallback when the rename cannot happen; a saturated queue deleting on the calling thread; the
  orphan sweep removes orphans and nothing else.
- `ha-raft` `ArcadeStateMachineDeferredDropTest` - `applyDropDatabaseEntry` deregisters the database
  and clears `databases/<name>` synchronously while leaving the physical delete to the background;
  eviction of the bootstrap baseline is preserved; replay after the rename is a no-op.

Pre-existing coverage that must stay green: `RaftDropDatabase3NodesIT` (asserts
`databases/<name>` is gone on all three peers - still true, the rename happens before apply returns),
`ArcadeStateMachineBootstrapBaselinePersistenceTest`, `ArcadeStateMachineAppliedIndexPerDatabaseTest`,
`PostServerCommandHandlerIT`.

## Verification

| Suite | Result |
|---|---|
| `engine` `LocalDatabaseCloseForDropTest` | 4/4 |
| `ha-raft` full unit suite (`mvn -pl ha-raft test`) | 754/754 |
| `ha-raft` `RaftDropDatabase3NodesIT` | 1/1 |
| `server` `PostServerCommandHandlerIT` | 23/23 |
| `engine` `com.arcadedb.database.*` | 149 run, 1 pre-existing failure |

The engine failure is `DatabaseLifecycleBackgroundThreadsTest.engineBackgroundThreadsAreDaemon`
(`Query engine 'js' was not found`), reproduced identically on the base commit with the change
stashed - it is a local classpath gap for the polyglot engine, unrelated to this work.

## Known gaps

- The non-HA drop path (`PostServerCommandHandler.dropDatabase` when the database is not
  `HAReplicatedDatabase`) still deletes inline. It does not run on the apply loop and has no quorum
  budget, so it is out of scope here.
- The client-visible indeterminate outcome (`ReplicationDispatchedTimeoutException`) is not addressed;
  this change removes the dominant reason to hit it for a drop, but option 1 of the issue (a way to
  confirm a dispatched drop committed) remains open.
- The deleter's pool is not wired into `PoolMetrics`, so it does not appear on the Studio "Executor
  Pools" card. `PoolMetrics` lives in `server` and binds JVM-wide engine singletons through their
  `getInstance()`; `server` cannot depend on `ha-raft` (the dependency runs the other way, `provided`),
  and this pool is a per-state-machine instance, so surfacing it needs a registration SPI rather than a
  new `bindPool` call. Every other `ha-raft` pool is unmetered today for the same reason. The saturation
  signal the card would carry is covered in the meantime by a throttled WARNING (60 s window, matching
  the engine pools) emitted whenever the queue is full and the delete runs on the calling thread.
- Physical deletion failures are still swallowed by `FileUtils.deleteRecursively`. A staged directory
  that survives is now retried by the sweep on the next restart, which is strictly better than the
  previous behaviour where a half-deleted `databases/<name>` was reloaded as a live database.
