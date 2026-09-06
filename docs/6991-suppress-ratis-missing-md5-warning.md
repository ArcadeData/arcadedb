# #6991 - suppress the Ratis `SimpleStateMachineStorage` "has missing MD5 file" warning

## Goal

A follower restart with an existing snapshot marker must log nothing at WARNING for the missing
digest, while every genuine `SimpleStateMachineStorage` warning stays visible.

## Analysis

### Where the warning comes from

`org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.cleanupOldSnapshots()` (Ratis 3.3.0,
line 134) logs

```
LOG.warn("Snapshot file {} has missing MD5 file.", snapshot);
```

once for every rediscovered `snapshot.<term>_<index>` file whose `.md5` companion is absent.

ArcadeDB deliberately writes no `.md5` companion: `ArcadeStateMachine.registerSnapshotMarker` writes a
zero-byte placeholder whose *name* carries the `(term, index)` Ratis's snapshot-index bookkeeping and
log-purge contract point at. The real snapshot is the set of database files on disk, already durably
flushed by the `TransactionManager`, and a follower resyncs over HTTP through `DatabaseReconciler`
rather than through Ratis's chunk-verification path. So every marker ArcadeDB ever writes trips this
warning, forever.

Two call sites reach it, both after a snapshot checkpoint:

- ArcadeDB's own `registerSnapshotMarker` -> `storage.cleanupOldSnapshots(retain 1)`
- Ratis's `StateMachineUpdater.takeSnapshot` -> `cleanupOldSnapshots(snapshotRetentionPolicy)`

### Why the whole logger must NOT be pinned to SEVERE

The issue asked to check first whether that logger emits anything worth seeing. It does.
`SimpleStateMachineStorage` has exactly two WARNING-level messages:

| line | message | verdict |
|---|---|---|
| 134 | `Snapshot file {} has missing MD5 file.` | by design for ArcadeDB, pure noise |
| 247-248 | `Failed to updateLatestSnapshot from {}` + a directory listing | a genuine I/O failure reading the snapshot directory - must stay visible |

A `...SimpleStateMachineStorage.level = SEVERE` line in `arcadedb-log.properties` (the treatment the
`GrpcLogAppender` flood gets) would hide the second one too, which contradicts the second acceptance
criterion. JUL properties cannot express a per-message threshold, so the suppression is a
`java.util.logging.Filter` installed on that one logger instead.

## Change

New `com.arcadedb.server.ha.raft.ratis.RatisSnapshotDigestWarningFilter`:

- suppresses a record only when its message contains `has missing MD5 file` **and** its level is at
  most `WARNING`, so a future Ratis release that raised the same text to `SEVERE` would still surface
- chains to whatever `Filter` the logger already carried instead of replacing it
- is idempotent, and holds a strong reference to the configured `Logger` (JUL's `LogManager` keeps only
  a weak one, so an unreferenced configured logger can be collected and re-created without the filter)

Installed from the two places the md5-less marker regime begins:

- `ArcadeStateMachine.initialize(...)`, right before `storage.init(raftStorage)`
- `RaftHAServer.start()`, next to the existing `Logger.getLogger("org.apache.ratis")` level pin

The comment next to the `GrpcLogAppender` entry in `engine/src/main/resources/arcadedb-log.properties`
and `package/src/main/config/arcadedb-log.properties` now records why this second suppression is *not*
a properties entry, so nobody "completes the pattern" by pinning the whole logger later.

## Tests

`ha-raft/src/test/java/com/arcadedb/server/ha/raft/ratis/RatisSnapshotDigestWarningFilterTest.java`,
driving the real Ratis `SimpleStateMachineStorage` through a real `ArcadeStateMachine` and a real
`RaftStorage` over a temp dir (no mocks), with a counting `java.util.logging.Handler` attached to the
Ratis logger:

- control: with the filter removed, registering markers really does emit the warning (proves the test
  can fail)
- with the filter installed by `ArcadeStateMachine.initialize`, the same path emits nothing
- a genuine `Failed to updateLatestSnapshot` warning from the same logger still gets through
- a `SEVERE` record carrying the same text still gets through
- installing twice does not stack filters, and a pre-existing filter still gets consulted

## Follow-up noted, out of scope - filed as #7209

`SimpleStateMachineStorage.cleanupOldSnapshots` only ever sets `deleteIdx` after it has seen
`numSnapshotsRetained` snapshots that *have* an md5. ArcadeDB writes none, so `deleteIdx` stays `-1`
and no old marker is ever deleted - the "Keep only the latest marker" intent in
`registerSnapshotMarker` is a no-op today and zero-byte markers accumulate for the life of the node
(which is also why the warning count grows with uptime). Silencing the log does not change that; it
should be fixed separately, either by pruning the markers directly or by writing the (constant,
zero-byte) md5 companion.
