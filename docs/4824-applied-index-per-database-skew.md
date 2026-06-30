# Issue #4824 — Single global `applied-index` file vs per-database snapshot installs

## Problem

`ArcadeStateMachine` multiplexes every database onto one Raft group, but persists a single
global scalar at `<SERVER_DATABASE_DIRECTORY>/.raft/applied-index`. That scalar is advanced on
every `applyTransaction` regardless of which database the entry targeted, so it represents the
union of all databases' Raft-log progress. A per-database snapshot install fast-forwards one
database's on-disk state without a matching meaning for the global counter, so the persisted
value cannot be trusted to answer a per-database question.

The one genuinely per-database decision that consumes the scalar is the replay-skip in
`applyBootstrapFingerprintEntry`:

```java
final long persistedApplied = readPersistedAppliedIndex();
if (persistedApplied >= index) {  // "this DB's baseline already applied" -> skip verification
  return;
}
```

Here `index` is the Raft index of THIS database's `BOOTSTRAP_FINGERPRINT_ENTRY`, but
`persistedApplied` is the GLOBAL applied index. If a co-located database advanced the global
index past this entry's index, the bootstrap verification for a database that was never actually
bootstrapped is silently skipped — the "value that mixes databases" defect described in the issue.

`reinitialize()`'s snapshot-gap decision (`snapshotIndex > persistedApplied + tolerance`) is
legitimately global: the Ratis snapshot index it compares against is itself global, so it keeps
using the global value (behaviour unchanged).

## Fix

Make applied-index tracking per-database-aware (suggested fix option 2):

- The persisted file becomes a small JSON document: a `global` Raft-log position plus a `db` map
  of `database name -> highest applied Raft index for that database`. A legacy plain-number file
  is read as the `global` value with an empty per-database map (honest: no per-database evidence).
- `applyTransaction` records the applied index against the database the entry targeted
  (`decoded.databaseName()`) as well as the global position.
- `applyBootstrapFingerprintEntry` now consults the PER-DATABASE applied index (strict, no global
  fallback): verification is skipped only when there is positive per-database evidence that this
  database was already advanced past the entry. Absent that evidence it re-verifies, which is
  idempotent (a fingerprint match returns immediately).
- The whole-state-machine Ratis install path records the snapshot index for every present
  database, since after a full install every database is at that index.
- An in-memory cache (volatile global + `ConcurrentHashMap`) keeps the hot apply path cheap: the
  file is parsed once on first access; each apply updates memory and serialises the small map.

## Tests

`ArcadeStateMachineAppliedIndexPerDatabaseTest`:
- `bootstrapSkipNotTriggeredByAnotherDatabasesAppliedIndex` — regression: a co-located database's
  high applied index must NOT skip another database's bootstrap verification (fails before fix).
- `bootstrapSkipHonorsPerDatabaseAppliedIndex` — a database's OWN per-database applied index does
  still skip its bootstrap verification (legitimate skip preserved).
- `legacyPlainNumberFileReadAsGlobalNotPerDatabase` — a legacy plain-number file is honoured as
  the global value and yields no per-database evidence.
