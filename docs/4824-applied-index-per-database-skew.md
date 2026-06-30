# Issue #4824 - Single global `applied-index` file vs per-database snapshot installs

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
bootstrapped is silently skipped - the "value that mixes databases" defect described in the issue.

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
- `bootstrapSkipNotTriggeredByAnotherDatabasesAppliedIndex` - regression: a co-located database's
  high applied index must NOT skip another database's bootstrap verification (fails before fix).
- `bootstrapSkipHonorsPerDatabaseAppliedIndex` - a database's OWN per-database applied index does
  still skip its bootstrap verification (legitimate skip preserved).
- `legacyPlainNumberFileReadAsGlobalNotPerDatabase` - a legacy plain-number file is honoured as
  the global value and yields no per-database evidence.
- `perDatabaseValuesRoundTripAcrossRestart` - per-database and global values round-trip through the
  file and recover in a fresh state machine.
- `fullInstallRecordsSnapshotIndexForEveryPresentDatabase` - a full state-machine install records
  the snapshot index for every present database (exercises the `getDatabaseNames()` path).

## Upgrade behavior (one-time)

A legacy plain-number `.raft/applied-index` file written by an older version carries no per-database
breakdown, so on the FIRST restart after upgrade every per-database read returns -1. Any
`BOOTSTRAP_FINGERPRINT_ENTRY` still above the latest Ratis snapshot at that moment will re-run
verification instead of being skipped. This is bounded (it only affects bootstrap entries still above
the latest snapshot, mostly recently-formed/low-snapshot clusters) and safe:

- matching local fingerprint -> returns immediately (no bytes moved);
- locally-fresher copy (`local lastTxId > baseline`) -> hits the existing "refusing to overwrite
  local data" guard: a SEVERE log line, but no data loss;
- genuinely-behind copy (`local < baseline`) -> re-installs from the leader, the correct action
  anyway (download-before-close keeps the database open on failure).

From the first post-upgrade apply onwards the per-database map is authoritative and the skew is fixed.

A downgrade degrades gracefully too: an older binary reading the new JSON document fails to parse it
as a plain number, logs at FINE, and treats the global value as -1, which re-runs the same idempotent
verification. No external reader of `.raft/applied-index` exists in the tree, so the format change is
self-contained.

## PR

https://github.com/ArcadeData/arcadedb/pull/4847

## Review cycles

- cycle 1 (`5267f6b`): gemini + claude reviewed. Addressed: synchronise the persisted-index writers
  on `appliedIndexFileLock` (in-memory + shared temp-file race; both bots); evict dropped databases
  from the per-database map (claude); add a test for the full-install path and stub
  `getDatabaseNames()` (claude); document that the hot-path JSON allocation is dominated by the
  atomic-rename I/O that already ran every apply (claude); fix the doc test count (claude).
- cycle 2 (`851e3d5`): gemini re-posted the (already-applied) synchronisation suggestion - no-op.
  Addressed claude: fold the DROP-entry global advance + per-database eviction into a single atomic
  write (`writePersistedAppliedIndexDroppingDatabase`) to remove the two-write crash window;
  document the one-time legacy-upgrade re-verification behavior (bounded and safe) in code and in the
  "Upgrade behavior" section above.
- cycle 3 (`6422e01`): gemini re-posted the (already-applied) synchronisation suggestion - no-op.
  Addressed claude: do not latch the load cache when the file path is unresolvable (server not wired)
  so a later-available file is never masked; add a direct DROP-eviction test; note the graceful
  downgrade path; comment that `globalAppliedIndex` mirrors `lastAppliedIndex`; replace em dashes in
  this doc. The "Review cycles" section is retained per the resolve-issue-with-review Phase 6 convention.
- cycle 4 (`5c3e016`): gemini re-posted the (already-applied) synchronisation suggestion - no-op.
  Claude rated all remaining points non-blocking polish. Applied: soften the
  `globalAppliedIndex`/`lastAppliedIndex` comment (they can briefly differ after `reinitialize()`);
  note that the corrupt-file -> gap-detection suppression coupling is deliberate; add the upgrade-merge
  test (`writePreservesOtherDatabasesEntriesOnDisk`) for the load-before-mutate property.

## Final state

max-cycles-reached (4 cycles). All actionable review feedback addressed; the final-cycle reviews are
approval-level / non-blocking polish (gemini's recurring synchronisation note was already implemented
in cycle 1). Merge remains the developer's responsibility.
