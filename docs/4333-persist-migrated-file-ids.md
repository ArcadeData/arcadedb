# Fix #4333: Persist migratedFileIds in schema.json

## Issue

`LocalSchema.migratedFileIds` (the `oldFileId -> newFileId` map written by LSM/vector index compaction) is a heap-only `ConcurrentHashMap`. It is never included in `schema.json`. After a restart the map is empty. WAL records addressed to the old fileId hit `!existsFile(...)` in `applyChanges` and are silently skipped with only a FINE-level log.

## Root Cause

- `LocalSchema.toJSON()` does not serialize `migratedFileIds`
- `LocalSchema` loading code does not deserialize `migratedFileIds`
- `TransactionManager.applyChanges()` cannot distinguish a safe compaction-migration skip from a
  genuinely unexpected missing file because the migration map is always empty after restart

## Files Changed

- `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`
  - `toJSON()`: serialize `migratedFileIds` as a JSON object under key `"migratedFileIds"`
  - loading block: deserialize `migratedFileIds` from JSON (near the `extensions` loading section)
- `engine/src/main/java/com/arcadedb/engine/TransactionManager.java`
  - `applyChanges()`: consult `migratedFileIds` when a fileId is missing; log FINE for known
    migrations, WARNING for genuinely unknown missing files

## Test

`engine/src/test/java/com/arcadedb/index/MigratedFileIdsPersistenceTest.java`

Two tests:
1. `migratedFileIdsArePersisted` - after compaction triggers `setMigratedFileId`, close and reopen
   the database; migration map must still be populated (proves persistence)
2. `migratedFileIdsAreLoadedOnReopen` - verifies the loaded map has the same content as before
   close (proves deserialization correctness)

## Verification

Run: `mvn test -pl engine -Dtest=MigratedFileIdsPersistenceTest`
Run: `mvn test -pl engine -Dtest=LSMTreeIndexCompactionTest`

## PR

https://github.com/ArcadeData/arcadedb/pull/4342

## Review Cycles

### Cycle 1 - a47f16b11

**gemini-code-assist** reviewed (COMMENTED):

1. HIGH - `setMigratedFileId` does not call `saveConfiguration()`, so if no other schema change follows compaction the map is not saved. Applied: added `saveConfiguration()` call inside `setMigratedFileId`.
2. MEDIUM - `root.getJSONObject("migratedFileIds")` throws if value is JSON null. Applied: added `!root.isNull("migratedFileIds")` guard.
3. MEDIUM - WARNING log too noisy for schema-drop deletions during Raft replay. Applied: downgraded to INFO level.

Follow-up commit: `3098f1590`

### Cycle 2+

gemini-code-assist does not re-review follow-up pushes on this repo (documented in memory). State: `timeout`.

## Final State

`timeout` - cycle 1 review addressed and pushed; gemini does not re-review. Developer should verify and merge PR #4342.

## Status

- [x] Tests written
- [x] Implementation complete
- [x] Cycle-1 review addressed
- [x] Tests pass
