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

## Status

- [x] Tests written
- [x] Implementation complete
- [x] Tests pass
