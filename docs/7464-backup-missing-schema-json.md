# #7464 - A full backup whose `schema.json` is missing reports success

Issue: https://github.com/ArcadeData/arcadedb/issues/7464

## Problem

Both full-backup paths treat an absent `schema.json` as "nothing to archive" and carry on:

- `FullBackupFormat.backupFromFrozenFiles` -> `compressFile`, which ends
  `logger.logLine(2, " not found"); return 0;`
- `FullBackupFormat.backupFromSnapshot` -> `PageSnapshot.getConfigurationFiles()`, whose producer
  `PageManager.captureConfigurationFiles` catches `NoSuchFileException` per file and omits the entry.

That is correct for `configuration.json`, which only exists once a setting has been persisted. It is not correct
for `schema.json`: `LocalDatabase.create()` writes it before the database is usable, so its absence on an open
database means something is wrong - and the backup still writes its central directory and prints
"Full backup completed".

The consequence is worse than a missing entry. `DatabaseFactory.exists()` is:

```java
boolean exists = new File(databasePath + File.separator + LocalSchema.SCHEMA_FILE_NAME).exists();
if (!exists)
  exists = new File(databasePath + File.separator + LocalSchema.SCHEMA_PREV_FILE_NAME).exists();
return exists;
```

and neither backup path archives `schema.prev.json` either, so the restored directory satisfies neither arm:
the database is not recognised as existing at all, from an archive whose backup reported success.

## Root cause

There is no point in either backup path at which anybody asks "does this archive contain a database?". Each
file is archived independently and a file that is not there is simply skipped, so the *set* of entries is never
checked against the minimum a restore needs.

## Completeness

### 1. The invariant

> A full backup that reports success has written a non-empty `schema.json` entry into its archive; and a restore
> that reports success has produced a directory `DatabaseFactory.exists()` recognises as a database.

Two clauses because they protect two different populations: the first stops new bad archives being produced, the
second stops the bad archives that older builds have *already* produced from silently restoring to an
unrecognisable directory.

### 2. Enumerating the ways to violate it

```
$ grep -rn "new FullBackupFormat" --include="*.java" . | grep -v /target/
integration/src/main/java/com/arcadedb/integration/backup/Backup.java:166:      return new FullBackupFormat(database, settings, logger);

$ grep -rn "extends AbstractBackupFormat" --include="*.java" . | grep -v /target/
integration/src/main/java/com/arcadedb/integration/backup/format/FullBackupFormat.java:59:public class FullBackupFormat extends AbstractBackupFormat {
```

`FullBackupFormat` is the only backup format and `Backup` is its only constructor, so every producer of a backup
archive (CLI, console, SQL `BACKUP DATABASE`, the server's HTTP and gRPC handlers, the scheduled backup task)
reaches the archive through `FullBackupFormat.backupDatabase()`. That makes it the single chokepoint the issue
asks the fix to sit at.

Inside it there are exactly two paths that write configuration entries:

```
$ grep -n "getConfigurationFile()\|getConfigurationFiles()" integration/src/main/java/com/arcadedb/integration/backup/format/FullBackupFormat.java
218:      for (final PageSnapshot.SnapshotConfigFile config : snapshot.getConfigurationFiles())
247:    origSize += compressFile(archive, ((LocalDatabase) database.getEmbedded()).getConfigurationFile());
248:    origSize += compressFile(archive, ((LocalSchema) database.getSchema()).getConfigurationFile());
```

Consumers of the snapshot's captured configuration, to confirm no other main-code reader has to change:

```
$ grep -rn "getConfigurationFiles()" --include="*.java" . | grep -v /target/ | grep /src/main/
integration/src/main/java/com/arcadedb/integration/backup/format/FullBackupFormat.java:218
engine/src/main/java/com/arcadedb/engine/PageSnapshot.java:196
```

Readers of the two schema file names, which is what pins the restore-side clause to
`DatabaseFactory.exists()` rather than to `schema.json` alone:

```
$ grep -rn "SCHEMA_FILE_NAME\|SCHEMA_PREV_FILE_NAME" engine/src/main/java server/src/main/java integration/src/main/java
engine/src/main/java/com/arcadedb/database/DatabaseFactory.java:86,88   <- exists(): schema.json OR schema.prev.json
engine/src/main/java/com/arcadedb/database/LocalDatabase.java:342,343   <- same two-arm check
engine/src/main/java/com/arcadedb/database/BootstrapFingerprint.java:77,78
engine/src/main/java/com/arcadedb/schema/LocalSchema.java:94,95,252,2082,2083,2669,2890
engine/src/main/java/com/arcadedb/schema/SortedIndexBuildRecoveryMarker.java:177,182
engine/src/main/java/com/arcadedb/engine/PageManager.java:928           <- captures configuration.json + schema.json
```

`LocalSchema.readConfiguration()` (line 2082-2088) falls back to `schema.prev.json` when `schema.json` is
missing **or zero-length**, which is why the backup-side clause requires a *non-empty* entry and not merely a
present one: a zero-byte `schema.json` in the archive restores to a database that opens with an empty schema -
every type invisible - and reports no error at all.

Sibling shape - other "absent file, log and carry on" sites on this path:

```
$ grep -rn "not found\"" --include="*.java" integration/src/main/java
.../importer/SourceDiscovery.java:877        throws
.../importer/Neo4jImporter.java:704          throws
.../importer/OrientDBImporter.java:206       throws
.../exporter/Exporter.java:165               logs, database missing - different concern
.../backup/format/FullBackupFormat.java:358  <- the one in scope
.../backup/Backup.java:148                   throws
```

Only `FullBackupFormat:358` skips silently, and it is the site being fixed.

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `backupFromSnapshot` (default path) - `schema.json` absent at t0, omitted from the window | yes | yes |
| `backupFromFrozenFiles` (suspend-and-freeze fallback) - `compressFile` logs " not found" | yes | yes |
| `backupFromSnapshot` - `schema.json` present but zero-length | yes | yes |
| `backupFromFrozenFiles` - `schema.json` present but zero-length | yes | yes |
| `FullRestoreFormat` sequential walk - archive produced by an older build, no `schema.json` | yes | yes |
| `FullRestoreFormat` parallel extractor (#6086) - same archive | yes | yes |
| `FullRestoreFormat` - archive carrying only `schema.prev.json` | yes, accepted | yes |
| Brand-new database, no DDL ever run - must still back up and restore | yes (unchanged behaviour) | yes |
| `configuration.json` absent from either path | argued (below) | n/a |
| Neither path archives `schema.prev.json` | filed as [#7637](https://github.com/ArcadeData/arcadedb/issues/7637) | n/a |

**Argued - `configuration.json`.** It is written only once a setting has been persisted
(`LocalDatabase.CONFIGURATION_FILE_NAME`, absent on a freshly created database), it is not consulted by
`DatabaseFactory.exists()`, and a restore without it opens normally on the defaults. Absent is a legitimate
state for it and the fix deliberately leaves that behaviour untouched.

### 4. Reachability

`FullBackupFormat` is constructed on every backup (`Backup.java:166`, verified above) and the new check sits in
`writeArchive`, which every attempt of the retry loop runs. `FullRestoreFormat.restoreDatabase()` is the single
body both restore paths return through. No new configuration gates either check: both are unconditional.

### 5. Residual risk

- `schema.prev.json` is still not archived by either path (follow-up #7637). After this fix that is a loss of the
  restored database's *corruption fallback*, not of its recognisability: `schema.json` is now guaranteed present
  and non-empty in every archive this build produces, so `readConfiguration()` never needs the fallback on a
  fresh restore, and the first DDL on the restored database recreates it.
- A restore refused by the new check leaves the partially extracted directory in place **on the embedded/CLI
  path**, exactly as the pre-existing `stats.files() == 0` refusal immediately above it does. A directory without
  either schema file is not recognised as a database by `LocalDatabase`/`DatabaseFactory`, so nothing opens it;
  a retry needs `-o`, which was already true for the sibling refusal. On the **server** path there is no residue
  at all: `ServerControlPlane.performRestore` restores into a temporary directory and its
  `catch (InvocationTargetException ...)` deletes it before rethrowing
  (`server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1722-1731`), so the refusal cannot leave
  anything behind or touch the existing target.
- The backup check cannot distinguish "schema.json vanished" from "the database directory is being deleted under
  the backup". Both fail the backup, which is the intended answer to either.

## The fix

### Backup side - `FullBackupFormat`

`compressEntry` and `compressFile` are the only two methods that append an entry, and each now calls
`recordArchivedEntry(name, origSize)`, which raises a `schemaArchived` flag for a **non-empty** `schema.json`.
`writeArchive` resets the flag at the start of every attempt and calls `checkArchiveCarriesTheSchema()` after the
existing `failure.get()` rethrow and before `archive.close()`:

- After the failure rethrow, because a backup that died halfway is also missing entries and its root cause is the
  one worth reporting.
- Before `archive.close()`, so the throw lands in `writeArchive`'s `finally` and the archive is **aborted**: no
  central directory is written, so even if the caller's `backupFile.delete()` fails, nothing will restore from
  what is left.

Nothing about the per-file behaviour changed: an absent file is still skipped where it was skipped before. What
changed is that the *set* of entries is now checked once, at the only moment at which it is final.

`BackupException` is not a `PageSnapshotException`, so it lands in `backupDatabase`'s
`catch (final Exception e) { backupFile.delete(); throw e; }` and does **not** retry onto the frozen-files path -
which is the right answer, since that path reads the same missing file.

### Restore side - `FullRestoreFormat`

`restoreDatabase()` gained `checkRestoredDirectoryIsADatabase(databaseDirectory)` immediately after the existing
`stats.files() == 0` refusal, testing the extracted directory for a non-empty `schema.json` **or** a non-empty
`schema.prev.json` - the same two arms as `DatabaseFactory.exists()`. Checked on the directory rather than on
entry names so the sequential walk and the #6086 parallel extractor are both covered at the point they return
through.

This half exists for the archives older builds have already written: the backup-side fix cannot reach those, and
without it they still extract quietly into a directory that is later reported as "database not found".

## Test results

New: `integration/src/test/java/com/arcadedb/integration/backup/Issue7464BackupRequiresSchemaTest.java` (6) and
`integration/src/test/java/com/arcadedb/integration/restore/Issue7464RestoreRequiresSchemaTest.java` (7).

Before the fix, 8 of the 13 failed ("Expecting code to raise a throwable") and the 5 controls passed - so each
bug case was driven through its own entry point and each could fail.

```
mvn -o -pl integration verify -DskipITs=false -Dfailsafe.excludedGroups=benchmark,vector -DexcludedGroups=benchmark,vector
  surefire: Tests run: 496, Failures: 0, Errors: 0, Skipped: 9
  failsafe: Tests run: 136, Failures: 0, Errors: 0, Skipped: 0     BUILD SUCCESS
```

The failsafe run includes every backup/restore IT: `BackupCompressionIT`, `FullBackupIT`,
`Issue5517BloomFilterBackupIT`, `Issue6075SnapshotBackupIT`, `Issue6114LockFreeBackupIT`,
`Issue7280SealedTimeSeriesBackupIT`, `Issue7458BackupSurvivesConcurrentCloseIT`,
`Issue7586BackupArchiveEntriesSitAtArchiveRootIT`, `Issue6086ParallelRestoreIT`,
`Issue7567PreFixTimeSeriesPropertyRestoreIT`.

```
mvn -o -pl server test -Dtest='com.arcadedb.server.backup.*Test'
  Tests run: 169, Failures: 0, Errors: 0, Skipped: 0               BUILD SUCCESS
```

**Not run locally:** the `server` module's backup/restore *ITs* (`BackupApiCommandsIT`,
`BackupRestoreDeleteApiIT`, `Issue7392OnDemandBackupWithoutAutoBackupIT`, …). Port 2480 was held by a foreign
JVM for the whole session (`lsof -nP -iTCP:2480 -sTCP:LISTEN` -> `java 41389`), and server tests bind fixed
ports, so a local run would have failed as authentication errors rather than telling us anything. They exercise
the happy path through `Backup`/`Restore`, which the 136 integration ITs above cover, and CI runs them.

## Impact

- A backup that would produce an unrestorable archive now fails loudly instead of printing "Full backup
  completed", and leaves no archive for retention to count or for an operator to restore from.
- A restore from an archive an older build already wrote now fails at the restore, naming the file, instead of
  producing a directory that is reported as "database not found" later and elsewhere.
- No cost on the success path: one string comparison per archive entry, and two `File.length()` calls per
  restore.
- No new configuration and no new dependency.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen the author's reasoning. **The `Task` tool is
disabled in this session** ("No such tool available: Task. Task is disabled for this session, in subagents as
well as here"), and `ListAgents` offered no general-purpose peer, so the pass was run by the author against the
author's own patch. That is weaker by construction - it is the same reader - and it is recorded here rather than
claimed as the uninfluenced pass the skill asks for.

Findings, each verified by reading the tree:

1. **A backup of a brand-new database with no DDL was untested, and is the case the new refusal could most
   plausibly break.** Every existing backup test, and both of the new controls, ran a DDL first. If `schema.json`
   were only written on the first schema save, the refusal would fail a perfectly legitimate backup of an empty
   database. Verified it is not: `LocalDatabase.create()` calls `schema.saveConfiguration()` directly
   (`engine/src/main/java/com/arcadedb/database/LocalDatabase.java:351`), before any type exists, and
   `LocalSchema.create()` (line 260) only builds the dictionary. **Real and in scope - fixed here:**
   `aBackupOfABrandNewDatabaseWithNoSchemaYetStillSucceeds` now backs up, restores and reopens a type-less
   database on both paths. Added to the coverage table.

2. **"A restore refused by the new check leaves the partially extracted directory behind" was too broad.**
   True on the embedded/CLI path; false on the server path, where `performRestore` extracts into a temp
   directory and deletes it in `catch (final InvocationTargetException e)` before rethrowing
   (`ServerControlPlane.java:1722-1731`). **Real - the residual-risk note above was corrected**, since an
   overstated risk is as misleading as an understated one.

3. **`schemaArchived` is a non-volatile instance field read on a different statement from its writes.** Checked
   whether a worker thread could set it: it cannot. `ParallelZipArchiveWriter.addEntry` accumulates
   `uncompressedSize` in its own loop and returns `new EntryStats(uncompressedSize, compressedSize)` before any
   worker result is needed (`ParallelZipArchiveWriter.java:199-241`), and `ZipStreamArchiveWriter.addEntry` does
   the same (`ZipStreamArchiveWriter.java:61-78`); the workers only compress. `suspendFlushAndExecute` and
   `encryptFile` both run their callbacks on the calling thread. So every write and the single read happen on the
   backup thread. **Not real** - no change made.

4. **A second restore format could bypass the restore-side check.** `grep -rn "extends AbstractRestoreFormat"`
   returns exactly one class, `FullRestoreFormat`. **Not real.**

5. **An existing test elsewhere could build a synthetic archive the new restore check would now refuse.**
   `grep -rln "ZipOutputStream" server/src/test grpcw/src/test console/src/test` returns nothing. **Not real.**
