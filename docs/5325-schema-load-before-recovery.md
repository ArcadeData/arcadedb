# 5325 - schema.load() runs before checkForRecovery()

## Problem

`LocalDatabase.openInternal` loaded the schema and only then called `checkForRecovery()`, which is the
method that acquires the exclusive OS lock on `database.lck`. Loading the schema of a database that
needs recovery is not a read-only operation:

- `LocalSchema.load` runs `SortedIndexBuildRecoveryMarker.recoverInterruptedBuilds`, which **drops
  component files** left by an interrupted sorted index build.
- When the dictionary page never reached disk the file is zero length, and
  `Dictionary.createHeaderPageIfMissing` **writes and commits** a fresh header page.

Both happened while the database was still unlocked, so an open could mutate a database that another
process owns. Reproduced by `RecoveryLockOrderingTest.openOfALockedCrashedDatabaseWritesNothingBeforeItIsRejected`:
with the lock held by a second channel, the rejected open still wrote a 327680-byte dictionary page.

The "unopenable database" symptom quoted in the issue (`SchemaException: File with id '0' was not
found`) was already fixed in #5322, which deferred the header-page creation to
`createHeaderPageIfMissing()` after the component is registered. What remained was the ordering itself.

## Fix

`checkForRecovery()` split into two phases in `LocalDatabase`:

- `prepareRecovery()` - sets `lockFile`, performs the READ_ONLY-needs-recovery rejection, acquires the
  exclusive lock, and returns whether the WAL still has to be replayed. Called **before**
  `schema.create()` / `schema.load()`.
- `performRecovery()` - resets the cached bucket record counts, fires `DB_NOT_CLOSED`, and runs
  `transactionManager.checkIntegrity()`. Called **after** the schema is loaded, because the WAL replay
  resolves file ids and the dictionary through the registered components.

The replay therefore keeps its existing position (it needs the schema), while everything that takes
ownership of the database now precedes any write to it.

## Tests

- `engine/src/test/java/com/arcadedb/database/RecoveryLockOrderingTest.java` (new)
  - `openOfALockedCrashedDatabaseWritesNothingBeforeItIsRejected` - fails before the fix
    (dictionary file grew to 327680 bytes), passes after.
  - `readOnlyOpenOfACrashedDatabaseIsRejectedBeforeTheSchemaIsLoaded` - guards the READ_ONLY path.
- `engine/src/test/java/com/arcadedb/database/UnflushedDictionaryRecoveryTest.java` (new) - a crash
  that leaves the dictionary page unflushed must still replay the WAL: the type and the record
  committed before the kill survive, and no WAL file is quarantined as `.corrupt`.

Existing recovery/lock tests re-run green (33 tests, 0 failures, 0 errors): `WalRecoveryCorrectnessTest`,
`FlushRobustnessTest`, `TransactionManager*Test`, `WALFile*Test`, `EmptyDictionaryFileReopenTest`,
`FailedOpenPreservesWalTest`, `Issue4511OpenFailureLockLeakTest`,
`Issue4547ReadOnlyRecoveryLockLeakTest`, `Issue5067KillLockSymmetryTest`,
`DatabaseFactoryFailedOpenTest`, `DatabaseFactoryTest`.

A wider `com.arcadedb.database.** + engine.** + schema.**` run was also made; it is reported here only
for completeness, because it was invalidated part-way by the machine running out of disk (the resulting
`NoClassDefFoundError`s name classes that exist in the tree). The `com.arcadedb.database.**` portion
completed before that and was compared against the unmodified baseline: the only two failures,
`DatabaseGetSizeTest.getSizeMultipleCalls` (async page-flush timing) and
`ScriptTriggerSandboxTest.benignTriggerStillWorks` (GraalVM polyglot class init), reproduce identically
without this change and are unrelated to it.

## PR

https://github.com/ArcadeData/arcadedb/pull/5351

## Review cycles

### Cycle 1 - `7ca03dd`

Both gating bots responded. No code changes were required, so the branch ends the cycle unchanged.

- **gemini-code-assist** (COMMENTED, 1 inline) - asked for braces around the single-statement `else`
  in `prepareRecovery()`. Declined: CLAUDE.md sets the opposite convention ("if statements with only
  one child sub-statement don't require a curly brace open/close"), and that `else` is verbatim
  pre-existing code the diff only relocated out of `checkForRecovery()`.
- **claude** - no blocking findings; confirmed the split is behavior-preserving against the base
  revision (exception propagation, `.lck` not being in `SUPPORTED_FILE_EXT`, ordering inside
  `performRecovery()`). Two optional notes, both declined with rationale:
  - a shared test helper for the `.dict`/`.wal` listing code - `EmptyDictionaryFileReopenTest` already
    carries its own copy, so self-contained-per-class is the existing convention;
  - the clean-path `database.lck` creation moving ahead of `schema.load()` - already covered under
    Notes below.

Final state: `clean-approval`.

## Notes

- `database.lck` is now created (non-recovery READ_WRITE path) before the schema is loaded rather than
  after. A failure during the load consequently leaves the marker behind, which makes the next open
  recover - the behaviour `releaseResourcesOnOpenFailure` already documents as intentional.
- Not changed: `createHeaderPageIfMissing()` still runs during the load, so an empty dictionary page is
  committed at version 0 before the WAL replay. `UnflushedDictionaryRecoveryTest` shows the replay
  handles it (a zero-length dictionary file implies its page was never flushed, so the WAL still holds
  the whole version chain starting at 0). Moving the call after the replay would need the schema load
  to be split as well and is out of scope here.
