# Fix #4331 - TransactionManager WAL Version Gap Recovery

## Issue

`TransactionManager.applyChanges(ignoreErrors=true)` silently skips WAL version gaps during crash
recovery. When the WAL page version is more than one ahead of the on-disk page version, the code
logs a WARNING and `continue`s past that page but still applies sibling pages in the same
multi-page transaction. Result: torn transaction on disk. After "successful" recovery all WAL
files are dropped, making the inconsistency permanent and undiagnosable.

## Root Cause

In `checkIntegrity()` (line 275 before fix):
```java
applyChanges(walPositions[lowerTx], Collections.emptyMap(), true); // ignoreErrors=true
```

In `applyChanges`, when `ignoreErrors=true` and a version gap is detected:
```java
if (txPage.currentPageVersion > page.getVersion() + 1) {
    if (!tx.forceApply) {
        LogManager.instance().log(this, Level.WARNING, ...);
        if (ignoreErrors)
            continue;  // skips only THIS page, continues other pages in same tx - torn transaction!
        throw new WALVersionGapException(...);
    }
}
```

The `continue` skips the gapped page but continues applying sibling pages of the same
transaction. After recovery the WAL is unconditionally dropped, making corruption permanent.

## Fix

Changed `checkIntegrity()` to:
1. Pass `ignoreErrors=false` to `applyChanges` - this throws `WALVersionGapException` on any gap,
   stopping the entire transaction replay immediately (no torn transactions).
2. Catch `WALVersionGapException` and log SEVERE.
3. Close WAL files without deleting them (preserve for manual inspection).
4. Break the recovery loop - no more transactions are applied after the gap.
5. Always create a new WAL pool and clear page cache so the database can continue operating.

## Files Changed

- `engine/src/main/java/com/arcadedb/engine/TransactionManager.java`
  - `checkIntegrity()`: uses `ignoreErrors=false`, catches `WALVersionGapException`, preserves WAL

## Tests Written

- `engine/src/test/java/com/arcadedb/engine/WALVersionGapRecoveryTest.java`
  - `applyChangesThrowsOnVersionGap`: verifies `WALVersionGapException` thrown with gap
  - `versionGapPageNotApplied`: verifies gapped page leaves on-disk version unchanged
  - `checkIntegrityPreservesWALOnVersionGap`: integration test - injects a gap WAL file,
    reopens DB, verifies WAL files preserved

## Test Results

- All new tests pass
- `TransactionManagerCloseWALFsyncTest` still passes (no regression)
- `ApplyChangesPartialReplayTest` still passes (no regression)
