# Issue #4332 - TransactionManager.close(false) deletes WAL without fsyncing data pages

## Root Cause

`LocalDatabase.internalClose()` calls `fileManager.close()` (which closes all `FileChannel` handles)
and then `transactionManager.close(drop)` (which deletes `.wal` files), but never calls
`channel.force(true)` on any data file.

`PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database)` is called before
`fileManager.close()`, so all pending pages are written to the OS buffer cache. However, OS
`write()` does not guarantee persistence through a power loss. Without `channel.force(true)` (fsync)
before WAL deletion, a power cut between WAL deletion and the OS cache flush permanently loses
committed transactions.

## Changes

### `engine/.../engine/PaginatedComponentFile.java`
- Added `force(boolean metaData)` method: calls `channel.force(metaData)` on the open channel.

### `engine/.../engine/FileManager.java`
- Added `syncFiles()` method: iterates all registered `PaginatedComponentFile` instances and calls
  `force(true)` on each, logging warnings for individual failures. Errors on one file do not
  prevent syncing the rest.

### `engine/.../database/LocalDatabase.java`
- In `internalClose()`, added a call to `fileManager.syncFiles()` immediately after
  `PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(this)` when `!drop`. This ensures all data
  files are on physical storage before WAL files are deleted.

## Test

`engine/.../engine/TransactionManagerCloseWALFsyncTest.java`

- `noWalFilesAfterCleanClose`: commits records, closes database, asserts no `.wal` files remain.
- `dataReadableAfterReopenWithoutWALRecovery`: commits records, closes, manually deletes any
  leftover WAL files, reopens, and asserts all committed records are readable - proving data
  pages were persisted independently of the WAL.
