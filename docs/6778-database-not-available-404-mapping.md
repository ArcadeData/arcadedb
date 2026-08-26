# Issue #6778: 503 mapping for `DatabaseIsClosedException` is broader than the resync race it fixes

## Root cause

Follow-up from the #6770 / PR #6776 review loop.

`AbstractServerHttpHandler.sendMappedErrorResponse` maps every `DatabaseIsClosedException` to a
retryable 503 (added in #6776 for the HA snapshot-reinstall resync race). That mapping is correct for
the resync case, but `LocalDatabase.checkDatabaseIsOpen()` throws the same exception type for **any**
operation on a closed database - including one closed permanently by a concurrent `DROP DATABASE` /
`CLOSE DATABASE`.

Net effect for a dropped/closed database racing an in-flight request:

1. The in-flight request hits `DatabaseIsClosedException` -> 503 ("retry me").
2. The client's automatic retry (`RemoteHttpComponent`) re-resolves the database with `allowLoad=false`
   (`DatabaseAbstractHandler.execute`, line ~74).
3. Once the registry entry is gone, `ArcadeDBServer.getDatabase()` throws
   `DatabaseOperationException("Database '...' is not available")` (line ~1011).
4. `sendMappedErrorResponse` has no arm for that message, so it falls through to the generic 500
   "Internal error" - opaque to the client, and not the accurate 404 the situation calls for.

## Approach taken

Of the two candidates the issue records, this implements #2 (narrower blast radius, no new
whole-server/resync-tracking state needed): a dedicated `DatabaseNotAvailableException` (a
`DatabaseOperationException` subtype) is thrown from the single call site instead of the generic
type, and `sendMappedErrorResponse` gets an explicit arm mapping it to 404 "Database not found".

Approach #1 (scoping the 503 itself to the resync condition) is intentionally **not** attempted here:
it needs a resync-vs-permanent-close signal that does not exist yet, and the issue's own text flags
`ArcadeDBServer.isSnapshotInstallInProgress()` as the wrong tool for it (whole-server condition, not
per-database). The wasted 503-then-retry round trip for the permanent-close race therefore still
happens - it now just ends in an accurate 404 instead of a generic 500.

## Changes

- `engine/src/main/java/com/arcadedb/exception/DatabaseNotAvailableException.java` (new): narrow
  `DatabaseOperationException` subtype for "no open database handle when the caller forbade loading one".
- `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`: `getDatabase(..., allowLoad=false)`
  throws the new type instead of the generic `DatabaseOperationException`.
- `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java`: new arm in
  `sendMappedErrorResponse` mapping `DatabaseNotAvailableException` to 404 "Database not found"; the
  existing #6778 pointer comment on the `DatabaseIsClosedException` arm is updated to reflect that the
  generic-500 tail is now closed.
- `server/src/test/java/com/arcadedb/server/http/handler/Issue6201ErrorStatusParityTest.java`: new
  `MAPPED_FAILURES` row (bare / `TransactionException`-wrapped / `CommandExecutionException`-wrapped
  parity) plus a dedicated regression test naming the #6778 scenario.

## Test plan

- `Issue6201ErrorStatusParityTest` (new row + new test) - confirmed red before the handler arm was
  added (fell through to the generic 500 "Internal error" arm), green after.
- Full `server` module `http.handler` package test run for regressions.
