# Review notes deferred from cycle 2 (base commit 7a3eabfd0d)

`claude` reviewed commit `7a3eabfd0d9f23d600ac4be9c7236c84c83ba713`. One actionable finding was applied
directly (see the commit on top of this note); the "main concern" below was deferred to a follow-up issue
rather than fixed in this review cycle.

## Deferred: 503 mapping for `DatabaseIsClosedException` is broader than the resync race it fixes

`claude`'s review pointed out that `AbstractServerHttpHandler.sendMappedErrorResponse`'s new arm maps
*every* `DatabaseIsClosedException` to 503, not only the HA snapshot-reinstall resync race this PR targets
(#6770). `LocalDatabase.checkDatabaseIsOpen()` throws the same exception type for any operation on a
closed database, including a concurrent `DROP DATABASE`/close admin action - which is a *permanent* close,
not transient. Verified against the code: `PostServerCommandHandler.dropDatabase()`'s non-HA branch and
`closeDatabase()` do not hold `databasesLock` across their close/remove sequence the way
`SnapshotInstaller.swapAndReopen` deliberately does, so an in-flight request can race a permanent close the
same way it races a resync. The client's automatic retry then re-resolves the database with
`allowLoad=false`, which throws `DatabaseOperationException` once the registry entry is gone - unmapped, so
it falls to the generic 500 this PR did not change for that path.

**Why deferred, not fixed here:** distinguishing "transient resync" from "permanent close" needs a signal
that does not exist yet. `ArcadeDBServer.isSnapshotInstallInProgress()` looked like a candidate but covers a
different, whole-server snapshot-install condition (already consulted at request entry in this same file,
line ~252) - reusing it for the per-database resync-reinstall case (#5977 pattern) needs verification it is
semantically correct, which is more design work than a review-response cycle should absorb unattended. Filed
as https://github.com/ArcadeData/arcadedb/issues/6778 with both candidate approaches from the review.

## Applied, not deferred

- Extracted `sendRetryableResponse(exchange, throwable)` to remove the near-duplicate log+503-response code
  between the `NeedRetryException` and `DatabaseIsClosedException` arms (claude, "Minor" note, cycle 2).
- Added a pointer comment at the `DatabaseIsClosedException` arm referencing issue #6778.
