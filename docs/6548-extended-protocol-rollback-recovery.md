# Issue #6548: Postgres extended-protocol ROLLBACK recovery never reaches the new dispatch

## Root cause

`PostgresNetworkExecutor.parseCommand()` starts with:

```java
if (errorInTransaction)
  return;
```

This early return happens **before** the query text is inspected at all. Once `errorInTransaction` is
set (a statement erred inside an explicit `BEGIN` block), a client that sends an explicit `ROLLBACK` (or
`COMMIT`/`END`, which real Postgres treats identically while aborted) over the **extended** query protocol
to recover from the aborted transaction never reaches the BEGIN/COMMIT/ROLLBACK dispatch added by #6546 for
the simple-query path (`queryCommand()`) - `Parse` just silently drops the message (no `ParseComplete`, no
error).

When `Sync` eventually arrives, its `errorInTransaction` branch (`syncCommand()`) does call
`database.rollback()` and clears `errorInTransaction`, but it never touches `explicitTransactionStarted`.
Since `writeReadyForQueryMessage()` checks `errorInTransaction` first and `explicitTransactionStarted`
second, the client ends up back at `ReadyForQuery` status `'T'` instead of `'I'` - the same "wedged forever"
symptom #6543 fixed, just reached via the aborted-transaction path.

## Fix

`parseCommand()` now recognizes a BEGIN/COMMIT/ROLLBACK-family statement (via the existing
`isTransactionEndStatement`/`isCommitStatement`/`isRollbackStatement` helpers) **before** falling through to
the unconditional early return while `errorInTransaction` is set. Mirroring `queryCommand()`'s
aborted-transaction dispatch (#6457/#6542):

- If the active transaction is still active, roll it back.
- Clear both `explicitTransactionStarted` and `errorInTransaction`.
- Real Postgres treats a COMMIT/END of an aborted transaction the same as ROLLBACK (there is nothing left to
  commit), so the portal's query text is overridden to `"ROLLBACK"` before `setEmptyResultSet()` runs - this
  makes `executeCommand()`'s later `writeCommandComplete()` report the `ROLLBACK` tag regardless of which of
  the three keywords the client actually sent, exactly like the simple-query dispatch already does.
- The portal is stored and `ParseComplete` is written, so the following Bind/Execute/Sync round trip runs
  through the ordinary (non-aborted) code paths - no changes were needed in `bindCommand()`/`executeCommand()`
  themselves, since by the time they run, `errorInTransaction` is already false.

Every other statement (anything that is not a transaction-end statement) still falls through to the silent
`return` - `bindCommand()`'s own `errorInTransaction` check (issue #6545) is what turns an attempt to Bind
one of those into an `ErrorResponse`.

## Files changed

- `postgresw/src/main/java/com/arcadedb/postgres/PostgresNetworkExecutor.java` - `parseCommand()`.

## Review follow-up: trailing ';' / whitespace

`isCommitStatement()`/`isRollbackStatement()` match by exact string equality, so `abortedUpperCaseText` has to
be trimmed and stripped of a trailing `;` before comparison - the same normalization `queryCommand()` applies
to its own `queryText` (`queryText = readString().trim(); if (queryText.endsWith(";")) queryText = ...`)
before its aborted-transaction check runs. Without it, a client that sends `"ROLLBACK;"` (a real Postgres
statement terminator many drivers append) over the extended protocol while aborted falls through to the
silent `return`, reproducing this issue's "wedged forever" symptom via a trailing semicolon instead of via
the missing dispatch. Caught by the Claude Code review on this PR.

Fixed narrowly: a local `abortedText` is trimmed and semicolon-stripped before uppercasing, only for the
match this PR's new branch performs. `portal.query` itself is left untouched (the matched branch overwrites
it outright with `"ROLLBACK"`; the unmatched branch discards the portal). The non-aborted BEGIN/COMMIT/
ROLLBACK dispatch a few lines below (`upperCaseText` at parseCommand()'s top) has the identical gap, but that
variable is also read by several unrelated checks (SET/SHOW/SAVEPOINT/system-query), so normalizing it there
is a materially larger, riskier change outside this PR's scope - left as a pre-existing issue, not introduced
here, callable out as a fast follow if desired.

## Tests

- `postgresw/src/test/java/com/arcadedb/postgres/Issue6548AbortedTransactionRollbackExtendedProtocolIT.java` (new)
  - Reproduces the exact wedge: `BEGIN` (extended) -> a malformed `Parse` aborts the transaction -> `ROLLBACK`
    sent as its own Parse/Bind/Execute/Sync must clear both `errorInTransaction` and
    `explicitTransactionStarted`, reporting status `'I'`, not `'T'`.
  - Covers `COMMIT`/`END` while aborted producing the `ROLLBACK` command tag, per real Postgres semantics.
  - Covers `"ROLLBACK;"`, `" ROLLBACK "`, and `" ROLLBACK; "` while aborted, locking in the trim/semicolon-strip
    fix above - verified red against the code before that fix (protocol desync, same signature as the
    original bug), green after.
  - Confirms the session is fully usable again after recovery (`SELECT 1` succeeds with no `ErrorResponse`).

## Verification

- **Red/green on the new test.** Ran `Issue6548AbortedTransactionRollbackExtendedProtocolIT` against the
  unfixed code first (`git stash` on the source change only): both tests failed. The `ROLLBACK` scenario
  failed with a protocol desync (`EOFException` client-side, server-side `PostgresProtocolException:
  Unexpected message type`) rather than a clean assertion failure - the unfixed `Parse` drops the message
  without draining nothing extra, but the *previous* portal at the unnamed-portal slot was already removed
  by the prior statement's `Execute` (which always removes its portal - `getPortal(name, true)`), so `Bind`
  hits the pre-existing "portal not found" fast path in `bindCommand()`, which returns without consuming the
  rest of the Bind message body, desyncing the wire for anything pipelined after it. This is a sharper
  real-world symptom than the issue's own description (connection actively closed, not just wedged at status
  `'T'`) for any client that re-Parses the unnamed portal per statement. Re-ran after restoring the fix
  (`git stash pop`): both tests passed.
- **Full aborted-transaction/rollback regression group**, fix applied: `Issue6543RollbackExtendedProtocolIT`,
  `Issue6545AbortedTransactionExtendedQueryIT`, `Issue6457AbortedTransactionSimpleQueryIT`,
  `Issue6548AbortedTransactionRollbackExtendedProtocolIT` - 9/9 passed.
- **Full `postgresw` integration-test suite** (all 18 `*IT` classes, dependency-module unit tests skipped to
  keep the run scoped): 19/19 of the tests that ran passed; one class, `PostgresProtocolIT`, crashed its
  forked JVM (`SurefireBooterForkException`, exit 143) both in the full run and when re-run in isolation.
  The host was under severe memory pressure at the time (415MB free of 35GB used, ~15GB in the zram
  compressor, multiple concurrent `mvn`/`java` processes from other sessions on the same machine) - the fork
  could not start at all, not merely fail a test, and the class is unrelated to this change (a general JDBC
  connectivity IT, not aborted-transaction/extended-protocol logic). Treated as a pre-existing environmental
  flake, not a regression from this fix.
- Full reactor compile (`mvn -o -pl postgresw -am clean test-compile`) passed with no new warnings beyond
  pre-existing ones in this file.
