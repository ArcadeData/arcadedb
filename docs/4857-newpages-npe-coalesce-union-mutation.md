# Issue #4857: Intermittent "newPages is null" NPE in PostCommandHandler

https://github.com/ArcadeData/arcadedb/issues/4857

## Reported symptom

Under a memory-constrained node (`-Xmx512M`, ZGC generational, `-Darcadedb.profile=low-ram`),
executing a large batch of Gremlin `addE()`/`property()` mutations nested inside
`coalesce()`/`union()` over the HTTP command endpoint, within a single explicit HTTP
transaction session, intermittently fails with:

```
Error on command execution (PostCommandHandler):
Cannot invoke "java.util.Map.containsKey(Object)" because "this.newPages" is null
```

Roughly one failing command per few thousand edges. Not reproducible on an unconstrained
node at the same shape/scale. Flattening the coalesce/union-nested mutation into a
top-level `addE`/`property` statement was reported as a workaround.

## Root cause

This is **not** a Gremlin-specific bug (coalesce/union do not affect it directly) - it's a
cross-thread race between the HTTP session idle-timeout sweep and an in-flight command on
the same session.

- `TransactionContext.newPages` (`engine/src/main/java/com/arcadedb/database/TransactionContext.java:83`)
  is a lazily-initialized `Map<PageId, MutablePage>`, set in `begin()` and nulled out in
  `rollback()` (`TransactionContext.java:270`) before `status` flips to `INACTIVE` in the
  subsequent `reset()` call (line 294).
- `TransactionContext.getPageToModify(BasePage)` (`TransactionContext.java:440-455`) only
  guards on `isActive()` (`status != INACTIVE`) and then unconditionally calls
  `newPages.containsKey(pageId)` at line 447 with no null check. This is the exact NPE site,
  reached via `LocalBucket.createRecordInternal()` -> `database.getTransaction().getPageToModify(foundPage)`
  when a new edge/vertex record is appended into an existing page.
- `HttpSessionManager` (`server/src/main/java/com/arcadedb/server/http/HttpSessionManager.java`)
  runs a background `java.util.Timer` that calls `checkSessionsValidity()` every
  `arcadedb.server.http.sessionExpireTimeout` (default **5 seconds**,
  `GlobalConfiguration.SERVER_HTTP_SESSION_EXPIRE_TIMEOUT`). For any session whose
  `elapsedFromLastUpdate() > transactionTimeoutInMs`, it calls `session.cancel()`, which
  calls `transaction.rollback()` directly.
- `HttpSession.execute()` (`server/src/main/java/com/arcadedb/server/http/HttpSession.java`)
  serializes command execution on a session via a `ReentrantLock`, and stamps `lastUpdate`
  **before** running the callback, refreshing it again only **after** the callback finishes.
  So a command that runs longer than the session timeout looks idle to the timer for its
  entire duration.
- Critically, `HttpSession.cancel()` (pre-fix) never acquired that `ReentrantLock`. So the
  background Timer thread could call `transaction.rollback()` - nulling `newPages` - while
  the worker thread executing the HTTP command was still inside the locked region of
  `execute()`, deep inside `LocalBucket.createRecordInternal()` mutating the same
  `TransactionContext`. The worker's `isActive()` check still returned `true` (status flips
  to `INACTIVE` only after `newPages` is nulled), so it proceeded to dereference the
  just-nulled `newPages` -> NPE.

This matches every reported symptom:
- **Memory-pressure/GC correlated**: `-Xmx512M` + ZGC + low-ram profile make a ~3900-edge
  batch (125 coalesce/union branches per command) far more likely to exceed the 5-second
  default session timeout via GC pauses and page eviction/compaction overhead.
- **Intermittent, "one failing command per few thousand edges"**: only commands whose
  wall-clock time straddles the 5-second timer tick get raced.
- **Not reproducible on an unconstrained node at the same scale**: the command finishes
  comfortably inside 5 seconds there, so the timer never observes a stale session mid-command.
- **Workaround (flatten coalesce/union into separate top-level statements) "fixes" it**: not
  because coalesce/union is special, but because splitting one slow command into many fast
  ones keeps each `/command` call's duration under the session timeout, refreshing
  `lastUpdate` often enough that the sweep never fires mid-command.

## Fix

`HttpSession.cancel()`'s timeout-sweep caller now goes through a non-blocking variant
(`cancelIfIdle()`) that first attempts to acquire the same `ReentrantLock` used by
`execute()`. If the lock is held (a command is actively executing on this session), the
sweep leaves the session alone for this tick instead of rolling back underneath the
in-flight command; the session is re-evaluated on the next tick once the command finishes
and refreshes `lastUpdate`. The explicit `cancel()` path (client-initiated `/rollback`,
`close()`, and server shutdown) now also acquires the lock (bounded wait), closing the same
race for those callers - the lock is reentrant, so it's a no-op when the caller already
holds it (e.g. `execute()`'s own catch-block cleanup).

`HttpSessionManager.checkSessionsValidity()` only removes a session from the map when the
lock was actually acquired (i.e. the session was genuinely idle - either its transaction
was rolled back or was already inactive), so a busy session is retried on a later tick
instead of losing its session id mid-command.

## Files changed

- `server/src/main/java/com/arcadedb/server/http/HttpSession.java`
- `server/src/main/java/com/arcadedb/server/http/HttpSessionManager.java`

## Tests

- `server/src/test/java/com/arcadedb/server/http/HttpSessionTimeoutRaceTest.java` (new) -
  reproduces the race directly against `HttpSession`/`HttpSessionManager` without needing
  HTTP/Gremlin: starts a long-running command on a session (holding `execute()`'s lock) and
  concurrently drives `checkSessionsValidity()` past the timeout, asserting the transaction
  is not rolled back mid-command and no NPE occurs; also asserts a genuinely idle session is
  still cancelled and removed by the sweep.

## Verification

- `HttpSessionTimeoutRaceTest` (new): confirmed it fails against the pre-fix code
  (`timeoutSweepDoesNotCancelSessionWithInFlightCommand` - expected 0 sessions expired
  while a command is in-flight, got 1) and passes against the fix, stable across 5
  consecutive runs.
- `HttpAuthSessionManagerTest` (10/10), `AutoCommitParameterTest` (7/7),
  `Issue4141SessionManagementIT` (4/4), `HTTPTransactionIT` (3/3): all pass unchanged,
  confirming no regression to session lifecycle/timeout behavior.
- `mvn -pl server -am install -DskipTests`: compiles clean.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/4858

## Review cycles

### Cycle 1 - commit `61455eb6`

Reviewed by `claude[bot]` and `gemini-code-assist`. Both independently converged on the same
transaction-leak bug in `HttpSession.close()` (bounded 5s `cancel()` timeout + session already
untracked = leaked transaction if a command runs past 5s), and gemini's inline suggestion matched
claude's suggested fix exactly (`lock.lockInterruptibly()`). Claude also flagged a residual,
lower-severity race: `lastUpdate` refreshed after `unlock()` in `execute()`, leaving a narrow
window for the sweep to prematurely roll back a transaction whose command just finished.

Applied both fixes (see `docs/review-deferred-61455eb6.md` for the full disposition, including
three items both reviewers explicitly framed as follow-up/out-of-scope and left unaddressed):
- `HttpSession.cancel()` switched from bounded `tryLock(5s)` to unbounded `lockInterruptibly()`.
- `HttpSession.execute()`'s post-callback `lastUpdate` refresh moved inside the locked region.
- New `@Tag("slow")` regression test `closeWaitsForInFlightCommandThenRollsBackInsteadOfLeaking`,
  confirmed to fail against the pre-fix bounded-timeout code and pass against the fix.
- Full `HttpSessionTimeoutRaceTest` suite (4 tests) stable across 3 reruns; `HttpAuthSessionManagerTest`
  (10/10), `AutoCommitParameterTest` (7/7), `Issue4141SessionManagementIT` (4/4), `HTTPTransactionIT`
  (3/3) all pass unchanged.

### Cycle 2 - commit `aa96add5`

Pushed the cycle-1 fixes. Polled for a re-review on this commit for two consecutive windows
(~15 min then ~10 min). The "Claude Code Review" GitHub Actions workflow triggered on the push
(`pull_request: synchronize`) but remained `in_progress` for the full polling window without
posting a new issue comment; `gemini-code-assist` posted no new top-level review either (its one
visible inline comment on this SHA was GitHub re-pointing the *cycle-1* comment's `commit_id` to
track the new head - same `id`/`created_at` as before, not a new finding; confirmed the flagged
line already contains the fix).

No new actionable feedback surfaced in this cycle - stopping the automated review loop here per
the per-iteration timeout policy rather than waiting indefinitely.

## Status

**Final state: timeout** (cycle 2, waiting on bot re-review). One productive review cycle
completed with real feedback applied (see Cycle 1). PR left open for the developer to check for
a delayed cycle-2 review and merge when ready.
