# Issue #4857: Intermittent "newPages is null" NPE in PostCommandHandler

https://github.com/ArcadeData/arcadedb/issues/4857

## Symptom

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
and refreshes `lastUpdate`.

The explicit `cancel()` path (client-initiated `/rollback`, `close()`, and server shutdown)
now blocks unboundedly on the same lock (via a polling `tryLock` loop, functionally
`lockInterruptibly()`) instead of giving up after a bounded wait: `close()` untracks the
session from `HttpSessionManager` before calling `cancel()`, so a bounded wait that gave up
while a command was still in-flight would leak the transaction - nothing else would ever roll
it back afterward. The wait logs a throttled WARNING past 5s so a stuck server shutdown (which
calls `cancel()` per session) is diagnosable instead of a silent hang.
`HttpSessionManager.close()` also now snapshots the session list under the write lock before
iterating, since `cancel()` can block for a while per session, widening the pre-existing window
for a concurrent `checkSessionsValidity()` tick to mutate the same map underneath a live
iterator.

`execute()`'s own catch-block rollback calls a lock-free `rollbackIfActive()` helper directly
instead of re-entering through `cancel()`: `ReentrantLock.lockInterruptibly()` checks the
calling thread's interrupt status before its reentrant fast path, so if the worker thread's
interrupt flag happened to be set when the command failed, going through `cancel()` here would
throw `InterruptedException` and silently skip the rollback.

`HttpSessionManager.checkSessionsValidity()` only removes a session from the map when the
lock was actually acquired (i.e. the session was genuinely idle - either its transaction
was rolled back or was already inactive), so a busy session is retried on a later tick
instead of losing its session id mid-command.

Also closed a narrower race: `execute()` previously refreshed `lastUpdate` after releasing
the lock, leaving a window where the sweep could `tryLock()` successfully and see a stale
timestamp, prematurely rolling back a transaction whose command had just cleanly finished.
The refresh now happens inside the locked region.

## Tests

Added to `server/src/test/java/com/arcadedb/server/http/HttpSessionTimeoutRaceTest.java` (new
file), constructed against a real embedded `TransactionContext`/`HttpSession` without needing
HTTP, Gremlin, or a running `ArcadeDBServer` - the `ServerSecurityUser` is a real instance too
(its `equals()`/`getName()` never touch the `ArcadeDBServer` field the constructor takes, so a
`null` server is passed rather than mocking):

- `timeoutSweepDoesNotCancelSessionWithInFlightCommand` - starts a long-running command
  (holding `execute()`'s lock) and drives `checkSessionsValidity()` past the timeout,
  asserting the transaction is not rolled back mid-command.
- `timeoutSweepStillCancelsAGenuinelyIdleSession` / `timeoutSweepRemovesASessionWithAnAlreadyInactiveTransaction` -
  confirm idle and stale sessions are still correctly reaped by the sweep.
- `closeWaitsForInFlightCommandThenRollsBackInsteadOfLeaking` (`@Tag("slow")`, holds a command
  past the 5s bound) - confirms `close()` eventually rolls back instead of leaking.
- `executeRollsBackOnFailureEvenWhenInterruptFlagIsSet` - confirms the catch-block rollback
  still runs when the worker thread's interrupt flag is set at failure time.

## Test results

- `HttpSessionTimeoutRaceTest` (5 tests): each new test confirmed to fail against the
  corresponding pre-fix code and pass against the fix; stable across repeated reruns.
- `HttpAuthSessionManagerTest` (10/10), `AutoCommitParameterTest` (7/7),
  `Issue4141SessionManagementIT` (4/4), `HTTPTransactionIT` (3/3): all pass unchanged,
  confirming no regression to session lifecycle/timeout behavior.
- `mvn -pl server -am install -DskipTests`: compiles clean.

## Impact analysis

- The race is specific to the HTTP session layer (`HttpSession`/`HttpSessionManager`) and
  applies to any command - SQL, Cypher, Gremlin, or otherwise - that runs longer than
  `arcadedb.server.http.sessionExpireTimeout` (default 5s) inside an explicit session; the
  reported Gremlin coalesce/union shape is just what happened to make one command slow
  enough to trigger it.
- Fix is localized to `HttpSession`/`HttpSessionManager`; no engine-layer (`TransactionContext`)
  changes were needed since the invariant it assumes (`isActive()` implies `newPages != null`)
  is restored by preventing the concurrent rollback, not by adding a null guard downstream.
- `HttpSessionManager.close()` (server shutdown) and `HttpSession.close()` now block on an
  in-flight command until it finishes before rolling back, rather than racing or giving up
  after 5s - an intentional behavior change (graceful wait instead of a silent leak or a
  data-corrupting race).
