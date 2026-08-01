# Issue #5630 - Flaky CI tests: wall-clock budgets, short read timeouts, and unsettled counters

Issue: https://github.com/ArcadeData/arcadedb/issues/5630

## Problem

Six ITs failed across CI runs of #5612. Every one passed when re-run in isolation on the same commit,
and none of them touches anything that PR changed. The issue groups them into three shapes, each with
its own fix:

| test | how it failed | shape |
|---|---|---|
| `Issue3122AsyncParallelCommandsIT.databaseAsyncCommandsRunInParallel` | 26831 ms against a 3000 ms budget | wall-clock budget |
| `Issue5470BatchStreamStallIT` | `Read timed out after 2000 milliseconds` | short read timeout |
| `HttpRedMetricsIT.unmatchedUrisCollapseToBoundedPathTag` | counter read `49`, wanted `>= 50` | unsettled counter |
| `Bolt5002RoutingTableIT.neo4jSchemeRoutesReadsAndWrites` | expected `1` got `0`, plus `DatabaseAreNotIdentical: Types: DB1 6 <> DB2 5` | unsettled replication |
| `Issue5410AbandonedTicketReleaseIT` | HA raft, 44 s | not addressed - see below |
| `Issue5569SlotMergeDeleteRaftIT` | HA raft, 13 s, also fails on main | not addressed - see below |

None of these is a product defect. In every case the assertion encodes an assumption about *timing*
that holds on an unloaded developer machine and does not hold on a shared CI runner. The fix is to
assert the property actually under test rather than a wall-clock proxy for it, without weakening what
the test detects.

## Root cause per test

### `HttpRedMetricsIT` - the meter is recorded after the response is sent

`AbstractServerHttpHandler.handleRequest` captures `httpStartNanos` at entry and records the
`arcadedb.http.requests` timer in its `finally` block, which runs after the exchange has been
completed. A client that has already read the response code is therefore racing the meter update.
With 50 sequential requests the last one had not been recorded when the sum was read: 49, not 50.

**Fix:** the positive meter assertions in all three test methods are wrapped in an Awaitility
`untilAsserted` poll (30 s ceiling, 100 ms interval). The negative assertions are deliberately left
outside the poll and moved *after* it:

- `rawUnmatchedMeters` must be zero - polling a negative assertion would pass on the first tick before
  any request was recorded, so it is checked once, after the poll has proven all 50 landed. This is
  strictly stronger than the original ordering, not weaker.
- `rawPathTimer` must be null - same reasoning.

### `Issue3122AsyncParallelCommandsIT` - a wall-clock budget is not a parallelism assertion

All three test methods asserted `totalTime < SLEEP_DURATION * 1.5`, i.e. under 3000 ms. The reported
run took 26831 ms while running perfectly in parallel; the same test takes ~7 s alone on a developer
machine, so the budget was already tighter than an unloaded local run.

**Fix:** no assertion compares elapsed time to a constant any more.

- The two callback-driven tests (`databaseAsyncCommandsRunInParallel`, `simpleSqlAsyncCommandsRunInParallel`)
  now record the instant each command signals completion and assert the two completions are **less than
  one SLEEP apart**. Run sequentially the second command cannot start until the first finishes, so its
  completion is at least `SLEEP_DURATION` later regardless of machine load. The gap is invariant under
  a uniform slowdown; total elapsed time is not.
- `httpAsyncCommandsRunInParallel` has no completion callback, so it measures a **single-command
  baseline back to back with the concurrent pair** on the same server, and asserts the pair costs less
  than 1.5x the baseline. Sequential execution costs ~2x the baseline, parallel ~1x; the threshold sits
  midway and load inflates both measurements together.
- The `latch.await` / `future.get` / `waitCompletion` timeouts were raised to liveness-guard values
  (120 s / 60 s) and are documented as such. They exist so a wedged executor fails rather than hangs;
  they are not performance assertions.

Timestamps are written to an `AtomicLongArray` before `latch.countDown()`, so `await()` happens-after
every write.

### `Issue5470BatchStreamStallIT` - the socket timeout was shorter than CI scheduling noise

The scenario needs the ordering `socket read timeout < injected stall < streaming budget`, so that the
relaxed streaming budget is proven to govern a server-side stall. The absolute values did not matter,
but at 2 s the socket timeout was also shorter than the pauses a loaded runner puts between the
client's writes of the 1.7 MB body, so the server timed the upload out mid-body.

**Fix:** the three values are scaled together, preserving the ordering under test and widening the
margin against runner scheduling by 5x:

| constant | before | after |
|---|---|---|
| `READ_TIMEOUT_MS` (`NETWORK_SOCKET_TIMEOUT`) | 2 000 | 10 000 |
| `STALL_MS` | 5 000 | 20 000 |
| streaming budget (`SERVER_HTTP_STREAMING_READ_TIMEOUT`) | 60 000 | 120 000 |

The client-side `setSoTimeout` in the two raw-socket tests and the `elapsedMs` bound in
`stalledClientIsCutOffAndAnswered` were tied to `STREAMING_BUDGET_MS` rather than left at a bare
`60_000`. This keeps the assertion, not the client socket, as the thing that fails when the server
wrongly waits for the streaming budget.

### `Bolt5002RoutingTableIT` - a routed read hits a follower that has not applied the schema entry

`waitForAllServers()` tracks the Raft applied index, which advances before the applied schema entry is
visible through the database handle the test reads. `executeRead` is routed to a follower, so it saw
zero records. The `DatabaseAreNotIdentical: Types: DB1 6 <> DB2 5` suppressed in the same run is the
same lag surfacing in teardown: a follower still missing the `Bolt5002Route` type.

**Fix:** `awaitRoutedWriteOnEveryNode()` polls every started node until it holds both the type and its
single record before the routed read runs. This settles the teardown comparison as well.

## Not addressed here

`Issue5410AbandonedTicketReleaseIT` and `Issue5569SlotMergeDeleteRaftIT` are left untouched.

Neither matches the three shapes above. The issue records only a duration for each, with no assertion
message identifying what actually failed, and notes that `Issue5569SlotMergeDeleteRaftIT` **also fails
on main** (run 30592955402). That makes it a candidate real defect in the HA/raft area rather than a
test-hardening target - "is this failure mine?" cannot be answered from the recorded evidence. Widening
a timeout in either would risk masking a genuine bug. They need their own investigation with a captured
failure, and should be tracked separately.

## Verification

- `mvn -o -pl server -DskipTests test-compile` - BUILD SUCCESS
- `mvn -o -pl bolt -DskipTests test-compile` - BUILD SUCCESS (also confirms Awaitility is on the test
  classpath in `bolt`; it is declared in the root POM's `<dependencies>`, not `<dependencyManagement>`,
  so every module inherits it and no new dependency was added)

**The four IT classes were not run locally.** Ports 2480/2481 were held for the duration of this work
by an ArcadeDB server running under the developer's IntelliJ debugger, which `BaseGraphServerTest`
needs to bind. CI is the verification gate for this change, by the developer's decision.

When reading that CI run, note what the issue itself records about this repo: the failing step is the
**reporter** (`IT Tests Reporter` / `HA IT Tests Reporter`), not the test step. Maven runs with test
failures ignored, so `Run Integration Tests with Coverage: success` does **not** mean the tests passed.

### Open item: proving the hardened assertions can still fail

A hardening change is only worth anything if the assertion it leaves behind still detects the
regression it guards, and a green CI run does not establish that - a test that can no longer fail is
also green. Two checks are worth running before this is trusted, neither of which CI performs:

1. `Issue3122AsyncParallelCommandsIT` - force `setParallelLevel(1)` so execution is sequential. The
   completion-gap assertion must fail, with a reported gap of at least `SLEEP_DURATION`. This is the
   important one: it is the assertion that was rewritten most substantially.
2. `HttpRedMetricsIT` - the polls are wrapped around assertions that were already there and are
   unchanged in substance, so the risk is lower, but a run with the path-collapsing behaviour reverted
   should still fail on `rawUnmatchedMeters`.

Recorded as an explicit gap rather than left implied.
