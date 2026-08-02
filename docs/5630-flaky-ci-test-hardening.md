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
| `Bolt5002RoutingTableIT.neo4jSchemeRoutesReadsAndWrites` | expected `1` got `0`, plus `DatabaseAreNotIdentical: Types: DB1 6 <> DB2 5` | **not a flake - real divergence, reverted** |
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
  midway. What makes the ratio hold where an absolute budget does not is that the dominant term in both
  measurements is the same server-side SLEEP, a wall-clock wait rather than CPU work and so largely
  load-independent - not that load inflates the two measurements proportionally.
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

### `Bolt5002RoutingTableIT` - NOT a flake. Reverted from this change.

This test was attacked as an "unsettled replication" case, on the theory that `waitForAllServers()`
returns before the applied schema entry is visible and a bounded poll would settle it. **That diagnosis
was wrong, and the issue's classification of this test is wrong too.**

A poll was added over every started node, waiting up to 60 s for the `Bolt5002Route` type and its single
record. CI answered unambiguously:

```
org.awaitility.core.ConditionTimeoutException:
  [server 1 must have replicated type Bolt5002Route]
  Expecting value to be true but was false within 1 minutes
  Suppressed: DatabaseComparator$DatabaseAreNotIdentical: Types: DB1 6 <> DB2 5
```

Server 1 never received the type at all - not within 60 s, and the teardown comparison independently
reported the same divergence the original issue recorded as a suppressed error. State that does not
settle in 60 s is not lag, and no amount of polling fixes it. There is a real replication defect behind
this test.

The change was therefore **reverted**; the file is byte-identical to `main` on this branch. The remaining
value of the experiment is diagnostic: it converts "expected 1 got 0" into evidence that a specific node
never receives a schema entry written through the Cypher-over-Bolt path.

**Follow-up:** needs its own issue and investigation. A plausible starting point - unverified, stated as a
hypothesis only - is the `#5492`/`#5655` family: code that commits while holding the inner `LocalDatabase`
rather than the wrapped replicated instance applies locally and replicates nothing. The Bolt write path
should be checked against `getWrappedDatabaseInstance()`. Do not treat that as a diagnosis; it is only
where to look first.

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

**The IT classes were not run locally.** Ports 2480/2481 were held for the duration of this work by an
ArcadeDB server running under the developer's IntelliJ debugger, which `BaseGraphServerTest` needs to
bind. CI was the verification gate, by the developer's decision.

### CI result (run 30706875826, commit `037e32702`)

The `integration-tests` job reported `Run Integration Tests with Coverage: success` and
`IT Tests Reporter: failure` - exactly the trap the issue documents. The Maven step runs with
`--fail-never`, so its success is unconditional and carries no information; the reporter is the gate.

| class | result | elapsed |
|---|---|---|
| `Issue3122AsyncParallelCommandsIT` | 3/3 pass | 8.1 s |
| `HttpRedMetricsIT` | 3/3 pass | 0.5 s |
| `Issue5470BatchStreamStallIT` | 3/3 pass | 31.0 s |
| `Bolt5002RoutingTableIT` | 1 error - real defect, reverted | 110.6 s |

The three hardening fixes are confirmed by the authoritative signal. Two incidental confirmations: the
0.5 s on `HttpRedMetricsIT` supports having declined `@Tag("slow")` for it, and the 31 s on `Issue5470`
is the runtime cost the reviews flagged.

### Proof that the rewritten assertion can still fail - DONE

A green CI run does not establish this: a test that can no longer fail is also green. The largest
rewrite in this change is the completion-gap assertion, and it was verified directly.

`databaseAsyncCommandsRunInParallel` drives `database.async()`, which lives in the engine and needs no
server, so the occupied port was not an obstacle. A throwaway harness ran the exact assertion logic
against a real embedded database at both parallel levels:

| `parallelLevel` | measured gap | `gap < SLEEP_DURATION` |
|---|---|---|
| 1 (sequential) | 2001 ms | **fails** - correct |
| 2 (parallel) | 0 ms | passes - correct |

The sequential result is not merely empirical. With one worker the second command's 2000 ms sleep
cannot begin until the first has completed, so the gap has a hard floor of `SLEEP_DURATION`; the
assertion is `isLessThan(SLEEP_DURATION)`, which fails at the boundary as well as above it. The
parallel side has the whole of `SLEEP_DURATION` as margin. The assertion discriminates.

The harness was deleted after the run; it is not part of this branch.

### Remaining verification gap

`HttpRedMetricsIT`, `Issue5470BatchStreamStallIT` and `Bolt5002RoutingTableIT` were not exercised
against their failure modes. Their assertions are unchanged in substance - the polls wrap conditions
that were already being asserted, and the timeout constants were scaled without altering the ordering
under test - so the risk is materially lower than for the Issue3122 rewrite. CI green plus the review
spot-check is the evidence for those three.

## Review cycle 1 (`ec04ad3fe`)

Bot review raised five points. Outcomes:

1. **Accepted - `Issue5470` client `soTimeout` equalled the streaming budget.** The comment claimed the
   client timeout outlives the budget so that a server which wrongly waits is caught by the elapsed-time
   assertion, but the value was set *equal* to it, making the two race. Introduced
   `CLIENT_SO_TIMEOUT_MS = STREAMING_BUDGET_MS + 30_000` so the assertion, not a `SocketTimeoutException`,
   is what reports the failure. Correct catch.
2. **Declined - widening `SEQUENTIAL_COST_FRACTION` from 1.5 to 1.6-1.7.** The suggestion treats the
   threshold as trading only against false failures, but the sequential ratio is
   `(2*SLEEP + o) / (SLEEP + o)` for per-request overhead `o`, which *decreases* toward 1.0 as `o` grows -
   it is 2.0 only when overhead is negligible and already 1.5 once overhead reaches one SLEEP. Raising the
   constant therefore buys false-failure margin by surrendering false-pass margin, and a regression guard
   that silently passes is the worse failure. Kept at 1.5, with the reasoning recorded in the constant's
   javadoc so it is not re-litigated.
   **However**, the review was pointing at something real next to it: the ratio is meaningless if the
   baseline is degenerate. Added an assertion that `singleMs >= SLEEP_DURATION`, which closes the case
   where `waitCompletion` returns before the command is picked up and the threshold collapses to the HTTP
   round trip. That is the cheaper protection for the property the suggestion was reaching for.
3. **Point 5 (treat the negative check as blocking, not a follow-up)** - agreed and discharged; see the
   proof table above.
4. Points 3 and 4 were confirmations of existing reasoning; no change needed.

## Review cycle 2 (`3ff6ba5f5`)

No blocking points. Outcomes:

1. **No change - residual flakiness in `assertCommandsOverlapped`.** Correctly observed: if a runner starves
   the second worker for a whole SLEEP, two genuinely parallel commands can still complete `>= SLEEP` apart.
   The reviewer recommends keeping it anyway and that is right - the old assertion failed under *any* uniform
   slowdown, this one fails only under differential starvation of one worker. Strict improvement, residual
   risk accepted knowingly.
2. **Confirmed and documented - the baseline guard's dependency on enqueue-before-202.** The review flagged
   that `singleMs >= SLEEP_DURATION` is only sound if the command is enqueued before the 202 the client
   observes, otherwise the guard added in cycle 1 would produce *false failures*. Verified in the source
   rather than assumed: `PostCommandHandler` line 214-217 calls `executeCommandAsync(...)` - which enqueues
   synchronously via `database.async().command(...)` - and only then constructs the 202. The ordering holds;
   a comment now records it at the assertion.
3. **Accepted for `Issue3122` only - `@Tag("slow")`.** This class now waits out two SLEEPs in the HTTP test
   where it previously waited one, so the change itself made it slower and the tag is earned under the
   CLAUDE.md convention for multi-second regression tests. Declined for `HttpRedMetricsIT`, which issues 50
   local HTTP requests and is sub-second unless it fails.
   Worth recording: `excludedGroups` is **commented out** in the root POM (line 216), so no tag filtering is
   currently active. Tagging changes nothing about what CI runs today - it is classification, which is also
   why it carries no risk of silencing the very test being de-flaked. If tag filtering is ever re-enabled,
   `slow` must stay inside the IT job or this and the two already-tagged classes would stop running.
4. **Answered - which CI signal is authoritative.** Verified against `.github/workflows/mvn-test.yml`: the
   integration job runs `./mvnw verify ... --fail-never`, so `Run Integration Tests with Coverage: success`
   is unconditional and carries no information. The gate is the `IT Tests Reporter` step (dorny/test-reporter
   over `**/failsafe-reports/TEST*.xml`). All four classes land in that job - it excludes only
   `e2e,load-tests,e2e-ha,ha-raft`, and `Bolt5002RoutingTableIT` runs in the `bolt` module despite extending
   `BaseRaftHATest`. Do not read this PR as verified until that reporter is green.

## Review cycle 3 (`615206dd7`)

1. **Declined - "trim or drop this doc; I don't see an existing `docs/NNNN-*.md` per-issue convention".**
   The premise is factually wrong, and cycle 2's review asserted the opposite about the same file, so it
   was checked against the repository rather than split the difference: `git ls-files docs` matches **68**
   files of the form `docs/NNNN-*.md`, and **31** of those carry review-cycle sections of exactly the kind
   this one has. Both the file and its cycle notes are house style. Keeping them.
2. **Accepted - the stated reason the HTTP ratio is robust was imprecise.** The doc and the class javadoc
   said the ratio survives because "load inflates both measurements together". That is not the mechanism:
   the dominant term in both is the server-side `SLEEP`, a wall-clock wait rather than CPU work, so it is
   largely load-*independent*, and it is that shared constant dominating both measurements that pins the
   parallel ratio near 1.0. Corrected in both places. The 1.5 threshold is unaffected - and note this
   sharpens rather than contradicts the cycle-1 argument for not raising it, which turned on the *overhead*
   term growing.
3. Points 2, 3 and 5 (runtime cost, residual differential-starvation flake, the Bolt helper) were
   confirmations of decisions already recorded above; no change.
