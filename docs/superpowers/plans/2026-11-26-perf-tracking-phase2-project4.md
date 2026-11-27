# Performance tracking phase 2, project 4 (open-loop ramp) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `ramp` workload: on the graph OLTP point lookup, arrivals on a fixed schedule (open loop, latency from the
scheduled start, so no coordinated omission), the rate raised step by step until the server breaks, published as one
capacity number `knee_per_s` and the p99 at the knee.

**Architecture:** A pure `OpenLoop` driver (scheduler + client pool + per-step verdict) and a pure `RampSearch` (coarse 25%
steps, then bisection between the last passing and the first failing rate), both unit-tested with a fake clock and a fake
operation; a thin `RampWorkload` that reuses `GraphOltpWorkload`'s dataset and point-lookup query. The runner learns that a
workload may declare its own repetitions and warm-up (a full ramp per repetition is long). One PR.

**Tech Stack:** Java 21 (virtual threads for in-flight requests), JUnit 5 + AssertJ, Testcontainers (IT only), ArcadeDB remote HTTP client.

**Spec:** `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md`, section 3 "Open-loop ramp" ("On the graph OLTP
point lookup: arrivals on a fixed schedule, latency measured from the scheduled start (no coordinated omission), the rate
raised in steps of 25% until p99 over 50 ms, errors over 0.1% or dropped arrivals. Metric `knee_per_s` (higher is better)
and the p99 at the knee."), section 2 (S sizing, caps), and "A workload may declare its own reps, warm-up and step limit (a
default method on `Workload`, overriding the run's)" from project 6, brought forward here.

## Global Constraints

- JSON only through `JSONObject` / `JSONArray`; `final` on locals and parameters; single-statement `if` without braces;
  imports, no fully qualified names; no `System.out`; no em dash anywhere.
- Tests: `assertThat(...)`; never assert on raw wall-clock time (the driver takes an injectable clock); no hand-picked ports;
  Docker tests are `*IT`.
- Bench tests: `./mvnw -o -pl bench -am test -Dtest='com.arcadedb.bench.**.*Test' -Dsurefire.failIfNoSpecifiedTests=false`.
- Break rule per step (spec values): p99 > **50 ms**, or errors > **0.1%** of the step's arrivals, or **any** dropped arrival.
- Coarse ramp: start **100/s**, multiply by **1.25** per step; then **3** bisection steps between the last passing and the
  first failing rate (resolution about 3%, so a knee that wobbles across one coarse step is not a 20% "regression").
- Step length: **10 s** at XS and S, **20 s** at M and L, preceded by **2 s** of untimed warm traffic at the step's rate.
  At most **40** coarse steps (100/s x 1.25^39 is about 600k/s: a ceiling the client never reaches).
- Metrics: `knee_per_s` (rate unit `ops/s`, HIGHER), `knee_p99_ms` (`ms`, LOWER), `steps` (count, unit `steps`, LOWER,
  informational). Gate units already known; no gate change.
- **Client capacity is not the knee:** a fixed pool of **128** clients (one `RemoteDatabase` each, warmed by one untimed
  query); an arrival that finds no free client is a drop. The case also reports `client_limited` = 1 when the scheduler fell
  more than 1 ms behind its schedule on more than 1% of the arrivals of the knee step, and the case then fails with a message
  (a knee the client cannot drive is not a server number).
- Repetitions: the workload declares **3** reps and **1** warm-up (one ramp each), overriding the run's 5/2.
- Commits authored by robfrank only, no Co-Authored-By. The PR says `Part of #12433`.

## Review Focus

- Coordinated omission: latency is `completion - scheduled start`, never `completion - send`; an arrival that waits for a
  client counts its wait. Pinned in Task 1 (`latencyIsMeasuredFromTheScheduledStart`).
- A ramp that never breaks below the ceiling reports the ceiling with a `ceiling_reached` note, not a fake knee; a ramp that
  breaks at the first step reports `knee_per_s` 0 and fails the case. Pinned in Task 2.
- Errors and drops counted per step, not cumulatively. Pinned in Task 1.
- The weeknight S budget: today's S run is close to 240 min once every phase 2 workload runs (first measured on the
  2026-11-27 04:30 run). The S inclusion is decided from that run (Task 4).
- Virtual threads: nothing pinned on a monitor while waiting on the network inside the request (the remote client must not
  hold a `synchronized` lock across I/O on a virtual thread); if it does, use a platform thread pool of 128 instead.

---

### Task 1: OpenLoop driver (pure, fake clock)

**Files:** Create `bench/src/main/java/com/arcadedb/bench/workload/ramp/OpenLoop.java`; test `bench/src/test/java/com/arcadedb/bench/workload/ramp/OpenLoopTest.java`.

**Interfaces:**
- `interface OpenLoop.Clock { long nanos(); void parkUntil(long nanos); }` (system and fake implementations).
- `record OpenLoop.StepResult(double ratePerS, long arrivals, long errors, long dropped, long lateStarts, double p99Ms, double p50Ms)`
  with `boolean broke()` (p99 > 50 ms, errors > 0.1% of arrivals, dropped > 0) and `String why()`.
- `OpenLoop(Clock clock, int clients, IntConsumer op, Executor executor)`; `StepResult runStep(double ratePerS, Duration warm, Duration length)`:
  arrival i is scheduled at `start + i / rate`; the scheduler waits until then, takes a free client index (or counts a drop),
  submits the op; latency = completion time - scheduled time; an exception from `op` counts an error; `lateStarts` counts
  arrivals the scheduler started more than 1 ms after their scheduled time.

- [ ] Step 1: failing tests `arrivalsFollowTheSchedule` (rate 100/s for 1 s: 100 arrivals at 10 ms spacing on the fake clock),
  `latencyIsMeasuredFromTheScheduledStart` (an op that blocks the only client makes the next arrival's latency include its
  wait), `aBusyPoolDrops`, `errorsAreCountedPerStep`, `breakRule` (p99 49.9 ms ok, 50.1 ms broke; 1 error in 1,000 ok, 2 broke; 1 drop broke),
  `warmTrafficIsNotCounted`.
- [ ] Step 2: run, expect failure. Step 3: implement. Step 4: bench unit suite passes.

### Task 2: RampSearch (pure)

**Files:** Create `.../workload/ramp/RampSearch.java`; test `RampSearchTest.java`.

**Interfaces:** `record RampSearch.Knee(double ratePerS, double p99Ms, int steps, boolean ceilingReached, String breakReason)`;
`static Knee search(DoubleFunction<OpenLoop.StepResult> step, double start, double factor, int maxCoarse, int bisections)`.

- [ ] Step 1: failing tests with a fake step function breaking above a known capacity C:
  `kneeWithinThreePercentOfCapacity` (C = 7,300: knee in [0.97 C, C]), `coarseThenBisectionStepCount`,
  `breaksAtFirstStepGivesZero` (knee 0, reason of the first step), `neverBreaksReportsTheCeiling` (`ceilingReached`).
- [ ] Steps 2-4 as above.

### Task 3: Workload-declared reps and the ramp workload

**Files:**
- Modify `bench/src/main/java/com/arcadedb/bench/runner/Workload.java` (`default OptionalInt reps()`, `default OptionalInt warmup()`,
  both empty), `runner/BenchRunner.java` (use `w.reps().orElse(reps)` / `w.warmup().orElse(warmup)` per workload; the run file
  records the effective values per case, as today in `CaseResult`), `workload/GraphOltpWorkload.java` (package-private
  `int[] userIds()`), `WorkloadRegistry.java` (append `new RampWorkload()`).
- Create `bench/src/main/java/com/arcadedb/bench/workload/RampWorkload.java`.
- Test: `BenchRunnerTest.aWorkloadsOwnRepsAndWarmupWin`, `RampWorkloadTest`, `RampWorkloadIT`.

**Interfaces:** `RampWorkload` named `ramp`, one case `knee-point-lookup-cypher`; `prepare` builds/reuses the graph through a
`GraphOltpWorkload` instance (`prepare(ctx, "point-lookup-cypher")`) and opens + warms the 128 clients; `measure` runs one
`RampSearch.search` with `MATCH (u:User {id: $id}) RETURN u.id AS n` over random user ids; publishes the metrics of the
Global Constraints; `teardown` closes the clients.

- [ ] Step 1: failing tests: `BenchRunnerTest.aWorkloadsOwnRepsAndWarmupWin` (a fake workload declaring 3/1 runs 4 measures
  under a 5/2 runner; one declaring nothing runs 7), `RampWorkloadTest.casesAndDeclaredRepetitions`, `metricsAreTheSpecs`.
- [ ] Step 2-3: implement. Step 4: unit suite passes; `RampWorkloadIT` at XS (short steps: a package-private override of the
  step length so the IT ends in about a minute): case `ok`, `knee_per_s` > 0, `steps` > 0.
- [ ] Step 5: run `ramp` at S once from the bench jar on Docker; report the knee, its p99, steps, `client_limited`, and the
  case's total time.

### Task 4: PR, fit check, S inclusion

- [ ] Push, open the PR (`Part of #12433`) with this plan and the measured numbers.
- [ ] From the 2026-11-27 weeknight S run (first with every phase 2 workload): if its job time + 2 x the measured ramp time
  stays under **225 min**, keep `ramp` at S; otherwise leave it out at S with `cases(Scale)` (it then runs every Saturday at
  M) and say so in the spec and the PR.
- [ ] After merge, dispatch `ramp` at M on the merge commit (it does not run at S); numbers in the PR. Merge stays with the user.
