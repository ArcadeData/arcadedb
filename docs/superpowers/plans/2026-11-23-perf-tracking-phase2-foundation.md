# Performance tracking phase 2, project 1 (foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the bench a schedule with room for phase 2 (S weeknights, M Saturdays) and make a run lose nothing: restart a
dead server, publish partial runs, flag a host that was not quiet, keep histograms and diagnostics, and stop the queue from
silently dropping runs.

**Architecture:** Bench-side changes (result model v2, runner hooks, `BenchMain` wiring, gate) land in one arcadedb PR on
branch `feat/perf-phase2-foundation`; workflow changes land in one PR in the private infra repository (branch
`feat/phase2-schedule`, clone at `~/projects/arcade/arcadedb-perf-infra`) after the arcadedb PR is merged, because the bench
jar is built from arcadedb `main` and the Saturday run needs the gate's scale-aware run key first.

**Tech Stack:** Java 21, JUnit 5 + AssertJ, Testcontainers (ITs only), HdrHistogram 2.2.2, GitHub Actions, bash.

**Spec:** `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md` (sections 1 and 2).

## Global Constraints

- JSON only through `com.arcadedb.serializer.json.JSONObject` / `JSONArray`; getters with defaults.
- `final` on locals and parameters; single-statement `if` without braces; imports, no fully qualified names.
- Tests: `assertThat(...)` style; never assert on raw wall-clock time; no hand-picked ports; Docker tests stay `*IT`.
- Bench tests run with `./mvnw -o -q -pl bench -am test -Dtest=<X> -Dsurefire.failIfNoSpecifiedTests=false` (never
  `mvn install`). Compile after every Java change.
- Older result files (schemaVersion 1, and the pre-schema shape the gate already reads) must still load everywhere.
- The infra repository is private: never link it from arcadedb issues, PR bodies or docs; its PR body may reference
  arcadedb freely.
- Commits authored by robfrank only, no Co-Authored-By; no em dash anywhere.
- Spec values: clock gap > **1 s** marks `hostNotQuiet`; stall = one call waiting **40 s**; at most **3** dumps per run;
  class histogram **top 30**; nightly run key `nightly:<ref>:<scale>`, a marker without scale reads as **M**; weeknight
  cron `30 4 * * 1-5` scale S budget **240**; Saturday cron `0 20 * * 6` scale M budget **720**; dispatches **720**.

## Review Focus

- A result file written mid-run (`complete: false`, some cases missing) must upload, export and be judged like a full one;
  pinned in Task 2.
- A server that dies twice in one workload must not loop: the rest of that workload is `not started: server died twice`,
  the next workload gets a fresh server; pinned in Task 4.
- A marker written by phase 3 before this change (key `nightly:<ref>`, open issue #13093) must still match its run, not
  open a duplicate issue; pinned in Task 6.
- The watchdog must never fire on the native variant by sending a signal that kills the server (native has no thread-dump
  support unless built for it); pinned in Task 5.
- The queue guard must not block itself or the run it guards (it lists *other* waiting runs only); pinned in Task 9.

---

### Task 1: Result model v2

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/model/CaseResult.java`, `RunResult.java`
- Modify: `bench/src/main/java/com/arcadedb/bench/store/Uploader.java` (stores the file's own version)
- Test: `bench/src/test/java/com/arcadedb/bench/model/RunResultTest.java`

**Interfaces:**
- Produces: `CaseResult` gains two trailing components `JSONObject notes` (never null in memory, `{}` when empty; JSON key
  `notes`, omitted when empty) and `JSONArray histograms` (one base64 string per measured rep, JSON key `histograms`,
  omitted when empty). A convenience constructor with the old 9 components keeps every existing call site compiling.
  `CaseResult withNote(String key, Object value)` and `CaseResult withStatus(Status status, String error)` return copies.
- Produces: `RunResult(RunInfo info, List<CaseResult> results, boolean complete, JSONObject diagnostics)`, old 2-arg
  constructor kept (complete = true, diagnostics = `{}`). `SCHEMA_VERSION = 2`; `fromJSON` accepts 1 and 2 (v1: complete
  true, no notes/histograms/diagnostics). JSON keys `complete`, `diagnostics`.
- Note keys used by later tasks: `serverDied` (string), `serverRestarted` (true), `hostNotQuiet` (string), `stallDumps` (int).

- [ ] **Step 1: Write failing tests** in `RunResultTest`: `v2RoundTripKeepsNotesHistogramsCompleteAndDiagnostics`
  (write a run with one note, two histogram strings, `complete=false`, diagnostics `{"ingestion":{"threads":42}}`, read it
  back, assert each); `v1FileStillReads` (a v1 JSON literal reads with `complete()==true`, empty notes and histograms);
  `emptyNotesAndHistogramsAreOmitted` (`toJSON().has("notes")` is false); `unknownVersionIsRefused` (version 3 throws).
- [ ] **Step 2: Run** `./mvnw -o -q -pl bench -am test -Dtest=RunResultTest -Dsurefire.failIfNoSpecifiedTests=false`; expect
  compile failure.
- [ ] **Step 3: Implement** the components above; `Uploader` keeps writing `Run.schemaVersion` from the file read.
- [ ] **Step 4: Run** `RunResultTest`, `UploaderIT` is Docker-only: run `ResultsStoreTest` and `RawDataTest` instead; all pass.
- [ ] **Step 5: Commit** `feat(bench): result schema v2 with case notes, histograms, completeness and diagnostics (#12433)`

### Task 2: Write the run file after every case

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/runner/BenchRunner.java`, `bench/src/main/java/com/arcadedb/bench/BenchMain.java`
- Test: `BenchRunnerTest.java`, `BenchMainTest.java`, `bench/src/test/java/com/arcadedb/bench/gate/RawDataTest.java`

**Interfaces:**
- Produces: `List<CaseResult> run(Workload w, WorkloadContext ctx, Predicate<String> caseFilter, Consumer<CaseResult> onCase)`;
  the 3-arg `run` delegates with a no-op consumer. `onCase` is called once per case, in order, after teardown.
- `BenchMain.runCommand` rewrites `<out>/<runId>.json` (same runId) after every case with `complete=false` and once more at
  the end with `complete=true`; the runId is fixed before the first workload starts.

- [ ] **Step 1: Failing tests:** `BenchRunnerTest.onCaseSeesEveryCaseInOrder` (3 fake cases, one failing: the consumer gets
  3 results in case order, the failing one included); `BenchMainTest.runFileIsRewrittenAfterEachCase` (a fake workload whose
  second case's `measure` reads the out directory: it finds the file with 1 result and `complete=false`; after the run the
  file has 2 results and `complete=true`); `RawDataTest.incompleteFileIsReadLikeAnyOther` (a `complete:false` file with one
  case yields that case's points).
- [ ] **Step 2: Run** the three classes; expect failures.
- [ ] **Step 3: Implement.** Write through a temp file and atomic move (`Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)`) so a
  kill mid-write never leaves half a file.
- [ ] **Step 4: Run** `BenchRunnerTest`, `BenchMainTest`, `RawDataTest`; pass.
- [ ] **Step 5: Commit** `feat(bench): the run file is rewritten after every case, so a cut-short run keeps what it measured (#12433)`

### Task 3: Clock check

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/runner/BenchRunner.java`
- Test: `BenchRunnerTest.java`

**Interfaces:**
- Produces: package-private constructor parameter `ClockPair clocks` on `BenchRunner` (record `ClockPair(LongSupplier
  wallMillis, LongSupplier monoNanos)`, default `System::currentTimeMillis` / `System::nanoTime`). Per case the runner
  takes both at the start and at the end; when `wallDelta - monoDelta > 1000 ms` it adds note `hostNotQuiet` =
  `"wall clock ran <N> ms ahead of the monotonic clock"`.

- [ ] **Step 1: Failing tests:** `clockJumpMarksHostNotQuiet` (fake clocks: wall +5,000 ms, mono +1 s: note present, text
  names 4000 ms); `smallDriftIsQuiet` (wall ahead by 900 ms: no note); `noteDoesNotChangeStatus` (status stays OK).
- [ ] **Step 2: Run** `BenchRunnerTest`; fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `BenchRunnerTest`; pass.
- [ ] **Step 5: Commit** `feat(bench): a case during which the host slept is marked hostNotQuiet (#12433)`

### Task 4: Server alive check and restart

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/runner/ServerGuard.java`
- Modify: `BenchRunner.java`, `BenchMain.java`, `bench/src/main/java/com/arcadedb/bench/server/ServerUnderTest.java`
- Test: `BenchRunnerTest.java`, `BenchMainTest.java`, `bench/src/test/java/com/arcadedb/bench/server/ServerUnderTestIT.java`

**Interfaces:**
- Produces: `interface ServerGuard { String deathReason(); WorkloadContext restart(); }` - `deathReason()` returns null when
  the server answers, else a reason (`"OOMKilled"`, `"exit code 137"`, `"not answering /api/v1/ready"`); `restart()` closes
  the dead server, starts a fresh one with the same image and settings and returns the new context.
- Produces: `List<CaseResult> run(Workload w, WorkloadContext ctx, ServerGuard guard, Predicate<String> caseFilter,
  Consumer<CaseResult> onCase)`; a null guard means no check (keeps the older overloads).
- Produces: `String ServerUnderTest.deathReason()` (HTTP `GET /api/v1/ready` with a 5 s timeout, then Docker inspect
  `State.OOMKilled` / `State.ExitCode` / `State.Running`).
- Behaviour before each case after the first: if `deathReason()` is non-null, the previous case is amended - FAILED or
  TIMEOUT: error becomes `"server died: <reason>; " + error`; OK: note `serverDied` = reason, status kept - and `onCase` is
  NOT called again for it (BenchMain rewrites the file from the amended list); then `restart()`, and the next case gets note
  `serverRestarted` = true. A second death in the same workload: every remaining case is FAILED `not started: server died
  twice`, no restart. A failed `restart()`: remaining cases FAILED `server restart failed: <describe(t)>`.

- [ ] **Step 1: Failing tests** (fake `ServerGuard` scripted per call): `deadServerIsRestartedBeforeNextCase`;
  `failedCaseCarriesTheDeathReason`; `okCaseKeepsStatusAndGetsServerDiedNote`; `secondDeathStopsTheWorkload`;
  `restartFailureFailsTheRest`; `BenchMainTest.nextWorkloadGetsAFreshServerAfterTwoDeaths` (fake `ServerFactory` counts
  starts).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement** the guard in `BenchMain` around `ServerFactory`, and `ServerUnderTest.deathReason()`.
- [ ] **Step 4: Run** `BenchRunnerTest`, `BenchMainTest`; pass. Add `ServerUnderTestIT.deathReasonNamesAKilledContainer`
  (start, `docker kill`, assert reason non-null) and run it once locally if Docker is up; say so in the report if not.
- [ ] **Step 5: Commit** `feat(bench): a dead server is restarted for the next case and the case it died in says why (#12433)`

### Task 5: Stall watchdog and end-of-workload diagnostics

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/runner/StallWatchdog.java`
- Modify: `BenchRunner.java`, `BenchMain.java`, `ServerUnderTest.java`
- Test: `bench/src/test/java/com/arcadedb/bench/runner/StallWatchdogTest.java`, `BenchMainTest.java`

**Interfaces:**
- Produces: `StallWatchdog(Supplier<StackTraceElement[]> workerStack, LongSupplier monoNanos, Duration threshold,
  int maxDumps, Function<Integer, Boolean> dump)` with `void sample()` (called every 5 s by a daemon scheduler the runner
  owns per case) and `int dumpsTaken()`. A stall is the worker's stack unchanged (same frames) for >= threshold; one dump per
  stall, a new stall needs a changed stack in between; `maxDumps` is per run (the runner shares one budget object).
- Produces: `boolean ServerUnderTest.threadDump(Path file)` - JVM: `docker exec <id> jcmd 1 Thread.print` into `file`,
  true; native: false without touching the process (no signal is ever sent). `JSONObject ServerUnderTest.diagnostics()` -
  `threads` from `/proc/<State.Pid>/status` on the host (absent when unreadable) and, JVM only, `classHistogram`: the
  first 30 rows of `jcmd 1 GC.class_histogram` as a JSONArray of strings.
- Dumps go to `<out>/dumps/<runId>-<n>.txt`; the case gets note `stallDumps` = dumps taken during it (omitted when 0).
  `BenchMain` stores `server.diagnostics()` under `diagnostics.<workload>` before closing each server.

- [ ] **Step 1: Failing tests** (`StallWatchdogTest`, fake clock and stack): `unchangedStackFor40sDumpsOnce`;
  `changingStackNeverDumps`; `newStallAfterProgressDumpsAgain`; `budgetOfThreeIsPerRun`; `dumpReturningFalseCountsNothing`;
  `BenchMainTest.diagnosticsLandUnderTheWorkloadName` (fake server returns `{"threads":7}`).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement.** The watchdog thread must never outlive its case (cancel in `finally`).
- [ ] **Step 4: Run** `StallWatchdogTest`, `BenchRunnerTest`, `BenchMainTest`; pass.
- [ ] **Step 5: Commit** `feat(bench): thread dumps on a 40 s stall and a class histogram per workload (#12433)`

### Task 6: Latency histograms in the result

**Files:**
- Modify: `bench/pom.xml` (explicit `org.hdrhistogram:HdrHistogram`, version from the reactor's dependency management or
  2.2.2; already in ATTRIBUTIONS.md), `runner/LatencyRecorder.java`, `runner/Measurement.java`, `runner/BenchRunner.java`,
  the five workloads that use a `LatencyRecorder`
- Test: `bench/src/test/java/com/arcadedb/bench/runner/LatencyRecorderTest.java` (create), `BenchRunnerTest.java`

**Interfaces:**
- Produces: `String LatencyRecorder.encodedHistogram()` - an HdrHistogram in microseconds, 3 significant digits, highest
  trackable 1 hour, `encodeIntoCompressedByteBuffer`, base64; `static Histogram LatencyRecorder.decode(String)` for tests and
  later merges.
- Produces: `Measurement(List<Metric> perRep, String histogram)` with the 1-arg constructor kept (histogram null);
  `static Measurement of(List<Metric> perRep, LatencyRecorder recorder)`. The runner collects one histogram per measured rep
  (warm-ups ignored) into `CaseResult.histograms`.

- [ ] **Step 1: Failing tests:** `encodedHistogramRoundTripsCountAndP99` (1,000 samples 1..1000 ms: decoded total count 1000,
  p99 within 1% of 990 ms); `emptyRecorderEncodesAnEmptyHistogram`; `BenchRunnerTest.histogramsOnePerMeasuredRep` (2 warm-ups,
  3 reps: 3 entries).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement**, then switch the five workloads to `Measurement.of(..., recorder)` where a recorder exists.
- [ ] **Step 4: Run** `LatencyRecorderTest`, `BenchRunnerTest`, `MetricsTest`; compile the module.
- [ ] **Step 5: Commit** `feat(bench): latency histograms per repetition in the result (#12433)`

### Task 7: Gate - hostNotQuiet points are not valid, nightly run key carries the scale

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/gate/Point.java`, `RawData.java`, `Judge.java`, `Planner.java`, `Marker.java`
- Modify: `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md` (Testing: "an S and an M nightly of the **same**
  commit make two events")
- Test: `JudgeTest.java`, `PlannerTest.java`, `RawDataTest.java`, `MarkerTest.java`; re-run `GateFixtureTest`

**Interfaces:**
- Produces: `Point` gains `boolean hostNotQuiet`; `valid()` is `"ok".equals(status) && !hostNotQuiet`. `RawData` sets it from
  `notes.hostNotQuiet` presence. `Judge` neither judges nor uses as baseline a hostNotQuiet point (the newest such point is
  skipped: the line is judged on its newest quiet point, or not at all).
- Produces: `Planner.runKey(String trigger, String ref, String scale)` = `trigger + ":" + ref + ":" + scale` for `nightly`,
  `trigger + ":" + ref` for `release`. `Marker` stores `scale` for nightly events; a marker without `scale` reads as `"M"`.
  Run comments and markers written from now on use the new key; a marker's recorded runs without scale match the M key.

- [ ] **Step 1: Failing tests:** `JudgeTest.hostNotQuietNewestIsNotJudged`, `JudgeTest.hostNotQuietIsNeverABaseline`;
  `RawDataTest.notesHostNotQuietMarksThePoint`; `PlannerTest.sameCommitSAndMNightlyAreTwoEvents` (Friday S worse and Saturday
  M worse at one commit: two issues, two comments);
  `PlannerTest.legacyMarkerWithoutScaleStillMatchesItsMRun` (marker JSON as phase 3 wrote it for #13093: no new issue, no
  second comment); `MarkerTest.missingScaleReadsAsM`.
- [ ] **Step 2: Run** the gate test classes; fail.
- [ ] **Step 3: Implement**; update the spec line.
- [ ] **Step 4: Run** every class under `bench/src/test/java/com/arcadedb/bench/gate`; `GateFixtureTest`'s pinned plan must not
  change (its fixtures are M nightlies and releases). If it changes, stop and report the diff instead of re-pinning.
- [ ] **Step 5: Commit** `feat(bench): the gate skips points of a host that was not quiet, and nightly runs are keyed by scale (#12433)`

### Task 8: arcadedb PR

- [ ] **Step 1:** Run all bench unit tests: `./mvnw -o -q -pl bench -am test -Dtest='com.arcadedb.bench.**.*Test' -Dsurefire.failIfNoSpecifiedTests=false`;
  read the `Results:` summary; all pass.
- [ ] **Step 2:** Push `feat/perf-phase2-foundation`, open the PR (`Part of #12433`, not `Closes`), with the spec and this plan
  committed in it. Handle the review surfaces (`pr-review-surfaces` skill). Merge stays with the user.

### Task 9: Infra PR - schedule, upload as its own step, queue guard

Starts after the Task 8 PR is merged. Repository: `~/projects/arcade/arcadedb-perf-infra`, branch `feat/phase2-schedule`
off `origin/main`.

**Files:**
- Modify: `.github/workflows/nightly.yml`, `.github/workflows/release.yml`, `scripts/run-all.sh`
- Create: `scripts/upload.sh`, `scripts/queue-guard.sh`

**Decisions:**
- `nightly.yml` schedules `30 4 * * 1-5` and `0 20 * * 6`. Scale: dispatch input, else `S` when `github.event.schedule ==
  '30 4 * * 1-5'`, else `M`. Job `timeout-minutes`: 240 for the weeknight schedule, 720 otherwise. LDBC steps run only when
  the workloads are `all` and the scale is `M`.
- The Measure step (and both Measure steps of `release.yml`) gets `timeout-minutes` = job budget - 20, so a long run fails
  its step instead of cancelling the job.
- `run-all.sh` stops after restoring the host (steps 1-6); upload and export move to `scripts/upload.sh <out>
  [--replace-release]` (today's step 7, unchanged logic, including the `replaced.txt` lines). A new step "Upload and export"
  runs `if: always() && steps.decide.outputs.skip != 'true'` before the publish checkout; publish checkout and publish change
  `!cancelled()` to `always()`.
- Queue guard: a first job `queue-guard` on `ubuntu-latest` (`permissions: actions: read`) runs `scripts/queue-guard.sh`:
  `gh run list --json databaseId,status,workflowName` filtered to `nightly.yml`, `release.yml`, `backfill-commit-dates.yml`
  runs in status `pending` or `waiting` other than `$GITHUB_RUN_ID`; any found -> `::error::` naming each (`<workflow> run
  <id>`) and exit 1. The perf-01 job `needs: queue-guard`. Add it to every workflow that uses the `perf-01` group.

- [ ] **Step 1:** Write `queue-guard.sh` with a `--from-json <file>` test mode; test it with three JSON fixtures in
  `scripts/test/` (none waiting -> 0; only itself -> 0; another waiting -> 1 and the message names it) via a small bats-free
  bash test script `scripts/test/queue-guard-test.sh`. Run it; pass.
- [ ] **Step 2:** Extract `upload.sh`; `run-all.sh` ends after `restore`; `shellcheck` both.
- [ ] **Step 3:** Edit the workflows; `actionlint` all three.
- [ ] **Step 4:** Commit, push, open the PR. After merge: dispatch `nightly.yml` with `scale=S workloads=all` once and check
  that the upload step ran and the file landed in the public data repo.

### Task 10: S sizing check (after Task 9 is merged)

- [ ] **Step 1:** From the dispatched S run's raw files (jvm and native), list every case whose timed metric (unit `s`) has
  a median under 5 s, and every latency case with fewer than 1,000 operations per repetition (operations = the workload's
  per-rep count, read from its code at S).
- [ ] **Step 2:** For each, raise its S size in the workload (`records(Scale)` and friends) to the smallest value that meets
  the rule, keeping the case under the 30 min step limit; one commit per workload, unit test `<Workload>Test`/`IT` sizes
  updated. Open one arcadedb PR `perf(bench): S sizes measure something (#12433)`.
- [ ] **Step 3:** After merge, the first scheduled weeknight run must end inside 240 min; if it does not, move the slowest
  cases to Saturday only and update the spec with the measured reason.
