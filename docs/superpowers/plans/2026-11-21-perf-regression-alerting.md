# Performance regression alerting (phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every nightly and release run published to ArcadeData/arcadedb-perf is judged every 6 hours, and a regression opens,
updates and closes one issue per regression event in ArcadeData/arcadedb.

**Architecture:** A new package `com.arcadedb.bench.gate` reads the raw run files, judges the newest point of every line per
trigger, and turns the verdicts plus the current labelled issues into a JSON plan (open, comment, update body, close). Two CLI
commands expose it (`bench gate-pairs`, `bench gate`). A workflow `.github/workflows/perf-regression.yml` lists the issues,
answers the bench-change question with `git diff`, runs the gate and applies the plan with `actions/github-script`.

**Tech Stack:** Java 21, `com.arcadedb.serializer.json.JSONObject`/`JSONArray`, JUnit 5 + AssertJ, GitHub Actions
(`actions/github-script`).

**Spec:** `docs/superpowers/specs/2026-11-21-perf-regression-alerting-design.md`

## Global Constraints

- Judged triggers: `nightly` and `release` only.
- A line is (machine, variant, scale, workload, case, metric); `_status` is the case's status line.
- A run is one trigger at one measured version: nightly = `arcadedb.commit`, release = `arcadedb.tag`. Never identify by `runId`.
- Valid point = case status `ok`. A failed or timed-out point is never a value baseline.
- `spread` = (max - min) / median of the repetition values; 0 with fewer than 2 values.
- `band` = max(spread(baseline), spread(newest), 0.10).
- Worse when `change > band` AND |median difference| >= floor. Floors: `ms` 1, `s` 0.5, `bytes` 1048576, any other unit 0.
- Base 0: judged by the absolute floor alone.
- Improvements go in comments only, never open an issue.
- Close: nightly issue after every line recovered on 2 consecutive nightly runs; release issue after 1 later release.
- Label `performance-regression`. Titles: `Performance regression: N lines worse between <base7>..<new7>` and
  `Performance regression: N lines worse in release <tag> vs <tag>`.
- Marker: `<!-- perf-regression {json} -->` in the issue body.
- Issues read: open, plus closed in the last 30 days.
- A manual close (closer is not `github-actions[bot]`) means accepted.
- At most one comment per issue per run.
- `bench gate` makes no network call.
- The 10% band floor and unit floors are constants in one class, each with a test.
- JSON with `JSONObject`/`JSONArray` only; `final` on locals and parameters; no braces on single-statement `if`; AssertJ
  `assertThat(...)`; no new dependencies; no `System.out` debug left; author robfrank only, no Co-Authored-By; never the em dash.

## Rulings made while planning (the spec left these to the builder)

- **Bench changes, two passes:** `bench gate-pairs` prints the (base, new) bench commit pairs the plan needs; the workflow
  answers each with `git diff --quiet A B -- bench/` into `bench-changes.json` (`{"A..B": true|false}`); `bench gate` reads
  it. A missing pair or a null bench commit renders as "unknown". Keeps the gate free of git and network.
- **Comment per run is an upsert:** each comment carries `<!-- perf-regression-run <trigger>:<ref> -->`. The apply script
  edits an existing comment with that key instead of adding one, so the jvm, native and ldbc files of one nightly arriving in
  three invocations still give one comment, whose body the gate renders whole each time.
- **Body before comment:** the apply script updates the body (marker) before it posts the comment. A failure in between
  loses a comment, never doubles one.
- **Recovery streak per line:** each line keeps `streak` (consecutive recovered runs of the issue's trigger) and
  `lastRun`; the issue closes when every line's streak reaches 2 (nightly) or 1 (release). Lines of one nightly arrive in
  different invocations, so an issue-level streak would count half-runs.
- **Same-ref re-runs:** a later point with the same commit (nightly) or tag (release) replaces the earlier one as "the
  run's point"; the baseline is always a point with a different ref.
- **Status baseline:** the `_status` line compares with the previous point of a different ref whatever its status, so an
  already failing case is not reported again.
- **Going live:** the scheduled run applies the plan only when the repository variable `PERF_REGRESSION_LIVE` is `true`;
  until then it prints the plan. `workflow_dispatch` has `dry_run` (default `true`). This is how "dry run before the
  schedule is enabled" works with a workflow that must exist on `main` to be dispatched.
- Body is always `IssueText.body(marker)`: a re-render, never an append.
- Links: dashboard `https://perf.arcadedb.com/workload.html?w=<workload>`; raw file
  `https://github.com/ArcadeData/arcadedb-perf/blob/main/data/<yyyy>/<MM>/<runId>.json` (year and month from the `runId`);
  compare `https://github.com/ArcadeData/arcadedb/compare/<base>...<new>` (commits, or tags for releases).
- Cron `17 */6 * * *`.

## Review Focus

1. **The three files of one nightly in three invocations:** one issue and one comment, not three. Pinned in Task 4
   (`nightlyFilesArrivingSeparatelyJoinOneIssueAndOneComment`).
2. **A workflow re-run with nothing new:** an empty plan. Pinned in Task 4 (`rerunWithNoNewDataProducesAnEmptyPlan`) and on
   the real fixture in Task 5.
3. **The 2026-11-17 file of the older shape** (no `schemaVersion`, `config`, `commitDate`, `values`): read, ordered by run
   time, spread 0, bench commit unknown, no crash. Pinned in Task 1 (`readsTheOlderFileShape`).
4. **A marker a human edited into invalid JSON, or an issue with the label and no marker:** ignored with a warning on
   stderr, never a crash that silences every other alert. Pinned in Task 3 (`unreadableMarkerIsIgnored`).
5. **A case that failed with metrics (recall under its gate):** one `_status` line, its metric lines not judged. Pinned in
   Task 2 (`failedCaseMetricsAreNotJudged`).

---

### Task 1: Read the raw files into lines of points

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/gate/LineKey.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/Point.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/RawData.java`
- Test: `bench/src/test/java/com/arcadedb/bench/gate/RawDataTest.java`

**Interfaces:**
- Produces:
  - `record LineKey(String machine, String variant, String scale, String workload, String caseName, String metric) implements Comparable<LineKey>`; `static final String STATUS = "_status"`; `String id()` = the six fields joined by `/` (stable key used in markers).
  - `record Point(String trigger, String ref, String commit, String tag, Instant t, Instant runAt, String runId, String benchCommit, String status, double median, double[] values, String unit, String better)`; `ref` = commit for nightly, tag for release; `boolean valid()` = `"ok".equals(status)`; for `_status` points `median` is `NaN`, `values` empty.
  - `final class RawData { static Map<LineKey, List<Point>> read(Path dataDir) }`: walks `dataDir` recursively for `*.json`, keeps triggers `nightly`/`release` (and a release only with a non-empty tag), emits one `_status` point per case per run plus one point per metric, each list ordered by `t` then `runAt`; returns a `TreeMap`.

- [ ] **Step 1: Write the failing tests** in `RawDataTest` (each writes small JSON files into a `@TempDir`):
  - `keepsOnlyNightlyAndRelease`: files with triggers `nightly`, `release` (tag `26.11.1`), `manual`, and `release` with no tag; assert only the first two contribute points.
  - `emitsAStatusLineAndOneLinePerMetric`: one case with two metrics; assert three keys, `_status` point has `status` `ok` and `Double.isNaN(median)`.
  - `nightlyRefIsCommitReleaseRefIsTag`: assert `ref` `abc123` for nightly, `26.11.1` for release.
  - `ordersByCommitDateThenRunTime`: a release with `commitDate` 2026-11-04 measured on 2026-11-20 sorts before a nightly with `commitDate` 2026-11-19; two points with equal `t` order by `startedAt`.
  - `readsTheOlderFileShape`: the exact content of `2026-11-17T03-00-00Z-1eeb9420ac-jvm.json` from arcadedb-perf (no `schemaVersion`, `config: null`, no `commitDate`, metrics without `values`) yields a point with `t == runAt == 2026-11-17T03:00:00Z`, `values.length == 0`, `benchCommit == null`.
- [ ] **Step 2: Run** `./mvnw -o -q -pl bench test -Dtest=RawDataTest` - expected: compilation failure (classes missing).
- [ ] **Step 3: Implement** `LineKey`, `Point`, `RawData.read`. Do not reuse `RunResult.fromJSON`: it rejects the older shape. `t` = `arcadedb.commitDate` when present and non-empty, else `startedAt` (as `Exporter.loadSeries` does). Machine from `machine.id`, variant from `arcadedb.variant`, bench commit from `config.benchCommit` (`config` may be JSON null).
- [ ] **Step 4: Run** the same command - expected: 5 tests pass.
- [ ] **Step 5: Commit** `feat(bench): gate reads the published raw runs into lines of points`

### Task 2: Judge the newest point of each trigger

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/gate/Rule.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/Verdict.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/Judge.java`
- Test: `bench/src/test/java/com/arcadedb/bench/gate/RuleTest.java`, `bench/src/test/java/com/arcadedb/bench/gate/JudgeTest.java`

**Interfaces:**
- Consumes: `LineKey`, `Point` (Task 1).
- Produces:
  - `final class Rule { static final double MIN_BAND = 0.10; static double absoluteFloor(String unit); static double spread(double median, double[] values); static Verdict.Kind compare(Point baseline, Point newest) }` plus `static double band(Point baseline, Point newest)` and `static double change(Point baseline, Point newest)` (relative, positive = worse; 0 when the base is 0).
  - `record Verdict(LineKey key, String trigger, Kind kind, Point newest, Point baseline, double change, double band)`; `enum Kind { WORSE, BETTER, SAME, NOT_JUDGED }`.
  - `final class Judge { static List<Verdict> judge(Map<LineKey, List<Point>> lines) }`: one verdict per (line, trigger) that has a newest point; `baseline` may be null (NOT_JUDGED).
  - `static Point newest(List<Point> points, String trigger)` and `static Point baselineOf(List<Point> points, Point newest, boolean statusLine)` on `Judge`, package-private, reused by Task 4 for recovery.

- [ ] **Step 1: Write the failing `RuleTest`:**
  - `bandFloorIsTenPercent`: `MIN_BAND == 0.10`; base 100 ms values {100,100}, new 109 ms {109,109}: SAME; new 111 ms: WORSE.
  - `bandWidensToTheSpread`: base median 100 values {80,100,120} (spread 0.4), new 135: SAME; new 145: WORSE.
  - `msFloorIsOneMillisecond`: base 0.4 ms, new 0.6 ms: SAME (`absoluteFloor("ms") == 1.0`).
  - `secondsFloorIsHalfASecond`: `absoluteFloor("s") == 0.5`; base 2.0 s, new 2.4 s (+20%): SAME; base 4 s, new 4.6 s: WORSE.
  - `bytesFloorIsOneMebibyte`: `absoluteFloor("bytes") == 1048576`; base 4 MiB, new 4.9 MiB: SAME.
  - `ratesHaveNoFloor`: `absoluteFloor("records/s") == 0` and `absoluteFloor("ratio") == 0`.
  - `higherIsBetterFlipsTheSign`: records/s 1000 to 800: WORSE; 1000 to 1200: BETTER.
  - `zeroBaseUsesTheFloorAlone`: ms base 0, new 0.5: SAME; new 2: WORSE; ratio base 0, new 0.01 lower-is-better: WORSE.
  - `spreadNeedsTwoValues`: `spread(10, new double[]{10}) == 0`.
- [ ] **Step 2: Write the failing `JudgeTest`:**
  - `statusFlipIsOneWorseLine`: case ok then failed, with two metrics on the ok run; exactly one WORSE verdict, on `_status`.
  - `failedCaseMetricsAreNotJudged`: newest run `failed` but carries metric `recall` far below baseline; metric verdict NOT_JUDGED.
  - `alreadyFailingIsNotReportedAgain`: statuses ok, failed, failed; `_status` newest verdict is not WORSE.
  - `invalidBaselineIsSkipped`: metric points ok(100), failed(-), ok(150) ms: baseline is the 100 point, WORSE.
  - `judgesEachTriggerSeparately`: an M line with nightly points and a release point measured later at an older commit date; two verdicts, the nightly one compares nightly with nightly.
  - `releaseBaselineIsThePreviousDifferentTag`: releases 26.10.2 (100), 26.11.1 (100), 26.11.1 re-run (130): baseline is 26.10.2, WORSE.
  - `sameCommitRerunIsNotItsOwnBaseline`: two nightly points of commit `a`, one of commit `b` before them: baseline is `b`.
  - `noBaselineIsNotJudged`: a single point: NOT_JUDGED.
- [ ] **Step 3: Run** `./mvnw -o -q -pl bench test -Dtest='RuleTest,JudgeTest'` - expected: compilation failure.
- [ ] **Step 4: Implement** `Rule`, `Verdict`, `Judge`. Status line: WORSE when baseline status `ok` and newest `failed`/`timeout`, BETTER when baseline not ok and newest ok, else SAME. Metric line: NOT_JUDGED when newest is invalid or no valid baseline exists.
- [ ] **Step 5: Run** the same command - expected: all pass.
- [ ] **Step 6: Commit** `feat(bench): gate judges each line against its baseline with a spread band and unit floors`

### Task 3: Issue state and its marker

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/gate/Marker.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/TrackedIssue.java`
- Test: `bench/src/test/java/com/arcadedb/bench/gate/MarkerTest.java`

**Interfaces:**
- Consumes: `LineKey.id()` (Task 1).
- Produces:
  - `final class Marker` (mutable, JSON-backed): fields `trigger`, `ref` (the event's run), `baseRef` (start of the commit or tag range), and per line id a `JSONObject` entry `{baseRef, baseMedian, baseValues[], baseStatus, unit, better, lastRun, streak, recovered}`; `static Marker parse(String issueBody)` returns `null` when there is no marker or its JSON is invalid; `String render()` returns `<!-- perf-regression {json} -->`; accessors `JSONObject line(String lineId)`, `Set<String> lineIds()`, `void putLine(String lineId, JSONObject entry)`.
  - `record TrackedIssue(int number, boolean open, boolean closedByMaintainer, Marker marker)`; `static List<TrackedIssue> readAll(Path issuesFile, PrintStream warn)` reads the workflow's issue list `[{number, state, closedBy, body}]`, skips (with one warning line naming the number) issues whose marker is null. `closedByMaintainer` = closed and `closedBy` is not `github-actions[bot]`.

- [ ] **Step 1: Write the failing `MarkerTest`:**
  - `roundTripsThroughTheBody`: build a marker with one line, render, embed between other text, parse; equal content.
  - `unreadableMarkerIsIgnored`: an issues file with three issues: one valid, one whose marker JSON is truncated, one with no marker; `readAll` returns one issue and writes two warning lines naming #2 and #3.
  - `botCloseIsNotAcceptance`: `closedBy` `github-actions[bot]` gives `closedByMaintainer == false`; `robfrank` gives true; open issue gives false.
- [ ] **Step 2: Run** `./mvnw -o -q -pl bench test -Dtest=MarkerTest` - expected: compilation failure.
- [ ] **Step 3: Implement** `Marker` and `TrackedIssue`. The marker is found with the regex `<!-- perf-regression (\{.*?\}) -->` (DOTALL); `render()` must never emit `-->` inside the JSON (escape `>` as `>`).
- [ ] **Step 4: Run** - expected: 3 pass.
- [ ] **Step 5: Commit** `feat(bench): gate keeps each regression's state in a marker in its issue`

### Task 4: The plan

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/gate/Planner.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/IssueText.java`
- Create: `bench/src/main/java/com/arcadedb/bench/gate/BenchChanges.java`
- Test: `bench/src/test/java/com/arcadedb/bench/gate/PlannerTest.java`, `bench/src/test/java/com/arcadedb/bench/gate/IssueTextTest.java`

**Interfaces:**
- Consumes: `RawData`, `Judge`, `Verdict`, `Rule.compare`, `Marker`, `TrackedIssue`.
- Produces:
  - `final class BenchChanges { static BenchChanges read(Path file) /* missing file = all unknown */; Boolean changed(String base, String next) /* null = unknown */; static Set<String> pairsNeeded(List<Verdict> verdicts) /* "A..B", skips nulls and A == B */ }`.
  - `final class Planner { static JSONObject plan(Map<LineKey, List<Point>> lines, List<TrackedIssue> issues, BenchChanges bench) }`. The plan is `{"label":"performance-regression","actions":[...]}` with actions, in this order per issue: `{"op":"create","key":"<trigger>:<ref>","title","body"}`, `{"op":"update","issue":N,"title","body"}`, `{"op":"comment","issue":N,"runKey":"<trigger>:<ref>","body"}`, `{"op":"close","issue":N,"body"}` (close carries its last comment). An issue the plan creates can only be referenced by `key`, so the comment for a new issue is folded into its body.
  - `final class IssueText`: `static String title(Marker m)`, `static String body(Marker m, BenchChanges bench)`, `static String runComment(String runKey, List<String> rows)` (starts with `<!-- perf-regression-run <runKey> -->`).

- [ ] **Step 1: Write the failing `PlannerTest`** (synthetic `Map<LineKey, List<Point>>` and `TrackedIssue` lists built by small helpers in the test):
  - `newEventOpensOneIssue`: two lines worse in nightly `b` vs `a`; one `create`, title `Performance regression: 2 lines worse between aaaaaaa..bbbbbbb`, body contains the marker and `compare/a...b`.
  - `rerunWithNoNewDataProducesAnEmptyPlan`: feed the marker from the previous test's create back as an open issue with the same data; `actions` is empty.
  - `nightlyFilesArrivingSeparatelyJoinOneIssueAndOneComment`: invocation 1 sees jvm lines worse at `b` (create); invocation 2 adds native lines worse at `b`: one `update` (title now counts all lines) and no `create`; invocation 3 adds nothing new: empty.
  - `stillWorseIsCommented`: open issue for `b`; nightly `c` still worse on its line; `update` (line `lastRun` = `c`, streak 0) then one `comment` with runKey `nightly:c` containing `still worse`.
  - `recoveredTwiceClosesANightlyIssue`: runs `c` and `d` back within the band of the recorded baseline; after `c`: comment `recovered`, no close; after `d`: `close`.
  - `recoveryStreakResetsWhenWorseAgain`: recovered at `c`, worse at `d`, recovered at `e`: no close.
  - `oneRecoveredReleaseClosesAReleaseIssue`: release issue `26.11.1 vs 26.10.2`, release `26.12.1` within band: `close`; title of the create was `Performance regression: 1 lines worse in release 26.11.1 vs 26.10.2`.
  - `maintainerCloseMeansAccepted`: issue closed by `robfrank` whose line baseRef equals the line's current baseline ref: no action for that line.
  - `reopenedIssueIsTrackedAgain`: same issue with `open == true`: still worse produces a comment.
  - `releaseRemeasuredWithReplaceDoesNotCommentTwice`: release issue for `26.11.1` reported; the data now holds a new `runId` for `26.11.1` with the same values: empty plan.
  - `improvementsGoInTheComment`: a line of an open issue gets BETTER than its recorded baseline (that is recovery) and another line of the same issue's run, not in the issue, improves: the comment lists it under `Improvements`; no `create` for improvements alone.
  - `benchChangeNote`: with `BenchChanges` saying `x..y` true, the body contains `the bench changed between these runs: check it first`; false: no note; unknown: `bench change unknown`.
- [ ] **Step 2: Write the failing `IssueTextTest`:** `bodyHasTheTableAndLinks` (columns workload, case, scale, variant, metric, before, after, change, band; dashboard link `https://perf.arcadedb.com/workload.html?w=vector`; raw links for both runIds under `data/2026/11/`) and `rowWithItsOwnOlderBaselineShowsIt` (a line whose baseline is commit `z` while the event range is `a..b` shows `z` in its row) and `howToAccept` (body says closing the issue accepts the regression).
- [ ] **Step 3: Run** `./mvnw -o -q -pl bench test -Dtest='PlannerTest,IssueTextTest'` - expected: compilation failure.
- [ ] **Step 4: Implement** `BenchChanges`, `IssueText`, `Planner`, following the spec's "Each invocation" steps 1 to 6 and the rulings above. For a line already tracked, recovery is `Rule.compare(recorded baseline point, newest)` != WORSE (the recorded baseline is rebuilt from the marker's `baseMedian`, `baseValues`, `baseStatus`, `unit`, `better`). A line counts as reported for a run when its `lastRun` equals the newest point's ref.
- [ ] **Step 5: Run** - expected: all pass.
- [ ] **Step 6: Commit** `feat(bench): gate plans the issues, comments and closes of each regression event`

### Task 5: `bench gate` and `bench gate-pairs`, and the real-data fixture

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/BenchMain.java` (usage line at :78, the `switch` at :82-115, option sets at :135-146)
- Create: `bench/src/test/resources/gate/data/2026/11/` with copies of these arcadedb-perf files: `2026-11-17T03-00-00Z-1eeb9420ac-jvm.json`, the six `ed6090eaa6` and `0f8f74e803` nightly files, the five `e5040d0339` (26.10.2) and five `0f3bb2238c` (26.11.1) release files
- Create: `bench/src/test/resources/gate/expected-plan.json`
- Test: `bench/src/test/java/com/arcadedb/bench/BenchMainTest.java`, `bench/src/test/java/com/arcadedb/bench/gate/GateFixtureTest.java`

**Interfaces:**
- Consumes: `RawData.read`, `Judge.judge`, `TrackedIssue.readAll`, `BenchChanges`, `Planner.plan`.
- Produces (used by Task 6):
  - `bench gate-pairs --data <dir>`: prints each needed pair as `A B` on its own line.
  - `bench gate --data <dir> --issues <file> [--bench-changes <file>] --out <file>`: writes the plan JSON (`toString(2)`), prints a one-line summary `plan: C create, U update, K comment, X close`, exits 0; exit 2 on a missing required option.
  - `static final Set<String> GATE_OPTIONS = Set.of("data", "issues", "bench-changes", "out")` and `GATE_PAIRS_OPTIONS = Set.of("data")`.

- [ ] **Step 1: Write the failing tests:**
  - `BenchMainTest.gateOptionsAreKnown`: add `gate` and `gate-pairs` invocations to the existing option census (the block at :357-369) with `containsKeys`.
  - `BenchMainTest.gateWritesAPlan`: run `gate` on a `@TempDir` with one nightly pair worse and an empty issues file `[]`; exit 0, the out file has one `create`.
  - `GateFixtureTest.realDataPlanIsPinned`: run the planner over `gate/data` with no issues and no bench changes; `assertThat(plan.toString(2)).isEqualTo(expected-plan.json content)`.
  - `GateFixtureTest.realDataRerunIsEmpty`: feed the pinned plan's created bodies back as open issues; the second plan has no actions.
- [ ] **Step 2: Run** `./mvnw -o -q -pl bench test -Dtest='BenchMainTest,GateFixtureTest'` - expected: failures.
- [ ] **Step 3: Implement** the two commands in `BenchMain`. Generate `expected-plan.json` once by running the planner, then **check it by hand** against the dashboard numbers: every created issue's lines must be real changes over the band (write the check in the commit body: which lines, why each is or is not plausible). Do not pin a plan you have not read.
- [ ] **Step 4: Run** - expected: pass; then `./mvnw -o -q -pl bench test` (whole module, unit tests only) - expected: green.
- [ ] **Step 5: Commit** `feat(bench): bench gate and gate-pairs commands, pinned over real published runs`

### Task 6: The workflow

**Files:**
- Create: `.github/workflows/perf-regression.yml`

**Interfaces:**
- Consumes: `bench gate-pairs`, `bench gate` (Task 5), the jar `bench/target/arcadedb-bench-*-cli.jar`.

- [ ] **Step 1: Write the workflow** with:
  - `on: schedule: - cron: '17 */6 * * *'` and `workflow_dispatch: inputs: dry_run: {type: boolean, default: true}`.
  - `permissions: contents: read, issues: write`; `concurrency: {group: perf-regression, cancel-in-progress: false}`.
  - Steps: checkout arcadedb with `fetch-depth: 0`; checkout `ArcadeData/arcadedb-perf` into `perf` (`persist-credentials: false`); setup-java temurin 21 with the Maven cache; `./mvnw -B -pl bench -am package -DskipTests`.
  - `actions/github-script` "List regression issues": `issues.listForRepo({labels:'performance-regression', state:'all', since: <now - 30 days>})` paginated; for closed issues, `issues.get` for `closed_by.login`; drop closed issues whose `closed_at` is older than 30 days; write `issues.json` as `[{number, state, closedBy, body}]`.
  - Bench-changes step (bash): for each `A B` from `gate-pairs`, `git diff --quiet "$A" "$B" -- bench/` with exit 0 = false, 1 = true, anything else (unknown commit) = omitted; write `bench-changes.json` with `jq`.
  - Gate step: `java -jar "${jar}" gate --data perf/data --issues issues.json --bench-changes bench-changes.json --out plan.json`; `cat plan.json` into the job summary inside a `<details>` block.
  - `env: LIVE: ${{ (github.event_name == 'schedule' && vars.PERF_REGRESSION_LIVE == 'true') || (github.event_name == 'workflow_dispatch' && !inputs.dry_run) }}`.
  - Apply step `if: env.LIVE == 'true'`, `actions/github-script`: create the label if `getLabel` 404s (color `d93f0b`, description `Automated: a nightly or release benchmark got worse`); then the actions in order: `create` (remember `key` to number), `update`, `comment` (list the issue's comments, edit the one whose body starts with `<!-- perf-regression-run ${runKey} -->`, else create), `close` (comment then `update state: closed, state_reason: completed`).
  - Pinned SHAs copied from `bolt-nightly.yml`: checkout `3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`, setup-java `77dd5b2ce225ea11469928ac317850245a74ae78 # v6.2.0`, github-script `3a2844b7e9c422d3c10d287c895573f7108da1b3 # v9.0.0`.
- [ ] **Step 2: Verify** `actionlint .github/workflows/perf-regression.yml` (if installed) and `python3 -c "import yaml,sys; yaml.safe_load(open(sys.argv[1]))" .github/workflows/perf-regression.yml` - expected: no output. Run the gate step's commands locally against a checkout of arcadedb-perf with `issues.json` = `[]` - expected: the same plan as the fixture test's, plus the 2026-11-21 26.11.1 re-run if it has published.
- [ ] **Step 3: Commit** `ci(perf): judge published benchmark runs every 6 hours and open regression issues`

### After the merge (the maintainer, not a task)

1. `gh workflow run perf-regression.yml -f dry_run=true` and read the plan in the job summary.
2. When it is right, set the repository variable `PERF_REGRESSION_LIVE=true`; the next scheduled run opens the issues.
