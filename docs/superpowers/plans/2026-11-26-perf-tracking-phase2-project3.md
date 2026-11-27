# Performance tracking phase 2, project 3 (issue scenarios) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `issues` workload: one case per fixed `performance` issue whose fix we want to keep, judged by a ratio of
two forms of the same query on the same server or by a fitted exponent, so a regression of a fixed performance bug fails
its case on the next night, on a noisy host too.

**Architecture:** One `IssuesWorkload` in `bench/src/main/java/com/arcadedb/bench/workload`, cases `issue-<number>`, each
backed by a small `IssueScenario` (dataset, the two forms or the sweep, the limit). A pure `IssueJudge` turns timings into
the metric and the verdict and is unit-tested for every branch. The gate gains absolute floors for the two new units so a
nightly wobble of a ratio near 1 is not reported. One PR.

**Tech Stack:** Java 21, JUnit 5 + AssertJ, Testcontainers (IT only), ArcadeDB remote HTTP client (`RemoteDatabase`).

**Spec:** `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md`, section 3 "Issue scenarios" ("A case is either
a ratio of two forms of the same query on the same server (median of 5 after warm-up; metric `ratio`, lower is better;
failed above the issue's limit; a slow form under 5 ms is not judged) or a fitted exponent at n/4, n/2, n (metric
`exponent`; limit 1.4 for linear, 0.4 for flat)").

## Global Constraints

- JSON only through `com.arcadedb.serializer.json.JSONObject` / `JSONArray`; `final` on locals and parameters;
  single-statement `if` without braces; imports, no fully qualified names; no `System.out`; no em dash anywhere.
- Tests: `assertThat(...)`; never assert on raw wall-clock time; no hand-picked ports; Docker tests are `*IT`.
- Bench tests: `./mvnw -o -pl bench -am test -Dtest='com.arcadedb.bench.**.*Test' -Dsurefire.failIfNoSpecifiedTests=false`.
- **S sizing rule** and caps: each case is a timed measurement of at least 5 s per repetition at S (the forms are run in
  batches until then); warm-up + reps of a case fit 30 min per step and 120 min per case at every scale. S budget for the
  whole workload: **10 min per variant**.
- Units: a ratio metric uses unit **`factor`** and an exponent unit **`exponent`** (not `ratio`: `ratio` is the vector
  recall's unit and must keep a zero absolute floor). Both `better: lower`.
- Commits authored by robfrank only, no Co-Authored-By. The PR says `Part of #12433`.

## Measured on main (2f88c9cffc, embedded, 2026-11-26): every scenario is still fixed

| Case | Kind | Slow form | Reference form | Fixed value | Bug value | Limit | Dataset (S and up) |
|---|---|---|---|---|---|---|---|
| `issue-12725` | ratio | openCypher `MATCH (n:T) WHERE n.k IN [100 literals] RETURN n.k` | same with `IN $list` (the same 100 values) | 1.14 | 10.3 | **3.0** | 50k `T` |
| `issue-12713` | ratio | SQL `WHERE d > '<datetime string literal>'` on a DATETIME `d` | `WHERE d > :d` with `:d` bound to a `LocalDateTime` (NOT `date(...)`: a `date('<const>', '<fmt>')` in a WHERE is itself evaluated per record today, 3.4x) | 1.0-1.1 | 5-22 | **2.5** | 100k |
| `issue-12281` | ratio | SQL `WHERE 5 = id`, `WHERE :lo < id`, `WHERE NOT (id >= 3)` (one sample = the three) | `WHERE id = 5`, `id > :lo`, `id < 3` | 1.8-2.6 (sub-0.1 ms) | 772 | **10** | 200k, UNIQUE `id` |
| `issue-11963` | ratio | SQL `WHERE unique_key = :k AND tenant = :t` | `WHERE unique_key = :k` | 1.00 | 36-1141 | **10** | 50k, UNIQUE `unique_key`, NOTUNIQUE `tenant` (one big tenant) |
| `issue-12522` | ratio | openCypher `MATCH (n {uuid: $u}) RETURN n.uuid` | `MATCH (n:Entity {uuid: $u}) RETURN n.uuid` | 3.3-3.7 | 126 | **20** | 50k `Entity` + 2k each of 3 more types, an index on `uuid` in EVERY type |
| `issue-12141` | ratio | openCypher `MATCH (d:D) WITH d ORDER BY d.key LIMIT 10 RETURN d.key` | `MATCH (d:D) RETURN d.key ORDER BY d.key LIMIT 10` | 1.3-1.5 | 14 | **4** | 100k, indexed `key` |
| `issue-11701` | ratio | SQL `SELECT k FROM Rec ORDER BY k ASC LIMIT 10` (data in `SubRec EXTENDS Rec`, `k` MANDATORY NOTNULL, UNIQUE) | `SELECT k FROM Rec WHERE k >= 0 ORDER BY k ASC LIMIT 10` | 0.6 | 127 | **5** | 400k |
| `issue-11871` | ratio | SQL `SELECT min(k) AS m FROM T WHERE k > :lo LIMIT 10` and the same with `TIMEOUT 60000 EXCEPTION` | the same without LIMIT / TIMEOUT | 1.0-1.1 | 37-462 | **5** | 400k, indexed `k` |
| `issue-11682` | exponent | openCypher `MATCH (n:L) WHERE n.key IN $ids RETURN count(n) AS n`, `|ids|` = 4k, 8k, 16k | (sweep) | 0.47 | 1.93 | **1.3** | 16k+ `L`, UNIQUE `key` |
| `issue-11700` | exponent | SQL keyset page `WHERE graph_name = :v AND external_id > :after ORDER BY external_id LIMIT 100`, `:after` mid-group, over dataset sizes n/4, n/2, n (one type each) | (sweep over size, fixed selectivity) | -0.12 | 0.66 | **0.55** | 160k, 320k, 640k |

Rulings made while implementing (Task 2):
- `issue-11682` returns `count(n)`, not `n.key`: `|ids|` rows over HTTP made the cost linear by serialization alone. Cost if
  wrong: none for detection, the bug is quadratic (1.93) and the count still needs every seek.
- `issue-11700` limit **0.55**, not 0.4: over HTTP the fixed sweep read up to 0.46 per repetition at S. The bug proxy
  (`external_id + '' > :after`) measured over HTTP at S reads **1.20** (5.8, 9.2, 30.6 ms), so 0.55 keeps a wide margin.
  Cost if wrong: a regression to a slope between 0.55 and the bug's would pass unseen.
- A failed verdict is timed once more and fails the case only if the second timing fails too: the runner calls
  `measure` for its warm-ups too, so one noisy repetition would otherwise fail the case. Cost if wrong: a regression that
  fails only every other timing passes; a real regression is 2-20x the limit and fails twice.
- Ratio forms are compared as a multiset unless both statements order their rows. Cost if wrong: a form answering the
  same rows in another order when it should not is not caught for unordered statements, which promise no order.

Measured over HTTP (Docker, JVM, S, main image built 2026-11-26) the fixed values differ from the embedded table above:
`issue-11682` exponent about 0.93 (each execution sends, parses and seeks |ids| keys, linear by nature; the embedded 0.47
was fixed-cost dominated, HTTP read 0.89-0.93), `issue-11700` about 0.31-0.33, and the sub-millisecond ratios (#12281, #11963, #12522, #12141,
#11701, #11871) about 1.0 (the HTTP round trip dominates both forms); #12725 1.02 and #12713 1.05.

Not used: the openCypher half of #11701 (DESC on a nullable key reads every record in its "after null keys" pass, so its
ratio is about 1000 fixed or broken; a ratio cannot judge it), and the `date(...)` form as a reference (see #12713 row).

## Review Focus

- A case whose two forms return different rows measures nothing: every ratio scenario checks once in `prepare` that both
  forms return the same rows (or count), and fails the case otherwise; pinned in Task 2 (`formsMustAgree`).
- Sub-millisecond forms (#12281, #11963, #12522, #11871) are dominated by parse and plan time: one sample runs a batch of
  each form, alternating, so a slow neighbour or a GC hits both forms alike; pinned in Task 1 (`samplesAlternateForms`).
- The spec's "a slow form under 5 ms is not judged" must not hide a regression: it applies to the slow form's median per
  execution; a broken fix costs well over 5 ms per execution at the sizes above, so it is judged; pinned in Task 1
  (`aFastSlowFormIsNotJudgedButAnExpensiveOneIs`).
- An exponent fit over three points with one outlier: the fit uses the medians of 5, and the IT asserts the fitted value
  is finite; pinned in Task 1 (`exponentOfLinearAndFlatSeries`).
- The gate must not alert on a ratio moving from 1.1 to 1.3 night to night; pinned in Task 3.

---

### Task 1: IssueJudge (pure) and the sampling loop

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/issues/IssueJudge.java`, `IssueSampler.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/issues/IssueJudgeTest.java`, `IssueSamplerTest.java`

**Interfaces:**
- `record IssueJudge.Verdict(double value, boolean judged, boolean failed, String reason)`.
- `static Verdict IssueJudge.ratio(double slowMedianMs, double referenceMedianMs, double limit)`: value = slow/reference;
  `judged` = `slowMedianMs >= 5.0`; `failed` = judged and value > limit; reason names both medians and the limit.
- `static double IssueJudge.exponent(double[] sizes, double[] medians)`: least-squares slope of ln(median) on ln(size).
- `static Verdict IssueJudge.exponent(double[] sizes, double[] medians, double limit)`: judged when the largest size's
  median >= 5 ms; failed above the limit.
- `IssueSampler(LongSupplier nanos)` with `double[] alternate(Runnable slow, Runnable reference, int samples, int batch)`
  returning `{slowMedianMsPerExecution, referenceMedianMsPerExecution}`: each sample times `batch` executions of the slow
  form then `batch` of the reference, order swapped on every other sample; `static int batchFor(double msPerExecution,
  double targetMs)` sizes a batch so one sample takes about `targetMs`.

- [ ] Step 1: failing tests `ratioUnderLimitPasses`, `ratioOverLimitFails` (message names medians and limit),
  `aFastSlowFormIsNotJudgedButAnExpensiveOneIs` (slow 4.9 ms ratio 50 -> not judged; slow 6 ms ratio 50 -> failed),
  `exponentOfLinearAndFlatSeries` (sizes 1,2,4 medians 10,20,40 -> 1.0 +- 0.01; medians 10,10,10 -> 0.0),
  `samplesAlternateForms` (a fake clock and counting runnables: order slow,ref,ref,slow,...; medians per execution),
  `batchForTargetsTheSampleTime`.
- [ ] Step 2: run, expect compile failure. Step 3: implement. Step 4: run the bench unit suite; pass.
- [ ] Step 5: commit `feat(bench): judge an issue scenario by a ratio of two forms or a fitted exponent (#12433)`.

### Task 2: The issues workload and its ten scenarios

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/issues/IssueScenario.java` (interface),
  one class per scenario family as fits (e.g. `SqlRatioScenario`, `CypherRatioScenario`, `ExponentScenario` with the
  table's statements as data), `bench/src/main/java/com/arcadedb/bench/workload/IssuesWorkload.java`
- Modify: `WorkloadRegistry.java` (append `new IssuesWorkload()` at the end)
- Test: `bench/src/test/java/com/arcadedb/bench/workload/IssuesWorkloadTest.java`, `IssuesWorkloadIT.java`

**Interfaces:**
- `interface IssueScenario { String caseName(); int issue(); void load(RemoteDatabase db, Scale scale, long seed);
  void checkForms(RemoteDatabase db); Measurement measure(RemoteDatabase db, IssueSampler sampler, Scale scale); }`.
- `IssuesWorkload` named `issues`, `cases()` = the ten `issue-<n>` names in the table's order; each case gets its own
  database (`bench_issue_<n>`), loaded in `prepare` only when missing.
- Metrics per case: `factor` (unit `factor`, LOWER) or `exponent` (unit `exponent`, LOWER), plus `slow_ms` and
  `reference_ms` (unit `ms`, LOWER, per execution) for ratios, or `size_1_ms`..`size_3_ms` for exponents, so a reader sees what the
  ratio is made of. A failed verdict throws `CaseFailedWithMetrics` with the verdict's reason (the metrics stay published).

- [ ] Step 1: failing tests (`IssuesWorkloadTest`): `casesAreTheTenOfThePlan` (exact names and order), `everyLimitIsTheTables`,
  `formsMustAgree` (a fake scenario whose forms return different rows fails `checkForms` with both counts),
  `aFailedVerdictKeepsItsMetrics`.
- [ ] Step 2: run; fail. Step 3: implement the ten scenarios with the table's statements, sizes and limits; `checkForms`
  in `prepare`; 2 warm-up samples then 5 samples per repetition; batches sized so one repetition takes at least 5 s.
- [ ] Step 4: unit suite passes. `IssuesWorkloadIT` at XS on the JVM image: every case `ok`, and every `checkForms` ran.
  Then run `issues` at S once from the bench jar on Docker; put each case's value, slow/reference ms and the workload's
  total time in the report (budget 10 min per variant).
- [ ] Step 5: commit `feat(bench): issue scenarios - ten fixed performance issues kept fixed by a ratio or an exponent (#12433)`.

### Task 3: Gate floors for the new units

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/gate/Rule.java`
- Test: `bench/src/test/java/com/arcadedb/bench/gate/RuleTest.java` (or the existing gate test that covers `absoluteFloor`)

**Interfaces:** `absoluteFloor("factor")` = **1.0** (a ratio must move by a whole factor to be news), `absoluteFloor("exponent")`
= **0.25**; `ratio` keeps 0.

- [ ] Step 1: failing tests `factorWobbleIsNotReported` (1.1 -> 1.9 SAME), `factorJumpIsReported` (1.1 -> 4.0 WORSE),
  `exponentFloor` (0.45 -> 0.6 SAME; 0.45 -> 1.2 WORSE), `recallRatioKeepsNoFloor` (0.99 -> 0.85 WORSE).
- [ ] Step 2-4: implement, run every class under `bench/src/test/java/com/arcadedb/bench/gate`; `GateFixtureTest`'s pinned
  plan must not change.
- [ ] Step 5: commit `feat(bench): the gate gives an issue ratio and an exponent an absolute floor (#12433)`.

### Task 4: PR and fit check

- [ ] Push, open the PR (`Part of #12433`) with this plan, the measured table and the S numbers; handle review surfaces.
- [ ] Dispatch the nightly with `commit=<head>`, `workloads=issues`, `scale=S`, then `scale=M`; every case `ok`, inside the
  budget and caps; numbers in the PR body. Merge stays with the user.
