# Columnar Faceting - Performance Tuning Implementation Plan (plan 7, GitHub #10662)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** bring inserts and updates into a type with COLUMNAR indexes within measured noise of a type without them, and make facet queries and `GROUP BY` faster where the measurements say it pays, without ever changing a count.

**Architecture:** measure first, then pull levers in the order the measurements rank them. Task 0 makes the three engine benchmarks emit JSON (machine state included) and adds a switch per code-path lever (`ColumnarTuning`), so one JVM can alternate a lever on and off over the same data. Task 1 measures the run-to-run noise of every metric and profiles ingest and queries with async-profiler, which ranks the cost centers. Each lever task then writes its correctness test first, implements the lever behind its switch, measures it against the noise floor and keeps it only if it wins; otherwise it is reverted and recorded. Task 17 compares the final tree with the baseline tree, alternating, and writes the before/after report.

**Tech Stack:** Java 21 (JDK 25 on the bench host), JUnit 5, AssertJ, `StallAwareStopwatch`, async-profiler 4.6 (`/opt/homebrew/lib/libasyncProfiler.dylib`, `jfrconv`), `ThreadMXBean` CPU and allocation counters, bash, Python 3 stdlib (run with `python3 -I`), Testcontainers (plan 6 load and HA harness).

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 2, 5.7, 6.2-6.3, 7.2-7.4, 7.6, 10). **Roadmap:** `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md`, section "Tuning phase (#10662)" (levers, exit criterion, appended tuning items), "Pre-merge steps" (patch amplification numbers) and "Plan 6 outcome" (load baselines). Rulings: `.superpowers/sdd/plan{3,4,5,6}-rulings.md`. **Baseline commit:** `8da84eb9f3` (HEAD when this plan was written). Open questions for the user: `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/planning-notes.md`.

## User decisions (2026-11-02)

Binding for every task; they settle the questions in the planning notes.

- Task 16 (GROUP BY with a WHERE): build behind `arcadedb.columnar.groupByAccelerationWhere`, default **false** this release; measure first (Step 0) and build only if the gain beats noise; replicas stay exact through R7's engine change (the replicated apply marks pages before writing them).
- Task 15 (record-granular pending marks): build only if the Task 1 profile ranks re-reads or patching among the top costs; R6's design (no allocation per record, 16 slots per page cap).
- Task 8 (parallel counting): on `QueryEngineManager`'s existing pool (R9); default **off** unless the 1M-record broad case clearly wins; build only if counting is over about 40% of facet time at 1M records.
- Task 4 (apply lag bound): keep **500 ms** and the exit criterion; `applyThreads` stays 2; a 1000 ms run is reported for information only.
- Task 3 (flush coalescing): accepted defaults 4096 rows, 30 s and the 512-unflushed-page flush (R11); only the force of an unchanged dictionary is skipped (R10).
- Task 17: remove the `ColumnarTuning` switches in Step 1b so the final measurement runs on the tree that ships; commit the benchmark scripts under `engine/src/test/scripts/` (outside CI).
- Task 14 (page-read hardening) is done now (R20); read events in the shared hierarchy build stay documentation only.

**Shipped vs decided** (measured outcomes that differ from the decisions above; details in the roadmap's "Tuning outcome (#10662)"):
- Task 3: flush coalescing shipped with `flushMinRows` 0 and `flushMaxAge` 0 (off); 4096 rows / 30 s lost on apply lag in the
  single-server load test (spikes of 1.7-2.3 s). The skip of the force of an unchanged dictionary (`SINGLE_PUBLISH_FSYNC`) was reverted
  (NOISE). The 512-unflushed-page flush shipped, always armed.
- Task 8: `parallelCountTasks` default 4, kept on the 1M broad WIN; provisional on the load evidence (4 vs 1 at `parallelMinMatches`
  256 showed no pool starvation, batch G3). The pool question stays open for the user.

## Global Constraints

- **No git commits, no stash, no `mvn install`.** The user commits after review. Every "Commit" step of the template is "Stop: changes ready for review", listing the files touched.
- No em dash characters in code comments, docs or messages. Docs in American English. Never name the customer behind this feature.
- Java style of the repo: `final` where possible; single-statement `if` without braces; imports, never fully qualified names; no `System.out` left behind; no Claude author tags.
- JSON via `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, getters with defaults.
- Primitive arrays over collections of boxed values on every hot path; no allocation per record on the apply or counting paths unless a task says so.
- Tests: AssertJ `assertThat(x).isTrue();`. Benchmarks are `@Tag("benchmark")` and report only: they log and write JSON, they never assert a time. Correctness tests assert behavior, never a time. Long functional tests `@Tag("slow")`. No raw wall-clock asserts anywhere; time measured with `StallAwareStopwatch`. `@Timeout` only as a hang detector.
- Test servers only through fixture bases (`BaseGraphServerTest`, `StaticBaseServerTest`, `BaseRaftHATest`, `ContainersTestTemplate`); URLs from bound ports; extra ports from `allocateFreePorts` / `allocateFixturePorts`.
- Maven: module runs with `-am` (`./mvnw -o -pl engine -am test -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`), `verify` when gremlin or graphql are in the reactor. Skip lanes with `-DexcludedGroups`, never `-Dtest='!Foo'`. Benchmarks run with `-DexcludedGroups=none`.
- No `ForkJoinPool.commonPool()` and no new pool: Task 8 submits to `QueryEngineManager`'s pool per `.claude/skills/engine-concurrency/SKILL.md` (preflight R9).
- No new dependency.
- **HA code unchanged** (`ha-raft`, `SnapshotInstaller`) unless a lever requires it. One engine-side change to the Raft apply path is proposed, pending the user's ruling: Task 16 adds a pre-apply mark in `TransactionManager.applyChangesInternal` (engine module, preflight R7); the alternative is a decline of the WHERE path on replicas.
- **Queries without FACETS keep their plan and their results**, with one exception that needs the user's ruling: Task 16 (GROUP BY with a WHERE). Task 14 touches the multi-page read path that every read uses and runs the full gates.
- **Every optimization keeps counts exact**: the oracle set (O, below) stays green after every lever, and every lever's own correctness test is written and seen failing (or proven able to fail by a mutation) before the lever is implemented.
- After every Java change: compile the touched modules and fix all errors before the next step.
- Container runs under `caffeinate -dims` (plan 6 ruling).

## Measurement protocol (every lever task uses it)

- **Switches.** Every code-path lever is guarded by `ColumnarTuning.enabled(Lever)` (Task 0), default on. A benchmark alternates it on and off inside one JVM over the same data: 2 warm-up rounds, then 5 measured rounds, the order rotating (on/off, off/on, ...). A configuration lever (a `GlobalConfiguration` key) is compared by alternating whole runs with `engine/src/test/scripts/columnar-bench.sh` (5 runs per side, ABABABABAB).
- **Noise floor.** Task 1 runs every benchmark metric with `Lever.NONE` (on and off are the same code): per metric, `noise = max(|median_on - median_off| / median_off, dev(on), dev(off))`, where `dev(x) = median(|x_i - median(x)|) / median(x)` is the median relative deviation of the rounds (median-based, so one outlier round cannot set it; the first version used the range `(max - min) / (2 * median)`, which one outlier dominated and more rounds never lowered, Task 1 concern 4). Stored in `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/bench/noise-floor.json`. The GC figures (`gcCount`, `gcMs`) stay in the JSON but are not verdict metrics (floors 0.65-1.0 at 0-19 collections per phase); the apply allocation is the GC proxy.
- **Verdict** (computed by `columnar_bench_compare.py`): for a lower-is-better metric, `delta = (median_off - median_on) / median_off`. **WIN** iff `delta > max(noise floor of the metric, dev of either side of this run)` and `median_on < min_off`. **LOSS** iff the symmetric condition holds the other way. Otherwise **NOISE**. Fewer than 5 samples on a side is **INVALID** (nonzero exit); a compare without `--noise` warns that it is not protocol-valid.
- **Decision gate per lever.** Keep only on WIN of its primary metric with no LOSS on its guard metrics (apply lag, facet query latency, catch-up, GC). A correctness-shaped item (Tasks 12, 14) is kept on "tests green and no LOSS". Anything else is reverted (code, switch constant and benchmark method removed; correctness tests kept only when they pin behavior that still exists) and recorded. Every verdict is a row in `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/levers.md`: lever, task, metric, median on/off, delta, noise, verdict, kept or reverted, JSON file names.
- **Machine discipline.** AC power; no other Maven build, surefire fork or container on the host (`pgrep -fl 'surefire|maven|docker run'` empty); a run whose JSON reports a 1-minute load average above `cores / 2` at start is discarded and rerun. The JSON records it all (Task 0).
- **Lever selection follows the measured ranking.** Task 1 ends with a ranking of cost centers; the executor builds the lever tasks in that order (the order below is the expected one) and records in `levers.md` every lever whose cost center measured below its noise floor as "not built: below noise", with the number. Tasks 9-14 come from the recorded tuning list and are built regardless of rank, since each also fixes a behavior (plan cache churn, double firing, cancellation, an unsafe walk).

## Gates (referenced by the tasks)

**O, oracle set** (every lever):
```bash
./mvnw -o -pl engine -am test -Dtest='ColumnarEquivalenceTest,FacetOracleSelfTest,ColumnarDivergenceStressTest,FacetPatchingTest,SQLFacetsIntegrationTest,DisjunctiveFacetsTest,ColumnarGroupByAccelerationTest,FacetReadEventsTest,HierarchyFacetIntegrationTest' -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: `Failures: 0, Errors: 0`.

**G1, engine and SQL regression set** (every lever; plan 6 gate step 2a, with `-am`):
```bash
./mvnw -o -pl engine -am test -Dtest='com/arcadedb/index/columnar/**/*Test,com/arcadedb/query/facet/**/*Test,ColumnarGroupBy*Test,com/arcadedb/query/sql/**/*Test,com/arcadedb/function/sql/**/*Test,SelectStatementExecutionTest,*Planner*Test,*GroupBy*Test' -DexcludedGroups=benchmark -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: about 5284 run, 0 failures, at most the one known error `CreateDocumentTypeStatementExecutionTest.edgeTypePageSize` (red on main, #10876). Tasks 14 and 15 add `'LocalBucket*Test,*MultiPage*Test,*Chunk*Test,*Transaction*Test,*WAL*Test'` to the list.

**G2, server, metrics, gRPC and HA modules** (every lever that touches the apply, commit, publish or query paths):
```bash
./mvnw -o -pl server,metrics,grpc-client,ha-raft,integration -am verify -DskipITs=false \
  -Dit.test='Columnar*IT,Facet*IT,PrometheusColumnar*IT,SnapshotInstallerIntegrationIT' -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dtest='Columnar*Test,PoolMetrics*Test,SnapshotInstaller*Test' -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: BUILD SUCCESS; the plan 6 gate classes (ColumnarMetricsIT 8, FacetsHttpIT 12, FacetAdvancedHttpIT 5, FacetsSubtypeAclIT 12, PrometheusColumnarQueryMetricsIT 1, FacetsGrpcIT 14, FacetsFollowerQueryIT 5, ColumnarSnapshotInstallRebuildIT 2, SnapshotInstallerIntegrationIT 1, SnapshotInstallerRetryTest 5, SnapshotInstallerSSLTest 2, ColumnarAcquireNewDatabaseRebuildTest 2, ColumnarSnapshotExclusionTest 4) all green, and so are the other classes the command selects: `ColumnarFollowerConvergenceIT`, `ColumnarFollowerCheckpointCrashIT`, `ColumnarForwardedWritesIT`, `ColumnarLeaderFailoverIT`, `ColumnarNoRaftTrafficIT`, `PrometheusColumnarMetricsIT`, `FacetsMemoryBudgetIT`; `-Dtest='Columnar*Test'` with `-am` also runs every engine `Columnar*Test` (slow ones included).

**G3, plan 6 final gate** (from `.superpowers/sdd/2026-10-19-columnar-faceting-6-resilience-load/final-review-B.md` "Final-gate commands"; `IMG=arcadedata/arcadedb:26.11.1-facets-tuning-<task>`). Required by every lever that touches shared engine code (any file outside `index/columnar`, `query/facet`, `function/sql/facet`, `query/sql/executor/{Facet*,ColumnarGroupBy*}`) and at the two batch ends (after Task 5, after Task 14) and in Task 17. Consecutive levers may share one G3 run at the batch end; a red G3 is bisected by switching levers off one at a time.
```bash
./mvnw -o package -Pdocker -pl package -am -DskipTests -Ddocker.skip=true
docker build -t $IMG package/target/arcadedb-26.11.1-SNAPSHOT.dir
./mvnw -o -pl e2e-ha,load-tests -am verify -DskipITs -Dtest='com/arcadedb/containers/ha/**/*Test,ContainersTestTemplateTest' -Dsurefire.failIfNoSpecifiedTests=false
caffeinate -dims ./mvnw -o verify -Pintegration -pl e2e-ha -am --batch-mode -Dit.test=HaChaosIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dtest='com/arcadedb/containers/ha/chaos/ChaosConfigTest' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Dfailsafe.excludedGroups= -Darcadedb.test.image=$IMG \
  -Dchaos.seed=43 -Dchaos.duration=PT30M -Dchaos.maxSteps=10 -Dchaos.faults=kill:3,pause:3,rolling,isolate,loss,latency \
  '-Dchaos.serverOpts=-Darcadedb.ha.snapshotThreshold=500 -Darcadedb.ha.logPurgeGap=16'
caffeinate -dims ./mvnw -o verify -Pintegration -pl e2e-ha -am --batch-mode -Dit.test=HaChaosIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dtest='com/arcadedb/containers/ha/chaos/ChaosConfigTest' -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Dfailsafe.excludedGroups= -Darcadedb.test.image=$IMG \
  -Dchaos.seed=42 -Dchaos.duration=PT20M -Dchaos.maxSteps=6 -Dchaos.faults=kill,rolling,isolate,loss,pause,latency \
  '-Dchaos.serverOpts=-Darcadedb.ha.snapshotThreshold=500 -Darcadedb.ha.logPurgeGap=16 -Darcadedb.columnar.patchBudget=0 -Darcadedb.columnar.patchBudgetRatio=0'
caffeinate -dims ./mvnw -o verify -Pintegration -pl e2e-ha -am --batch-mode \
  -Dit.test='LeaderFailoverIT#leaderFailoverNewReplicaRebuildsFacets,RollingRestartIT#rollingRestartRecoversFacetProjectionsWithoutRebuild,NetworkPartitionRecoveryIT#partitionRecoveryKeepsFacetsExact,SplitBrainIT#splitBrainMinorityLeaderRejoinsWithExactFacets,PacketLossIT#moderatePacketLossKeepsFacetsExact' \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dtest='com/arcadedb/containers/ha/facet/FacetSchemaTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Darcadedb.test.image=$IMG
caffeinate -dims ./mvnw -o verify -Pintegration -pl load-tests -am --batch-mode \
  -Dit.test='SingleServerFacetLoadTestIT,ThreeNodesFacetLoadTestIT,SingleServerTimeSeriesLoadTestIT,ThreeNodesTimeSeriesLoadTestIT' \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dtest='ContainersTestTemplateTest,FacetDatabaseWrapperTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Darcadedb.test.image=$IMG
```
Pass criteria (gate report 2026-10-30): chaos `CHAOS result PASS`, `facetWarnings []`, seed 43 draws kill and pause (else seed 44, 45), seed 42 `facets.nonExact > 0` and `metaInconsistent == 0`; five HA methods green; four load JSONs `"passed": true`, nonExact 0, failedQueries 0, rebuilds 0, fallback scans 0 during the run. `ThreeNodesTimeSeriesLoadTestIT` red is classified against #10970 first. Copy `load-tests/target/facet-load/*.json` into `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/bench/g3-<task>/`.

## Review Focus

1. **A change no pending mark names.** Record-granular pending (Task 15) is exact only if every write path names its records; a placeholder move, a neighbour relocated off its page by another record's growth (its page is marked whole), a chunk chain grown by an update that rewrites only the content page, an external-property body, a slot freed and reused within one pass, an edge added to a vertex, a bulk write without WAL and a Raft apply must each still reach the facets. Pinned in Task 15 (`recordPendingOracleUnderEveryMutationKind`, `pathsWithoutRecordsMarkWholePages`).
2. **GROUP BY with a WHERE under a concurrent commit.** A row the WHERE accepted on its new version must never be counted from older column values (plan 5 review C1), and a replayed row must pass the complete WHERE again (I1). Pinned in Task 16 (`concurrentCommitBetweenAcquireAndPullNeverYieldsAnImpossibleGroup`, `replayReEvaluatesTheCompleteWhere`, `slotReusedAfterAcquireIsNotCountedWithTheDeletedRecordsColumns`).
3. **A pooled counter that is not clean or not charged.** A counter array reused by the next query must start at zero, and an idle pooled array must count against `queryMemoryBudget`. Pinned in Task 6 (`pooledArrayIsZeroOnBorrow`, `idlePooledBytesCountAgainstTheBudget`).
4. **A longer-lived overlay at a crash.** Flush coalescing (Task 3) keeps more rows only in memory; a kill -9 with a coalesced overlay must recover to the oracle with no rebuild (unflushed pages stay below `REAPPLY_FLOOR`), and no WAL file may be dropped before the recovery set covering those rows is fsynced. Pinned in Task 3 (`crashWithCoalescedOverlayRecoversWithoutRebuild`, `walDropWaitsForTheCoalescedOverlaysRecoverySet`).
5. **A cached plan reused for the next page.** With `AFTER :c` cached (Task 9), page 2 must not repeat page 1, and a cursor value that the first execution would refuse must still be refused on the cached plan. Pinned in Task 9 (`pagingWithCachedPlanVisitsEveryValueOnce`, `invalidCursorRefusedOnCachedPlan`).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `engine/src/main/java/com/arcadedb/index/columnar/ColumnarTuning.java` (new) | Lever enum and on/off switches for A/B measurement | 0 |
| `engine/src/test/java/com/arcadedb/index/columnar/BenchmarkReport.java` (new, `public`: used from `query.facet`) | Samples per metric and variant, medians, spread, alternation, JSON with machine state | 0 |
| `engine/src/test/java/com/arcadedb/index/columnar/{ColumnarIngestOverheadBenchmark,ColumnarCountBenchmark}.java`, `engine/src/test/java/com/arcadedb/query/facet/FacetsVsGroupByBenchmark.java` (modify) | Emit JSON; lever variant; apply and maintenance thread CPU and allocation | 0 |
| `engine/src/test/java/com/arcadedb/query/facet/FacetTuningBenchmark.java` (new) | One report-only method per query-side lever | 0 (shell), 6-13, 16 |
| `engine/src/test/scripts/columnar-bench.sh`, `columnar_bench_compare.py`, `test_columnar_bench_compare.py` (new) | Runbook: run, capture machine state, compare, noise floor, profile ranking | 0 |
| `engine/src/main/java/com/arcadedb/index/columnar/ColumnarApplier.java` (`onSlot`) | Allocation-free decode; deleted chunked record as empty slot; re-read only marked slots (build order 2, 14, 15) | 2, 14, 15 |
| `engine/src/main/java/com/arcadedb/index/columnar/{ColumnarValueExtractor,GlobalDictionary}.java` | Allocation-free decode | 2 |
| `engine/src/main/java/com/arcadedb/index/columnar/{ColumnarIndexManager,ColumnarFlusher,ColumnarStore,ColumnarManifest}.java`, `GlobalConfiguration.java` | Flush coalescing, no redundant publish force, apply lag bound | 3, 4 |
| `engine/src/main/java/com/arcadedb/database/TransactionContext.java:3775-3814`, `DirtyPageTracker.java` | Commit-path marks | 5, 15 |
| `engine/src/main/java/com/arcadedb/query/facet/{FacetMemoryBudget,DenseFacetCounter,TermsCollector,CrossBucketOrdinalMap,FacetExecutor}.java` | Counter pooling, restricted ordinal maps, parallel counting (on `QueryEngineManager`'s pool), one hierarchy view | 6, 7, 8, 12 |
| `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestData.java` | `authors` knob (default 50) | 6 |
| `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestHooks.java` | `HIERARCHY_RECORD_READS`; `reach` made public as a test seam | 11, 16 |
| `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestAccess.java` | `hierarchyRebuildCount(HierarchyManager)` | 12 |
| `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java:284-300` | AFTER parameter no longer blocks plan caching | 9 |
| `engine/src/main/java/com/arcadedb/function/sql/facet/SQLFunctionFacetSearchValues.java` | Direct count path, fixed statement shape | 10 |
| `engine/src/main/java/com/arcadedb/index/columnar/HierarchyBuilder.java` | One scan, cancellation in CPU passes | 11 |
| `engine/src/main/java/com/arcadedb/query/sql/executor/FacetCollectStep.java`, `query/facet/FacetFallbackScanner.java` | Facet values from the rows of a read-event type | 13 |
| `engine/src/main/java/com/arcadedb/engine/{LocalBucket,PageManager}.java` | Walk redone under the lock when a publication overlapped; deleted chunked record as empty slot | 14 |
| `engine/src/main/java/com/arcadedb/query/sql/executor/{ColumnarGroupByPlanner,ColumnarGroupByStep}.java`, `index/columnar/ColumnarSnapshot.java`, `GlobalConfiguration.java` | GROUP BY with a WHERE, second-snapshot validation | 16 |
| `engine/src/main/java/com/arcadedb/engine/TransactionManager.java:1597-1618` | Pre-apply mark on the replicated apply (pending the user's ruling, R7) | 16 |
| `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md`, `docs/superpowers/specs/2026-10-28-columnar-faceting-overview.md` | "Tuning outcome (#10662)" and updated cost figures | 17 |

## Independence and rulings

- **Independent, any order:** Tasks 2, 3, 4, 6, 7, 9, 10, 11, 12, 13, 14. Each touches its own files (Tasks 2, 14 and 15 all edit `ColumnarApplier.onSlot`: build 2, then 14, then 15). Task 5 only after Task 1 shows the commit-path cost above noise: not built (0.05% of committer CPU, Task 1).
- **Need a ruling from the user before starting** (questions in `planning-notes.md`): Task 8 (parallel counting on `QueryEngineManager`'s pool, and its default), Task 15 (commit path carries one bit per record), Task 16 (changes default GROUP BY execution; pre-apply mark on replicas or a decline there), the default values of Tasks 3 and 4 and Task 3's reduced fsync scope (the code and keys can be built and measured before the ruling; defaults change only after it), and Task 17's removal of `ColumnarTuning`.
- **Sequenced:** Task 0 then 1 first; 15 before 16 when both are approved (16's tests then also cover record-granular pending); 17 last.

---

### Task 0: Benchmark harness, JSON reports and lever switches

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarTuning.java`
- Create: `engine/src/test/java/com/arcadedb/index/columnar/BenchmarkReport.java`, `BenchmarkReportTest.java`, `ColumnarTuningTest.java`
- Create: `engine/src/test/java/com/arcadedb/query/facet/FacetTuningBenchmark.java` (class shell with the shared fixture only)
- Create: `engine/src/test/scripts/columnar-bench.sh`, `engine/src/test/scripts/columnar_bench_compare.py`, `engine/src/test/scripts/test_columnar_bench_compare.py`
- Modify: `ColumnarIngestOverheadBenchmark.java`, `ColumnarCountBenchmark.java`, `FacetsVsGroupByBenchmark.java`

**Interfaces:**
- Produces:
```java
public final class ColumnarTuning {
  public enum Lever { NONE, DIRECT_DECODE, SINGLE_PUBLISH_FSYNC, SKIP_REDUNDANT_MARK, POOLED_COUNTERS, CACHED_RESTRICTED_ORDINAL_MAPS,
                      CACHED_AFTER_PLANS, SEARCH_VALUES_DIRECT, HIERARCHY_ONE_PASS, HIERARCHY_ONE_VIEW, READ_EVENTS_FROM_ROWS, RECORD_PENDING }
  public static boolean enabled(Lever lever);                      // plain array read, default true; NONE always true
  public static AutoCloseable override(Lever lever, boolean on);   // tests and benchmarks only, restores the previous value on close
  static boolean[] parse(String disabledList);                     // "-Darcadedb.columnar.tuning.off=A,B" read once at class init
}
public final class BenchmarkReport {                               // engine test-jar, public: used from com.arcadedb.query.facet
  public BenchmarkReport(String benchmark);
  public void add(String metric, String unit, String variant, double value);
  public static double median(double[] values); public static double spread(double[] values);   // (max - min) / median
  public void alternate(ColumnarTuning.Lever lever, int warmups, int rounds, String metric, LongSupplier runNanos);
  public Path write();              // null unless -Dcolumnar.bench.label; to ${columnar.bench.out:-target/columnar-bench}/<benchmark>-<label>-<epochMs>.json
  public static JSONObject machine(); // cores, os, arch, java.version, maxMemory, loadAverage, columnar.bench.rev, columnar.bench.dirty, startedAt
}
```
  JSON: `{benchmark, label, lever, machine{...}, metrics{<name>{unit, variants{<variant>[samples]}}}}`.
- Every benchmark honors `-Dcolumnar.bench.rounds` (default 5), `-Dcolumnar.bench.lever` (default `NONE`) and, for the ingest benchmark, `-Dcolumnar.bench.variants` (subset of `A,B,B',Boff`, default all). The ingest benchmark gains variant `Boff` (B with `-Dcolumnar.bench.lever` switched off; the rotation becomes A, B, B', Boff) and per phase the metrics `applyCpuMs`, `applyAllocMb`, `maintenanceCpuMs` (sums of `ThreadMXBean` CPU time and allocated bytes over threads named `ArcadeDB-ColumnarApply-*` and `ArcadeDB-ColumnarMaintenance-*`), `recordsReread`, `flushCount`, `mergeCount` (from `ColumnarStats`).
- `columnar-bench.sh <label> <rounds> <target>`, target in `ingest | count | facets | tuning:<method> | load-single | load-three`; env `BENCH_DIR` (default `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/bench`), `BENCH_LEVER`, `BENCH_OPTS` (extra `-D` settings), `BENCH_IMAGE` (load targets). Writes `machine-<label>-<ts>.json` (git rev and dirty count, `uptime`, `sysctl -n hw.ncpu hw.memsize machdep.cpu.brand_string`, `pmset -g batt` first line, `pmset -g therm`, count of other java/docker processes, `java -version`) and moves the JSON reports there.
- `columnar_bench_compare.py compare <dirOrFileA> <dirOrFileB> [--noise noise-floor.json]`, `noise <dir>` (writes `noise-floor.json`), `rank <collapsed.txt> <categories.tsv>`; prints a table and the verdict rule of the Measurement protocol.

- [ ] **Step 1: Write the failing tests**

`BenchmarkReportTest`: `medianAndSpreadOfFiveSamples` (`{10, 12, 11, 30, 9}`: median 11, spread `21 / 11`); `writesNothingWithoutLabel` (`write()` is null); `jsonCarriesMachineStateAndEveryVariant` (keys `machine.cores`, `machine.loadAverage`, `metrics.m.variants.on` and `.off` with 5 samples each); `alternateRotatesOrderAndRestoresTheLever` (recorded order on, off, off, on, on, off, ...; `ColumnarTuning.enabled(lever)` is true afterwards). `ColumnarTuningTest`: `everyLeverDefaultsOn`, `overrideRestoresOnClose`, `parseTurnsListedLeversOff` (`parse("DIRECT_DECODE, RECORD_PENDING")`), `parseRejectsAnUnknownLever` (IllegalArgumentException naming it). `test_columnar_bench_compare.py` (unittest): `test_win_beyond_noise`, `test_within_noise_is_noise`, `test_overlapping_ranges_is_noise`, `test_regression_is_loss`, `test_noise_floor_file_raises_threshold`, `test_rank_sums_samples_by_category`.

- [ ] **Step 2: Run them, expect failures**

Run: `./mvnw -o -pl engine -am test -Dtest='BenchmarkReportTest,ColumnarTuningTest' -Dsurefire.failIfNoSpecifiedTests=false` and `python3 -I engine/src/test/scripts/test_columnar_bench_compare.py`
Expected: compilation failure (classes missing); Python `ModuleNotFoundError` / failures.

- [ ] **Step 3: Implement the classes and scripts above; wire the three benchmarks to `BenchmarkReport` (every logged figure is also added as a sample) and add the ingest metrics listed in Interfaces**

The benchmarks keep their log lines unchanged. `ColumnarTuning` holds a `static final boolean[]`, written only by `override` (benchmarks flip it between rounds, while no pass runs; document that it is not a production setting).

- [ ] **Step 4: Run the tests and a smoke run of each benchmark**

Run the Step 2 commands: PASS. Then `BENCH_LEVER=NONE engine/src/test/scripts/columnar-bench.sh smoke 1 <target>` for `ingest`, `count` and `facets`: one JSON per benchmark under `$BENCH_DIR/smoke/`, `machine-smoke-*.json` present, `python3 -I engine/src/test/scripts/columnar_bench_compare.py compare "$BENCH_DIR/smoke" "$BENCH_DIR/smoke"` prints NOISE for every metric. (Here and below, every `bench/...` argument means `"$BENCH_DIR/..."`; `BENCH_DIR` defaults to `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/bench`.)

- [ ] **Step 5: Save the harness patch for the baseline tree**

`{ git diff 8da84eb9f3 -- <the three modified benchmarks>; for f in <every file Task 0 created>; do git diff --no-index /dev/null "$f" || true; done; } > .superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/task0-harness.patch` (used by Task 17 to give the baseline the same harness; `git diff <rev>` alone omits untracked files, and `--no-index` exits 1 when files differ). Prove it: `git -C <scratch worktree at 8da84eb9f3> apply --check` on the patch. Stop: changes ready for review (files above).

---

### Task 1: Noise floor and profile ranking

**Files:**
- Create (working notes, not committed): `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/bench/noise-floor.json`, `profile/*.jfr`, `profile/*.html`, `profile/categories.tsv`, `ranking.md`, `levers.md`

**Interfaces:**
- Consumes: Task 0 harness.
- Produces: `noise-floor.json` (per benchmark metric), `ranking.md` (cost centers with their share of apply, commit and query CPU and allocation, and the lever order derived from it).

- [ ] **Step 1: Noise floor, engine benchmarks**

Run, under `caffeinate -dims` and the machine discipline: `BENCH_LEVER=NONE engine/src/test/scripts/columnar-bench.sh nf 5 ingest`, then `... nf 5 count`, then `BENCH_OPTS=-DfacetBench.records=200000 ... nf 5 facets`, and `... nf 5 facets` with `-DfacetBench.records=1000000` (the spec's scale point). Then `python3 -I engine/src/test/scripts/columnar_bench_compare.py noise "$BENCH_DIR/nf"`.
Expected: `noise-floor.json` with one entry per metric; insert/update noise of the order of the plan 3 ledger ranges (A 377-397 ms, about 2.5%). A metric with noise above 10% is flagged in `ranking.md` as unusable for verdicts, and its benchmark gets more rounds (`-Dcolumnar.bench.rounds=9`) before any lever relies on it.

- [ ] **Step 2: Noise floor, load tests**

Build `arcadedata/arcadedb:26.11.1-facets-tuning-base` from the current tree (G3 image commands). Run `BENCH_IMAGE=... engine/src/test/scripts/columnar-bench.sh nf 2 load-single` (two runs) and compare with the plan 6 gate JSONs (`load-tests/target/facet-load/` of 2026-10-30, ops/s 1045.9 HTTP, query p50 22.5 ms, lagMs p95 492, patched p95 6554). Record the spread per metric; load numbers are guard metrics only (never a primary WIN).

- [ ] **Step 3: Profile ingest**

```bash
P=.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/profile
# -DargLine does not reach the surefire fork (the POM's <argLine>@{argLine} ...</argLine> late-binds the empty POM property):
# attach the agent through JAVA_TOOL_OPTIONS, one JFR per JVM (%p = pid), and keep the surefire fork's file (stacks with ForkedBooter)
JAVA_TOOL_OPTIONS="-agentpath:/opt/homebrew/lib/libasyncProfiler.dylib=start,event=cpu,interval=1ms,alloc=512k,lock=1ms,jfr,file=$PWD/$P/%p.jfr" \
  caffeinate -dims ./mvnw -o --batch-mode -pl engine -am test -Dgroups=benchmark -DexcludedGroups= -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ColumnarIngestOverheadBenchmark -Dcolumnar.bench.variants=B -Dcolumnar.bench.rounds=3
mv $P/<fork pid>.jfr $P/ingest-B.jfr
jfrconv --cpu --threads $P/ingest-B.jfr $P/ingest-B-cpu.html
jfrconv --alloc --threads $P/ingest-B.jfr $P/ingest-B-alloc.html
jfrconv --lock $P/ingest-B.jfr $P/ingest-B-lock.html
jfrconv --cpu --threads -o collapsed $P/ingest-B.jfr $P/ingest-B-cpu.collapsed
python3 -I engine/src/test/scripts/columnar_bench_compare.py rank $P/ingest-B-cpu.collapsed $P/categories.tsv
```
The same recipe is in `engine/src/test/scripts/columnar-bench.sh --help`. Collapsed frames use `/` separators and `_[j]`/`_[i]` suffixes; `rank` takes the first matching `categories.tsv` line, so list the callee categories (dictionary, compare, publish, decode) before their callers (extract, pageRead).
`categories.tsv` (thread regex, frame regex, category): apply threads with `newImmutableRecord|BinarySerializer` = **apply.decode**, `ColumnarValueExtractor|RecordReader` = **apply.extract**, `GlobalDictionary` = **apply.dictionary**, `liveValueLookup|Pass.stage|Pass.same` = **apply.compare**, `forEachCommittedRecordSlot|visitCommitted|getImmutablePage|assembleFromNewestCommitted` = **apply.pageRead**, `Pass.publish|OverlayPageBlock` = **apply.publish**; maintenance threads with `ColumnarFlusher|SegmentWriter` = **flush**, `MergePolicy|merge` = **merge**, `force|fsync` = **fsync**; committer thread with `markPages|DirtyPageTracker` = **commit.marks**; everything else on committer = **commit.other**. Allocation profile summed the same way.

- [ ] **Step 4: Profile queries**

Same `JAVA_TOOL_OPTIONS` agent recipe (Step 3) on `-Dtest=FacetsVsGroupByBenchmark#oneBucket` (`-DfacetBench.records=200000`), then on the `lagging` phase alone if the benchmark exposes it (otherwise the whole run, read by phase from the HTML). Categories: `MatchSet` = **match**, `forEachOrdinal|DenseFacetCounter|HashFacetCounter` = **count**, `FacetPatcher` = **patch**, `CrossBucketOrdinalMap` = **ordinalMap**, `FacetRanker` = **rank**, `SelectExecutionPlanner|SqlParser|AntlrParser` = **plan**.

- [ ] **Step 5: Write `ranking.md` and the lever order**

One table: category, share of apply CPU (or query CPU), share of allocation, lever that addresses it, expected gain = share x achievable reduction, decision (build in order N / not built: below noise). Required lines: the apply share of total CPU during B inserts versus `(B - B') / B` from the ledger (+6-10% vs +2-4%), and the commit-path share (decides Task 5). Initialize `levers.md` with the header row, then one row per roadmap lever not planned here ("skip pages whose only change is outside projected properties", "off-heap buffers reuse", "patching per page read, a budget expressed in pages read"), with the cost center that would justify it and "not built: <reason or ranking number>". Stop: no source changes in this task.

---

### Task 2: Allocation-free apply decode

**First lever (Task 1 ruling): allocation-free old-value lookups in the apply pass.** Task 1 ranked apply.compare first (44.6% of apply CPU, 45.9% of apply allocation): every old-value lookup (`liveValueLookup` -> `SegmentReader`) allocates a `SegmentReader$MemoSlot`, a `BlockCache$Key` and a lambda. Built before the decode work below, as its own lever with its own switch (`ColumnarTuning.Lever.ALLOCATION_FREE_LOOKUP`, added by this task), correctness test and decision gate; scope stays inside the apply pass.
- Files: `ColumnarApplier.java` (`liveValueLookup`, `Pass.stage` / `Pass.same`), `SegmentReader.java` (memo slot and block key reuse per pass, no capturing lambda per call), `engine/src/test/java/com/arcadedb/index/columnar/ColumnarOldValueLookupTest.java` (new).
- Step L1 (failing test first): `oldValueLookupEqualsTheAllocatingLookupForEveryColumnKind` (for every column kind, deleted rows, overlay vs segment rows, a row moved between segments by a merge, a value appended to the dictionary in the same pass: the reused-slot lookup returns exactly what the allocating one returns, lever on and off) and `lookupAllocatesNothingPerCall` (ThreadMXBean allocated bytes of 10,000 lookups on the test thread below one object per lookup with the lever on; above with it off, so the test can fail). Run red (compilation failure: lever missing).
- Step L2: implement behind the switch; O and G1 green.
- Step L3 (decision gate): `BENCH_LEVER=ALLOCATION_FREE_LOOKUP engine/src/test/scripts/columnar-bench.sh alloc-free-lookup 9 ingest`, then `compare <dir> <dir> --pair Boff:B --noise "$BENCH_DIR/noise-floor.json"`. Primary: `insert.applyCpuMs`, `insert.applyAllocMb`, `update.applyCpuMs`, `update.applyAllocMb`; guards: `catchUp.ms`, `update.recordsReread`. Keep on WIN of a primary metric with no LOSS on a guard, else revert; row in `levers.md`. Then the decode lever below.

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarApplier.java:730-800` (`onSlot`, `extract`)
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarValueExtractor.java` (`RecordReader`)
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/GlobalDictionary.java`
- Test: `engine/src/test/java/com/arcadedb/index/columnar/ColumnarDirectDecodeTest.java` (new), `GlobalDictionaryTest.java`

**Interfaces:**
- Consumes: `ColumnarTuning.Lever.DIRECT_DECODE`.
- Produces: `RecordReader.begin(Binary content, DocumentType type, DatabaseInternal db): boolean` (false: not a projected record kind, as `!(record instanceof Document)` today); `GlobalDictionary.getOrAddUtf8(Binary buffer, int offset, int length): int` and `ordinalOfUtf8(...)`, which return exactly the ordinal `getOrAdd(String)` / `ordinalOf(String)` return for the decoded string.

- [ ] **Step 1: Write the failing tests**

`ColumnarDirectDecodeTest.directDecodeEqualsRecordDecodeForEveryRecordKind`: 10,000 `ColumnarTestData(42)` records plus a vertex with in and out edges, an edge type with a COLUMNAR property, a record holding `"12"` in a LONG property (coerced), a BY ITEM list with duplicates, a null and a missing property, a record in a bucket with external properties; for each, the `RowValues` (presence, ordinals, longs, doubles, lists, failures, coerced flags) of the direct path equal those of the `newImmutableRecord` path. `GlobalDictionaryTest.utf8LookupAgreesWithStringLookup`: ASCII, non-ASCII (`"Zürich"`, an emoji), empty string, a value appended before and after a reopen of the dictionary generation; `ordinalOfUtf8` of an absent value is -1.

- [ ] **Step 2: Run, expect compilation failure**

Run: `./mvnw -o -pl engine -am test -Dtest='ColumnarDirectDecodeTest,GlobalDictionaryTest' -Dsurefire.failIfNoSpecifiedTests=false`

- [ ] **Step 3: Implement**

`onSlot` reads the projected properties from `content` through `RecordReader.begin(Binary, ...)` (one header walk, as today, skipping the vertex/edge header exactly as `BinarySerializer` does) when `DIRECT_DECODE` is on; the dictionary lookups of one record are batched per column (one `getOrAddUtf8` loop per list) rather than one call per value through the sink; a string value of an ORDINAL or ORDINAL_LIST column goes to `getOrAddUtf8` without building a `String`: `getOrAddUtf8` computes `String.hashCode()` of the decoded UTF-16 units from the bytes, probes the existing `table`, and verifies a hit by comparing those units with the stored `String`; only a miss builds the `String` it appends. No second table, no stored bytes (heap grows with cardinality only, spec 5.7). The record path (`newImmutableRecord`) stays for a bucket with an external-property bucket and for a record with a column not walked by `RecordReader.prepare` (nested path, `@` property, a name unknown at `prepare`). Other kinds keep their conversion. A deserialization failure stays "unreadable" exactly as today.

- [ ] **Step 4: Run the tests, then O and G1**

Expected: PASS; O and G1 green.

- [ ] **Step 5: Measure and decide**

`BENCH_LEVER=DIRECT_DECODE engine/src/test/scripts/columnar-bench.sh direct-decode 9 ingest`, compare B vs Boff (`--pair Boff:B --noise ...`). Primary: `insert.applyCpuMs`, `insert.applyAllocMb`, `update.applyCpuMs` (`insert.ms` reported, not a primary: its floor is about 10%); guards: `update.ms`, `catchUp.ms`. Record in `levers.md`. Expected: apply allocation down by most of the per-record `ImmutableDocument` and `String`; insert overhead toward the B' floor.

- [ ] **Step 6: G2 (apply path), then stop: changes ready for review**

Layers: unit (both tests), integration (O, G1), HA integration (G2), load (G3 at the batch end after Task 5).

---

### Task 3: Flush coalescing and no redundant publish force

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java:371-385`, `ColumnarIndexManager.java` (`scheduleMaintenance` :2252-2290), `ColumnarFlusher.java` (`shouldFlush` :66-81), `GlobalDictionary.java` (`fsync` :297-312), `ColumnarStore.java` / `ColumnarManifest.java` (force counters for the test)
- Test: `ColumnarFlusherTest.java`, `ColumnarCrashRecoveryTest.java`, `ColumnarDurabilityTest.java`

**Interfaces:**
- Produces: `COLUMNAR_FLUSH_MIN_ROWS` (`arcadedb.columnar.flushMinRows`, DATABASE, default 4096 pending ruling) and `COLUMNAR_FLUSH_MAX_AGE` (`arcadedb.columnar.flushMaxAge`, ms, DATABASE, default 30000 pending ruling). A flush is due when the overlay holds `>= flushMaxRows`; or `>= flushMinRows` rows and is `>= flushInterval` old; or is `>= flushMaxAge` old; or the projection's unflushed pages reach `REAPPLY_FLOOR / 2` (512); or, with no row, unflushed pages wait and the last flush is `>= flushMaxAge` old (the deletes-only branch of `ColumnarFlusher.shouldFlush`); or a close or `flushNow` asks. A WAL drop never asks: `persistRecoverySet` covers the unflushed pages. `Lever.SINGLE_PUBLISH_FSYNC`: `GlobalDictionary.fsync()` skips `channel.force` when `appendPosition` equals the last durable length (it still returns the durable state); every new file is still forced once, the directory twice (pre-rename ordering, post-rename durability, spec 5.5), the WAL once. The scope is reduced from "one force per file and one directory force" (preflight R10), pending the user's confirmation.

- [ ] **Step 1: Write the failing tests**

`ColumnarFlusherTest`: `smallOverlayWaitsForMinRowsUntilMaxAge` (100 rows, flushInterval 1000, maxAge 30000, test clock: no flush at 1001 ms, one at 30000 ms); `fullOverlayFlushesAtOnce`; `walDropWaitsForTheCoalescedOverlaysRecoverySet` (the recovery set file holds the pages of the unflushed rows before `TransactionManager` drops the WAL file, via the existing housekeeping hook; it passes on today's code, so it is proven able to fail by a mutation: skip `persistRecoverySet`); `publishForcesOnlyWhatChanged` (counters: one force per file written, zero for a dictionary of an unchanged column, exactly two directory forces, one WAL force). `ColumnarCrashRecoveryTest.crashWithCoalescedOverlayRecoversWithoutRebuild` (a bucket of about 4,000 pages; 3,000 scattered updates leave more than `REAPPLY_FLOOR` pages unflushed only if the page bound is missing; `KillPoint` before the flush; reopen; `FacetOracle` equal; `rebuilds` 0 for every reason including `unclean_pages`).

- [ ] **Step 2: Run, expect failures** (`./mvnw -o -pl engine -am test -Dtest='ColumnarFlusherTest,ColumnarCrashRecoveryTest,ColumnarDurabilityTest' -Dsurefire.failIfNoSpecifiedTests=false`)

- [ ] **Step 3: Implement the due rule and the skipped dictionary force; existing defaults stay until the ruling**

- [ ] **Step 4: Tests, O, G1, `MergePolicyTest` (segment count stays bounded), G2: green**

- [ ] **Step 5: Measure and decide**

Config lever: `BENCH_OPTS='-Darcadedb.columnar.flushMinRows=0 -Darcadedb.columnar.flushMaxAge=1000'` (today's behavior) vs proposed defaults, 5 alternating runs each with the script, target `ingest`. Primary (engine `ingest`): `maintenanceCpuMs`, `flushCount`, `mergeCount`; guards: insert/update ms, apply lag, query p95, segments at the end; `load-single` once per side as a guard only (`lagMs`, query p95, `segments`, `wal`: the load JSONs carry no flush or merge count). The plan 6 three-node run did 3,497 flushes and 1,463 merges per node in 455 s with an overlay of at most 21-28 rows (figure from the plan 6 progress log). Stop: changes ready for review.

---

### Task 4: Apply scheduling: lag bound

**Files:**
- Modify: `GlobalConfiguration.java:393-402`, `ColumnarIndexManager.java:158-169, 1553-1586`
- Test: `ColumnarIndexManagerLifecycleTest.java` (pacing cases)

**Interfaces:**
- Produces: `COLUMNAR_APPLY_MAX_LAG` (`arcadedb.columnar.applyMaxLag`, ms, DATABASE, default 500 = today's `APPLY_PACING_LAG_LIMIT_MS`); the pacing factor caps at `max(1, applyMaxLag / applyInterval)`. `APPLY_PACING_LAG_LIMIT_MS` and `APPLY_PACING_MAX_FACTOR` are removed. Thread priority is not a lever (HotSpot ignores Java thread priorities on Linux by default) and neither is per-database fairness of the apply pool unless Task 1 shows one database starving another; both recorded in `levers.md`. Guard added to the measurements: WAL files and bytes on the leader at the end of `load-three` (plan 5 parked item: the DB-wide `queuedColumnarMarks` counter can delay WAL drops under sustained load; plan 6 O15 saw one 28-62 MB file).

- [ ] **Step 1: Failing tests**: `pacingNeverExceedsApplyMaxLag` (applyMaxLag 1000, interval 100: factor reaches 10, never 11, via `paceAfterPassForTesting`); `applyMaxLagBelowIntervalKeepsTheBaseInterval`; `oldestMarkOlderThanMaxLagResetsPacing`.
- [ ] **Step 2: Run, expect failures**; **Step 3: implement**; **Step 4: tests, `FacetOracleSelfTest` ten times (a shell loop of the O command for that class, stopping at the first red) with `-Darcadedb.columnar.applyMaxLag=1000`, O, G1: green**.
- [ ] **Step 5: Measure**: script runs `ingest` with `applyMaxLag` 500 vs 1000, 5 alternating each; plus `load-single` once per side for `lagMs`, `patched`, query p95. `applyThreads` is recorded in `levers.md` as "not a lever for one database: at most one pass per database runs at a time (`ColumnarApplyPool`)". `applyMaxLag` 1000 is measured and reported; the default stays 500 unless the user amends Task 17 Step 4 ("apply lag no worse than plan 6") for it. Record the trade-off (insert ms vs lag and patched). Stop: changes ready for review.

---

### Task 5: Commit-path marks (only if Task 1 ranks `commit.marks` above noise)

**Not built (0.05% of committer CPU)**: Task 1 measured `commit.marks` at 5 of 10,228 committer samples, below the 9.2-9.4% committer floor (ruling 2026-11-02). Recorded in `levers.md`; the steps below are kept for reference only.

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/database/TransactionContext.java:3775-3814`, `ColumnarIndexManager.java:1317-1340` (`drainSequence`), `DirtyPageTracker.java:92-140` (`drainInto` bumps the manager's counter)
- Test: `ColumnarCommitVisibilityTest.java`, `DirtyPageTrackerTest.java`

**Interfaces:**
- Produces: `ColumnarIndexManager.drainSequence(): long`, one `AtomicLong` per manager incremented by every `drainInto` of every tracker of the database (primary and external) before it seals the epoch; the second mark is skipped under `Lever.SKIP_REDUNDANT_MARK` only when the value read before mark #1 equals the one read after `raisePageCounts()`, the commit wrote WAL, and no file of the commit has a watcher (`watchersByFileId`, hierarchy sources, plan 5 R9); otherwise mark #2 runs as today. With Task 15, a skip is allowed only when mark #1 carried the same slot marks. Mark #1 still precedes visibility (Task 16 relies on it). The page sort is skipped when the array is already ascending.

- [ ] **Step 1: Failing tests**: `secondMarkSkippedWhenNoDrainRanBetween` (mark count 1 per commit, via the tracker's test counter); `secondMarkKeptWhenADrainSealedTheEpoch` (`TransactionContext.afterFirstColumnarMarkForTesting` runs a drain: the page is pending after the commit); `watchedTagTypeCommitStillRebuildsTheHierarchy` (a hierarchy watcher pass forced between mark #1 and visibility through the same seam; after the commit `HierarchyManager.view()` holds the new tag); `noWalCommitKeepsBothMarks`; `pageIsPendingBeforeItsCommitIsVisible` (existing visibility test, extended to the skip case).
- [ ] **Step 2-4**: run red, implement, run green; O, G1 plus `'*Transaction*Test,*WAL*Test'`, G2, G3 (shared commit path).
- [ ] **Step 5: Measure**: lever `SKIP_REDUNDANT_MARK`, target `ingest`, primary `insertMs` on B' (the commit path alone). If NOISE (expected: under 0.5 ms per 400 commits), revert and record. Stop: changes ready for review.

**Batch end (ingest):** G3 with `IMG=...-tuning-ingest`; compare its load JSONs with Task 1 Step 2 (guards).

---

### Task 6: Counter pooling inside the budget

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetMemoryBudget.java`, `DenseFacetCounter.java`, `TermsCollector.java` (and the other dense users); `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestData.java` (`authors` knob, default 50, for the 100,000-author benchmark)
- Test: `FacetMemoryBudgetTest.java`, `FacetCounterTest.java`; benchmark method `FacetTuningBenchmark.pooledCounters`

**Interfaces:**
- Produces: `FacetMemoryBudget.borrowDense(int cardinality, Reservation charged): int[]` (a zeroed array of exactly `cardinality`, from the pool's list of that length, else new; the caller reserves `bytesFor(cardinality)` as today, and the pool releases the same amount from its standing reservation when it hands a pooled array over, so one array is charged once at every moment; the pool is thread safe: concurrent queries borrow and give back) and `giveBack(int[] counts, int touchedTo)` (zeroes `[0, touchedTo)` and keeps it only if the pool's standing reservation, at most `queryMemoryBudget / 8`, can absorb it). Idle pooled bytes are reserved, so `available()` excludes them; a budget resize evicts the pool first. Under `Lever.POOLED_COUNTERS` off, borrow allocates and give-back drops.

- [ ] **Step 1: Failing tests**: `pooledArrayIsZeroOnBorrow` (fill, give back with `touchedTo`, borrow: all zero); `idlePooledBytesCountAgainstTheBudget` (`available()` drops by the pooled bytes); `poolNeverHoldsMoreThanItsShare`; `budgetShrinkEvictsThePool`; `concurrentQueriesStayWithinBudgetWithPooling` (16 threads, existing test pattern, the reserved total never above the budget).
- [ ] **Step 2-4**: red, implement, green; O, G1, G2 (`FacetsHttpIT`, `PrometheusColumnarQueryMetricsIT` budget waits).
- [ ] **Step 5: Measure**: `FacetTuningBenchmark.pooledCounters`: 8 threads x 200 broad FACETS queries over `tags` (800 values) and `author` (100,000 values) at 200k records; metrics `queryMsP50`, `allocMbPerQuery`, `gcCount`; lever `POOLED_COUNTERS`. Primary: allocation; guard: latency. Stop: changes ready for review.

---

### Task 7: Cached ordinal maps for restricted users

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/facet/CrossBucketOrdinalMap.java:82-145`, `ColumnarIndexManager.ordinalMapEntry`
- Test: `CrossBucketOrdinalMapTest.java`; server `FacetsSubtypeAclIT` (G2); benchmark `FacetTuningBenchmark.restrictedUserMultiBucket`

**Interfaces:**
- Produces: a restricted user's map comes from a cached entry keyed by `(declaring type, property, sorted bucket ids of the snapshots passed to of())`, used only when `readsEveryBucket` is false (the full-access entry is unchanged), at most 4 entries per `(type, property)`, least recently used evicted; an entry merges only the dictionaries of its bucket set (the plan 4 Task 17 rule stays: its size never counts values only hidden buckets hold). Under `Lever.CACHED_RESTRICTED_ORDINAL_MAPS` off: today's private `new Entry()`.

- [ ] **Step 1: Failing tests**: `restrictedUserReusesItsMapAcrossQueries` (`version()` equal on the second query); `restrictedEntryNeverMergesHiddenBucketValues` (a value only in the hidden bucket: `mergedOf` is -1, `size()` equals the readable union); `usersWithDifferentBucketSetsGetDifferentEntries`; `compactionRestartsTheRestrictedEntry` (`compactDictionaryNow`, new version); `entriesPerKeyAreBounded` (5 bucket sets, 4 entries).
- [ ] **Step 2-4**: red, implement, green; O, G1, G2.
- [ ] **Step 5: Measure**: 3 buckets, user reading 2, broad `tags` facet, 200k records; lever `CACHED_RESTRICTED_ORDINAL_MAPS`; primary `queryMs`. Stop: changes ready for review.

---

### Task 8: Parallel counting per segment (needs the user's ruling)

**Files:**
- Modify: `FacetExecutor.java:322-408` (and `FacetPlan.newCollectorLike`), `GlobalConfiguration.java`
- Test: `FacetParallelCountTest.java` (new), `PoolMetrics*Test` (G2, unchanged: the query pool is already bound)

**Interfaces:**
- Produces: `COLUMNAR_PARALLEL_COUNT_TASKS` (`arcadedb.columnar.parallelCountTasks`, SERVER, 1 = off; default per ruling): the most tasks one query forks, submitted to `QueryEngineManager.getInstance()` (bounded queue, caller-runs, already in `PoolMetrics` as the query pool) and awaited with `GraphAlgorithms.awaitFutures`, which reclaims queued tasks; the caller counts range 0 itself. No new pool (engine-concurrency skill items 1 and 7; the pool choice is recorded as a question for the user). `COLUMNAR_PARALLEL_MIN_MATCHES` (`arcadedb.columnar.parallelMinMatches`, default 262144). A bucket's segments are split into contiguous ranges; each task counts into a fresh collector from `FacetPlan.newCollectorLike(i)` (same spec, column, ordinal map; its own `CollectorReservations`) whose counter is reserved from `FacetMemoryBudget` (a refused reservation runs that range on the caller); partial collectors merge in segment order, so ties and `otherCount` are deterministic. Patching and scans stay on the caller.

- [ ] **Step 1: Failing tests**: `parallelCountEqualsSequentialForEveryFacetKind` (terms, list terms, range, histogram, DISTINCT, STATS, HIERARCHY on 300k records with `flushMaxRows` 4096 for many segments, threads 4 vs 1: identical `FacetResults.toJSON()`); `refusedTaskBudgetRunsOnTheCaller`; `deadlineCancelsParallelTasks` (TIMEOUT clause; every forked task has ended, counted by the test, when `execute` returns); `belowMinMatchesStaysSequential`.
- [ ] **Step 2-4**: red, implement, green; O, G1, G2.
- [ ] **Step 5: Measure**: script, target `facets` at 1M records, `parallelCountTasks` 1 vs 4 (whole runs, 5 alternating); primary broad facet ms; guard selective ms and budget waits. Stop: changes ready for review.

---

### Task 9: `AFTER :c` plan caching and rank-view cadence

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java:284-300`
- Modify (only if Step 6 says so): `ColumnarIndexManager.java:164-167` (`RANK_VIEW_MIN_INTERVAL_MS`)
- Test: `engine/src/test/java/com/arcadedb/query/facet/TermsAfterCursorTest.java`; benchmark `FacetTuningBenchmark.afterPaging`

**Interfaces:**
- Produces: under `Lever.CACHED_AFTER_PLANS`, `facetsDependOnInputParameters` ignores `item.after` (the collect step recompiles the facets, the cursor included, per execution). Intervals, bounds, ranges and INCLUDE keep blocking the cache.

- [ ] **Step 1: Failing tests**: `afterParameterPlanIsCached` (`ExecutionPlanCache.instance(db).contains(sql)` true after page 1); `pagingWithCachedPlanVisitsEveryValueOnce` (2,500 tag values, `TOP 100 AFTER :c`, loop until `after` is null: union equals `FacetOracle.expectedCounts` keys, no duplicate, 25 pages); `invalidCursorRefusedOnCachedPlan` (a cursor the first execution would refuse, sent on execution 2, raises the same exception and message); `cachedPlanSeesValuesAppendedBetweenPages`.
- [ ] **Step 2-4**: red, implement, green; O, G1, G2; G3 (shared planner, may share the batch-end run).
- [ ] **Step 5: Measure**: `afterPaging`: 50,000 values, pages of 100 through all, lever on/off; primary total ms.
- [ ] **Step 6: Rank-view cadence**: same method after appending 20% new values (unranked tail) vs after a forced rank rebuild; change the interval only on a WIN of the tail case beyond noise, else record "cadence unchanged". Stop: changes ready for review.

---

### Task 10: `facet.searchValues` direct count path and fixed statement shape

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/function/sql/facet/SQLFunctionFacetSearchValues.java:160-232`
- Test: `engine/src/test/java/com/arcadedb/function/sql/facet/SQLFunctionFacetSearchValuesTest.java`; benchmark `FacetTuningBenchmark.searchValues`

**Interfaces:**
- Consumes: `MatchSet.fromProjection(DatabaseInternal, int[] bucketIds, long budgetWaitMs)`, `FacetExecutor.execute(...)`, `TermsFacetSpec` with an INCLUDE list, `FacetFallbackScanner.firesReadEvents(db, type)`, `TransactionContext.hasChangesInFiles(IntHashSet)`.
- Produces: under `Lever.SEARCH_VALUES_DIRECT`, a batch is counted by `FacetExecutor.execute` over `MatchSet.fromProjection` (no SQL text, no plan cache entry) when no bucket of the type fires read events, the caller's transaction changed none of them, and `MatchSet.fromProjection` returned an entry (a usable snapshot) for every readable bucket; otherwise the SQL path, whose statement carries `B` INCLUDE parameters, `B` = `search.batch` rounded up to a power of two (16, 32, ..., up to `2 * topMax`); a short batch repeats its last candidate (equal INCLUDE values collapse into one bucket, plan 4 ruling 107). The plan stays uncached (INCLUDE parameters, Task 9); the gain is one parse per shape.

- [ ] **Step 1: Failing tests**: `directPathEqualsSqlPathCounts` (3 buckets, prefixes `a`..`z`, limits 1/7/50, admin and a restricted user: identical rows and `exact`); `sqlPathReusesOneStatementTextPerBatchShape` (limits 1 and 7 send the same text; limit 50 the 128-parameter text; asserted on the statement texts the function builds, through a test hook); `bucketWithoutUsableSnapshotTakesTheSqlPath`; `readEventTypeTakesTheSqlPath`; `ownTransactionChangesTakeTheSqlPath` (uncommitted value still not suggested, as documented).
- [ ] **Step 2-4**: red, implement, green; O, G1, G2 (`FacetAdvancedHttpIT`).
- [ ] **Step 5: Measure**: `searchValues`: 20,000 values, 26 prefixes, limit 20, lever on/off; primary ms per call. Stop: changes ready for review.

---

### Task 11: Hierarchy build in one scan, cancellable CPU passes

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/HierarchyBuilder.java:77-330`, `ColumnarTestHooks.java`
- Test: `HierarchyBuilderTest.java`; benchmark `FacetTuningBenchmark.hierarchyBuild`

**Interfaces:**
- Produces: under `Lever.HIERARCHY_ONE_PASS`, one scan of the vertex type collects keys and parent links (edge reads per vertex as today) as (child node, parent RID) pairs in primitive arrays; the parent node and its rank are resolved after the scan and the sort, so a tag created during a build is in it whole or not at all. `checkCancelled` also runs every `CANCEL_CHECK_MASK + 1` nodes in `sortByValue`, `depths` and `cycleNodes`. `ColumnarTestHooks.HIERARCHY_RECORD_READS` (AtomicLong, like `INDEX_WRITES`).

- [ ] **Step 1: Failing tests**: `oneScanReadsEachTagOnce` (1,000 tags: reads == 1,000); `cancelStopsTheSort`, `cancelStopsTheDepthPass`, `cancelStopsTheCyclePass` (100,000-node chain; the cancel supplier turns true on its first call after the scan checks, counted; `CancellationException` thrown, and the supplier was called more times than the scans alone call it); existing deep-chain and cycle tests stay green.
- [ ] **Step 2-4**: red, implement, green; O, G1 (`HierarchyManagerTest`, `HierarchyFacetIntegrationTest`).
- [ ] **Step 5: Measure**: `hierarchyBuild`, 100,000 tags depth 6, lever on/off; primary build ms. Stop: changes ready for review.

---

### Task 12: One hierarchy view per query

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetExecutor.java` (`plan`, `FacetPlan`), `HierarchyFacetCollector.java:333-372` (`takeView`, `fixedView`)
- Modify (test access): `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestAccess.java` (`hierarchyRebuildCount(HierarchyManager)`; `rebuildCount()` is package-private)
- Test: `HierarchyFacetIntegrationTest.java`

**Interfaces:**
- Produces: under `Lever.HIERARCHY_ONE_VIEW`, the plan of a HIERARCHY facet takes the view once per (declaring type, property) and passes it as `fixedView` to the projected and the scanned collectors of that type; the READ check runs on the manager before the view as today.

- [ ] **Step 1: Failing test**: `projectedAndScannedCollectorsShareOneView` (one bucket scanned through the caller's own uncommitted write, one projected; the tag tree changes at a hook between the two collectors' openings: `ColumnarTestAccess.hierarchyRebuildCount(manager)` advances at most once across the query, counts equal the oracle for one consistent tree).
- [ ] **Step 2-4**: red, implement, green; O, G1. Correctness-shaped: kept on green with no LOSS on `FacetTuningBenchmark.hierarchyBuild`'s query variant. Stop: changes ready for review.

---

### Task 13: Read events fire once per matched record

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/FacetCollectStep.java`, `engine/src/main/java/com/arcadedb/query/facet/FacetFallbackScanner.java:231-290`, `FacetExecutor.java` (scan of those buckets skipped)
- Test: `FacetReadEventsTest.java`; benchmark `FacetTuningBenchmark.readEventType`

**Interfaces:**
- Produces: under `Lever.READ_EVENTS_FROM_ROWS`, for a bucket whose type fires read events, `FacetCollectStep`, when the target has a bucket that fires read events, asks `FacetExecutor` at open for a scan plan of those buckets (collectors, reservations, the `maxFallbackScans` permit; no permit: those facets are `unavailable / fallback scans capped` as today); each drained row of such a bucket (the record as the read events left it) is extracted once and given, through the existing `FacetFallbackScanner.ScanSink`, to the collectors of the facets its fail mask admits (all FILTERs passed: every facet; exactly one failed: that facet only, spec 7.5), and is not added to the builder; the executor then skips those buckets. `source` stays `scan`, reason `read_events`; `maxFallbackScans` still bounds it.

- [ ] **Step 1: Failing tests**: `afterReadListenerFiresOncePerMatchedRecordWithWhere` (listener counter == matches; today 2x); `disjunctiveFiltersOnReadEventTypeEqualNormalPlan`; `afterReadListenerFiresOncePerRecordWithoutWhere` (no-WHERE shape; or record it in `levers.md` as out of scope); existing `beforeReadTriggerHidesTheTenantFromEveryKind`, `afterReadListenerFiltersAndRewritesRecords`, `databaseReadListenerReadsEveryType` stay green.
- [ ] **Step 2-4**: red, implement, green; O, G1, G2.
- [ ] **Step 5: Measure**: `readEventType`: 200k records with an AFTER READ listener, broad WHERE, lever on/off; primary ms. Stop: changes ready for review.

---

### Task 14: `LocalBucket.assembleFromNewestCommitted` hardening

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/engine/LocalBucket.java:6500-6560`, `engine/src/main/java/com/arcadedb/engine/PageManager.java:1895-1910`, `TransactionManager.java` (test hook in `applyChangesInternal`), `ColumnarApplier.java:730-770` (after Task 2, before Task 15)
- Test: `engine/src/test/java/com/arcadedb/engine/LocalBucketAssembleRaceTest.java` (new), `ColumnarApplierTest.java`

**Interfaces:**
- Produces: `PageManager.publishPagesHookForTesting` (static volatile `Runnable`, run inside `publishPages` between its two halves, null in production); the walk samples `getPublicationSequence()` before and after; when the first sample is odd or the two differ it walks again (bounded retries, as the chain validation does), and only then, on a leader, under `executeInLock` (the page-manager lock does not cover the replicated apply, `PageManager.java:155-157`). Hooks: `PageManager.publishPagesHookForTesting` and its twin in `TransactionManager.applyChangesInternal` between `beginPublication()` and the page writes. A chunked record deleted during the walk reports `EMPTY` to the apply pass instead of unreadable. No switch (correctness item). #10941 is fixed (`490e48c345`, in the baseline): the hardening covers replicas. The `FacetPatcher.recordAt` mapping (roadmap Plan 6 outcome, plan 6 ruling 129) is kept unless a follower test under replicated apply shows no `BrokenChunkChainException` with the hardening; the decision is recorded in `levers.md`.

- [ ] **Step 1: Failing tests**: `walkOverlappingAPublicationIsRedone` (the hook rewrites the chain mid-walk: the content equals one commit's version, never a mix; a redo counter is 1); `walkOverlappingAReplicatedApplyIsRedone` (same, through the `applyChangesInternal` hook); `chunkedRecordDeletedDuringWalkIsAnEmptySlot` (apply pass: `apply.unreadable` unchanged, the position is not live).
- [ ] **Step 2-4**: red, implement, green; O, G1 with the multi-page additions, G2, G3 (shared read path).
- [ ] **Step 5: Guard**: `ingest` and `facets` with the harness: no LOSS versus the previous kept state. Stop: changes ready for review.

**Batch end (query side and tuning list):** G3 with `IMG=...-tuning-query`.

---

### Task 15: Record-granular pending for hot pages (needs the user's ruling)

**Files:**
- Modify: `DirtyPageTracker.java`, `ColumnarIndexManager.java` (`markRecords`), `TransactionContext.java:3775-3814`, `ColumnarApplier.java` (re-read only marked slots), `ColumnarProjection.java` (`pendingPositions`), `query/facet/MatchSet.java` (`fromProjection` over slot marks)
- Test: `DirtyPageTrackerTest.java`, `ColumnarDirtyPageHooksTest.java`, `ColumnarRecordPendingTest.java` (new)

**Interfaces:**
- Consumes: the slots the transaction wrote per page, captured into a reused `long[]` during `commit1stPhase` from the same per-slot tracking the slot merge keeps (inserts, updates in place, deletes), with no `RID` or collection allocated; `getWrittenRecordIdsIn` stays a DDL-time API.
- Produces: `ColumnarIndexManager.markRecords(int fileId, long[] positions, int count)` on the local commit path, beside `markPages`; an epoch keeps, per page, either "whole page" or up to `MAX_SLOT_MARKS_PER_PAGE = 16` slots (more, or any page-only mark, makes it whole). A page is marked whole when the transaction poisoned it for the slot merge (`TransactionContext.poisonPageMerges` / `poisonSlotRebasePage`: a relocation of a neighbour off the page, a chunk tail moved, a placeholder content chunked, `packPageHoles`, a delete or write of a non-plain record) or created it. Slot marks are kept as a per-page stripe of `maxRecordsInPage / 64` words set with atomic ORs (one bit per record, the spec 2 bound), allocated with the page's chunk; a page whose stripe would exceed `MAX_SLOT_MARKS_PER_PAGE` bits set escalates to whole. Draining moves slots to `inFlight` with the page; a reader of `pendingPositions` that meets a page in flight without slots takes it whole. Raft apply, WAL replay, `markPagesWithoutWAL` and content/chunk/external pages stay page-granular (content pages still resolve to owner positions through `ContentOwnerMap`). The apply pass re-reads only the marked slots of a slot-marked page; `pendingPositions` holds only marked slots there. Under `Lever.RECORD_PENDING` off: every mark is whole page.

- [ ] **Step 1: Failing tests**: `recordPendingOracleUnderEveryMutationKind` (apply suspended; insert, update, delete, slot reuse in one pass, grow into a placeholder, grow into chunks, update of a grown record, external-property update, edge added to a vertex, delete and reinsert in one transaction, rollback, a growth that relocates a neighbour off the page (#10637, `sealFirstPage`-style fixture from `Issue6149PlaceholderPrefersChunksTest`), a chunk tail moved off the page (#10669), a placeholder's content chunked (#10859), then an update of that neighbour that writes only its content page; FACETS equal GROUP BY before and after catch-up); `pendingPositionsAreTheWrittenRecordsOnly`; `slotMarkedPageReReadsOnlyMarkedSlots` (`apply.records.reread` delta == written records); `overflowPastSixteenSlotsMarksTheWholePage`; `pathsWithoutRecordsMarkWholePages` (WAL replay, `TransactionManager.applyChanges`, no-WAL write).
- [ ] **Step 2: Mutation proof**: drop one slot from a mark in the implementation (local edit, reverted after): `recordPendingOracleUnderEveryMutationKind` must fail. Record it.
- [ ] **Step 3-4**: implement, green; O, G1 with the transaction additions, G2, G3.
- [ ] **Step 5: Measure**: `facets` 200k lagging broad (`patched`, ms; baseline 120,152 patched, 105 ms vs 26 ms caught up) and `ingest` updates (`recordsReread`, `applyCpuMs`, `updateMs`), lever `RECORD_PENDING`; `load-single` once per side (`patched` p95 6,554, query p95 52.5 ms). Stop: changes ready for review.

---

### Task 16: GROUP BY with a WHERE (needs the user's ruling)

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupByPlanner.java:100-110`, `ColumnarGroupByStep.java`, `engine/src/main/java/com/arcadedb/index/columnar/ColumnarSnapshot.java`, `ColumnarProjection.java`, `ProjectionState.java`, `GlobalConfiguration.java`
- Modify (pending the user's ruling, R7): `engine/src/main/java/com/arcadedb/engine/TransactionManager.java:1597-1618` (pre-apply mark)
- Modify (test seam): `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestHooks.java` (`reach` public, documented as a test seam; production cost unchanged: one volatile read)
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/ColumnarGroupByWhereTest.java` (new), `ColumnarGroupByEligibilityTest.java`, server `FacetsFollowerQueryIT` (G2); benchmark `FacetTuningBenchmark.groupByWhere`

**Interfaces:**
- Produces: `COLUMNAR_GROUP_BY_ACCELERATION_WHERE` (`arcadedb.columnar.groupByAccelerationWhere`, DATABASE, default per ruling); `long ColumnarSnapshot.stateSequence()` (per-projection counter bumped at every state publish: apply, flush, merge, compaction, rebuild); hook names `groupBy.afterFirstAcquire` and `groupBy.beforeReplay` through `ColumnarTestHooks.reach`.
- Algorithm (plan 5 review C1 "fix that keeps the column reads", roadmap tuning item): acquire S1 per bucket; drain upstream, holding matched positions per bucket in a `PositionBitmap` charged to the budget; acquire S2; if any bucket's `stateSequence` differs between S1 and S2, decline and replay; otherwise count positions not pending in S2 from the columns, and positions pending in S2 by re-reading the record, re-evaluating the statement's complete original WHERE (index-consumed conditions included, parameters from the statement's context) and counting the version read. The replay after any decline re-evaluates the complete WHERE on every record (I1). REPEATABLE_READ and the caller's own changes keep declining. Relies on mark #1 preceding page visibility (`TransactionContext`, kept by Task 5) and, on a replica, on the pre-apply mark this task adds to `applyChangesInternal`: the keys of `tx.pages` marked before `beginPublication()`, the existing mark kept after (today the replicated apply marks only once, after the page writes, so a WHERE drain on a follower could read a visible but unmarked page). If the user prefers no engine change, the step declines on a database that is a replica instead (no WHERE acceleration on followers).

- [ ] **Step 0: Gain bound (decision before any code)**: time the drain alone (`SELECT @rid FROM Asset WHERE brand <> 'brand-1'`, fully iterated) against the normal `GROUP BY brand` with the same WHERE, 200k and 1M, harness 5 rounds. If `(groupBy - drain) / groupBy` is within noise, stop: record "no gain without a native match source" and propose that as a follow-up; Steps 1-5 are not built.
- [ ] **Step 1: Write the C1 test first**: `concurrentCommitBetweenAcquireAndPullNeverYieldsAnImpossibleGroup` (`SELECT brand, count(*) AS c FROM Asset WHERE brand = 'B' GROUP BY brand`; at `groupBy.afterFirstAcquire` another thread commits A to B on one record; the accelerated rows equal the normal plan on the post-commit data, never `{brand: 'A'}`). Also `replayReEvaluatesTheCompleteWhere` (decline forced by a tight budget; at `groupBy.beforeReplay` a held row is updated to stop matching: not counted), `slotReusedAfterAcquireIsNotCountedWithTheDeletedRecordsColumns`, `indexConsumedWhereIsReEvaluatedOnReplay`, `repeatableReadDeclines`, `cachedPlanWithParameterWhereBindsPerExecution`; a unit test that the replicated apply marks before the first page reaches the read cache; `FacetsFollowerQueryIT.followerWhereGroupByNeverYieldsAnImpossibleGroup` (G2; a Raft apply paused between its page writes through a test seam while a follower runs the accelerated statement). Report whether the FACETS-with-WHERE follower path had the same window (preflight I22 observation).
- [ ] **Step 2: Prove the test can fail**: enable WHERE shapes in `analyze` with S1-only counting (no S2 check). Run `./mvnw -o -pl engine -am test -Dtest=ColumnarGroupByWhereTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL with the impossible `{brand: 'A', count: 1}` row.
- [ ] **Step 3: Implement the S2 validation, record counting and replay re-evaluation**
- [ ] **Step 4: Tests green; `ColumnarGroupByAccelerationTest`, `ColumnarGroupByEligibilityTest`, O, G1, G2, G3 (changes default GROUP BY execution).**
- [ ] **Step 5: Measure**: `groupByWhere`: broad and selective WHERE, 200k and 1M, caught up and under 50 writes/s from a background thread; metrics ms and decline rate; key on vs off (whole runs). Under ingest, a decline rate above 50% is recorded as a finding (a design that counts pages published between S1 and S2 from records is the follow-up). Stop: changes ready for review.

---

### Task 17: Before/after report and final gates

**Files:**
- Modify: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md` (new section "Tuning outcome (#10662)"), `docs/superpowers/specs/2026-10-28-columnar-faceting-overview.md` (Costs paragraph and benchmark tables)
- Modify (per ruling, Step 1b): remove `ColumnarTuning` and every `enabled(...)` check (kept levers become unconditional), `BenchmarkReport.alternate` and the `Boff` variant
- Create (working notes): `.superpowers/sdd/2026-11-02-columnar-faceting-7-tuning/tuning-report.md`

- [ ] **Step 1: Baseline tree**: `git worktree add --detach .worktrees/columnar-tuning-baseline 8da84eb9f3` (from the main checkout), then `git -C <that worktree> apply <task0-harness.patch>`; build its image `arcadedata/arcadedb:26.11.1-facets-tuning-base` and the final tree's `...-tuning-final`.
- [ ] **Step 1b: Shipped tree**: per the Q8 ruling, remove `ColumnarTuning` and every `enabled(...)` check (kept levers unconditional), `BenchmarkReport.alternate` and the `Boff` variant; compile; O and G1 green. Steps 2-5 run on this tree.
- [ ] **Step 2: Engine before/after**: for each of `ingest`, `count`, `facets` (200k and 1M), run baseline and final alternately, 5 runs each (the script with `BENCH_DIR` per side; the baseline runs from its worktree with `-f <worktree>/pom.xml`); `columnar_bench_compare.py compare "$BENCH_DIR_BASE" "$BENCH_DIR_FINAL" --noise "$BENCH_DIR/noise-floor.json"`. The query-side levers' gains are their Task verdicts (`levers.md`): their benchmarks need the lever's code, which the baseline does not have.
- [ ] **Step 3: Load before/after**: `load-single` 3 runs per image, alternating; `load-three` 1 run per image.
- [ ] **Step 4: Exit criterion**: inserts and updates (B vs A) within noise of A at default settings, judged by the apply overhead (apply CPU `applyCpuMs`, apply allocation `applyAllocMb`) and records re-read (`recordsReread`) at 9 rounds (`-Dcolumnar.bench.rounds=9`) against an A-vs-A floor (two A runs, alternating, `columnar_bench_compare.py noise` over them), not by `insert.ms` alone: its floor (about 10%, Task 1) is as large as the overhead being tuned, so `insert.ms` is reported but not the judge; facet latency (plan 4: 17-30 ms caught up, 105 ms lagging at 200k) and apply lag (plan 6: lagMs p95 297-492, apply-lag max ~500 ms) no worse; every remaining cost listed with its knob.
- [ ] **Step 5: Final gates**: O, G1 (with the multi-page and transaction additions), G2, G3 on `-tuning-final`; all green per their criteria.
- [ ] **Step 6: Write `tuning-report.md`** (machine state of both sides, per metric median before/after, delta, noise, verdict, the `levers.md` table, kept/reverted levers with reasons) and the roadmap section (summary, exit-criterion verdict, knobs, follow-ups). Remove the baseline worktree (`git worktree remove`). Stop: changes ready for review.
