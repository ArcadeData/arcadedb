# Perf tracking phase 2, project 6: mixed phase, hybrid retrieval, soak - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Three bench workloads: `mixed` (six kinds of work at once), `hybrid-rag` (vector, full-text and a 2-hop walk behind visibility filters) and `soak` (the mixed phase for 2 hours, with leak slopes).

**Architecture:** Two PRs. PR 1 (Tasks 1-5) adds `mixed` and `hybrid-rag` as ordinary single-server workloads. PR 2 (Tasks 6-8) adds two `Workload` defaults (a step limit and a trigger filter), a `SoakWorkload` that reuses the mixed phase from Task 2, and the slope metrics. Nothing in the private infrastructure repository changes unless the Saturday budget check in Task 8 says so.

**Tech Stack:** Java 21, the `bench` module (Testcontainers, `RemoteDatabase`, `LatencyRecorder`), JUnit 5 + AssertJ.

**Spec:** `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md`, section 3, "Mixed phase, hybrid retrieval, soak (project 6)", plus "Order and delivery" and "Testing".

## Global Constraints

- Workload code lives in `bench`, reusing existing support classes (`RemoteClients`, `DatasetGenerator`, `VectorRecall`, `LineProtocolClient`, `Metrics`). No new dependency.
- Every case is sized so that warm-up + reps fit the per-step limit (30 min) and the case cap (120 min) at every scale it runs at. The soak is the exception: it declares its own step limit (Task 6).
- S sizing rule: a latency case runs at least 1,000 operations per repetition at S. Operations that are slow by nature (compaction, DDL) are exempt, and their p99 is reported as measured.
- New metric units must be ones the gate knows (`ms`, `s`, `bytes`, rates, `ratio`) or be added to `Rule.absoluteFloor` as never-judged.
- Every correctness check must be able to fail: a unit test injects a wrong answer and sees the case fail.
- After every project, the weeknight S run must still end inside 240 minutes. If it does not, the slowest cases leave S (`cases(Scale.S)` empty) and the spec says why.
- The private infrastructure repository is private. Never name or link it from arcadedb code, PRs, issues or docs.
- Code style per CLAUDE.md: `final` wherever possible, no fully qualified names, JSON via `JSONObject`, `assertThat(...)` assertions, no em dash anywhere, no `System.out`.
- Commits: author robfrank only, no Co-Authored-By. Do not commit unless the user asks (CLAUDE.md).

## Review Focus

1. **Retryable conflicts under concurrency.** Six writers hit one database, so `ConcurrentModificationException` / `NeedRetryException` will happen. Expected: an operation is retried up to 3 times with the retries inside its latency, and only an exhausted or non-retryable failure fails the case. Test: `MixedPhaseTest.aConflictIsRetriedAndCountsOnce`.
2. **An operation thread that hangs.** One stuck thread must not keep a repetition open past its window. Expected: threads are joined with a bound after the window, a thread still alive fails the case, and the clients are closed. Test: `MixedPhaseTest.aThreadThatOutlivesTheWindowFailsTheRepetition`.
3. **A visibility leak that only shows up on hub vertices.** The 2-hop walk from a hub returns thousands of candidates, so a filter that drops on the walk but not on the final rows would leak. Expected: every row of every case is checked, not a sample. Test: `HybridRagWorkloadTest.aRestrictedRowInAnyCaseFailsIt`.
4. **A visible set smaller than k.** A tenant with fewer visible chunks than 100 would make recall@100 divide by the wrong count. Expected: exact top-k is `min(k, visible)` and recall divides by that. Test: `FilteredExactTest.recallDividesByTheVisibleCountWhenItIsBelowK`.
5. **Slopes from too few samples.** A soak cut short (server death, cancel) has 2-3 samples, and a 2-point slope is noise. Expected: under 6 samples, no slope metric and the case is `failed` with "too few samples". Test: `SlopeTest.fewerThanSixSamplesHaveNoSlope`.

---

## PR 1: `mixed` and `hybrid-rag`

### Task 1: Mixed phase building blocks (commit-gap tracker, retrying operation loop)

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/mixed/CommitGaps.java`
- Create: `bench/src/main/java/com/arcadedb/bench/workload/mixed/OperationLoop.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/mixed/CommitGapsTest.java`, `OperationLoopTest.java`

**Interfaces:**
- Produces: `final class CommitGaps { CommitGaps(long startNanos); void committed(long nanos); long longestGapNanos(long endNanos); }`. It is thread-safe, and the gap counts from `startNanos` to the first commit and from the last commit to `endNanos`.
- Produces: `final class OperationLoop implements Runnable`, built with `OperationLoop(String name, Operation op, long pauseMillis, AtomicBoolean stop, CommitGaps gaps, boolean commits, int capacity)`. It also has `interface Operation { void run() throws Exception; }`, `LatencyRecorder latencies()`, `long completed()`, `long retries()` and `Throwable failure()`.

- [ ] **Step 1: Write the failing tests**

```java
// CommitGapsTest
@Test void theLongestGapIncludesTheEdgesOfTheWindow() {
  final CommitGaps gaps = new CommitGaps(0);
  gaps.committed(100); gaps.committed(150); gaps.committed(400);
  assertThat(gaps.longestGapNanos(450)).isEqualTo(250);
  assertThat(new CommitGaps(0).longestGapNanos(1_000)).isEqualTo(1_000); // no commit at all: the whole window
}
@Test void commitsFromManyThreadsAreAllSeen() // 8 threads x 1000 committed() at increasing times, then longestGap is the largest step

// OperationLoopTest
@Test void aConflictIsRetriedAndCountsOnce()   // op throws ConcurrentModificationException twice, then succeeds: completed()==1, retries()==2, failure()==null, latencies().count()==1
@Test void aFourthConflictInARowIsAFailure()  // op always throws NeedRetryException: failure() is that exception after 1 + 3 tries, the loop ends
@Test void aNonRetryableErrorEndsTheLoopAtOnce() // IllegalStateException: failure() set, completed()==0, retries()==0
@Test void aCommittingOperationReportsEachSuccessToTheGaps()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -o -pl bench test -Dtest='CommitGapsTest,OperationLoopTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure (classes missing).

- [ ] **Step 3: Implement both classes**

`CommitGaps`: an `AtomicLong last` plus an `AtomicLong longest`. In `committed`, swap `last` with `getAndAccumulate(max)` so out-of-order calls cannot shrink a gap, then fold the gap into `longest`. `OperationLoop.run`: loop until `stop` is set or a failure occurs. Each pass tries the op up to 4 times. Retryable means `ConcurrentModificationException`, `NeedRetryException` or a `RemoteException` whose message names either of them (the remote client wraps them). The latency is recorded once per success and covers all tries. Then `gaps.committed(System.nanoTime())` when `commits` is true, then sleep `pauseMillis` (an interrupt stops the loop cleanly).

- [ ] **Step 4: Run the tests to verify they pass**

Same command. Expected: PASS.

- [ ] **Step 5: Commit** (only when the user asks)

### Task 2: `mixed` workload

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/mixed/MixedPhase.java` (the six loops over one window, reusable by the soak)
- Create: `bench/src/main/java/com/arcadedb/bench/workload/MixedWorkload.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/mixed/MixedPhaseTest.java`, `bench/src/test/java/com/arcadedb/bench/workload/MixedWorkloadTest.java`, `bench/src/test/java/com/arcadedb/bench/workload/MixedWorkloadIT.java`

**Interfaces:**
- Consumes: `CommitGaps` and `OperationLoop` from Task 1.
- Produces: `MixedWorkload.DATABASE = "mixed"` and `static void prepareDatabase(WorkloadContext ctx)`, which loads the base data unless it is already there.
- Produces: `final class MixedPhase`, with `MixedPhase(ServerUnderTest server, long seed, Scale scale)`, `Result run(Duration window)` and `record Result(Map<String, LatencyRecorder> latencies, long longestCommitGapNanos, long operations, double seconds, List<String> failures)`. The `latencies` keys are exactly `ingest`, `search`, `compaction`, `ddl`, `timeseries`, `vector_churn`.
- Produces: `MixedPhase.check()`, which throws `IllegalStateException` naming each mismatch, and `long expectedDocs()` / `long expectedPoints()` (cumulative over repetitions).

**Data (database `mixed`):**
- `MixDoc` document type: `id LONG` (UNIQUE index), `body STRING` (FULL_TEXT index, 40 words of `DatasetGenerator.text`), and `embedding ARRAY_OF_FLOATS` (LSM_VECTOR, 128 dimensions, COSINE, from `DatasetGenerator.embeddingSpace(128)`).
- `MixSensor` time-series type: `CREATE TIMESERIES TYPE MixSensor TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (value DOUBLE)`. It is written through `LineProtocolClient` with measurement `MixSensor`. The implementer confirms with the IT that line-protocol points land in that type, and falls back to SQL `INSERT` batches if they do not (a ruling for the ledger).
- Base docs per scale: XS 5,000; S 50,000; M 200,000; L 1,000,000. The window per repetition: XS 10 s; S 45 s; M 90 s; L 120 s. `reps()` = 3, `warmup()` = 1.

**The six loops (thread count, pause, one operation):**

| Loop | Threads | Pause | One operation | Commits |
|---|---|---|---|---|
| `ingest` | 2 | 0 | `sqlscript` BEGIN, 100 `INSERT INTO MixDoc`, COMMIT, with ids from a shared `AtomicLong` above the base | yes |
| `search` | 2 | 0 | alternately a BM25 top-10 (`SEARCH_INDEX('MixDoc[body]', :q)`) and a `vectorNeighbors('MixDoc[embedding]', :q, 10)` | no |
| `compaction` | 1 | 2,000 ms | `COMPACT INDEX` of `MixDoc[body]`, `MixDoc[id]` and `MixDoc[embedding]` in turn | no |
| `ddl` | 1 | 1,000 ms | `CREATE DOCUMENT TYPE MixScratch<n>`, `CREATE PROPERTY MixScratch<n>.v LONG`, `CREATE INDEX ON MixScratch<n> (v) NOTUNIQUE`, `DROP TYPE MixScratch<n> UNSAFE` | yes |
| `timeseries` | 1 | 0 | one line-protocol batch of 500 points over 50 sensors, with timestamps from a shared counter | yes |
| `vector_churn` | 1 | 0 | `sqlscript` BEGIN, 50 `UPDATE MixDoc SET embedding = [...] WHERE id = <random base id>`, COMMIT | yes |

- [ ] **Step 1: Write the failing tests**

```java
// MixedWorkloadTest (no server)
@Test void listsOneCase()            // cases() == List.of("mixed-phase"), name() == "mixed"
@Test void declaresThreeRepsAndOneWarmUp()
@Test void sizesPerScale()           // baseDocs(S)==50_000, window(S)==Duration.ofSeconds(45), window(M)==90 s; warm-up + 3 reps of the L window stay under 30 min each
@Test void publishesOneP99PerOperationAndTheLongestGap() // metrics(result) names: ingest_p99_ms, search_p99_ms, compaction_p99_ms, ddl_p99_ms, timeseries_p99_ms, vector_churn_p99_ms, max_commit_gap_ms, ops_per_s; all ms metrics Better.LOWER
@Test void aFailedLoopFailsTheCaseWithItsMetrics() // a Result with failures=["ingest: boom"] -> CaseFailedWithMetrics whose metrics() hold the 8 metrics

// MixedPhaseTest (no server: the loops take injected Operations through a package-private constructor)
@Test void aThreadThatOutlivesTheWindowFailsTheRepetition() // one op sleeps past window + join bound (bound injected as 200 ms): failures names the loop, run() returns
@Test void theCheckFailsOnAMissingDocument()     // expectedDocs 10, counted 9 -> IllegalStateException mentioning "MixDoc"
@Test void theCheckFailsOnALeftoverScratchType() // a MixScratch type still listed -> IllegalStateException
```

```java
// MixedWorkloadIT (@Tag("slow"), real server via ServerUnderTest.start on arcadedata/arcadedb:latest, scale XS)
@Test void aRepetitionRunsAllSixLoopsAndPassesItsCheck() // every latency map entry count() > 0, failures empty, check() passes
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -o -pl bench test -Dtest='MixedWorkloadTest,MixedPhaseTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement `MixedPhase` and `MixedWorkload`**

`MixedPhase.run(window)` opens one `RemoteDatabase` per thread before the clock starts, starts every loop as a daemon thread named `bench-mixed-<loop>`, waits for the window, sets `stop`, then joins each thread with a 60 s bound. A thread still alive gets an interrupt and a failure entry. Then it closes the clients and adds acknowledged docs and points to the cumulative expectations. `check()` (untimed) runs these checks:
- `count(*)` of `MixDoc` must equal `expectedDocs()`;
- `count(*)` of `MixSensor` must equal `expectedPoints()`;
- `schema:types` must have no type named `MixScratch%`.

`MixedWorkload.measure` runs the phase, then the check. A check failure, or any loop failure, throws `CaseFailedWithMetrics` carrying the metrics. `ops_per_s` is the total number of operations over the window. Register nothing yet (Task 5).

- [ ] **Step 4: Run the unit tests and the IT**

Run: `mvn -o -pl bench test -Dtest='MixedWorkloadTest,MixedPhaseTest,OperationLoopTest,CommitGapsTest' -Dsurefire.failIfNoSpecifiedTests=false`, then `mvn -o -pl bench verify -DskipITs=false -Dit.test=MixedWorkloadIT`
Expected: PASS for both.

- [ ] **Step 5: Commit** (only when the user asks)

### Task 3: Hybrid retrieval data set and filtered exact top-k

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/rag/RagDataset.java`
- Create: `bench/src/main/java/com/arcadedb/bench/workload/rag/FilteredExact.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/rag/RagDatasetTest.java`, `FilteredExactTest.java`

**Interfaces:**
- Produces: `final class RagDataset`, with `RagDataset(long seed, int chunks)`, `int entities()` (= chunks / 20) and `int tenant(int chunk)` (0..9). `String visibility(int chunk)` returns `public` 50%, `private` 40% or `restricted` 10%, decided by a hash of seed and chunk. `int[] mentions(int chunk)` returns 3 distinct entities drawn Zipf with s = 1.1 over `entities()`. `int[] related(int entity)` returns 4 distinct entities, uniform and never itself. `static boolean visible(String visibility, int chunkTenant, int queryTenant)` means public, or private with the same tenant.
- Produces: `final class FilteredExact`, with `static int[] topK(float[][] base, double[] norms, boolean[] visible, float[] query, int k)`, which returns `min(k, visibleCount)` ids, and `static double recall(int[] approx, int[] exact)`, which divides by `exact.length`.

- [ ] **Step 1: Write the failing tests**

```java
@Test void theSameSeedGivesTheSameData()                  // two RagDataset(42, 10_000) agree on visibility, tenant, mentions for 1,000 chunks
@Test void visibilitySharesAreCloseToTheirTargets()       // over 50,000 chunks: public 0.48..0.52, private 0.38..0.42, restricted 0.08..0.12
@Test void mentionsAreSkewedTowardsHubs()                 // over 50,000 chunks, the top 1% of entities hold at least 20% of all mentions
@Test void relatedNeverPointsAtItself()
@Test void visibleFollowsTheTenantRule()                  // public any tenant true; private same tenant true, other false; restricted false
@Test void topKOnlyReturnsVisibleIds()
@Test void recallDividesByTheVisibleCountWhenItIsBelowK() // 3 visible of 100, k 10: exact length 3; approx holding those 3 -> recall 1.0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -o -pl bench test -Dtest='RagDatasetTest,FilteredExactTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement both classes**

Zipf: precompute the cumulative weights `1 / rank^1.1` once per data set (a `double[]` of `entities()`), then draw by binary search over a `SplittableRandom` seeded with `seed ^ chunk`. Rank 0 is entity 0, so hubs are the low ids. `FilteredExact.topK` uses the same cosine as `VectorRecall.exactTopK` and skips ids that are not visible.

- [ ] **Step 4: Run the tests to verify they pass**

Same command. Expected: PASS.

- [ ] **Step 5: Commit** (only when the user asks)

### Task 4: `hybrid-rag` workload

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/HybridRagWorkload.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/HybridRagWorkloadTest.java`, `bench/src/test/java/com/arcadedb/bench/workload/HybridRagWorkloadIT.java`

**Interfaces:**
- Consumes: `RagDataset` and `FilteredExact` from Task 3; `DatasetGenerator.embeddingSpace(128)` / `embeddings(n, space)` / `text(60)`.
- Produces: `name()` = `hybrid-rag`. `cases()` = `vector-k100-filtered`, `fulltext-filtered`, `hop2-filtered`, `rag-pipeline`. `static int chunks(Scale)` returns XS 5,000; S 50,000; M 200,000; L 1,000,000. `static int queries(Scale)` returns XS 100; S, M and L 1,000. `static void checkVisible(String caseName, int queryTenant, List<Row> rows)`, with `record Row(int id, int tenant, String visibility)`, throws `IllegalStateException` naming the first leaked row.

**Schema (database `hybrid_rag`):** a `Chunk` vertex type with `id INTEGER` (UNIQUE), `tenant INTEGER` (NOTUNIQUE), `visibility STRING`, `body STRING` (FULL_TEXT) and `embedding ARRAY_OF_FLOATS` (LSM_VECTOR, 128 dimensions, COSINE). An `Entity` vertex type with `id INTEGER` (UNIQUE). Edge types `Mentions` (Chunk to Entity) and `Related` (Entity to Entity). The load runs in batches of 1,000 in `sqlscript` transactions, all edges after all vertices. `prepare` skips the load when the counts already match, as `VectorWorkload.dataReady` does.

**Queries.** Each query `i` uses tenant `i % 10`, a query embedding from `embeddings(queries, space)` with seed + 1, and a BM25 term from `DatasetGenerator`. Every query selects `id, tenant, visibility` so that every row can be checked. The filter is `(visibility = 'public' OR (visibility = 'private' AND tenant = :t))`.
- `vector-k100-filtered`: `SELECT id, tenant, visibility FROM (SELECT expand(vectorNeighbors('Chunk[embedding]', :q, 400))) WHERE <filter> LIMIT 100`. The metrics are `recall_at_100` (unit `ratio`, higher is better) against `FilteredExact.topK(..., 100)` for that tenant, plus qps and p50/p95/p99.
- `fulltext-filtered`: `SELECT id, tenant, visibility, $score AS score FROM Chunk WHERE SEARCH_INDEX('Chunk[body]', :q) = true AND <filter> ORDER BY score DESC LIMIT 50`.
- `hop2-filtered` (Cypher): `MATCH (c:Chunk {id: $id})-[:Mentions]->(:Entity)-[:Related]->(:Entity)<-[:Mentions]-(c2:Chunk) WHERE c2.visibility = 'public' OR (c2.visibility = 'private' AND c2.tenant = $t) RETURN DISTINCT c2.id AS id, c2.tenant AS tenant, c2.visibility AS visibility LIMIT 100`. Half the start chunks mention one of the 10 hub entities (ids 0-9), half are drawn uniformly.
- `rag-pipeline`: one operation is three steps, timed together:
  1. the vector query of the first case;
  2. the full-text query of the second;
  3. the 2-hop query from each of the top 10 ids, where the top 10 come from fusing the two lists by reciprocal rank (k = 60) on the client.

  It reports qps, p50/p95/p99 and `recall_at_100` of its vector step.
- Recall floor: `MIN_RECALL_K100`. The implementer runs the XS IT and a laptop S run and sets the floor at 0.1 below the lowest per-rep recall seen, capped at 0.9. The measured value goes in a comment and in the ledger. A recall below the floor throws `CaseFailedWithMetrics`.

- [ ] **Step 1: Write the failing tests**

```java
@Test void listsFourCases()
@Test void sizesPerScale()                       // chunks(S)==50_000, queries(S)==1_000 (S sizing rule)
@Test void aRestrictedRowInAnyCaseFailsIt()      // for each of the 4 case names: checkVisible(case, 3, [Row(7, 3, "restricted")]) throws, message names case and id 7
@Test void anotherTenantsPrivateRowFailsTheCase() // checkVisible(c, 3, [Row(8, 4, "private")]) throws
@Test void visibleRowsPass()                     // public of tenant 9 and private of tenant 3 for query tenant 3: no throw
@Test void rrfFusionPrefersIdsOnBothLists()      // fuse([1,2,3],[3,4]) top = 3
@Test void recallBelowTheFloorFailsWithMetrics() // a measured recall of MIN_RECALL_K100 - 0.01 -> CaseFailedWithMetrics carrying recall_at_100
```

```java
// HybridRagWorkloadIT (@Tag("slow"), XS, real server)
@Test void everyCaseRunsAndReturnsOnlyVisibleRows() // prepare + one measure per case, no exception, recall_at_100 > 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -o -pl bench test -Dtest='HybridRagWorkloadTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure.

- [ ] **Step 3: Implement `HybridRagWorkload`**

Precompute in `prepare`, once per workload instance: base embeddings and norms, one `boolean[]` visible mask per tenant (10 masks) and the exact top-100 per query. Every row from every query goes through `checkVisible` inside the timed loop: it is a field check on rows already in hand, so it costs microseconds. A leak throws `CaseFailedWithMetrics` with the metrics measured so far.

- [ ] **Step 4: Run the tests and the IT**

Run: `mvn -o -pl bench test -Dtest='HybridRagWorkloadTest,RagDatasetTest,FilteredExactTest' -Dsurefire.failIfNoSpecifiedTests=false`, then `mvn -o -pl bench verify -DskipITs=false -Dit.test=HybridRagWorkloadIT`
Expected: PASS for both.

- [ ] **Step 5: Commit** (only when the user asks)

### Task 5: Register, measure, PR 1

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/workload/WorkloadRegistry.java` (append `new MixedWorkload(), new HybridRagWorkload()`)
- Modify: the registry assertion in the existing test that lists every workload (`RampWorkloadTest` checks the registry; extend it to the two new names)
- Modify: the spec, section 3, project 6: one line per workload with the measured S duration and whether it stays at S

- [ ] **Step 1: Registry test first**: assert `WorkloadRegistry.all()` names contain `mixed` and `hybrid-rag`. Run it, see it fail, then register both and see it pass.
- [ ] **Step 2: Full bench unit suite**: `mvn -o -pl bench test`. Expected: every test passes (449 before this project, plus the new ones).
- [ ] **Step 3: Laptop S run of each workload** (`java -jar bench/target/arcadedb-bench-*.jar --workload mixed --scale S ...`, as the HA plan ran it). Record wall time, cases `ok`, and every metric in the ledger.
- [ ] **Step 4 (needs the user's go): commit, push and open PR 1** (`Part of #12433`). Then dispatch S and M on perf-01 with `bench_ref` = the PR head, one after the other (the queue holds one waiting run). S decision: the 11-27 S night took 184 min, and `ha` adds about 13. If both new workloads together add more than 30 min (both variants), the slower one leaves S (`cases(Scale.S)` empty), and the spec and PR say so. Native results are the merge condition.

---

## PR 2: `soak`

### Task 6: A workload's own step limit and trigger filter

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/runner/Workload.java`
- Modify: `bench/src/main/java/com/arcadedb/bench/runner/BenchRunner.java`
- Modify: `bench/src/main/java/com/arcadedb/bench/BenchMain.java` (the workload selection)
- Test: `bench/src/test/java/com/arcadedb/bench/runner/BenchRunnerTest.java`, `bench/src/test/java/com/arcadedb/bench/BenchMainTest.java`

**Interfaces:**
- Produces: `default Optional<Duration> stepLimit()` (empty means the run's step limit) and `default boolean runsOn(String trigger)` (true by default) on `Workload`.
- Produces: `BenchRunner.stepLimitOf(Workload)` and `BenchRunner.caseLimitOf(Workload)`. A declared step limit replaces the warm-up and repetition limit. The case cap becomes `max(run cap, prepare limit + (warm-up + reps) x step limit)`, so a long soak is not cut by the 120-minute cap. A run with no cap (zero) keeps none.

- [ ] **Step 1: Write the failing tests**

```java
// BenchRunnerTest
@Test void aWorkloadsStepLimitReplacesTheRunsForItsRepetitions() // run step 10 ms, workload stepLimit 500 ms, a measure sleeping 100 ms -> ok
@Test void aWorkloadWithoutAStepLimitKeepsTheRuns()              // same sleep, no override -> timeout
@Test void theCaseCapGrowsToHoldTheDeclaredSteps()               // caseLimitOf with run cap 120 min, prepare 30, 0 warm-up, 1 rep, step 150 min -> 180 min
// BenchMainTest
@Test void aWorkloadThatDoesNotRunOnTheTriggerIsLeftOut()        // --workload all --trigger release: a workload with runsOn("release")==false gets no case lines, and the run file says nothing about it
```

- [ ] **Step 2: Run them to verify they fail**, implement, and run them again to verify they pass, together with the whole `BenchRunnerTest` and `BenchMainTest`.
- [ ] **Step 3: Commit** (only when the user asks)

### Task 7: Slopes and the `soak` workload

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/soak/Slope.java`
- Create: `bench/src/main/java/com/arcadedb/bench/workload/soak/SoakSampler.java`
- Create: `bench/src/main/java/com/arcadedb/bench/workload/SoakWorkload.java`
- Modify: `bench/src/main/java/com/arcadedb/bench/server/ServerUnderTest.java` (add `public int threadCount()` visibility if needed, and `public int openFiles()` = the entry count of `/proc/1/fd` via `docker exec`, -1 on failure)
- Modify: `bench/src/main/java/com/arcadedb/bench/gate/Rule.java` (unit `per_hour` added to the never-judged units)
- Test: `SlopeTest`, `SoakSamplerTest`, `SoakWorkloadTest`, `RuleTest`

**Interfaces:**
- Consumes: `MixedPhase` and `MixedWorkload.prepareDatabase` from Task 2; `stepLimit()` and `runsOn()` from Task 6.
- Produces: `final class Slope { static OptionalDouble perHour(double[] hours, double[] values); }`, a least-squares slope that is empty under 6 samples.
- Produces: `final class SoakSampler`, which runs on its own thread and samples every 10 s while the phase runs:
  - heap used, summed from `jvm_memory_used_bytes{area="heap",...}` scraped from `<httpUrl>/prometheus` with the root credentials (the plugin is in the bench's default plugin list);
  - `threadCount()`;
  - `openFiles()`.

  `double[][] heapFloors()` returns the minimum heap of each 5-minute window as `{hourOffset, bytes}` pairs, which is the bench's "heap after GC" and works on JVM and native alike. `threads()` and `files()` return per-minute medians.
- Produces: `SoakWorkload`, with `name()` = `soak` and the single case `mixed-2h`. `cases(scale)` is non-empty at M only (Saturday lane). `runsOn(trigger)` is true for `nightly` and `manual`, so release runs leave it out. `reps()` = 1, `warmup()` = 0 and `stepLimit()` = 150 minutes. The phase window is 120 minutes, with the M base data of `mixed`.
- Metrics:
  - `heap_floor_slope_per_hour`: unit `per_hour`, the slope divided by the mean floor; the case fails over 0.05;
  - `thread_slope_per_hour`: fails over 10;
  - `open_files_slope_per_hour`: fails over 10;
  - the six `<op>_p99_ms` and `max_commit_gap_ms`, as in `mixed`.

  A failed check throws `CaseFailedWithMetrics` naming each limit crossed.

**Ruling owed to the spec:** the spec says the slopes are published with unit `ratio`. Under `ratio` the gate would judge a near-zero slope with relative bands, which is night-to-night noise; the case's own limits are the judgment. This plan uses `per_hour`, never judged, and Task 8 updates the spec line.

- [ ] **Step 1: Write the failing tests**

```java
// SlopeTest
@Test void aStraightLineGivesItsSlope()            // hours 0..1 step 0.1, values 100 + 20 h -> 20.0 within 1e-9
@Test void fewerThanSixSamplesHaveNoSlope()        // 5 points -> empty
@Test void flatNoiseIsNearZero()                   // values 100 +/- 1 alternating over 24 points -> |slope| < 1
// SoakSamplerTest
@Test void parsesTheHeapLinesOfAPrometheusScrape() // canned scrape text with 3 heap pools and 2 nonheap pools -> sum of the 3 heap values
@Test void theFloorOfAWindowIsItsMinimum()         // samples 10 s apart over 10 min, a sawtooth -> 2 floors, each the window minimum
// SoakWorkloadTest
@Test void runsOnlyOnSaturdayLaneScaleAndTriggers() // cases(S) empty, cases(M)==["mixed-2h"], cases(L) empty; runsOn("release") false, runsOn("nightly") true
@Test void aGrowingHeapFailsTheCase()              // floors rising 10%/h of the mean -> CaseFailedWithMetrics naming heap_floor_slope_per_hour
@Test void aThreadLeakFailsTheCase()               // threads rising 12/h -> fails naming thread_slope_per_hour
@Test void aCutShortSoakFailsForTooFewSamples()    // 4 samples -> failed, message "too few samples"
@Test void aSteadyServerPasses()
// RuleTest
@Test void perHourIsNeverJudged()                  // absoluteFloor("per_hour") is infinite
```

- [ ] **Step 2: Run them to verify they fail**, implement, then run `mvn -o -pl bench test` (whole bench suite) to verify everything passes.
- [ ] **Step 3: A short real soak**: a package-private window override of 12 minutes (6 heap-floor windows of 2 minutes each, via a constructor used only by the IT) in `SoakWorkloadIT` (`@Tag("slow")`, XS data). Expected: `ok`, with the three slopes present.
- [ ] **Step 4: Register `new SoakWorkload()` in `WorkloadRegistry` and extend the registry test**.
- [ ] **Step 5: Commit** (only when the user asks)

### Task 8: Saturday budget and PR 2

- [ ] **Step 1 (needs the user's go): commit, push and open PR 2** (`Part of #12433`). Dispatch `soak` at M on perf-01 with `bench_ref` = the PR head and `force=true`. Record the duration per variant.
- [ ] **Step 2: Saturday budget**: take the duration of the latest Saturday M night with every other workload and LDBC, add the soak (both variants). If the total passes 720 min, apply the spec's overflow rule. The first step is soak on JVM only: an infra PR that passes `--workload` without `soak` to the native variant on Saturday. Do not link that PR from arcadedb.
- [ ] **Step 3: Spec**: change the soak line to unit `per_hour` (never judged), with the reason, and record the measured soak duration and the budget decision.
- [ ] **Step 4: Report on #12433** once both PRs are merged and the first Saturday that runs the soak is green: one comment with the S/M/soak numbers (the issue has no checkbox for project 6).
