# Continuous Performance Tracking (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `bench` runner measures ingestion, graph OLTP, vector, full-text and faceting, plus the LDBC Graphalytics
run, on `perf-01` every night against one `main` commit, on both the JVM and the native image. Results go into an
ArcadeDB results server, and a public repo carries the raw JSON and a static dashboard.

**Architecture:** A new Maven module `bench/` in the public repo holds the runner, the workloads, the results format,
the uploader, the exporter and the LDBC adapter, all behind one CLI (`BenchMain`). A new private repo,
`ArcadeData/arcadedb-perf-infra`, holds the provisioning scripts for `perf-01` and the workflows, which run on the
self-hosted runner. A new public repo, `ArcadeData/arcadedb-perf`, holds `data/**.json` and the GitHub Pages
dashboard.

**Tech Stack:** Java 21, Testcontainers (already used by `load-tests`), `com.arcadedb.serializer.json`, the ArcadeDB
`RemoteDatabase` HTTP client, Bash and GitHub Actions, and a static HTML dashboard on Apache ECharts 5.5.1 from
cdnjs.

**Spec:** `docs/superpowers/specs/2026-11-17-continuous-performance-tracking-design.md`

## Global Constraints

- Java 21. JSON only through `com.arcadedb.serializer.json.JSONObject`/`JSONArray`. No new Maven dependency unless it is
  on the CLAUDE.md allow-list, and none is expected (Testcontainers and the client are already in the reactor).
- Imports, never fully qualified names. `final` on locals and parameters. No braces on single-statement `if`. Tests use
  AssertJ (`assertThat(x).isTrue()`).
- Performance: latency samples go in `long[]` (nanoseconds), never `List<Long>`.
- `bench` unit tests run in the CI modules shard; its `*IT` classes are not run by any CI lane. Unit tests run with
  `mvn -o -pl bench -am test`. Container tests are
  `*IT` classes, run with `mvn -o -pl bench -am verify -Pintegration`, the same profile `load-tests` uses.
  Every command below that filters with `-Dtest` or `-Dit.test` also needs `-Dsurefire.failIfNoSpecifiedTests=false`
  and `-Dfailsafe.failIfNoSpecifiedTests=false`. Without them the `-am` upstream modules fail for matching no tests.
  `-am test` reruns the full suites of the upstream modules even when they are installed: to test bench alone, filter
  with `-Dtest='com.arcadedb.bench.**'` and the two flags above.
- Never bind a fixed port. Every server is a Testcontainers container, and its URL is built from `getMappedPort`.
- **No git commits in `ArcadeData/arcadedb`.** The maintainer commits after review. Each task ends with "stop for
  review", not a commit. Commits in the two new repos are fine.
- Results JSON: `schemaVersion: 1` and the field shape of spec section 3. A failed or timed-out case is recorded with
  `status`, never dropped.
- Every metric carries `unit` and `better` (`"lower"` or `"higher"`).
- `perf-01`: Ubuntu 24.04, i5-13500, the server under test pinned to the P-core threads and the bench client to the
  E-cores. Governor `performance` and `no_turbo=1` during a run, both restored afterwards. Swap off for the server
  under test.
- Nightly budget: 3 hours, scale `M`, 5 reps, 2 warm-ups. Scale `L` runs only on release tags and manual dispatch.
- Results server: the latest ArcadeDB release, database `perf`. `perf-writer` can create, read and delete `Run` and
  `Measurement` records. `perf-reader` is read-only.

## Review Focus

1. **A case crashes or the server under test OOMs mid-run.** That case gets `status: failed` with the error, the
   remaining cases still run, and the results file is still written. Tested in Task 2 (`failingCaseIsRecordedAndRunContinues`).
2. **A run is uploaded twice, or the upload dies halfway.** The store holds exactly one copy of the run, never half of
   one. Tested in Task 9 (`reuploadReplacesRun`, `failedUploadLeavesPreviousCopy`).
3. **A workflow is cancelled while the CPU is in benchmark mode.** The governor and turbo are restored anyway, and a busy
   machine fails the run before it measures anything. Tested in Task 12 (`trap` restore test, idle-check test).
4. **The native nightly failed, so `latest-native` is still yesterday's commit.** The nightly skips instead of
   measuring the JVM at a different commit or measuring the same commit twice. Tested in Task 13 (`resolve-commit.sh`
   cases).
5. **Vector throughput rises because recall fell.** Every vector case carries `recall@k`, and a run whose recall is
   below 0.90 marks that case `failed`. Tested in Task 6 (`recallIsAlwaysReported`, `lowRecallFailsCase`).

---

## File Structure

```
ArcadeData/arcadedb (public)
  pom.xml                                         modify: add <module>bench</module> next to load-tests
  .github/workflows/native-image.yml              modify: add org.opencontainers.image.revision label
  bench/pom.xml                                   new; depends on arcadedb-network, load-tests test-jar, testcontainers
  bench/src/main/java/com/arcadedb/bench/
    BenchMain.java                                CLI: run | list | upload | export | ldbc-convert | init-store
    model/RunResult.java  CaseResult.java  Metric.java  RunInfo.java     results format, toJSON/fromJSON
    runner/Workload.java  WorkloadContext.java  BenchRunner.java  LatencyRecorder.java  Stats.java
    env/CpuTopology.java  MachineProbe.java  IdleCheck.java
    server/ServerUnderTest.java                   container start, pinning, limits, RSS/disk probes, image info
    workload/IngestionWorkload.java  GraphOltpWorkload.java  VectorWorkload.java  FullTextWorkload.java  FacetWorkload.java
    workload/Scale.java  data/DatasetGenerator.java
    store/ResultsStore.java  Uploader.java  Exporter.java
    ldbc/LdbcAdapter.java
  bench/src/test/java/com/arcadedb/bench/...      unit tests mirror the packages; *IT for container tests
  bench/src/test/resources/                       lscpu-extended-i5-13500.txt, mode1-summary.json, run-sample.json

ArcadeData/arcadedb-perf-infra (private, new)
  provision/provision.sh  perf-mode.sh  idle-check.sh  results-server.sh  test/*.bats
  scripts/resolve-commit.sh  scripts/already-measured.sh
  .github/workflows/nightly.yml  release.yml

ArcadeData/arcadedb-perf (public, new)
  data/YYYY/MM/<runId>.json   site/index.html  site/workload.html  site/app.js  site/series/  site/fixtures/
```

---

### Task 1: `bench` module and the results format

**Files:**
- Create: `bench/pom.xml`, `bench/src/main/java/com/arcadedb/bench/model/{RunResult,RunInfo,CaseResult,Metric}.java`
- Modify: `pom.xml` (add `<module>bench</module>` after `load-tests`)
- Test: `bench/src/test/java/com/arcadedb/bench/model/RunResultTest.java`, `bench/src/test/resources/run-sample.json`

**Interfaces:**
- Produces:
  - `record Metric(String name, String unit, Better better, double median, double[] values)` with
    `enum Better { LOWER, HIGHER }`, serialized lowercase.
  - `record CaseResult(String workload, String caseName, String scale, JSONObject params, int reps, int warmup,
    List<Metric> metrics, Status status, String error)` with `enum Status { OK, FAILED, TIMEOUT }`. The JSON key for
    `caseName` is `"case"`.
  - `record RunInfo(String runId, Instant startedAt, Instant finishedAt, String trigger, String source,
    JSONObject arcadedb, JSONObject machine, JSONObject config)`
  - `record RunResult(RunInfo info, List<CaseResult> results)` with `JSONObject toJSON()`,
    `static RunResult fromJSON(JSONObject)`, `void write(Path dir)` (writes `<runId>.json`), and
    `static RunResult read(Path)`.
  - `static final int SCHEMA_VERSION = 1`. `fromJSON` throws `IllegalArgumentException` on any other version.

- [ ] **Step 1: Write the failing tests** in `RunResultTest`:
  - `roundTripPreservesEverything`: parse `run-sample.json`, then `toJSON()`, then parse again. The two results are equal
    field by field, `values` arrays included.
  - `failedCaseKeepsStatusAndError`: a `CaseResult` with `Status.FAILED`, `error = "OOM"` and no metrics serializes to
    `"status":"failed","error":"OOM","metrics":[]`.
  - `rejectsUnknownSchemaVersion`: `schemaVersion: 2` makes `fromJSON` throw `IllegalArgumentException` with a message
    naming the version.
  - `runIdFormat`: `RunInfo.newRunId(Instant.parse("2026-11-17T03:00:00Z"), "1e1ad51768", "jvm")` returns
    `"2026-11-17T03-00-00Z-1e1ad51768-jvm"`. Colons are replaced so the id is safe as a filename.
- [ ] **Step 2: Run** `mvn -o -pl bench -am test -Dtest=RunResultTest -Dsurefire.failIfNoSpecifiedTests=false` and
  check that it FAILS at compilation.
- [ ] **Step 3: Implement** the records and `bench/pom.xml`:
  - parent `arcadedb-parent`, artifact `arcadedb-bench`;
  - dependencies `arcadedb-network`, `arcadedb-load-tests` (`<type>test-jar</type>`, compile scope),
    `org.testcontainers:testcontainers`, `logback-classic`;
  - deploy skipped (copy `load-tests`), and the `integration` profile copied from `load-tests/pom.xml`;
  - `maven-shade-plugin` producing `arcadedb-bench-<ver>-cli.jar` with `Main-Class: com.arcadedb.bench.BenchMain`.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 2: Runner, statistics, workload contract

**Files:**
- Create: `runner/{Workload,WorkloadContext,BenchRunner,LatencyRecorder,Stats}.java`
- Test: `runner/BenchRunnerTest.java`, `runner/StatsTest.java`

**Interfaces:**
- Consumes: Task 1 `CaseResult`, `Metric`.
- Produces:
  - `interface Workload { String name(); List<String> cases(); void prepare(WorkloadContext ctx, String caseName) throws Exception; Measurement measure(WorkloadContext ctx, String caseName) throws Exception; default void teardown(WorkloadContext ctx) {} }`.
    `Measurement` is `record Measurement(List<Metric> perRep)`: one call to `measure` returns one value per metric for
    a single repetition, and the runner collects the reps.
  - `record WorkloadContext(ServerUnderTest server, Scale scale, long seed, JSONObject params)`. `server` may be null
    in unit tests.
  - `final class LatencyRecorder { LatencyRecorder(int capacity); void record(long nanos); long percentile(double p); int count(); }`,
    backed by a growable `long[]` and sorted once on read.
  - `final class Stats { static double median(double[] v); }`
  - `final class BenchRunner { BenchRunner(int reps, int warmup, Duration caseTimeout); List<CaseResult> run(Workload w, WorkloadContext ctx, Predicate<String> caseFilter); }`
    Per case: `prepare`, then `warmup` calls of `measure` (discarded), then `reps` calls. Each metric's `median` comes
    from the reps. Any exception gives `FAILED` with `error = class + ": " + message`. Going past `caseTimeout` gives
    `TIMEOUT`. The run then moves to the next case. `teardown` always runs.

- [ ] **Step 1: Write the failing tests:**
  - `StatsTest.medianOddAndEven`: `{3,1,2}` gives 2, and `{4,1,3,2}` gives 2.5.
  - `StatsTest.percentilesOnKnownSet`: record 1..100. p50 = 50, p95 = 95, p99 = 99 (nearest-rank).
  - `BenchRunnerTest.warmupIsDiscarded`: a fake workload returns its call index as the metric value. With reps=3 and
    warmup=2, `values` is `{3,4,5}` and `median` is 4.
  - `BenchRunnerTest.failingCaseIsRecordedAndRunContinues`: case `a` throws `OutOfMemoryError("boom")`, case `b`
    succeeds. The result has two entries: `a` with `FAILED` and an error containing `"boom"`, `b` with `OK`.
  - `BenchRunnerTest.timeoutIsRecorded`: a case that sleeps past a 200 ms `caseTimeout` gives `TIMEOUT`.
  - `BenchRunnerTest.teardownRunsAfterFailure`: a counter shows that `teardown` was called after the failing case.
- [ ] **Step 2: Run** `mvn -o -pl bench -am test -Dtest='BenchRunnerTest,StatsTest' -Dsurefire.failIfNoSpecifiedTests=false`.
  Expected: FAIL.
- [ ] **Step 3: Implement.** Run each case on one dedicated single-thread `ExecutorService` owned by `BenchRunner`
  (not the common pool), and use `Future.get(timeout)` for the timeout.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 3: Machine probe, CPU topology, idle check

**Files:**
- Create: `env/{CpuTopology,MachineProbe,IdleCheck}.java`
- Test: `env/CpuTopologyTest.java`, `env/IdleCheckTest.java`, `bench/src/test/resources/lscpu-extended-i5-13500.txt`

**Interfaces:**
- Produces:
  - `record CpuTopology(String performanceCpus, String efficiencyCpus)`, with
    `static CpuTopology parse(String lscpuExtendedOutput)`. CPUs are grouped by the `MAXMHZ` column: the highest value
    is the P-cores, everything else the E-cores. Each set is a cpuset string such as `"0-11"`. If every CPU has the
    same `MAXMHZ` (not a hybrid CPU), both sets equal all CPUs.
  - `final class MachineProbe { static JSONObject probe(String machineId); }` reads `/proc/cpuinfo` model name,
    `nproc`, `MemTotal`, `/etc/os-release`, `uname -r`, `scaling_governor` of cpu0 and
    `/sys/devices/system/cpu/intel_pstate/no_turbo`. Any missing file becomes `"unknown"`, never an exception.
  - `final class IdleCheck { static Optional<String> check(double load1, long memUsedMb, int cores, double maxLoadPerCore, long maxMemUsedMb); }`
    Returns a reason when the machine is busy. Defaults: `maxLoadPerCore = 0.10`, `maxMemUsedMb = 8192`.

- [ ] **Step 1: Write the fixture.** Save `lscpu --extended=CPU,CORE,MAXMHZ` from `perf-01`. Until it exists, write the
  i5-13500 layout by hand: CPUs 0-11 at 4800.0000 (6 cores x 2 threads), CPUs 12-19 at 3500.0000.
- [ ] **Step 2: Write the failing tests:**
  - `parsesHybridI5`: `performanceCpus` is `"0-11"`, `efficiencyCpus` is `"12-19"`.
  - `nonHybridUsesAllCpusForBoth`: 8 CPUs, all at 3000, give `"0-7"` for both sets.
  - `IdleCheckTest.busyByLoad`: load 5.0 on 20 cores (above 0.10 x 20) gives a reason containing `"load"`.
  - `IdleCheckTest.busyByMemory`: 11000 MB used gives a reason containing `"memory"`.
  - `IdleCheckTest.idle`: load 0.5, 3000 MB gives `Optional.empty()`.
- [ ] **Step 3: Run** `mvn -o -pl bench -am test -Dtest='CpuTopologyTest,IdleCheckTest' -Dsurefire.failIfNoSpecifiedTests=false`.
  Expected: FAIL.
- [ ] **Step 4: Implement.** `MachineProbe` is called by `BenchMain` only. It is not unit-tested beyond a smoke call in
  Task 5, which must not throw on macOS either.
- [ ] **Step 5: Run** the same command. Expected: PASS.
- [ ] **Step 6: Stop for review.**

### Task 4: Server under test

**Files:**
- Create: `server/ServerUnderTest.java`
- Test: `server/ServerUnderTestIT.java`

**Interfaces:**
- Consumes: `CpuTopology` (Task 3) and `ServerWrapper` from the `load-tests` test-jar.
- Produces: `final class ServerUnderTest implements AutoCloseable`
  - `static ServerUnderTest start(String image, String cpuset, int memLimitGb, String heap, Map<String,String> settings)`:
    `GenericContainer` with `withCreateContainerCmdModifier` setting `cpusetCpus`, `memory`, `memorySwap == memory`;
    root password `ServerUnderTest.PASSWORD = "perfbench"`; the server settings passed the way
    `ContainersTestTemplate.configureServer` does (JVM: `JAVA_OPTS`/`ARCADEDB_OPTS_MEMORY`; native: the heap in the
    command line); waits for `/api/v1/ready`.
  - `ServerWrapper wrapper()`, `String httpUrl()`
  - `JSONObject imageInfo()`, with keys `image`, `imageDigest` and `revision` (from the
    `org.opencontainers.image.revision` label, or `"unknown"`).
  - `void resetPeak()` and `long peakRssBytes()`. Nothing runs inside the container, because the amd64 native image
    is built `FROM scratch` and has no shell, `cat` or `du`. Instead, a daemon sampler thread reads
    `DockerClient.statsCmd(containerId)` once a second and keeps the maximum `memory_stats.usage`. A workload calls
    `resetPeak()` at the start of each case.
  - `long diskBytes(String database)`: the databases directory is bind-mounted from a host temp dir, which is created
    per server and deleted on `close()`. The size is the sum of `Files.size` over `<hostDir>/<database>`, computed on
    the host. A value that can't be read is recorded as `-1`, never dropped.
  - `JSONObject config()`: `heap`, `cpuset`, `memLimitGb`, `settings`.
- [ ] **Step 1: Write the failing IT** `ServerUnderTestIT`, running against `arcadedata/arcadedb:latest` (JVM):
  - `startsPinnedAndLimited`: after `start(image, "0-1", 4, "2g", Map.of())`, the container's
    `HostConfig.CpusetCpus` is `"0-1"`, `Memory` is 4 GiB and `MemorySwap` is 4 GiB.
  - `reportsRssAndDisk`: after creating database `probe` and inserting 1000 docs, `peakRssBytes() > 0` and
    `diskBytes("probe") > 0`.
  - `imageInfoHasDigest`: `imageDigest` starts with `"sha256:"`.
- [ ] **Step 2: Run** `mvn -o -pl bench -am verify -Pintegration -Dit.test=ServerUnderTestIT`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same command. Expected: PASS. Then run it again with
  `-Darcadedb.bench.image=arcadedata/arcadedb:latest-native`. Expected: PASS, with RSS and disk > 0. This proves
  neither probe needs anything inside the container.
- [ ] **Step 5: Stop for review.**

### Task 5: CLI `run` and `list`

**Files:**
- Create: `BenchMain.java`, `workload/Scale.java`, `workload/WorkloadRegistry.java`
- Test: `BenchMainTest.java`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces:
  - `enum Scale { XS, S, M, L }`. `XS` exists only for tests and smoke runs.
  - `WorkloadRegistry.all()` returns `List<Workload>` in a fixed order: ingestion, graph-oltp, vector, fulltext, facet.
    Workloads register here as Tasks 6-8 add them.
  - CLI: `bench run --workload <name|all> --scale <XS|S|M|L> --image <ref> --variant <jvm|native> [--reps 5] [--warmup 2] [--timeout-min 30] [--seed 42] [--trigger manual] [--machine-id local] [--server-cpus auto] [--client-cpus auto] [--skip-idle-check] --out <dir>`.
    `auto` means `CpuTopology` P-cores for the server and E-cores for the client. The client pinning is applied by the
    calling script (`taskset`), and `BenchMain` only records it in `config`.
  - `bench list` prints one line per workload: `name  case1,case2,...`.
  - Each workload gets a fresh `ServerUnderTest`. `run` writes one `RunResult` with `source: "bench"` and exits 0 even
    when cases failed (failures are data). It exits 2 on bad arguments and 3 when the idle check fails.
- [ ] **Step 1: Write the failing tests:**
  - `listPrintsAllWorkloads`: the output contains `ingestion`, `graph-oltp`, `vector`, `fulltext` and `facet`.
    It starts with only the workloads that exist yet; extend it as Tasks 6-8 land.
  - `runWritesResultFile`: a fake workload registered with a test hook, `--skip-idle-check`, and `--out` set to a
    temp dir. Exactly one `*.json` is written, it parses with `RunResult.read`, and `info.arcadedb.variant` is `"jvm"`.
  - `badScaleExits2`.
  - `machineProbeDoesNotThrowHere`: `MachineProbe.probe("t")` returns an object with key `cpu`.
- [ ] **Step 2: Run** `mvn -o -pl bench -am test -Dtest=BenchMainTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 3: Implement** the argument parsing by hand (no CLI library). The fake workload is injected through a
  package-private `BenchMain.run(String[] args, List<Workload> registry)`.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 6: Workloads: ingestion and vector

**Files:**
- Create: `data/DatasetGenerator.java`, `workload/IngestionWorkload.java`, `workload/VectorWorkload.java`
- Test: `data/DatasetGeneratorTest.java`, `workload/VectorRecallTest.java`, `workload/IngestionWorkloadIT.java`, `workload/VectorWorkloadIT.java`

**Interfaces:**
- Consumes: `Workload`, `WorkloadContext`, `LatencyRecorder`, `ServerUnderTest`.
- Produces:
  - `DatasetGenerator(long seed)`:
    - `Iterator<String> csvRows(int n)`: the columns `id,name,age,city,score`;
    - `float[][] vectors(int n, int dim)`: unit-normalized, from `SplittableRandom(seed)`;
    - `String text(int words)`: words taken from `english_words.txt` (`load-tests` test resource).
  - `static int[] VectorRecall.exactTopK(float[][] base, float[] query, int k)`: cosine brute force.
  - `static double VectorRecall.recall(int[] approx, int[] exact)`
- Sizes per scale (designer's numbers, to be adjusted in Task 13's dry run to fit 3 h):

  | | XS | S | M | L |
  |---|---|---|---|---|
  | ingestion rows | 10k | 200k | 2M | 10M |
  | vectors (dim 128) | 2k | 20k | 200k | 1M |
  | vector queries | 50 | 200 | 1000 | 1000 |

- Ingestion cases and metrics:
  - `bulk-import-csv`: the CSV is copied into the container, then `IMPORT DATABASE file:///tmp/rows.csv`. Metrics:
    `records_per_s` (higher), `total_s` (lower), `disk_bytes` (lower), `peak_rss_bytes` (lower).
  - `tx-insert-1t` and `tx-insert-8t`: SQL `INSERT` in batches of 500 per transaction over HTTP. Metrics:
    `records_per_s`, plus `latency_p50_ms`/`p95`/`p99` per batch.
- Vector cases:
  - `build`: insert the vectors, then `CREATE INDEX ON Vec (v) LSM_VECTOR METADATA {dimensions: 128, similarity: 'COSINE'}`.
    Metrics: `build_s`, `disk_bytes`, `peak_rss_bytes`.
  - `knn-k10`: `SELECT expand(vectorNeighbors('Vec[v]', :q, 10))` per query. Metrics: `queries_per_s`, latency
    p50/p95/p99, and `recall_at_10` (higher), averaged over the queries.

- [ ] **Step 1: Write the failing tests:**
  - `DatasetGeneratorTest.sameSeedSameData`: two generators with seed 42 produce identical first 100 CSV rows and
    vectors. Seed 43 produces different ones.
  - `DatasetGeneratorTest.vectorsAreUnitLength`: every norm is within 1e-5 of 1.
  - `VectorRecallTest.exactTopKOnTinySet`: with base `{[1,0],[0,1],[0.7,0.7]}`, query `[1,0]` and k=2, the result is `{0,2}`.
  - `VectorRecallTest.recallFraction`: `recall({1,2,3,4},{1,2,5,6})` is 0.5.
  - `VectorWorkloadIT.recallIsAlwaysReported` (scale XS): the `knn-k10` case is `OK`, and its metrics include
    `recall_at_10` with median > 0.90.
  - `VectorWorkloadIT.lowRecallFailsCase`: params `{"efSearch": 1}` (or whichever smallest search-quality knob
    `LSM_VECTOR` exposes; look it up in `engine` before writing the test) give a case `FAILED` with an error containing
    `"recall"`, and the metrics are still present.
  - `IngestionWorkloadIT.allCasesOkAtXS`: three cases, all `OK`, and `records_per_s > 0`.
- [ ] **Step 2: Run** `mvn -o -pl bench -am test -Dtest='DatasetGeneratorTest,VectorRecallTest' -Dsurefire.failIfNoSpecifiedTests=false`,
  then `mvn -o -pl bench -am verify -Pintegration -Dit.test='IngestionWorkloadIT,VectorWorkloadIT'`. Expected: FAIL.
- [ ] **Step 3: Implement.** The recall threshold is the constant `VectorWorkload.MIN_RECALL = 0.90`. A case below it
  throws after its metrics have been attached. To allow that, extend `BenchRunner` with
  `class CaseFailedWithMetrics extends Exception { List<Metric> metrics; }`, so the runner keeps the metrics on a
  `FAILED` case. Add a `BenchRunnerTest` for that path. Register both workloads in `WorkloadRegistry`.
- [ ] **Step 4: Run** both commands. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 7: Workloads: graph OLTP and full-text

**Files:**
- Create: `workload/GraphOltpWorkload.java`, `workload/FullTextWorkload.java`
- Test: `workload/GraphOltpWorkloadIT.java`, `workload/FullTextWorkloadIT.java`

**Interfaces:**
- Consumes: as in Task 6. The graph is built with `DatabaseWrapper` from the `load-tests` test-jar
  (`createDatabase`, `createSchema`, `addUserAndPhotos`, `createFriendships`, `createLike`), so the schema matches the
  existing load tests.
- Graph sizes: users / friendships at XS 1k/5k, S 20k/200k, M 200k/2M, L 1M/10M. Each query runs 1000 times per rep,
  with ids picked from the seeded generator.
- Graph OLTP cases. Each one exists in a Cypher and a SQL variant, named `<case>-cypher` and `<case>-sql`:
  - `point-lookup`: a `User` by `id`;
  - `hop1`, `hop2` and `hop3`: friends at 1, 2 and 3 hops, distinct count;
  - `pattern-like`: friends who liked the same photo (MATCH pattern).

  Metrics: `queries_per_s`, latency p50/p95/p99.
- Full-text sizes: documents of 200 words, at XS 2k, S 50k, M 500k, L 2M.
- Full-text cases:
  - `build`: `CREATE INDEX ON Doc (body) FULL_TEXT`. Metrics: `build_s`, `disk_bytes`.
  - `bm25-single-term`, `bm25-multi-term` and `bm25-phrase`: `SEARCH_INDEX('Doc[body]', :q)` with the terms drawn by
    seed from the word list. Metrics: `queries_per_s`, latency p50/p95/p99.
  - `hybrid`: `bm25-single-term` combined with a vector kNN on a 128-dim embedding column. Use the hybrid search
    function the engine exposes; check `engine` for its SQL name before writing the test.
- [ ] **Step 1: Write the failing ITs** (scale XS):
  - `GraphOltpWorkloadIT.allCasesOk`: 12 cases, all `OK`.
  - `GraphOltpWorkloadIT.cypherAndSqlAgree`: for a fixed seed, `hop2-cypher` and `hop2-sql` return the same distinct
    count for the first 10 ids. This guards against comparing two engines that answer different questions.
  - `FullTextWorkloadIT.allCasesOk`: 5 cases, all `OK`.
  - `FullTextWorkloadIT.queriesReturnHits`: at least 90% of `bm25-single-term` queries return at least one hit, so the
    benchmark is not timing empty results.
- [ ] **Step 2: Run** `mvn -o -pl bench -am verify -Pintegration -Dit.test='GraphOltpWorkloadIT,FullTextWorkloadIT'`. Expected: FAIL.
- [ ] **Step 3: Implement** both workloads and register them.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 8: Workload: faceting

**Files:**
- Create: `workload/FacetWorkload.java`
- Test: `workload/FacetWorkloadIT.java`

**Interfaces:**
- Consumes: `FacetSchema`, `FacetDatabaseWrapper.ingestConcurrently` and `FacetSnapshot` from the `load-tests` test-jar.
  Read `FacetLoadTestTemplate` to see how they are combined, and reuse that sequence instead of rewriting it.
- Records at XS 10k, S 200k, M 2M, L 10M.
- Cases:
  - `facet-query-after-load`: the facet query from `FacetLoadTestTemplate`, repeated. Metrics: `queries_per_s`,
    latency p50/p95/p99.
  - `facet-under-ingest`: the same query while `ingestConcurrently` writes. Metrics: query latency p50/p95/p99,
    `ingest_records_per_s`, and `facet_lag_ms` (lower), taken from `FacetSnapshot.lagMs()` at the end.
- [ ] **Step 1: Write the failing IT** `FacetWorkloadIT.bothCasesOkAndLagRecorded` (XS): both cases `OK`, and the
  `facet_lag_ms` metric is present and >= 0.
- [ ] **Step 2: Run** `mvn -o -pl bench -am verify -Pintegration -Dit.test=FacetWorkloadIT`. Expected: FAIL.
- [ ] **Step 3: Implement** and register the workload. If a `FacetDatabaseWrapper` helper is test-scoped in a way the
  test-jar does not expose, make it `public` in `load-tests`, and change nothing else in that module.
- [ ] **Step 4: Run** the same command, then `mvn -o -pl load-tests -am verify -Pintegration -Dit.test=SingleServerFacetLoadTestIT`
  to check that `load-tests` still passes. Expected: PASS for both.
- [ ] **Step 5: Stop for review.**

### Task 9: Results store: init and uploader

**Files:**
- Create: `store/ResultsStore.java`, `store/Uploader.java`
- Test: `store/UploaderIT.java`

**Interfaces:**
- Consumes: `RunResult` (Task 1).
- Produces:
  - `ResultsStore.init(String url, String rootUser, String rootPassword, String writerPassword, String readerPassword)`.
    It is idempotent and creates the following:
    - database `perf`;
    - document type `Run`, with a unique index on `runId`;
    - document type `Measurement`, with properties `runId`, `workload`, `case`, `scale`, `metric`, `unit`, `better`,
      `median`, `values` (LIST), `status`, `error`, `commit`, `variant`, `machineId` and `startedAt` (DATETIME);
    - a non-unique index on `(workload, case, scale, metric, variant, machineId, startedAt)` and one on `runId`;
    - users `perf-writer` (create, read and delete on `Run` and `Measurement` in `perf`) and `perf-reader` (read).
  - `Uploader.upload(String url, String user, String password, RunResult run)`. In one transaction it deletes the
    `Measurement` and `Run` records with that `runId`, then inserts one `Run` and one `Measurement` per (case, metric).
    A case with no metrics gets one `Measurement` with `metric = "_status"` and `median = null`, so failures show up
    in the queries.
  - CLI: `bench init-store --url ... --root-password-env VAR --writer-password-env VAR --reader-password-env VAR`
    and `bench upload --url ... --user perf-writer --password-env VAR <file|dir>`. Passwords come from environment
    variables, never from the command line.
- [ ] **Step 1: Write the failing ITs** against an `arcadedata/arcadedb:latest` container:
  - `initIsIdempotent`: running init twice raises no error, and both types exist.
  - `uploadInsertsRows`: after uploading `run-sample.json`, `Run` has 1 record and `Measurement` has exactly
    sum(metrics per case).
  - `reuploadReplacesRun`: change one median, upload again. The counts are unchanged and the new median is stored.
  - `failedUploadLeavesPreviousCopy`: upload, then upload a copy whose last `Measurement` breaks a schema constraint
    (`startedAt` not a date). The upload throws, and the store still holds the first copy, with unchanged counts.
  - `writerCannotDropType`: as `perf-writer`, `DROP TYPE Run` is refused.
  - `readerCannotDelete`: as `perf-reader`, `DELETE FROM Run` is refused.
  - `failedCaseVisible`: a case with `FAILED` and no metrics becomes one `_status` row with `status = 'failed'`.
- [ ] **Step 2: Run** `mvn -o -pl bench -am verify -Pintegration -Dit.test=UploaderIT`. Expected: FAIL.
- [ ] **Step 3: Implement** with `RemoteDatabase`, using `begin()`/`commit()` around the whole upload.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 10: Exporter (series for the dashboard)

**Files:**
- Create: `store/Exporter.java`
- Test: `store/ExporterIT.java`

**Interfaces:**
- Consumes: the store schema from Task 9.
- Produces:
  - `Exporter.export(String url, String user, String password, Path outDir)`, which writes:
    - `index.json`: `{"generatedAt": ..., "workloads": [{"workload", "cases": [{"case", "scale", "metrics": [{"metric", "unit", "better", "file"}]}]}]}`;
    - one file per (workload, case, scale, metric), named `series/<workload>/<case>__<scale>__<metric>.json`:
      `{"metric", "unit", "better", "lines": [{"machineId", "variant", "points": [{"t", "commit", "tag", "median", "status", "runId"}]}]}`.
      Points are sorted by `t`. A failed point has `median: null` and `status: "failed"`.
    - `latest.json`: for each series, the last point and the previous point.
  - CLI: `bench export --url ... --user perf-reader --password-env VAR --out <dir>`.
- [ ] **Step 1: Write the failing ITs:**
  - `exportsOneFilePerSeries`: upload 3 runs (2 jvm, 1 native) of `run-sample.json` with shifted `startedAt`. The
    expected files exist, each has two lines (jvm, native), and the jvm line has 2 points in time order.
  - `failedPointExportedAsNull`: a `FAILED` case appears as a point with `median == null`.
  - `machinesNeverShareALine`: the same series uploaded from `machineId` `perf-01` and `laptop` gives two separate lines.
- [ ] **Step 2: Run** `mvn -o -pl bench -am verify -Pintegration -Dit.test=ExporterIT`. Expected: FAIL.
- [ ] **Step 3: Implement.** Use one query per (workload, case, scale, metric), ordered by `startedAt`, against the
  composite index.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Stop for review.**

### Task 11: LDBC adapter

**Files:**
- Create: `ldbc/LdbcAdapter.java`
- Test: `ldbc/LdbcAdapterTest.java`, `bench/src/test/resources/mode1-summary.json`
- Modify (LDBC repo `ldbc_graphalytics_platforms_arcadedb`): `README.md`, adding a "Building against a snapshot" note:
  `mvn package -DskipTests -Darcadedb.version=<x.y.z-SNAPSHOT>` after `mvn install -DskipTests` in arcadedb. The
  `arcadedb.version` property already exists in its `pom.xml:20`, so no code change is needed there.

**Interfaces:**
- Consumes: the `mode1-summary.json` shape that `scripts/run_mode1.py` writes:
  `{<mode>: {"algorithms": {<ALGO>: {"processing_time": <s>, "success": <bool>}}}}`.
- Produces:
  - `RunResult LdbcAdapter.convert(JSONObject summary, RunInfo info, String dataset)`: one `CaseResult` per (mode,
    algorithm), with `workload = "graph-analytics"`, `case = <mode>-<algo lowercase>`, `scale = dataset`, and one
    metric `processing_s` (lower). A `success: false` algorithm becomes `FAILED`. The result has `source = "ldbc"`
    and `variant = "jvm"`.
  - CLI: `bench ldbc-convert --summary <path> --dataset datagen-7_5-fb --commit <sha> --machine-id perf-01 --trigger nightly --out <dir>`.
- [ ] **Step 1: Write the fixture** from a real run's `mode1-summary.json` (look under `weekly-results/` in the LDBC
  repo), with one algorithm set to `success: false`.
- [ ] **Step 2: Write the failing tests:**
  - `convertsEveryAlgorithm`: the number of cases equals the number of (mode, algo) pairs in the fixture, and their
    values equal `processing_time`.
  - `failedAlgorithmIsFailedCase`.
  - `sourceIsLdbcAndVariantJvm`.
- [ ] **Step 3: Run** `mvn -o -pl bench -am test -Dtest=LdbcAdapterTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.
- [ ] **Step 4: Implement.**
- [ ] **Step 5: Run** the same command. Expected: PASS.
- [ ] **Step 6: Stop for review.**

### Task 12: Native image revision label

**Files:**
- Modify: `.github/workflows/native-image.yml:606-609`, the `docker buildx build` step.

- [ ] **Step 1: Add** `--label "org.opencontainers.image.revision=$(git rev-parse HEAD)"` and
  `--label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"` to the build command. Use the checked-out
  tree's HEAD, not `github.sha`, because a dispatch can build another ref.
- [ ] **Step 2: Verify** with `actionlint .github/workflows/native-image.yml` (expected: no errors). Then, after the
  next nightly,
  `docker pull arcadedata/arcadedb:latest-native && docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' arcadedata/arcadedb:latest-native`
  must print a 40-character SHA. That check is owed until the next nightly runs.
- [ ] **Step 3: Stop for review.**

### Task 13: Private infra repo: provisioning `perf-01`

**Repo:** `ArcadeData/arcadedb-perf-infra` (new, private). Commits are allowed here.

**Files:**
- Create: `provision/provision.sh`, `provision/perf-mode.sh`, `provision/idle-check.sh`, `provision/results-server.sh`,
  `provision/test/perf-mode.bats`, `provision/test/idle-check.bats`, `README.md`

**Interfaces:**
- Produces:
  - `perf-mode.sh on|off|status`. `on` saves the current governor and `no_turbo` to
    `/var/lib/perf/perf-mode.saved`, then sets `performance` on every CPU and `no_turbo=1`. `off` restores the saved
    values. `status` prints `governor=<x> no_turbo=<y>`. Workflows call `on` behind a `trap 'perf-mode.sh off' EXIT`.
    The sysfs root can be overridden with `PERF_SYSFS_ROOT` for tests.
  - `idle-check.sh`: exits 3 with a reason when the 1-minute load is above `0.10 * nproc`, or when memory used
    (`MemTotal - MemAvailable`) is above 8192 MB. It applies the same thresholds as `IdleCheck`.
  - `results-server.sh start|stop|status`: runs `arcadedata/arcadedb:<latest release>` as container `perf-results`,
    with a volume `/var/lib/perf/results-db`, pinned to the E-cores (from `lscpu --extended`), on port 2480 bound to
    the Tailscale interface address, or `127.0.0.1` until Tailscale is installed.
  - `provision.sh` is idempotent. It checks Docker, creates the `perf` user in the `docker` group, installs the GitHub
    Actions runner as a service with labels `self-hosted,perf` (registration token from the env), adds a sudoers rule
    letting `perf` run only `perf-mode.sh`, and installs Temurin 21 and Temurin 25 under `/opt/jdk`. Graphalytics needs
    25. Tailscale is installed only with `--with-tailscale <authkey>`, using tag `tag:perf-runner`.
- [ ] **Step 1: Write the failing bats tests** against a fake sysfs tree in a temp dir:
  - `perf-mode on then off restores powersave and no_turbo 0`;
  - `trap restores even when the script is killed`: start a subshell with the trap and `on`, kill it with TERM, then
    check that `status` shows the original values;
  - `idle-check fails on high load` and `idle-check passes when idle`, with `/proc/loadavg` and `/proc/meminfo`
    overridable through `PERF_PROC_ROOT`.
- [ ] **Step 2: Run** `bats provision/test`. Expected: FAIL.
- [ ] **Step 3: Implement** the scripts, and make `shellcheck provision/*.sh` clean.
- [ ] **Step 4: Run** `bats provision/test && shellcheck provision/*.sh`. Expected: PASS.
- [ ] **Step 5: Run on `perf-01`** (manual, by the maintainer): `sudo ./provision/provision.sh`, then
  `perf-mode.sh on && perf-mode.sh status` (expected `governor=performance no_turbo=1`), then `perf-mode.sh off`
  (expected `governor=powersave no_turbo=0`), then `results-server.sh start` and
  `curl -s localhost:2480/api/v1/ready` (expected HTTP 204). Then run `bench init-store` once.
- [ ] **Step 6: Commit** in `arcadedb-perf-infra`.

### Task 14: Private infra repo: nightly and release workflows

**Files:**
- Create: `scripts/resolve-commit.sh`, `scripts/already-measured.sh`, `scripts/run-all.sh`, `test/resolve-commit.bats`,
  `.github/workflows/nightly.yml`, `.github/workflows/release.yml`

**Interfaces:**
- Consumes: the `bench` CLI (Tasks 5, 9-11), the scripts from Task 13, and the image label from Task 12.
- Produces:
  - `resolve-commit.sh <native-image-ref>`: prints the revision label. It exits 4 when the label is missing.
  - `already-measured.sh <commit> <machineId>`: queries the results server as `perf-reader` and exits 0 if a
    `bench`-source `Run` exists for that commit and machine.
  - `run-all.sh --commit <sha> --native-image <ref> --jvm-image <ref> --scale M --trigger nightly`:
    1. `idle-check.sh`;
    2. `results-server.sh stop`;
    3. `trap 'perf-mode.sh off; results-server.sh start' EXIT`, then `perf-mode.sh on`;
    4. `taskset -c <E-cores> java -jar bench-cli.jar run --workload all --variant jvm ...`, then the same with
       `--variant native`;
    5. the LDBC Mode 1 run (`run_mode1.py` with `--dist` built against the snapshot), then `bench ldbc-convert`;
    6. on exit, the trap restores the CPU mode and restarts the results server;
    7. `bench upload` for every file, `bench export --out site/series`.
  - `nightly.yml`:
    - `on: schedule: cron "30 4 * * *"` (after `native-image.yml`, which runs around 01:41 UTC), plus
      `workflow_dispatch` with inputs `workloads`, `scale` and `commit`;
    - `runs-on: [self-hosted, perf]`, `concurrency: perf-01`, `timeout-minutes: 240`;
    - steps: pull `latest-native`; `resolve-commit.sh`; `already-measured.sh` (skip the job if measured, which is not a
      failure); checkout `ArcadeData/arcadedb` at the commit; `./mvnw -B install -Pdocker -DskipTests`, tagging the JVM
      image `arcadedb-bench:<sha>-jvm`; build `bench`; checkout and build the LDBC repo with
      `-Darcadedb.version=<snapshot>`; `run-all.sh`; checkout `ArcadeData/arcadedb-perf` with the publish token; copy
      the results to `data/YYYY/MM/` and the series to `site/series/`; commit; push.
  - `release.yml`: `on: repository_dispatch: types: [arcadedb-release]` and `workflow_dispatch` with input `tag`. It
    uses `arcadedata/arcadedb:<tag>` and `arcadedata/arcadedb:<tag>-native`, scale `L`, trigger `release`, and runs
    the same steps, without the skip check.
  - Secrets: `PERF_WRITER_PASSWORD`, `PERF_READER_PASSWORD`, `PERF_PUBLISH_TOKEN` (a fine-grained PAT or GitHub App
    token with contents:write on `ArcadeData/arcadedb-perf` only). Tailscale needs no secret on the runner once
    `provision.sh --with-tailscale` has run.
- [ ] **Step 1: Write the failing bats tests** for `resolve-commit.sh`, with `docker` stubbed by a function:
  - `label present prints sha`;
  - `label missing exits 4`;
  - `nightly skip when already measured`: `already-measured.sh` stubbed to exit 0 makes the decision helper
    `should-run.sh` print `skip`.
- [ ] **Step 2: Run** `bats test`. Expected: FAIL.
- [ ] **Step 3: Implement** the scripts and workflows, and make `actionlint` and `shellcheck` clean.
- [ ] **Step 4: Run** `bats test && actionlint && shellcheck scripts/*.sh`. Expected: PASS.
- [ ] **Step 5: Dry run on `perf-01`:** `workflow_dispatch` with scale `S`. Check that the run finishes, the files land
  in `arcadedb-perf`, and the governor is back to `powersave` afterwards. Then dispatch with scale `M` and record the
  total time. If it is over 3 h, shrink the `M` sizes in Tasks 6-8 (the `Scale` table) and repeat. Record the final
  `M` sizes and the measured duration in the infra `README.md`.
- [ ] **Step 6: Commit** in `arcadedb-perf-infra`.

### Task 15: Public results repo and dashboard

**Repo:** `ArcadeData/arcadedb-perf` (new, public). Commits are allowed here.

**Files:**
- Create:
  - `README.md`, covering what is measured, the machine, how to rerun locally (`bench run ...`), and that the raw
    JSON in `data/` is the source of truth;
  - `site/index.html`, `site/workload.html`, `site/app.js`, `site/style.css`;
  - `site/fixtures/series/...`: a copy of the Task 10 IT output;
  - `.github/workflows/pages.yml`, which deploys `site/` to Pages on push to `main`.

**Interfaces:**
- Consumes: the exporter output (Task 10): `index.json`, `latest.json` and `series/**`.
- Produces:
  - `index.html`: one card per workload. Each card shows a sparkline per primary metric (`records_per_s`,
    `queries_per_s`, `processing_s`, `recall_at_10`) for jvm and native, the latest value, and the % change against
    the previous release-tagged point.
  - `workload.html?w=<workload>`: one ECharts line chart per (case, scale, metric), with one line per
    (machineId, variant). Release-tag points are marked on the x-axis. Failed points are drawn as red markers at the
    axis base, not interpolated. The tooltip shows the commit (linked to GitHub), the median, and a link to the raw
    JSON in `data/`. Graph analytics carries the note "JVM only: Graphalytics runs embedded".
  - ECharts is loaded from `https://cdnjs.cloudflare.com/ajax/libs/echarts/5.5.1/echarts.min.js` (Apache-2.0, exact
    version). Light and dark themes come from `prefers-color-scheme`. The page must have no horizontal scroll at
    375 px wide.
- [ ] **Step 1: Serve the fixtures:** `cp -r site/fixtures/series site/series && python3 -m http.server -d site 8000`.
- [ ] **Step 2: Implement** the pages against the fixtures.
- [ ] **Step 3: Verify in a browser** (manual or Claude in Chrome):
  - the index shows 6 workload cards (graph-analytics included);
  - the vector page shows `recall_at_10` next to `queries_per_s`;
  - the fixture's failed point is a red marker;
  - jvm and native are two lines;
  - at 375 px there is no horizontal scroll;
  - dark mode is readable.
- [ ] **Step 4: Remove** `site/series` from the commit (the nightly fills it), commit, and enable Pages.
- [ ] **Step 5: After the first real nightly,** open the Pages URL and check that the latest point's commit matches
  the native image's revision label from that night.

---

## Spec coverage check

| Spec item | Task |
|---|---|
| `bench` module, CLI, warm-up/reps/median | 1, 2, 5 |
| Workloads phase 1 (ingestion, OLTP, vector + recall, full-text + hybrid, facet) | 6, 7, 8 |
| Core metrics (throughput, p50/95/99, RSS, disk) with unit and better | 2, 4, 6-8 |
| Results JSON, failed cases kept | 1, 2 |
| Results server, users, idempotent replace | 9, 13 |
| Public raw JSON and static dashboard, machines never share a line | 10, 14, 15 |
| JVM and native at the same commit, revision label | 12, 14 |
| Nightly `M` 3 h, release `L` | 14 |
| `perf-01` pinning, governor and turbo, swap off, idle check | 3, 4, 13, 14 |
| LDBC on the runner against the commit, JVM-only | 11, 14 |
| Tailscale (deferred until upload is tested) | 13 (`--with-tailscale`) |
| Phase 2 (time series, HA, protocols) | not in this plan |
