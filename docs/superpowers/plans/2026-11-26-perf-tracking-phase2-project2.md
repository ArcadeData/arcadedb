# Performance tracking phase 2, project 2 (time series, correctness, filtered BM25, wire protocols) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `timeseries`, `correctness` and `protocols` workloads and the two filtered BM25 cases of `fulltext` to the
bench, each correct by construction (a wrong answer fails its case), sized for the S weeknight lane and the M Saturday lane.

**Architecture:** Each workload is a `Workload` in `bench/src/main/java/com/arcadedb/bench/workload`, registered in
`WorkloadRegistry`, with pure static check functions unit-tested for their failure, and one Docker IT at XS. Four PRs, one
per workload, in task order (PR A: Task 1; PR B: Task 2; PR C: Task 3; PR D: Tasks 4-6). Each PR is dispatched at S and M
from its branch commit before merge (Task 7), since merging adds it to the nightly `all`.

**Tech Stack:** Java 21, JUnit 5 + AssertJ, Testcontainers (ITs only), ArcadeDB remote HTTP and gRPC clients (already in
`bench`), Neo4j Java driver `${neo4j-driver.version}` (root pom, Apache 2.0) and PostgreSQL JDBC 42.7.14 (BSD-2-Clause), both new to `bench`.

**Spec:** `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md` (section 3: time series, wire protocols,
correctness, filtered BM25; "Order and delivery"; "Testing"). Inputs: `docs/superpowers/specs/2026-11-21-perf-tracking-phase2-3-inputs.md`
(candidates #2 to #4, #9).

## Global Constraints

- JSON only through `com.arcadedb.serializer.json.JSONObject` / `JSONArray`; getters with defaults.
- `final` on locals and parameters; single-statement `if` without braces; imports, no fully qualified names; no `System.out`.
- Tests: `assertThat(...)` style; never assert on raw wall-clock time; no hand-picked ports; Docker tests are `*IT`.
- Bench tests: `./mvnw -o -pl bench -am test -Dtest='com.arcadedb.bench.**.*Test' -Dsurefire.failIfNoSpecifiedTests=false`;
  read the `Results:` summary. Compile after every Java change.
- **S sizing rule** (spec section 2): a timed operation takes at least 5 s per repetition, a latency case runs at least
  **1,000** operations per repetition. A case that cannot meet it at S within its budget is left out at S through
  `Workload.cases(Scale)` (merged with the S sizing PR), never measured as noise.
- **Case cap**: warm-up + reps of one case fit the 30 min per-step limit and the 120 min case cap at every scale it runs at.
- **S budget per variant** (JVM and native each), so the weeknight job stays well inside 240 min (today about 70 min for
  both variants): `timeseries` 12 min, `correctness` 8 min, the two filtered BM25 cases 10 min, `protocols` 30 min.
- Metric units the phase 3 gate knows: `ms`, `s`, `bytes`, rates (`.../s`), `ratio`.
- A new dependency goes in `bench/pom.xml` and `ATTRIBUTIONS.md` (license allow-list: Apache 2.0, BSD-2 are allowed).
- The infra repository is private: never named or linked from arcadedb PRs, issues or docs.
- Commits authored by robfrank only, no Co-Authored-By; no em dash anywhere. PRs say `Part of #12433`, never `Closes`.

## Review Focus

- A protocol that cannot carry a shape (the 15 x 768 float parameter over PostgreSQL, a 10-statement transaction over
  HTTP without a session) must yield an **absent** case, never a faked one that runs a different statement; pinned in Task 6
  (`casesOmitWhatAProtocolCannotCarry`).
- Parity compares values normalised by type: an `Integer` 3 from Bolt and a `Long` 3 from HTTP, a `float[]` and a list of
  doubles, are equal; a row order difference on an `ORDER BY` query is a mismatch, on an unordered one it is not; pinned in
  Task 5 (`ParityTest`).
- The time series aggregate reads sealed data only (see `TimeSeriesDatabaseWrapper.aggregate`): the verification check
  must wait for sealing with a bounded poll and fail with the values it saw, not pass on an empty result; pinned in Task 1
  (`emptyAggregateFailsTheCheck`).
- A count check that matches zero rows passes for the wrong reason: every correctness predicate must match at least one
  row and fewer than all; pinned in Task 2 (`predicatesAreSelective`).
- 16 clients record latency from 16 threads: `LatencyRecorder` is not thread-safe, so each thread owns one and they are
  merged after the threads join; pinned in Task 6 (`sixteenClientsRecordEveryOperation`).

---

### Task 1: Time series workload (PR A)

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/TimeSeriesWorkload.java`,
  `bench/src/main/java/com/arcadedb/bench/workload/LineProtocolClient.java`
- Modify: `bench/src/main/java/com/arcadedb/bench/workload/WorkloadRegistry.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/TimeSeriesWorkloadTest.java`,
  `bench/src/test/java/com/arcadedb/bench/workload/TimeSeriesWorkloadIT.java`

**Interfaces:**
- Consumes: `TimeSeriesDatabaseWrapper` (load-tests test-jar) for `createDatabase()`, `createSchema()` (type `sensor`,
  tags `sensor_id`, `region`, fields `temperature`, `humidity`), `countPoints()`, `aggregate(sensorId)` returning
  `AggResult(avg, min, max, count)`; `ServerUnderTest.wrapper()`; `LatencyRecorder`, `Measurement.of`, `Metrics.rate`,
  `Metrics.latencies`.
- Produces: `TimeSeriesWorkload` named `timeseries`, cases `ingest-line-protocol`, `ingest-sql`, `range-query`,
  `aggregate-query`, `aggregate-continuous` (constants `INGEST_LP`, `INGEST_SQL`, `RANGE`, `AGGREGATE`, `AGGREGATE_CA`).
  `static int pointsPerRep(Scale)`, `static int sensors(Scale)`, `static int pointsPerSensor(Scale)`, `static final int BATCH = 500`.
  `static void checkVerification(AggResult got, long expectedCount, double expectedMin, double expectedMax)` throws
  `IllegalStateException` naming expected and got. `LineProtocolClient(ServerWrapper server, String database)` with
  `void write(String body)` (POST `/api/v1/ts/<db>/write?precision=ms`, basic auth root, throws on a non-2xx or a partial write).

Decisions:
- The bench posts line protocol itself (`LineProtocolClient`, `java.net.http.HttpClient`) because the wrapper times its
  batches into a global Micrometer timer; one `LatencyRecorder` sample per batch of `BATCH` points.
- `ingest-sql`: one `sqlscript` request per batch of `BATCH` INSERTs, one sample per request.
- Each ingest rep writes into fresh sensor ids (`ingest-<rep>-<n>`), so reps never overwrite each other; at S
  `pointsPerRep(S)` is at least `1_000 * BATCH` (the S rule on batches).
- `range-query` and `aggregate-*` read a dataset loaded once in `prepare()`: `sensors(scale)` sensors x
  `pointsPerSensor(scale)` points, plus the verification series (`ingestVerificationSeries()` semantics: min 15.0, max 34.0,
  known count). Queries per rep: 1,000 at every scale. Window: one random sensor, a random 1 h window.
- `aggregate-query`: `SELECT ts.timeBucket('1h', ts) AS tbucket, avg(temperature), min(temperature), max(temperature)
  FROM sensor WHERE sensor_id = ? AND ts BETWEEN ? AND ? GROUP BY tbucket`.
- `aggregate-continuous`: `prepare()` creates `CREATE CONTINUOUS AGGREGATE sensor_hourly AS SELECT ts.timeBucket('1h', ts)
  AS hr, sensor_id, avg(temperature) AS avgT, min(temperature) AS minT, max(temperature) AS maxT FROM sensor GROUP BY hr,
  sensor_id` (if grouping by a tag is refused, group by `hr` only and query the hour); each rep runs 1,000 reads of it and
  then one ingest burst of `pointsPerRep(scale)` points while it is maintained. Metrics: `queries_per_s`, p50/p95/p99 of the
  reads, and `ingest_points_per_s_maintained` (rate).
- Correctness: every case's `prepare()` ends with `checkVerification` on the verification series (bounded poll, 60 s, for
  sealing; the last values seen are in the failure).

- [ ] **Step 1: Write failing unit tests** in `TimeSeriesWorkloadTest`: `casesAreTheFiveOfTheSpec` (exact list);
  `sAndAboveMeetTheBatchRule` (`pointsPerRep(S) / BATCH >= 1_000`); `verificationMatchPasses`
  (`checkVerification(new AggResult(24.5, 15.0, 34.0, 1000), 1000, 15.0, 34.0)` does not throw);
  `wrongMinFailsTheCheck`, `wrongCountFailsTheCheck` (message contains `expected` and the got value);
  `emptyAggregateFailsTheCheck` (`checkVerification(null, ...)` throws, message contains `no rows`).
- [ ] **Step 2: Run** `TimeSeriesWorkloadTest`; expect compile failure.
- [ ] **Step 3: Implement** `TimeSeriesWorkload`, `LineProtocolClient`, register in `WorkloadRegistry.all()` after `facet`.
- [ ] **Step 4: Run** `TimeSeriesWorkloadTest`; pass. Write `TimeSeriesWorkloadIT` in the style of `FacetWorkloadIT`
  (XS, JVM image, every case `ok`, every metric present); run it once if Docker is up, else say so in the report.
- [ ] **Step 5: Run** all bench unit tests; pass. Commit
  `feat(bench): time series workload - ingest, range, aggregate and continuous aggregate (#12433)`.

### Task 2: Correctness workload (PR B)

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/CorrectnessWorkload.java`
- Modify: `WorkloadRegistry.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/CorrectnessWorkloadTest.java`, `CorrectnessWorkloadIT.java`

**Interfaces:**
- Consumes: `VectorRecall.exactTopK`, `DatasetGenerator.embeddingSpace` / `embeddings`, `VectorWorkload.MIN_RECALL` (0.90),
  `CaseFailedWithMetrics` (a failed case that keeps its metrics), `RemoteClients.open` / `count`.
- Produces: `CorrectnessWorkload` named `correctness`, cases `vector-recall-after-churn`, `index-vs-scan-count`,
  `count-star-vs-iterated`. `static void checkCount(String what, long expected, long got)` throws
  `IllegalStateException("<what>: expected <e>, got <g>")`. `static List<String> predicates()` (the index-vs-scan predicates,
  each a SQL condition on `v`). Metric `check_s` (unit `s`, lower is better) on every case, plus `recall_at_10`
  (`ratio`, higher is better) on the vector case.

Decisions:
- `vector-recall-after-churn`: vectors as in `VectorWorkload` (128 dims, embedding-like); per rep delete 10% and re-insert
  them with new embeddings, then recall@10 of 1,000 queries against brute force over the live set; under `MIN_RECALL` the
  case fails with its metrics (`CaseFailedWithMetrics`).
- `index-vs-scan-count`: type `Item` with `v` (LONG, indexed NOTUNIQUE) and `v_copy` (same value, no index); per rep delete
  and re-insert 5% of the rows, then for each predicate `count` through `v` vs through `v_copy` and vs the ledger the client
  keeps. Predicates: `= k`, `BETWEEN a AND b`, `IN [..5 values..]`, `> c`.
- `count-star-vs-iterated`: on `Item`, `SELECT count(*)` vs the number of rows of `SELECT @rid FROM Item` read to the end
  vs the ledger.
- Sizes: `items(Scale)` and `vectors(Scale)` chosen so each case is at least 5 s per rep at S and inside 8 min per variant
  for the three cases together.

- [ ] **Step 1: Failing tests:** `casesAreTheThreeOfTheSpec`; `equalCountsPass`; `differentCountsFailNamingBoth`
  (`checkCount("index", 10, 9)` throws, message contains `10` and `9`); `predicatesAreSelective` (on a generated ledger of
  `items(XS)` values, each predicate matches more than 0 and fewer than all rows, evaluated by a Java mirror
  `static long expectedMatches(String predicate, long[] values)`); `recallUnderGateFails` (a static
  `checkRecall(double recall, List<Metric> metrics)` throws `CaseFailedWithMetrics` at 0.89 and not at 0.90).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement**; register after `timeseries`.
- [ ] **Step 4: Run** unit tests; pass. `CorrectnessWorkloadIT` at XS; run once if Docker is up.
- [ ] **Step 5: Commit** `feat(bench): correctness next to the numbers - recall after churn, index vs scan, count(*) vs iterated (#12433)`.

### Task 3: Filtered BM25 cases (PR C)

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/workload/FullTextWorkload.java`,
  `bench/src/main/java/com/arcadedb/bench/data/DatasetGenerator.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/FullTextBroadTest.java`, `FullTextWorkloadIT.java` (extend)

**Interfaces:**
- Produces: cases `bm25-broad-or` and `bm25-broad-or-filtered` (constants `BROAD`, `BROAD_FILTERED`) appended to
  `FullTextWorkload.cases()`. `String DatasetGenerator.broadText(int words)` drawing from a small vocabulary
  (`static final String[] BROAD_VOCABULARY`); `static String broadQuery(SplittableRandom r)` (an OR of terms). `static int
  posts(Scale)`. Metrics `queries_per_s`, p50/p95/p99.

Decisions:
- The broad corpus is a **separate type** `Post` (`body` full-text indexed, `deleted_at` DATETIME, null on 90% of the
  rows) in the same database, built only in the `prepare()` of the two broad cases. `Doc` stays as it is, so the existing
  `fulltext` lines (build, the three BM25 shapes, hybrid) keep their history.
- Queries: top 50 by score; `bm25-broad-or-filtered` adds `AND deleted_at IS NULL`. Each query must return 50 rows, and no
  filtered row may have `deleted_at` set (checked on every query; a violation fails the case).
- Queries per rep: 1,000 at S (the S rule); `posts(Scale)` chosen so the two cases together stay inside 10 min per
  variant at S.

- [ ] **Step 1: Failing tests:** `broadQueriesMatchAbout45Percent` (over `posts(XS)` generated bodies and 50 generated
  queries, the mean share of bodies matching a query is between 0.40 and 0.50, matching = shares a term, computed in Java);
  `deletedShareIsTenPercent` (generated `deleted_at` flags: 0.08 to 0.12); `existingCasesKeepTheirOrder` (the first five
  entries of `cases()` are unchanged).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** unit tests; pass. Extend `FullTextWorkloadIT` to assert the two new cases are `ok` at XS; run once.
- [ ] **Step 5: Commit** `feat(bench): broad OR BM25 queries, plain and filtered by deleted_at (#12433)`.

### Task 4: Per-workload server settings and the Bolt and PostgreSQL ports (PR D)

**Files:**
- Modify: `bench/src/main/java/com/arcadedb/bench/runner/Workload.java`, `bench/src/main/java/com/arcadedb/bench/BenchMain.java`,
  `bench/src/main/java/com/arcadedb/bench/server/ServerUnderTest.java`, `bench/pom.xml`, `ATTRIBUTIONS.md`
- Test: `bench/src/test/java/com/arcadedb/bench/BenchMainTest.java`, `bench/src/test/java/com/arcadedb/bench/server/ServerUnderTestTest.java`

**Interfaces:**
- Produces: `default Map<String, String> Workload.serverSettings()` returning `Map.of()`.
  `BenchMain.ServerFactory.start(String image, String cpuset, int memLimitGb, String heap, Map<String, String> settings)`
  (the restart after a death passes the same settings). `ServerUnderTest` always exposes 2480, 50051, **7687** and **5432**;
  `int boltPort()`, `int postgresPort()` return the mapped ports.
- Dependencies: `org.neo4j.driver:neo4j-java-driver:${neo4j-driver.version}` and `org.postgresql:postgresql:42.7.14`
  (a `postgresql.version` property in `bench/pom.xml`), compile scope, shaded into the bench jar; one `ATTRIBUTIONS.md`
  entry each.

- [ ] **Step 1: Failing tests:** `BenchMainTest.eachWorkloadStartsItsServerWithItsOwnSettings` (a fake factory records the
  settings per start: a workload declaring `{"arcadedb.server.plugins": "X"}` gets it, the next workload gets `{}`);
  `BenchMainTest.aRestartKeepsTheWorkloadSettings` (with `DyingServers`); `ServerUnderTestTest.workloadSettingsWinOverDefaults`
  (`settings(Map.of("arcadedb.server.plugins", "A"))` keeps `A`).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement**; add the dependencies and attributions; run
  `python3 .github/scripts/check-license-allowlist.py` if it runs locally, else note it for CI.
- [ ] **Step 4: Run** all bench unit tests; pass.
- [ ] **Step 5: Commit** `feat(bench): a workload can declare its server settings, and the server maps the Bolt and PostgreSQL ports (#12433)`.

### Task 5: Protocol clients and parity (PR D)

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/protocol/ProtocolClient.java`, `HttpProtocolClient.java`,
  `GrpcProtocolClient.java`, `BoltProtocolClient.java`, `PostgresProtocolClient.java`, `Parity.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/protocol/ParityTest.java`

**Interfaces:**
- Produces: `enum Protocol { HTTP, GRPC, BOLT, POSTGRES }` (case-name form `http`, `grpc`, `bolt`, `pg`).
  `interface ProtocolClient extends AutoCloseable { List<Map<String, Object>> query(String cypher, Map<String, Object>
  params); void transaction(List<String> cyphers, List<Map<String, Object>> params); }`. `static ProtocolClient
  ProtocolClient.open(Protocol p, ServerUnderTest server, String database)`. HTTP and gRPC through `RemoteDatabase` /
  `RemoteGrpcDatabase` with language `opencypher`; Bolt through the Neo4j driver (`bolt://host:boltPort`, basic auth root);
  PostgreSQL through JDBC with the `{cypher}` statement prefix (positional parameters are bound by name order in the
  statement; a shape that cannot be expressed so is absent, see Task 6).
- Produces: `static Object Parity.normalise(Object value)` (any integral number -> `Long`, any floating number -> `Double`,
  arrays and lists -> `List` of normalised, maps -> `TreeMap` of normalised; strings unchanged) and
  `static String Parity.diff(List<Map<String, Object>> expected, List<Map<String, Object>> got, boolean ordered)`: null when
  equal after normalising, else a one-line description of the first difference (row index, column, both values).

- [ ] **Step 1: Failing tests** (`ParityTest`): `integerAndLongAreEqual`; `floatArrayAndDoubleListAreEqual`;
  `orderedDiffersOnRowOrder`; `unorderedIgnoresRowOrder`; `missingColumnIsADiff` (message names the column);
  `extraRowIsADiff`.
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement** the clients and `Parity`.
- [ ] **Step 4: Run** `ParityTest` and all bench unit tests; pass.
- [ ] **Step 5: Commit** `feat(bench): openCypher clients over HTTP, gRPC, Bolt and PostgreSQL, and a row parity check (#12433)`.

### Task 6: Protocols workload (PR D)

**Files:**
- Create: `bench/src/main/java/com/arcadedb/bench/workload/ProtocolsWorkload.java`
- Modify: `WorkloadRegistry.java`, `bench/src/main/java/com/arcadedb/bench/runner/LatencyRecorder.java`
- Test: `bench/src/test/java/com/arcadedb/bench/workload/ProtocolsWorkloadTest.java`, `ProtocolsWorkloadIT.java`,
  `bench/src/test/java/com/arcadedb/bench/runner/LatencyRecorderTest.java`

**Interfaces:**
- Consumes: Task 4 `serverSettings()`, `boltPort()`, `postgresPort()`; Task 5 `ProtocolClient`, `Parity`.
- Produces: `ProtocolsWorkload` named `protocols`, `serverSettings()` =
  `arcadedb.server.plugins` -> `PostgresProtocolPlugin,GrpcServerPlugin,BoltProtocolPlugin,PrometheusMetricsPlugin`, plus
  `arcadedb.grpc.enabled=true`, `arcadedb.grpc.port=50051`, `arcadedb.grpc.mode=standard`. `enum Shape { POINT, HOP1,
  PAGE_1000, UNWIND_MERGE_500, VECTOR_PARAM, TX_10 }` (case-name form `point`, `hop1`, `page1000`, `merge500`,
  `vector-param`, `tx10`). Cases `<shape>-<protocol>-<clients>c` for clients 1 and 16, built by
  `static List<String> caseNames(Scale scale)`; `static boolean carries(Protocol p, Shape s)`.
  `void LatencyRecorder.addAll(LatencyRecorder other)`.

Decisions:
- Dataset (database `bench_protocols`): `User {id, name}` with `FriendOf` edges and `Vec {id, embedding}` (768 floats,
  15 rows looked up by the vector parameter shape); sizes per scale chosen by the builder, at least 1,000 users at XS so the
  page shape returns 1,000 rows.
- Shapes return property columns only (never nodes or relationships), with `ORDER BY` where the shape has an order:
  point `MATCH (u:User {id: $id}) RETURN u.id, u.name`; hop1 `... -[:FriendOf]->(f) RETURN f.id ORDER BY f.id`; page
  `MATCH (u:User) WHERE u.id >= $from RETURN u.id, u.name ORDER BY u.id LIMIT 1000`; merge500 `UNWIND $rows AS r MERGE
  (u:User {id: r.id}) SET u.name = r.name` on ids above the dataset range, reset by the case teardown; vector-param `UNWIND
  $vectors AS v RETURN size(v)` with 15 vectors of 768 floats; tx10: 10 point MERGEs in one transaction.
- `carries` answers false for (POSTGRES, VECTOR_PARAM) and (POSTGRES, UNWIND_MERGE_500) unless the builder shows a list
  parameter binds over JDBC with `{cypher}`; HTTP carries TX_10 through `RemoteDatabase.begin/commit`. An absent case is
  absent from `caseNames`, never run against a substitute statement.
- **Parity** in each non-HTTP case's `prepare()`: for a fixed set of 20 ids, the read shapes' rows on this protocol vs on
  HTTP (`Parity.diff`); a difference fails the case with the diff as its error. Write shapes check the written rows by an
  HTTP read afterwards.
- 16 clients: 16 threads, each with its own `ProtocolClient` and `LatencyRecorder`, merged with `addAll` after join.
  1,000 operations per rep in total at S (spread over the clients). A case that breaks the 30 min `protocols` budget at S is
  dropped at S in `caseNames(S)` (heaviest first: merge500-1c, tx10-1c, page1000-1c), and the PR body lists which and why.

- [ ] **Step 1: Failing tests:** `LatencyRecorderTest.addAllKeepsEverySample` (two recorders of 3 and 4 samples -> 7,
  p100 is the max of both); `ProtocolsWorkloadTest.caseNamesFollowTheSpecFormat` (every name matches
  `^(point|hop1|page1000|merge500|vector-param|tx10)-(http|grpc|bolt|pg)-(1|16)c$`);
  `casesOmitWhatAProtocolCannotCarry` (no name for a (protocol, shape) where `carries` is false);
  `httpCarriesEveryShape`; `serverSettingsEnableTheFourProtocols`; `sixteenClientsRecordEveryOperation` (the static
  driver `static LatencyRecorder runClients(int clients, int ops, IntConsumer op)` with a counting op: 1,000 samples, 1,000
  calls).
- [ ] **Step 2: Run**; fail.
- [ ] **Step 3: Implement**; register after `correctness`.
- [ ] **Step 4: Run** unit tests; pass. `ProtocolsWorkloadIT` at XS on the JVM image: every case `ok`, and one injected
  mismatch (a test-only hook that changes one HTTP row) fails exactly the non-HTTP cases of that shape. Run once.
- [ ] **Step 5: Commit** `feat(bench): wire protocol workload - the same openCypher shapes over HTTP, gRPC, Bolt and PostgreSQL with row parity (#12433)`.

### Task 7: Fit check and PRs (once per PR A, B, C, D)

- [ ] **Step 1:** Push the branch, open the PR (`Part of #12433`; this plan committed in PR A). Handle the review surfaces
  (`pr-review-surfaces` skill).
- [ ] **Step 2:** Dispatch the nightly workflow with `commit=<branch head sha>`, `workloads=<name>`, `scale=S`, then again
  with `scale=M`. From the raw files: every case `ok`, each S timed case at least 5 s, each S latency case at least 1,000
  operations, the workload inside its S budget per variant, every M case inside the 120 min cap. Put the numbers in the PR
  body.
- [ ] **Step 3:** If a case breaks a rule, fix the size or leave it out at S (`cases(Scale)`), push, dispatch again.
  Merge stays with the user.
- [ ] **Step 4:** After the last PR is merged, the first scheduled weeknight run must end inside 240 min; if not, move the
  slowest cases to Saturday only and update the spec with the measured reason. Tick the time series and wire protocol
  items of #12433.
