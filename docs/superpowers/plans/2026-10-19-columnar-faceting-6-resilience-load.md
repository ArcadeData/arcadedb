# Columnar Faceting - HA Resilience and Load Implementation Plan (plan 6 of 6)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** prove under faults and under load that facets computed from the node-local columnar projection equal `GROUP BY` on every node once the cluster converges, that non-exact answers always carry their lag, that a restarted node keeps its projection and that a new replica rebuilds it in the background.

**Architecture:** one shared, container-free facet toolkit in the `load-tests` test-jar (schema and statements, seeded asset generator, response parser, Prometheus scrape parser, a `FacetDatabaseWrapper`), consumed by three users: the chaos harness (a new `FacetWorkload` running beside the ledger `Workload`, and a `FacetCheck` invoked at the existing `Checkpoint` convergence point, reporting `FACET_*` violations through `Violation`/`ChaosReport`), five `e2e-ha` scenario ITs (one new facet test method per class, existing methods untouched), and two load ITs modeled on the time-series ones. The facet data lives in its own type `FacetAsset`, so updates and deletes never touch the insert-only `ChaosOp` ledger and its invariants I1-I5 stay as they are.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Testcontainers + Toxiproxy, Awaitility, Micrometer (client side) and the server's `/prometheus` endpoint (server side), `com.arcadedb.serializer.json.JSONObject`/`JSONArray`, `RemoteDatabase`/`RemoteGrpcDatabase` (plan 4 adds `ResultSet.getFacets()`).

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 6.5, 6.6, 7.3, 7.8, 8.3, 9, 10). **Roadmap and cross-plan contract:** `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md`. Assumes plans 3 and 4 are merged: `CREATE INDEX ... COLUMNAR`, `SELECT ... FACETS ...`, HTTP `facets.meta = {exact, patched, lagMs}`, `FacetResult.source()` in `index|scan|unavailable`, metrics under `arcadedb.columnar.*`. Plans 3-5 are delivered in this branch (plan 5 adds GROUP BY acceleration, ON by default, which Task 2's truth queries must avoid). Task 0 (lag floor in `FacetExecutor`) was the only planned engine change; the reviews added three more: Task 8 (`SnapshotInstaller.acquireNewDatabase` marks a new replica's snapshot for a `snapshot_install` rebuild), Task 12 (`FacetPatcher.recordAt` maps a broken chunk chain to a retryable `ConcurrentModificationException`, see #10941) and the final fix batch (merge starvation in `MergePolicy`/`ColumnarIndexManager`, the first failed page read logged at FINE, inline publish of an added column on an empty bucket, fallback scans tagged with their reason). Everything else lives in `load-tests` and `e2e-ha`. The outcome, open follow-ups and the baseline numbers are in the roadmap's "Plan 6 outcome".

## Global Constraints

- **No git commits.** The user commits after review. Where the plan template says "Commit", the step is "Stop: changes ready for review", listing the files touched.
- No em dash characters in code comments, docs or messages.
- Java style of the repo: `final` on variables and parameters where possible; single-statement `if` without braces; imports, never fully qualified names; no `System.out` left behind; no Claude author tags.
- JSON via `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, using getters with defaults.
- Primitive arrays over collections of boxed values on every hot path.
- Tests: AssertJ style `assertThat(x).isTrue();`. Slow tests `@Tag("slow")`, benchmarks `@Tag("benchmark")`. Never assert raw wall-clock time; use `com.arcadedb.utility.StallAwareStopwatch`.
- Test servers only through fixture bases (`BaseGraphServerTest`, `StaticBaseServerTest`, `BaseRaftHATest`); URLs from bound ports; extra ports from `allocateFreePorts` / `allocateFixturePorts`.
- Module test runs use `-am` (`mvn -o -pl engine -am test -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`), and `verify` when gremlin or graphql are in the reactor. Skip lanes with `-DexcludedGroups`, never `-Dtest='!Foo'`.
- No `ForkJoinPool.commonPool()`; background work on a dedicated pool following `.claude/skills/engine-concurrency/SKILL.md`.
- No new dependency unless a plan says so; RoaringBitmap only after plan 1 measures a need (Apache 2.0, update `ATTRIBUTIONS.md` and `NOTICE`).
- After every Java change: compile the touched modules and fix all errors before the next step.

Plan-6 specifics (derived from the modules this plan touches):

- Container tests reach servers only through `ContainersTestTemplate` (`load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplate.java`) and the mapped ports it reads back (`ServerWrapper(container)`); Toxiproxy ports stay inside the Docker network as the existing ITs do. No new host port constants.
- No Mockito: fakes in the style of `FakeNodeControl`, `FakeLoad`, `LedgerNodeReader` and `CheckpointTest.ScriptedNodeReader`; HTTP fakes with `com.sun.net.httpserver.HttpServer` as in `WorkloadTest`.
- Load and scenario ITs assert only stable invariants: equality after quiesce, zero errors, `exact` after quiesce, metric counts that are zero or non-zero by construction. Latency, lag and throughput are logged, never asserted.
- CI lanes: chaos stays `@Tag("chaos")` (excluded by `e2e-ha/pom.xml` `failsafe.excludedGroups`, run by `.github/workflows/ha-chaos-tests.yml`); new `e2e-ha` scenario methods are untagged (nightly `ha-resilience-tests.yml`); new load ITs are untagged (nightly `load-tests.yml`). Unit tests (`*Test.java`) go in `e2e-ha`, which the main unit lane runs (`mvn-test.yml:249` excludes only `e2e` and `load-tests`). The nightly `ha-resilience-tests.yml` and `load-tests.yml` also run against `latest-native` (built from main): the new methods run natively once the branch is merged; no JVM-only option may carry a test's premise.
- Server settings in scenario ITs are passed as environment variables named after the key (`.withEnv("arcadedb.ha.snapshotThreshold", "200")`), never inside `ARCADEDB_OPTS_MEMORY`: `ContainersTestTemplate.configureServer` already owns that variable and the native image ignores it, while `GlobalConfiguration` reads a setting from the environment variable named after it on both images.
- Module test runs with `-am` filter by package (`-Dtest='com/arcadedb/containers/ha/**/...'`) or by a class name unique in the reactor, so upstream modules (24 `Facet*Test` classes in engine/server/grpc) run nothing.

## Review Focus

1. A `fileSize` exactly on a range boundary (1024, 1048576): half-open intervals put it in the upper bucket; the `GROUP BY` side must use `>= lo AND < hi`. Pinned in Task 2 (`boundaryValuesLandInTheUpperRange`).
2. A record whose `brand` was removed (`UPDATE ... REMOVE brand`): counted in `brand.missing`, never as a `null` value bucket. Pinned in Task 1 (`absentMissingDefaultsToZeroAndMissingBucketIsNotCountedTwice`) and Task 2 (`removedBrandCountsAsMissing`).
3. A record grown into a placeholder (or chunks) and then updated again, which rewrites only the content page: the new value must reach the facets on every node. Pinned in Task 2 (`grownRecordUpdatedAgainIsCounted`) and exercised by the chaos `UPDATE_GROWN` op (Task 6).
4. A delete followed by an insert that reuses the freed slot before the apply thread runs: the new record counts once, the old one not at all. Pinned in Task 2 (`deleteThenInsertReusingTheSlotCountsOnce`).
5. A late Raft commit landing between the `GROUP BY` read and the facet read at the checkpoint: must be retried, not reported as drift. Pinned in Task 4 (`lateCommitBetweenReadsIsRetriedNotReported`).

---

## File Structure

| File | Responsibility |
|---|---|
| `engine/src/main/java/com/arcadedb/query/facet/FacetExecutor.java:137-138` (modify, Task 0) | `lagMs >= 1` whenever a bucket's snapshot has a pending epoch |
| `engine/src/test/java/com/arcadedb/query/facet/FacetPatchingTest.java` (modify, Task 0) | `inexactAnswerAlwaysReportsLag` |
| `load-tests/src/test/java/com/arcadedb/test/support/FacetSchema.java` (new) | `FacetAsset` DDL, every write statement, the `FACETS` query, the equivalent `GROUP BY` queries and their normalization into one count map |
| `load-tests/src/test/java/com/arcadedb/test/support/AssetGenerator.java` (new) | Seeded generator of asset field values (dependency-free variant of plan 3's `ColumnarTestData`) |
| `load-tests/src/test/java/com/arcadedb/test/support/FacetSnapshot.java` (new) | Parsed facet response: meta plus normalized counts; diff helper |
| `load-tests/src/test/java/com/arcadedb/test/support/PrometheusScrape.java` (new) | Sums Prometheus text-format samples by name and label |
| `load-tests/src/test/java/com/arcadedb/test/support/FacetDatabaseWrapper.java` (new) | Per-node client: schema, mixed writes on owned keys, facet queries with client metrics, stable comparisons, metric scrape |
| `load-tests/src/test/java/com/arcadedb/test/support/DatabaseWrapper.java:108-153` (modify) | Extract `createDatabase` body into `static void createDatabase(ServerWrapper)` for reuse |
| `load-tests/src/test/java/com/arcadedb/test/load/SingleServerFacetLoadTestIT.java` (new) | Single-server facet load |
| `load-tests/src/test/java/com/arcadedb/test/load/ThreeNodesFacetLoadTestIT.java` (new) | Three-node facet load, follower lag |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/facet/{FacetSnapshotTest,PrometheusScrapeTest,FacetSchemaTest}.java` (new) | Unit tests of the shared toolkit (run in the main unit lane) |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetReader.java` (new) | Interface: per-node `GROUP BY` counts and facet snapshot |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/HttpFacetReader.java` (new) | `FacetReader` over each node's `/api/v1/query` |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetCheck.java` (new) | `FACET_DIVERGENCE`, `FACET_DRIFT`, `FACET_META`, `FACET_STALL`, `FACET_ERROR`, `FACET_SCAN` |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FakeFacetReader.java` (new) | Scripted fake shared by three test classes |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetWorkload.java` (new) | Facet writers and queries on `FacetAsset` |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/CombinedLoad.java` (new) | Fans `LoadGenerator` calls out to the ledger workload and the facet workload |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{Checkpoint,ChaosConfig,HaChaosIT}.java` (modify) | Wire the facet check, the facet writers and the config knob |
| `e2e-ha/src/test/java/com/arcadedb/containers/ha/{LeaderFailoverIT,RollingRestartIT,NetworkPartitionRecoveryIT,SplitBrainIT,PacketLossIT}.java` (modify) | One facet test method each |

## Shared names (every task uses these)

```java
// com.arcadedb.test.support (load-tests test-jar)
public final class FacetSchema {
  public static final String TYPE = "FacetAsset";
  public static final List<String> DDL;                 // see Task 2
  public static final List<String> CHECKED_FILTERS = List.of("", "fileSize >= 1024");   // "" = no WHERE
  public static final long SMALL_MAX = 1024L, MEDIUM_MAX = 1_048_576L;                   // range boundaries
  public static final String INSERT, INSERT_NO_BRAND, UPDATE_VALUES, REMOVE_BRAND, GROW, UPDATE_GROWN, DELETE, LINK;
  public static String facetQuery(String where);
  public static SortedMap<String, Long> expectedCounts(Rows rows, String where) throws IOException;
  @FunctionalInterface public interface Rows { JSONArray query(String sql) throws IOException; }
}
public record FacetSnapshot(boolean exact, long patched, long lagMs, Map<String, String> sources, SortedMap<String, Long> counts) {
  public static FacetSnapshot parse(JSONObject response);    // response = whole HTTP body: {"result":[..],"facets":{..}}
  public static FacetSnapshot of(FacetResults results);      // via results.toJSON()
  public boolean settled();          // lagMs == 0 and every source is "index"
  public boolean comparable();       // exact and no source is "unavailable"
  public boolean metaConsistent();   // exact || lagMs > 0 (needs Task 0's lag floor)
  public static List<String> diff(Map<String, Long> expected, Map<String, Long> actual, int limit);
}
```

Approximate DISTINCT is out of scope: plan 6 queries request none (`FacetResults.exact()` is false for one with `lagMs` 0 by design).

Count-map key format (both sides, so maps compare with `equals`): terms bucket `alias=value`, range bucket `alias=key`, `alias.missing` (terms and range facets), `alias.other`. Entries with value 0 are dropped on both sides. Aliases: `brand`, `tags`, `size`.

## Cross-plan contract this plan relies on (recorded in the roadmap; names as plans 3 and 4 define them)

1. **Plan 3 adds a counter `arcadedb.columnar.rebuilds`** (plan 3 Task 18), tags `database`, `type`, `reason` in `create | snapshot_install | missing | corrupt | unclean_no_wal | unclean_pages | schema | check_fix` (`corrupt` = a projection file failed its CRC or format check, distinct from `missing`; `unclean_pages`: after an unclean open, more than 1024 pages and a quarter of a bucket changed since its segments), counted when a bucket projection's rebuild starts, never for a `create` on an empty bucket. After an unclean open (SIGKILL) a projection is unusable until its page check ends, so facets read `scan` for a while: expected, not a rebuild. Series exist from the first 5 s refresh of `ColumnarMetrics`; an absent series reads 0. A restart assertion without a label filter therefore also catches a `corrupt` rebuild, which is intended. Normal open plus recovery (manifest load, persisted dirty set, WAL-replay marks, spec 6.5) never increments it. None of the listed plan-3 metrics (`apply.lag.ms`, `apply.pages`, ...) can tell "recovered" from "rebuilt" after a process restart, because counters restart with the process. Optional, logged only: `arcadedb.columnar.recovery.pages` (pages re-read at open).
2. **Plans 3 and 4 register their meters on `Metrics.globalRegistry`** with a `database` tag, so the `PrometheusMetricsPlugin` endpoint `GET /prometheus` exposes them. Names used here and their owners: plan 3 Task 18 counter `arcadedb.columnar.rebuilds` (`arcadedb_columnar_rebuilds_total`), gauges `arcadedb.columnar.segments` (`arcadedb_columnar_segments`) and `arcadedb.columnar.apply.lag.ms` (`arcadedb_columnar_apply_lag_ms`); plan 4 Task 13 function counters `arcadedb.columnar.query.budget.waits`, `.query.patch.overflows`, `.query.fallback.scans`, `.query.patched` (`arcadedb_columnar_query_budget_waits_total`, `arcadedb_columnar_query_patch_overflows_total`, `arcadedb_columnar_query_fallback_scans_total`, `arcadedb_columnar_query_patched_total`). Since the plan 6 final fix batch the fallback-scan counter is one series per `reason` tag; a scrape without a label filter sums them.
3. **Plan 4 response shape is pinned**: `FacetResults.toJSON()` is exactly the HTTP `facets` object; terms facets always carry `missing` and `otherCount` (0 allowed); range buckets carry `key`; a facet with `source: "unavailable"` carries no buckets. `FacetResult.source()` is per facet.
4. **Plan 3: the projection directory is excluded from Raft snapshots** (node-local, spec 6.6), so a snapshot install always leads to a `snapshot_install` rebuild.
5. **Fixtures**: the roadmap's "Shared test fixtures" gives plan 6 its own `AssetGenerator` in the `load-tests` test-jar, which `e2e-ha` already depends on (`e2e-ha/pom.xml`), so one copy serves both. Plan 3's `FacetOracle` / `ColumnarTestData` (engine test-jar) are not used: the container tests compare against `GROUP BY` through `FacetSchema.expectedCounts` over HTTP.

## Carried from plans 3-5

Open items of plans 3-5 that land in this plan, each with its owner (preflight section 4).

| # | Item (source) | Owner in this plan |
|---|---|---|
| O1 | Divergence with a rebuild concurrent with vertex/edge writes (plan 3 Task 23; engine stress test exists) | HA variant: Task 7 (chaos, snapshot installs under writes) and Task 8 (writes during the rejoiner's rebuild). External bodies stay out of `FacetAsset`: carry to the final review |
| O2 | HA ITs must await every node before writing (plan 3 final review C M9) | Task 3 `awaitSchema` (settled), Task 7 setup, Tasks 8, 11, 12 |
| O3 | Callers must pass a timeout to `awaitCaughtUp` (plan 3 Task 16) | N/A over HTTP; every plan 6 wait is bounded (Task 3) |
| O4 | Insert overhead +6-10% with five COLUMNAR indexes (roadmap Tuning phase, #10662) | Tasks 11, 12 durable numbers |
| O5 | Overlay size under load (plan 3 Task 16 review) | Tasks 11, 12 record `arcadedb_columnar_overlay_rows` |
| O6 | External-bucket pages not covered by the unclean-open page check (plan 3 final fix batch 2) | Not absorbed (`FacetAsset` has no EXTERNAL body): carry to the final review |
| O7 | Counter arrays not pooled, revisit with plan 6 numbers (roadmap Plan 4 additions) | Tasks 11, 12 record `query_budget_waits_total` and query latency |
| O8 | Page-granular pending: broad queries patch most matches under write load (plan 4, "with plan 6 acceptance") | Tasks 11, 12 record `patched` percentiles; Task 4 FACET_STALL text |
| O9 | `exact=false` with `lagMs=0` (plan 4 final review C-M5) | Task 0 |
| O10 | Fallback-scan cap 2/db during rebuilds, capped facets read `unavailable` (plan 4 final review C M9) | Tasks 3, 4 treat `unavailable` as not comparable; Task 8 logs the source sequence |
| O11 | Unclosed result sets leak budget (plan 4 C-I2, fixed by a `Cleaner`) | Task 3: close every ResultSet anyway (try-with-resources) |
| O12 | Fallback scans ignore the query deadline (plan 4 C-I3, fixed) | none |
| O13 | `BaseRaftHATest.awaitAppliedIndex` budget flake (plan 4 final batch 2, fixed) | none; any in-process HA test added here uses `BaseRaftHATest`/`ColumnarHATestSupport` and `allocateFixturePorts` |
| O14 | Approximate DISTINCT makes `exact` false at lag 0 (plan 4 final review C, by design) | Shared names note: plan 6 requests no DISTINCT |
| O15 | One DB-wide `queuedColumnarMarks` counter may delay WAL drops; possible double fsync per manifest publish (plan 5 main-merge review) | Task 12 logs each node's WAL file count and size at the end |
| O16 | Facet values typed per transport (plan 4 C-I8, fixed) | Task 11 runs HTTP and gRPC |
| O17 | Hierarchy workload (roadmap row 6, "5 optional") | Not absorbed: spec 10 does not require HA coverage of HIERARCHY/DISTINCT/STATS; record as a follow-up |
| O18 | Pre-existing flake `Issue8859SaveElementStepIndexDdlTest` on main (plan 5 final gate, unfiled) | none (excluded by the package-filtered globs) |

---

### Task 0: Lag floor in `FacetExecutor` (engine)

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetExecutor.java:137-138`
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetPatchingTest.java` (add one test)

**Interfaces:**
- Consumes: plan 4 `FacetExecutor` lag computation (`now - snapshots[i].oldestPendingEpochStartMs()`, ms resolution, no floor), `planPatches`.
- Produces: a bucket whose snapshot has pending positions reports `lagMs >= 1` (spec 8.3: `lagMs` is the age of the oldest undrained epoch, which is never 0 ms old in meaning). FACET_META (Task 4) is LIVENESS for `exact=false` with `lagMs=0` (controller ruling, Task 0).

Decision: `lagMs = Math.max(lagMs, Math.max(1L, now - snapshots[i].oldestPendingEpochStartMs()))`. Controller ruling on the preflight open question: the uncovered-position safety net (`planPatches` refusing a bucket whose snapshot reports no pending epoch) does NOT set a lag: `lagMs` keeps the spec 8.3 meaning (age of the oldest undrained epoch, 0 when none). FACET_META therefore classifies `exact=false` with `lagMs=0` as a LIVENESS warning (logged with the sample), not SAFETY; SAFETY stays with FACET_DIVERGENCE/FACET_DRIFT (wrong counts).

- [ ] **Step 1: Write the failing test** (as implemented, controller ruling: deterministic, no spin on `currentTimeMillis`): extract the lag computation into `static long lagOf(long nowMs, long oldestPendingEpochStartMs)` and test it directly, `lagOfAPendingEpochIsAtLeastOneMillisecond` (`lagOf(1_000, 1_000) == 1`, `lagOf(1_001, 1_000) == 1`, `lagOf(1_250, 1_000) == 250`); the integration side is `overBudgetIsInexactAndFlagged` (apply suspended, `patchBudget` 5 and `patchBudgetRatio` 0, so the answer is inexact and must carry `lagMs >= 1`). A first probabilistic version (`inexactAnswerAlwaysReportsLag`, 100 rounds, ~11 s) was replaced and later removed.
- [ ] **Step 2: Run it to verify it fails**: `./mvnw -o -pl engine -am test -Dtest='FacetPatchingTest,FacetExecutor*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: `lagOfAPendingEpochIsAtLeastOneMillisecond` FAIL (`lagMs` 0) against a stub without the floor.
- [ ] **Step 3: Implement the floor.**
- [ ] **Step 4: Run the command of Step 2.** Expected: all PASS.
- [ ] **Step 5: Stop: changes ready for review** - `FacetExecutor.java`, `FacetPatchingTest.java`.

---

### Task 1: Response and metric parsers

**Files:**
- Create: `load-tests/src/test/java/com/arcadedb/test/support/FacetSnapshot.java`
- Create: `load-tests/src/test/java/com/arcadedb/test/support/PrometheusScrape.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/facet/FacetSnapshotTest.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/facet/PrometheusScrapeTest.java`

**Interfaces:**
- Consumes: plan 4 `com.arcadedb.query.facet.FacetResults#toJSON()`.
- Produces: `FacetSnapshot` (see Shared names); `public final class PrometheusScrape { public static double sum(String body, String name); public static double sum(String body, String name, String label, String value); }` (0 when absent). Every shared toolkit type in `load-tests` is public with public members: the consumers are in other packages and another module.

- [ ] **Step 1: Write the failing tests**

```java
// FacetSnapshotTest
private static final String SPEC_RESPONSE = """
    {"result":[{"@rid":"#12:4051"}],"facets":{"meta":{"exact":true,"patched":12,"lagMs":340},
     "brand":{"source":"index","buckets":[{"value":"Nike","count":1240},{"value":"Zed","count":0}],"otherCount":310,"missing":4},
     "size":{"source":"index","buckets":[{"key":"small","from":0,"to":1048576,"count":830}]}}}""";

@Test void parsesMetaAndNormalizedCounts() {
  final FacetSnapshot s = FacetSnapshot.parse(new JSONObject(SPEC_RESPONSE));
  assertThat(s.exact()).isTrue();
  assertThat(s.patched()).isEqualTo(12);
  assertThat(s.lagMs()).isEqualTo(340);
  assertThat(s.sources()).containsEntry("brand", "index").containsEntry("size", "index");
  assertThat(s.counts()).containsExactly(entry("brand.missing", 4L), entry("brand.other", 310L),
      entry("brand=Nike", 1240L), entry("size=small", 830L));            // zero bucket dropped, sorted keys
}
@Test void absentMissingDefaultsToZeroAndMissingBucketIsNotCountedTwice() { /* bucket {"value":null,"missing":true,"count":3} plus result "missing":3 -> brand.missing == 3; no "missing" member -> no entry */ }
@Test void unavailableFacetIsNotComparable() { /* "source":"unavailable", no buckets -> comparable() false, settled() false */ }
@Test void metaConsistency() {   // exact=false needs lagMs>0
  assertThat(snapshot(false, 0).metaConsistent()).isFalse();
  assertThat(snapshot(false, 5).metaConsistent()).isTrue();
  assertThat(snapshot(true, 0).metaConsistent()).isTrue();
}
@Test void settledNeedsLagZeroAndIndexEverywhere() { /* lag 0 + scan -> false; lag 0 + index -> true; lag 3 + index -> false */ }
@Test void diffListsMissingExtraAndDifferentKeys() {
  assertThat(FacetSnapshot.diff(Map.of("brand=A", 2L, "brand=B", 1L), Map.of("brand=A", 3L, "tags=x", 1L), 10))
      .containsExactly("brand=A expected=2 actual=3", "brand=B expected=1 actual=0", "tags=x expected=0 actual=1");
}

// PrometheusScrapeTest
private static final String BODY = """
    # HELP arcadedb_columnar_rebuilds_total
    # TYPE arcadedb_columnar_rebuilds_total counter
    arcadedb_columnar_rebuilds_total{database="chaos",reason="create",type="FacetAsset",} 1.0
    arcadedb_columnar_rebuilds_total{database="chaos",reason="snapshot_install",type="FacetAsset",} 3.0
    arcadedb_columnar_rebuilds_total_extra 9.0
    arcadedb_columnar_segments{database="chaos",} 4.0
    """;
@Test void sumsEverySeriesOfExactlyThatName() { assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_rebuilds_total")).isEqualTo(4.0); }
@Test void filtersByLabel() { assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_rebuilds_total", "reason", "snapshot_install")).isEqualTo(3.0); }
@Test void absentNameIsZero() { assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_nope")).isZero(); }
@Test void unlabelledSample() { assertThat(PrometheusScrape.sum("x_total 2.5\n", "x_total")).isEqualTo(2.5); }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest='FacetSnapshotTest,PrometheusScrapeTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `FacetSnapshot` / `PrometheusScrape` not found. (`verify`, not `test`: `e2e-ha` consumes the `load-tests` test-jar, which only the package phase builds.)

- [ ] **Step 3: Implement `FacetSnapshot` and `PrometheusScrape`**

`parse`: read `facets` (empty object when absent, then `exact=false`, `lagMs=0`); every key except `meta` is an alias; buckets keyed by `value` (terms) or `key` (range/histogram), a bucket with `"missing": true` is skipped (the result's `missing` member already counts those records; `FacetBucket.toJSON` writes both); `otherCount` to `alias.other`; `missing` to `alias.missing` (terms and range results); drop zeros; `TreeMap`. `of(results)` = `parse(new JSONObject().put("facets", results.toJSON()))`. `diff` walks the sorted union of keys, absent = 0, stops at `limit`. `PrometheusScrape`: per line, skip `#` and blank; name ends at the first `{` or space; label match on the literal `label="value"` substring inside the braces; value is the last whitespace token parsed with `Double.parseDouble`.

- [ ] **Step 4: Run tests to verify they pass**

Run: the command of Step 2. Expected: `Tests run: 10, Failures: 0, Errors: 0`.

- [ ] **Step 5: Stop: changes ready for review** - `FacetSnapshot.java`, `PrometheusScrape.java`, `FacetSnapshotTest.java`, `PrometheusScrapeTest.java`.

---

### Task 2: Facet schema, statements and generator, proven on an embedded database

**Files:**
- Create: `load-tests/src/test/java/com/arcadedb/test/support/FacetSchema.java`
- Create: `load-tests/src/test/java/com/arcadedb/test/support/AssetGenerator.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/facet/FacetSchemaTest.java` (embedded `DatabaseFactory`, same pattern as `chaos/ChaosSchemaTest.java:49-63`)

**Interfaces:**
- Consumes: Task 1 `FacetSnapshot.of`, plan 4 `ResultSet.getFacets()`.
- Produces: `FacetSchema` (Shared names); `public final class AssetGenerator { public AssetGenerator(long seed); public Map<String,Object> insertParams(long aid); public boolean omitBrand(); public Map<String,Object> valuesParams(long aid); public Map<String,Object> growParams(long aid); public Map<String,Object> keyParams(long aid); public Map<String,Object> linkParams(long from, long to); public static final List<String> BRANDS /* 12 */, TAGS /* 64 */; }`. Not thread-safe: one instance per writer thread.

Decisions:
- DDL (all `IF NOT EXISTS`): `CREATE VERTEX TYPE FacetAsset`, `CREATE EDGE TYPE FacetLink`, properties `aid LONG`, `brand STRING`, `tags LIST OF STRING`, `fileSize LONG`, `CREATE INDEX ON FacetAsset (aid) UNIQUE`, `CREATE INDEX ON FacetAsset (brand) COLUMNAR`, `CREATE INDEX ON FacetAsset (tags BY ITEM) COLUMNAR`, `CREATE INDEX ON FacetAsset (fileSize) COLUMNAR`. Vertex type on purpose: `LINK` rewrites the vertex head pointer without changing faceted values (spec 6.3 no-op skip).
- Statements (named parameters): `INSERT` sets `aid, brand, tags, fileSize, body`; `INSERT_NO_BRAND` omits `brand` (absent, never null); `UPDATE_VALUES` sets `brand, tags, fileSize WHERE aid = :aid`; `REMOVE_BRAND` = `UPDATE FacetAsset REMOVE brand WHERE aid = :aid`; `GROW` sets `body, brand`; `UPDATE_GROWN` sets `brand, fileSize` (content page only once grown); `DELETE` = `DELETE FROM FacetAsset WHERE aid = :aid`; `LINK` = `CREATE EDGE FacetLink FROM (SELECT FROM FacetAsset WHERE aid = :from) TO (SELECT FROM FacetAsset WHERE aid = :to)`.
- `facetQuery(where)` = `SELECT @rid FROM FacetAsset[ WHERE <where>] FACETS brand TERMS TOP 1000, tags TERMS TOP 1000, size: fileSize RANGES ([0, 1024) AS 'small', [1024, 1048576) AS 'medium', [1048576, *) AS 'large') LIMIT 1`. `TOP 1000` equals `COLUMNAR_TOP_MAX` and exceeds both cardinalities, so `other` is 0 by construction.
- `expectedCounts(rows, where)` runs: `SELECT v, count(*) AS c FROM (SELECT brand AS v FROM FacetAsset[ WHERE ..]) GROUP BY v` (null group to `brand.missing`); `SELECT tag AS v, count(*) AS c FROM (SELECT tags AS tag FROM FacetAsset[ WHERE ..] UNWIND tag) GROUP BY tag`; three `SELECT count(*) AS c FROM FacetAsset WHERE [<where> AND ]fileSize >= lo AND fileSize < hi` (the last without upper bound); and `SELECT count(*) AS c FROM FacetAsset WHERE [<where> AND ]fileSize IS NULL` to `size.missing` (0 today, dropped). Every truth query reads from a subquery or carries a WHERE, so GROUP BY acceleration (plan 5, which takes only a whereless GROUP BY over a schema type) never answers it: the truth reads the records, as `FacetOracle.normalPlan` does embedded. BY ITEM counts a record once per distinct item; the generator keeps tags distinct and non-empty, so the UNWIND truth agrees.
- Generator: tags 1-4 distinct values (never empty, so `tags.missing` is 0 on both sides); `omitBrand()` true 5%; `fileSize` drawn 10% from `{1023, 1024, 1048575, 1048576}` and otherwise log-uniform in `[0, 8 MB)`; `body` 200 B on insert; `growParams` body size drawn from `{2 KB, 20 KB, 70 KB}` (70 KB exceeds a 64 KB page, forcing chunks; 20 KB outgrows a full page, forcing a placeholder).

- [ ] **Step 1: Write the failing tests**

```java
// helper in the test: compare facets with GROUP BY on the embedded db, waiting for lag 0 too
private void assertFacetsEqualGroupBy() throws Exception {
  for (final String where : FacetSchema.CHECKED_FILTERS) {
    final SortedMap<String, Long> expected = FacetSchema.expectedCounts(this::rows, where);
    assertThat(facets(where).counts()).as("patched, where='%s'", where).isEqualTo(expected);
    await().atMost(Duration.ofSeconds(30)).until(() -> facets(where).settled());
    assertThat(facets(where).counts()).as("lag 0, where='%s'", where).isEqualTo(expected);
  }
}
private FacetSnapshot facets(final String where) { try (ResultSet rs = database.query("sql", FacetSchema.facetQuery(where))) { return FacetSnapshot.of(rs.getFacets()); } }
private JSONArray rows(final String sql) { /* query, each Result.toJSON() into a JSONArray */ }

@Test void ddlIsIdempotent()                      // run DDL twice; 3 COLUMNAR indexes + 1 UNIQUE on FacetAsset
@Test void everyStatementRuns()                   // each statement once with generator params; no exception
@Test void insertedAssetsMatch()                  // 500 inserts, assertFacetsEqualGroupBy()
@Test void boundaryValuesLandInTheUpperRange()    // fileSize 1023,1024,1048575,1048576 -> size=small 1, medium 2, large 1
@Test void removedBrandCountsAsMissing()          // 10 inserts, REMOVE_BRAND on 3 -> brand.missing == 3, no "brand=null" key
@Test void grownRecordUpdatedAgainIsCounted()     // insert 50, GROW all (each size), UPDATE_GROWN all to brand BRANDS.get(0) -> brand=<b0> == 50
@Test void deleteThenInsertReusingTheSlotCountsOnce()  // insert 100, delete 50, insert 50 new keys before awaiting -> totals 100
@Test void linkingVerticesChangesNoCounts()       // facets before == after 100 LINK ops
@Test void mixedRandomOperationsMatch()           // 2000 seeded random ops over all statements, assertFacetsEqualGroupBy()
@Test void truthNeverReadsTheProjection()         // after mixedRandomOperationsMatch's data, run every expectedCounts query, then
                                                  // ((LocalDatabase) database).getColumnarIndexManager().getStats().groupByAcceleratedCount() is 0;
                                                  // run SELECT brand AS v, count(*) AS c FROM FacetAsset GROUP BY brand once -> 1 (the counter can move)
@Test void readOnlyReopenComparesEqual()          // 500-insert data in two embedded databases with the same statements, close both,
                                                  // reopen both ComponentFile.MODE.READ_ONLY, new DatabaseComparator().compare(a, b) throws nothing
                                                  // (proves HaChaosIT's end-of-run compareAllDatabases works over COLUMNAR indexes)
```

Test setup: set `GlobalConfiguration.TYPE_DEFAULT_BUCKETS` to 10 on the database configuration before running `FacetSchema.DDL` (`ContainersTestTemplate.baseSettings` uses 10), so the embedded proof goes through `CrossBucketOrdinalMap` as the containers do.

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest=FacetSchemaTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `FacetSchema` not found.

- [ ] **Step 3: Implement `FacetSchema` and `AssetGenerator`** with the values above. Validate a `where` against `[A-Za-z0-9_ >=<.']*` in `facetQuery`/`expectedCounts` (constants only, but a typo must fail loudly, as `ChaosSchema.recordPage` does for RIDs).

- [ ] **Step 4: Run tests to verify they pass**

Run: the command of Step 2. Expected: `Tests run: 11, Failures: 0`. A failure of `grownRecordUpdatedAgainIsCounted` or `deleteThenInsertReusingTheSlotCountsOnce` is a plan 3 defect (placeholder reverse map, slot reuse): report it, do not loosen the test.

- [ ] **Step 5: Stop: changes ready for review** - `FacetSchema.java`, `AssetGenerator.java`, `FacetSchemaTest.java`.

---

### Task 3: `FacetDatabaseWrapper`

**Files:**
- Create: `load-tests/src/test/java/com/arcadedb/test/support/FacetDatabaseWrapper.java`
- Modify: `load-tests/src/test/java/com/arcadedb/test/support/DatabaseWrapper.java:108-153` (body moves to `public static void createDatabase(final ServerWrapper server)`; the instance method delegates)

**Interfaces:**
- Consumes: Tasks 1-2; `DatabaseWrapper.Protocol {HTTP, GRPC}`; `ContainersTestTemplate.DATABASE`, `PASSWORD`.
- Produces:
```java
public class FacetDatabaseWrapper implements AutoCloseable {
  public FacetDatabaseWrapper(ServerWrapper server, DatabaseWrapper.Protocol protocol, int writerId); // owns keys (writerId << 32) | seq; generator seed = writerId
  public void createDatabase(); public void createSchema();
  public long awaitSchema(int timeoutSeconds);                  // until facetQuery("") on THIS node is settled(): every source index, lagMs 0
  public int ingest(int count);                                 // returns acknowledged inserts
  public OpCounts runMixedOps(int ops);                         // record OpCounts(long acked, long failed)
  public FacetSnapshot queryFacets(String where);               // also records client metrics, see below
  public SortedMap<String, Long> groupBy(String where);
  public FacetSnapshot awaitSettledFacets(String where, int timeoutSeconds);
  public void assertFacetsMatchGroupBy(int timeoutSeconds);     // every CHECKED_FILTER, on this node
  public List<String> awaitRebuildToIndex(int timeoutSeconds);  // returns the distinct source sequence seen
  public double scrapeMetric(String name); public double scrapeMetric(String name, String label, String value);
  public static void assertSameFacetsOnEveryNode(List<FacetDatabaseWrapper> nodes, int timeoutSeconds);
  public String describe();                                     // host:port for messages
}
```

Decisions:
- Writes and queries go through `RemoteDatabase` (HTTP, `CONNECTION_STRATEGY.FIXED`, timeout 30 s) or `RemoteGrpcDatabase`, built exactly as `DatabaseWrapper.connectToDatabaseHttp/Grpc` (`DatabaseWrapper.java:78-102`); facets via `ResultSet.getFacets()` then `FacetSnapshot.of`. This exercises plan 4's remote clients; the chaos harness exercises raw HTTP JSON (Task 4).
- "Schema ready" means every facet answers from the index at lag 0 on every node, before any write (plan 3 final review C M9: a COLUMNAR index created on an empty type publishes asynchronously, and a write racing it counts a `create` rebuild and fallback scans). Every caller runs `awaitSchema` on every wrapper before the first write.
- Every `ResultSet` is closed (try-with-resources), although the server releases unclosed ones through a `Cleaner`.
- Mixed-op percentages: INSERT 45 (`INSERT_NO_BRAND` when `omitBrand()`), UPDATE_VALUES 25, REMOVE_BRAND 2, GROW 8, UPDATE_GROWN 8, DELETE 12; an op that needs a live key inserts instead when none exists. Live keys in a `long[]` with swap-remove on DELETE (removed on any outcome); grown keys in a second `long[]` (may hold deleted keys: the UPDATE matches nothing, harmless).
- Client metrics (Micrometer, `Metrics.globalRegistry`): `Timer arcadedb.test.facets.write` tag `op`; `Counter arcadedb.test.facets.write.error`; `Timer arcadedb.test.facets.query` with `publishPercentiles(0.5, 0.95, 0.99)`; `DistributionSummary arcadedb.test.facets.lagMs` and `arcadedb.test.facets.patched`, both with the same percentiles; counters `arcadedb.test.facets.exact`, `.nonexact`, `.query.error`, `.meta.inconsistent` (a sample with `!metaConsistent()`).
- Stable read used by both assertion methods: `g1 = groupBy(w)`, poll `queryFacets(w)` every 500 ms until `comparable()`, `g2 = groupBy(w)`; retry while `g1 != g2`; then compare and, on mismatch, `throw new AssertionError(describe() + " where='" + w + "' " + diff)`. `assertFacetsMatchGroupBy` does this for the first comparable sample and again for the first `settled()` sample. Retry exceptions until the timeout (the node may be catching up), as `DatabaseWrapper.await` does (`:296-320`).
- `assertSameFacetsOnEveryNode`: poll until `groupBy` is equal on every node for every filter twice in a row, then `assertFacetsMatchGroupBy` on each node; on timeout the message lists `FacetSnapshot.diff` of each node against node 0.
- `awaitRebuildToIndex`: poll every 500 ms; record each new distinct per-facet source tuple; a `comparable()` sample must equal `groupBy` (stable read); once every source is `index` and `settled()`, return; a non-`index` source after an all-`index` sample fails ("projection fell back after it was usable").
- `scrapeMetric`: `GET /prometheus` with Basic `root:PASSWORD` on this node (`HttpURLConnection`, as `TimeSeriesDatabaseWrapper.latestTimestamp`, `:348-374`), then `PrometheusScrape.sum`.

- [ ] **Step 1: Implement `FacetDatabaseWrapper` and the `DatabaseWrapper.createDatabase` extraction.** No container-free unit test: its pure logic lives in Tasks 1-2, its I/O is exercised by Tasks 8-12.
- [ ] **Step 2: Compile both modules**

Run: `./mvnw -o -pl load-tests,e2e-ha -am verify -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Run the existing load-tests unit and e2e-ha unit suites that cover `DatabaseWrapper` consumers**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest='com/arcadedb/containers/ha/**/Facet*Test,ChaosSchemaTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all PASS.

- [ ] **Step 4: Stop: changes ready for review** - `FacetDatabaseWrapper.java`, `DatabaseWrapper.java`.

---

### Task 4: `FacetCheck` and its HTTP reader

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetReader.java`
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/HttpFacetReader.java`
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetCheck.java`
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FakeFacetReader.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetCheckTest.java`

**Interfaces:**
- Consumes: Tasks 1-2; `ChaosHttp.post` (`ChaosHttp.java:62`), `HttpNodeReader.checkStatus` (`HttpNodeReader.java:124`, package-private static) and `HttpNodeReader.ServerErrorException`; `Violation`, `ResultKind`, `InvariantChecker.MAX_KEYS`.
- Produces:
```java
public interface FacetReader {
  SortedMap<String, Long> groupBy(int node, String where) throws IOException;
  FacetSnapshot facets(int node, String where) throws IOException;
}
public final class HttpFacetReader implements FacetReader { public HttpFacetReader(Endpoints endpoints, String database); }
public final class FacetCheck {
  public FacetCheck(FacetReader reader, int nodes, Duration timeout, Duration pollInterval);
  public List<Violation> run() throws InterruptedException;   // empty = pass; stops at the first violation
}
final class FakeFacetReader implements FacetReader {
  final Map<Integer, SortedMap<String, Long>> data = new HashMap<>();          // GROUP BY truth per node
  final Map<Integer, Deque<FacetSnapshot>> script = new HashMap<>();           // served in order, last one repeats; empty = exact settled copy of data
  final Set<Integer> serverError = new HashSet<>(); final Set<Integer> unreadable = new HashSet<>();
  Runnable afterFirstFacetRead;                                                 // e.g. a late commit
  static FacetSnapshot sample(boolean exact, long lagMs, String source, SortedMap<String, Long> counts);
}
```

Invariants (all `Violation(kind, invariant, message, new long[0], details)`; details carry `FacetSnapshot.diff` lines, capped at `InvariantChecker.MAX_KEYS`):

| Invariant | Kind | When |
|---|---|---|
| `FACET_DIVERGENCE` | SAFETY | `GROUP BY` never identical on every node (twice in a row) within the timeout |
| `FACET_DRIFT` | SAFETY | on a node, the first `comparable()` sample, or the first `settled()` sample, differs from that node's `GROUP BY`, read stably |
| `FACET_META` | LIVENESS | a sample with `exact=false` and `lagMs=0` (logged with the sample; wrong counts are FACET_DIVERGENCE/FACET_DRIFT) |
| `FACET_STALL` | AVAILABILITY | quiesced, but no `comparable()` sample or no `settled()` sample within the timeout (apply thread stuck, rebuild never ends, or the memory budget keeps refusing the patch set) |
| `FACET_ERROR` | SAFETY | a 5xx on a facet or `GROUP BY` query (node up, cannot serve its data; mirrors `SCAN_ERROR`) |
| `FACET_SCAN` | AVAILABILITY | other I/O failures persisting until the timeout |

Algorithm, per filter in `FacetSchema.CHECKED_FILTERS`: (1) poll `groupBy` on every node until identical and unchanged across two polls; (2) per node, poll `facets` until `comparable()`, checking `metaConsistent()` on every sample; read `groupBy` again; if it changed (a late commit), go back to (1); compare (`FACET_DRIFT`, message names node, filter and the sample's `exact/patched/lagMs`); (3) poll until `settled()`, compare again (message says "at lag 0"). Cross-node facet equality follows from (1) plus (2)/(3) on each node. One deadline for the whole check.

- [ ] **Step 1: Write the failing tests**

```java
@Test void identicalNodesWithExactFacetsPass()            // data identical on 3 nodes, default script -> isEmpty()
@Test void facetsThatDisagreeWithGroupByAreFacetDrift() {
  // node 1 script: one settled sample with brand=Nike 4 while data says 5
  final List<Violation> v = check(reader, Duration.ofSeconds(5)).run();   // deadlines: 5 s unless the case expects a timeout, as CheckpointTest
  assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
  assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
  assertThat(v.getFirst().message()).contains("node 1").contains("where=''");
  assertThat(v.getFirst().details()).containsExactly("brand=Nike expected=5 actual=4");
}
@Test void driftWhilePatchingIsFacetDrift()                // first exact sample (patched=7, lagMs=40) wrong, settled one right -> FACET_DRIFT, message contains "patched=7"
@Test void driftOnlyAtLagZeroIsFacetDrift()                // patched sample right, settled one wrong -> message contains "at lag 0"
@Test void nonExactSamplesAreNotCompared()                 // 3 samples exact=false lagMs=50 with wrong counts, then right -> isEmpty()
@Test void scanSourceIsComparedAndIndexAwaited()           // samples: unavailable, scan (right counts), index settled -> isEmpty()
@Test void scanSourceWithWrongCountsIsFacetDrift()
@Test void projectionThatNeverSettlesIsFacetStall() {      // exact, lagMs stays 30
  assertThat(check(reader, Duration.ofMillis(300)).run()).singleElement()
      .satisfies(x -> { assertThat(x.invariant()).isEqualTo("FACET_STALL"); assertThat(x.kind()).isEqualTo(ResultKind.AVAILABILITY); });
}
@Test void sourceThatNeverReturnsToIndexIsFacetStall()
@Test void exactFalseWithoutLagIsFacetMeta()
@Test void nodesHoldingDifferentDataAreFacetDivergence()   // data node 2 brand=Nike 4 vs 5 -> details contain "node 2: brand=Nike expected=5 actual=4"
@Test void lateCommitBetweenReadsIsRetriedNotReported()    // afterFirstFacetRead bumps brand=Nike on every node -> isEmpty()
@Test void serverErrorIsFacetError()
@Test void unreadableNodeUntilTheDeadlineIsFacetScan()
@Test void secondFilterIsCheckedToo()                      // drift only for where='fileSize >= 1024' -> message contains it
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest=FacetCheckTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `FacetCheck` not found.

- [ ] **Step 3: Implement `FacetReader`, `FakeFacetReader`, `FacetCheck`, `HttpFacetReader`.** `HttpFacetReader` posts `{"language":"sql","command":...,"params":{}}` to `/api/v1/query/<db>` with connect 5 s / read 120 s (as `HttpNodeReader`), passes status and body to `HttpNodeReader.checkStatus(node, ...)`, returns `FacetSnapshot.parse(new JSONObject(body))` for facets and `FacetSchema.expectedCounts(sql -> post(node, sql).getJSONArray("result"), where)` for `GROUP BY`.

- [ ] **Step 4: Run tests to verify they pass**

Run: the command of Step 2. Expected: `Tests run: 15, Failures: 0`.

- [ ] **Step 5: Stop: changes ready for review** - the five files above.

---

### Task 5: Report facet violations at the existing checkpoint

**Files:**
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/Checkpoint.java:42-63` (field and constructor), `:119-125` (violations block)
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/CheckpointTest.java` (add tests), `ChaosRunnerTest.java` (add one test and a harness variant)

**Interfaces:**
- Consumes: Task 4 `FacetCheck`, `FakeFacetReader`.
- Produces: `public Checkpoint(NodeReader, Ledger, InvariantChecker, int nodes, Duration convergenceTimeout, Duration pollInterval, FacetCheck facets)`; the existing 6-argument constructor delegates with `null`.

Decision: the facet check runs only after convergence, the scan, the late-commit re-read, `DIVERGENCE` and the ledger checks, and only when they produced no violation (`violations.isEmpty() && facets != null`), so it reuses the convergence point and never masks a ledger finding. `Result.durationMillis` includes it, so `trends.csv` shows its cost. Violations reach `ChaosReport.ledgerDiff` and `summary.json` through the unchanged `ChaosRunner.fromViolations`/`finish` path (`ChaosRunner.java:361-380`).

- [ ] **Step 1: Write the failing tests**

```java
// CheckpointTest
@Test void facetDriftIsReportedAfterLedgerChecksPass() throws InterruptedException {
  // identical ChaosOp rows on 3 nodes; FakeFacetReader with node 0 drift
  assertThat(checkpoint(reader, facetCheck(drifting)).run().violations()).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
}
@Test void facetCheckIsSkippedWhenTheLedgerAlreadyFailed()   // node 1 lost an acked key -> containsExactly("CONVERGENCE"), FakeFacetReader never called
@Test void noFacetCheckKeepsTheOldBehaviour()                // 6-arg constructor, identical nodes -> isEmpty()

// ChaosRunnerTest
@Test void facetDriftEndsTheRunAsSafety() throws IOException {
  final ChaosResult result = harnessWithFacets(config(), driftingReader()).runner().run();
  assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
  assertThat(result.steps()).isEqualTo(1);
  assertThat(Files.readString(dir.resolve("7/ledger-diff.txt"))).contains("== FACET_DRIFT (SAFETY)");
  assertThat(new JSONObject(Files.readString(dir.resolve("7/summary.json"))).getString("result")).isEqualTo("SAFETY");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest='CheckpointTest,ChaosRunnerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL (7-argument constructor missing).

- [ ] **Step 3: Implement the constructor overload and the call at `Checkpoint.java:124`.** Update the class Javadoc (lines 30-37) with one sentence on the facet check.

- [ ] **Step 4: Run tests to verify they pass**

Run: the command of Step 2. Expected: all tests of both classes PASS (existing ones unchanged).

- [ ] **Step 5: Stop: changes ready for review** - `Checkpoint.java`, `CheckpointTest.java`, `ChaosRunnerTest.java`.

---

### Task 6: Facet workload beside the ledger workload

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetWorkload.java`
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/CombinedLoad.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/FacetWorkloadTest.java` (fake `HttpServer`, as `WorkloadTest.java:42-60`), `CombinedLoadTest.java` (two `FakeLoad`s)

**Interfaces:**
- Consumes: `LoadGenerator` (`LoadGenerator.java`), `Endpoints`, `ChaosHttp`, Task 2 `FacetSchema`, `AssetGenerator`.
- Produces:
```java
public final class FacetWorkload implements LoadGenerator {
  public FacetWorkload(long seed, int writers, Endpoints endpoints, String database);
  public FacetWorkload(long seed, int writers, Endpoints endpoints, String database, int connectTimeoutMs, int readTimeoutMs, int thinkMs);
  public record Stats(long writesOk, long writesFailed, long queries, long queryServerErrors, long nonExact) {}
  public Stats stats();
  // acked() returns writesOk; the runner never reads it (CombinedLoad reports the ledger workload)
}
public final class CombinedLoad implements LoadGenerator {
  public CombinedLoad(LoadGenerator primary, LoadGenerator... others);   // acked() = primary.acked()
}
```

Decisions:
- Same closed-loop shape as `Workload` (`Workload.java:70-123`): one thread per writer named `chaos-facet-writer-<w>`, `Random(seed ^ (0x5DEECE66DL * (w + 1)))`, a fair `ReentrantReadWriteLock` gate for `quiesce`/`resume`, think 10 ms, each op sent once to a node picked at random whatever its state, reads timeouts 15 s. Each writer owns `new AssetGenerator(seed ^ (0x5DEECE66DL * (w + 1)))`, used only by its thread (the generator is not thread-safe, and a shared one would make the statement sequence depend on thread interleaving).
- Op mix (percent): INSERT 35 (5% of them `INSERT_NO_BRAND`), UPDATE_VALUES 20, REMOVE_BRAND 3, GROW 10, UPDATE_GROWN 10, DELETE 12, LINK 5, QUERY 5 (`facetQuery` with a random `CHECKED_FILTERS` entry, to `/api/v1/query`, response parsed and `nonExact` counted; a 5xx is counted in `queryServerErrors`, never a violation: faults are in progress). Keys `(w << 32) | seq`, owned by one writer, so no write-write conflict between writers. Outcomes are not ledgered: `FacetCheck` compares each node with itself and with its peers.
- `CombinedLoad`: `start` primary then others; `quiesce` primary then others (each blocks until idle); `resume` in reverse order; `close` every member even if one throws (rethrow the first). `close` is idempotent on both `CombinedLoad` and `FacetWorkload`, as `LoadGenerator` requires (the runner and the try-with-resources both close).

- [ ] **Step 1: Write the failing tests**

```java
// FacetWorkloadTest
@Test void sameSeedSendsTheSameStatementSequence()   // two runs, 1 writer, 200 requests each -> identical list of "command" values
@Test void mixCoversEveryStatement()                 // 2 writers, 2000 requests -> each FacetSchema statement and the facet query appear; DELETE share in [8%,16%]
@Test void writesTargetOnlyOwnedKeys()               // 2 writers: every :aid, :from, :to param satisfies (key >>> 32) in {0, 1}
@Test void quiesceBlocksEveryWriter()                // after quiesce, request count unchanged for 300 ms; after resume it grows
@Test void serverErrorsOnQueriesAreCountedNotThrown() // status 500 -> stats().queryServerErrors() > 0, writers still running
// CombinedLoadTest
@Test void fansOutAndReportsThePrimary()             // start/quiesce/resume/close reach both FakeLoads; acked() equals the primary's
                                                     // (both FakeLoads with progress = false, so acked() does not move between reads)
@Test void closeReachesEveryMemberWhenOneFails()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest='FacetWorkloadTest,CombinedLoadTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL.

- [ ] **Step 3: Implement `FacetWorkload` and `CombinedLoad`.**

- [ ] **Step 4: Run tests to verify they pass**

Run: the command of Step 2. Expected: `Tests run: 7, Failures: 0`.

- [ ] **Step 5: Stop: changes ready for review** - the four files above.

---

### Task 7: Wire facets into `HaChaosIT` and `ChaosConfig`, then run a seeded chaos

**Files:**
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosConfig.java:39-42` (record header), `:83-102` (`fromProperties`), `:52-81` (validation), `:134-149` (`replayCommand`)
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/HaChaosIT.java:118-128` (wiring), `:178-204` (`createDatabaseAndSchema`)
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosConfigTest.java`

**Interfaces:**
- Consumes: Tasks 4-6.
- Produces: `ChaosConfig.facetWriters()` (`int`, new last record component), property `chaos.facetWriters`, default 2, `0` disables facets entirely (no facet DDL, no `FacetWorkload`, no `FacetCheck`).

- [ ] **Step 1: Write the failing tests** (`ChaosConfigTest`)

```java
@Test void facetWritersDefaultToTwoAndReplay() {
  final ChaosConfig config = ChaosConfig.fromProperties(props());
  assertThat(config.facetWriters()).isEqualTo(2);
  assertThat(config.replayCommand()).contains(" -Dchaos.facetWriters=2");
}
@Test void facetWritersZeroDisablesFacets() { assertThat(ChaosConfig.fromProperties(props("chaos.facetWriters", "0")).facetWriters()).isZero(); }
// in rejectsInvalidValues(): props("chaos.facetWriters", "-1") -> IllegalArgumentException containing "chaos.facetWriters"
```

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest=ChaosConfigTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL (`facetWriters()` missing).

- [ ] **Step 3: Implement**
  - `ChaosConfig`: component, `facetWriters >= 0` check, `integer(properties, "chaos.facetWriters", 2)`, ` -Dchaos.facetWriters=` in `replayCommand` after `chaos.writers`.
  - `HaChaosIT.createDatabaseAndSchema`: when `facetWriters > 0`, run `FacetSchema.DDL` after `ChaosSchema.DDL`, then await on every node `new HttpFacetReader(control, ChaosSchema.DATABASE).facets(node, "").settled()` (Awaitility 60 s, `ignoreExceptions`, as `:199-203`), before the workloads start; never the inherited `ContainersTestTemplate.DATABASE` (`"playwithpictures"`).
  - After a PASS, `HaChaosIT` runs `compareAllDatabases(ChaosSchema.DATABASE)` (`:134-142`), which opens every node's copy READ_ONLY in the test JVM, COLUMNAR shells included: Task 2 `readOnlyReopenComparesEqual` proves that path embedded.
  - `HaChaosIT.chaos`: `FacetCheck facetCheck = facetWriters == 0 ? null : new FacetCheck(new HttpFacetReader(control, ChaosSchema.DATABASE), config.nodes(), config.convergenceTimeout(), Duration.ofSeconds(1))`; 7-argument `Checkpoint`; `FacetWorkload` in the try-with-resources beside `Workload`, seeded `config.seed()`; runner load = `new CombinedLoad(workload, facets)` when enabled; after `runner.run()`, `logger.info("CHAOS facets: {}", facets.stats())`.

- [ ] **Step 4: Run unit tests**

Run: `./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest='com/arcadedb/containers/ha/**/*Test' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all PASS.

- [ ] **Step 5: Build the image under test and run a short seeded chaos**

```bash
./mvnw clean install -Pdocker -DskipTests --batch-mode   # tags arcadedata/arcadedb:latest and :26.11.1-SNAPSHOT (package/pom.xml:417-428)
./mvnw verify -Pintegration -pl e2e-ha -Dit.test=HaChaosIT -Dfailsafe.excludedGroups= \
  -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT \
  -Dchaos.seed=42 -Dchaos.duration=PT20M -Dchaos.maxSteps=8 -Dchaos.faults=kill,rolling,isolate,loss,pause,latency \
  '-Dchaos.serverOpts=-Darcadedb.ha.snapshotThreshold=500 -Darcadedb.ha.logPurgeGap=16'
```
The low snapshot threshold makes a restarted follower install a snapshot (spec 10 "snapshot install mid-apply"); `latency` covers the spec 10 "delay" condition. Expected: `CHAOS result PASS`, `e2e-ha/target/chaos/42/summary.json` `"result": "PASS"`, the `CHAOS facets:` line shows `queries > 0` and `writesOk > 0`. A `FACET_*` result is a finding: keep `target/chaos/42/` and the replay line, report it, do not weaken the check.

- [ ] **Step 6: Stop: changes ready for review** - `ChaosConfig.java`, `ChaosConfigTest.java`, `HaChaosIT.java`.

---

### Task 8: `LeaderFailoverIT` - a new replica rebuilds in the background

**Files:**
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/LeaderFailoverIT.java` (new method after `leaderFailover`, `:46-149`; existing methods untouched)

**Interfaces:**
- Consumes: Task 3 `FacetDatabaseWrapper`; `ContainersTestTemplate.createArcadeContainer(name, serverList, quorum, network)` (`:580`), `startCluster`, `findLeaderIndex` (`:1050`), `waitForRaftLeader`, `waitForContainerHealthy` (`:1033`).
- Produces: `void leaderFailoverNewReplicaRebuildsFacets()`.

Decision: `createArcadeContainer` copies an empty host directory into the container and `stop()` removes the container, so the killed leader comes back with no database and no Raft storage: a genuinely new replica. Each container gets `.withEnv("arcadedb.ha.snapshotThreshold", "200").withEnv("arcadedb.ha.logPurgeGap", "16")` (GlobalConfiguration reads a setting from the environment variable named after it on both images, `ContainersTestTemplate.configureServer`; `ARCADEDB_OPTS_MEMORY` is owned by `configureServer` and ignored by the native image), so the log is purged. A fresh replica with no local copy is bootstrapped from a leader snapshot whatever the log purge (`ArcadeStateMachine` late-joiner path), so the rejoiner installs a snapshot either way; the low threshold also exercises the purge path on the survivors.

- [ ] **Step 1: Write the test** (`@Test @Timeout(value = 15, unit = TimeUnit.MINUTES)`)

```java
// 3 nodes, f[i] = new FacetDatabaseWrapper(servers.get(i), Protocol.HTTP, i)
f[0].createDatabase(); f[0].createSchema(); for (f) f.awaitSchema(60);
f[0].ingest(2_000); f[0].runMixedOps(1_000);
FacetDatabaseWrapper.assertSameFacetsOnEveryNode(List.of(f[0], f[1], f[2]), 120);
// kill leader L, elect among survivors, write 1_000 mixed ops through survivor S1 (writerId 10), converge survivors
// before L restarts: before[s] = survivor.scrapeMetric("arcadedb_columnar_rebuilds_total", "reason", "snapshot_install")
//   (awaited non-stale: wait one 5 s metrics refresh); log before[s] (a lagging survivor may have installed during the ingest, spec 6.6 accepts it)
// restart L (fresh container), wait healthy, rejoined = new FacetDatabaseWrapper(new ServerWrapper(containers[L]), HTTP, 20)
rejoined.awaitSchema(180);
// start survivor1.runMixedOps(1_000) in a thread (writerId 30), running while awaitRebuildToIndex polls (rebuild under writes)
final List<String> sources = rejoined.awaitRebuildToIndex(300);   // stable-read comparison tolerates the moving data
logger.info("New replica facet sources: {}", sources);
// join the writer thread
assertThat(rejoined.scrapeMetric("arcadedb_columnar_rebuilds_total", "reason", "snapshot_install")).isGreaterThanOrEqualTo(1.0);
for (survivor) assertThat(survivor.scrapeMetric("arcadedb_columnar_rebuilds_total", "reason", "snapshot_install"))
    .as("survivors install nothing during the rejoin").isEqualTo(before[s]);
FacetDatabaseWrapper.assertSameFacetsOnEveryNode(List.of(rejoined, survivor1, survivor2), 180);
```
Whether `scan`/`unavailable` is observed depends on how fast the rebuild is, so it is logged, not asserted; what is asserted is that every comparable sample is right, that `index` is reached and never left, and that a rebuild happened.

- [ ] **Step 2: Run it**

Run: `./mvnw verify -Pintegration -pl e2e-ha -Dit.test='LeaderFailoverIT#leaderFailoverNewReplicaRebuildsFacets' -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT` (image from Task 7 Step 5; rebuild it after any server change).
Expected: PASS. Then run the whole class to prove the other methods are unaffected: `-Dit.test=LeaderFailoverIT`.

- [ ] **Step 3: Prove the rebuild assertions discriminate** - the survivors' `snapshot_install` delta is 0 in the same run while the rejoiner's is >= 1, and Task 9's restarts (persistent storage) assert no rebuild at all. Optionally rerun without the two `withEnv` settings and log the rejoiner's count: a fresh replica is bootstrapped from a leader snapshot (`ArcadeStateMachine` late-joiner path) whatever the log purge, so `>= 1` is expected there too; it is not a plan 3 defect.

- [ ] **Step 4: Stop: changes ready for review** - `LeaderFailoverIT.java`.

---

### Task 9: `RollingRestartIT` and `NetworkPartitionRecoveryIT` - restart recovers without a rebuild

**Files:**
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/RollingRestartIT.java` (new method after `rollingRestart`, `:83-286`; reuse `hasLeader`, `:48-72`)
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/NetworkPartitionRecoveryIT.java` (new method after `partitionRecovery`, `:56-160`)

**Interfaces:**
- Consumes: Task 3; `createPersistentArcadeContainer` (`ContainersTestTemplate.java:608` 4 args / `:620` 6 args, bind mounts survive `stop()`/`start()`, and `stop()` SIGKILLs, so restart goes through crash recovery).
- Produces: `void rollingRestartRecoversFacetProjectionsWithoutRebuild()`, `void partitionRecoveryKeepsFacetsExact()`.

- [ ] **Step 1: Write `rollingRestartRecoversFacetProjectionsWithoutRebuild`** (`@Timeout(20 MINUTES)`)

```java
// 3 persistent nodes; ingest 3_000 + runMixedOps(1_000) via node 0; assertSameFacetsOnEveryNode(all, 120)
for (int i = 0; i < 3; i++) {
  final int node = i;
  await().atMost(60, SECONDS).until(() -> f[node].scrapeMetric("arcadedb_columnar_segments") > 0);  // something flushed to disk
  f[i].close(); containers[i].stop();
  await().atMost(30, SECONDS).until(() -> hasLeader(liveServers));
  f[(i + 1) % 3].runMixedOps(500);                         // writes continue while the node is down
  containers[i].start(); waitForContainerHealthy(containers[i], 90);
  f[i] = new FacetDatabaseWrapper(new ServerWrapper(containers[i]), Protocol.HTTP, 100 + i);
  await().atMost(60, SECONDS).until(() -> f[node].scrapeMetric("arcadedb_columnar_segments") > 0); // metrics bound, manifest loaded
  FacetDatabaseWrapper.assertSameFacetsOnEveryNode(List.of(f), 180);
  assertThat(f[i].scrapeMetric("arcadedb_columnar_rebuilds_total")).as("restart must not rebuild").isZero();
}
```
`rebuilds_total` with no label filter: the counter restarts with the process and an existing index never takes the `create` reason at open, so any value above 0 is a rebuild caused by the restart (spec anti-goal 2). It is asserted after `assertSameFacetsOnEveryNode`, by which time any rebuild the open scheduled has started and been counted (series bind on the 5 s `ColumnarMetrics` refresh). After the SIGKILL the projection is unusable until its page check ends, so facets read `scan` for a while: the stable-read helpers tolerate it, and `awaitRebuildToIndex`'s "fell back after usable" logic is not used on a restart. Sizing: 3000 records over 10 buckets stay far below the 1024-page `unclean_pages` floor; a larger ingest must keep that true.

- [ ] **Step 2: Write `partitionRecoveryKeepsFacetsExact`** (`@Timeout(15 MINUTES)`): as `partitionRecovery` (`:56-160`), with facet data instead of users: ingest 2_000; isolate a follower; `runMixedOps(1_500)` through the leader while querying the other (connected) follower 20 times with `queryFacets("")`, each `assertThat(s.metaConsistent()).isTrue()`; heal with the same reconnect plus stop/start; on the restarted node await `scrapeMetric("arcadedb_columnar_segments") > 0` (60 s); `assertSameFacetsOnEveryNode(all, 180)`; then on the restarted node `scrapeMetric("arcadedb_columnar_rebuilds_total") == 0` (same order as Step 1).

- [ ] **Step 3: Run both**

Run: `./mvnw verify -Pintegration -pl e2e-ha -Dit.test='RollingRestartIT#rollingRestartRecoversFacetProjectionsWithoutRebuild,NetworkPartitionRecoveryIT#partitionRecoveryKeepsFacetsExact' -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT`
Expected: PASS. Then each whole class, to check the untouched methods.

- [ ] **Step 4: Stop: changes ready for review** - `RollingRestartIT.java`, `NetworkPartitionRecoveryIT.java`.

---

### Task 10: `SplitBrainIT` and `PacketLossIT`

**Files:**
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/SplitBrainIT.java` (new method after `splitBrainPrevention`, `:50-178`)
- Modify: `e2e-ha/src/test/java/com/arcadedb/containers/ha/PacketLossIT.java` (new method after `moderatePacketLoss`, `:136-199`; reuse the proxy constants `:49-67`)

**Interfaces:**
- Consumes: Task 3.
- Produces: `void splitBrainMinorityLeaderRejoinsWithExactFacets()`, `void moderatePacketLossKeepsFacetsExact()`.

- [ ] **Step 1: Write `splitBrainMinorityLeaderRejoinsWithExactFacets`** (`@Timeout(15 MINUTES)`): persistent 3 nodes; ingest 2_000 + mixed 500; converge; isolate the leader as `splitBrainPrevention` does (`:85`); wait for a leader among the other two; `runMixedOps(1_000)` through the new leader while the majority follower's samples must be `metaConsistent()`; heal (reconnect, stop/start as at `:135-141`); on the restarted old leader await `scrapeMetric("arcadedb_columnar_segments") > 0` (60 s); `assertSameFacetsOnEveryNode(all, 180)`; then on the restarted old leader `scrapeMetric("arcadedb_columnar_rebuilds_total") - scrapeMetric("arcadedb_columnar_rebuilds_total", "reason", "snapshot_install") == 0`, the install count logged (Raft never applied its uncommitted entries, so its pages roll forward, not back; a deposed leader may be resynced by a snapshot, which rebuilds by design, spec 6.6). The isolated node is not queried while isolated: the existing tests note its mapped port is unreachable from the host.

- [ ] **Step 2: Write `moderatePacketLossKeepsFacetsExact`** (`@Timeout(15 MINUTES)`): proxies and 2-node cluster as `moderatePacketLoss`; ingest 1_000; add 20% `limitData` toxics on both Raft proxies; run `runMixedOps(1_000)` on node 0 in one thread while another thread queries node 1 every 200 ms until the writer ends, each sample `metaConsistent()`; log max observed `lagMs`; remove toxics; `assertSameFacetsOnEveryNode(List.of(f0, f1), 180)`.

- [ ] **Step 3: Run both**

Run: `./mvnw verify -Pintegration -pl e2e-ha -Dit.test='SplitBrainIT#splitBrainMinorityLeaderRejoinsWithExactFacets,PacketLossIT#moderatePacketLossKeepsFacetsExact' -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT`
Expected: PASS, then each whole class.

- [ ] **Step 4: Stop: changes ready for review** - `SplitBrainIT.java`, `PacketLossIT.java`.

---

### Task 11: `SingleServerFacetLoadTestIT`

**Files:**
- Create: `load-tests/src/test/java/com/arcadedb/test/load/SingleServerFacetLoadTestIT.java` (shape of `SingleServerTimeSeriesLoadTestIT.java:37-114`)

**Interfaces:**
- Consumes: Task 3; `ContainersTestTemplate.createArcadeContainer(name, network)` (`:554`), `startContainers`; the client meters through a cumulative registry, not `sumCounter` (see Step 1).
- Produces: `void singleServerFacetLoadTest(DatabaseWrapper.Protocol protocol)`.

Decisions: `@ParameterizedTest(name = "Facet load test with {0}") @EnumSource(DatabaseWrapper.Protocol.class)`; `WRITERS = 4`, `OPS_PER_WRITER = 20_000`, `QUERY_THREADS = 2`; writers are `FacetDatabaseWrapper(server, protocol, t)` running `runMixedOps`, started only after `awaitSchema` returned on the server; query threads loop `queryFacets(random CHECKED_FILTER)` until the writers' executor terminates; progress logged every 5 s (`groupBy("")` total and the last sample's `lagMs`/`patched`), as the TS test logs point counts.

- [ ] **Step 1: Write the test.** After the writers end and the query threads are joined:

```java
admin.assertFacetsMatchGroupBy(120);
final FacetSnapshot settled = admin.awaitSettledFacets("", 120);
assertThat(settled.exact()).isTrue();
assertThat(settled.lagMs()).isZero();
assertThat(total("arcadedb.test.facets.query.error")).isZero();
assertThat(total("arcadedb.test.facets.write.error")).isZero();
assertThat(total("arcadedb.test.facets.meta.inconsistent")).isZero();
assertThat(fallbackScansAfterSetup).isZero();   // projection present the whole run: the change since the setup
assertThat(admin.scrapeMetric("arcadedb_columnar_rebuilds_total")).as("indexes created on an empty type never rebuild").isZero();
assertNoCorruptedReads(baseline);
```
As implemented (rulings of the Task 11 and 12 reviews): the client meters are read from a cumulative `SimpleMeterRegistry` added for the run (`total(...)`), not `sumCounter`, which reads a 10 s step registry and misses most of a run (#10945); the fallback-scan counter is read right after `awaitSchema` and the assertion is on its change over the run, since an added column on an open projection was published by an async create rebuild during the setup (F2; published inline since the final fix batch); setup query errors (followers polled before the database exists) are excluded; any "Possible corrupted record" or "Skipping corrupted property" line in the container logs fails the run (#10701); a duplicate key on a retried INSERT counts as acked (its first attempt committed).
Logged, not asserted: every meter in `Metrics.globalRegistry` (query latency p50/p95/p99, `lagMs` and `patched` percentiles), throughput `(acked ops) / elapsed seconds` and queries per second, and the server-side `arcadedb_columnar_query_budget_waits_total`, `_query_patch_overflows_total`, `_query_patched_total`, `_query_fallback_scans_total`, `_overlay_rows`, `_apply_lag_ms`, `_segments`; and written as one JSON object (`com.arcadedb.serializer.json.JSONObject`: throughput, query p50/p95/p99, lag and patched percentiles, budget waits, patch overflows, fallback scans, overlay rows, apply lag, nonexact count) to `load-tests/target/facet-load/SingleServerFacetLoadTestIT-<protocol>.json`, with one summary log line. These files are the plan 6 baseline of the roadmap Tuning phase (#10662).

- [ ] **Step 2: Run it**

Run: `./mvnw verify -Pintegration -pl load-tests -Dit.test=SingleServerFacetLoadTestIT -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT`
Expected: 2 PASS (HTTP, GRPC); the log shows `patched` percentiles above 0 at least once, which proves the queries ran while the projection lagged (inspect, do not assert). With `patchBudgetRatio` 1.0 a lagging answer is patched, not inexact; non-zero `nonexact` means the memory budget refused: inspect.

- [ ] **Step 3: Stop: changes ready for review** - `SingleServerFacetLoadTestIT.java`.

---

### Task 12: `ThreeNodesFacetLoadTestIT`

**Files:**
- Create: `load-tests/src/test/java/com/arcadedb/test/load/ThreeNodesFacetLoadTestIT.java` (shape of `ThreeNodesTimeSeriesLoadTestIT.java:42-166`, including the `tearDown` override that skips `compareAllDatabases` for non-persistent containers)

**Interfaces:**
- Consumes: Task 3; `createArcadeContainer(name, SERVER_LIST, "majority", network)`, `startCluster`.
- Produces: `void threeNodeFacetLoad(DatabaseWrapper.Protocol protocol)`.

Decisions: `NUM_WRITERS = 3`, `OPS_PER_WRITER = 15_000`, writers on node 0 (writes forward to the leader), started only after `awaitSchema` returned on every node; 3 query threads, thread `q` queries node `q`, so two of them read followers; each sample `metaConsistent()` is counted, not thrown, so one bad sample does not stop the load; per-node max `lagMs` logged.

- [ ] **Step 1: Write the test.** After the writers end:

```java
FacetDatabaseWrapper.assertSameFacetsOnEveryNode(List.of(r0, r1, r2), 180);
for (final FacetDatabaseWrapper r : List.of(r0, r1, r2)) {
  final FacetSnapshot settled = r.awaitSettledFacets("", 120);
  assertThat(settled.exact()).as(r.describe()).isTrue();
  assertThat(r.scrapeMetric("arcadedb_columnar_query_fallback_scans_total")).as(r.describe()).isZero();
  assertThat(r.scrapeMetric("arcadedb_columnar_rebuilds_total", "reason", "snapshot_install")).as(r.describe()).isZero();
}
assertThat(total("arcadedb.test.facets.query.error") - setupQueryErrors).isZero();
assertThat(total("arcadedb.test.facets.meta.inconsistent")).isZero();
assertThat(total("arcadedb.test.facets.write.error")).isZero();
```
As implemented: the same cumulative registry, setup-error exclusion, fallback change since the setup and corrupted-record check as Task 11; per node also `rebuilds_total == 0` of any reason (no restart, the type is empty at creation). A follower's facet query can meet a chain a replicated apply is still writing; since Task 12 that read fails with a retryable `ConcurrentModificationException` (#10941), which the wrapper retries and counts.
Logged and written: same as Task 11, per node (`load-tests/target/facet-load/ThreeNodesFacetLoadTestIT-<protocol>.json`, one object per node). Also logged per node at the end: the WAL file count and total size (`docker exec ... ls -l /home/arcadedb/databases/<db>/*.wal`), next to those numbers (plan 5 main-merge review: one DB-wide `queuedColumnarMarks` counter may delay WAL drops under sustained load). `snapshot_install == 0` per node holds: 45000 writes stay below the default 100000 snapshot threshold.

- [ ] **Step 2: Run it**

Run: `./mvnw verify -Pintegration -pl load-tests -Dit.test=ThreeNodesFacetLoadTestIT -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT`
Expected: 2 PASS.

- [ ] **Step 3: Run every touched suite once more**

```bash
./mvnw -o -pl e2e-ha -am verify -DskipITs -Dtest='com/arcadedb/containers/ha/**/*Test' -Dsurefire.failIfNoSpecifiedTests=false   # every e2e-ha unit test, nothing upstream
./mvnw verify -Pintegration -pl load-tests -Dit.test='SingleServerTimeSeriesLoadTestIT,ThreeNodesTimeSeriesLoadTestIT,SingleServerFacetLoadTestIT,ThreeNodesFacetLoadTestIT' -Darcadedb.test.image=arcadedata/arcadedb:26.11.1-SNAPSHOT
```
Expected: all PASS (the time-series ITs prove the `DatabaseWrapper` extraction and the shared template changed nothing).

- [ ] **Step 4: Stop: changes ready for review** - `ThreeNodesFacetLoadTestIT.java`.

---

## Gate (roadmap row 6)

Task 0's engine test passes; the chaos run of Task 7 Step 5 and both load ITs report no `FACET_*` violation and no failed assertion, and the load ITs leave their `target/facet-load/*.json` baselines; the five scenario methods pass in the nightly `ha-resilience-tests.yml` run (`-Dit.test` input of that workflow reruns a single class).
