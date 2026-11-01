# HA Integration Tests Runtime Reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the `ha-integration-tests.yml` lane (244 IT classes, 506 `@Test`) back under its cap with real headroom, by removing duplicate tests, regrouping `Issue<N>` classes by topic and booting the 3-node cluster once per class instead of once per test.

**Architecture:** Four phases, each shippable alone. (1) Raise the workflow cap. (2) Delete proven-duplicate ITs and stop paying for red/slow ones. (3) Add an opt-in class-scoped cluster fixture in `ha-raft`. (4) Merge topic groups of `Issue*IT` classes onto that fixture, and move stray tests into the existing class that owns their subject.

**Tech Stack:** JUnit 5, Maven Failsafe (`-Pintegration`), `BaseRaftHATest` / `BaseGraphServerTest`, GitHub Actions.

**Spec:** Analysis of run 41176861440 (cancelled at the 120 min cap, 240 of 244 classes done, 115.7 min summed class time). Cause: `BaseGraphServerTest.beginTest()` is `@BeforeEach`, so every `@Test` boots its own cluster (about 10-15 s each; `Issue10067ImplicitTypeInClientTransactionOnAFollowerIT` = 17 tests, 221 s).

## Global Constraints

- Do not commit; the maintainer commits after review (project CLAUDE.md). Steps that say "Commit" mean "stage and stop".
- No `System.out`; `final` on variables and parameters; no fully qualified names; assertions in the form `assertThat(x).isTrue();`.
- Never assert raw wall-clock time; use `StallAwareStopwatch`.
- Never hard-code ports; derive from `getServerHttpPort(i)` / `allocateFixturePorts(n)`.
- Run ITs with `./mvnw -o verify -Pintegration -pl ha-raft -am -DskipTests=false -Dit.test=<Class>` from the repo root is NOT allowed at reactor root (`-Dit.test` kills the parent module). Use `cd ha-raft && ../mvnw -o verify -Pintegration -Dit.test=<Class>` after installing upstream modules once.
- Every test removed or merged must keep every assertion that is not provably covered elsewhere. Count `@Test` before and after; the delta must be explained in the PR body.
- No tracking `.md` in PRs other than this plan; analysis goes in the PR body.
- Docs: American English.

## Review Focus

- A merged class where one test leaves the cluster degraded (stopped node, quarantine, isolated leader) and the next test inherits it: expect the next test to fail or pass for the wrong reason.
- A shared-cluster test that creates a type, database setting or user with a fixed name: expect a collision with the next test; names must be unique per test.
- A shared cluster whose teardown check fails silently: expect a poisoned cluster to be reused instead of rebooted.
- A deleted "duplicate" that asserted one extra thing: expect a lost regression guard.
- A test that depends on a fresh cluster's initial state (Raft log index, term, leader id): expect a failure only when run second in the class.

---

## Phase 1: Raise the cap

### Task 1: Raise `timeout-minutes` and record the measured runtime

**Files:**
- Modify: `.github/workflows/ha-integration-tests.yml` (`timeout-minutes: 120` and its comment block)

**Interfaces:**
- Produces: a 150 minute cap that later phases may lower again.

- [ ] **Step 1:** Change `timeout-minutes` from `120` to `150`. Replace the comment with: run 41176861440 summed 115.7 min of class time across 240 of 244 classes and was cancelled at 120; 150 keeps the #5973 livelock net and leaves about 25 percent headroom.
- [ ] **Step 2:** Run `python3 .github/scripts/check-workflow-artifact-deps.py` if present, and `actionlint .github/workflows/ha-integration-tests.yml` if installed. Expected: no findings.
- [ ] **Step 3:** Stage `.github/workflows/ha-integration-tests.yml`. Stop for review.

---

## Phase 2: Remove duplicates, stop paying for red and oversized tests

### Task 2: Decide and remove each probable duplicate

**Files:**
- Delete or modify, per verdict: the seven pairs below, all under `ha-raft/src/test/java/com/arcadedb/server/ha/raft/`.

| Pair | Verdict | Outcome |
|---|---|---|
| `Issue10965ReplicaCommitRefusedDuringRestoreClaimIT` / `Issue10974RestoreHoldRefusesReplicaCommitIT` | duplicate | `Issue10965...IT` deleted (its assertions are a subset of #10974's); #10974 later merged into `HaRestoreClaimIT` (Task 8) |
| `Issue4416SlicedSealedStoreIT` / `RaftTimeSeriesOversizedSealedIT` | distinct | both kept: opposite branches of the same guard (sliced vs skipped compaction) |
| `Issue4458TsWalVersionGapIT` / `TimeSeriesThreeNodeWalGapReproIT` | distinct | both kept: WAL-gap counter on 2 nodes vs line-protocol ingestion on 3 nodes |
| `Issue8487TargetedTransferUnderWritesIT` / `Issue8533TargetedTransferUnderWritesWinsIT` | overlap | #8487's two extra assertions folded into #8533's existing `@Test` (no new cluster boot); `Issue8487...IT` deleted; #8533 later merged into `HaLeadershipTransferIT` (Task 8) |
| `Issue9449AcceptDivergedDatabaseIT` / `Issue9498AcceptStaleSnapshotIT` | distinct | merged in Task 8 into `SoleVoterOperatorOverrideIT` |
| `Issue10060SchemaRefusalNamesTheLeaderIT` / `Issue10066SecurityRefusalNamesTheLeaderIT` | distinct | merged in Task 7 into `HaPeerAddressRoutingIT` (well-configured cluster) and `HaPeerAddressRoutingMapMutationIT` (ambiguous address map) |
| `Issue10634ContinuousAggregateLateSampleReplicationIT` / `Issue10673LateSampleRecordShippedBeforeTheSamplesIT` | distinct | both kept: watermark and transactional path vs record-before-samples ordering |

**Interfaces:**
- Produces: a verdict table (kept / deleted / folded, with the assertion lines that justify it) in the PR body.

- [ ] **Step 1:** For each pair, read both classes and list their assertions side by side. Verdict per pair: `duplicate` (one assertion set contains the other), `overlap` (fold the extra assertions into the kept class as a new `@Test`), or `distinct` (leave, merge in Phase 4).
- [ ] **Step 2:** For every `duplicate` and `overlap`, run the surviving class: `cd ha-raft && ../mvnw -o verify -Pintegration -Dit.test=<Survivor>`. Expected: PASS.
- [ ] **Step 3:** Prove the survivor can fail: temporarily break the production behavior it names (or invert one assertion), rerun, expect FAIL, revert.
- [ ] **Step 4:** Delete the redundant class. Check the unit-test siblings (`Issue4416*Test`, `Issue10965*Test`, etc.) still compile.
- [ ] **Step 5:** Stage and stop. PR body carries the verdict table and the `@Test` count delta.

### Task 3: Fix the red Issue10067 classes and tag the oversized ones

**Files:**
- Modify: `Issue10067ImplicitTypeInClientTransactionOnAFollowerIT.java`, `Issue10067IndexedSuperTypeOnAFollowerIT.java` (1 failure and 3 errors in run 41176861440)
- Modify: `RaftHAComprehensiveIT.java` (260 s), `RaftHARandomCrashIT.java` (223 s), `RaftHTTP2ServersCreateReplicatedDatabaseIT.java` (132 s)

**Interfaces:**
- Consumes: the lane tags in CLAUDE.md. Produces: `@Tag("slow")` on classes moved to the slow lane.

- [ ] **Step 1:** Reproduce locally: `cd ha-raft && ../mvnw -o verify -Pintegration -Dit.test=Issue10067*IT`. Read the four failures; if the cause is a product bug, file an issue and do not mask it; if it is test-only, fix it.
- [ ] **Step 2:** Confirm the CI lane selection first: check that the `integration` profile does not already exclude `slow` for failsafe (`failsafe.excludedGroups` in `pom.xml:68`). Only tag classes into a lane that some workflow actually runs; otherwise they silently stop running.
- [ ] **Step 3:** Decide per big class: keep in this lane, or tag `slow` if a workflow runs `slow` ITs. Record the decision in the PR body.
- [ ] **Step 4:** Rerun the touched classes. Expected: PASS. Stage and stop.

---

## Phase 3: Class-scoped cluster fixture

### Task 4: `SharedClusterRaftHATest` base class (opt-in)

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SharedClusterRaftHATest.java`
- Modify: `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java` (guard in `checkEndTestIsALifecycleMethod`, around line 112)
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SharedClusterFixtureIT.java`

**Interfaces:**
- Consumes: `BaseRaftHATest` (`beginTest()`, `endTest()`, `getServerCount()`, `servers`).
- Produces: `abstract class SharedClusterRaftHATest extends BaseRaftHATest` annotated `@TestInstance(Lifecycle.PER_CLASS)` with:
  - `@BeforeAll void startSharedCluster()` calling `super.beginTest()`;
  - `@AfterAll void stopSharedCluster()` calling `super.endTest()`;
  - overrides of `beginTest()` and `endTest()` WITHOUT `@BeforeEach` / `@AfterEach`, so JUnit does not run them per test;
  - `@BeforeEach void ensureClusterIsHealthy()`: every server `isStarted()`, one leader, every replica connected and caught up; otherwise restart the whole cluster (stop, `deleteDatabaseFolders()`, start) before the test runs;
  - `protected String uniqueName(String prefix)` returning `prefix + "_" + counter`, for types, users and settings;
  - `protected boolean isClusterSharedAcrossTests()` returning `true`, consumed by the guard in `BaseGraphServerTest`.

- [ ] **Step 1:** Write `SharedClusterFixtureIT` with three tests: (a) all three see the same `servers[0]` instance (`assertThat(seen).hasSize(1)`); (b) a test that stops a follower and does not restart it, followed by a test asserting `ensureClusterIsHealthy()` rebooted a full 3-node cluster (use `@TestMethodOrder(OrderAnnotation)`); (c) `uniqueName("T")` never repeats across tests.
- [ ] **Step 2:** Run it. Expected: FAIL (class not defined).
- [ ] **Step 3:** Add `protected boolean isClusterSharedAcrossTests() { return false; }` to `BaseGraphServerTest` and make `checkEndTestIsALifecycleMethod()` return early when it is `true`. Implement `SharedClusterRaftHATest` per the Interfaces block.
- [ ] **Step 4:** Run `SharedClusterFixtureIT`. Expected: PASS. Then run `RaftReplicationIT` and `Issue5297PolymorphicCountIT` (untouched, per-test fixture) to confirm the guard change breaks nothing.
- [ ] **Step 5:** Run `HardcodedTestServerPortsTest` in `server`. Expected: PASS. Stage and stop.

### Task 5: Pilot on one existing multi-test class and measure

**Files:**
- Modify: `Issue10157ClusterRoutesRefuseUndeclaredMethodsIT.java` (4 tests, 34 s; stateless route checks)

**Interfaces:**
- Consumes: `SharedClusterRaftHATest` from Task 4.

- [ ] **Step 1:** Record baseline: `cd ha-raft && ../mvnw -o verify -Pintegration -Dit.test=Issue10157*IT`; note `Time elapsed` from the Failsafe summary.
- [ ] **Step 2:** Change `extends BaseRaftHATest` to `extends SharedClusterRaftHATest`; replace any fixed type or user names with `uniqueName(...)`.
- [ ] **Step 3:** Rerun. Expected: PASS and elapsed at least 25 percent lower. Run it 5 times in a row; all PASS.
- [ ] **Step 4:** Record baseline vs after in the PR body. This number sizes the Phase 4 payoff; if the saving is under 15 percent, stop and rethink before Phase 4. Stage and stop.

---

## Phase 4: Merge by topic, move strays

**Rules for every merge task:** merged class `extends SharedClusterRaftHATest` only if no member test stops, isolates, quarantines or restarts a node, replaces a database, or changes cluster-wide settings it does not restore; such tests stay on `BaseRaftHATest` in a separate `...DisruptiveIT` class of the same topic. Keep each source test method name and its `Issue<N>` reference in a `@DisplayName` or Javadoc line. Delete the source class only after the merged class passes. Per task: record `@Test` count before and after (must be equal), run the merged class 3 times, and run `Issue*` neighbors untouched.

### Task 6: TLS and removed-node families

**Files:**
- Create: `HaTlsPeerDialIT.java` (10 tests, `BaseRaftHASslTest`, per-test fixture, merge for naming only) from `Issue7563HaTlsPeerDialIT` and `Issue7835HaTlsRemainingPeerDialsIT`
- Create: `HaRemovedNodeIT.java` (15 tests, disruptive, per-test fixture) from `Issue9510*`, `Issue9589*`, `Issue9590*`, `Issue9606*`, `Issue7040*`
- Left as they are, because their fixture differs: `Issue3890RaftMtlsIT` (3 servers, mutual auth, own PKI), `Issue3890RaftServerOnlyTlsIT`, `Issue7836HaTlsSniIT` and `Issue7836HaTlsWrongCertificateNameIT` (own subject alternative names), `Issue7837RemoteShutdownCredentialIT` (`BaseRaftHATest`, 2 servers), `Issue5275MembershipSelfHealIT` (needs persistent Raft storage)
- Outcome: 7 sources deleted; both new classes keep the per-test fixture, so this task saves no cluster boot

- [ ] **Step 1:** Merge TLS classes; run 3 times; PASS.
- [ ] **Step 2:** Merge removed-node classes; run 3 times; PASS.
- [ ] **Step 3:** Delete the sources. Stage and stop.

### Task 7: Forwarding, routing and security-routing families

**Files:**
- Create (forwarded writes, 10 sources, 26 tests): `HaForwardedWriteIdempotencyIT.java` (shared, 3 servers, 20 tests: `Issue8323*`, `Issue8343*`, `Issue8347*`, `Issue8359*`, `Issue7603*`, `Issue9689*` and the parts of `Issue6191*` / `Issue7516*` that do not touch the address map; two tests pinned with `@Order`), `HaForwardedWriteIdempotencyTwoNodeIT.java` (shared, 2 servers: `Issue8355*`), `HaForwardedWriteIdempotencyDisruptiveIT.java` (per-test, ALL quorum, stops a follower: `Issue8481*`), `HaForwardedWriteMisaddressedLeaderIT.java` (per-test: the `Issue6191*` / `Issue7516*` tests that inject addresses, which raced the capability monitor on a shared cluster)
- Create (security routing, 14 tests): `HaSecurityMutationRoutingIT.java` (shared, 8 tests: `Issue8109*`, `Issue8370*`, `Issue7380*`, `Issue7559*`), `HaSecurityMutationRoutingTwoNodeIT.java` (shared, 2 servers: `Issue7833*`, `Issue7549*`, part of `Issue8034*`), `HaSecurityMutationRoutingDisruptiveIT.java` (per-test: the `Issue8034*` test that transfers leadership)
- Create (peer address routing, 15 tests): `HaPeerAddressRoutingIT.java` (shared: the well-configured halves of `Issue10060*` / `Issue10066*`), `HaPeerAddressRoutingDeclaredGrpcIT.java` (shared: `Issue6091*`), `HaPeerAddressRoutingMapMutationIT.java` (per-test: `Issue6267*`, `Issue9255*` and the ambiguous halves of `Issue10060*` / `Issue10066*`), `HaPeerAddressRoutingUndeclaredPortsIT.java` (per-test: `Issue6183*`), `HaPeerAddressRoutingRestartIT.java` (per-test: `Issue9243*`)
- Moved: `Issue5314*` into `GetClusterHandlerIT`; `Issue5503*` into `RaftFollowerWriteForwardIT`; `Issue8784*` into `RaftReplicationWriteAgainstReplicaIT`
- Not moved, because the fixture of the target does not fit: `Issue5297*` (`RaftReplicationIT` is a hand-built standalone cluster), `Issue10342*` (2 servers vs 3, persistent storage, its own `@AfterEach`), `Issue579*` (`RaftSplitBrain3NodesIT` is an in-process `BaseMiniRaftTest`)

- [ ] **Step 1:** Create each merged class; members that mutate the address map, need an ambiguous `serverList`, restart a node or move leadership go to a per-test sibling of the same topic.
- [ ] **Step 2:** Perform the moves whose target fixture fits, each as a method in the target class using `uniqueName(...)` if the target is shared.
- [ ] **Step 3:** Run all touched classes 3 times; PASS. Delete sources. Stage and stop.

### Task 8: Schema, compaction, restart and leadership families

**Files:**
- Create: `HaSchemaDeltaReplicationIT.java` (shared, 6 tests: `Issue6988*` incremental, `Issue6989*` drop, `Issue6990*`), with per-test siblings `HaSchemaDeltaReplicationFreshClusterIT` (`Issue6989*` single property, cumulative counter), `HaSchemaDeltaReplicationFullRebuildIT` (`Issue6988FullRebuildFallbackIT`, a start-time setting) and `HaSchemaDeltaReplicationRestartIT` (`Issue8728*`, persistent storage, follower restart)
- Create: `HaSchemaDdlReplicationIT.java` (shared, 2 servers, 23 tests: `Issue9747*`, `Issue9762*`, `Issue9763*`, `Issue9764*`, `Issue7438*`), `HaSchemaDdlReplicationThreeNodeIT` (shared, 3 servers: `Issue8404*`, `Issue10349*`, `Issue10506*`), and per-test siblings `HaSchemaDdlReplicationFailoverIT` (`Issue9747ConstraintAfterFailoverIT`), `HaSchemaDdlReplicationUnknownOutcomeIT` (`Issue9904*`), `HaSchemaDdlReplicationIsolationIT` (`Issue9934*`), `HaSchemaDdlReplicationRestartIT` (the `Issue9762*` follower restart test)
- Create: `HaCompactionReplicationIT.java` (shared, `Issue4743CompactionEntrySizeIT` + the split variant of `Issue5443*`), with per-test siblings `HaCompactionReplicationSingleEntryIT` (`Issue5443*` at the stock buffer), `HaCompactionReplicationStockConfigIT` (`Issue4743LargeRecordReplicationIT`), `HaCompactionReplicationBloomFilterIT` and `HaCompactionReplicationBloomFilterFullIT` (`Issue5517*` x2)
- Create: `HaInPlaceRestartRecoveryIT.java` (per-test, persistent storage: `Issue4839*`, `Issue8901*`, `Issue9411*`) and `HaInPlaceRestartRecoveryHealthMonitorIT` (per-test, health monitor on: `Issue5271*`, `Issue8652*`); `Issue8900ClosedServerInPlaceRestartIT` left as it is
- Create: `HaLeadershipTransferIT.java` (per-test, 9 tests: `Issue4809*`, `Issue5276*`, `Issue8480*`, `Issue8533*` carrying the folded #8487 assertions, `Issue8491*`, `Issue8592*`, `RaftTransferLeadershipIT`); `Issue8483QuarantinedLeaderHandsOffLeadershipIT` left as it is (2 servers, its own health interval)
- Create: `HaMixedVersionIT.java` (shared: `Issue10051MixedVersion*`, `Issue10856*`), `HaMixedVersionSchemaDeltaIT` (shared, populated fixture database: `Issue7219*` x2), `HaMixedVersionThreeNodeIT` (`Issue7511*`)
- Create: `HaStaleHandleIT.java` (shared: `Issue8281*`), `HaStaleHandleImplicitHaIT` (`Issue8270ImplicitHADatabaseWrappedIT`), `HaStaleHandleDisruptiveIT` (`Issue8270FollowerRestartUnderLoadIT`, `Issue8282*`, `Issue8383*`)
- Create: `HaRestoreClaimIT.java` (shared: `Issue10881*`, `Issue10974*`), `HaRestoreClaimReplicatedDropIT` (`Issue8035*`), `HaRestoreClaimTwoNodeIT` (`Issue8363*`)
- Create: `HaExecutorMetricsIT.java` (shared: `Issue7856*`, `Issue8856*`) and `HaExecutorMetricsDisruptiveIT` (the `Issue8856*` test that disturbs the cluster)
- Merge: `Issue5492*` x2 into `Issue5655CypherCommitsOnInnerDatabaseIT`, renamed `InnerDatabaseCommitReplicationIT` (shared, 7 tests); `Issue9449*` and `Issue9498*` into `SoleVoterOperatorOverrideIT` (shared, 4 tests)

- [ ] **Step 1:** Create the classes one family at a time; after each, run it 3 times and delete its sources.
- [ ] **Step 2:** After all families: `ls ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue*IT.java | wc -l`. Expected: only classes with no topic home remain (single-purpose regressions such as `Issue7259*`, `Issue8317*`, `Issue7819*`).
- [ ] **Step 3:** Count `@Test` across `*IT.java`. Expected: Phase 2 count minus deliberately removed duplicates, nothing else.
- [ ] **Step 4:** Stage and stop.

### Task 9: Verify the lane and lower the cap

**Files:**
- Modify: `.github/workflows/ha-integration-tests.yml`

- [ ] **Step 1:** Trigger `workflow_dispatch` on the branch: `gh workflow run ha-integration-tests.yml --ref <branch>`. Not done on this branch: it needs a push, which is left to the maintainer.
- [ ] **Step 2:** Read total wall time from the run and the per-class times (`gh run view <id> --log`). The original expectation of "under 70 min" is NOT reachable by this branch: the estimated class time after the merges is about 103 min against the 115.7 min baseline. The merges saved about 11.5 min (measured locally) plus about 1 min of deleted duplicates; the rest of the lane is untouched.
- [ ] **Step 3:** Set `timeout-minutes` to measured time ×1.5, rounded up to 5. Update the comment with the new measurement. Stage and stop.

---

## Self-review

- Coverage: Phase 1 = cap; Phase 2 = duplicates (Task 2), red/slow classes (Task 3); Phase 3 = fixture (Tasks 4-5); Phase 4 = grouping and moves (Tasks 6-9). Every row of the earlier analysis is assigned to one task.
- Open decision for the maintainer: Task 3 Step 2/3 (move big classes to a `slow` lane only if a workflow runs that lane) and Task 4's `isClusterSharedAcrossTests()` hook in the `server` test-jar.
- Not verified yet: duplicates were identified from names and Javadoc, not body diffs; Task 2 Step 1 is where that gets checked.

---

## Outcome

- **Built:** the cap raised to 150 min (Task 1); 2 duplicate ITs removed (Task 2); `SharedClusterRaftHATest` with `SharedClusterFixtureIT` and the `isClusterSharedAcrossTests()` guard in `BaseGraphServerTest` (Task 4); the pilot on `Issue10157ClusterRoutesRefuseUndeclaredMethodsIT` (about 70 percent faster, Task 5); the topic classes listed under Tasks 6 to 8 (`Ha*IT`, `InnerDatabaseCommitReplicationIT`, `SoleVoterOperatorOverrideIT`), each on the shared fixture only where no member test disturbs the cluster; `Issue8203RaftFixturePortsTest` extended to scan the shared-cluster base so the hard-coded Raft port guard still covers the merged classes.
- **Test count:** 505 -> 508 `@Test`: minus 2 deleted duplicates, plus 5 tests of the fixture itself. Every other source test is kept under its `Issue<N>` reference.
- **Left as they are, deliberately:** the classes whose fixture fits no topic class (Task 6, Task 7 and Task 8 bullets above), the three Task 7 moves whose target fixture differs, and every `Issue*IT` with no topic home.
- **Parked for the maintainer:**
  - `ha-raft/CLAUDE.md` lines 92, 109 and 111 name deleted classes (`Issue6191FollowerForwardLoopIT`, `Issue6267AmbiguousAddressVisibilityIT`, `Issue5492TruncateBatchNotReplicatedIT`); their homes are `HaForwardedWriteMisaddressedLeaderIT`, `HaPeerAddressRoutingMapMutationIT` and `InnerDatabaseCommitReplicationIT`.
  - The #10537 product regression behind `Issue10067*IT` and probably `Issue10093CompositeLabelOverIndexedTypeIT` (a multi-label `CREATE` in a client transaction on a follower is refused) keeps the lane red; it needs an issue and a fix, not a test change.
  - `stopCapabilityMonitor()` does not drain a capability round already in flight, so a test that pauses the monitor can still be healed by a round started before the pause.
  - A class-scoped SSL base would save about 108 s per lane run on `HaTlsPeerDialIT`.
  - `Issue7837RemoteShutdownCredentialIT` could join the shared fixture on its own for about 12 s.
