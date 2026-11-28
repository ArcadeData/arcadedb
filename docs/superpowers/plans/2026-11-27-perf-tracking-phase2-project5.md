# Performance tracking phase 2, project 5 (HA) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `ha` workload: a three-node Raft cluster of the image under test, each node on its own third of the
server cores, measuring write throughput through the leader and replication lag to the followers, and failing a case when
an acknowledged write is missing on any node or the nodes' contents differ.

**Architecture:** `ServerUnderTest` learns to start one cluster member (Docker network, alias, Raft settings); a new
`ClusterUnderTest` starts three of them on cpusets split from the run's server cpuset and finds the leader. A workload may
declare that it brings its own servers, so `BenchMain` hands it the server spec instead of starting the single server.
Pure parts (cpuset split, write ledger, content digest, lag matching) are unit-tested; the cluster is exercised by one IT.
One PR in arcadedb; no perf-infra change.

**Tech Stack:** Java 21, Testcontainers (`GenericContainer`, `Network`), ArcadeDB remote HTTP client (`RemoteDatabase`,
`CONNECTION_STRATEGY.FIXED`, read consistency `EVENTUAL`), JUnit 5 + AssertJ.

**Spec:** `docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md`, section 3 "HA (`ha`, project 5)", the
"Order and delivery" table and its rule "After every project the weeknight S run must still end inside 240 minutes".

## Global Constraints

- JSON only through `JSONObject` / `JSONArray`; `final` on locals and parameters; single-statement `if` without braces;
  imports, no fully qualified names; no `System.out`; no em dash anywhere; primitive arrays over boxed collections on hot paths.
- Tests: `assertThat(...)`; never assert on raw wall-clock time; no hand-picked host ports (Testcontainers maps them);
  Docker tests are `*IT`.
- Bench tests: `./mvnw -o -pl bench -am test -Dtest='com.arcadedb.bench.**.*Test' -Dsurefire.failIfNoSpecifiedTests=false`.
- Cluster (spec): three server containers on one Docker network, the server cores split in three cpusets, the client on its
  own cores (the run's `--client-cpus`, unchanged). Raft settings as `ContainersTestTemplate.haSettings`:
  `arcadedb.ha.enabled=true`, `arcadedb.ha.quorum=majority`, `arcadedb.server.readinessRequiresHA=true`,
  `arcadedb.ha.serverList=ha-0:2434:2480,ha-1:2434:2480,ha-2:2434:2480`, `arcadedb.server.name=ha-<i>`, plus the bench's
  own `ServerUnderTest.settings` defaults. Raft port 2434 stays inside the network (not exposed).
- Node memory: `nodeMemGb = max(2, memLimitGb / 3)`, heap `max(1, nodeMemGb * 6 / 10)` GB (16 GB run limit: 5 GB, heap 3g).
- Cases and metrics (spec): `write-throughput-1w` and `write-throughput-8w`: `records_per_s` (rate, `records/s`) and
  `latency_p50_ms` / `latency_p95_ms` / `latency_p99_ms` (`Metrics.latencies`); `replication-lag`: `lag_p50_ms`,
  `lag_p95_ms`, `lag_max_ms` (`ms`, LOWER), over both followers together. Every case also reports `unknown_writes`
  (unit `writes`, LOWER).
- One write = one transaction inserting one `HaRecord {id: long, writer: int, payload: string(200)}` through the leader;
  `id` has a unique index. Rep length: **15 s** at XS and S, **45 s** at M and L (time-bounded, so the budget is predictable).
- Replication lag: one writer at a scheduled **100 writes/s** for the rep, one poller per follower; lag = first time the
  follower shows the id minus the leader's acknowledgement time, clamped at 0. Followers are read with read consistency
  `EVENTUAL` (the `RemoteDatabase` default, sent as `X-ArcadeDB-Read-Consistency`).
- Write ledger (spec): every acknowledged id is looked up on every node after the rep; an exception on a write makes its id
  **unknown** (it may have landed), counted in `unknown_writes`, never a failure by itself; a missing acknowledged id
  fails the case. Before the check, wait up to **60 s** for each follower to reach the leader's record count.
- Digest (spec): per type of the database, SHA-256 over `SELECT FROM <type> ORDER BY @rid`, each record as its JSON with
  sorted keys including `@rid`; a node whose digest differs from the leader's fails the case, naming the node and type.
- The dashboard note (spec: "the numbers track the engine's replication cost, not a real network") goes in the workload's
  class Javadoc and in the PR body; the dashboard itself is in the public results repo and gets the line after merge.
- Commits authored by robfrank only, no Co-Authored-By. The PR says `Part of #12433`.

## Review Focus

- **Ruling against the spec's delivery table:** the spec puts the cpusets in perf-infra; the bench already resolves the
  server cpuset (`--server-cpus auto`, P-cores `0-11` on perf-01) and pins every container itself, so the split lives in
  the bench and perf-infra is unchanged. Cost if wrong: one infra PR later.
- A cpuset with fewer CPUs than nodes (a laptop, `--server-cpus none`): every node gets the whole set, and the run file says
  `cpusets: shared`. Pinned in Task 1 (`fewerCpusThanNodesShareTheWholeSet`, `emptyMeansUnpinned`).
- A leader change during a rep (election timeout under load): writes keep going to the old leader and fail. The rep then
  counts them unknown, re-resolves the leader after every 3 failures in a row, and the case fails only on a ledger or
  digest miss; the change is published as `leader_changes` (unit `changes`, never judged by the gate). Pinned in Task 3 (`aLeaderChangeIsRecordedNotFatal` with a fake cluster).
- A node that dies mid-case: the cluster reports which node and why (container state), and the case fails with that
  message; the next case starts a fresh cluster. Pinned in Task 2 (`deathReasonNamesTheNode`).
- Containers and the Docker network are removed even when the leader never gets elected (start timeout **90 s**). Pinned
  in Task 2 (`aClusterThatNeverElectsIsClosed`).
- The S budget: the 11-27 S night ended at 184 of 240 min. The `ha` S cost is measured in Task 4; above 25 min for both
  variants it runs from M up, as `ramp` does.

---

### Task 1: Cpuset split and node sizing (pure)

**Files:** Create `bench/src/main/java/com/arcadedb/bench/server/Cpusets.java`; test `bench/src/test/java/com/arcadedb/bench/server/CpusetsTest.java`.

**Interfaces:**
- `static String[] Cpusets.split(String cpus, int parts)`: parses `0-3,8,10-11` style lists; returns `parts` contiguous
  groups in the listed order, sizes differing by at most one (the first groups get the extra CPU); an empty string gives
  `parts` empty strings; fewer CPUs than parts gives the whole set to every part.
- `static int nodeMemGb(int memLimitGb, int nodes)` and `static String nodeHeap(int nodeMemGb)` per the Global Constraints.

- [ ] Step 1: failing tests: `splitsTwelveIntoThreeFours` (`0-11` gives `0-3`, `4-7`, `8-11`), `keepsListOrderAndRanges`
  (`0-3,8-11` in 2 gives `0-3` and `8-11`), `unevenSplitGivesTheExtraToTheFirst` (`0-6` in 3 gives `0-2`, `3-4`, `5-6`),
  `fewerCpusThanNodesShareTheWholeSet` (`0-1` in 3 gives `0-1` three times), `emptyMeansUnpinned`, `nodeSizing` (16 gives 5
  and `3g`; 6 gives 2 and `1g`; 2 gives 2 and `1g`).
- [ ] Step 2: run, expect failure. Step 3: implement. Step 4: bench unit suite passes.

### Task 2: Cluster members and `ClusterUnderTest`

**Files:**
- Modify `bench/src/main/java/com/arcadedb/bench/server/ServerUnderTest.java`: `static ServerUnderTest startMember(String image,
  String cpuset, int memLimitGb, String heap, Map<String, String> settings, Network network, String alias)`, sharing the
  existing `start` body (network and alias are the only additions; same ready wait, same sampler).
- Create `bench/src/main/java/com/arcadedb/bench/server/ClusterUnderTest.java` (`AutoCloseable`).
- Test `bench/src/test/java/com/arcadedb/bench/server/ClusterUnderTestTest.java` (pure parts), and the IT of Task 4.

**Interfaces:**
- `static ClusterUnderTest ClusterUnderTest.start(String image, String serverCpus, int memLimitGb, int nodes,
  Map<String, String> settings)`: one `Network`, members `ha-0..ha-<n-1>` on `Cpusets.split`, Raft settings of the Global
  Constraints, waits up to 90 s for a leader via `GET /api/v1/cluster` (`"isLeader":true`, as
  `ContainersTestTemplate.waitForRaftLeader`), closes everything it started on failure.
- `List<ServerUnderTest> members()`, `int leaderIndex()`, `int refreshLeader()` (asks the members again, returns the index),
  `String deathReason()` (`null`, or `ha-<i>: <ServerUnderTest.deathReason()>` for the first dead member),
  `JSONObject config()` (image, per-member cpuset, node memory and heap, `cpusets: split|shared`), `close()` (members, then
  the network; never throws).
- Package-private seam for tests: `static int leaderOf(List<String> clusterAnswers)` parsing the `/api/v1/cluster` bodies
  (index of the first `"isLeader":true`, -1 for none).

- [ ] Step 1: failing tests: `leaderOfFindsTheLeader`, `leaderOfNoneIsMinusOne`, `deathReasonNamesTheNode` (a member whose
  death reason is scripted through a `ServerUnderTest` test seam, or `deathReason` computed from a list of member reasons:
  pick the seam that needs no container), `aClusterThatNeverElectsIsClosed` (a start whose leader wait is a seam returning -1
  closes every member and the network: assert through a recording closer).
- [ ] Step 2: run, expect failure. Step 3: implement. Step 4: unit suite passes; `./mvnw -o -pl bench -am compile`.

### Task 3: Workloads that bring their own servers, and the `ha` workload

**Files:**
- Modify `bench/src/main/java/com/arcadedb/bench/runner/Workload.java`: `default boolean ownsServers() { return false; }`.
- Modify `bench/src/main/java/com/arcadedb/bench/runner/WorkloadContext.java`: add `ServerSpec spec` (record `ServerSpec(String
  image, String serverCpus, int memLimitGb, String heap)`), keeping a 4-argument constructor (spec `null`) so every existing
  call compiles unchanged.
- Modify `bench/src/main/java/com/arcadedb/bench/BenchMain.java`: for a workload with `ownsServers()`, start no server and
  no `Guard`, run it with `runner.run(w, ctx, null, null, ...)` and the spec in the context; the run file records the
  cluster config the workload hands back (`HaWorkload.lastClusterConfig()`, or a generic `Workload.serverConfig()` default
  returning null: pick the one that keeps `BenchMain` free of `ha` names).
- Create `bench/src/main/java/com/arcadedb/bench/workload/ha/WriteLedger.java`, `ContentDigest.java`, `LagMatcher.java`
  (pure), and `bench/src/main/java/com/arcadedb/bench/workload/HaWorkload.java`; `WorkloadRegistry` appends `new HaWorkload()`.
- Tests: `bench/src/test/java/com/arcadedb/bench/workload/ha/{WriteLedgerTest,ContentDigestTest,LagMatcherTest}.java`,
  `HaWorkloadTest.java`, `BenchMainTest.aWorkloadThatOwnsItsServersGetsNoServer`.

**Interfaces:**
- `WriteLedger(int capacityHint)`: `void acked(long id)`, `void unknown(long id)`, `long ackedCount()`, `long unknownCount()`,
  `long[] missingFrom(long[] sortedIdsOnNode)` (acked ids absent on the node; unknown ids are never missing). Backed by
  growable `long[]`, not boxed sets.
- `ContentDigest`: `static String of(Iterator<JSONObject> recordsInRidOrder)` (hex SHA-256 of each record's sorted-key JSON
  plus `\n`); `static List<String> differences(String type, String[] digestsByNode)` (`ha-<i> differs from the leader on
  <type>` lines).
- `LagMatcher(long[] ackNanosById)`: `void seen(long upToId, long nowNanos)` records the lag of every id up to `upToId` not
  seen before on this follower, clamped at 0; `LatencyRecorder lags()`.
- `HaWorkload` named `ha`; cases `write-throughput-1w`, `write-throughput-8w`, `replication-lag` at every scale (Task 4 may
  restrict S); `prepare` starts the cluster (`ClusterUnderTest.start(spec.image(), spec.serverCpus(), spec.memLimitGb(), 3,
  serverSettings())`) once per case, creates the database `ha` on the leader, waits until it exists on every member (60 s),
  creates `HaRecord` with the unique index; `measure` runs one time-bounded rep; after each rep the ledger and digest checks
  run (outside the timed window); `teardown` closes the cluster.
- A write that throws: id goes to `unknown`; after **3** consecutive failures the writer calls `refreshLeader()` once and
  continues against the new leader; the leader change is logged.

- [ ] Step 1: failing tests:
  - `WriteLedgerTest`: `ackedIdMissingOnANodeIsReported`, `unknownIdsAreNeverMissing`, `growsPastItsHint`.
  - `ContentDigestTest`: `keyOrderDoesNotChangeTheDigest`, `oneChangedValueChangesIt`, `differencesNameTheNodeAndType`.
  - `LagMatcherTest`: `lagIsFromTheAckClampedAtZero`, `anIdIsCountedOncePerFollower`, `aJumpRecordsEveryIdUpToIt`.
  - `HaWorkloadTest`: `casesAndMetricsAreTheSpecs` (names and units of the Global Constraints), `ownsItsServers`,
    `aMissingAckedWriteFailsTheCaseWithMetrics` and `aDigestMismatchFailsTheCaseWithMetrics` (through a package-private
    `verify(WriteLedger, long[][] idsByNode, String[][] digestsByTypeAndNode)` returning the failure message or null),
    `aLeaderChangeIsRecordedNotFatal` (a scripted writer target that fails 3 times then succeeds on the refreshed leader:
    unknown 3, case ok).
  - `BenchMainTest.aWorkloadThatOwnsItsServersGetsNoServer` (the factory's `start` is never called; the context carries the
    spec).
- [ ] Step 2: run, expect failure. Step 3: implement. Step 4: unit suite passes.

### Task 4: IT, S cost, PR

**Files:** test `bench/src/test/java/com/arcadedb/bench/workload/HaWorkloadIT.java`; modify the spec
(`docs/superpowers/specs/2026-11-23-perf-tracking-phase2-design.md`, HA section) with the ruling on cpusets and the S decision.

- [ ] Step 1: `HaWorkloadIT` at XS with a package-private rep-length override of **3 s**, image `arcadedata/arcadedb:latest`
  (as the other ITs): all three cases `ok`, `records_per_s` > 0, `lag_max_ms` >= `lag_p50_ms`, `unknown_writes` 0, and after
  `teardown` no container or network of the run is left (Testcontainers' `DockerClientFactory` listing by the run's label).
- [ ] Step 2: run `ha` at S once from the bench jar on Docker locally; report per case time, `records_per_s`, lag
  p50/p95/max, `unknown_writes`.
- [ ] Step 3: push, open the PR (`Part of #12433`) with this plan, the dashboard note and the numbers; dispatch `ha` at S
  and at M on perf-01 with `bench_ref` set to the PR head. If the two S variants together take over **25 min**, restrict
  S with `cases(Scale)` returning empty (as `RampWorkload`), and say so in the spec and the PR.
- [ ] Step 4: after merge, dispatch `ha` at M on the merge commit; numbers in the PR. Merge stays with the user.
