# HA Chaos Test - Design

Date: 2026-09-23
Module: `e2e-ha` (package `com.arcadedb.containers.ha.chaos`)
Status: design, pending review

## 1. Purpose

The `e2e-ha` resilience ITs (`LeaderFailoverIT`, `SplitBrainIT`, `PacketLossIT`, `RollingRestartIT`, ...) each build a
fresh 3-node Raft cluster, inject exactly one fault, assert, and tear down. They cannot find bugs that only appear when
faults follow each other on the same long-lived cluster (a follower still catching up from a partition when the leader
is killed, a paused ex-leader resuming after two elections, WAL recovery after a SIGKILL during snapshot install), nor
slow degradation over hours (log growth, memory growth, slow divergence).

`HaChaosIT` is one long-running test that drives a **seeded, randomized sequence of faults under continuous write load**
against a single 3- or 5-node cluster, checking **safety invariants against a client-side ledger of acknowledged writes**
after every step, and recording resource trends for soak analysis. The same harness gives a short nightly-sized run or
a multi-hour soak by changing the duration.

### Success criteria

- A violated safety invariant (lost acknowledged write, phantom write, resurrected failed write, partial transaction,
  divergence) fails the run with a report precise enough to reproduce: seed, fault sequence, ledger diff, container logs.
- A given seed replays the same fault sequence, targets and hold times.
- The test never runs as part of the existing nightly `ha-resilience-tests.yml` job; it has its own workflow.
- The harness logic (ledger, invariant checker, config, fault selection, majority rule) is unit-tested without
  containers, including proof that each invariant can fail.

### Out of scope (v1)

- Membership changes (add/remove peer, replace a node with an empty data dir) - follow-up; needs its own ledger
  semantics for the replaced node.
- Resource faults (disk full, clock skew).
- Two faults active at the same time. Compounding comes from sequencing with no reset between steps; true overlap is
  a later extension of the same `Fault` interface.
- Linearizability / read-your-writes checks on follower reads.
- Thresholds on resource trends (report-only in v1).

## 2. Components

All classes live in `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/`.

| Class | Responsibility | Depends on |
|---|---|---|
| `HaChaosIT` | Entry point, `@Tag("chaos")`, extends `ContainersTestTemplate`, `useToxiproxy() = true`. Builds the cluster through Toxiproxy, creates the schema, runs `ChaosRunner`, stops the nodes gracefully, and preserves the database files whenever the final result is not `PASS`. Its inner `DockerNodeControl` implements `NodeControl`, `Endpoints` and `ChaosRunner.TrendSource` over the Docker API and the Toxiproxy client. | all below |
| `ChaosConfig` | Immutable config parsed from system properties (section 6); validates values and builds the replay command. | - |
| `ClusterState` | What the runner has done to each node (`UP`, `DOWN`, `PAUSED`, `ISOLATED`, `DEGRADED`) and the majority rule (`canImpair`). Runner thread only. | - |
| `NodeControl` | Everything a fault can do to a node (kill, graceful stop, start, pause, disconnect, toxics), plus `findLeader`, `awaitLeader` and `unexpectedExit`. `FakeNodeControl` backs the unit tests. | `ClusterState` |
| `Fault` | Interface: `name()`, `canApply(ClusterState)`, `inject(ClusterState, NodeControl, Random)`, `heal(ClusterState, NodeControl)`, `expectsWritesAvailable()`. | `ClusterState`, `NodeControl` |
| `NodeFault` (with `NodeFault.Kind`: kill, stop, pause, isolate), `SplitFault`, `RollingRestartFault`, `ToxicFault` (`LATENCY`, `LOSS`) | The v1 catalog (section 4). | `Targets` |
| `FaultPicker` | Weighted, seeded pick among the faults whose `canApply` holds. | `Fault` |
| `Targets` | Seeded choice of target nodes by role (`LEADER`, `FOLLOWER`, `ANY`) and of the 1-or-2 count on 5 nodes. | `NodeControl` |
| `Workload` | K writer threads producing ledger-tracked operations (section 5), quiesce/resume at an operation boundary. Implements `LoadGenerator`. | `ChaosHttp`, `OpOutcome`, `Endpoints`, `Ledger` |
| `ChaosHttp` | Minimal HTTP client that tells "never sent" (connect failure) apart from "sent, answer unknown"; POST auto-retry disabled. | - |
| `OpOutcome` | Maps an HTTP status or an exception to `ACKED`, `FAILED` or `UNKNOWN` (section 3). | `Ledger` |
| `Endpoints`, `Endpoint` | The current host and port of each node, re-read after every restart. | - |
| `Ledger` | Thread-safe record of every operation key and its outcome (`IN_FLIGHT`, `ACKED`, `FAILED`, `UNKNOWN`, later `ACKED_LATE`, `LOST_UNKNOWN`) plus the pair flag. Keys are `(writer, seq)` encoded as `long` over primitive arrays, so a soak with millions of operations stays GC-light. | - |
| `NodeSnapshot` | One node's key set and per-key edge count as read at a checkpoint, with `diff` against another node. | `Ledger` |
| `InvariantChecker` | Invariants I1-I5 of one converged snapshot against the ledger (section 3), including `UNKNOWN` resolution and late commits. Pure logic. | `Ledger`, `NodeSnapshot` |
| `NodeReader`, `HttpNodeReader` | Reads counts and pages every key of one node from its own `/api/v1/query` endpoint. A 5xx is a `ServerErrorException`. `LedgerNodeReader` backs the unit tests. | `Endpoints`, `ChaosHttp` |
| `Checkpoint` | Convergence wait, per-node scan, cross-node diff, then `InvariantChecker` (section 3). | `NodeReader`, `InvariantChecker` |
| `ChaosRunner` | The step loop (section 3). Owns the single `Random(seed)`. | all of the above |
| `ChaosReport`, `StepRecord`, `TrendRow`, `ChaosResult` | Report files (section 7) and the run's result. | - |
| `ResultKind`, `ChaosFailure`, `Violation` | Result categories (`PASS`, `SAFETY`, `AVAILABILITY`, `HARNESS`), the exception that ends a run with a category, and one violated invariant with its keys. | - |

Reused unchanged from `ContainersTestTemplate`: container creation, `disconnectFromNetwork` / `reconnectToNetwork`,
Toxiproxy container and client, `waitForRaftLeader`, `compareAllDatabases`, `dumpContainerLogs`.

`DatabaseWrapper` is **not** reused for the workload: it logs and counts errors instead of surfacing each operation's
outcome, which the ledger needs.

Template change: an overload of `createPersistentArcadeContainer` that accepts heap and container memory limit, so a
5-node cluster fits on a 16 GB CI runner. Existing callers are unaffected.

## 3. Step loop and invariants

### Loop

```
warmup: create DB + schema, start Workload, wait until the first N ops are ACKED
repeat until duration elapsed or maxSteps reached:
  0. fail with AVAILABILITY if a node believed UP or DEGRADED exited on its own
  1. fault = weighted pick(random, catalog filtered by canApply(clusterState))
  2. log step start (fault, targets, leader, ledger counts)
  3. fault.inject()
  4. hold random(holdMin..holdMax), writers running
  5. availability check during hold (see below)
  6. fault.heal()
  7. converge: all nodes UP, leader elected, all nodes know it (bounded)
  8. checkpoint
  9. calm random(calmMin..calmMax) under load, no fault
final: stop Workload, full checkpoint, stop containers gracefully, compareAllDatabases()
```

All random decisions (fault choice, targets, hold/calm durations, writer-to-node routing) derive from `chaos.seed`.
Writer routing uses per-writer `Random` instances seeded from the master seed so thread scheduling does not perturb the
fault sequence. Replay reproduces decisions, not timing.

### Operation outcomes

Each op is attempted exactly once (no client retry):

- `ACKED`: 2xx response.
- `FAILED`: only a failure to establish the TCP connection (the request was never sent) or an authentication rejection
  (401/403). The classification is conservative: any error not positively known to be pre-append is `UNKNOWN`.
- `UNKNOWN`: timeout, connection reset after send, any other 4xx or 5xx, or any unclassified error. A no-leader or
  no-quorum rejection is `UNKNOWN` too: its HTTP status cannot prove the transaction was not appended to the Raft log.

### Checkpoint

1. **Quiesce**: writers stop at an op boundary; in-flight ops complete or time out.
2. **Converge**: poll every node with a local read (`arcadedb.ha.readConsistency` defaults to `eventual`, so a follower
   answers from its own applied state) for its `ChaosOp` and `NEXT` counts; wait until all nodes report equal counts on
   two consecutive polls. Not converging within the bound is a `SAFETY` violation (`CONVERGENCE`). Its
   `ledger-diff.txt` lists the keys that differ between the readable nodes' index scans, and each node's records read
   from the buckets in RID order (`SELECT @rid, id ... ORDER BY @rid`, bypassing the index) compared with that node's
   own index: a duplicate id, a record the index has no entry for, a record without an id, an index entry without a
   record. `count(*)` reads the buckets, so these explain a count mismatch that the index scans cannot see.
3. **Scan and diff**: page every key of every node, then re-read the counts. If they changed (a legal late commit
   landed during the scan) go back to step 2 within the same deadline. Otherwise any key that differs between node 0
   and another node is a `SAFETY` violation (`DIVERGENCE`). A 5xx while scanning is `SAFETY` (`SCAN_ERROR`: the node is
   up but cannot serve its own data); any other scan failure is `AVAILABILITY` (`SCAN`).
4. **Check** node 0's key set against the ledger:
   - **I1 no lost acks**: every `ACKED` / `ACKED_LATE` key is present.
   - **I2 no phantoms**: every present key exists in the ledger.
   - **I3 no resurrected failures**: no `FAILED` key is present.
   - **I4 unknowns resolve once**: each `UNKNOWN` key is resolved to `ACKED_LATE` (present) or `LOST_UNKNOWN` (absent).
     From then on a key once seen must never disappear (violation). A key resolved absent may still appear later
     without a violation: Raft may legitimately commit an entry from an earlier term that was in some logs but not yet
     applied at the checkpoint. It is then re-resolved to `ACKED_LATE` and counted in the report as a late commit.
   - **I5 atomicity**: for every atomic-pair op, the `NEXT` edge exists if and only if its op key exists. A violation
     means the transaction was applied partially, or the pair's target was not visible on the leader (stale read).
5. **Resume** writers.

### Availability checks

Reported as a separate category from safety:

- For faults with `expectsWritesAvailable()` (the cluster keeps a connected majority), the ACKED count must advance during
  the hold, after the `chaos.availabilityGrace` election grace period. The window after the grace is never shorter than
  25 s, so a writer blocked on a paused node for its 15 s read timeout cannot make the cluster look unavailable.
- `split` (leader in the minority) is verified the same way: ACKED progress during the hold proves the majority side
  elected a new leader and commits.
- A leader must be elected and known by every node within `chaos.electionTimeout` after each heal.
- A node the runner believes `UP` or `DEGRADED` that exited on its own (crash, OOM kill) is `AVAILABILITY`, checked at
  the start of every step and after a failed heal or leader wait. So is a restarted node that exits right after start.

### Failure handling

The first violation stops the run (fail fast): the report is written with the result category, containers' logs are
dumped via `dumpContainerLogs`, and the test fails with a message naming the invariant, the step, and the replay
command. Result categories: `PASS`, `SAFETY`, `AVAILABILITY`, `HARNESS` (the harness itself broke, e.g. the Toxiproxy
container died or Docker refused a command). Violation names within them: `I1`-`I5`, `CONVERGENCE`, `DIVERGENCE`,
`SCAN_ERROR` and `PAGE_COMPARE` (SAFETY); `SCAN` and runner-level availability failures (AVAILABILITY); `QUIESCE`
(HARNESS).

## 4. Fault catalog (v1)

"Minority" = at most 1 node for N=3, at most 2 for N=5.

| Fault | Mechanism | Targets | N=5 | Writes expected available |
|---|---|---|---|---|
| `kill` | `killContainerCmd` (SIGKILL), then start the same container | leader or follower (50/50) | 1 or 2 nodes | yes, after election |
| `stop` | `stopContainerCmd` (SIGTERM, 30 s grace), then start | leader or follower | 1 or 2 nodes | yes, after election |
| `rolling` | graceful restart of every node in turn, waiting for a leader between nodes | all | - | yes, mostly |
| `pause` | `pauseContainerCmd` / `unpauseContainerCmd` | leader or follower | 1 or 2 nodes | yes, after election |
| `isolate` | `disconnectFromNetwork` / `reconnectToNetwork` | leader or follower | 1 or 2 nodes | yes, after election |
| `split` | disconnect a minority set containing the leader | leader (+1 follower for N=5) | - | yes, new leader on the majority side |
| `latency` | Toxiproxy latency toxic on the node's Raft proxy, 200-2000 ms, with jitter | any node | 1 or 2 nodes | yes, slower |
| `loss` | Toxiproxy `limit_data` / `timeout` toxics, as `PacketLossIT` does | any node | 1 or 2 nodes | yes or degraded |

`pause` of the leader is the key case for stale-leader writes: after unpause the ex-leader must not commit anything the
new leader did not, which I1/I2/I5 detect.

**Toxiproxy wiring**: every node's Raft traffic always goes through the proxy
(`serverList = proxy:<raftProxyPort_i>:<httpProxyPort_i>,...`, as in `PacketLossIT`), so toxic faults can target any
node at any step without rebuilding the cluster. The proxy container is never a fault target; its death is a `HARNESS`
result.

**Majority rule** (`ClusterState.canImpair`): a fault is rejected if nodes already impaired plus nodes it would impair
exceed the minority. With one fault active at a time this mostly constrains the 2-node variants on N=5.

**Selection**: uniform over enabled faults by default; `chaos.faults=kill:3,pause:2,split:1` gives weights.

## 5. Workload

- `chaos.writers` threads (default 4), closed loop with a small think time: throughput self-throttles when the cluster
  slows down.
- Each op targets a node chosen by the writer's seeded `Random` among all nodes, regardless of their state: writes to a
  stopped node fail fast, writes to a paused one time out as `UNKNOWN`, writes to followers exercise leader forwarding.
- Schema: vertex type `ChaosOp` (`id LONG` with a `UNIQUE` index, plus `w`, `s`, `pair`); edge type `NEXT`. `id` is the
  ledger key; `w` and `s` repeat writer and sequence for humans reading a dump.
- Op mix:
  - 80%: single vertex `INSERT INTO ChaosOp SET id = :id, w = :w, s = :s, pair = false`.
  - 20%: atomic pair, one transaction creating the `ChaosOp` and a `NEXT` edge from it to a random earlier `ACKED` key.
- Every request has a bounded client timeout (15 s) so a paused node yields `UNKNOWN` instead of hanging a writer.

## 6. Configuration

| Property | Default | Notes |
|---|---|---|
| `chaos.seed` | random | Chosen value logged first and written in the report. |
| `chaos.nodes` | 3 | 3 or 5. |
| `chaos.duration` | `PT20M` | ISO-8601 duration. |
| `chaos.maxSteps` | unlimited | Whichever of duration / maxSteps is reached first ends the run. |
| `chaos.writers` | 4 | |
| `chaos.faults` | all | Comma list, optional `:weight`. |
| `chaos.holdMin` / `chaos.holdMax` | 10 s / 60 s | |
| `chaos.calmMin` / `chaos.calmMax` | 10 s / 30 s | |
| `chaos.convergenceTimeout` | 2 min | |
| `chaos.electionTimeout` | 60 s | |
| `chaos.availabilityGrace` | `PT20S` | Election grace before ACKED progress is required during a fault. |

The hold of a fault that expects writes to stay available is at least `chaos.availabilityGrace` + 25 s, whatever
`chaos.holdMin` says (section 3).

Nodes in the chaos run use `-Xms1G -Xmx1G` (1 GB heap) and a 2 GB container limit.

Local run:

```
mvn -Pdocker install -DskipTests
mvn verify -Pintegration -pl e2e-ha -Dit.test=HaChaosIT -Dfailsafe.excludedGroups= \
    -Dchaos.seed=42 -Dchaos.duration=PT10M
```

## 7. Report

Written to `e2e-ha/target/chaos/<seed>/`:

- `summary.json`: config, result category, steps completed, ledger totals per outcome, replay command.
- `steps.log`: one line per step - fault, targets, leader before/after, time to new leader, convergence time, ACKED during
  hold, ledger totals. A step that fails after its fault was injected, before its checkpoint, gets a
  `step=N fault=... targets=... FAILED kind=... message=...` line instead.
- `ledger-diff.txt` (on failure): up to 100 offending keys per violated invariant.
- `trends.csv`: one row per checkpoint - per-node container memory (docker stats, one shot), per-node `databases/` and
  `databases/.raft-storage` directory sizes (`replicationBytes`, from the host bind mounts), ACK throughput since the
  previous checkpoint. Report-only.
- `databases/arcadedb-<i>/` (whenever the final result is not `PASS`, including a `PASS` downgraded by
  `compareAllDatabases`): a copy of each node's data directory, taken after the nodes were stopped gracefully (SIGTERM,
  30 s grace, after unpausing a paused node), since the template's teardown deletes `target/databases`.

## 8. CI

- `e2e-ha/pom.xml` sets `<failsafe.excludedGroups>chaos</failsafe.excludedGroups>`. The root pom already feeds failsafe's
  `excludedGroups` from that property (#5697), so `ha-resilience-tests.yml` skips `HaChaosIT` unchanged.
- New `.github/workflows/ha-chaos-tests.yml`, build steps copied from `ha-resilience-tests.yml`:
  - `schedule`: weekly, Sunday 02:00 UTC, 3 nodes, `PT60M`, random seed.
  - `workflow_dispatch` inputs: `nodes`, `duration`, `seed`, `faults`, `writers`.
  - Runs `-Dit.test=HaChaosIT -Dfailsafe.excludedGroups=` plus the chaos.* properties; surefire runs the harness unit
    tests in the same invocation.
  - `timeout-minutes` comes from a timeout_minutes input (default 105 = the 60-minute scheduled run + 45); GitHub expressions cannot parse an ISO-8601 duration.
  - Uploads `e2e-ha/target/chaos/`, `e2e-ha/target/logs/`, failsafe reports.
  - Writes seed, result and replay command to `$GITHUB_STEP_SUMMARY`.

## 9. Testing the harness

- Unit tests, no containers (`*Test` classes run by surefire in the `e2e-ha` module, so every PR's unit-test lane runs
  them):
  - `Ledger`: outcome transitions, concurrent recording, `UNKNOWN` resolution.
  - `InvariantChecker`: synthetic key sets that violate each of I1-I5 individually and one that passes; each must
    produce exactly the expected violation.
  - `ChaosConfig`: parsing, defaults, rejection of invalid values (nodes not 3/5, min > max, unknown fault name).
  - Fault selection: the same seed yields the same fault/target/hold sequence; weights respected.
  - `ClusterState.canImpair`: majority rule for N=3 and N=5.
- Smoke: `HaChaosIT` with 3 nodes and `chaos.maxSteps=3` locally to verify end-to-end wiring (containers, faults heal,
  checkpoint passes).

Only `HaChaosIT` carries the `chaos` tag; the harness unit tests are plain `*Test` classes, untagged. The nightly HA job
runs with `-DskipTests`, so it skips them.

## 10. Known findings

The first smoke runs found a suspected follower index corruption: after a graceful restart under load (seed 42,
`kill,stop`) a restarted follower came back with extra `ChaosOp` rows and a corrupt `UNIQUE` index on `id`, and a
similar suspected corruption followed a rolling restart (seed 44). Suspected ArcadeDB product bugs, not harness
artifacts; to be filed and investigated separately.
