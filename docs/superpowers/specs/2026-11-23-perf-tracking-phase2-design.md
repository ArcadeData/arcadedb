# Performance tracking phase 2: schedule, measurement and the missing workloads - design

Issue: #12433. Parent: #12427. Phase 1: `docs/superpowers/specs/2026-11-17-continuous-performance-tracking-design.md`.
Phase 3 (alerting, live since 2026-11-22): `docs/superpowers/specs/2026-11-21-perf-regression-alerting-design.md`. Inputs:
`docs/superpowers/specs/2026-11-21-perf-tracking-phase2-3-inputs.md` (the "inputs note" below; it lands with docs PR
#12876, and that version is authoritative).

## Intent

Phase 1 measures six workloads; the parent issue promises every major feature. Phase 2 adds **all** the missing parts at
once as a roadmap: the three workloads of #12433 (time series, HA, wire protocols), the ten candidates and five
measurement additions of the inputs note, and a schedule that has room for them. When every scenario exists, some of them
move to other machines; phase 2 only keeps that possible.

## Goals

- A schedule that fits everything: **S every weeknight**, **M every Saturday night**, **L on demand and for releases**.
- A run that loses nothing: a crashed server is restarted for the next case, a cancelled job still publishes the cases it
  finished, a host that was not quiet is recorded and not judged.
- Every workload of the parent issue and of the inputs note measured on that schedule, published to the dashboard and
  judged by the phase 3 gate without changes to either beyond what is listed here.

## Non-goals

- Other machines, cloud hosts, a second runner (later; results already carry `machine` in every line).
- Alert channels other than the phase 3 issue.
- Running the external load-test harness of the inputs note: its scenarios are re-written in the bench, with our own data.

## Anti-goals (a failure even if it works)

- A weeknight run that measures noise: an S case shorter than its own jitter is worse than no case.
- A schedule that silently drops a run: a pending run replaced in the `perf-01` concurrency group, a job cancelled at its
  timeout with nothing published.
- A workload that passes because it checks nothing: every correctness check must be able to fail (a test proves it).

## Constraints

- One machine, perf-01, one job at a time (`concurrency: perf-01`). The JVM and native variants run one after the other.
- Same runner, result format and export. A new metric is a `Metrics` entry with a unit the phase 3 rule knows (`ms`, `s`,
  `bytes`, rates, ratios); a new result field is added to the schema with `schemaVersion` raised and older files still read.
- Workload code lives in the `bench` module, reusing the `load-tests` support classes (`ContainersTestTemplate`,
  `TimeSeriesDatabaseWrapper`, `FacetDatabaseWrapper`, `ServerWrapper`). No new dependency unless strictly necessary.
- Every case is sized so that warm-up + reps fit the per-step limit (30 min) and the case cap (120 min) at every scale it
  runs at (#13095).
- The perf-infra repository is private: never named or linked from a public place (issues, PR bodies, docs in arcadedb).

## Approaches

| | Shape | Trade-off |
|---|---|---|
| **A (chosen)** | one roadmap spec, six projects delivered in order, each with its own plan and PR | the user asked for everything; one spec keeps the schedule, sizing and gate decisions consistent, separate PRs keep reviews small |
| B | one spec and one PR per workload, designed when started | decisions made six times, the schedule decided last although every sizing depends on it |
| C | one big PR | unreviewable; one bad workload blocks the rest |

The maintainer chose the scope (all missing parts) and the schedule; the order of the projects is my call, set so that each
project builds on the previous one.

## Design

### 1. Schedule (project 1)

| Lane | When (UTC) | Scale | Workloads | Job budget | Trigger |
|---|---|---|---|---|---|
| weeknight | Mon-Fri 04:30 | S | all bench workloads | 240 min | `nightly` |
| Saturday | Sat 20:00 | M | all bench workloads + LDBC + soak | 720 min | `nightly` |
| release | on tag | M then L (L: 3 reps, 1 warm-up) | all bench workloads (+ LDBC at M) | 720 min | `release` |
| on demand | dispatch | any | one or all | 720 min | `manual` |

- A **job budget** covers the whole job: both variants one after the other, plus LDBC where it runs.
- One `nightly.yml` with two cron entries; the scale comes from which schedule fired (`github.event.schedule`). Both use
  trigger `nightly`, so the dashboard charts them and the gate judges them with no change: scale is part of every line, an S
  point is only compared with S points, an M point with the previous Saturday's M point.
- `should-run.sh` already answers per scale, so an S and an M run of the same commit are both measured.
- LDBC moves to Saturday only (its dataset is fixed, it does not scale with S/M).
- Saturday 20:00 runs on the native image built that morning (01:41), so JVM and native still measure the same commit,
  and the 12-hour budget ends before Sunday's (absent) run and well before Monday 04:30.
- Consequences accepted: an M regression is reported with a week of commits as its range; a nightly M issue needs two
  recovered Saturdays to close itself (two weeks). The S lines catch the same regression the next weeknight.
- **Run identity in the gate gains the scale** for nightly runs (my call, the reviewer found it): `Planner.runKey` is
  `trigger:ref`, so a Friday S run and a Saturday M run of the same commit would share one event and one "already
  reported" mark, and the M results would never be commented. The nightly key becomes `nightly:<ref>:<scale>`; release keys
  stay `release:<tag>` (M and L of one release are one run, as phase 3 decided). A marker written before the change (no
  scale) reads as scale M, so the open issues keep working.
- **Saturday overflow** (my call): if M + LDBC + soak passes 720 min, first the soak runs on the JVM only, then Saturday M
  runs 3 reps and 1 warm-up as L does. The spec is updated with the measured reason either way.
- **No silent replacement in the queue** (my call): every workflow that runs on perf-01 starts with a short job on a
  GitHub-hosted runner that lists the repository's queued runs; if one is already waiting for perf-01, the new run fails
  with an error naming it, instead of replacing it. The scheduled lanes do not overlap (weeknight ends by 08:30, Saturday
  by 08:00 Sunday), so only dispatches and release tags can meet it.
- The M nightly history continues on Saturdays; the S lines start empty (the first S night has no baseline, as the gate
  already handles).

### 2. Measurement additions (project 1)

- **Server alive check and restart.** Before each case the runner checks the server answers (`/api/v1/ready`). If it does
  not, the runner reads why from Docker (exit code, `OOMKilled`), records it on the case that was running when it died
  (that case is `failed`, message `server died: OOMKilled` or the exit code), starts a fresh server with the same image and
  settings, and records `serverRestarted` on the next case. Workloads already rebuild their data in `prepare()` when the
  baseline is missing; the restart costs time, not cases. A second death in the same workload stops the workload (the rest
  `failed`, `not started: server died twice`), so a crash loop cannot eat the budget.
- **Results written as they come.** The run file is rewritten after every case (same `runId`), and the workflow's publish
  step runs `if: always()`, so a job cancelled at its timeout publishes the cases it finished. The file carries
  `complete: false` until the last case; the dashboard and gate read it like any other (a missing case is simply absent).
- **Stall watchdog.** When one client call has waited 40 s, the bench takes a thread dump of the server (JVM: `jcmd 1
  Thread.print` in the container; native: whatever the image supports, else nothing and a note) - at most 3 per run. Dumps
  are files next to the run (workflow artifact, kept 30 days); the case records how many were taken.
- **Latency histogram in the result.** Each latency case keeps its HdrHistogram (compressed, base64) per repetition, so reps
  and runs can be merged exactly later. `schemaVersion` raised; older files have none.
- **End-of-workload diagnostics.** At the end of each workload: `GC.class_histogram` top 30 and the live thread count (JVM;
  native: thread count only), stored under the workload's diagnostics in the run file, not as metrics.
- **Clock check.** During each case the bench compares the wall clock with the monotonic clock; a gap over 1 s means the host
  slept or was suspended. The case records `hostNotQuiet` with the reason. **Gate change:** a point marked `hostNotQuiet` is
  not valid (never a baseline, never judged), as the inputs note's rule 1 asks.
- **S sizing rule.** Every case at S must measure something: a timed operation (a build, an import) takes at least 5 s per
  repetition, a latency case runs at least 1,000 operations per repetition. Phase 1 S sizes that break the rule are raised
  (first suspects: vector build 20k, full-text build 50k); a unit test per workload asserts its S size against the rule's
  inputs where it can, and a first dispatched S run checks the rest.

### 3. Workloads

Each is a `Workload` in the bench (`name()`, `cases()`, `prepare()`, `measure()`), runs on both variants and at every
scale unless stated. Sizes are the builder's, under the S sizing rule and the case cap.

**Time series** (`timeseries`, project 2). Reuses `TimeSeriesDatabaseWrapper`.
- `ingest-line-protocol`, `ingest-sql`: points/s, batch p50/p95/p99.
- `range-query`: one sensor over a window; qps, p50/p95/p99.
- `aggregate-query`: `time_bucket` avg/min/max over a window; qps, p50/p95/p99.
- `aggregate-continuous`: the same aggregate read from a continuous aggregate, and the ingest rate while it is maintained.
- Correctness: the aggregate of a verification series is checked against its known min/max/count; a mismatch fails the case.

**Wire protocols** (`protocols`, project 2). The server of this workload enables the Bolt and PostgreSQL plugins and maps
their ports (other workloads keep today's server).
- Protocols: HTTP, gRPC, Bolt, PostgreSQL. Shapes (inputs note #2): point lookup, 1 hop, a 1,000-row page, `UNWIND` `MERGE`
  of 500, a 15 x 768 float parameter, a 10-statement transaction. Clients: 1 and 16.
- Case name `<shape>-<protocol>-<clients>c`; metrics ops/s, p50/p95/p99.
- **Parity:** the rows for a fixed set of keys must be identical on every protocol (values normalised by type); a mismatch
  fails the case of the protocol that differs from HTTP.
- openCypher on every protocol; where a protocol cannot carry a shape, the case is absent rather than faked.

**Correctness next to the numbers** (`correctness`, project 2).
- `vector-recall-after-churn`: delete and re-embed a share of the vectors, then recall@10 against brute force (gate as in
  the vector workload).
- `index-vs-scan-count`: count through an index vs a forced scan, on several predicates.
- `count-star-vs-iterated`: `count(*)` vs an iterated count after inserts and deletes.
- Each case reports its duration; a wrong answer fails the case.

**Filtered BM25** (cases of `fulltext`, project 2). The corpus gains a broad vocabulary share and a `deleted_at` property.
- `bm25-broad-or`: top 50 of OR queries matching about 45% of the corpus; `bm25-broad-or-filtered`: the same with
  `deleted_at IS NULL`. qps, p50/p95/p99.

**Issue scenarios** (`issues`, project 3). One case per fixed `performance` issue whose fix we want to keep.
- A case is either a **ratio** of two forms of the same query on the same server (median of 5 after warm-up; metric
  `ratio`, lower is better; failed above the issue's limit; a slow form under 5 ms is not judged) or a **fitted exponent**
  at n/4, n/2, n (metric `exponent`; limit 1.4 for linear, 0.4 for flat).
- First set: about ten scenarios chosen from closed `performance` issues, each case named `issue-<number>`; more are added
  one PR at a time. Ratios and exponents hold on a noisy host where absolute times do not.

**Open-loop ramp** (`ramp`, project 4). On the graph OLTP point lookup: arrivals on a fixed schedule, latency measured from
the scheduled start (no coordinated omission), the rate raised in steps of 25% until p99 over 50 ms, errors over 0.1% or
dropped arrivals. Metric `knee_per_s` (higher is better) and the p99 at the knee.

**HA** (`ha`, project 5). Three server containers on one Docker network (`ContainersTestTemplate` HA settings), the server
cores split in three cpusets, the client on its own cores. On one host the network is a loopback bridge: the numbers track
the engine's replication cost, not a real network, and the dashboard page says so.
- `write-throughput`: records/s and p95 through the leader at 1 and 8 writers.
- `replication-lag`: p50/p95/max ms from a commit on the leader to its visibility on each follower.
- **Write ledger:** every acknowledged write is looked up on every node at the end; an answer like
  `DatabaseIsReopeningException` counts as unknown (it may have landed), reported as a metric; a missing acknowledged write
  fails the case. **Digest:** a content digest per type compared across nodes; a difference fails the case.

**Mixed phase, hybrid retrieval, soak** (project 6).
- `mixed`: ingest, search, compaction, DDL, time-series inserts and vector churn at once; per operation p99 and the longest
  gap with no commit (ms, lower is better).
- `hybrid-rag`: large-k vector search, full-text and a 2-hop walk behind visibility filters, on skewed data with hub
  vertices; qps, p50/p95/p99, recall of the vector step.
- `soak`: the mixed phase for 2 hours, Saturday only, one repetition and no warm-up. A workload may declare its own reps,
  warm-up and step limit (a default method on `Workload`, overriding the run's), which the soak uses; the slopes are
  published as metrics (unit `ratio`, per hour) so the dashboard charts them, and over a limit the case is `failed`; least-squares slope of heap after GC (limit 5% of its mean per hour),
  of threads and of file descriptors (limit 10 per hour). Over a limit fails the case.

### Order and delivery

| # | Project | Repos | Depends on |
|---|---|---|---|
| 1 | schedule + measurement additions + S sizing | arcadedb (bench, gate), perf-infra | - |
| 2 | time series, wire protocols, correctness, filtered BM25 | arcadedb (bench) | 1 |
| 3 | issue scenarios | arcadedb (bench) | 1 |
| 4 | open-loop ramp | arcadedb (bench) | 1 |
| 5 | HA | arcadedb (bench), perf-infra (cpusets) | 1 |
| 6 | mixed phase, hybrid retrieval, soak | arcadedb (bench), perf-infra (soak on Saturday) | 2, 5 |

Each project gets its own plan and PR (or one PR per workload in project 2); a workload is added to the schedule by the PR
that adds it, after one dispatched S and M run shows it fits. After every project the weeknight S run must still end
inside 240 minutes; if it does not, the slowest cases move to Saturday only, and the spec is updated with the reason.

## Testing

- Unit tests in `bench` for every workload's case list, sizing and correctness checks, each proven able to fail (a wrong
  answer injected fails the case).
- Runner tests: a server that dies mid-case is restarted, the dying case is `failed` with the Docker reason, a second death
  stops the workload; the run file is rewritten after each case and readable when cut short; the clock check marks a
  simulated jump; the watchdog takes at most 3 dumps.
- Gate tests: a `hostNotQuiet` point is neither baseline nor judged; an S and an M nightly of the same commit make two
  events with their own ranges.
- Workflow: one dispatched run at S and one at M per project before its schedule entry is enabled; actionlint on every
  workflow change.

## Left to the builder

- The exact sizes per scale under the S sizing rule and the case cap.
- Field names of the new result entries (`complete`, `serverRestarted`, `hostNotQuiet`, histograms, diagnostics).
- Which ten issues form the first issue-scenario set, and each one's limit.
- How the native image is asked for a thread dump, if it can be at all.
- Dashboard tweaks the new workloads need (a note on the HA page, ratio/exponent units).
