# Continuous performance tracking: inputs for phase 2 and phase 3

Parent: #12427. Phase 2: #12433. Phase 3: #12434. Design of phase 1:
`docs/superpowers/specs/2026-11-17-continuous-performance-tracking-design.md`.

This note collects what the next two phases should start from: what phase 1 taught us on perf-01, and what a contributor's
external load-test harness for ArcadeDB does that the `bench` module does not. It is an input, not a design: phase 3 still
needs its own design and phase 2 its plan.

## What phase 1 taught us

- **The case cap binds at L.** A case runs warmup + reps times inside one 30-minute cap. At L, 7 builds of a 1M-vector
  index (about 140 to 200 s each) do not fit; the release L step now runs 3 reps and 1 warmup. The engine build is linear
  (embedded: 200k in 12 s, 1M in about 90 s). Any new workload must be sized against the cap at every scale it runs.
- **The nightly is at its 3-hour budget.** M took 2h55 and 3h16 (the overrun is the native `fulltext/build` timeout,
  #12839). Phase 2 workloads cannot simply be added to the nightly.
- **A failure must not cascade.** The bench does not restart a server that died, so one crash failed the next cases of the
  same workload (26.11.1 native L: bulk-import OOM, then tx-insert could not connect). Each case should check the server is
  alive before it starts.
- **Synthetic data must look like real data.** Uniform 128-d vectors gave recall 0.24 at M; embedding-like vectors (64
  clusters, 16 latent dimensions) give 0.995. The same caution applies to text: our BM25 queries are selective, real ones
  are not (see the filtered BM25 shapes below).

## Phase 3: regression alerting

### How the external harness judges a change

| Tool | Rule | Gap |
|---|---|---|
| `compare` (two runs) | `rel = (new - base) / abs(base)`, sign flipped for higher-is-better; worse when `rel > 0.15`, one threshold for every metric | no noise model; a metric whose base is 0 is never judged, so an error count going from 0 to N passes |
| `trend` (a run log) | each run against the **previous valid run** of the same label; a run is invalid when the host was loaded or a correctness check failed; a change is shown above 10% | fixed band |
| `report` (pooled rounds) | mean per label, spread = max / min - 1; flagged only when `abs(ratio - 1) > max(spread_base, spread_new, 0.10)`; a noisy round is dropped when a quieter one exists | none worth noting |
| issue and probe gates | the ratio of two forms of the same query on the same server (median of 5 after warm-up), fail above a factor; a slow form under 5 ms is not judged; or a log-log exponent fitted at n/4, n/2, n (at most 1.4 for linear cost, 0.4 for flat) | not a trend, a fixed expectation |

Their host was often at a 1-minute load of 50 to 250 and a same-build control arm reached 1.4x on single statements:
ratios and exponents held, absolute times did not.

### Starting point for our rule

1. Compare each point with a baseline of **valid** runs of the same line (machine, variant, scale, workload, case, metric):
   the previous valid nightly, or the median of the last N valid ones. A run is invalid when its idle check, or a check
   during the run, says the host was not quiet, or the case failed.
2. Flag a change only when it exceeds the **noise band** `max(spread of the line, floor)`, with the spread taken from our
   own repetitions (we keep every rep's value) and the floor a per-metric minimum (10% to start).
3. Add an **absolute floor** per unit (a latency under a few ms, a count of 0) so small and zero bases are judged by an
   absolute change, not a ratio.
4. Treat status separately from value: `ok` to `failed` or `timeout` is always an alert; a recall below its gate already
   fails the case.
5. Record a **control**: one cheap, stable case run every night whose drift measures the host, not the engine.
6. Keep the decision in data: an alert names the line, the two values, the band and the runs compared, so a reader can
   check it without rerunning.

Open questions for the design: where alerts go (an issue per regression, a comment on the commit range, a daily
summary), how a known regression is acknowledged so it stops alerting, and whether the gate runs in the nightly workflow
or in a separate job on the exported data.

## Phase 2: workload candidates

Ordered by value for the cost. "Lane" is where it fits: the nightly (M, 3-hour budget), the release run, or a stress lane
that does not exist yet (weekly, longer budget).

| # | Candidate | What it measures | Lane | Effort |
|---|---|---|---|---|
| 1 | **Issue scenarios**: one scenario per `performance` issue, judged by a ratio of two forms or a fitted exponent | regressions of fixed performance bugs, robust on a noisy host; the external set has 95 (26 still failing on 26.12.1-SNAPSHOT) and runs in about 150 min at a quarter of its scale | nightly, a subset | S each |
| 2 | **Protocol parity**: the same openCypher over HTTP, gRPC and Bolt (PostgreSQL to add): point, 1 hop, a 1000-row page, UNWIND MERGE of 500, a 15 x 768 float parameter, a 10-statement transaction, at 1 and 16 clients | per protocol and shape: ops/s, p50/p95/p99; the rows of fixed keys must match on every protocol | nightly | S-M |
| 3 | **Correctness next to the numbers**: recall after deletes and re-embeds; an index count against a forced-scan count; `count(*)` against an iterated count | a fast wrong answer is not a speed-up | nightly | S |
| 4 | **Filtered BM25**: top 50 of OR queries matching about 45% of the corpus, plain and with `deleted_at IS NULL` | on 750k documents: about 210 ms plain and about 2 s filtered (the filter gets no limit and ORDER BY drains about 340k matches); a planner change brings it to about 9 ms | nightly | M |
| 5 | **Open-loop ramp**: arrivals on a schedule, latency from the scheduled start (no coordinated omission), the rate raised in steps until p99, errors or drops break a limit | `knee_per_s`, one capacity number per workload; our closed loop understates tail latency at saturation | nightly, one workload | M |
| 6 | **HA**: 3-node write throughput and replication lag, with a write ledger (every acknowledged write looked up on every node; an answer like `DatabaseIsReopeningException` counts as unknown, it may have landed) and a content digest per type across nodes | the lag number is only meaningful with both checks | release, then nightly if it fits | L |
| 7 | **Mixed phase**: ingest, search, compaction, DDL, time-series inserts and vector churn at once | convoy effects (#12098 DDL against commits, #12251 a continuous aggregate against inserts) and the longest gap with no commit | release / stress | M |
| 8 | **Hybrid retrieval turn**: large-k vector search, full-text and a 2-hop walk behind visibility filters, on skewed data with hub vertices | the RAG application shape our single-feature workloads miss | release | M-L |
| 9 | **Time series**: ingest rate, range and aggregate queries, with and without a continuous aggregate | as in the phase 1 design | nightly at S | M |
| 10 | **Soak**: hours of the mixed phase; least-squares slope of heap after GC (limit 5% of its mean per hour), of threads and file descriptors (limit 10 per hour) | leaks we never see in a 30-minute case | stress | S once the mixed phase exists |

### Measurement additions

Cheap, and useful to every workload:

- check the server is alive before each case; restart it (and say so) when it died
- a stall watchdog: a thread dump of the server when a call has waited 40 s (at most 3 per run)
- keep the latency histogram in the result, so runs and reps can be merged exactly
- `GC.class_histogram` top 30 and the threads started at the end of each workload
- a clock check during the run (wall clock ahead of the monotonic clock means the host slept)

## Findings from the external harness worth tracking

- #12481: `RID` and `PageId` hash codes cluster; about 42% of BM25 top-50 CPU is in HashMap tree-bin operations, a better
  hash gives about +27% at 8 clients (it changes iteration order seen by 5 engine tests).
- #12350: 100 rows of 768 floats sent as SQL literals filled a 6 GB heap through the statement cache; as a parameter they
  do not.
- Bolt: abandoned connections keep their threads (about 330 extra per node), and a message over 16 MiB is never answered.
- Commit gaps of 2 to 7 s with 8 writers and 6 bulk loaders on 3 nodes, with periodic snapshots off; cause not found.
- Open issues whose scenario passes on 26.12.1-SNAPSHOT (commented on each): #11701, #11822, #11978, #12355, #11932,
  #11989, #12127.
