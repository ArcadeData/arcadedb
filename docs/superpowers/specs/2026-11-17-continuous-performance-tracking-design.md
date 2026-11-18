# Continuous performance tracking - design

Date: 2026-11-17
Status: approved 2026-11-17

## Intent

ArcadeDB has load tests (`load-tests/`), microbenchmarks (`@Tag("benchmark")`, `benchmark-tests.yml`) and a graph
benchmark repo (`ldbc_graphalytics_platforms_arcadedb`), but nothing keeps their numbers over time: `load-tests` uploads
its JSON baselines as GitHub artifacts that expire after 7 days, the benchmark lane writes nothing durable, and LDBC
keeps `weekly.json` files on a laptop. Graph algorithms are covered, the rest of the product (vector, full-text,
faceting, time series, HA, wire protocols) is not.

The goal is a comprehensive, repeatable benchmark that exercises every major feature, runs on a regular schedule on
dedicated hardware, stores every result in an ArcadeDB server ("eat your own food"), and publishes the numbers
publicly for transparency.

**First goal: monitoring.** Charts and numbers that maintainers read and act on. Alerting is a later phase built on the
same data, so the data model must make it possible, but this design does not build it.

## Goals

- One benchmark runner covering ingestion, graph OLTP, graph analytics, vector, full-text, faceting, time series, HA and
  wire protocols, on both the JVM and the native image.
- Every run produces results in one fixed JSON format, with enough metadata (commit, image, machine, config) to make two
  data points comparable or to tell they are not.
- Results stored durably in an ArcadeDB results server on the tailnet; maintainers query it live.
- A public static dashboard plus the raw JSON, published after every run.
- Runs nightly on `main` and on every release tag, on a self-hosted runner.
- The workloads run identically on a developer laptop (`bench run ...`) and in CI.

## Non-goals

- Alerting and regression gating (phase after this one; the data model supports it).
- Competitor comparisons (the LDBC repo already does multi-vendor; this tracks ArcadeDB against itself).
- Benchmarking PRs before merge.
- Replacing `load-tests`: it stays a correctness/stability-under-load suite with pass/fail semantics.

## Anti-goals (a failure even if it works)

- A public endpoint to a live ArcadeDB server. Nothing on the tailnet is reachable from the internet.
- Numbers that move 2x run to run, so a chart shows noise instead of trends (the LDBC repo documents ~2x OLTP noise on
  shared hardware; dedicated hardware and medians of repetitions are what make the charts meaningful).
- Comparing numbers from different machines or configs on the same line without saying so.
- A vector "speedup" that is really a recall loss: vector results always carry recall.
- Fork PRs or public contributors able to trigger jobs on the self-hosted runner.

## Constraints

- Workload code in the public `ArcadeData/arcadedb` repo (transparency; anyone can rerun locally).
- Runner, workflows, Tailscale credentials and results-server credentials in a **private** repo. Only the private repo's
  workflows can target the self-hosted runner.
- Tailscale secures runner <-> results server. The runner reaches the results server only over the tailnet.
- Project rules apply: Java 21, `JSONObject`/`JSONArray` from `com.arcadedb.serializer.json`, Apache-compatible
  dependencies only, no fixed ports (bench starts servers via Testcontainers and reads back bound ports).
- Dashboard libraries must be Apache-2.0-compatible (e.g. Apache ECharts).

## Approaches considered

| # | Approach | Verdict |
|---|---|---|
| 1 | Extend `load-tests` ITs and make them emit results JSON | Rejected: JUnit/Failsafe owns warm-up, repetitions and parameters; results tangle with pass/fail |
| 2 | **New `bench` module: standalone Java runner, one class per workload, reuses `load-tests` support classes, emits one results JSON per run; uploader loads it into the results DB; adapter ingests LDBC results** | **Chosen** (by the maintainer): full control of warm-up/reps/params, same invocation locally and in CI, `load-tests` stays a test suite |
| 3 | Off-the-shelf tools (Gatling, k6, JMH) | Rejected: HTTP-centric, would need custom code for Bolt/Postgres/gRPC, several result formats to reconcile |

Execution host: **`perf-01`, a dedicated bare-metal machine (chosen by the maintainer)**, rather than EC2 via
`arcadedb-deployments`/`arcadedb-ami`. On EC2 every launch lands on a different physical host, with invisible
neighbours, EBS latency that varies, and no control over frequency. Launch-to-launch variance is typically a few
percent to over 10%, more on disk-heavy workloads, so the trend lines would show cloud noise. Metal instances avoid
that, but at about $500 a month. AWS remains an option for later needs a single box cannot cover (3-node HA over a
real network, Graviton, release-time cloud reference numbers), but it is not part of this design.

Publication approaches: **A. static export to GitHub Pages + raw JSON in a public repo (chosen)**; B. live read-only
public ArcadeDB endpoint (rejected: internet-exposed server, expensive-query abuse); C. raw JSON only (rejected: not
readable at a glance).

## Architecture

```
 public: ArcadeData/arcadedb                private: <org>/arcadedb-perf-infra (name: builder's call)
 ┌──────────────────────────┐               ┌──────────────────────────────────────────┐
 │ bench/  (runner+workloads)│◄── checkout ──│ workflows: nightly main, release tags,    │
 │ load-tests/ (support)     │   at commit   │   manual dispatch; runner labels; secrets │
 └──────────────────────────┘               └───────────────┬──────────────────────────┘
                                                            │ runs-on: [self-hosted, perf]
                                            ┌───────────────▼──────────────────────────┐
                                            │ self-hosted runner (dedicated machine)    │
                                            │ build image or pull native -> bench run   │
                                            │ -> results/*.json -> uploader              │
                                            └───────────────┬──────────────────────────┘
                                                 tailnet    │ HTTP (results user)
                                            ┌───────────────▼──────────────────────────┐
                                            │ results server: ArcadeDB, db `perf`       │
                                            │ maintainers query live (Studio over tailnet)
                                            └───────────────┬──────────────────────────┘
                                                            │ export job (same workflow, after upload)
                                            ┌───────────────▼──────────────────────────┐
                                            │ public: <org>/arcadedb-perf (name: builder)│
                                            │ data/**.json (raw) + GitHub Pages dashboard │
                                            └──────────────────────────────────────────┘
```

### 1. `bench` module (public repo)

- New Maven module `bench/`, not part of the default build lanes (skipped in regular CI like `load-tests`).
- Reuses `load-tests` support classes (`ContainersTestTemplate` container setup, `DatabaseWrapper`, `PrometheusScrape`,
  `FacetDatabaseWrapper`, `TimeSeriesDatabaseWrapper`). They live in `load-tests/src/test/java`; how bench consumes them
  (test-jar dependency vs. moving them to a shared support module) is the builder's call.
- CLI: `bench run --workload <name|all> --scale <S|M|L> --image <jvm|native|tag> [--reps N] [--warmup N] --out <dir>`.
  `bench list` prints workloads and their parameters.
- Each workload is one class implementing a small interface: `prepare` (schema + dataset), `warmup`, `measure`
  (returns metrics), `teardown`. The runner owns repetitions, warm-up, timing and the median.
- Server under test runs in a container (Testcontainers), with a fixed heap and CPU/memory limits recorded in the
  results. Each workload gets a fresh container unless it declares it can share one.
- Datasets are generated deterministically from a seed (no downloads) for ingestion, OLTP, faceting, full-text, time
  series. Vector uses a deterministic synthetic set by default; a public ANN dataset (e.g. SIFT/GloVe subsets) is the
  builder's call for phase 2 if synthetic recall proves unrepresentative.

### 2. Workload catalog

Core metrics for every workload: throughput (ops/s), latency p50/p95/p99, peak RSS of the server container, on-disk
size after load. Workload-specific extras listed below. Every metric carries a unit and a direction (lower/higher is
better), so later alerting needs no per-metric knowledge.

| Area | Workload | Extra metrics | Phase |
|---|---|---|---|
| Ingestion | Bulk import (CSV/JSONL via importer), transactional insert at 1/N threads | records/s, total time | 1 |
| Graph OLTP | Cypher and SQL: point lookup, 1-3 hop traversal, MATCH patterns | per-query latency | 1 |
| Graph analytics | PR, WCC, BFS, LCC, CDLP, SSSP on GAV | GAV build time | 1 (ingested from LDBC, not reimplemented) |
| Vector | Index build, kNN at fixed k | build time, **recall@k** (always) | 1 |
| Full-text | Index build, BM25 queries, hybrid vector+text | build time | 1 |
| Faceting | Facet queries over growing data | facet lag (from `FacetSnapshot`) | 1 |
| Time series | Ingest rate, range and aggregate queries | | 2 |
| HA | 3-node write throughput | replication lag | 2 |
| Protocols | Same small query mix over HTTP, Bolt, Postgres, gRPC | | 2 |

Every workload runs on both the JVM image (built from the commit) and the native image.

### 3. Results JSON format

One file per run (`results/<run-id>.json`), versioned by a `schemaVersion` field. Shape (builder may refine field names):

```json
{
  "schemaVersion": 1,
  "runId": "2026-11-17T03:00:00Z-1e1ad51768-jvm",
  "startedAt": "...", "finishedAt": "...",
  "trigger": "nightly | release | manual",
  "source": "bench | ldbc",
  "arcadedb": { "commit": "1e1ad51768", "branch": "main", "tag": null, "version": "26.12.1-SNAPSHOT",
                "image": "jvm | native", "imageDigest": "sha256:..." },
  "machine": { "id": "perf-01", "cpu": "...", "cores": 16, "memGb": 64, "os": "...", "kernel": "..." },
  "config": { "heap": "8g", "cpuLimit": 8, "memLimitGb": 16, "jvm": "...", "settings": { } },
  "results": [
    { "workload": "vector", "case": "knn-k10", "scale": "M", "params": { "dim": 768, "k": 10 },
      "reps": 5, "warmup": 2,
      "metrics": [ { "name": "throughput", "unit": "ops/s", "better": "higher", "median": 1234.5,
                     "values": [1220.1, 1234.5, 1250.0, 1199.0, 1240.2] } ],
      "status": "ok | failed | timeout", "error": null }
  ]
}
```

A failed or timed-out case is recorded with its status, never dropped: a missing point and a failed point read
differently on a chart.

### 4. Results server and data model

- ArcadeDB on the tailnet, database `perf`. It runs the latest release, not a nightly, so the store can't break along
  with the thing it measures. It is hosted on `perf-01` for now, and is down only while a benchmark is measuring
  (see section 6). Two users: `perf-writer` (insert only, used by the uploader) and `perf-reader`
  (read only, used by the export job and maintainers).
- Types (builder may refine):
  - `Run` document: the run-level fields above (commit, image, machine, config, trigger, timestamps).
  - `Measurement` document: one row per (run, workload, case, scale, metric) with `median`, `values`, unit, better,
    status, plus denormalized `commit`, `image`, `machineId`, `startedAt` for cheap time-ordered queries. Indexed on
    `(workload, case, scale, metric, image, machineId, startedAt)`.
  - Optional graph edges (`Run -> PREVIOUS -> Run` per machine+image) if it makes "compare with previous" queries
    simpler; builder's call.
- The uploader is idempotent on `runId`: re-uploading a run deletes its `Run` and `Measurement` rows and inserts the new
  ones in one transaction, never duplicates them. So `perf-writer` has create, read and delete rights on `Run` and
  `Measurement` in `perf` only, no schema or user rights. *(Designer's call.)*
- The raw JSON files are the source of truth; the DB can be rebuilt from them.

### 5. Public repo and dashboard

- Public repo `<org>/arcadedb-perf` holds `data/<yyyy>/<mm>/<run-id>.json` (the raw files, unchanged) and a static site
  on GitHub Pages.
- After upload, the export job queries the results server (read user) for the series the dashboard shows, writes
  `site/series/*.json`, commits raw + series to the public repo with a bot token scoped to that repo only.
- Dashboard: static HTML + Apache ECharts. Pages: overview (one sparkline per workload, latest vs previous release),
  per-workload page (time series per case/metric, JVM and native side by side, release tags marked on the x-axis,
  click a point to see commit + raw JSON link). One line per (machine, config); different machines never share a line.
- Internally, maintainers use Studio over the tailnet for ad-hoc queries; no extra internal dashboard in phase 1.

### 6. Runner and scheduling (private repo)

- One dedicated physical or bare-metal machine as the self-hosted runner, labeled `[self-hosted, perf]`, doing nothing
  else during runs. Turbo/frequency scaling pinned where the OS allows; the settings go into `machine` in the results.
- Workflows: nightly, on each release tag, and manual dispatch with workload/scale/commit inputs. One run at a time
  (`concurrency` group per machine). *(Designer's calls below.)*
  - **JVM and native measure the same commit.** The nightly runs after `native-image.yml` has published
    `latest-native`. It reads the commit the native image was built from, builds the JVM image from that same commit,
    and skips the run if that commit was already measured. `native-image.yml` therefore gains an
    `org.opencontainers.image.revision` label, because today the image carries no version label. A release run uses
    that release's published JVM and native images. The native variant is the one published for the runner's
    architecture.
  - **Time budget:** a nightly run must finish within 3 hours. Nightly runs scale `M` with 5 reps and 2 warm-ups. Scale
    `L` runs only on release tags and manual dispatch. Dataset sizes per scale are picked to fit that budget.
- Repos: both the private infra repo and the public results repo live in the `ArcadeData` org. The runner is registered
  at the private repo level, so no other repo can target it. The export job pushes to the public repo with a GitHub App
  (or fine-grained PAT) that can only write to that repo. *(Designer's call.)*
- The private repo's workflows are the only ones allowed to target the runner; the public repo has no workflow that does.
- Tailscale: the runner and results server join the tailnet with tagged auth keys; ACL lets only `tag:perf-runner` write
  to the results server and `tag:maintainers` read it.
- LDBC *(maintainer's decision)*: the ArcadeDB-only Graphalytics run (Mode 1, embedded) moves onto the perf runner.
  It runs nightly at scale `M`, built against the same commit as the bench run. The LDBC repo gains an
  `arcadedb.version` override so it can build against a locally installed snapshot of that commit. Its output goes
  through an `ldbc` adapter in `bench` to the results format (`source: "ldbc"`, the runner's `machine`) and is
  loaded by the same uploader. It is JVM-only: Graphalytics runs embedded, so there is no native leg, and the
  dashboard says so. The multi-vendor comparison stays on the maintainer's laptop and is not tracked.
- Hardware (provided by the maintainer): `perf-01`, bare metal (`systemd-detect-virt`: none), Ubuntu 24.04.4 LTS,
  x86_64, Intel Core i5-13500, 62 GB RAM, 31 GB swap, two Micron 3400 512 GB NVMe in an md array (`/dev/md2`, 170 GB
  free), Docker 29.7.1. Tailscale is not installed yet; it is added when the upload path is first tested.
  Provisioning (scripted, idempotent) *(designer's calls)*:
  - **Hybrid CPU.** The i5-13500 has 6 performance cores (12 threads) and 8 efficiency cores. If the scheduler moves a
    benchmark between the two kinds of core, the numbers swing for no reason. The server-under-test container is
    therefore pinned with `--cpuset-cpus` to the P-core threads, which the provisioning script detects from
    `lscpu --extended` rather than hard-coding. The bench client JVM is pinned to the E-cores, so the client never
    steals CPU from the server. The CPU sets are recorded in `config`.
  - **Frequency.** The governor is `powersave` today. Provisioning sets it to `performance` and disables turbo
    (`intel_pstate/no_turbo=1`) for the duration of a run, then restores both. The values are recorded in `machine`.
  - **Swap.** The server-under-test container runs with swap disabled (`--memory-swap` equal to `--memory`), so a run
    that runs out of memory fails visibly instead of slowing down silently.
  - **Idle check.** About 11 GB of RAM was in use when the machine was inspected. Before measuring, the runner records
    load average and memory in use and fails the run if the machine is not idle, so numbers from a busy machine are
    never stored as valid.
  - **Results server.** Unless a second host shows up, it runs on `perf-01` in its own container, pinned to the
    E-cores. The runner stops it while a benchmark is measuring and restarts it for the upload and export steps.
  - Docker and the GitHub runner service; Tailscale with a tagged auth key once the upload path is tested.

## Phasing

1. **Phase 1:** `bench` module + format + uploader; workloads ingestion, graph OLTP, vector, full-text, faceting; LDBC
   adapter; results server; private-repo nightly workflow; public repo with raw JSON and dashboard.
2. **Phase 2:** time series, HA, protocols; release-tag backfill of the last few releases.
3. **Phase 3 (separate design):** alerting/regression detection on the stored data (the LDBC `check_regressions.py`
   logic is the starting point).

## Left to the builder

- Module/repo names, CLI flag spelling, exact JSON field names (keeping the shape and `schemaVersion`).
- How `bench` consumes the `load-tests` support classes.
- Dataset sizes for each scale, chosen so a full nightly run fits the time budget.
- Dashboard layout details.
- Whether to add the `PREVIOUS` edges.
