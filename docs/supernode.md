# Super-node write contention

Living design + development log for improving ArcadeDB's behaviour when many
transactions concurrently write edges into the **same hot vertex** (a
"super-node": a treasury/float account, a popular product, a hub in a
star schema). Target release: **26.8.1**.

Keep this document updated as the work progresses (design decisions, benchmarks,
follow-ups). Sections at the bottom track status and next steps.

---

## 1. Problem

In a graph with a power-law degree distribution a handful of vertices accumulate
a huge fraction of the edges. Every edge pointing at such a hub is prepended to
the hub's edge-list **head chunk** - a single record on a single page. Under
ArcadeDB's **page-level MVCC**, two transactions that append to that head chunk
concurrently collide: one commits, the other gets a
`ConcurrentModificationException` and retries the **whole transaction**.

Symptoms (reported from the field, payments workload, 3-node HA cluster):

- Leader-side processing time jumping from ~20-40 ms to **over a minute**,
  breaching a 1-minute application timeout.
- Followers healthy; the pathology is on the leader, where each retry re-runs the
  **entire Raft replication round**, so retries pile up into a latency cascade.

The contention scales with data growth: as the hub's edge list grows and hot
accounts get hotter, the collision probability per operation rises, so a workload
that was fine yesterday degrades today with no code change.

### Where it lives in the engine

- `EdgeLinkedList.add()` - prepends to `lastSegment` (the head chunk). O(1), but
  every concurrent appender hits the **same page**.
- MVCC conflict detection is **page-level** (`TransactionManager` /
  `TransactionContext.commit1stPhase` compare `currentPageVersion` vs
  `page.getVersion()`), so two appends to the same head-chunk page conflict even
  though they are logically independent.
- `ConcurrentModificationException extends NeedRetryException`, so
  `LocalDatabase.transaction(...)` retries the whole block.

---

## 2. Two independent levers

Contention on a super-node has two distinct costs, and they need two distinct
fixes:

1. **Wasted work / latency tail** - the retry storm. Each conflict throws away a
   whole transaction (and under HA a whole replication round) and redoes it.
   → Fixed by the **commutative edge-append merge** (this release).
2. **Throughput ceiling** - even with zero retries, a single hot chunk is a
   one-at-a-time version chain: every append must build on the latest committed
   version of that one page, so hub commits serialise.
   → Needs a **structural fix**: striped/sharded edge lists (follow-up).

The benchmarks in §6 quantify both: the merge collapses the retry storm and the
66-second latency tail, but raw super-node throughput stays ~45% below a
non-contended workload until the edge list itself is sharded.

---

## 3. Delivered: commutative edge-append merge

### Idea

Appends to an edge-list chunk **commute** - the order of two edges in a vertex's
edge list carries no semantics. So a commit-time page-version conflict whose
**only** cause is concurrent in-chunk appends can be resolved by **replaying this
transaction's appends on top of the newer committed page**, instead of failing
the whole transaction. This is the edge-list analogue of a striped/`LongAdder`
merge, applied at commit time.

### Config

`arcadedb.graph.edgeAppendMerge` (`GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE`,
`SCOPE.DATABASE`, default **true**).

### How it works

At `TransactionContext.commit1stPhase` (leader / embedded only), the page-version
check loop is wrapped: when `checkPageVersion` fails for a page whose every
modification in this transaction was a tracked in-chunk edge append, the page is
**rebased** instead of aborting:

1. Evict the stale page copy so the reload observes the current committed version.
2. Reload the chunk fresh from the bucket (bypassing the tx record cache, which
   still holds the stale, already-appended instance).
3. Re-apply this transaction's appends via `MutableEdgeSegment.add(...)`; if a
   chunk filled up in the meantime, fall back to a normal full retry.
4. Write the merged chunk back (`updateRecordNoLock`), re-check the version, and
   continue the commit. The merged page is what gets written to the WAL and
   replicated - so followers apply the merged bytes verbatim.

### Correctness guards

- **Leader / embedded only** (`commit1stPhase(isLeader)`). Followers apply the
  leader's already-merged WAL verbatim; they never rebase, so no HA divergence.
  The leader merges, then replicates the merged result.
- **Only pure-append pages are eligible.** Any non-commutative write **poisons**
  the page (excludes it): edge removal / relink (`EdgeLinkedList.updateSegment`),
  bulk `addAll`, and - crucially - **new-chunk allocation**. A brand-new chunk's
  page has no committed version containing that chunk, so rebasing it would target
  the wrong bytes; new-chunk poison is centralised in
  `LocalDatabase.createRecordNoLock` for any `MutableEdgeSegment`, covering
  `GraphEngine.createInEdgeChunk`/`createOutEdgeChunk` and the
  `EdgeLinkedList.add` chunk-full branch in one place.
- Tracking is lazy (only allocated when the feature is on and an append happens)
  and cleared on `reset()`/`kill()`.
- **Allocation-free hot path.** Appends are keyed by *segment* RID (a super-node
  is one segment however many edges it receives) with the (edge, vertex) pairs
  packed into growable primitive arrays (`EdgeAppendBuffer`), not one object per
  edge. Poisoned pages are held in a `LongHashSet` of packed `(fileId,
  pageNumber)` keys, so poisoning and the per-append skip check allocate nothing
  (no `PageId` objects); `PageId`s are materialised only on the rare commit
  conflict. A just-created chunk (e.g. a new source vertex's edge list) poisons
  its own page, so its appends are skipped and never cost a buffer. Measured
  overhead on a single-threaded 200k-edge insert: **+39 bytes/edge** vs
  feature-off (~0.7% on top of the ~5.3 KB/edge that edge creation already
  allocates), throughput within noise. This makes always-on cheap; the flag is
  kept for 26.8.1 purely as an operational kill-switch for new commit-path code.

### Touched files

| File | Change |
|---|---|
| `GlobalConfiguration` | `GRAPH_EDGE_APPEND_MERGE` flag |
| `TransactionContext` | append tracking + poison + `rebaseEdgeAppends` + commit-loop hook |
| `LocalDatabase` | central new-chunk poison in `createRecordNoLock` |
| `EdgeLinkedList` | register in-place appends; poison remove/bulk paths |
| `PageManager` | `edgeAppendMerges` stat |

---

## 4. Deferred: striped / sharded edge list (throughput)

To let concurrent appends to one hub run in parallel, a hub's edge list must span
**multiple pages/files** so writers don't all serialise on one head-chunk page.
ArcadeDB already keys edge buckets per vertex bucket (`getEdgesBucketName`), so
the files exist; the change is to let a *single hot vertex* keep a head chunk in
several of them and hash appends across the stripes.

Design guidance (from the initial review):

- **Adaptive, not blanket.** Degree is power-law: the vast majority of vertices
  have a handful of edges. Striping every vertex bloats the vertex header
  (2 → 2N pointers), scatters tiny chunks, and regresses the hottest read path
  (`out()`/`in()`/degree/`isConnectedTo` open N stripe iterators). Instead,
  **promote a vertex to a striped representation only when it crosses a degree
  threshold** (ConcurrentHashMap-treeify style) via a small "stripe directory"
  record; small vertices and old databases keep the current single-list format
  unchanged.
- **Per-bucket granularity** because MVCC conflicts are page-level: stripe across
  distinct files/pages so appends never false-share.
- **Deterministic hash by edge RID** so delete/lookup goes straight to the owning
  stripe.
- Read order changes (per-stripe order, merged) - confirm nothing guarantees edge
  iteration order and doc the change.
- Composes with the merge: merge kills the retry storm today; striping unlocks
  throughput.

---

## 5. Related pre-existing bug (separate)

**Issue #5147** - concurrent edge insertion into one super-node can **drop edges**
and occasionally orphan an edge-list chunk (`inEdgesHeadChunk not found` SEVERE at
`GraphEngine.createInEdgeChunk`). It reproduces with the append-merge **disabled**,
so it is pre-existing and independent; the merge actually *mitigates* it (fewer
retries = smaller race window). Hypothesis: the concurrent chunk-full transition
(new chunk + `previous` relink + vertex head-pointer update in
`EdgeLinkedList.add`) is not serialised as one MVCC unit. Needs its own fix. The
benchmarks below tolerate this with lenient assertions; the merge's own
correctness is gated by the deterministic `ConcurrentEdgeAppendMergeTest`.

---

## 6. Benchmarks

All numbers: 8 threads, tiny `txRetryDelay`, macOS dev box. Reproduce:

```
# Embedded (engine):
mvn -pl engine test -Dtest=SuperNodeConcurrentAppendBenchmark -DexcludedGroups=
mvn -pl engine test -Dtest=SuperNodeConcurrentAppendBenchmark -DexcludedGroups= -Darcadedb.graph.edgeAppendMerge=false

# 3-node HA (ha-raft):
mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups=
mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups= -DedgeAppendMerge=false
mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups= -DsuperNode=false   # control
```

### Embedded, 32,000 edges into one hub

| | full-tx retries | append merges | throughput |
|---|---|---|---|
| append-merge OFF | 11,298 (35.3%) | 0 | 9,898 edges/s |
| append-merge ON | **226 (0.7%)** | 31,446 | 11,038 edges/s |

Full-transaction retries collapse ~50×.

### 3-node HA (Raft), 4,000 edges into one hub

| Workload (ON unless noted) | throughput | full-tx retries | max commit latency | leader merges |
|---|---|---|---|---|
| Super-node | 52 edges/s | 135 (3.4%) | **408 ms** | 3,978 |
| Super-node, **OFF** | 52 edges/s | 14,002 (350%) | **66,697 ms** | 0 |
| Control (distinct targets) | **94 edges/s** | 2,950 (74%) | 489 ms | 0 |

Reading the HA numbers:

- **Latency tail:** OFF's worst commit takes **66.7 s** (a single edge insertion
  breaching a 1-minute timeout - exactly the field symptom). ON's worst is
  **408 ms**. This is the headline win.
- **Cluster load:** OFF pushed **18,002** attempts through the leader for 4,000
  successful commits; ON pushed **4,135**. Same 4,000 replicated, but **4.3× less**
  leader CPU / network / fsync.
- **Throughput is unchanged by the merge (52 → 52)** because a single hot chunk is
  a serialised version chain: each append rebases onto the latest version, so hub
  commits advance one at a time regardless of retries. The distinct-target
  **control hits 94 edges/s** on the same cluster, proving 52/s is a
  *super-node* ceiling, not an HA-replication ceiling. Closing that gap needs the
  striped edge list (§4).

### Verification

- `ConcurrentEdgeAppendMergeTest` (engine): correctness gate, deterministically
  clean at 32k concurrent edges (`check database` clean, no lost/duplicated).
- No regressions: MVCCTest, ConcurrentWriteTest, BasicGraphTest,
  DanglingEdgeIteratorTest, ACIDTransactionTest, Issue4940/4959,
  LocalTransactionCommitLockRetryTest.

---

## 7. Status

| Item | Status |
|---|---|
| Commutative edge-append merge (engine) | ✅ implemented, tested, benchmarked |
| Embedded + HA benchmarks | ✅ in `@Tag("benchmark")` tests |
| Issue #5147 (chunk-allocation race) | 🐞 filed, not started |
| Adaptive striped edge list (throughput) | ⏳ designed, not started |

## 8. Next steps

1. Prototype the **adaptive striped edge list** (promote-on-hot, per-bucket
   stripes, hash by edge RID) and re-run the HA benchmark to measure the
   throughput half.
2. Fix **#5147** (serialise the concurrent chunk-full transition; replace the
   silent "chunk not found → create new" fallback with fail-loud/retry).
3. Consider group-committing multiple pending hub appends into one replicated
   round (reduces the per-commit HA cost further).

## 9. Changelog

- **2026-07-08** - Root-caused the field issue (contention retries, not GC/
  compaction). Implemented the commutative edge-append merge behind
  `arcadedb.graph.edgeAppendMerge` (default on). Added embedded + 3-node HA
  benchmarks. Filed #5147 for the separate chunk-allocation race. Established via
  the HA control run that the remaining throughput ceiling is the single hot
  chunk, motivating the striped-edge-list follow-up.
- **2026-07-08** - Allocation-free rework of the append tracking (segment-keyed,
  primitive `EdgeAppendBuffer`, `LongHashSet` poison keyed by packed page id,
  lazy `PageId`). Per-append overhead dropped from ~+504 to **+39 bytes/edge**;
  merge behaviour and correctness unchanged. Added the single-threaded overhead
  micro-benchmark.
