# Super-node write contention

Living design + development log for ArcadeDB's behaviour when many transactions
concurrently write edges into the **same hot vertex** (a "super-node": a
treasury/float account, a popular product, a hub in a star schema).

**The retry-storm and data-loss halves shipped in 26.7.2** (PR #5148: commutative
edge-append merge + the #5147/#5153 lost-update fixes + a deterministic HA gate).
This document now leads with **what is left to do** (section A); the shipped work
is kept below as **reference** (section B).

## Status

| Item | Status |
|---|---|
| Commutative edge-append merge (`arcadedb.graph.edgeAppendMerge`, default on) | ✅ shipped 26.7.2 (#5148) |
| #5147 insertion lost-update (deferred-update MVCC gap) | ✅ fixed 26.7.2 (#5148) |
| #5153 removal-direction lost-update twin | ✅ fixed 26.7.2 (#5148) |
| Deterministic 3-node HA correctness gate | ✅ `SuperNodeAppendHAConsistencyIT` |
| **Adaptive striped/sharded edge list (throughput)** | ⏳ **TODO - [#5156](https://github.com/ArcadeData/arcadedb/issues/5156)** |
| **Remove-path anchoring cost optimisation** | ⏳ **TODO - [#5155](https://github.com/ArcadeData/arcadedb/issues/5155)** |

---

# A. To do

## A.1 Adaptive striped / sharded edge list - throughput ([#5156](https://github.com/ArcadeData/arcadedb/issues/5156))

**Why.** Even with zero retries, a single hot chunk is a one-at-a-time version
chain: every append to a vertex lands on the *same* head-chunk page, so hub
commits serialise. Measured in the 3-node HA benchmark, appends into one hub cap
at **~52 edges/s** while a distinct-target control on the same cluster hits
**~94 edges/s** - the single hot chunk costs ~45% of throughput independent of the
retry storm (which the merge already handles). Striping is the lever that closes
that gap.

**Idea.** Let a *single hot vertex* keep a head chunk in **several** edge buckets
(stripes) and hash appends across them, so concurrent appends hit different
pages/files and run in parallel. ArcadeDB already keys edge buckets per vertex
bucket (`getEdgesBucketName`), so the files exist.

**Design guidance:**

- **Adaptive, not blanket.** Degree is power-law: the vast majority of vertices
  have a handful of edges. Striping every vertex bloats the vertex header
  (2 → 2N pointers), scatters tiny chunks, and regresses the hottest read paths
  (`out()`/`in()`/degree/`isConnectedTo` would each open N stripe iterators).
  Instead, **promote a vertex to a striped representation only when it crosses a
  degree threshold** (ConcurrentHashMap-treeify style) via a small "stripe
  directory" record; small vertices and old databases keep the current single-list
  format unchanged (format-versioned).
- **New config `SUPERNODE_THRESHOLD`** (e.g. default 128, tunable) as the promotion
  barrier. Gates the striped *structure* only - the edge-append merge stays
  unconditionally on (its overhead is ~0.7% and contention is not a function of
  degree, so a low-degree-but-hot vertex still needs the merge).
- **Per-bucket / per-page granularity** because MVCC conflicts are page-level:
  stripe across distinct files/pages so appends never false-share.
- **Deterministic hash by the NEIGHBOUR vertex RID** (the second element of each
  `(edgeRID, vertexRID)` entry), NOT the edge RID. `isConnectedTo`,
  `containsVertex`, `getFirstEdgeConnectedToVertex`, and `removeVertex` all key on
  the neighbour vertex, so hashing by it localises them to one stripe (O(degree/N));
  appends still spread because a hub's edges come from many distinct neighbours; and
  `deleteEdge` still resolves the stripe from the edge's endpoints. Hashing by edge
  RID would give zero read benefit on those hot paths. (Only a bare
  `containsEdge(edgeRID)` can't localise - uncommon.)
- **Reads merge N stripe chains** (and can parallelise); degree = sum of per-stripe
  counts. **Edge iteration order changes** (per-stripe order, merged) - confirm
  nothing guarantees edge order and document it.
- **Composes with the merge**: merge kills the retry storm; striping unlocks
  throughput. Both stay on.

**Open questions / risks:**

- **Promotion detection without new contention.** Do NOT add a per-append degree
  counter on the vertex record - it would make every append rewrite the vertex
  record and reintroduce the very contention we are removing. Use an approximate
  signal computed where the vertex is already touched (e.g. at a chunk-full
  transition), not per edge.
- **Stripe count N**: fixed (8/16) to start; adaptive N (scale with degree) later.
- **Format/versioning + migration**: the "striped" marker + stripe-directory
  record; old vertices and DBs read as single-list; promotion is lazy on write.

**Acceptance:** concurrent appends to one super-node scale beyond the single-chunk
ceiling (target: close the 52→94 eps HA gap; re-run the benchmarks in §B.5); no
regression for sub-threshold vertices; correctness under concurrency (extend the
poison/anchor stress + HA consistency tests to the striped layout); existing
databases keep working without migration.

## A.2 Remove-path anchoring cost ([#5155](https://github.com/ArcadeData/arcadedb/issues/5155))

The #5153 fix makes `EdgeLinkedList.loadChunkForWrite` anchor (mutable-page fetch)
*every* chunk the remove walk visits, including read-only hops. Bounded (a
32k-edge hub is ~8 pages) and unmodified pages are dropped at commit, so **not a
correctness issue** - but it is wasted mutable-page churn on long chains.
`removeVertex` genuinely modifies many chunks; `removeEdge`/`removeEdgeRID` modify
only one, so the optimisation is: traverse read-only and anchor just the chunk
about to be modified. Measure on a large super-node before optimising.

## A.3 Group-commit hub appends (idea, not scoped)

Consider group-committing multiple pending hub appends into one replicated round
to cut the per-commit HA cost further. Speculative; revisit after striping.

---

# B. Shipped in 26.7.2 (reference)

## B.1 The problem

In a graph with a power-law degree distribution a handful of vertices accumulate a
huge fraction of the edges. Every edge pointing at such a hub is prepended to the
hub's edge-list **head chunk** - a single record on a single page. Under ArcadeDB's
**page-level MVCC**, two transactions that append to that head chunk concurrently
collide: one commits, the other gets a `ConcurrentModificationException` and
retries the **whole transaction**.

Symptoms (reported from the field, payments workload, 3-node HA cluster):

- Leader-side processing time jumping from ~20-40 ms to **over a minute**,
  breaching a 1-minute application timeout.
- Followers healthy; the pathology is on the leader, where each retry re-runs the
  **entire Raft replication round**, so retries pile up into a latency cascade.

The contention scales with data growth: as the hub's edge list grows and hot
accounts get hotter, the collision probability per operation rises, so a workload
that was fine yesterday degrades today with no code change.

**Where it lives:** `EdgeLinkedList.add()` prepends to `lastSegment` (the head
chunk) - O(1) but every concurrent appender hits the **same page**; MVCC conflict
detection is **page-level**; `ConcurrentModificationException extends
NeedRetryException`, so the whole `LocalDatabase.transaction(...)` block retries.

## B.2 Two independent levers

Contention on a super-node has two distinct costs:

1. **Wasted work / latency tail** - the retry storm (under HA, each retry re-runs a
   replication round). → Fixed by the **commutative edge-append merge** (§B.3).
2. **Throughput ceiling** - even with zero retries, a single hot chunk is a
   one-at-a-time version chain. → Needs the **striped edge list** (§A.1, TODO).

## B.3 Commutative edge-append merge (shipped)

**Idea.** Appends to an edge-list chunk **commute** - order carries no semantics.
So a commit-time page-version conflict whose **only** cause is concurrent in-chunk
appends is resolved by **replaying this transaction's appends on top of the newer
committed page**, instead of failing the whole transaction (the edge-list analogue
of a striped/`LongAdder` merge, at commit time).

**Config.** `arcadedb.graph.edgeAppendMerge` (`GRAPH_EDGE_APPEND_MERGE`,
`SCOPE.DATABASE`, default **true**; kept as an operational kill-switch).

**How it works** (in `TransactionContext.commit1stPhase`, leader/embedded only):
when `checkPageVersion` fails for a page whose every modification was a tracked
in-chunk append, the page is rebased instead of aborting - evict the stale copy,
reload the chunk from the current committed page (bypassing the tx record cache),
re-apply this tx's appends (`MutableEdgeSegment.add`), write back and re-check; if
a chunk filled up meanwhile, fall back to a full retry. The merged page is what is
written to the WAL and replicated, so followers apply the merged bytes verbatim.

**Correctness guards:**

- **Leader / embedded only** (`commit1stPhase(isLeader)`); followers never rebase,
  so no HA divergence.
- **Only pure-append pages are eligible.** Every non-commutative write **poisons**
  the page: edge removal/relink (`EdgeLinkedList.updateSegment`), bulk `addAll`,
  `deleteAll`, and new-chunk allocation (centralised in
  `LocalDatabase.createRecordNoLock`).
- **Bulk import (`GraphBatch`) neither tracks nor poisons** - safe only because a
  `GraphBatch` tx never also drives `EdgeLinkedList.add` on the same page (implicit
  invariant; any future mixing must poison those pages).
- **Allocation-free hot path**: appends keyed by *segment* RID with the pairs packed
  into primitive arrays (`EdgeAppendBuffer`); poisoned pages held as packed-long
  keys in a `LongHashSet`; `PageId` materialised only on the rare conflict. Measured
  overhead: **+39 bytes/edge** (~0.7%) vs feature-off, throughput within noise.

**Touched files:** `GlobalConfiguration` (flag), `TransactionContext` (tracking +
poison + `rebaseEdgeAppends` + commit-loop hook), `LocalDatabase` (central
new-chunk poison), `EdgeLinkedList` (register appends; poison remove/bulk paths),
`PageManager` (`edgeAppendMerges` stat).

## B.4 The two lost-update fixes (#5147, #5153) (shipped)

Surfaced while benchmarking: concurrent edge writes on one super-node could **drop
edges** - an edge record committed with no back-reference in the target's edge list
(`check database` → `missingReferenceBack`). **Pre-existing and independent of the
merge** (reproduced with it disabled).

**Root cause** (not a chunk-allocation race): a deferred-update MVCC gap. The chunk
was read via an immutable `lookupByRID` which (under READ_COMMITTED) does not retain
the page in the transaction; the page was captured only later by the deferred
`updateRecord` - at the *newer* version if a concurrent tx touched the same chunk in
between. The commit-time version check then compared the newer version against
itself, found no conflict, and the stale chunk buffer overwrote the concurrent
change.

**Fixes** (anchor the chunk's page in the tx at read time so the conflict is caught
and the tx retries; correct with the merge off too):

- **#5147 (insertion):** `GraphEngine.createInEdgeChunk`/`createOutEdgeChunk` anchor
  the head chunk via `fetchPageInTransaction`. Regression
  `Issue5147SuperNodeChunkRaceTest` - was losing ~130-160 edges/run, now exact.
- **#5153 (removal twin):** `EdgeLinkedList.removeEdge`/`removeEdgeRID`/`removeVertex`
  load each chunk they modify through `loadChunkForWrite` (anchors + reads from the
  anchored page), via the new `EdgeSegment.getPreviousRID()`. Regression
  `ConcurrentEdgeAppendMergeTest.concurrentAppendsAndRemovesStayConsistent`.

## B.5 Benchmarks

Reproduce (8 threads, tiny `txRetryDelay`):

```
# Embedded (engine):
mvn -pl engine test -Dtest=SuperNodeConcurrentAppendBenchmark -DexcludedGroups=
mvn -pl engine test -Dtest=SuperNodeConcurrentAppendBenchmark -DexcludedGroups= -Darcadedb.graph.edgeAppendMerge=false

# 3-node HA (ha-raft):
mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups=
mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups= -DedgeAppendMerge=false
mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups= -DsuperNode=false   # control
```

**Embedded, 32,000 edges into one hub:**

| | full-tx retries | append merges | throughput |
|---|---|---|---|
| append-merge OFF | 11,298 (35.3%) | 0 | 9,898 edges/s |
| append-merge ON | **226 (0.7%)** | 31,446 | 11,038 edges/s |

**3-node HA (Raft), 4,000 edges into one hub:**

| Workload (ON unless noted) | throughput | full-tx retries | max commit latency |
|---|---|---|---|
| Super-node | 52 edges/s | 135 (3.4%) | **408 ms** |
| Super-node, **OFF** | 52 edges/s | 14,002 (350%) | **66,697 ms** |
| Control (distinct targets) | **94 edges/s** | 2,950 (74%) | 489 ms |

The headline win is the **latency tail**: OFF's worst commit is 66.7 s (a single
edge insertion breaching a 1-minute timeout - the field symptom); ON's worst is
408 ms, with 4.3× less leader work. Throughput is unchanged by the merge (single
hot chunk = serialised version chain); the control's 94 eps proves 52/s is a
*super-node* ceiling, not an HA ceiling → §A.1 striping.

**Verification:** `ConcurrentEdgeAppendMergeTest` (correctness, incl. mixed
append+remove and merge-disabled), `Issue5147SuperNodeChunkRaceTest`,
`SuperNodeAppendHAConsistencyIT` (3-node consistency gate); no regressions in
MVCC/ACID/graph/tx suites.

## B.6 Changelog

- **2026-07-08** - Root-caused the field issue (contention retries, not GC/
  compaction). Implemented the commutative edge-append merge (default on) + embedded
  and 3-node HA benchmarks.
- **2026-07-08** - Allocation-free rework of the append tracking (segment-keyed
  primitive `EdgeAppendBuffer`, `LongHashSet` packed-page poison, lazy `PageId`):
  per-append overhead ~+504 → **+39 bytes/edge**.
- **2026-07-08** - Fixed **#5147** (deferred-update MVCC gap; head-chunk read-time
  anchoring) and the removal twin **#5153** (`loadChunkForWrite` +
  `EdgeSegment.getPreviousRID()`). Added the deterministic HA gate
  `SuperNodeAppendHAConsistencyIT`.
- **2026-07-08** - 5 rounds of code review addressed (rebase hardening,
  `deleteAll` poison, null-tolerant integrity asserts, merge-off regression test,
  `GraphBatch`/slot-reuse invariants documented). **PR #5148 merged into 26.7.2.**
  Filed follow-ups #5155 (remove-path cost) and #5156 (striping).
