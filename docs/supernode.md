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
| **Adaptive striped/sharded edge list (throughput)** | 🚧 **IMPLEMENTED on this branch - [#5156](https://github.com/ArcadeData/arcadedb/issues/5156)** (`GRAPH_SUPERNODE_THRESHOLD` default 128, `GRAPH_SUPERNODE_STRIPES` default 16) |
| **Remove-path anchoring cost optimisation** | ⏳ **TODO - [#5155](https://github.com/ArcadeData/arcadedb/issues/5155)** |

Implementation notes that refined the A.1 design (2026-07-10):

- **Stripe pool DDL is deferred on server/HA.** Creating the pool buckets inside the
  promoting user transaction made followers diverge (bucket-count mismatch in the
  3-node IT - the #4083 SCHEMA_ENTRY/TX_ENTRY ordering hazard, exactly as A.1
  warned). Embedded creates the pool inline (schema ops are local); a wrapped
  (server/HA) database hands creation to a one-shot helper thread outside any
  transaction and the vertex promotes at a later chunk-full once the pool exists.
- **Stripe hosts are a dedicated per-type bucket pool** (`<Type>_sn_stripe_<i>`,
  direction-blind), NOT the per-vertex-bucket edge files sketched below:
  `TYPE_DEFAULT_BUCKETS` defaults to 1, so reusing vertex-bucket files would give
  zero parallelism on default-configured types.
- **Iteration order is a REAL guarantee that striping breaks** (issue #689 asserts
  reverse-insertion order on `getVertices()` and is regression-tested with a
  10k-edge vertex). A promoted super-node cannot preserve global insertion order
  across stripes; it returns newest-generation-first as an approximation. The #689
  test now pins the classic layout explicitly; the trade-off must be called out in
  the release notes.
- **`GraphBatch` (bulk import) fails loud on a promoted vertex** instead of
  corrupting the directory via its direct head-pointer manipulation: bulk edge
  import into an already-promoted vertex throws with a clear message (disable
  promotion or use the standard API). Follow-up candidate.
- Promotion detection = chunk-full + geometric estimate as designed; at the 8 KB
  cap a one-time chain walk (cycle-guarded) honours thresholds above the cap
  estimate.
- **Stripes are PRE-WARMED at promotion** (all N first chunks created inside the
  promotion transaction), not lazily initialised: the 3-node IT showed lazy
  per-stripe init rewrote the directory once per stripe, each rewrite serialising
  concurrent appenders on the directory file's commit lock for a full replication
  round - a thundering herd that produced 5s LockTimeoutExceptions. Similarly, the
  append FAST PATH reads the directory WITHOUT anchoring its page (a stale stripe
  head is safe: the append lands mid-chain, valid for the unordered list) - the
  anchored fresh read happens only on slot writes (~1/1000 appends) - because an
  anchored-but-unmodified page still contributes its FILE to the commit lock set,
  which was re-serialising every striped append through the directory's file.
- **Engine bug found by the HA IT (pre-existing, fixed on this branch):**
  `TransactionContext.commit1stPhase` wrapped `LockTimeoutException` - which
  extends `NeedRetryException` and is designed to be retryable - into a
  NON-retryable generic `TransactionException`, so commit-lock contention failed
  transactions to the caller instead of retrying. The catch now rethrows every
  `NeedRetryException` as-is. This affects any heavily concurrent workload on
  main, independent of striping.
- **Cross-file commit publication is not atomic for uncoordinated readers**: a
  concurrent commit's directory page can become visible a moment before the
  stripe chunk page of the same commit (pages are published one at a time and a
  plain reader takes no commit lock), so a freshly-read stripe head RID can
  transiently resolve to "record not found". `StripedEdgeList` maps that
  `RecordNotFoundException` to a retryable `ConcurrentModificationException`
  (`loadStripeHead`), letting the transaction retry loop re-read a consistent
  view. Found by `Issue5147SuperNodeChunkRaceTest` under the default threshold.

### Two lost-update gaps found by the widened test net (2026-07-10)

Running the whole `com.arcadedb.graph` package (270 tests, incl. merge-disabled
concurrency runs) after the hot-vertex fix surfaced two silent lost-update gaps.
Both are of the same family as #5147: a WRITE based on a record buffer whose
page registration does not match the version the buffer was read at.

1. **Deferred-update erase inside one transaction.** Record updates are DEFERRED
   (`updatedRecords`, applied to pages only at commit), so re-reading a record
   raw from its page mid-transaction resurrects the pre-update state, and a
   subsequent `updateRecord` REPLACES the earlier deferred copy - silently
   erasing this transaction's own previous writes. Hit twice: the striped
   directory's slot updates (each stripe chunk-full erased all earlier slot
   updates in the same transaction - `InsertGraphIndexTest` lost 690/1000
   edges), and the classic head chunk in multi-append transactions. Rule now
   enforced: writers re-read their own deferred writes via
   `TransactionContext.getWrittenRecord()` - updated-records/mutable working
   copy ONLY, never the read-only record cache (a read-only cached copy may be
   one committed version older and would roll a concurrent change back).
2. **Pre-anchor read paired with a post-anchor version.** The factory's striped
   dispatch reads the head record BEFORE anchoring its page (deliberate - a
   directory anchor would re-serialise striped appends). If a concurrent commit
   publishes between that read and `anchorHeadChunkPage`, the transaction holds
   a ONE-VERSION-OLDER buffer at the FRESH page version: the commit-time MVCC
   check passes (versions match) and the stale buffer overwrites the concurrent
   append. Deterministically reproduced with merge disabled (classic hub lost
   ~140/32000 per run; the page-version write history shows the winner of
   version N+1 carrying version N-1's entry count). Fix: classic head loads and
   `loadChunkForWrite` re-read the chunk THROUGH the just-anchored page
   (bucket-level read, bypassing the record cache), after first preferring the
   transaction's own written copy per rule 1.

Also hardened: a transiently unresolvable head RID (a concurrent commit's pages
publish one file at a time; readers take no commit lock) now surfaces as a
retryable `ConcurrentModificationException` everywhere - the factory previously
"recovered" by RESETTING the head to a fresh chunk, orphaning the entire
existing list (observed: 145 edges gone in one hit). Merge-disabled concurrency
runs: 5/5 classic + 10/10 striped green after these fixes.

### The hot-vertex file lock: the REAL super-node serialiser under HA (found 2026-07-10)

With striping in place the 3-node benchmark refused to move: classic, 8 stripes
and 32 stripes all pinned at ~52 edges/s and ~153 ms average latency, while the
no-hub control did 104/s. A sampled trace of the commit lock sets showed why:
**the hub vertex's own bucket file was in the lock set of EVERY append
transaction**, held for a full replication round (p50 ≈ 19 ms) - 8 threads
serialising on one 19 ms lock is exactly 52/s, regardless of the edge-list
layout.

Root cause (pre-existing on main, NOT a striping bug):
`GraphEngine.connectIncomingEdge` (and the OUT twins) eagerly called
`vertex.modify()` before touching the edge list. For a vertex not already in
the transaction, `ImmutableVertex.modify()` reloads the record via
`getPageToModify(...)`, which anchors the vertex page - and an
anchored-but-unmodified page still contributes its FILE to the commit lock set.
The vertex record is only actually rewritten on a head flip (rare), first-chunk
creation (once) or promotion (once); every other append paid the lock for
nothing. This also explains why the 26.7.2 append-merge plateaued at ~52-57/s:
the merge removed the retry storm, but every hub append still queued one full
replication round on the hub vertex file.

Fix on this branch: the connect paths pass the immutable vertex down and
`modify()` happens only in the three paths that really rewrite the vertex
record, each guarded by a stale-head re-check (`modify()` reloads the record,
so a concurrently moved head is detected and surfaced as a retryable conflict
instead of silently orphaning the concurrent chunk - the MVCC protection the
eager anchor used to provide, without the per-append lock).

Measured effect (same build, same day, 3-node Raft, 8 writer threads x 500
edges into one hub, promotion transition excluded via single-threaded warm-up):

| run | throughput | avg / max commit latency | MVCC conflicts | full retries |
|---|---|---|---|---|
| classic hub, BEFORE fix | 52/s | 154 ms / 0.4 s | 4115 | 3.4% |
| striped 8, BEFORE fix | 51/s | 153 ms / 0.6 s | 2428 | 2.2% |
| striped 32, BEFORE fix | 52/s | 152 ms / 0.7 s | 857 | 2.9% |
| classic hub, AFTER fix | 58/s | 136 ms / 1.1 s | 4107 | 3.4% |
| **striped 8, AFTER fix** | **79/s** | **101 ms / 0.6 s** | 2395 | 5.1% |
| **striped 32, AFTER fix** | **120/s** | **66 ms / 0.26 s** | 923 | 3.7% |
| control (no shared hub) | 104/s | 76 ms / 0.3 s | 1892 | 47.3% |

With the hot-vertex lock gone and 32 stripes, the super-node workload
**out-throughputs the control**: stripes + append-merge absorb hub conflicts
with 3.7% full retries, while the control burns 47% full retries on its own
bucket collisions. 8 threads on 8 stripes still collide ~60% of the time
(birthday math), so the stripe count is the scaling lever; a follow-up could
size the default relative to expected writer concurrency (the generational
directory already supports growing the stripe count later).

---

# A. To do

## A.1 Adaptive striped / sharded edge list - throughput ([#5156](https://github.com/ArcadeData/arcadedb/issues/5156))

**Why.** Even with zero retries, hub commits serialise. Two independent mechanisms
cause it, and the second one is the dominant one under HA:

1. **Page-version chain**: every append lands on the *same* head-chunk page, so
   each commit must build on the previous committed version.
2. **Per-FILE commit locks held across the whole replication round.** On the
   leader, `commit1stPhase` acquires the commit locks of every touched *file* and
   they are released only at `reset()` after phase 2 - i.e. **across the full Raft
   quorum round-trip** (see `RaftReplicatedDatabase.commit`: phase 1 → replicate →
   phase 2 → reset; the #4936/#4937 semantics require the locks to stay held). All
   of a hub's IN edges live in ONE file (`getEdgesBucketName` = one `_in_edges` /
   `_out_edges` file per vertex bucket), so concurrent hub appends queue on that
   file's lock, **one quorum round at a time**.

Measured in the 3-node HA benchmark: appends into one hub cap at **~52 edges/s**
while a distinct-target control on the same cluster hits **~94 edges/s**.

**Consequence for the design (this supersedes the earlier "per-page" note): stripes
MUST live in different FILES, not merely different pages.** Page-level striping
inside one file would leave the file-lock serialisation intact and gain ~nothing
under HA - the deployment where the problem was reported.

**Idea.** Let a *single hot vertex* keep a head chunk in **several** edge FILES
(stripes) and hash appends across them, so concurrent appends take different file
locks and replicate in parallel. Reuse the **existing per-vertex-bucket edge
files**: stripe `i` of a hub in vertex bucket `b` lives in the edge file of vertex
bucket `(b + i) % typeBuckets`. No new file types, bounded file count, and edge
chunks are only ever reached by pointer (never scanned by file association), so
hosting another bucket's chunks is harmless.

### Dual-layout mechanics: how classic and striped coexist

**Today's placement invariant (why the hub serialises).** Edge-chunk files exist
per VERTEX BUCKET (`getEdgesBucketName`: `<vertexBucket>_out_edges` /
`_in_edges`), but a single vertex lives in exactly one bucket and its list never
leaves that bucket's file: the first chunk is created in the vertex's own bucket's
edge file (`GraphEngine.createIn/OutEdgeChunk`) and every later chunk is created
in the *same bucket as the previous chunk* (`EdgeLinkedList.add`, chunk-full
branch). So **for one vertex, the whole linked list per direction = exactly ONE
file, forever** - multiple buckets only spread *different vertices* across files,
never one vertex's list. (The edge *records* - the `Edge` type with properties -
are separate and do spread across the edge type's buckets; only the adjacency
chunk list is single-file per vertex+direction.)

**The discriminator is one byte that already exists.** The vertex record format
does NOT change - it keeps its two head-pointer RIDs. What changes is what kind of
record the pointer lands on; every record starts with a record-type byte and
`RecordFactory` already dispatches on it:

- head → record type **3** (`EdgeSegment`) = **classic layout** (today's path,
  untouched);
- head → record type **7** (`StripeDirectory`, new; 0-6 are taken) = **striped**.

No vertex-format bump, no flags, no migration: an old database contains only
type-3 heads and reads classic forever until a vertex naturally promotes.

**Classic layout** (every vertex below the threshold):

```
 VERTEX record  (vertex bucket b)                      format UNCHANGED
 ┌──────────────────────────────────────────────────┐
 │ type=1 │ OUT head RID │ IN head RID │ properties… │
 └──────────────────┬───────────────────────────────┘
                    │ IN head RID
                    ▼
        file "Account_b_in_edges"        ONE file for ALL this bucket's IN lists
 ┌───────────────────────────────┐  prev  ┌──────────────┐  prev  ┌─────────────┐
 │ CHUNK  type=3  (8KB)          ├───────▶│ CHUNK (4KB)  ├───────▶│ CHUNK (64B) │─▶ ∅
 │ [used][prev][e,v][e,v][e,v]…  │        └──────────────┘        └─────────────┘
 └───────────────────────────────┘          older entries            oldest
        ▲
        │ EVERY concurrent append writes THIS page, and takes THIS file's
        └ commit lock for a FULL Raft round → the 52 edges/s serialisation point
```

**Striped layout** (only after promotion):

```
 VERTEX record  (unchanged - same two pointers)
 ┌──────────────────────────────────────────────────┐
 │ type=1 │ OUT head RID │ IN head RID │ properties… │
 └──────────────────┬───────────────────────────────┘
                    │ IN head RID now lands on a DIRECTORY instead of a chunk
                    ▼
 STRIPE DIRECTORY  type=7   (small record, lives in "Account_b_in_edges")
 ┌───────────────────────────────────────────────────────┐
 │ type=7 │ N=4 │ slot0 RID │ slot1 RID │ slot2 │ slot3  │  ◀── the stripe-head
 └───┬──────────────┬─────────────┬───────────┬──────────┘      pointers live HERE
     │ slot0        │ slot1       │ slot2     │ slot3
     ▼              ▼             ▼           ▼
 Account_b      Account_b+1   Account_b+2   Account_b+3     ◀── 4 DIFFERENT files
 _in_edges      _in_edges     _in_edges     _in_edges           = 4 commit locks
     │              │             │             │               = parallel Raft rounds
 ┌───────┐      ┌───────┐     ┌───────┐     ┌───────┐
 │ CHUNK │─prev▶│ CHUNK │     │ CHUNK │     │ CHUNK │
 └───┬───┘      └───────┘     └───┬───┘     └───────┘
 ┌───▼───┐       (new chain)  ┌───▼───┐      (new chain)
 │ CHUNK │                    │ CHUNK │
 └───┬───┘                    └───────┘
 ┌───▼───┐  ◀── stripe 0 = the OLD chain, exactly as it was. Promotion moves
 │ CHUNK │      NOTHING: it only writes the directory and flips the head pointer.
 └───────┘
```

Where the pointers live: the stripe-head pointers are a **packed array of N RIDs
in the directory record's body** - a RID already encodes the file (its `bucketId`
IS the bucket/file id), so a "pointer to another file" is just an ordinary RID.
The directory itself lives in the vertex's own bucket's edge file, and it is
**almost immutable**: rewritten only when a stripe's head chunk fills (slot update,
~1 per ~1000 appends per stripe) or on a stripe's lazy first use (slots start as
null RIDs). It never becomes a hot page.

**Dispatch** (single fork point, everything downstream polymorphic):

```
        any edge-list operation on vertex V, direction D
                          │
             load record at V.headRID(D)      (this lookup happens today anyway)
                          │
              first byte = record type?
              ┌───────────┴────────────┐
           type 3                    type 7
       (EdgeSegment)            (StripeDirectory)
              │                        │
        EdgeLinkedList           StripedEdgeList
        (today's class,         (wraps N EdgeLinkedLists,
         zero changes)           one per non-null slot)
              │                        │
   ┌──────────┴─────────┐   ┌──────────┴──────────────────────────┐
   │ add(e,v): head     │   │ add(e,v):    stripe[hash(v)%N].add  │ ◀ different files
   │ iterate/count:     │   │ contains(v): stripe[hash(v)%N] only │ ◀ O(degree/N)
   │   walk one chain   │   │ iterate:     concat N iterators     │
   │ remove: walk chain │   │ count:       sum of N chain counts  │
   └────────────────────┘   │ remove(e):   stripe from endpoints  │
                            │ deleteAll:   all N chains + dir     │
                            └─────────────────────────────────────┘
```

Mechanically: extract a small `EdgeList` interface with the operations
`GraphEngine` already uses; `EdgeLinkedList` implements it unchanged;
`StripedEdgeList` delegates to per-stripe `EdgeLinkedList`s. The fork lives in
`GraphEngine.getEdgeHeadChunk()` (plus the two `connect*Edge` sites), which
becomes the type-byte factory.

**Promotion** (atomic, inside the transaction that trips it):

```
 EdgeLinkedList.add(e, v)
     │
     ├── head chunk has room ─▶ in-place append (hot path, unchanged, merge-tracked)
     │
     └── CHUNK FULL  ◀── the one place that ALREADY rewrites the vertex record
            │
            ├── cumulative < SUPERNODE_THRESHOLD
            │      └─▶ classic: bigger chunk, vertex.head = new chunk    (today)
            │
            └── cumulative ≥ threshold     (derived from the chunk-size doubling
                     │                      schedule - no counter, no walk)
                     ▼  PROMOTE, same tx:
                     1. directory: slot0 = old chain head, slots 1..N-1 = null
                     2. vertex.head = directory RID
                     3. append (e,v) via striped path (hash(v) → stripe j →
                        allocates stripe j's first 64B chunk, slot j filled)
```

Racers at promotion conflict on the vertex/head page exactly like a chunk-full
does today, retry, and then see the directory - a one-time event per vertex.

**Compatibility:**

| Database | Behaviour |
|---|---|
| Old DB, old binaries | classic only, nothing changes |
| Old DB on new binaries | classic; a vertex promotes only when it crosses the threshold on a write |
| `SUPERNODE_THRESHOLD=0` (disabled) | directories never created → file stays readable by older versions |
| DB with a promoted vertex | NOT readable by older versions (unknown type byte 7) - the one compat cost, paid only after a promotion happens |

**Components to touch:** `RecordFactory` (type 7), new `StripeDirectory` +
`StripedEdgeList`, `EdgeLinkedList` (promotion check in the existing chunk-full
branch), `GraphEngine.getEdgeHeadChunk` + the two `connect*` sites (factory),
`DatabaseChecker` (follow directories), append-merge unchanged (a stripe head is
just a chunk).

**Design guidance:**

- **Adaptive, not blanket.** Degree is power-law: the vast majority of vertices
  have a handful of edges. Striping every vertex bloats the vertex header
  (2 → 2N pointers), scatters tiny chunks, and regresses the hottest read paths
  (`out()`/`in()`/degree/`isConnectedTo` would each open N stripe iterators).
  Instead, **promote a vertex to a striped representation only when it crosses a
  degree threshold** (ConcurrentHashMap-treeify style) via a small "stripe
  directory" record; small vertices and old databases keep the current single-list
  format unchanged.
- **New config `SUPERNODE_THRESHOLD`** (e.g. default 128 edges, tunable; 0 =
  disabled) as the promotion barrier. Gates the striped *structure* only - the
  edge-append merge stays unconditionally on (its overhead is ~0.7% and contention
  is not a function of degree, so a low-degree-but-hot vertex still needs the
  merge).
- **Zero-cost promotion trigger: the chunk-full transition.** The chunk-full
  branch of `EdgeLinkedList.add` is the ONE place that already rewrites the vertex
  record (head-pointer update) and allocates a record - promotion there costs no
  extra write and no extra contention. The vertex's approximate degree is
  **derivable from the geometric chunk-size schedule** (64→128→…→8192 doubling:
  cumulativeBytes ≈ 2×currentChunkSize − 64 until the cap, then +8192 per chunk),
  so `SUPERNODE_THRESHOLD` maps to a chunk-size boundary with **no degree counter
  and no walk**. Past the 8 KB cap, every ~700-1000-edge chunk-full is a further
  promotion opportunity. Concurrent racers to promote: one wins, the others
  conflict-and-retry, then see the directory (one-time cost).
- **Directory record, new record type byte; stripe 0 = the existing chain.** The
  vertex's head pointer simply flips to point at a small directory record (packed
  array of N stripe-head RIDs; immutable after creation so it never becomes a hot
  page itself). The vertex record format is **unchanged**, promotion is O(1) with
  **no data migration**, and old databases read as before.
- **Deterministic hash by the NEIGHBOUR vertex RID** (the second element of each
  `(edgeRID, vertexRID)` entry), NOT the edge RID. `isConnectedTo`,
  `containsVertex`, `getFirstEdgeConnectedToVertex`, and `removeVertex` all key on
  the neighbour vertex, so hashing by it localises them to one stripe (O(degree/N));
  appends still spread because a hub's edges come from many distinct neighbours; and
  `deleteEdge` still resolves the stripe from the edge's endpoints. Hashing by edge
  RID would give zero read benefit on those hot paths. (Only a bare
  `containsEdge(edgeRID)` can't localise - uncommon.)
- **Reads merge N stripe chains**; degree = sum of per-stripe counts. Parallel
  stripe reads must use the `QueryEngineManager` pool (never the JDK common pool -
  see the concurrency rules in CLAUDE.md). **Edge iteration order changes**
  (per-stripe order, merged) - confirm nothing guarantees edge order and document
  it.
- **Composes with the merge**: merge kills the retry storm (and mops up residual
  intra-stripe conflicts); striping unlocks throughput. Both stay on.
- **Synergy with #5155**: stripes cut the remove-walk length to degree/N, which
  also shrinks the remove-path anchoring cost by the same factor.

**Alternatives considered and rejected** (re-examined 2026-07-09):

- **Cross-transaction write combining on the leader** (merge k in-flight hub
  appends into one page update / one Raft round). Attacks the observed bottleneck
  most directly, but requires either releasing the file locks before replication
  (speculative version chaining + cascading aborts) or merging several
  transactions' WAL identities into one entry - both deep, risky changes to the
  hard-won tx/WAL contract (#4936/#4937/#5064). Kept as the A.3 idea, only worth
  revisiting after striping.
- **LSM-style deferred hub append** (commit the edge record synchronously, apply
  the hub back-reference asynchronously from a batching worker). Moves the
  contention to the buffer's file/page unless the buffer is itself striped
  (converges to striping) and adds read-reconciliation + durability complexity.
- **Record-level MVCC for edge buckets.** Now clearly insufficient: the dominant
  serialiser under HA is the per-FILE commit lock, which is *coarser* than the
  page - finer conflict detection changes nothing there.
- **LongAdder-style contention-probed stripe selection** (append to whichever
  stripe is free). Perfect write spreading, but destroys the read localisation of
  hash-by-neighbour (edges to V could be in any stripe). Hash-by-neighbour already
  spreads statistically well for hubs (many distinct neighbours); keep probing only
  as a fallback if real-world skew is ever observed.

**Open questions / risks:**

- **Stripe count N**: fixed (8/16) to start, but effective parallelism is
  `min(N, typeBuckets, concurrent writers)` - a 1-bucket vertex type gains nothing
  under HA because there is only one edge file to stripe into. Document that hub
  types should keep the default bucket count (≈ CPU cores); optionally warn on
  promotion when `typeBuckets == 1`.
- **HA gain is bounded by the control (~1.8× here)**: striping lifts the hub to the
  cluster's independent-transaction throughput, not beyond it (that residual
  ceiling is Raft/fsync bound - the A.3 territory). The embedded gain is likely
  larger - **measure an embedded distinct-target control first** to size it.
- **Format/versioning**: the directory record type; old vertices and DBs read as
  single-list; promotion is lazy on write. Demotion (edge deletions shrinking the
  vertex below threshold) is NOT needed - once striped, stay striped.

**Acceptance:** concurrent appends to one super-node scale beyond the single-file
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
- **2026-07-09** - Design re-check of the striping proposal (A.1). Key discovery:
  the per-FILE commit locks are held across the entire Raft replication round
  (phase 1 → quorum → phase 2 → reset), so the dominant hub serialiser under HA is
  the **file lock**, not the page-version chain - stripes must live in **different
  files** (page-level striping would gain ~nothing under HA). Sharpened the design:
  reuse the existing per-vertex-bucket edge files as stripe hosts, zero-cost
  promotion trigger at the chunk-full transition (degree derived from the geometric
  chunk-size schedule - no counter), immutable directory record + stripe 0 = the
  existing chain (no vertex-format change, no migration). Re-examined and rejected
  four alternatives (cross-tx write combining, LSM-style deferred append,
  record-level MVCC, contention-probed stripes) - documented in A.1.
