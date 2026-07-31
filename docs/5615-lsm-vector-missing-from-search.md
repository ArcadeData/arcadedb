# #5615 - LSM vector index: a committed vector is intermittently missing from search

Issue: https://github.com/ArcadeData/arcadedb/issues/5615

## Root cause

A graph rebuild occasionally emits an **orphan node**: a node that is present in the graph and carries a full
set of outgoing edges, but has **zero incoming edges**. Beam search starts at the entry node and only ever
follows edges forward, so an orphan can never be visited - at any `efSearch`, however good its score. The
vector itself, the location index, the ordinal map and the scoring path are all correct, which is why every
post-hoc diagnostic looked clean and why nine earlier theories all failed.

## Evidence

Reproducer: `LSMVectorIndexConcurrentRebuildVisibilityTest` (4 concurrent indexes,
`VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD=8`, inactivity rebuild 1 ms). Instrumented to walk the graph at the
moment of a miss, it reported:

```
batch 15: #1:8382 -> #1:6156
  retryHit=false wideEfHit=false
  rankProbe: expectedOrdinal=1500 expectedScore=1.0 betterScoring=0 bestOrdinal=1500 bestScore=1.0
             graphSize=1600 mapLen=1600
  reach:     entryNode=1050 visited=1599 ofUpperBound=1600
             targetReachable=false targetInGraph=true targetOutDegree=32 targetInEdges=0
```

- `expectedScore=1.0`, `betterScoring=0` - scored through exactly the `ArcadePageVectorValues` the search
  builds, the missing vector is the **globally best-scoring node**. The scoring input is not the problem.
- `graphSize == mapLen` - graph and ordinal map agree, so this is not a publication tear.
- `targetInEdges=0`, `visited=1599 of 1600` - BFS from the entry node reaches every node except this one.
- `retryHit=false`, `wideEfHit=false` (efSearch=2000 against a 1600-node graph) - the miss is **stable**, not
  a transient race in the read path. That single bit is what eliminated the remaining concurrency theories.

A second probe placed directly after `builder.build(vectors)` closed the causal chain. On a failing run:

```
PROBE5615 ORPHANS index=Vec3_0_... count=3 of 800 upper=800 sample=252(deg=32) 257(deg=32) 258(deg=32)
...
batch 7: #10:250 -> #10:65 ... expectedOrdinal=258 ... targetInEdges=0
```

The build reported three orphans; the search then missed **ordinal 258**, one of those exact three. Passing
runs reported zero orphan builds.

Note `GraphIndexBuilder.build()` does call `cleanup()` (verified in the 4.0.0-rc.7 bytecode:
`submit(addNodes) -> join -> cleanup`), and cleanup has connectivity machinery. It nonetheless leaves these
nodes unreferenced under concurrent rebuild pressure.

## The fix

`LSMVectorIndex.findUnreachableOrdinals` walks every edge from the graph entry node after a build and returns
the ordinals nothing reaches. `buildGraphFromScratchExclusively` re-queues those vectors into the delta buffer,
where `mergeWithDeltaScan` - an exhaustive linear scan - keeps them searchable until the next rebuild wires
them into the graph.

Two details worth noting:

- The walk and the vector reads run **before** the write lock is reacquired, so an O(V+E) traversal never
  stalls concurrent searches.
- Vectors at or past `deltaSnapshotId` are skipped: the existing trim already carries them over, and re-adding
  them would score the same vector twice in the delta scan.

Reachability is walked from the entry node rather than counted as in-degree, because a disconnected cycle gives
every node an in-edge while remaining unreachable.

## Theories eliminated (do not re-walk)

In addition to the six already recorded on the issue:

1. **Torn read of `graphIndex` inside the search** (read at several points after `ordinalToVectorId` is
   snapshotted). Dead: `graphSize == mapLen` on every failure, and the miss survives a retry.
2. **Stale searcher handed back across a graph swap.** `GraphSearcherPool.borrow` calls `drain()` *before*
   publishing `pooledGraph`/`pooledEpoch`, so a concurrent `release` reading the still-old matching pair can
   re-pool a searcher bound to the old graph. A **real race, filed as #5648**, but not this bug - the
   reproducer has one searching thread per index.
3. **The build reads a sentinel vector.** `ArcadePageVectorValues.getVector` returns `deletedSentinelVector` on
   eight paths, three of them silent. Instrumented all three: zero hits on a failing run.

## Review notes

- The walk runs on every rebuild, including the common no-orphan case: an O(V+E) pass plus a transient
  `boolean[upper]` + `int[upper]`. It is off the write lock and bounded by the graph the build just produced,
  which is far more expensive, so it is not worth pooling the arrays unless profiling says otherwise.
- `getVector` never returns null - an unreadable ordinal comes back as the sentinel - so the recovery path
  explicitly rejects the sentinel. The location check ahead of it cannot stand in: it reads the live index
  rather than the build snapshot, and says nothing about the document-read failures that also yield a sentinel.
- Only the from-scratch path needs this. Vectors ingested through the live builder are already in the delta
  buffer from insertion, so an orphan there is served by the delta scan until a rebuild absorbs it.

## Scope of the guarantee

This restores "every committed vector is findable" **for the running session, until the next rebuild** - not
unconditionally. The two gaps below are inherent to serving the recovery from an in-memory buffer, and both are
bounded by the next rebuild re-detecting the orphan.

## Known limitations

- **The recovery does not survive a restart.** `deltaVectors` is in-memory, so a restart drops the re-queued
  entries. The persisted graph still physically contains the orphan and reports the same node count as the
  ordinal map, so the staleness check on load (`graphSize < ordinalMap.length`) sees an up-to-date graph and
  the vector is unsearchable again until some mutation triggers a rebuild. Closing this would mean persisting
  the orphan set alongside the graph; it is left open because the builder defect is rare and any rebuild
  re-detects it.
- **An idle index keeps scanning the re-queued entries.** Re-queueing does not bump `mutationsSinceSerialize`
  (counting it would let an orphaned index rebuild itself forever), and the decrement at the end of a rebuild
  may cancel the inactivity timer, so there is no self-scheduled rebuild to clear them until the next real
  mutation.

## Tests

- `LSMVectorIndexGraphConnectivityTest` - deterministic coverage of the detection primitive against graphs
  whose shape is fixed by the test: a fully connected chain, a node with out-edges and no in-edges, several
  orphans at once, a disconnected cycle, and an empty graph, plus one case pinning that the sentinel is
  distinguishable from a real vector. Stubbing `findUnreachableOrdinals` to a no-op fails 3 of the 6, and
  stubbing `isDeletedSentinel` to `false` fails 1 more, confirming they bind.
- `LSMVectorIndexConcurrentRebuildVisibilityTest` - the end-to-end reproducer, which on a miss now reports
  whether the vector is reachable from the entry node at all.

## Verification

```
mvn -pl engine test -Dtest=LSMVectorIndexGraphConnectivityTest
mvn -pl engine test -Dtest=LSMVectorIndexConcurrentRebuildVisibilityTest -Dgroups=slow -DexcludedGroups=
mvn -pl engine test -Dtest='com.arcadedb.index.vector.*Test'   # 246 tests, 0 failures
```

The concurrency reproducer is inherently probabilistic - its flake rate on unfixed code swung between 1 run in
1 and 1 run in 10 - so a single clean run of it never proves anything on its own. The deterministic test is
what pins the fix.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5633

### Review cycles

| Cycle | Head | Outcome |
|---|---|---|
| 1 | `ba5ff86` | Sentinel not excluded when re-queueing - **real**, fixed (`isDeletedSentinel` + test). Three further notes assessed and documented rather than coded. |
| 2 | `4ce9ef6` | Main ask (exception storm in the level loop) rested on a **false premise** - `OnHeapGraphIndex.getNeighborsIterator` returns `EMPTY_NODE_ITERATOR` for an absent node, verified in bytecode, and the suggested `contains()` guard would add a lookup rather than remove one. Pushed back with evidence; corrected the misleading comment that caused it. Applied the entry-node null check and the level-union note. |
| 3 | `b1eb6e4` | Restart gap - **real**; verified the precise mechanism (the staleness check needs `graphSize < ordinalMap.length`, and an orphan is counted in the graph) and documented it. Guarded the null neighbor iterator. |
| 4 | `f8a494d` | No blockers. Softened the framing of the guarantee to match what is delivered. |

### Follow-ups not taken here

- **Upstream:** why `GraphIndexBuilder.build()` leaves a node unreferenced despite calling `cleanup()`.
- **Separate defect, filed as #5648:** `GraphSearcherPool.borrow` calls `drain()` *before* publishing
  `pooledGraph` / `pooledEpoch`, so a concurrent `release` reading the still-old matching pair can return a
  searcher bound to the old graph to the idle queue. Needs two searching threads on one index, so it is not
  #5615, but it produces the same silent wrong-result symptom.
- **Possible enhancement:** a bounded self-heal (one deferred rebuild when orphans are found) if orphan counts
  are ever observed to be large in practice.
