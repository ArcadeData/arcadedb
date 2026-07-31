# 5648 - GraphSearcherPool can hand out a searcher bound to a replaced graph

Issue: https://github.com/ArcadeData/arcadedb/issues/5648

## Root cause

`GraphSearcherPool` guarded reuse with two independent volatile fields, `pooledGraph` and `pooledEpoch`, and
`borrow` drained the idle queue **before** publishing them:

```java
if (pooledGraph != graph || pooledEpoch != epoch) {
  drain();              // empties the queue
  pooledGraph = graph;  // publishes only now
  pooledEpoch = epoch;
  return new GraphSearcher(graph);
}
```

A `release` running inside that window still reads the outgoing pair, matches it, and offers its searcher into the
queue after the drain has already swept past. The next `borrow` matches the newly published pair and polls that
searcher straight back out, so a search over the new graph runs on a `GraphSearcher` whose `View` was captured
from the old one.

The queue itself carried no record of which graph or epoch an entry belonged to, so `borrow` had no way to tell a
valid entry from a stale one. Draining is inherently racy against a concurrent `release`, which means the sweep
alone can never be the guarantee.

Two narrower windows shared the cause: `pooledGraph` was written before `pooledEpoch`, leaving the pair
transiently `(G_new, E_old)`, and `clear()` nulled `pooledGraph` before draining.

## Impact

`findNeighborsFromVector` scores and maps ordinals through the **new** epoch's vector values and
`ordinalToVectorId` snapshot while the borrowed searcher traverses the **old** graph's edges. The result is
silently wrong neighbours - unrelated RIDs, or misses - with no exception and nothing left behind to attribute
them to. It needs two concurrent searches on one index plus a graph swap or mutation, which is ordinary server
workload (concurrent HTTP query handlers against one index).

## Fix

`engine/src/main/java/com/arcadedb/index/vector/GraphSearcherPool.java`

1. **Every queued searcher carries the identity it was pooled under** (`Pooled` record), and `borrow` verifies it
   on the way out, closing and skipping anything that does not match the caller's own graph and epoch. This is
   what actually closes the race: whatever a `release` manages to leave in the queue, a borrower never trusts it.
2. **The published identity is a single immutable `Identity` record** behind one volatile field, so the pair can
   no longer be read half-updated.
3. **The identity is published before the drain**, so a `release` still running under the outgoing one rejects its
   searcher rather than re-pooling behind the sweep.

The sweep in `borrow` and the identity check in `release` are retained, but they are now only early-reclamation
optimizations rather than the correctness guarantee.

Cost: one small `Pooled` record (three fields) per `release`. That is negligible next to the `GraphSearcher` the
pool exists to avoid reallocating - its growable candidate heap and two result heaps were the single largest
source of garbage in a dense-search workload.

## Tests

`engine/src/test/java/com/arcadedb/index/vector/GraphSearcherPoolStaleGraphTest.java` (new, 5 tests)

The interleaving needs a `release` to land between a `borrow`'s drain and its publish - a two-instruction window
that no amount of thread scheduling reproduces reliably. The tests therefore assert the **state** that window
leaves behind (the queue holding an entry pooled under one identity while the pool advertises another) and require
the pool never to hand it out. That state is unreachable through the public API once the fix is in, so it is
planted on the private identity field by reflection, mirroring the reflection already used by
`LSMVectorIndexSearcherEpochTest`.

- `borrowNeverHandsOutASearcherPooledUnderAReplacedGraph` - the graph swap case from the issue
- `borrowNeverHandsOutASearcherPooledUnderAStaleEpoch` - same graph instance, mutated underneath (live-builder path)
- `everyStaleEntryIsReclaimedBeforeAFreshSearcherIsBuilt` - all stale entries dropped, none left occupying the pool
- `aReleaseUnderASupersededIdentityIsNotPooled` - guards publish-before-drain
- `anUnchangedIdentityStillReusesThePooledSearcher` - guards against the check becoming so strict that pooling stops

**Proven to fail first.** Run against the unfixed pool (with `plantIdentity` pointed at the old `pooledGraph` /
`pooledEpoch` fields), the three reproducers failed with `Expected not same: GraphSearcher@...` - the pool handing
back the searcher bound to the replaced graph. The two guard tests passed before and after, as intended.

### Results

| Suite | Result |
|---|---|
| `GraphSearcherPoolStaleGraphTest` | 5/5 pass |
| `LSMVectorIndexSearcherPoolTest`, `LSMVectorIndexSearcherEpochTest`, `LSMVectorIndexConcurrentRebuildVisibilityTest`, `DeltaScanVectorSearchTest`, `LSMVectorIndexRebuildTest` | 24/24 pass |
| Full `com.arcadedb.index.vector` package (benchmark/slow excluded) | 198/198 pass |

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5653

### Review cycles

**Cycle 1 - `22d4da2ab`** - LGTM with two optional nits and one benign edge flagged for the record.

- *Reference-identity nit.* The bot reported that the records' generated `equals`/`hashCode` go unused because
  "all comparisons are reference-based", and asked for a comment explaining that `==` on a record is intentional.
  Checked against the code first: no `Identity` or `Pooled` instance is ever compared with `==` anywhere. The
  comparisons are on their **fields** - an `ImmutableGraphIndex` reference and a primitive `long`. The framing was
  therefore inaccurate, but the readability concern underneath it was real, so the comment added documents the
  claim that is actually load-bearing: a rebuild publishes a new graph instance and a searcher's `View` is bound
  to the exact instance it was built from, so graphs must match by reference however equal their contents.
- *Rebuild-storm over-reclamation.* Confirmed real. The reuse loop matches against the caller's own pair rather
  than the current published one, so if the identity moves on again mid-loop, an entry a concurrent `release`
  queued under the newer identity is closed here even though it was still usable. Behaviour left unchanged - it
  costs one searcher reallocation and can never return a wrong searcher, whereas re-reading the published
  identity per iteration would put a volatile read on the search path to buy nothing but pooling efficiency. The
  reasoning is now recorded in the loop so it reads as a deliberate trade-off.

**Cycle 2 - `845434997`** - clean approval. Three non-blocking observations, none requesting a change:
`size()`/`idleCount` is approximate under races (fine for metrics; the new tests are single-threaded so they are
unaffected), the rebuild-storm trade-off is well documented, and `getDeclaredConstructors()[0]` is safe for a
record because the canonical constructor is the only declared one. The bot independently confirmed that
correctness no longer depends on the published identity at all, so every racy path degrades to an extra
allocation rather than a wrong result.

Final state: **clean-approval**.

## Notes

`searcherPoolEpoch()` was made strictly monotonic separately in #5621. This change is about the publication order
and the trustworthiness of the queue's contents, not the epoch value.
