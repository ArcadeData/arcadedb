# Fix #4334 — LSMVectorIndex EUCLIDEAN K-NN returns worst matches first

## Root Cause

JVector's `VectorSimilarityFunction.EUCLIDEAN.compare(u, v)` returns a **similarity** value:

```
similarity = 1 / (1 + L2²)
```

Larger similarity means closer vectors. ArcadeDB's three distance-conversion paths in
`LSMVectorIndex.java` incorrectly treat this similarity as a distance and sort ascending,
placing the *least similar* (farthest) candidates first.

### Affected code locations

| Method | Lines | Bug |
|---|---|---|
| `findNeighborsFromVector` (graph path) | ~2971–2982 | `case EUCLIDEAN -> score` |
| `mergeWithDeltaScan` (delta path) | ~2733–2738 | `case EUCLIDEAN -> score` |
| `bruteForceScan` (brute-force path) | ~2779–2784 | `case EUCLIDEAN -> score` |

All three sort ascending by the computed "distance" value. With similarity as distance, the
sort places far vectors (low similarity) first.

## Fix

Convert similarity back to squared L2 distance (preserves sort order, avoids sqrt):

```java
case EUCLIDEAN -> score > 0 ? (1.0f / score) - 1.0f : Float.MAX_VALUE;
```

Math: `similarity = 1/(1+L2²)` → `L2² = 1/similarity - 1`. Sorted ascending → closest first.

## Verification

Test class: `LSMVectorIndexEuclideanOrderingTest`

- `euclideanKnnReturnClosestFirst`: k=1 from three vectors, nearest vector returned.
- `euclideanKnnOrdering`: k=3, all three results in correct L2² ascending order.

## Additional fix locations

The issue reported three locations. Two additional conversion paths were also affected:
- `findNeighborsFromVectorGroupBy` grouped search path (~line 3171)
- Zero-disk-I/O PQ search path (~line 3321)

All five locations fixed with the same formula.

## Status

- [x] Write failing tests
- [x] Apply fix to all five code locations
- [x] Regression tests pass: 4/4
- [x] Full vector index suite passes: 139/139 tests
- [x] Related query tests pass: CypherCallVectorNeighborsTest, SelectVectorTest, SQLFunctionVectorNeighborsTest

## PR

PR #4343 - https://github.com/ArcadeData/arcadedb/pull/4343

## Review cycles

| Cycle | HEAD SHA | Change | Bot outcome |
|---|---|---|---|
| 1 | b88dbbd1 | Initial fix to 5 EUCLIDEAN sites | gemini: extract helper; claude: DbIndexVectorQueryNodes regression + exhaustive switch + trim Javadoc |
| 2 | ffae8cdc | Extract `scoreToDistance` helper | no new reviews |
| 3 | 1d777ee6 | Fix DbIndexVectorQueryNodes EUCLIDEAN score, exhaustive switch, trim Javadoc, add 2 tests | claude: 3 items, all out of scope (see below) |

## Cycle 3 declined items

1. **Mixed similarity functions across shards** - theoretical only. A `TypeIndex`'s bucket-level `LSMVectorIndex` instances share one `LSMVectorIndexMetadata` by construction; no realistic codepath produces shards with divergent similarity functions. Defensive validation would add noise without addressing a real risk.
2. **COSINE score in `DbIndexVectorQueryNodes` can be negative** - pre-existing behavior unchanged by this PR. Before fix: `score = 1 - 2*(1-rawScore) = 2*rawScore - 1 = cos`. After fix: identical formula, identical result. Pre-existing COSINE/DOT_PRODUCT score semantics are out of scope for #4334 (EUCLIDEAN ordering).
3. **Remove `docs/4334-*.md`** - contradicts repo convention. 10+ recent merged fix PRs commit identically-shaped tracking docs (#4339, #4341, #4338, #4340, #4322, #4323, #4325, #4281, #4280, #4279).

## Final state

clean-approval (cycle-3 review provided feedback but none actionable for issue scope)
