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
- [x] Regression tests pass: 2/2
- [x] Full vector index suite passes: 139/139 tests
- [x] Related query tests pass: CypherCallVectorNeighborsTest, SelectVectorTest
