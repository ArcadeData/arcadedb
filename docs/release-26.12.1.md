# ArcadeDB v.26.12.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.12.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### openCypher: a query cannot conclude with a reading clause (#12354)

A query has to end with a `RETURN`, a `FINISH`, an update clause (`CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`,
`FOREACH`), a unit `CALL { }` (one whose body returns nothing) or a procedure `CALL`, as Neo4j and Memgraph require. A query
that ends with `MATCH`, `OPTIONAL MATCH`, `UNWIND`, `WITH`, `LOAD CSV` or a returning `CALL { }` used to be accepted and
answered the variables in scope; it is now refused before it runs with Neo4j's error:

```
InvalidClauseComposition: Query cannot conclude with UNWIND (must be a RETURN clause, a FINISH clause, an update clause,
a unit subquery call, or a procedure call with no YIELD)
```

The body of a `CALL { }` concludes the same way: with a `RETURN`, or with an update clause. The bodies of `EXISTS { }` and
`COUNT { }` may still end with a pattern.

**Who sees a different answer.** A query written without its `RETURN`, such as `MATCH (n:Person)` or
`MATCH (n) WITH n.name AS name`, which answered the bound variables: add the `RETURN` that says what to answer
(`MATCH (n:Person) RETURN n`).

### SQL: `DELETE VERTEX` deletes only vertices (#11620)

The `VERTEX` keyword of `DELETE VERTEX FROM ...` was accepted and then discarded, so the statement behaved, and rendered in
`EXPLAIN`, as a plain `DELETE FROM`. It is now kept and does what it says:

- a target type that is not a vertex type (a document or an edge type, a TIMESERIES type) is refused before anything runs,
  whether or not it holds records, and so is an `INDEX:` target;
- a record that is not a vertex, reached through a RID, a bucket or a subquery, is not deleted: the statement fails on it,
  and the vertices it deleted before are rolled back with the statement (its implicit transaction, or its savepoint inside
  an explicit one). With `BATCH`, the batches already committed before the failure stay deleted;
- `EXPLAIN` shows `+ DELETE VERTEX`, and the statement renders back as `DELETE VERTEX FROM ...`.

**Who sees a different answer.** A `DELETE VERTEX FROM` aimed at documents or edges, which used to delete them: write
`DELETE FROM` instead. `DELETE FROM` on a vertex type keeps removing the vertices with their edges, as before.

### openCypher: the second argument of `node.relationship.types()` is APOC's relationship pattern (#12103)

`node.relationship.types(node, [relTypes], [direction])` (and `apoc.node.relationship.types`) takes APOC's relationship
pattern as its second argument, as APOC does: `(n, 'KNOWS>|<LIKES')` answers the types of that list the node has a
relationship of, in the pattern's order. The second argument used to be the direction, which is now the third one,
narrowing the pattern to that direction: an alternative without a marker is followed in it, and a marked one that points the
other way names no relationship. `(n, null, 'IN')` is every incoming type, `(n, 'KNOWS>', 'IN')` answers `[]`.

A call written for the old signature keeps its answer: a second argument that is a direction word (`IN`, `INCOMING`, `OUT`,
`OUTGOING`, `BOTH`, in any case) with no third argument is still read as the direction.

**Who sees a different answer.** A database with an edge type named exactly like that word (an edge type `OUT`): there
`(n, 'OUT')` is the pattern naming that type, as in APOC, and answers `['OUT']` or `[]`. Write `(n, null, 'OUT')` for the
direction. A call that passed a type name, `(n, 'KNOWS')`, used to fail as an unknown direction and now answers.

## Fixes

- **#12023** `vector.dotProduct`, `vector.l2Distance`, `vector.l1Distance` / `vector.manhattanDistance`, `vector.l1Norm`,
  `vector.multiScore` (`AVG`, `WEIGHTED`), `vector.sparseDot`, the sparse magnitude and the openCypher
  `vector_distance(..., EUCLIDEAN_SQUARED)` widen each component to double before multiplying or subtracting and sum in double,
  as the cosine already did (#4583): a pair of vectors with large components whose true answer is an ordinary float no longer
  answers `NaN` or `Infinity`, and a long sum no longer stops growing at the float precision of its running total. The SIMD
  paths of the dot product (4096 components and more) and of the L2 distance are kept, and an answer they return non-finite is
  recomputed in double.
- **#12103** The `node.*` functions and the `algo.*` path finders read APOC's relationship pattern syntax (`'KNOWS>|<LIKES'`,
  `'>'`, `'<KNOWS'`, a blank one for every relationship), as `path.*` already did: `apoc.node.degree(n, 'KNOWS>')` answered 0,
  and `apoc.algo.dijkstra(a, b, 'ROAD>', 'w')`, `apoc.algo.aStar` and `apoc.algo.allSimplePaths` refused `'ROAD>'` as a type
  that does not exist. `node.degree.in` and `node.degree.out` read the pattern with their direction fixed (an alternative
  marked the other way counts 0), and `node.degree` sums per alternative as APOC does. A pattern whose alternatives mix
  directions is walked as written, each alternative in its own direction. The fifth argument of `algo.dijkstra` is APOC's
  `defaultWeight` when it is a number (the weight of a relationship without a numeric weight), the direction ArcadeDB took
  there before when it is a string, and refused otherwise. An edge type whose name contains `|`, `<` or `>` (or `,` in the
  `algo.*` finders) can no longer be named in these arguments, as in `path.*` since #10025: the character reads as the
  separator or the direction marker.
- **#12160, #12233** An `LSM_TREE` index on a property no schema declares, written by an older build, keeps finding, removing
  and refusing duplicates of the list and number values that build keyed by their Java text (`[1.5, 2.0]`, `1.0`, `-0.0`,
  `1.0E300`) instead of the canonical text written today (`[1.5, 2]`, `1`, `0`). A STRING spelled like such an old key is still
  a different value. **Upgrade note:** every `LSM_TREE` index created before this release (26.11.x and earlier) is treated as
  possibly holding old keys, and its seeks, removals and unique checks on such values pay an extra read. A STRING key that
  reads as a number or a list (`1.0`, `2.500`, `[1.5, 2.0]`) also loads the record behind each entry a seek finds, and a
  unique STRING key that reads as a number or a list (`12345`, `2.5`, `[1.5, 2]`) loads one record at commit; a version, an
  address, a date or any other text pays nothing. Run `REBUILD INDEX <name>` (or `REBUILD INDEX *`) once after upgrading to
  write every entry under its canonical key and drop that cost: an index this build creates or rebuilds pays nothing.
