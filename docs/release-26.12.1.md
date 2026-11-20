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

### HTTP, gRPC and MCP: a standalone `BEGIN`, `COMMIT` or `ROLLBACK` is refused (#12202, #12232)

A SQL `BEGIN`, `COMMIT` or `ROLLBACK`, or an openCypher `START TRANSACTION`, `COMMIT` or `ROLLBACK`, sent as a command of its own
outside a session (`POST /api/v1/command/{db}` without `arcadedb-session-id`, `RemoteDatabase.command(...)`,
`RemoteGrpcDatabase.command(...)` outside `begin()`, the MCP `execute_command` tool) ran in the transaction the server opens
around that one request. It was answered OK, the `BEGIN` opened nothing the next command joined, and a later `ROLLBACK` undid
nothing: the writes in between were already durable. It is now refused with a client error (HTTP 400, gRPC `INVALID_ARGUMENT`,
`CommandSemanticException`) that names the alternatives:

- the transaction API of the client: `begin()`, `commit()`, `rollback()` or `transaction()`;
- the `/begin`, `/commit` and `/rollback` endpoints with the `arcadedb-session-id` header;
- in SQL, one `sqlscript` command that holds the whole block.

Unchanged: a `sqlscript` with `BEGIN ... COMMIT/ROLLBACK`, a statement inside an HTTP session or a gRPC transaction, the embedded
API, and Bolt, where `START TRANSACTION` opens a real transaction on the connection. A `BEGIN` of an isolation level the engine
cannot provide keeps its own error (#10386), and a SQL `EXPLAIN BEGIN`, which only plans, is still answered.

**Who sees a different answer.** A client that sent a standalone `BEGIN`, `COMMIT` or `ROLLBACK` and relied on the OK, for
instance a Studio user who types `BEGIN` alone in the SQL editor: use the transaction API of the client instead.

## Fixes

- **#12023** `vector.dotProduct`, `vector.l2Distance`, `vector.l1Distance` / `vector.manhattanDistance`, `vector.l1Norm`,
  `vector.multiScore` (`AVG`, `WEIGHTED`), `vector.sparseDot`, the sparse magnitude and the openCypher
  `vector_distance(..., EUCLIDEAN_SQUARED)` widen each component to double before multiplying or subtracting and sum in double,
  as the cosine already did (#4583): a pair of vectors with large components whose true answer is an ordinary float no longer
  answers `NaN` or `Infinity`, and a long sum no longer stops growing at the float precision of its running total. The SIMD
  paths of the dot product (4096 components and more) and of the L2 distance are kept, and an answer they return non-finite is
  recomputed in double.
