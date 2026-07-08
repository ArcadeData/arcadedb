# ArcadeDB v.26.7.2 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.7.2 development cycle, so the release notes are ready at tag time.

This release hardens **High Availability (Raft)** recovery. Raft storage is now **durable by default**
(previously ephemeral outside Kubernetes), which removes a class of permanent follower divergence after a
full-cluster cold restart - see **Breaking Changes** below. It also removes a diverged-follower log flood
that could starve the very snapshot resync meant to heal the node.

### Fixes

- **Remote API: `RemoteVertex.isConnectedTo()` now accepts a `Vertex` object, not only a `RID`.** Calling
  `vertex.isConnectedTo(otherVertex, ...)` from a remote database threw a `SQL syntax error at ... mismatched
  input '@'` because the argument was inlined into the generated SQL via its full `toString()` (e.g.
  `#4:0@Person[name=Jane]`) instead of just its identity ([#5122](https://github.com/ArcadeData/arcadedb/issues/5122)).
  All three `isConnectedTo` overloads now embed `argument.getIdentity()`, so passing a `Vertex` behaves exactly
  like passing its `RID`.

- **Bolt: a single failed TLS handshake no longer wedges the shared listener (DoS).** With Bolt-over-TLS
  (`arcadedb.bolt.ssl=OPTIONAL`/`REQUIRED`), one aborted or untrusted-certificate handshake could pin a CPU
  core at ~100% and stop the listener from completing handshakes for *all* subsequent clients until the server
  was restarted ([#5106](https://github.com/ArcadeData/arcadedb/issues/5106)). Two defects on the accept path
  caused it: the TLS-detection peek re-read a closed socket in a tight loop when a client closed early
  (busy-spin), and the blocking TLS handshake ran on the single accept thread, so a slow/stalled/failed
  handshake blocked every other connection. Transport detection and the TLS handshake now run on the
  per-connection thread (never on the accept thread) with a bounded read timeout, so a hostile or broken
  handshake only affects its own connection and the shared listener stays available.

- **Cypher: `exists()` now enforces multi-label constraints on an already-bound start variable.** A predicate
  like `exists((n:A:B)-->(:Node))` accepted the `A:B` label conjunction but silently ignored it on the bound
  variable `n`, so nodes lacking one of the labels (but with a matching outgoing edge) still passed
  ([#5095](https://github.com/ArcadeData/arcadedb/issues/5095)). The single-edge fast path in
  `PatternPredicateExpression` validated only the traversal target's labels/properties, never the start
  node's own declared pattern. It now checks the bound start vertex against its declared labels and inline
  properties before traversing, so `exists((n:A:B)-->(:Node))` matches exactly the nodes that also match
  `MATCH (n:A:B)`, in line with Neo4j.

- **Cypher: `shortestPath()`/`allShortestPaths()` now enforce inline edge property filters.** A pattern like
  `shortestPath((a)-[:LINK*1..3 {w: 1}]->(b))` accepted the `{w: 1}` edge filter but silently ignored it,
  traversing (and returning) paths that crossed edges with a different value
  ([#5096](https://github.com/ArcadeData/arcadedb/issues/5096)). The vertex-only BFS and the reused
  CSR-accelerated `SQLFunctionShortestPath` fast path could not see edge properties. When a relationship
  carries inline properties, both functions now route through an edge-aware BFS that walks only matching
  edges and keeps the concrete edge used to reach each vertex (so parallel edges with differing properties
  stay disambiguated); matching mirrors the standard variable-length `MATCH` path, so the same
  `{prop: value}` constraint behaves identically whether written as `shortestPath(...)` or as an inline
  variable-length pattern. The no-filter case keeps the existing fast path unchanged.

- **Cypher: `count(*)` after `OPTIONAL MATCH` no longer drops the null-preserving rows.** A query like
  `MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(*)` returned only the number of
  rows where the optional pattern matched (i.e. `count(m)`) instead of counting every row, including those
  where `m` is `null` ([#5094](https://github.com/ArcadeData/arcadedb/issues/5094)). The star-join count
  push-down (`DegreeProductOp`) built its per-vertex degree map by iterating edges, so central vertices with
  no matching edges never entered the map and their null rows were lost when every arm was optional. The
  OLTP degree-map path now adds one row per central-type vertex missing from the map when no mandatory arm
  exists, and filters out map entries that are not of the central type (they could previously inflate the
  count when the same edge type connected other vertex types). The CSR fast path falls back to the OLTP
  path in the all-optional-arms case, since the CSR node domain cannot distinguish zero-degree central
  nodes from nodes of other types. Row materialization of the same pattern was already correct; results
  now match Neo4j, Memgraph, and FalkorDB.

- **SQL: `FROM` can now be used as a property name.** `CREATE PROPERTY fulfilledBy.From STRING` failed with a
  parsing exception while the symmetric `fulfilledBy.To` worked, because `FROM` was a fully reserved keyword and
  `TO` a soft one ([#5092](https://github.com/ArcadeData/arcadedb/issues/5092)). The grammar now accepts `FROM`
  in every position where a property name is unambiguous: `CREATE/ALTER/DROP PROPERTY Type.From`, `CREATE INDEX
  ON Type (From)`, dotted access in expressions (`@this.From`, `chain.From`), `SET/ADD/PUT/INCREMENT From = ...`
  in `UPDATE`/`CREATE VERTEX`/`CREATE EDGE`, and `INSERT INTO t (From, ...) VALUES (...)` / `INSERT ... SET
  From = ...`. Bare references (`SELECT From`, `WHERE From = ...`) remain reserved - `SELECT FROM Type` without
  a projection must keep parsing as before - and still require back-tick quoting, like any fully reserved keyword.

- **Native `select()` API: `ORDER BY` on a non-index-served property no longer returns an empty result set.**
  When the fluent `database.select()...orderBy(...)` query could not be served directly by an index, the
  in-memory sort path built an empty `sortedResultSet` and never drained the underlying iterator (the
  materialization loop condition was `orderIndex < sortedResultSet.size()`, i.e. `0 < 0`, so it never ran).
  Every such query silently returned zero rows. The same executor also built a **descending** index range scan
  with an open (`null`) bound - an unsupported cursor shape that likewise returned empty - so `ORDER BY ...
  descending` on an indexed property was affected too ([#5079](https://github.com/ArcadeData/arcadedb/issues/5079)).
  The iterator now materializes the full result set via the internal fetch loop, sorts it, and honors `SKIP`
  and `LIMIT`; the index scan direction is decoupled from `ORDER BY` (always ascending selection, descending
  order served by the in-memory sort). Materialization is now lazy so it composes with the parallel iterator.

- **Transactions: in-transaction iteration on a non-unique index no longer drops a committed RID that shares
  its key with an uncommitted entry.** On a non-unique index, when an uncommitted row had the SAME composite
  key as an already-committed row, in-transaction index iteration emitted only ONE row for that key: the
  transaction overlay was treated as authoritative and shadowed the disk cursor's equal-key entries instead
  of merging the distinct RIDs. Both RIDs became visible again only after commit
  ([#5055](https://github.com/ArcadeData/arcadedb/issues/5055)). `LSMTreeIndexCursor` now UNIONs the disk and
  overlay RIDs at a colliding key. While fixing this, a related direction bug surfaced: the overlay range
  filter always used the ascending sense of the far bound, so a **descending** in-transaction range scan
  dropped uncommitted overlay entries sitting above the range's lower bound (the top of the scan) even with no
  collision; the filter is now order-aware.

- **Import/Export: `IMPORT DATABASE` no longer crashes on a JSONL dump that contains an `LSM_VECTOR` index.**
  Exporting a database with a vector index produced a valid JSONL dump, but re-importing it failed with
  `CommandExecutionException: Error on importing database` (root cause: `JSONObject[unique] not found`),
  leaving the target database half-imported. The JSONL schema loader assumed every index serialises the
  `unique`/`nullStrategy` fields, which `LSM_VECTOR` indexes do not. The importer now rebuilds vector
  indexes through the dedicated vector builder using the exported metadata (dimensions, similarity
  function, HNSW parameters); the index is restored and repopulated from the imported embeddings, so no
  manual filtering or post-import `create_vector_index()` step is needed
  ([#5069](https://github.com/ArcadeData/arcadedb/issues/5069)).

- **HA: leader now auto-recovers a follower's replication channel wedged on stale DNS after a pod-IP change.**
  On Kubernetes, a follower that restarts with a new pod IP could stay stranded until a manual
  `transferLeadership`, dropping the cluster to bare quorum for minutes
  ([#4696](https://github.com/ArcadeData/arcadedb/issues/4696)). The prior fix closed the inbound allowlist
  gap and hardened JVM DNS caching, but the leader's *outbound* Ratis gRPC appender channel kept returning
  the stale `UnknownHostException` and never re-resolved: Ratis's error path only calls
  `resetConnectBackoff()`, which never recreates the `ManagedChannel`. Now, once a follower has been
  continuously unreachable for `arcadedb.ha.peerChannelResetDuration` (default 60s), the leader closes that
  one follower's gRPC proxy (`PeerProxyMap.resetProxy`), so the appender's next send rebuilds a fresh channel
  and re-resolves DNS to the follower's current IP. The reset is retried once per interval while the follower
  stays unreachable, up to a small bounded number of attempts, so a first attempt that does not stick does not
  strand the follower; after the cap the leader gives up and logs for operator intervention, and the counter
  re-arms when the follower reconnects. Only the unreachable peer's channel is touched and leadership is
  unchanged, so there is no flapping risk. Set the duration to `0` to disable the automatic reset.

- **Cypher: 2-hop pattern comprehension with an anonymous middle node now returns matches.** A pattern
  comprehension such as `RETURN [(p)-[:KNOWS]->(:Person)-[:KNOWS]->(t:Person) | t]` silently returned an
  empty list, while the equivalent explicit `MATCH` and the single-hop pattern comprehension both worked
  ([#5007](https://github.com/ArcadeData/arcadedb/issues/5007)). The traversal resolved each hop's start
  node from its variable binding, but an anonymous intermediate node `(:Person)` carries no variable, so
  the second hop found `null` and stopped. `PatternComprehensionExpression` now carries the end vertex of
  each hop forward as the start of the next hop, so anonymous intermediate nodes match (and an anonymous
  uncorrelated start node no longer risks re-iterating). Named intermediate nodes were unaffected.
- **Cypher: `DELETE` of a relationship inside `FOREACH` now removes the edge.** A planner optimization drops
  a named edge variable from the matched row when it thinks the variable is unused downstream, so the match
  step can take the GAV/CSR fast path (which does not load edge objects). The "is this edge referenced?"
  analysis (`CypherExecutionPlan.isEdgeVariableReferenced`) walked `RETURN`/`WITH`/`UNWIND`/`SET`/`DELETE`
  but never looked inside a `FOREACH` clause. So `MATCH (a)-[r]->(b) FOREACH (x IN [r] | DELETE x)` had `r`
  stripped from the row; the `DELETE` then found `null` and silently no-op'd while the query still reported
  success ([#4912](https://github.com/ArcadeData/arcadedb/issues/4912)). The analysis now also inspects the
  `FOREACH` list expression and its inner write clauses (recursively for nested `FOREACH`), keeping the edge
  binding alive so the deletion runs. Node deletion in `FOREACH` was unaffected because vertex bindings were
  never dropped.
- **Cypher: `DELETE` of a relationship inside a `CALL` subquery now removes the edge.** Same root cause as the
  `FOREACH` fix above: the edge-reference analysis (`CypherExecutionPlan.isEdgeVariableReferenced`) never looked
  inside a `CALL` subquery clause, so `MATCH (a)-[r]->(b) CALL (r) { DELETE r }` had `r` stripped from the row;
  the inner `DELETE` then found `null` and silently no-op'd while the query still reported success
  ([#4913](https://github.com/ArcadeData/arcadedb/issues/4913)). The analysis now inspects a `CALL` subquery's
  explicit scope list (`CALL (r) { ... }`) and, for the implicit-import form (`CALL { WITH r ... DELETE r }`),
  the inner statement's clauses (recursively for nested `FOREACH`/`CALL`), keeping the edge binding alive so
  the deletion runs. Node deletion in `CALL` subqueries was unaffected because vertex bindings were never dropped.
- **Cypher: `REMOVE` of a relationship property now takes effect without a `WITH` projection.** Same root cause
  as the `FOREACH`/`CALL` fixes above: the edge-reference analysis (`CypherExecutionPlan.isEdgeVariableReferenced`)
  had no case for a top-level `REMOVE` clause, so `MATCH (a)-[r:T]->(b) REMOVE r.marked` had `r` stripped from the
  matched row; `RemoveStep` then found `null` and silently no-op'd while the query still reported success
  ([#5013](https://github.com/ArcadeData/arcadedb/issues/5013)). Projecting the edge first (`WITH r REMOVE r.marked`,
  `FOREACH (x IN [r] | REMOVE x.marked)`, or a `CALL` subquery) forced materialization, which is why those forms
  worked. The top-level clause scan now includes `REMOVE`, mirroring `SET`, so the edge binding survives and the
  property is removed. Removing node properties was unaffected because vertex bindings were never dropped.
- **SQL: map string-key indexing `$map["key"]` now works inside `INSERT ... CONTENT`.** `ArraySelector.getValue`
  had two overloads: the `Result`-based one returned any index value, but the `Identifiable`-based one was typed
  to return `Integer` and silently dropped non-numeric indexes. `INSERT ... CONTENT { ... }` evaluates its JSON
  through the `Identifiable` path, so an expression like `$test["name"]` resolved to `null`, while the equivalent
  `INSERT ... SET` (which goes through the `Result` path) returned the expected value
  ([#4915](https://github.com/ArcadeData/arcadedb/issues/4915)). The `Identifiable` overload now returns the raw
  index too, so String map keys resolve correctly. Numeric range selectors are unaffected (they use
  `ArrayNumberSelector`, not `ArraySelector`).
- **HASH index: cyclic overflow chain no longer spins a CPU core forever.** A corrupted (cyclic) overflow
  chain in a `HASH` index made the read/scan walkers - `searchBucket` and friends - loop endlessly, because
  they followed the `overflow -> overflow` pointers with no cycle detection (the write paths already guarded
  with a `visited` set). In the field this pinned two XNIO worker threads at 100% CPU (200% total) for two
  days, both stuck in `HashIndexBucket.searchBucket -> PageManager.getImmutablePage`, reached through the
  unique-constraint check of a batch vertex import; the request never returned and never failed
  ([#4743](https://github.com/ArcadeData/arcadedb/issues/4743)). The five read/scan chain walkers now bound
  the walk by the file's page count (a valid chain can never be longer) and throw an actionable
  `IndexException` naming the index and telling the operator to rebuild it, instead of hanging. The guard is
  allocation-free so the unique-check hot path is unaffected. Recovery: `DROP` and recreate (or
  `CHECK DATABASE FIX`) the affected index.
- **LSM index: compaction no longer loses a key that was deleted and re-inserted.** The compactor merges
  pages oldest to newest, accumulating each key's RIDs in an insertion-ordered set, while the reader treats
  the LAST position of an entry as the newest operation. A RID that was added, deleted, and added again
  (e.g. a record whose indexed property was changed away and back, across enough writes for the three
  operations to land in different sealed pages) kept its original pre-tombstone position in the set, so the
  tombstone ended up "newest" and the row silently vanished from the index after compaction: a query by that
  key returned nothing while the record still existed
  ([#4942](https://github.com/ArcadeData/arcadedb/issues/4942)). Duplicates now move to the end of the set so
  last-write-wins is preserved for both re-added RIDs and repeated tombstones.
- **LSM index: range scans are no longer truncated by a fully-deleted key on an older page.** The cursor
  constructor left a cursor positioned on a full-key tombstone (written by `Index.remove(keys)` without a
  RID, as the geospatial index does internally) alive but not counted in its active-iterator counter, while
  `next()` decrements that counter whenever ANY cursor is exhausted or leaves the range. The counter could
  hit zero with other cursors still holding valid rows, silently ending the scan: a range query could return
  0 rows when the surviving rows all sorted after the tombstoned key
  ([#4944](https://github.com/ArcadeData/arcadedb/issues/4944)). The constructor now skips past
  tombstone-only keys at initialization so every cursor left alive is counted, and it releases exhausted
  cursors instead of letting their stale keys take part in the merge.
- **LSM index: point lookup on a non-unique index no longer resurrects a deleted RID.** During a `get()` the
  per-RID tombstone set was local to each page lookup, while the whole-key removed set was already threaded
  across pages. On a non-unique index a per-RID tombstone and the original ADD routinely live in different
  pages, so when the walk reached the older page the deletion was forgotten and the deleted row reappeared
  in equality lookups, disagreeing with the range/cursor path
  ([#4945](https://github.com/ArcadeData/arcadedb/issues/4945)). The tombstone set is now threaded across
  pages and the compacted sub-index, exactly like the removed-keys set.
- **Index: updating the same record twice in one transaction no longer leaves a phantom index entry.** The
  eager in-transaction index update diffed every save against the record's committed buffer, which stays
  frozen until commit because serialization is deferred, so the intermediate ADD was never cancelled: after
  `orig -> mid -> final` in one transaction, a lookup of `mid` still resolved (to the wrong record) and, on a
  unique index, blocked a later legitimate insert of that value
  ([#4935](https://github.com/ArcadeData/arcadedb/issues/4935)). The second and subsequent updates now diff
  against a per-record snapshot of the previous in-transaction indexed state. The snapshot stores ONLY the
  indexed property values (not a full copy of the document), so the cost per update is independent of the
  document width and negligible for bulk updates.
- **Query: parallel bucket scans can no longer self-deadlock under load.** The blocking scan producers ran
  on the shared query-parallelism pool, whose caller-runs saturation policy executed a whole bucket scan
  synchronously on the CONSUMER thread, before the result set was even returned. For any bucket larger than
  the bounded result queue the scan then blocked forever on its own full queue - the only thread that could
  drain it was the one doing the blocking - permanently wedging the HTTP worker; under sustained load the
  wedged workers accumulated until the server stopped answering. Parallel scanning is enabled by default
  (`arcadedb.queryParallelScan=true`), so any multi-bucket full scan could hit this
  ([#4948](https://github.com/ArcadeData/arcadedb/issues/4948)). Scan producers now run on a dedicated
  producer pool that never runs tasks on the caller (visible as `pool=parallel_scan` in the Executor Pools
  metrics), which also stops blocked producers from pinning the shared compute pool's workers and coupling
  the latency of unrelated graph queries to the slowest scan consumer in the JVM
  ([#4950](https://github.com/ArcadeData/arcadedb/issues/4950)). Two behavior changes ride along: a scan
  producer that fails now FAILS the query instead of silently returning fewer rows, and a parallel-scan
  result set left neither consumed nor closed is abandoned after 10 minutes of consumer inactivity on a full
  buffer (its query then fails on the next access).

  **Upgrade guidance for the abandonment timeout** (parallel scan is enabled by default, so this applies to
  every multi-bucket full scan): a consumer that is alive but pauses BETWEEN reads for longer than the
  timeout - a stalled streaming client, a wire-protocol portal (Postgres `FETCH`, Bolt `PULL`) held open
  across a long idle gap, or very heavy per-row downstream processing - now receives a
  `CommandExecutionException` mentioning "Parallel scan abandoned", where it previously just blocked the
  scan's producers. If your deployment holds cursors open across idle pauses approaching 10 minutes, raise
  `arcadedb.parallelScanAbandonedTimeout` (milliseconds) or set it to `0` to disable the timeout and restore
  the previous park-until-closed behavior. Plain HTTP API and console clients are unaffected (they drain
  results synchronously).
- **Query: parallel scan workers no longer race on the caller's command context.** Every scan worker shared
  the caller's `CommandContext` and concurrently wrote `$current` into its non-thread-safe variables map on
  every row - corrupting `$current` semantics for downstream expressions and risking `HashMap` corruption
  under load ([#4949](https://github.com/ArcadeData/arcadedb/issues/4949)). Each worker now gets its own
  context copy, and `$current` on the caller's context is written only by the consumer.
- **Graph: interrupted parallel graph algorithms now fail instead of returning partial results as success.**
  PageRank/BFS chunk fan-outs, the Cypher fused-chain traversal and the partitioned triangle count all
  swallowed an interrupt during their fork/join wait and merged whatever chunks had completed, so a query
  killed by a timeout or cancel returned an incomplete answer as a successful one
  ([#4951](https://github.com/ArcadeData/arcadedb/issues/4951)). The shared await now cancels the
  outstanding chunks and throws, preserving the interrupt flag for the caller.
- **Storage: a slow reader can no longer poison the page cache with a stale version (lost update).** A
  reader that started a disk read of page version N before a committer cached version N+1 overwrote the
  newer committed page when its read completed, because the cache put was unconditional. Every subsequent
  reader then saw vN, and the commit-time version probe read the poisoned cache too, so a later transaction
  could pass its MVCC check and silently overwrite the lost committed update
  ([#4925](https://github.com/ArcadeData/arcadedb/issues/4925)). The cache put is now version-monotonic: an
  older page version never replaces a newer one, with the RAM accounting computed inside the same atomic
  operation.
- **Storage: page-cache RAM accounting can no longer go negative and disable eviction.** The bulk removal
  loops (database close/kill, file drop) subtracted a page's size from the cache accounting before removing
  it, unconditionally, while the eviction path subtracts only when its removal actually won. Racing the two
  subtracted the same page twice; once the counter drifted negative the `totalRAM < maxRAM` eviction check
  never fired again and the read cache grew without bound (real RSS growth, invisible in the stats)
  ([#4933](https://github.com/ArcadeData/arcadedb/issues/4933)). All accounting is now driven by the entry
  actually removed, so every page is subtracted exactly once.

- **Storage/engine robustness bundle.** Five defects from the 2026-07 engine audit: a plain I/O error while
  flushing one page no longer aborts the rest of the batch nor leaks its pages in the flush index (which
  hung `close()`/backup forever). The wait-for-flush is now bounded by a NO-PROGRESS window
  (`arcadedb.flushAllPagesTimeout`, default 60s, 0 = wait forever; a healthy slow backlog never trips it),
  and a close that gives up becomes CRASH-EQUIVALENT: the WAL files and the lock file are preserved so the
  next open runs recovery and replays the unflushed pages, instead of the close silently deleting the only
  durable copy of them ([#4928](https://github.com/ArcadeData/arcadedb/issues/4928)); the channel-reopen path no longer re-creates
  a file that DDL deleted while one of its pages was mid-flight in the flush thread (a one-page ghost file
  that got re-registered on the next open, [#4930](https://github.com/ArcadeData/arcadedb/issues/4930));
  `check()` on buckets beyond 2^31 positions no longer overflows its RID arithmetic (under `fix=true` it
  could delete an innocent record at the wrong RID, [#4931](https://github.com/ArcadeData/arcadedb/issues/4931));
  deleting a multi-page record no longer swallows the concurrent-modification retry signal (the delete
  reported success while orphaning the remaining chunks - a permanent space leak,
  [#4932](https://github.com/ArcadeData/arcadedb/issues/4932)); and scheduling async work concurrently with
  database close now fails with the intended "Async executor has been shut down" error instead of a raw
  NullPointerException ([#4955](https://github.com/ArcadeData/arcadedb/issues/4955)).

- **Commit: the WAL append is now the point of no return.** Page versions are validated and bumped BEFORE
  the transaction is appended to the WAL, so a WAL record can only exist for a transaction that can no
  longer fail validation - previously a phase-2 validation failure left the aborted transaction in the WAL
  with no abort marker, and crash recovery partially replayed it
  ([#4936](https://github.com/ArcadeData/arcadedb/issues/4936)). Enabling that guarantee, the commit lock
  set is verified AFTER all page-set mutation and extended when files joined late (EXTERNAL-property
  buckets, indexes created inside the transaction, and the vector index's companion graph file, which now
  counts in the lock set; [#4937](https://github.com/ArcadeData/arcadedb/issues/4937)). **Behavioral
  change for explicit locking:** a transaction using explicit locks that writes a file absent from its lock
  list - including files it could not have locked up front, such as an index created inside the same
  transaction or an EXTERNAL property's paired bucket - now fails with the "not all the modified resources
  were locked" error instead of committing without the lock (which was unsafe). Lock the type after
  creating its indexes, or create indexes outside explicit-lock transactions. Note the commit
  boundary this makes explicit: a failure AFTER the WAL append (e.g. while publishing pages) is resolved by
  recovery replay, never by abort - the caller may see an error for a transaction that becomes durable on
  restart. If that post-append failure ever happens, the database is FENCED: every further operation fails
  with a "close and reopen to run recovery" error, preventing new transactions from appending conflicting
  WAL records for the same page versions, and the ack-gated close preserves the WAL so the reopen replays
  the orphaned transaction.
- **WAL/recovery correctness (2026-07 audit).** Recovery now RE-APPLIES a WAL delta whose version equals
  the on-disk page version, repairing a torn page write (a 64KB flush spans many sectors; a power loss can
  persist the new version header while the delta sectors still hold the previous content - the old `<=`
  skip declared such a page "already applied" and left it silently corrupt despite an intact WAL,
  [#4926](https://github.com/ArcadeData/arcadedb/issues/4926); known limitation: async flush can coalesce
  several committed versions into one physical write, and only the newest version's region is repaired -
  fully closing the multi-version case needs per-page checksums to detect which pages are torn, tracked in
  [#5054](https://github.com/ArcadeData/arcadedb/issues/5054)). A clean close whose data fsync failed now
  preserves the WAL and the lock file for recovery instead of deleting them (after a failed fsync the OS
  may have dropped the dirty pages, so the WAL held the only durable copy;
  [#4934](https://github.com/ArcadeData/arcadedb/issues/4934) - the runtime WAL rotation also skips its
  drop pass when the pre-drop fsync fails).

- **LSM index hardening (2026-07 audit).** A failed compaction is now atomic: the in-RAM page count is
  rolled back (after draining in-flight flushes) so the orphaned leaf pages a mid-merge failure leaves on
  disk are never published as live series content by the next successful round, and a failed FIRST
  compaction drops the temp compacted file it registered instead of leaking one file per failed round
  ([#4946](https://github.com/ArcadeData/arcadedb/issues/4946)). In-transaction index iteration is fixed on
  two legs ([#4947](https://github.com/ArcadeData/arcadedb/issues/4947)): the transaction overlay now sorts
  NULL key components LOW, matching the on-disk comparator (they used to sort HIGH, so cursor navigation
  disagreed with the disk merge order), and overlay TreeMap navigation with PARTIAL keys now uses biased
  boundary keys - a prefix search used to land in the MIDDLE of the run of entries sharing the prefix,
  silently skipping the run's earlier uncommitted entries from in-transaction range scans (no nulls
  required). A further pre-existing defect discovered while testing - in-tx iteration on a non-unique index
  shadows committed RIDs whose key equals an uncommitted entry's key - is tracked in
  [#5055](https://github.com/ArcadeData/arcadedb/issues/5055).

- **Thread-context lifecycle hardening (2026-07 audit).** The periodic dead-thread sweep of the per-thread
  database contexts now rolls back any transaction the dead thread abandoned, releasing its file locks:
  before, a thread dying with an open transaction holding explicit locks (`acquireLock().type(...).lock()`)
  left the `LockManager` files owned by the dead thread forever, and every later commit touching them timed
  out until restart ([#4941](https://github.com/ArcadeData/arcadedb/issues/4941)); the cross-thread unlock
  works because the lock requester is now captured once at lock-acquisition time instead of being re-resolved
  as the calling thread at release time. Dead-thread detection no longer uses `Thread.getAllStackTraces()`,
  which misses virtual threads (a LIVE virtual thread's context was pruned as dead while its transaction was
  running) and captures every platform-thread stack at a global safepoint (a periodic multi-ms latency spike);
  each context entry now holds a `WeakReference<Thread>` checked with `isAlive()` - O(1), safepoint-free,
  virtual-thread-correct - and the GraphAnalyticalView async build/rebuild/compaction workers unregister their
  thread contexts on completion instead of leaking them until the next sweep
  ([#4956](https://github.com/ArcadeData/arcadedb/issues/4956)). The per-thread contexts map is now a
  `ConcurrentHashMap` and `removeAllContexts` (database close/drop from another thread) no longer prunes
  foreign registry entries, closing a data race with the owner thread's `init()` that could corrupt the plain
  `HashMap` or unregister a live context ([#4939](https://github.com/ArcadeData/arcadedb/issues/4939); the
  reported mid-commit cross-thread rollback interleaving is already excluded by the database RW lock - commit
  holds the read lock for its whole duration while close's rollbacks run under the write lock). Review
  follow-up: concurrent sweeps (two threads landing on the periodic boundary together) now claim each dead
  entry atomically, so its abandoned transactions are rolled back exactly once instead of racing a double
  rollback (double unlock, concurrent record reloads on shared caches); the sweep additionally claims each
  per-database context with a value-keyed remove, closing the same double-rollback race against
  `closeInternal`'s `removeAllContexts()` (which rolls back foreign contexts under the DB write lock the
  sweep does not take); the lazy requester capture is now an explicit `captureRequester()` invoked only
  from the lock-acquisition paths, with `getRequester()` a pure read for the release paths, so a future
  non-owner call can no longer pin the wrong lock identity (structural guard for the #4941 invariant).
  A sweep that performs real rollback work now emits a single attributable WARNING (transaction count,
  thread ids, databases; zero-work sweeps stay silent), and the exactly-once arguments are pinned to the
  JDK 21+ `Thread.threadId()` non-reuse guarantee at the registry declaration.
- **Async executor hardening (2026-07 audit).** Three fixes to the per-database async executor. The
  queue stall detector no longer false-positives on a worker merely busy with one slow task (it compared
  the identity of the queue head across two 5s windows, so any head task running longer than 10s made
  innocent producers throw "Asynchronous queue is stalled"); detection is now progress-based (per-worker
  completed-task counters) and, more importantly, a worker that schedules a cross-slot follow-up (the
  bidirectional-edge incoming-link task) into another worker's full queue now drains its OWN
  queue while it waits (the polled tasks are parked and run once the current task unwinds, so no
  transaction boundary can fall inside a suspended task's execution and per-task atomicity is
  preserved), so two workers cross-scheduling into each other no longer deadlock - a cycle the
  old detector could only break by throwing inside the worker, rolling back the whole in-flight commit
  batch (up to `commitEvery - 1` already-executed operations silently discarded) and dropping the
  follow-up (ghost half-edges) ([#4953](https://github.com/ArcadeData/arcadedb/issues/4953)). A
  producer blocked on the full queue of a worker wedged inside user code (an infinite loop or a
  blocking call in a task) still surfaces an error instead of hanging: a progress-gated backstop
  throws after 12 consecutive stall windows with zero completed tasks (60s with the default
  `checkForStalledQueuesMaxDelay` of 5s) - much longer than the old 10s false-positive-prone bound,
  tunable via `setCheckForStalledQueuesMaxDelay()`; a worker parked handing a task cross-slot with a
  flat completed count (a likely scheduling cycle) is reported faster, after 3 consecutive stall
  windows (15s by default), so a peer merely busy on one slow task does not trip it. The same applies
  to the backstop itself: a SINGLE legitimately long task (a large scan, a heavy user callback) that
  exceeds 12 x `checkForStalledQueuesMaxDelay` (60s by default) while producers wait on that worker's
  full queue is indistinguishable from a wedged one and trips the stall exception - raise the delay if
  your tasks legitimately run that long. Operators
  running scheduling chains deeper than two workers with individual tasks slower than 15s should
  raise `checkForStalledQueuesMaxDelay` (only the window duration is tunable, the counts are fixed),
  or the cross-slot detector can fire on a chain that would have resolved. The formerly documented
  `fast`-queue residual gap is fixed ([#5066](https://github.com/ArcadeData/arcadedb/issues/5066)):
  `arcadedb.asyncOperationsQueueImpl=fast` (also selected by the `high-performance` profile) now maps
  to Conversant's `DisruptorBlockingQueue` instead of `PushPullBlockingQueue`. The old class is an
  explicit single-producer/single-consumer design, while every async worker queue has many
  producers: a reproduction test showed concurrent offers silently LOSING tasks in normal
  operation, and its missing `remove(Object)` degraded the post-shutdown scheduling undo to a
  logged best-effort. The multi-producer replacement comes from the same library and jar, keeps the
  lock-free fast path (CAS-claimed sequences; note `fast` trades CPU for latency - the Disruptor
  queue busy-spins before parking, so idle workers cost more CPU than `standard`, same as the
  previous `fast` implementation), supports `remove(Object)` (the shutdown undo now
  works identically on both impls) and rounds capacities up to the next power of two (so for a
  non-power-of-two configured `ASYNC_OPERATIONS_QUEUE_SIZE / parallelLevel`, `fast` applies
  back-pressure at a slightly different real fill level than `standard`, which uses the exact size).
  Shutdown no
  longer strands completion waiters: an interrupted or exiting worker now notifies `completed()` on every
  task left in its queue instead of `queue.clear()`-ing them (threads blocked in `scanType()` /
  `waitCompletion()` hung forever), and `close()` no longer blocks unbounded on `queue.put(FORCE_EXIT)`
  under the lifecycle lock when a busy worker's queue is full - the marker is offered with a timeout,
  the worker is interrupted on failure, and a worker still alive after the 10s grace period is
  escalated to an interrupt and re-joined instead of being left running behind a returned `close()`
  ([#4954](https://github.com/ArcadeData/arcadedb/issues/4954)). Follow-up hardening on the helping
  loop itself: a worker handing off a cross-slot task to a wedged-alive peer while its OWN queue is
  empty (so the deferral budget cannot grow) no longer spins forever - the same 60s progress-gated
  backstop aborts the hand-off loudly via `onError` and the worker keeps serving its queue (failure mode
  when this trips on a bidirectional-edge follow-up: the outgoing direction is already persisted while the
  dropped follow-up carried the incoming link - a PARTIAL edge until the operation is repeated); and a
  worker that consumed the shutdown marker while help-waiting now abandons the hand-off immediately,
  so `close()` returns within one offer window on that path instead of waiting out the grace period.
  Low-severity cleanups from the same audit: async executor settings (`parallelLevel`, `commitEvery`,
  back-pressure, callbacks) are now `volatile` so post-startup changes reach the workers,
  `setCommitEvery(0)` is rejected instead of making every task fail on `count % 0`, the JVM-wide query
  parallelism and sparse-vector pools cancel a `Future` rejected after pool shutdown instead of leaving
  its waiter blocked forever, and `QueryEngineManager.register()` publishes a copy-on-write map so a
  post-construction registration cannot corrupt concurrent readers
  ([#4961](https://github.com/ArcadeData/arcadedb/issues/4961)).
- **`close()`/`drop()` no longer hang on a wedged async worker.** The graceful drain of in-flight
  asynchronous tasks on database close/drop used an UNBOUNDED wait, so a worker stuck inside a user task
  or callback blocked shutdown forever. It now waits at most `arcadedb.asyncCloseTimeout` ms (default
  60000, 0 = wait forever) before logging a WARNING and forcing the async workers down; the forced
  shutdown is itself bounded (FORCE_EXIT offer + interrupt + a ~10s join per worker, escalated to a
  second interrupt and join) and notifies completion of the leftover
  tasks ([#5080](https://github.com/ArcadeData/arcadedb/issues/5080)). Note for operators tuning a fast
  shutdown: in the worst case these bounded stages run SEQUENTIALLY - the async drain
  (`arcadedb.asyncCloseTimeout`), then the force-shutdown join, then the page-flush wait
  (`arcadedb.flushAllPagesTimeout`) - so a pathological close can take the sum. Each stage is individually
  bounded (shutdown always makes progress); lower the two timeouts if a tighter cap is needed.
- **PageManager lifecycle is refcounted.** The JVM-wide page manager was started and stopped on a racy
  "is the active-database map empty" check-then-act spanning factory instances: closing the last instance
  of one database could null the shared flush thread under a database whose open was still in flight
  (NPE on the first cache miss, or scheduled pages never flushed), and two concurrent opens could start two
  flush threads, leaking one with queued pages. Every open/create now acquires a reference and every close
  releases it under one global lock, with startup on the first acquire and teardown on the last release;
  `configure()` (the PROFILE setter hook) no longer starts a flush thread when no database is open, and a
  profile change while databases are OPEN is refused with a warning (the page manager keeps its current
  sizing rather than being swapped live under running queries; set the profile before opening databases)
  ([#4927](https://github.com/ArcadeData/arcadedb/issues/4927)).
- **Pool discipline, TimeSeries threading and low-severity storage/WAL/LSM cleanups (2026-07 audit).**
  The partitioned triangle-count operator now runs chunk 0 on the calling thread instead of submitting
  every chunk to the shared query pool and blocking on all of them - the same caller-runs-chunk-0
  discipline as `parallelForRange`, removing a pool-starvation deadlock risk when the operator is reached
  from a pool thread ([#4952](https://github.com/ArcadeData/arcadedb/issues/4952)).
  `TimeSeriesEngine.appendBatch` no longer dispatches shard writes to the shard executor when the calling
  thread has an enclosing transaction open - each TS-Shard thread ran with its own fresh transaction,
  publishing the shard pages out of band with the enclosing commit and, under HA, reordering the page
  versions on the Raft log; the per-shard sub-batches are now written sequentially in-thread in that case,
  honoring the `appendSamples` threading contract
  ([#4957](https://github.com/ArcadeData/arcadedb/issues/4957)). Low-severity storage/WAL cleanups
  ([#4958](https://github.com/ArcadeData/arcadedb/issues/4958)): the WAL read/write helpers now loop until
  complete with per-call buffers (a partial read decoded stale garbage from a shared buffer; a partial
  write left a torn record the gap detector then flagged as corruption); a corrupt per-page WAL header can
  no longer trigger a ~2GB transient allocation during recovery (the declared delta is clamped to the
  remaining file size - before, the resulting `OutOfMemoryError` escaped the recovery guard entirely);
  `TransactionManager.getStats()` no longer inflates its totals with polling frequency and no longer NPEs
  on read-only databases; the page cache-miss counter is actually incremented (it stayed at ~0 forever, so
  the hit/miss ratio was meaningless); a nested `suspendFlushAndExecute` (snapshot during backup) now runs
  the inner callback under the outer suspension instead of silently skipping it, and the HA Raft
  snapshot/checksums endpoint now serializes per database so a concurrent snapshot request can never
  stream files without owning the flush suspension (the suspend flag is first-caller-owned; the owner's
  exit would have resumed flushing mid-read for the other request, tearing the streamed files - reachable
  with the default `HA_SNAPSHOT_MAX_CONCURRENT` of 2); WAL files preserved by an
  aborted recovery are renamed to `.wal.corrupt` and the WAL pool counter is seeded past existing files,
  so preserved content is never adopted as an active WAL (appending new transactions after corrupt bytes)
  nor replayed again (the HA Raft snapshot checksum skips the preserved `.corrupt` files like it already
  skipped `.wal`); per-bucket free-space statistics now measure the usable content region (not the
  physical page size) and their counters are thread-safe. Low-severity LSM cleanups
  ([#4960](https://github.com/ArcadeData/arcadedb/issues/4960)): `BufferBloomFilter` (not yet wired into
  the read path) hardened before use - the bit index can no longer land one bit past the region, `add` is
  atomic (a dropped bit is a false negative, the one failure a bloom filter must never have) and two
  probes are derived from the 64-bit Murmur halves; the LSM mutable-index reference is now `volatile`
  (unlocked readers could observe a stale or partially published instance after a compaction swap);
  `close()` now cancels a scheduled-but-not-started compaction instead of silently doing nothing and logs
  loudly when an in-progress compaction blocks the close; `splitIndex` verifies the new-file lock outcome
  instead of ignoring it; a reloaded mutable index restores its uncompacted-pages counter from the file
  instead of hardcoding 1, so auto-compaction is no longer deferred by up to a full threshold of new
  pages after every restart. Items deliberately left open on #4958/#4960 with reasoning on the issues:
  the `activeWALFilePool` element publication (needs an `AtomicReferenceArray` refactor of a
  conflict-heavy file) and the descending duplicate-run cursor rescan (perf-only); the `createNewPage`
  rollback counter drift was subsequently fixed in
  [#5067](https://github.com/ArcadeData/arcadedb/issues/5067).
- **Storage: flush suspension is refcounted - overlapping backup/verify/snapshot each own their window
  (2026-07 audit follow-up).** `suspendFlushAndExecute` ownership was first-caller-wins: with two
  concurrent suspenders on the same database (SQL `BACKUP DATABASE`, the HA verify endpoint, HA snapshot
  serving - any combination), only the FIRST caller owned the suspend flag while the other still streamed
  the database files. The non-owner skipped the wait for the in-flight flush batch (so it could read a
  page mid-write) and, worse, kept streaming while the owner's exit synchronously flushed the deferred
  batches to disk - torn reads either way. The suspension is now a per-database REFCOUNT: flushing stays
  suspended while the count is above zero and resumes (deferred batches flushed) only when the LAST
  suspender exits, so every caller legitimately owns its whole window; a new suspender arriving while the
  last exit is mid-way through the deferred-batch writes waits until that resume completes. The #4728
  deferred-RAM backpressure accounting is unchanged, and the HA snapshot endpoint keeps its per-database
  serialization (no longer needed for correctness, retained to serialize same-database zip streams and
  keep each suspension window - and therefore the deferred backlog - short)
  ([#5068](https://github.com/ArcadeData/arcadedb/issues/5068)).
- **Transaction commit cleanups (2026-07 audit).** A phase-2 commit failure that happens BEFORE the
  transaction reaches the WAL now restores user-held record state like a phase-1 failure does (rollback):
  records created in the failed transaction get their optimistically-assigned RID reset to provisional and
  modified records are reloaded to their committed content, so retrying with the same in-memory objects
  re-inserts them instead of updating a record that was never persisted; a failure after the WAL append
  still keeps the assigned RIDs, since the transaction is durable and recovery replays it
  ([#4940](https://github.com/ArcadeData/arcadedb/issues/4940)). From the low-severity group
  ([#4959](https://github.com/ArcadeData/arcadedb/issues/4959)): a deferred update whose record was deleted
  by a concurrent transaction now fails the commit with a retryable `ConcurrentModificationException`
  instead of being silently skipped while the rest of the transaction commits (silent partial commit);
  `transaction()` no longer burns every retry attempt (plus retry delays) on a deterministic
  `DuplicatedKeyException` - one retry disambiguates it from a concurrency-induced duplicate. **Behavioral
  change:** `transaction(block, joinTx, attempts)` now caps duplicate-key retries at 2 attempts regardless
  of the `attempts` argument (duplicates are detected against durable state only, so a duplicate that
  survives one retry is deterministic and further attempts just burn time and retry delays);
  `executeLockingFiles` now locks on behalf of the current transaction's requester (thread or session), so
  a thread acting for a session no longer times out on locks its own session already holds; and the
  page-level MVCC isolation contract (no read-set validation: write skew and phantoms possible under both
  levels, unbounded per-transaction page cache under `REPEATABLE_READ`) is now documented on
  `Database.TRANSACTION_ISOLATION_LEVEL` and pinned by tests.
- **Parallel select no longer pins async workers at 100% CPU when the consumer stops draining (2026-07
  audit follow-up).** The native-query `select().parallel()` iterator hands records from its per-bucket
  async browse tasks to the consumer through a fixed 4,096-slot queue, and the producers offered into it
  with an unbounded busy-spin: a consumer that stopped fetching (an exception in user code, an early
  return, or plain abandonment) left one async worker per bucket spinning at 100% CPU forever, and the
  next `Database.close()`/`drop()` hung indefinitely behind the async executor's unbounded
  `waitCompletion()`. The offer is now bounded: producers park briefly between attempts and give up
  cleanly - through the regular task-completion path, preserving the async executor contracts hardened
  with #5062 - when the iterator is closed, the worker is interrupted (executor shutdown), the limit is
  already satisfied, or the consumer makes no progress for the whole stall bound (the select timeout when
  set, otherwise 60s, matching the async executor's zero-progress backstop). A consumer still alive after
  the producers stall out gets a `TimeoutException` instead of a silently truncated result set (unless it
  opted into truncation with a non-throwing timeout), and `SelectIterator` is now `AutoCloseable` so an
  early-terminating consumer can release the producers deterministically. The rework also fixes three
  latent defects in the parallel path: `limit` was accounted on the producer side, so the consumer could
  receive fewer records than requested (down to zero on a fast producer); `skip` crashed with a
  `NullPointerException` (the base constructor consumed the skipped records before the parallel machinery
  existed); and a record published between the consumer's last poll and the final producer's countdown
  could be silently dropped at end of stream. Review follow-ups on the same PR: the select `timeout` is
  enforced on every fetch, including when records are immediately available (it was previously only
  checked on an empty queue, so a slow consumer of an always-full queue could run unbounded past its
  timeout); the consumer got the same spin-then-park backoff as the producers, so a slow producer (e.g. a
  heavy WHERE scanning many non-matching rows) no longer pins the caller thread at 100% CPU; and
  `skip(s).limit(n)` together now returns `n` records after the `s` skipped ones (standard semantics, like
  the ORDER BY path) instead of `n - s` - this last fix is in the shared base iterator, so it corrects the
  serial non-ordered path too ([#5065](https://github.com/ArcadeData/arcadedb/issues/5065)).
- **Low-severity audit residuals (2026-07 audit).** Four small residuals collected from the audit
  follow-up PRs ([#5067](https://github.com/ArcadeData/arcadedb/issues/5067)):
  `TransactionContext.kill()` (test-only API) now releases the file locks acquired by a commit's 1st
  phase instead of leaking the `LockManager` entries until the whole lock manager is torn down (symmetry
  with `reset()`); `LocalBucket.updatePageStatistics` now measures free space against the usable page
  content region like `gatherPageStatistics` does (#4958), instead of the physical page size that
  overstated every page by the page header and skewed the reuse-space threshold; the LSM mutable index
  re-aligns its uncompacted-pages counter with the real page count at commit time, so page creations
  discarded by a rolled-back transaction can no longer inflate the counter and schedule auto-compaction
  early; and the periodic thread-context sweep now opportunistically drops the empty per-thread entries a
  foreign database close leaves behind (a claim/re-check protocol closes the #4939 re-registration race),
  so open/close churn on large long-lived thread pools no longer accumulates them. A file-enumeration
  audit of backup, snapshot and resync paths against the `.wal.corrupt` evidence files confirmed all
  sites use extension allowlists (report on the issue); no changes needed.

- **HA: a distinct error for "committed cluster-wide, local apply failed".** When the replication quorum
  durably commits a transaction but the leader's local phase-2 apply then fails, the application now
  receives `TransactionCommittedRemotelyException` - stating the transaction IS committed on the cluster,
  whether the local pages were reconciled, and that it must NOT be retried - instead of a generic commit
  failure that invited retries. The identities of records created in such a transaction are no longer reset
  to provisional (they are the identities the cluster committed), so an application-level retry no longer
  inserts duplicates of already-committed records. The contract also survives the wire: the HTTP command
  endpoint maps it to 409 Conflict (not a retry-worthy 5xx), and a follower forwarding a write to the
  leader reconstructs the same exception type instead of a generic commit failure
  ([#5064](https://github.com/ArcadeData/arcadedb/issues/5064)).

### Improvements

- **HA: throttled diverged-follower resync logging.** When a follower detects a WAL page-version gap it
  quarantines the affected database and downloads a fresh snapshot from the leader. Previously every
  subsequent committed entry for that database re-logged a full `SEVERE` stack trace (observed in the
  field at tens per second for the whole resync), which both flooded the logs and, on small nodes, stole
  the CPU/IO the snapshot download needed to complete. Now only the **first** gap logs loudly and triggers
  the download; subsequent entries emit a throttled one-line notice (at most once per 5s per database)
  and the redundant per-entry stack trace in `applyTransaction` is suppressed while the database is
  quarantined. Genuine (non-diverged) replication errors still log loudly. Recovery behaviour is
  otherwise unchanged.

## Breaking Changes (migration notes)

### 1. `raftPersistStorage` now defaults to `true` (durable Raft storage)

`arcadedb.ha.raftPersistStorage` now defaults to **`true`**. Previously it defaulted to `false`
(ephemeral) outside Kubernetes - the Raft storage directory was wiped and re-formatted on every server
start.

- **Why:** wiping the Raft log on restart means a follower that was merely lagging (had not yet applied
  every committed entry to its data files) loses the log that would let it catch up. On a full-cluster
  cold restart the cluster then starts a fresh log and treats the elected leader's on-disk database as
  ground truth; the lagging follower applies new deltas on top of stale pages and fails permanently with
  `WALVersionGapException` (page-version gaps), recoverable only by a full snapshot resync. On a
  single-seed cluster, wiping storage can also silently re-form a fresh empty single-node cluster (data
  loss / split brain). Durable storage lets a restarted node rejoin by replaying its persisted log.
  Persisting was already the default under Kubernetes, where a PersistentVolume made it essential
  ([#4835](https://github.com/ArcadeData/arcadedb/issues/4835)); it is now the default everywhere.
- **Impact:** existing HA deployments that relied on the wipe-on-restart behaviour will now preserve the
  Raft storage directory across restarts. This is the safer, faster path (log replay instead of forced
  full resync) and requires no action for most operators. The Raft storage directory now persists on
  disk between restarts; ensure it lives on durable storage (see `arcadedb.ha.raftStorageDirectory`).
- **Migration / opt-out:** a throwaway or test cluster that really wants ephemeral storage can still opt
  out explicitly with `arcadedb.ha.raftPersistStorage=false` (config file, server settings, or
  `-Darcadedb.ha.raftPersistStorage=false`). An explicit value is always honored.

### 2. Bolt: temporal values now use native PackStream structures (not ISO-8601 strings)

The Neo4j Bolt wire protocol now carries temporal values (`date`, `time`, `localtime`, `datetime`,
`localdatetime`) as **native PackStream temporal structures** in both directions, instead of the
previous ISO-8601 strings. Inbound datetime query parameters are decoded to `java.time` values
(previously silently dropped), and outbound temporal properties are returned as native structs
([#4905](https://github.com/ArcadeData/arcadedb/issues/4905),
[#4907](https://github.com/ArcadeData/arcadedb/issues/4907)).

- **Why:** a Neo4j-compatible driver now sends and receives real temporal values (`ZonedDateTime`,
  `LocalDate`, ...), matching Neo4j semantics, so temporal query parameters bind and temporal
  comparisons (`WHERE e.valid_at <= $ts`) work without any client-side conversion.
- **Impact:** a Bolt client that previously read a temporal property as a `String` (the ISO text) will
  now receive a native temporal type (e.g. `Value.asZonedDateTime()` / `asLocalDate()`), the same as
  against Neo4j. Clients relying on the old string form must read the native temporal instead.

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.1...26.7.2
