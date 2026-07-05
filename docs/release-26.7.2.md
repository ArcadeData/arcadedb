# ArcadeDB v.26.7.2 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.7.2 development cycle, so the release notes are ready at tag time.

This release hardens **High Availability (Raft)** recovery. Raft storage is now **durable by default**
(previously ephemeral outside Kubernetes), which removes a class of permanent follower divergence after a
full-cluster cold restart - see **Breaking Changes** below. It also removes a diverged-follower log flood
that could starve the very snapshot resync meant to heal the node.

### Fixes

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
