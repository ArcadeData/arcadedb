# Federated Databases - Sharding via Per-Database Ratis Groups

**Status:** Design / planning (not yet implemented). Author: Luca Garulli. This is a design document for a future ArcadeDB feature. It records the architecture, the decisions taken so far, the concrete code touch-points, and open questions, so work can be resumed later. Nothing here has been built yet.

## Context

We want ArcadeDB to distribute data across servers with real sharding: each shard lives on a chosen subset of servers (not replicated everywhere), a key hash routes a record to its owning shard, and cross-database RIDs let records/edges reference other shards. This is the foundation for horizontal scale-out (write throughput + storage beyond one node).

**The blocker today:** HA runs exactly ONE Raft group per cluster (`RaftGroupId` derived from the cluster name, `RaftHAServer.java:234`); `ArcadeStateMachine` multiplexes *every* database onto that single shared log, so **every database is replicated to every server** and there is one cluster-wide leader. There is no way to place a database on a subset of servers, no per-database leader, and no replication factor. So the load-bearing capability is **per-database placement**, and everything else (key routing, cross-DB RIDs, distributed transactions) stacks on top.

### Decisions locked with Luca
1. **Placement first.** Phase 1 = convert ha-raft from one shared group to **per-database Ratis groups**, where a group's member set IS its placement. Cross-DB RID / read routing / 2PC come after.
2. **Fixed over-provisioned slot-databases.** Pick a fixed slot count up front (e.g. 64), each slot = its own database = its own Ratis group. Hash(key) -> slot -> shard. This makes rebalancing cheap (see below).
3. **Manual, operator-driven placement** in the first cut (declare/trigger; the system executes the Ratis membership changes). Automatic balancer later.
4. **Affinity-key placement is the default and preferred way to decide which vertex lands in which shard** (not hash of the vertex's own primary key). See "Data placement & graph locality" below. Structure-aware heuristics are an opt-in fallback for graphs with no natural locality attribute.

### Why this generalizes rather than breaks today's behavior
Going multi-group subsumes the current model: a group's members are a *choice*. Members = all servers reproduces today's full replication exactly; members = a subset is a placed shard. So "replicated database" and "sharded database" become the same mechanism at different membership. A key win falls out for free: per-database snapshot/resync (fixes the `RaftHAServer.java:372` TODO where one stuck DB forces full resync of all) and per-database leaders (write scale-out even for co-located DBs).

### How this compares to ScyllaDB / DynamoDB
Both hash the partition key into a fixed token/slot space and place slots on nodes; adding nodes moves slots, not individual keys. We adopt the same idea, adapted to a physical-pointer graph DB: **the slot is also the unit of storage (a database/Ratis group)**, so moving a slot moves a whole database's replicas via Raft membership - the physical RID (`bucket:offset`) inside that slot-database never changes, so no RID is ever rewritten on rebalance. That property is what a graph DB needs and what neither Scylla nor Dynamo has to worry about (they look records up by key hash, not by stored pointer).

### The rebalancing model (design everything toward the cheap operation)
- **Physical rebalancing (everyday, online):** move a shard's replicas to different servers. A shard = a database = a Ratis group; to relieve a hot/full server, `GroupManagementApi.add(...)` a target server to that group (Ratis streams it a snapshot + log tail - the InstallSnapshot machinery already exists) then `remove(...)` the old one. The shard's slot id / database name is unchanged, so **every RID referencing it stays valid; zero record moves, zero RID/edge rewrites.** Adding capacity = add servers, relocate some slot-groups onto them.
- **Logical resharding (rare, heavy, avoid):** only needed if you exceed the provisioned slot count. Physically moves records into a new database = new RIDs + edge fixups. Over-provisioning slots up front makes this a rare maintenance event.
- **Granularity tradeoff:** slot count is sized to the largest planned cluster. 64 slots at RF 3 on 10 servers ~= ~19 Raft groups/server (fine for multi-raft); 1024 slots ~= ~300/server (heavier). Start modest, make it configurable and immutable per federation.

---

## Data placement & graph locality (which vertex goes in which shard)

The rebalancing model above answers *where a shard lives*. This section answers the other half: *which vertices and edges go into which shard*. For a graph DB this is the hard part, and getting it wrong makes sharding actively harmful.

### The problem: hashing the primary key maximizes edge-cut
Hashing a vertex's own primary key is perfectly balanced and client-computable, and it is the **worst** possible choice for locality. It ignores graph structure, so every edge becomes a coin flip on whether its two endpoints share a slot: with N slots, roughly `(N-1)/N` of all edges cross a shard boundary, and every traversal hop turns into a network round trip. Balanced minimum-edge-cut partitioning is NP-hard, so no system solves it exactly online; they pick a heuristic family instead.

### The tension: computable placement vs good locality
- **Hash(PK)** - placement is a pure function of the key. Client computes it, no lookup. Perfect balance. Terrible locality.
- **Structure-aware** (place a vertex near its neighbors) - great locality, but placement now depends on *graph state*, not on the key, so the client can no longer compute it and an insert must consult where the neighbors live.
- **Affinity key** - hash a coarser *business* attribute instead of the vertex's own identity. Still computable, and it colocates by design.

The "client hashes the key and routes" goal lives on the computable side, so the default must stay computable.

### Family 1 (DEFAULT, chosen): affinity-key placement
Do not hash the vertex identity. Hash a declared **locality attribute** that whole subgraphs share: `tenantId`, `customerId`, `regionId`, `accountId`, a root-entity id. Every vertex carrying the same affinity value lands in the same slot, so that subgraph is single-shard and traversals within it never leave the node. This is what Neo4j Fabric (manual/app-driven), JanusGraph's `PropertyPlacementStrategy`, and most production graph deployments rely on. It is the 80/20.

**Reuse already in the codebase:** `PartitionedBucketSelectionStrategy` (`engine/.../database/bucketselectionstrategy/`) already computes `(hash & 0x7fffffff) % total` over *declared property names*, enforces "the partition key cannot be modified," and its javadoc already anticipates the graph case ("If the record is part of a graph, then the edges will be updated accordingly."). That is affinity-key placement today at *bucket* granularity. Federation is the same idea lifted from bucket to **slot** granularity. The "cannot modify the partition key / else rewrite edges" caveat is the same RID-rewrite cost noted below, so the constraint is already understood in the engine.

- Skew caveat: affinity groups are uneven (a whale tenant is 1000x a small one), so a blind `hash(affinity) % slots` can hotspot. Mitigation: map affinity -> slot via the catalog with the ability to pin/split a hot affinity across a few slots, rather than a pure modulo.

### The one rule to adopt regardless of family: edges follow their source vertex
Store each edge in the **same slot as its source vertex**, deterministically (`edge.partition = sourceVertex.partition`). Then "give me the out-edges of V" is always single-shard, whatever the vertex placement. Nebula Graph does exactly this. ArcadeDB already stores edges in the vertex's own edge-linked-list, physically adjacent (`EdgeLinkedList`, `EdgeSegment`), so this rule is nearly free. The unavoidable cut is only at the *far* endpoint: following an edge to a target vertex on another slot. That cannot be eliminated, only minimized (families 1-2) and made cheap when it happens (batch + pipeline the remote fetches - TigerGraph's "don't fight the cut, make crossing fast" philosophy as the backstop).

### Family 2 (opt-in fallback): structure-aware streaming
For arbitrary graphs with no business locality attribute, decide placement at **insert time** with an online streaming heuristic (not offline repartition):
- **Linear Deterministic Greedy (LDG)** - place a new vertex in the slot holding the most of its already-known neighbors, penalized by how full that slot is. O(degree) per insert, incremental.
- **FENNEL** (Microsoft Research) - same shape with a tunable knob trading edge-cut against balance; state of the art for streaming.

Price: placement now depends on neighbor locations, so an insert must go through a coordinator that looks up where the neighbors live. Not client-computable. Acceptable for an advanced mode, not for the default. Decide-once at insert; do **not** continuously re-migrate to chase a better cut (that means RID rewrites - see constraint below).

### Family 3 (orthogonal, will be needed): vertex-cut for supernodes
Real graphs are power-law: a few celebrity vertices have millions of edges, and any vertex-placement scheme dumps that whole edge list into one slot. The fix is **vertex-cut / edge partitioning** (PowerGraph, HDRF): partition edges and mirror the high-degree vertex across the slots that hold its edges. JanusGraph ships this as "vertex partitioning." ArcadeDB's segmented edge lists (`EdgeSegment` / `EdgeLinkedList`) are a natural fit: a supernode's segments can span slots with the vertex mirrored, instead of one slot swallowing a billion edges.

### The constraint our own RID model imposes (this decides the strategy)
Because `partitionId` is baked into the `DistributedRID` (Phase 3), **moving a vertex to a different slot changes its RID**, which forces rewriting every edge that points to it - the expensive logical resharding we want to avoid. Consequence:

> Placement must be **good at insert time**, because migrating a vertex later is costly. Prefer decide-once-and-never-move strategies; offline "repartition the whole graph with METIS then migrate" is exactly what this architecture punishes.

This is the deciding reason affinity-key ranks first: it is a stable function of an immutable attribute, so a vertex's slot is fixed for life (no migration) and it stays client-computable. LDG/FENNEL decide well at insert but can drift as the graph grows, and acting on that drift costs RID rewrites, so they are "make the insert-time decision as good as possible," never "continuously rebalance vertices."

Nuance that helps: placement does not need to stay computable *after* creation. Once a vertex exists, every edge references it by full `DistributedRID` (partition included), so traversal needs no directory. Key-computability only matters for routing the **insert** and for **finding a vertex by business key** when you don't hold its RID - and the latter resolves through the unique index (key -> RID -> partition). So structure-aware placement is technically compatible; it just costs the client fast-path on insert.

### What the field converged on
| System | Graph placement |
|---|---|
| **JanusGraph** | partition id encoded in vertex id (our `DistributedRID` idea); `PropertyPlacementStrategy` colocates vertices sharing a property (affinity key); supernode splitting for hotspots. |
| **Nebula Graph** | `hash(srcVID)`; edges stored with their source - 1-hop out is always local. |
| **Dgraph** | shards by **predicate** (edge type), not vertex - a whole relationship type is single-shard. Orthogonal axis worth knowing. |
| **Neo4j Fabric** | no automatic partitioning; the application decides shard placement (affinity by convention). |
| **TigerGraph** | automatic hash partitioning; does not minimize cut, invests in a fast MPP traversal engine instead. |
| **Amazon Neptune** | does not shard the write graph at all - single primary, scale reads via replicas. A signal of how hard this is. |

### Placement recommendation for ArcadeDB
1. **Default = affinity-key placement.** Declare a partition property per type (reuse and lift `PartitionedBucketSelectionStrategy` from bucket to slot). `hash(affinityKey) % slots`, stable and client-computable. Fall back to `hash(PK)` only for types the user explicitly does not want colocated (pure document/KV, no traversals).
2. **Edges inherit their source vertex's slot**, deterministically. Nearly free given the edge-list storage; makes 1-hop out single-shard always.
3. **Vertex-cut for supernodes**, using the existing edge-segment structure to spread a hot vertex's edges.
4. **Optional streaming LDG/FENNEL** for advanced users with no affinity key - documented as coordinator-routed inserts, decide-once (no continuous re-migration).
5. **Accept the residual cut and make crossing cheap** (batched, pipelined remote endpoint fetch) as the backstop.

Net: the answer to "I need something better than hash(PK)" is not one algorithm - it is *let the user declare the locality* (affinity key) and default the graph's edges to follow their vertices. That colocates by design, stays client-computable, respects the immutable-RID constraint, and matches what JanusGraph / Neo4j / Nebula converged on.

---

## Ratis multi-group is natively supported (verified against the 3.2.2 jars in `.m2`)
- `RaftServer.Builder.setStateMachineRegistry(StateMachine.Registry)` where `StateMachine.Registry = Function<RaftGroupId, StateMachine>` - hosts many groups on one `RaftServer`, one state machine per group. Replaces today's single `setStateMachine(...)`.
- `RaftServer.getGroupIds()` / `getGroups()` / `getDivision(groupId)` - already abstract; the server can already address multiple groups.
- `GroupManagementApi.add(RaftGroup)/remove(groupId,...)/list()/info(groupId)` - create/destroy a group at runtime on a chosen peer subset = placement + rebalancing. ArcadeDB already calls this API (`KubernetesAutoJoin.java:113`).

So the single-group design is a choice, not a Ratis limitation.

---

## Phase 1 - Per-database placement (the deliverable)

### 1a. Multi-group conversion (behavior-preserving refactor)
Files: `ha-raft/.../RaftHAServer.java`, `ArcadeStateMachine.java`, `RaftReplicatedDatabase.java`, `RaftTransactionBroker.java`
- Replace `RaftServer.newBuilder().setStateMachine(single)` (`RaftHAServer.java:561-564,929-932`) with `setStateMachineRegistry(groupId -> stateMachineForGroup(groupId))`, lazily instantiating one `ArcadeStateMachine` per group.
- Derive each group's `RaftGroupId` from the **database name** (`nameUUIDFromBytes(dbName)`), not the cluster name (`:234`).
- Re-key the ~8 `getDivision(raftGroup.getGroupId())` leader/commit/apply sites (`:1036,1051,1429,1459,1750,1760,1770,1957`) and the resync loop (`:372`) to operate per shard group. `ArcadeStateMachine` stops multiplexing - each instance owns exactly one DB, so `appliedIndexByDb` collapses to a single scalar.
- **Initial member set = all servers**, so this step is externally behavior-identical to today (each DB its own group spanning the full peer list), giving a safety checkpoint before placement changes anything.
- **On-disk migration (key risk):** existing clusters have one shared Raft log holding all DBs; per-DB groups need per-DB logs. Approach: on upgrade, take a per-database snapshot from the current state, seed each new per-DB group from its snapshot, retire the shared log. Fresh clusters skip this. Gate behind an explicit migration step; document rollback.

### 1b. Placement = subset membership
- Allow a database's group to span a **subset** of the peer list. `createDatabase(name, members)` commits `GroupManagementApi.add(RaftGroup.valueOf(dbGroupId, subset))` only on the target members (today's `INSTALL_DATABASE_ENTRY`/`createInReplicas` hits the whole group - `RaftReplicatedDatabase.createInReplicas():1686`, `RaftTransactionBroker.replicateInstallDatabase():116`, `ArcadeStateMachine.applyInstallDatabaseEntry():1334`).
- A database is either **REPLICATED** (members = all servers, = today) or **PLACED** (members = subset). Store the mode + member set in the catalog and in the DB's schema/metadata.

### 1c. Per-database leader resolution + routing
- Add `getLeaderOf(dbName)` / `getReplicasOf(dbName)` reading that DB's group division; generalize the cluster-wide `getLeaderId/Name/HttpAddress` (`RaftHAServer.java:1031-1197`).
- Generalize `GET /api/v1/server?mode=cluster` (`GetServerHandler.java`) and follower->leader forwarding (`RaftReplicatedDatabase.forwardCommandToLeaderViaRaft:1731`, `ServerIsNotTheLeaderException` carrying the leader URL) to be **per-database**. Needed as soon as placement exists: once a DB lives on a subset, a client hitting a server that does not host it must be forwarded to one that does. Reuse `RemoteHttpComponent.requestClusterConfiguration()` on the client (now per-database).

### 1d. Manual rebalancing (online)
- Admin API/command to move a shard: `GroupManagementApi.add(server)` then `remove(oldServer)` on that DB's group; Ratis handles the data transfer. Generalize the existing `PostAddPeerHandler`/`PostLeaveHandler`/`DeletePeerHandler` (single-group) to target a specific database's group.

### 1e. Federation catalog (slot -> database + placement)
New `server/config/federated.json`, loaded in `ArcadeDBServer.loadConfiguration()` (~line 957), versioned + hot-reloadable like `server-groups.json`:
```json
{
  "version": 1,
  "routingEpoch": 1,
  "slots": 64,
  "hashAlgorithm": "murmur3",
  "shards": {
    "shard00": { "slots": "0-15",  "members": ["server1","server2","server3"] },
    "shard01": { "slots": "16-31", "members": ["server4","server5","server6"] },
    "shard02": { "slots": "32-47", "members": ["server7","server8","server9"] },
    "shard03": { "slots": "48-63", "members": ["server1","server5","server9"] }
  }
}
```
- Names only shard **databases** and their **member server names** (resolved to host:port via the existing `HA_SERVER_LIST` maps in `RaftHAServer`). No host/port duplication; leaders/topology come from HA at runtime.
- `slots` fixed at init (immutable). `routingEpoch` bumps on any placement change - the seam for fencing in-flight routing during a move and for later 2PC.
- New classes `server/.../federation/FederationManager` (+ `FederationCatalog`, `ShardDescriptor`). `GlobalConfiguration.FEDERATED_ENABLED` (SCOPE SERVER, default false) keeps the whole feature dormant otherwise.

### Phase 1 verification
- **Behavior-preserving check (1a):** existing HA integration tests (extend `ha-raft` / server HA suites) pass unchanged with per-DB groups where members = all servers; assert each DB now has its own group id via `getGroups()` and its own leader.
- **Placement (1b):** 3-server test cluster; create a PLACED database on 2 of the 3; assert the third has NO local copy (`existsDatabase` false there, no files) and the two do; assert writes go through that DB's own leader.
- **Routing (1c):** client hits a server that does not host the placed DB; assert it is forwarded and the query succeeds; kill the DB's leader, assert re-election within its group and routing follows.
- **Rebalance (1d):** move a placed DB from server B to server C online; assert data present on C, gone from B, no downtime, and RIDs of records in that DB are unchanged before/after.
- **Migration (1a):** open a pre-upgrade single-group database dir, run the migration, assert all DBs readable as per-DB groups with identical record content.
- Framework: JUnit5 + AssertJ; reuse `BaseGraphServerTest` and the existing multi-server HA test harness. Tag long multi-server tests `@Tag("slow")`.

---

## Later phases (summarized - detailed when we get there)

### Phase 2 - Key/write routing
Hash(partition key) -> slot -> shard database, reusing `PartitionedBucketSelectionStrategy`'s `(hash & 0x7fffffff) % slots` idea (`FederationManager.slotForKey`). The partition key is the **affinity key** by default (see "Data placement & graph locality"), not the vertex's own primary key. Smart client fast-path (client hashes, talks to the shard leader) + coordinator forwarding (any server forwards a misrouted write). Inserts land in the owning shard; edges land in their source vertex's shard. Shards are independent here (no cross-shard edges yet), matching Dynamo/Scylla's model.

### Phase 3 - Cross-database RID (the read-path federation)
Make records/edges reference other shards. Key pieces (designed earlier, now correctly sequenced after placement):
- **`DistributedRID extends RID`** carrying an `int partitionId` (base `RID` keeps its two fields - no per-instance RAM cost for the common local case, mirroring `DatabaseRID`). Base gains a virtual `getPartitionId()` (returns `LOCAL_PARTITION = -1`) and `isFederated()`; `equals/hashCode/compareTo` use the accessor so a `DistributedRID(#5:12:345)` never equals a plain `RID(#12:345)`. Format `#partitionId:bucket:offset`; 3-part parsing routes through a factory (a constructor can't return a subclass), plain `#b:o` stays a plain `RID`.
- **New binary type `TYPE_FEDERATED_COMPRESSED_RID = 32`** (next free byte; current max 31) writing three varints. Chosen only when `rid.isFederated()`, so non-federated on-disk bytes stay byte-identical (zero overhead, backward compatible). Fixed-width vertex edge-head pointers and edge-segment NEXT pointer stay unchanged (verified always local - and by the "edges follow source vertex" rule, an out-edge is always in the same slot as its vertex, so these stay local by construction).
- **Read-path routing** in `LocalDatabase.lookupByRID`: a foreign `partitionId` -> catalog -> shard DB -> HA topology -> `RemoteDatabase` fetch; local short-circuits. Cross-partition traversal works because endpoints resolve through `lookupByRID`. Scatter-gather SELECT fans partition-unpruned queries to each shard and merges.
- **Identity normalization** (biggest correctness risk): normalize a foreign RID to LOCAL form on arrival at its owning node so a record never has two identities. Confirm before coding.

### Phase 4 - Distributed ACID (cross-shard transactions)
Not too ambitious - CockroachDB, Spanner, YugabyteDB and TiDB all ship sharding + replication + distributed ACID in production. The fear ("2PC will make writes unusably slow") is a fear about the *wrong* 2PC. The design must follow these principles:

- **Single-shard is the fast path, and it is the common case.** A transaction touching one slot-group commits with one local Raft round - exactly today's HA commit, no distributed protocol. Only cross-shard transactions pay coordination cost. This is why affinity-key placement matters for *transactions*, not just traversals: colocate the data that is written together and the overwhelming majority of transactions stay single-shard. Get the affinity key wrong (every txn fans across shards) and no protocol saves you.
- **2PC always layered over Raft, never a bare blocking coordinator.** The two things that made classic 2PC unusable are fixed by consensus: (1) the transaction record is itself Raft-replicated, so a dead coordinator is recovered by another replica reading the record - no indefinitely-held locks; (2) each participant is a replicated group, so "prepare" durability is the quorum write you already pay for HA. The `routingEpoch` in the catalog is the fencing seam.
- **MVCC + write-intents** so reads never block writers and commit is a status-flag flip rather than a data rewrite. This is the biggest single win and is independent of 2PC.
- **Optimizations to design toward (add after the basic protocol works):** Parallel Commits (CockroachDB) collapses a distributed commit to ~1 consensus round by making the commit condition "all intents replicated"; a 1PC fast path skips 2PC entirely when a transaction turns out to touch a single shard.
- **Role models:** CockroachDB for the transaction protocol (same primitive we already have - each shard is a Raft group - so their published techniques stack directly on top). ScyllaDB is the model for the *placement* layer (token ring / vnodes), NOT for transactions: it deliberately chose AP / eventual consistency and offers no cross-partition ACID, so don't let its choice suggest sharding and ACID are incompatible. The deterministic-ordering alternative (Calvin / FaunaDB / VoltDB) is a poor fit here because it needs the read/write set up front, which graph traversals discover as they go.
- **Consistency guarantee to state explicitly:** without Spanner-style TrueTime we offer serializable-via-Raft-timestamps (Cockroach/Yugabyte style), not globally linearizable external consistency. That is the right and cheaper choice for a self-hosted DB; document precisely which isolation/consistency level cross-shard transactions provide, because that is the line users will scrutinize.

Keep all remote ops behind `FederationManager` as the single integration point.

---

## Critical files (Phase 1)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` (group registry, per-DB group ids, per-DB leader/commit/apply, membership)
- `ha-raft/.../ArcadeStateMachine.java` (de-multiplex: one instance per group)
- `ha-raft/.../RaftReplicatedDatabase.java`, `RaftTransactionBroker.java` (per-group create/commit/forward)
- `server/.../http/handler/GetServerHandler.java`, `PostAddPeerHandler.java`, `PostLeaveHandler.java`, `DeletePeerHandler.java` (per-database topology + membership)
- `server/.../ArcadeDBServer.java` (catalog load) + new `server/.../federation/FederationManager.java` (+ `FederationCatalog`, `ShardDescriptor`)
- `engine/.../database/bucketselectionstrategy/PartitionedBucketSelectionStrategy.java` (affinity-key primitive to lift from bucket to slot granularity)
- `engine/.../graph/EdgeLinkedList.java`, `EdgeSegment.java` (edges-follow-source rule; supernode vertex-cut)
- `network/.../remote/RemoteDatabase.java` + `RemoteHttpComponent.requestClusterConfiguration()` (per-database routing/discovery, reused)
- `engine/.../GlobalConfiguration.java` (`FEDERATED_ENABLED`), `server/config/federated.json` (new)

## Open questions to resolve during Phase 1
- **On-disk migration** from the shared log to per-DB groups: online vs offline; is an offline one-time migration acceptable for the first release?
- **Group-count ceiling:** validate multi-raft overhead at the intended slot count on a representative box before fixing the default slot count.
- **Reserved/system databases** (e.g. `.raft`, security config): stay REPLICATED on all servers; confirm they are excluded from slot placement.
- **Client compatibility:** non-federation-aware clients hitting a placed DB rely on coordinator forwarding; confirm the forward path covers all query/command/batch endpoints.
- **Affinity key ergonomics:** how is the affinity/partition key declared (schema DDL on the type? a federation-level mapping?), how is "no affinity key -> fall back to hash(PK) or streaming heuristic" surfaced, and how are cross-type affinities kept consistent (a vertex and its edges must resolve to the same slot).
- **Supernode threshold:** at what degree does a vertex get vertex-cut / mirrored, and is that automatic or operator-declared per type/label.
