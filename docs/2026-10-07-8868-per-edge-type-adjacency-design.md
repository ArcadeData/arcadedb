# Per-edge-type adjacency for supernodes: design

- Issue: #9264 (design step of the #9269 roadmap, problem statement #8868)
- Status: **PROPOSED, awaiting maintainer sign-off**. Nothing in this document is implemented. No code may depend on it
  until it is merged.
- Release of this document: 26.11.1. Implementation: read support 26.12.1 (#9267), opt-in writer 27.1.1 (#9268),
  default ON later (#9263).

## 1. Problem

A type-filtered hop on a promoted vertex, for example `in('Parent')` on a vertex with 883,209 incoming edges of which
one is `Parent`, costs O(total degree in that direction). #8417/#8870 removed the constant factor (a foreign entry is
rejected on its raw bucket number by `EdgeSegment.nextEntryInBuckets` against an `EdgeBucketMask`, without decoding
two `RID`s), but every chunk of every chain is still loaded and every entry's bucket number is still read. The goal is
**O(edges of the requested types)**, with a bounded constant for the part of the list written before the vertex
switched layout.

### What exists today (verified in the tree at `97405375df`)

| Piece | Where | Relevant facts |
|---|---|---|
| Classic list | `EdgeLinkedList`, `MutableEdgeSegment` (record type `3`) | One chain per vertex and direction. Each chunk: `[type:1][used:int][previous:RID]` then entries `(edgeRID, vertexRID)` as compressed RIDs, newest first. The vertex record holds the head RID (`getOutEdgesHeadChunk`/`getInEdgesHeadChunk`). |
| Promotion | `EdgeLinkedList.tryPromoteToSuperNode` | At a chunk-full, when the estimated degree reaches `GRAPH_SUPERNODE_THRESHOLD` (default 4096) and `GRAPH_SUPERNODE_STRIPES` >= 2 (default 16), the vertex head is flipped to a new `StripeDirectory`. |
| Stripe directory | `StripeDirectory` (record type `7`) | `[type:1][hashVersion:1][generations:1]` then per generation `[stripes:int][stripes x (bucketId:int, position:long)]`. Generation 0 is the pre-promotion classic chain, generation 1 the stripes. Entries are placed by `stripeOf(neighbourRID, stripes)`. Fixed width, slots updated in place. Lives in the same bucket as the classic chain. Stripe chains live in the per-vertex-type pool `<type>_sn_stripe_<slot>` (`InternalBucketNaming.superNodeStripeBucketName`). |
| Striped list | `StripedEdgeList` | Routes each operation to the chains of the directory: neighbour-keyed operations visit one stripe per generation (`chainsForNeighbour`), read walks interleave the stripes of a generation (`interleaved`, `InterleavedIterator`), maintenance walks concatenate (`allChains`). Plain appends never anchor or write the directory; only a stripe head flip does (`loadDirectoryForWrite` + `updateSlot`, which poisons the page for the edge-append merge). |
| Type filter | `EdgeBucketMask` | A filter is a set of **edge bucket ids** (subtypes included), resolved from the schema at query time. |
| Lightweight edges | `MutableLightEdge` | Stored as the marker RID `#<type first bucket>:-1`, so their entry's bucket id is the type's first bucket. |
| Record factory | `RecordFactory` | Record types `0..7` are taken (`0` document, `1` vertex, `2` edge, `3` edge segment, `4` embedded, `5` external value, `6` light edge, `7` stripe directory). Any other type throws `DatabaseMetadataException("Cannot find record type ...")`. This is true in every release back to at least 26.7.3. |
| Hash version check | `StripeDirectory.checkHashVersion` (#9211) | Present from 26.10.1. **Not** present in 26.8.1 and 26.9.1: they read any hash version with the version-0 placement. |
| Orphan reclaim | `GraphDatabaseChecker.reclaimOrphanedEdgeSegments` | Fails closed database-wide when any vertex head cannot be walked. Present (with `GraphDatabaseCheckerReclaimFailClosedTest`) since 26.8.1. |
| HA | `ha-raft` | `TX_ENTRY` replicates **pages**, so a follower never decodes an edge-list record while applying a transaction. It decodes it only when something reads the vertex. Peer capabilities: `PeerCapabilities` (tokens), `PeerCapabilityRegistry`, `RaftHAServer.allPeersSupport` / `peersMissingCapabilityNow`. |
| Merges | `TransactionContext.trackEdgeAppend` / `poisonEdgeAppendPage`, `LocalBucket` slot merge | In-chunk appends commute (edge-append merge). Any record that is not an `EdgeSegment` is a slot-merge candidate (`LocalBucket.isSlotMergeCandidate`), so a `StripeDirectory` rewrite merges with a concurrent rewrite of a **different slot** on the same page. |

## 2. Decisions

Each subsection is one item of #9264, with the decision first and the reasoning after it.

### 2.1 Layout: per-edge-type chains for promoted vertices only

**Decision.** Only a vertex whose list is promoted (the `GRAPH_SUPERNODE_THRESHOLD` path) gets per-type chains.
Low-degree vertices keep the single classic chain unchanged, on every release.

**Why.** Below the threshold a filtered walk reads at most ~4096 entries, a few chunks, and #8417 already skips foreign
entries without decoding them. Splitting every vertex would multiply records and pages for no measurable gain and
would make the format change touch every database instead of only the ones with supernodes.

**Rejected: per-segment type summary** (a bucket bitmap or count in each chunk). A filtered walk still has to load
every chunk to find the next one, because the `previous` pointer lives inside the chunk. It saves decoding, which
#8417 already made cheap, not page reads.

### 2.2 Record type: a new record type `8`, not a `HASH_VERSION` bump

**Decision.** The new per-vertex root is a new record, `EdgeTypeDirectory`, with `RECORD_TYPE = 8`.
`StripeDirectory.HASH_VERSION` stays `0`.

**Why.** Releases 26.8.1 through 26.9.x do not read the hash-version byte. A directory written with a new placement
under record type 7 would be read by them with the version-0 hash: `isConnectedTo`, `containsVertex` and
`removeVertex` would look in the wrong stripe and silently answer "not connected" or miss a removal. That is fail
open. Record type 8 is rejected by `RecordFactory` on every existing release, which is fail closed.

What "fail closed" means concretely on an older release that opens a database containing type-8 records:

| Operation on the older release | Result |
|---|---|
| Open the database | Succeeds. Records are decoded lazily. |
| Any read or write of a vertex whose head is a type-8 root, in that direction | `DatabaseMetadataException: Cannot find record type '8'`. |
| Vertices that never converted | Work normally. |
| `CHECK DATABASE` | Reports the vertex. The orphan reclaim is skipped database-wide (fail closed since 26.8.1), so the chains behind the root are **not** deleted as orphans. |
| A bucket scan that decodes every record of an edge-list bucket | Throws on the first type-8 record. |

### 2.3 Binary layout

#### 2.3.1 Edge-type key: the edge bucket id

**Decision.** Per-type chains are keyed by the **bucket id of the entry's edge RID**. A lightweight edge's key is its
marker's bucket, the type's first bucket. The reserved key `-1` holds any entry whose edge bucket is not a
non-negative id. No current writer is known to produce one, but the key exists so that such an entry can never be
dropped. A filtered walk never visits key `-1`, the same as `EdgeBucketMask`, which rejects negative ids today.

**Why not a type id.** The schema has no persistent numeric type id, and type names change on `ALTER TYPE ... NAME`.
The bucket id is already what the filter matches on (`EdgeBucketMask.of` resolves type names, subtypes included, to
bucket ids at query time). Keying by bucket therefore gives **exactly** the filtered results of the classic layout,
including after any schema change that moves the type-to-bucket mapping, because both resolve the mapping at query
time and both look at the bucket the edge was written to.

**Cost.** One key per edge bucket that has an edge at this vertex. With `arcadedb.typeDefaultBuckets` = 1 (the
default) that is one key per edge type. A type created with N buckets gets up to N keys per vertex, and a filtered hop
on it visits N small chains instead of one. This is accepted. Merging keys by type at write time would tie the on-disk
key to the schema at write time, which is the property rejected above.

**Inherited, not introduced.** `FileManager.newFileId()` appends to the in-memory file list, so ids are not reused
within one run. After a reopen the list is rebuilt from the files on disk, so the id of a dropped bucket that had the
highest id can be handed out again. Entries left behind by a forcibly dropped edge type would then match the new
type's filter. The classic layout has the same exposure today, because it also matches on the bucket number. This
design neither fixes nor worsens it.

#### 2.3.2 The root record (type 8)

The vertex head pointer of a promoted direction points to the root. The root never holds a chain head itself, so it
is rewritten only when a key is added, when a key's sub-directory is replaced (2.3.4), and when the drain (2.3.5)
finishes. Plain appends and head flips never touch it.

```
offset  size  field
0       1     recordType      = 8
1       1     formatVersion   = 0   reader rejects any other value (fail closed, see 2.3.6)
2       1     flags           = 0   reserved, reader rejects any non-zero bit
3       4     legacyBucketId        (-1, -1) = no legacy part
7       8     legacyPosition        else the RID of a type-7 StripeDirectory or of an EdgeSegment chain head
15      4     keyCount
19      ...   keyCount x key entry, ascending by edgeBucketId, unique (binary search):
              edgeBucketId       int
              subDirBucketId     int    RID of the key's sub-directory (a type-7 record, see 2.3.3)
              subDirPosition     long
```

Size: `19 + 16 x keys` bytes. 50 edge types at one vertex is under 1 KB.

Fixed-width integers, like type 7: the record is small and rarely written, and fixed offsets keep the reader simple
and the slot merge byte-exact.

The root lives in the same bucket as the vertex's classic chain, like the type-7 directory today.

#### 2.3.3 Per-key sub-directory: type-7 generations nest unchanged

**Decision.** Each key points to its own `StripeDirectory` (type 7, `HASH_VERSION` 0), used with these generation
roles:

| Generation | Role | Created |
|---|---|---|
| 0 | **Archive**: entries of this key moved out of the legacy part by the drain (2.3.5). `GRAPH_SUPERNODE_STRIPES` slots, placed by `stripeOf(neighbour)`, so a neighbour lookup reads one archive stripe and not the whole archive. | With the sub-directory, every slot empty `(-1, -1)`: an empty slot costs 12 bytes and no chunk. |
| 1 | **Live, single chain.** Every append of this key goes here until the key itself is hot. One slot. | With the sub-directory, holding the first chunk. |
| 2 | **Live, striped.** `GRAPH_SUPERNODE_STRIPES` slots, placed by `stripeOf(neighbour)`. | When generation 1 of this key crosses the supernode threshold (2.3.4). |

**Why nest type 7.** A key with its sub-directory **is** a type-7 striped list restricted to one edge bucket. The
whole of `StripedEdgeList` applies to it unchanged: neighbour-keyed lookups (one stripe per generation), the
interleaved read walk, the lazy slot, the stripe head flip through an anchored, poisoned directory write, and the
stale-head retry. The new code is a router over keys, not a second copy of the striping logic. The type-7 format
already allows an empty slot and any number of generations up to 127, so nothing in the format changes, and
`HASH_VERSION` 0 placement is reused as is.

**Why it stays fail closed.** An older release reaches a nested type-7 record only through a type-8 root, which it
cannot read. A nested sub-directory is never a vertex head, so no older code path treats it as one.

**Three implementation changes to type-7 code that this needs** (both internal, no format change):

- A `StripeDirectory` constructor for a sub-directory: generation 0 with empty slots, generation 1 with one slot.
- An append that targets a given generation instead of the newest one, used only by the drain to fill generation 0.
  It is the existing `StripedEdgeList.add` logic with the generation as a parameter.
- The pool bucket of a **single-slot** generation is chosen by a hash of the key's edge bucket id, not by the slot
  index. Otherwise every key's first chain lands in pool bucket 0, and appends of different light types contend on
  one file lock.

#### 2.3.4 Promotion, conversion, and edge types added later

All of these are lazy. No step moves existing entries, so every step is one bounded transaction.

| Event | What is written | Legacy part after the event |
|---|---|---|
| A classic vertex crosses the threshold with the setting ON (2.5) | Root, with legacy = the classic chain head. No keys yet. The vertex head is flipped to the root with the same stale-head guard as `tryPromoteToSuperNode`. | The classic chain: bounded by about one threshold (about 4096 entries). |
| An append to a vertex already promoted to type 7, with the setting ON | Root, with legacy = the existing type-7 directory. The vertex head is flipped from the type-7 RID to the root. | The whole type-7 list, until it is drained (2.3.5). |
| First append of an edge bucket that has no key yet (including any edge type created after promotion) | Root rewrite adding the key (anchored fresh copy, poisoned, as `loadDirectoryForWrite` does today), a new sub-directory, and its first chunk. | Unchanged. |
| A key's generation-1 chain crosses the threshold | A **new** sub-directory with generations 0 and 1 copied and generation 2 added, the root's pointer for that key swapped, and the old sub-directory deleted. The old sub-directory is loaded through its anchored page, so a concurrent head flip on it conflicts instead of being lost. | Unchanged. |

A replacement sub-directory rather than growing the old one in place keeps the type-7 property that a directory's
size never changes after creation.

**A reader holding a replaced or superseded RID.**

- *Vertex head flip (conversion).* Nothing is deleted. A reader that resolved the vertex before the flip still holds
  the classic chain or the type-7 directory, which stays valid and reachable as the legacy part. It sees the list as
  it was before the conversion, the same as a reader that resolved the head before any concurrent append.
- *Key promotion.* The old sub-directory **is** deleted, so a reader that took its RID from a root read before the
  swap can get `RecordNotFoundException` when it loads it. The new list class handles this exactly as
  `StripedEdgeList.addChain` handles a stripe head that is not visible: it re-reads the root once and follows the new
  pointer. If that still cannot be resolved, a strict operation (removal, neighbour-keyed dedup) raises a retryable
  `ConcurrentModificationException`, and a read walk skips that key with the existing throttled warning. A write
  never follows a stale sub-directory, because it loads the sub-directory through its anchored page, and the
  promoting transaction's delete fails that version check.

**Racing appenders are safe without moving anything.** A transaction that loaded the old structure before a
conversion committed appends into chunks that stay reachable: the classic chain or the type-7 directory becomes the
legacy part, which the root still references. This is the same argument that makes type-7 generation 0 safe today.
Only writes that rewrite the vertex record or a directory can conflict, and they already raise a retryable
`ConcurrentModificationException`.

**Rejected: eager re-partition at conversion.** Moving all entries into per-type chains at conversion is O(degree) in
one transaction (about 7 MB of chunks for the #8417 vertex), conflicts with every concurrent append for its whole
duration, and deletes chunks that a reader holding an older vertex version may still follow.

#### 2.3.5 Draining the legacy part

After a fresh promotion the legacy part is at most about one threshold, so a filtered hop costs O(type degree + about
4096 skipped entries), and the skip is the cheap #8417 one. After the conversion of an existing type-7 vertex, the
legacy part is the whole old list, and a filtered hop is still O(old degree). The **drain** removes that term. It is
needed to close #8868 for the databases that reported it, so it belongs in #9268.

The drain moves the legacy entries **oldest first** into the archive generation of their key, through the ordinary
append path. `MutableEdgeSegment.add` inserts at the front of a chunk and a full chunk is replaced by a new head whose
`previous` is the old one, so appending entries from oldest to newest leaves every archive chain newest first, with
normally sized chunks, and with no tail pointer to maintain.

One drain **step** is one transaction over one legacy chunk:

1. Load the root through its anchored page. Pick the legacy chain to drain: the legacy generations are drained oldest
   first (type-7 generation 0, the pre-promotion chain, before generation 1), and within a generation the stripes are
   taken in rotation, one chunk each, so the archive receives the stripes' eras interleaved rather than one stripe
   after another.
2. Walk that chain to its **tail** (its oldest chunk) and remember the tail's predecessor. The walk reads one
   chunk per step. The chain is at most the chunks of one stripe (about 55 chunks per stripe for the #8417 vertex with
   16 stripes), so a full drain costs O(chunks^2 / 2) chunk reads per stripe: a maintenance cost, not a hot-path one.
   Load the tail and the predecessor through `loadChunkForWrite`, so that a concurrent in-chunk append to either one
   conflicts instead of being lost.
3. Append the tail's entries, from its last entry (oldest) to its first, to the archive generation of their key,
   creating the key first if needed (2.3.4).
4. Unlink the tail: the predecessor's `previous` becomes null (a rewrite of a segment, so its page is poisoned for the
   edge-append merge), or, when the tail is the only chunk, the type-7 slot or the root's legacy pointer becomes
   empty. Delete the tail.
5. When the legacy part is empty, delete the type-7 directory if there was one and set the root's legacy pointer to
   `(-1, -1)`.

Since the drain takes the oldest entries first, everything in an archive is **older** than everything still in the
legacy part.

**Invariant: every committed state holds each entry exactly once**, either in the legacy part or in an archive, and
never in both. Copying a tail's entries (step 3) and unlinking and deleting that tail (step 4) happen in **one**
transaction, so a commit publishes both and a rollback, a lost conflict or a crash publishes neither. WAL recovery
replays only committed transactions. A drain step that loses a conflict is retried. A crash between steps leaves a
valid, partly drained vertex, and the next drain resumes from the current tail.

**Open decision D1 (surface).** How the drain is started: a Java API plus a SQL command (for example
`REBUILD EDGE LISTS [TYPE <vertexType>]`), and/or `CHECK DATABASE FIX`. This document recommends an explicit command
in #9268, and leaves automatic background draining out. A background task would be a new engine pool, and the
`engine-concurrency` rules would apply to it.

#### 2.3.6 Validation the reader must do from the first release

#9211 showed the cost of a format byte nobody checks. The type-8 reader in #9267 must reject with a
`DatabaseMetadataException`, from day one:

- `formatVersion != 0`, any non-zero `flags` bit, a record shorter than its header or than `19 + 16 x keyCount`;
- keys not strictly ascending;
- a sub-directory RID whose record is not type 7, or a legacy RID whose record is neither type 7 nor type 3.

A future change to the type-8 layout then bumps `formatVersion`, and every reader since 26.12.1 fails closed on it.

### 2.4 Ordering contract

The current contract (`StripedEdgeList` class Javadoc, `GRAPH_SUPERNODE_THRESHOLD` description) is: exact newest
first within a chain and across generations, the newest edge within the first `stripes` entries, and approximately
newest first globally with a rank error bounded by a multiple of the rank. That bound holds because the stripe hash
spreads the entries of **one** list evenly over the stripes. Arrivals of different edge types are not even, so it
cannot carry over to an unfiltered walk across keys.

**Decision.** On a type-8 vertex:

| Walk | Order |
|---|---|
| Filtered on one type with one bucket | The full type-7 contract above, over that type's own edges. This is **stronger** than today, where the rank is measured over the whole list. |
| Filtered on several keys, or unfiltered | Two eras. **Era 1**: the live generations (1 and 2) of every visited key, round-robin across keys (one `InterleavedIterator` per key, rotated by an outer `InterleavedIterator`). **Era 2**: the legacy part (its own existing order, filtered by `EdgeBucketMask` when the walk is filtered), then the archives of the visited keys (round-robin), which hold the oldest entries (2.3.5). Every edge appended after the conversion precedes every edge appended before it. |

What is guaranteed for a multi-key walk: exact newest first within a chain; approximately newest first within each
key (the type-7 contract); the newest edge of every visited key within the first `keys x stripes` entries; era 1
before era 2. **Not** guaranteed: any bound on the rank error between edges of different types. An application that
needs a cross-type order must sort, or read through an index, which the existing contract already says.

**Open decision D2.** Round-robin across keys favors light types: a type with 10 edges is drained as fast as a type
with 10,000 in the same walk. A rotation weighted by each key's approximate size (cumulative chunk sizes, already the
promotion estimator) would restore an approximate cross-type rank. It costs one size estimate per key per walk. This
document recommends plain round-robin, and the weighted rotation only if a user reports the order.

The maintenance walks (count, removal, checker, export) concatenate, as today.

### 2.5 Read-before-write rollout

| Release | Reads type 8 | Writes type 8 | Issue |
|---|---|---|---|
| 26.10.1 | no (fails closed) | no | #9211: validates type-7 `HASH_VERSION` |
| 26.11.1 | no (fails closed) | no | #9264 (this document), #9265 fixtures, #9266 |
| 26.12.1 | **yes** | no (test-only writer) | #9267 |
| 27.1.1 | yes | **opt-in**, default OFF | #9268 |
| later | yes | **default ON**, FORWARD-INCOMPATIBLE release note | #9263 |

**Setting (27.1.1):** a database-scoped `GlobalConfiguration`, for example `arcadedb.graph.supernodePerTypeChains`,
Boolean, default `false`. Its description must say **FORWARD-INCOMPATIBLE ON FIRST USE: once a vertex converts, the
database needs 26.12.1 or later**, following the wording of `GRAPH_SUPERNODE_THRESHOLD`. Turning it OFF stops new
conversions. It does not revert vertices that already converted: they keep being read and written as type 8, because
an append routed to type 7 over a type-8 root would have nowhere to go.

**Downgrade by one release works by construction:** a database written by 27.1.1 with the setting ON opens on
26.12.1, which reads type 8 and never writes it. A converted vertex on 26.12.1 is read-only in the sense that 26.12.1
has no writer: **#9267 must decide** what an append to a type-8 vertex does there. This document recommends that
26.12.1 ships the full append path behind the same code as 27.1.1 (only the conversion is gated by the setting), so
that a downgraded node can keep writing to vertices that already converted. Otherwise a one-release downgrade makes
those vertices read-only. This is **open decision D3**.

### 2.6 HA: the capability that gates writing

**Decision.**

- New token in `PeerCapabilities`: `GRAPH_EDGE_TYPE_DIRECTORY = "graph-edge-type-directory"`, added to `LOCAL` in
  #9267 (26.12.1). Per that class's contract it states what the build can **decode**: "this node's `RecordFactory`
  reads record type 8 and its `formatVersion` 0". It does not depend on the setting.
- Engine hook: `DatabaseInternal.canWriteRecordType(byte recordType)`, default `true`. `RaftReplicatedDatabase`
  overrides it for type 8 with a cached `RaftHAServer.allPeersSupport(GRAPH_EDGE_TYPE_DIRECTORY)`, cached for one
  second like `TxPreparedAtCapabilityCache`, because conversion is decided inside a user transaction. The engine calls
  it through `getWrappedDatabaseInstance()`, since edge-list code holds the embedded `LocalDatabase`.
- The writer converts a vertex only when the setting is ON **and** `canWriteRecordType(8)` is true **and** the
  vertex type's stripe pool is ready (`StripedEdgeList.ensureStripePool`, which on a server creates the pool outside
  the user transaction and skips this attempt, as promotion does today). Otherwise it keeps
  the type-7 or classic path and logs once per minute which peers are missing the token.

**What the gate covers, and what it cannot.** `TX_ENTRY` ships pages, so a follower without the token would apply a
type-8 page without complaint and fail only when it reads the vertex. The gate prevents that during a correctly
ordered rolling upgrade. It cannot help in two cases, which the release note must state:

- An operator who downgrades a node below 26.12.1 **after** type-8 records exist. That node fails closed on those
  vertices, and so would a single-node downgrade.
- A node below 26.12.1 that **joins** later, or installs a snapshot. Once any type-8 record exists, every node needs
  26.12.1 or later.

Neither case causes a **divergence**. The pages are byte-identical on every node, and the older node fails closed
with `Cannot find record type '8'` on the converted vertices only. Refusing such a node at join time would need a
membership gate that does not exist today, and the node usually still serves everything else. So the decision is a
**loud warning** instead of a refusal. In #9268, whenever the setting is ON for a database, the capability monitor
logs at WARNING, at most once per minute per peer, every configured peer that does not advertise
`graph-edge-type-directory`. The message names the peer and says that it cannot read converted vertices. A join
refusal stays possible later, if operators ask for it.

Every unknown peer counts as "no" (`PeerCapabilityRegistry`), so an unreachable node keeps the cluster on type 7.
That is safe, and a conversion is only an optimization.

### 2.7 Downgrade below the reader release

**Decision.** The documented path is **export on the newer release, import on the older**: `EXPORT DATABASE` in JSONL,
then `IMPORT DATABASE` on the older release. The JSONL exporter writes edges as `e` lines, and lightweight edges
separately (`JsonlExporterFormat.exportEdges` / `exportLightweightEdges`). Vertex lines no longer carry edge RIDs
(#7032), and the importer rebuilds every edge list from the edge lines, so the target release builds whatever layout
it knows. A backup restore copies files, so it is **not** a downgrade path.

Requirement for #9268: a test that exports a database containing converted vertices and imports it with the setting
OFF, and checks counts, filtered and unfiltered walks, and `isConnectedTo`. This is the closest an in-tree test can
come to the older importer.

**Not in 27.1.1: a "demote" command** that rebuilds a type-7 directory from a type-8 root. It is O(degree) per
vertex, needs its own conflict handling, and is only useful for going below 26.12.1, which export/import already
covers while the feature is opt-in. Export/import is heavy for a whole database, however, and default-ON makes the
format change reach users who never asked for it. **Open decision D4:** the demote command, or an equivalent
in-place downgrade path, is a **prerequisite of #9263** (default ON), not a someday item.

### 2.8 `GRAPH_EDGE_APPEND_MERGE` and concurrent writers

| Concurrent writes on one hot vertex | Records touched | Outcome |
|---|---|---|
| In-chunk appends, same or different type | The chunk only (root and sub-directory are read through the unanchored fast path, as `StripedEdgeList.add` does today) | Edge-append merge, as today. Different types write different chains, so they usually do not even share a page. |
| Head flips of **different** keys | Different sub-directory records | Different slots. When they share a page, the slot merge rebases them (`LocalBucket.isSlotMergeCandidate` accepts anything that is not an `EdgeSegment`). With `arcadedb.txPageSlotMerge` OFF, a retryable conflict. |
| Head flips of the **same** key and slot | Same sub-directory | A real conflict, retried. The same as type 7 today. |
| First edge of two new types, or a key promotion, at the same time | The root | A real conflict, retried. Once per (vertex, edge bucket), plus once per key promotion. |
| A drain step and an append into the drained chunk | The chunk (anchored by the drain) | A real conflict, retried. |

**Root size and the page merges.** The root grows by 16 bytes per key: 1,000 keys at one vertex is about 16 KB,
inside the default 64 KB page. A rewrite that grows the root is still a single-slot change while the page can host
it, so the slot merge covers it (`TX_PAGE_SLOT_MERGE` handles a record growth the page can host). A root that outgrows
its page becomes a multi-page record. Its continuation chunks are outside the slot merge, so concurrent root rewrites
then become plain retries, which is correct and only slower. Every root rewrite also poisons the root's page for the
edge-append merge **for the rewriting transaction only**. Other vertices' chunks on that page lose the append merge
for that one commit, which is at most one commit per root rewrite.

**Root contention.** A root rewrite happens once per (vertex, edge bucket) when the key is created, once per key
promotion, and once at the end of a drain. After a conversion, a burst of first appends of several new types on the
same hot vertex therefore conflicts on the root. Each loser gets a retryable `ConcurrentModificationException` and the
normal transaction retry handles it. On the retry the key exists, so the append takes the fast path and does not
touch the root again. The number of root conflicts is therefore bounded by the number of distinct edge buckets at the
vertex, not by the append rate. If #9268's concurrency test shows this burst matters, the mitigation is to create, in
the converting transaction itself, the keys of every edge bucket seen in the legacy part's head chunk. That is one
bounded read, and it needs no format change.

Every root and sub-directory rewrite poisons its page for the edge-append merge, as `StripedEdgeList.updateSlot` does,
and a new root, sub-directory or archive chunk is poisoned on creation, as `LocalDatabase.createRecord` does for
segments and directories today. The new record class must be added to the `instanceof` lists at those sites.

Test required in #9268: two or more threads append edges of **different** types to one promoted vertex. After keys
exist, the plain-append path must commit with zero `ConcurrentModificationException`s, and the final counts per type
must be exact.

## 3. Operations on a type-8 vertex

`K` is the set of keys a filter resolves to through `EdgeBucketMask` (all keys when unfiltered), `L` the legacy part.

| Operation | Chains visited |
|---|---|
| `edgeIterator` / `vertexIterator` / `ridIterator` (filtered or not) | The live and archive generations of the keys in `K` (2.4), then `L` with the bucket mask |
| `count(types)` | Same chains as the walk, concatenated. Still O(type degree): no counter is stored. |
| `isConnectedTo`, `containsVertex(rid, filter)`, `getFirstEdgeConnectedToVertex` | For each key in `K`: one stripe per generation (`stripeOf(neighbour)`), plus `L`'s `chainsForNeighbour`. Strict, as today. |
| `containsLightEdge(bucket, rid)` | Key `bucket` only, plus `L` |
| `removeEdge(edge)` | Key of `edge`'s bucket (neighbour-keyed), plus `L` |
| `removeEdgeRID(rid)` | Key of `rid`'s bucket (all its chains), plus `L` |
| `removeVertex(rid)` | Every key (neighbour-keyed), plus `L` |
| `deleteAll`, `anchorForFullRemoval`, `edgeIteratorForRemoval` | Everything: root, every sub-directory, every chain, `L` |
| `AdjacencyProbeCache` (`chainsForNeighbourProbe`) | The chains `containsVertex` reads |

### 3.1 What gets more expensive

Not everything improves. A neighbour-keyed operation that is **not** filtered by type (`removeVertex`, an unfiltered
`isConnectedTo` or `containsVertex`) visits up to three chains per key (archive stripe, generation 1, generation 2
stripe) plus the legacy part's two, where type 7 visits two chains in total. On a hub with 50 edge types that is up
to about 150 chain heads instead of 2. Most of them are the single small chunk of a light type, but each is a page
read. The same holds for an unfiltered walk, which opens one cursor per chain.

This is accepted because the operation the change exists for, a filtered hop, is the common shape in Cypher and SQL,
and because the cost is bounded by the number of edge types at the vertex, not by its degree. #9268's benchmark must
measure it: an unfiltered `isConnectedTo` and `removeVertex` on a hub with many edge types, type 7 against type 8.

## 4. Code that must learn about type 8

From `git grep -l "StripeDirectory\|StripedEdgeList" -- '*/src/main/*'` at `97405375df`, every site that dispatches on
or special-cases the type-7 directory today. #9267 (read) and #9268 (write) each own the rows marked for them.

| Site | Today | Needed | Issue |
|---|---|---|---|
| `RecordFactory` (both `newImmutableRecord`) | case `7` | case `8` | #9267 |
| `BinarySerializer.serialize` | case `7` | case `8` | #9267 |
| `GraphEngine.getOrCreateEdgeList` (write) and `GraphEngine.buildEdgeList` (read) | `instanceof StripeDirectory` | `instanceof EdgeTypeDirectory`, new list class | #9267 read, #9268 write |
| `GraphEngine` vertex-type drop (stripe pool removal) | pool buckets | unchanged: same pool | none |
| `GraphBatch` (several `head instanceof StripeDirectory` sites) | routes promoted vertices through `StripedEdgeList` | also route type 8 through the new list | #9268 |
| `GraphDatabaseChecker.markReachableSegments` | marks the directory and its chains | marks root, sub-directories, archives, legacy | #9267 |
| `GraphDatabaseChecker` `FIX` | rebuild and reclaim | repair or rebuild a type-8 root | #9268 |
| `LocalDatabase` (`createRecord`, record restore) | poisons new segments and directories | also the new record | #9268 |
| `LocalBucket` slot-merge comments | mention the directory as a candidate | the root and sub-directories are candidates too, no code change expected | #9268 (verify) |
| `AdjacencyProbeCache`, `GraphAnalyticalView`, `InterleavedIterator`, `InternalBucketNaming` | consume the list API or naming | no format logic, verify only | #9267 |
| `EdgeLinkedList.tryPromoteToSuperNode` | creates type 7 | creates type 8 when the setting and the capability allow it | #9268 |

## 5. Tests the sub-issues must carry

Section 6 maps these to the sub-issue edits. The concurrency and HA mixed-version tests are specified with their
behavior in 2.8 and 2.6.

- **#9265 (fixtures):** keep green through every later step. Add a **26.10.1** fixture next to 26.8.1 and 26.9.1: it is
  released, and it is the first release that validates `HASH_VERSION`.
- **#9267 (reader):** type-8 records built by a test-only writer: every row of section 3, every rejection of 2.3.6, a
  root with a type-7 legacy part and one with a classic legacy part, a partly drained vertex, the checker walk, the
  `GraphDatabaseChecker` orphan reclaim on a database with type-8 roots (no live root, sub-directory, archive or legacy
  chunk is reclaimed, and a real orphan still is), a reader holding a sub-directory RID that a key promotion deleted
  (2.3.4), and the capability token present in `PeerCapabilities.LOCAL`.
- **#9268 (writer):** fresh promotion and type-7 conversion under the setting; a type added after promotion; key
  promotion; the drain, interrupted and resumed; the concurrency table of 2.8; the HA gate in both directions
  (a peer without the token keeps the leader on type 7, all peers with it allow type 8); export/import with the
  setting OFF (2.7); the benchmark on the #8417 shape (883k IN edges, one `Parent`), before conversion, after
  conversion, and after the drain.
- **Regression guards carried by #9267 and #9268** (structural assertions on the chains visited, never wall-clock
  time):
  - after a full drain, `isConnectedTo(n, X)` on the hot type X visits at most one chain per generation of X's key
    plus none of the legacy part, the same O(degree/stripes) as type 7. This is the regression the adversarial pass
    found in an earlier draft of this document;
  - a filtered hop on a single-type key never opens a chain of another key;
  - a drain killed between steps, and one killed inside a step (rollback), followed by `CHECK DATABASE`: no error,
    and every edge counted exactly once by filtered and unfiltered walks.
- **Compatibility, all releases:** the #9265 fixtures (26.8.1, 26.9.1, 26.10.1) open and pass on every later build.
  The opposite direction, an older release opening type 8, cannot run in-tree against old binaries, so it is
  covered by the next item and by the per-release facts of 2.2, which were checked against the release tags.
- **Old reader fails closed:** a test that writes the type-8 bytes and opens them through a `RecordFactory` that knows
  only types `0..7`, so the claim in 2.2 is executed, not only argued.

## 6. Changes to the sub-issues once this is signed off

| Issue | Change |
|---|---|
| #9265 | Add the 26.10.1 fixture (section 5). |
| #9266 | No change. The set of supported hash versions still matters for type 7, and type 8 needs no new hash version. |
| #9267 | Add: format validation (2.3.6), the token name `graph-edge-type-directory`, `DatabaseInternal.canWriteRecordType`, decision D3 (whether the append path ships in the reader release), and the site table of section 4. |
| #9268 | Add: the drain (2.3.5) and its command (D1), key promotion (2.3.4), the export/import test (2.7), the concurrency tests (2.8), the missing-token WARNING (2.6), the regression guards of section 5, and the three benchmark points. |
| #9263 | Add D4 (demote command) as a prerequisite, and D2 (weighted cross-type order) to decide after the soak. |

## 7. Open decisions for the maintainer

| ID | Question | Recommendation |
|---|---|---|
| D1 | How the drain is started | Explicit SQL command plus Java API in #9268, no background task |
| D2 | Cross-type order of an unfiltered walk | Plain round-robin, weighted rotation only on demand |
| D3 | Does 26.12.1 (reader) also append to an already converted vertex? | Yes. Otherwise a one-release downgrade makes converted vertices read-only |
| D4 | Demote command | Not in 27.1.1, where export/import is the documented downgrade. A **prerequisite of #9263** (default ON) |
| D5 | Is the whole project worth steps 3 to 5 at `severity:minor`? After #8870 the #8417 case is about 76 ms for 50 hops. | Maintainer's call. This document makes the design ready, not mandatory |
