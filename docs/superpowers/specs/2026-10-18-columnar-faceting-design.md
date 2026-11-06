# Columnar faceting - design

Date: 2026-10-19
Audience: ArcadeDB core team
Supersedes: "ArcadeDB - Faceting colonnare: analisi" (Oct 8, 2026). This document reviews it (section 3) and replaces it where they disagree.

## 1. Intent

ArcadeDB has no faceting. Search-heavy workloads migrating from Elasticsearch (thousands of environments over hundreds of clusters) use facets heavily, and their absence blocks such migrations. The goal is unchanged from the original analysis: compute facet counts in the same request as a search (full-text, vector or SQL filter), with performance comparable to Elasticsearch aggregations, at a cost that depends on the number of matches and not on the size of the tenant.

The means changed, for three reasons:

- the time-series engine offers much less reusable machinery than the analysis assumed (3.1);
- the team accepted that the projection may lag committed data by seconds, like Elasticsearch's refresh. That takes the projection out of the record's transaction, so ingest pays nothing for it;
- an adversarial code review and a survey of how other systems facet (section 4) reshaped storage, change tracking and exactness.

The same projection is also a general analytical feature: it accelerates SQL `GROUP BY` over documents and vertices.

## 2. Goals, non-goals, anti-goals

Goals:

- Terms (top-N), multi-valued terms (lists such as `cq:tags`), disjunctive facets, range, date histogram, hierarchical facets over the tag graph, distinct count, min/max/avg/sum.
- One round trip: results and facets in the same response, over HTTP JSON, ndjson streaming, gRPC and `RemoteDatabase`.
- **Exact by default**: facet counts agree with the result set, even though the projection lags (7.3). When exactness cannot be afforded, the response says so.
- The projection catches up within about one second of a commit.
- Ingest cost unchanged: no extra page write, lock or retry on the record's commit path.
- Works with any match source: SQL `WHERE`, full-text, vector.
- Off heap: the projection lives on disk; heap use is proportional to dictionary cardinality, never to record count (5.7).

Non-goals (first release):

- Nested (sub-)facets and pipeline aggregations.
- Facets in Cypher and Gremlin. The engine API must not preclude them.
- Pre-aggregation for filter-only dashboards (star-tree, OpenSearch/Pinot). Revisit after v1.
- Sharded (partitioned) top-N refinement. Every node holds the whole database today, so node-local counts are exact.

Anti-goals, a failure even if it works:

- A restart or crash triggers a full rebuild. (A **new HA replica** or a Raft snapshot install does rebuild in the background; that is accepted, see 6.6.)
- Any cost on the record's commit path beyond setting a bit.
- Facet counts that silently disagree with the results.
- Approximate counts by default (a known source of user complaints at Typesense and Algolia).
- New SQL keywords that break existing schemas with properties named `tags`, `top`, `range`, `histogram`.
- Heap or open file handles growing per tenant × type × bucket × property.

## 3. Review of the original analysis

### 3.1 Time-series reuse

Checked in `engine/src/main/java/com/arcadedb/engine/timeseries/`:

| Original assumption | Code | Consequence |
|---|---|---|
| Sealed blocks can be reused | `.ts.sealed` is written with plain `FileChannel`, outside the WAL (`TimeSeriesSealedStore.java:120-127`); rewrites are whole-file temp plus `ATOMIC_MOVE` (`:2683-2718`) | Rewriting a whole file every few seconds is O(file); wrong model for upserts |
| Writes take part in the transaction | `TimeSeriesShard.appendSamples` runs its own independent transaction (#7410, #7657) | Answers the analysis's question: no. Irrelevant once facets are async |
| Blocks are addressable by position | Lookup by time range only, blocks sorted by `minTimestamp` and possibly overlapping (`:1593`, `:1722`) | Position addressing is new |
| Update and delete | None; retention and truncation only | Supersede and delete are new |
| Random-access codecs | Delta-of-Delta, Gorilla, Simple-8b are sequential; Dictionary is per-block, int16, max 65535 values; no frame-of-reference | Bit-packed codecs are new |
| Decoded column cache | Heap, budget heap/512 with a 4 MB minimum **per store** (`GlobalConfiguration.java:269-280`) | Grows per tenant; needs one server-wide budget |
| SIMD kernels take a match bitmap | `TimeSeriesVectorOps` has `long[]` bitmask variants, but only And/Or are vectorized and the filtered ops have no callers | Interface usable; kernels need work |
| HA | The leader ships whole sealed files; followers never compact | Not applicable: the projection is node-local |

**Decision (team):** do not refactor `TimeSeriesSealedStore` into a shared store. Extract only:

- the codecs, as utilities over primitive arrays;
- the self-describing block header and CRC format;
- a block cache with **one server-wide byte budget**, which the time-series engine may adopt too.

The facet store is its own append-only segment store (5.2). Every extraction step runs the full time-series test suite, and lands before any facet code depends on it.

### 3.2 Prior art inside ArcadeDB the analysis missed

- **Graph Analytical View** (`graph/olap/`): immutable snapshot swapped atomically, `DeltaOverlay` for changes not yet compacted, async update mode. Its **storage is not a model**: every column is an uncompressed heap array sized to the whole graph, and its side file (`GraphAnalyticalViewCSRPersistence`, #6583) is only a cache written on clean close, on the HA leader only, loaded back whole into heap. The projection borrows the update pattern only.
- **LSM vector index**: a node-local derived structure (its graph file is never replicated) with background rebuild. Model for HA and rebuild. Its page hook (`applyReplicatedPageUpdate`, `TransactionManager.java:1287-1303`) covers the index's own pages, not bucket pages, so the bucket hook in 6.2 is new code.

### 3.3 Other problems in the analysis

1. **RID positions are sparse.** `recordPosition()` is `pageId * maxRecordsInPage + slot`, with 2048 slots per page (256 for external-property buckets), but a 64 KB page holds about 280 records of 200 bytes. A block of 4096 positions is about 85% empty.
2. **Slots are reused** after deletes (`LocalBucket.getFreeSpaceInPage`). A create on a reused position is an overwrite.
3. **Records move without changing RID.** A record that outgrows its page leaves a placeholder pointer; later updates write only the content page (`LocalBucket.java:3992`). Chunked records span several pages. Change tracking must follow the content (6.3).
4. **Per-block dictionaries plus global ordinals rebuilt at compaction**, flagged by the analysis as its most delicate part, is avoidable (5.4).
5. **Full-text with LIMIT computes only the top matches** (#9895). Facets need the complete match set.
6. **One bucket per type by default** (`arcadedb.typeDefaultBuckets = 1`): parallelism must come from splitting segments, not from buckets.
7. **Hierarchical facets**: rolling leaf counts up the tree overcounts a document with two tags in one subtree. Dedup per document is required (7.6).
8. **No Roaring bitmaps** in the code base; only `long[]` masks and `BitSet`.
9. **"Same snapshot, no refresh delay"** is replaced by the async projection plus exactness by patching (7.3).
10. **Disjunctive facets** had no syntax at all (8.1).

## 4. What we borrow from other systems

| Technique | From | Where used |
|---|---|---|
| Counting into a dense `int[]` over global ordinals, switching to a primitive hash map when matches are far fewer than cardinality | Lucene `SortedSetDocValuesFacetCounts`, Elasticsearch terms `global_ordinals` vs `map` | 7.4 |
| A single global dictionary, so no ordinal map is rebuilt after each refresh (Elasticsearch's known first-query latency spike) | Lesson from Elasticsearch | 5.4 |
| A sorted view over insert-ordered ordinals | Lucene, Pinot, Tantivy keep sorted dictionaries | 5.4 |
| Disjunctive facets in one pass ("near-miss" counting) | Lucene `DrillSideways`, Solr `excludeTags` | 7.5 |
| Hierarchy through a `parents[]` array on the dictionary side | Lucene taxonomy facets | 7.6 |
| Exactness and freshness flags in the response, exact by default | ParadeDB visibility checks, Algolia `exhaustiveFacetsCount`, Mongo `count.lowerBound`, Typesense's approximate-count complaints | 7.3, 8.3 |
| Codec chosen per segment: bit-packed, linear, blockwise-linear, GCD reduction; sparse presence bitsets | Tantivy fast fields, Lucene NUMERIC doc values, Quickwit | 5.3 |
| Multi-valued values sorted and deduplicated per document | Lucene `SORTED_SET` | 5.3 |
| Append-only segments with live bitmaps and background merge; a delta structure absorbing writes | Lucene segments, pgfaceting delta table | 5.2 |
| Exact distinct count as a popcount over an ordinal bitset; HLL++ above a threshold | Elasticsearch `precision_threshold` | 7.7 |
| Rank by count first; resolve strings and compute stats only for the top candidates | Solr `prelim_sort`, ParadeDB | 7.4 |
| Date histogram with `calendar` vs `fixed` intervals, time zone, offset, minimum count, extended bounds; range facets with explicit boundaries and a default bucket | Elasticsearch `date_histogram`, Mongo Atlas Search | 8.2 |
| Value caps (default 10, configurable), facet-value search, an `AFTER` cursor | Algolia, Meilisearch, Elasticsearch composite | 8.2 |
| One atomic manifest for dictionary plus segments (a taxonomy sidecar out of sync returns wrong labels) | Lesson from Lucene taxonomy | 5.5 |
| Per-query memory budget and bucket caps (fielddata circuit breaker, Mongo 100k bucket cap) | Elasticsearch, Mongo | 5.7 |

Considered and deferred: per-value bitmap indexes for very low cardinality fields (Druid, Pinot, pgfaceting, Meilisearch), opt-in per field after v1; star-tree pre-aggregation.

## 5. Storage

### 5.1 Declaration

```sql
CREATE INDEX ON Asset (brand) COLUMNAR
CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR
CREATE INDEX ON Asset (fileSize) COLUMNAR
CREATE INDEX ON Asset (publishDate) COLUMNAR

CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR
  METADATA { "hierarchy": { "vertexType": "Tag", "key": "path", "parentEdge": "ParentOf" } }
```

- One index per property. A list of properties reads as a composite key everywhere else in ArcadeDB; it may be accepted as shorthand for one index per property.
- Lists use the existing `BY ITEM`.
- `COLUMNAR` is a new `Schema.INDEX_TYPE` with an `IndexFactoryHandler` registered in `LocalSchema`, one instance per bucket under a `TypeIndex`.
- It opts out of `TransactionIndexContext` (not maintained in the record's transaction) and out of planner index selection for `WHERE` and `ORDER BY`. It is read by the facet executor and, in phase 5, by `GROUP BY` acceleration, both through the exactness rules of 7.3.
- A property whose observed cardinality exceeds a configurable limit is refused at creation; if it crosses the limit later, the index keeps working with hash counting and reports a warning metric.

### 5.2 Segment store

Per bucket, **one directory holding all indexed properties** of that bucket (not one file per property), to bound open files across tenants.

- A **segment** is immutable. It holds, for a set of record positions sorted ascending, one column per indexed property, plus a **presence bitmap** over the bucket pages it covers. Position N maps to its row inside the segment by rank (popcount) over the presence bitmap.
- Each segment has a **live bitmap**. When a later segment carries a newer value for a position, or the record is deleted, the position's bit is cleared in the older segment. At most one live value per position exists across all segments. Live bitmaps are small files rewritten on publish, as Lucene's `.liv` files are.
- Counting does not need per-position lookups across segments: for each segment, count over `matches AND presence AND live`.
- **The in-memory overlay** is the newest, not yet flushed segment. It is flushed to a segment by size or age (seconds), on a dedicated pool.
- **Merge** rewrites segments with a high ratio of dead rows, or many small segments, into one. Policy by dead ratio and segment count, Lucene style.
- Segments are memory-mapped or read with `FileChannel` (builder decides) and served from the OS page cache; decoded data goes through the server-wide cache of 3.1.
- Per-segment column stats (min/max) allow skipping a whole segment for range facets and filters.
- Segment files use the self-describing header and CRC extracted from the time-series format.

### 5.3 Codecs

| Field type | Encoding | Point access |
|---|---|---|
| String | Global ordinal, bit-packed at the segment's width | Yes |
| List of strings | Offsets plus ordinals, sorted and deduplicated per document | Yes, via offsets |
| Integer, long, date and datetime (epoch, at the property's precision) | Chosen per segment: bit-packed with frame of reference, linear, or blockwise linear, all with GCD reduction | Yes |
| Float, double | Raw | Yes |
| Missing vs null | Sparse presence and null bitsets | Yes |

Value conversion (schemaless values such as `"12"` in a long property, embedded paths, lists of non-strings, datetime precision) follows **exactly** the rules `GROUP BY` uses today, so the equivalence test of 10 can hold. A value that cannot be converted is counted as missing and reported in a per-index metric.

### 5.4 Global dictionary

- One dictionary per indexed string property **per bucket**, mapping value to a dense `int` ordinal, assigned in insert order and never reused. Old segments stay valid without rewriting. Per bucket (not per type) keeps every publish, compaction included, to one manifest swap (5.5). With the default of one bucket per type the two are the same.
- Types with several buckets (or polymorphic queries over subtypes) merge through a **cross-bucket ordinal map** (bucket ordinal to merged ordinal, Lucene `OrdinalMap` style), cached per type in memory. Dictionaries are append-only, so the map is extended incrementally when a dictionary grows and rebuilt only for a bucket whose dictionary was compacted. Its size is proportional to cardinality times buckets, never to records.
- In memory: chunked append-only arrays with a volatile published size, so readers see a consistent prefix without copy-on-write. Single writer (the apply thread).
- On disk: an append-only file, fsynced **before** any segment referencing its new ordinals is published (5.5).
- A **sort-rank array** (ordinal to rank in value order) is rebuilt in the background when the dictionary grows; until rebuilt, value ordering for new ordinals is computed on the fly. It serves `ORDER BY VALUE`, string ranges and facet-value prefix search.
- Ordinals of values no longer present are reclaimed by a rare background dictionary compaction that remaps and rewrites segments, published with one manifest swap.

### 5.5 Manifest and durability

Each bucket's projection has one manifest naming the live segments, their live-bitmap versions, the dictionary lengths they need, and the recovery marker of 6.5. Publishing a flush, merge or dictionary compaction is: fsync data files, fsync dictionary, write the new manifest, atomic rename. A crash leaves the previous manifest valid. Files no longer named by any manifest and not held by a reader are deleted.

### 5.6 Reuse

- From time series: codecs, block header and CRC, server-wide cache (3.1).
- `TimeSeriesVectorOps` or `GraphOlapVectorOps` for counting kernels; the builder picks one and extends it with long-typed filtered variants.
- Graph Analytical View: snapshot plus overlay pattern.
- Vector index: node-local file handling and background rebuild; a **separate** rebuild semaphore, so facet rebuilds do not queue behind the JVM-wide vector rebuild semaphore.
- Background work on a dedicated pool, never `ForkJoinPool.commonPool()` (see the `engine-concurrency` skill).

### 5.7 Memory

In heap, per indexed property: segment directory, dictionary (proportional to cardinality), overlay (bounded by flush thresholds), dirty page sets. Order of magnitude to validate in the spike: tens of MB for 50M records and four facet fields.

Query time: count arrays and match bitmaps are drawn from a **server-wide byte budget with admission control**. A query that cannot get its budget waits, or switches its facets to hash counting; it never allocates unbounded `int[cardinality]` per parallel task. Example of what this prevents: 1M tag ordinals × 4 bytes × 16 tasks × 50 concurrent queries is about 3 GB.

## 6. Change tracking and recovery

### 6.1 Why bucket pages

Every path that changes a bucket knows page ids, and only some know records:

- a local commit (`TransactionContext`);
- a replicated commit on a follower, and a follower's write forwarded to the leader (#9428): WAL pages through `TransactionManager.applyChanges`, no record-level callback;
- WAL replay at startup.

### 6.2 Dirty page sets

- Each path sets a bit for every bucket page it changed, in a per-bucket set, for buckets of types with a `COLUMNAR` index. That is the only commit-path cost.
- **Ordering protocol**: the commit sets the bit only after the page is visible (after `commit2ndPhase`, `TransactionContext.java:2620`); the apply thread clears the bit before re-reading the page. A commit landing during a re-read sets the bit again and is picked up next pass. A follower read that catches a multi-page transaction half-applied heals the same way.
- Sets are **epoch-based**: the current epoch's set receives new bits; the apply thread drains older epochs. Each epoch records its start time, which gives `lagMs` (8.3) without per-page timestamps.
- WAL replay marks every page id it reads, **including pages it skips** as older versions (`TransactionManager.java:1179`); the mark goes before the skip, not in the component dispatch at `:1276-1304`.

### 6.3 Apply thread

One per database (or per type, builder decides), on a dedicated pool:

- takes dirty pages, re-reads every occupied slot from the current committed state, extracts indexed values, extends dictionaries, writes the overlay; changes to the same page coalesce;
- **follows placeholders**: when a slot holds a placeholder pointer or the first chunk of a chunked record, it reads the content where it lives and records **content page to owner position** in a reverse map kept by the projection. A dirty content page (which holds only `RECORD_PLACEHOLDER_CONTENT` or chunk slots, `LocalBucket.java:99-135`) is resolved to its owners through that map. External-property buckets are handled the same way. No extra write on the commit path;
- **skips no-op changes**: if the extracted values equal the live values, nothing is written. Adding an edge rewrites the vertex record's head pointer (`GraphEngine.java:814-900`) and would otherwise churn vertex projections.

Read amplification (about 280 records per re-read 64 KB page) is measured in the spike.

### 6.4 Schema and maintenance events

These do not reach the projection as dirty pages and are handled explicitly, on the leader and on followers (where they arrive as schema entries):

| Event | Action |
|---|---|
| Drop type, drop property, drop index | Drop the projection |
| Rename type or property | Update the index definition; data unchanged |
| `ALTER PROPERTY` type change | Rebuild that property's columns |
| `ALTER TYPE` adding buckets (`LocalDocumentType.java:1612-1622`) | Create empty projections for the new buckets |
| Removing a bucket, bucket truncate | Drop that bucket's projection |
| `check(fix=true)` repairs (`LocalBucket.java:1492`), backup restore | Rebuild the affected buckets |
| Changes to the hierarchy's vertex or edge type | Rebuild the `parents[]` array (7.6); no document re-apply |

### 6.5 Restart and crash

ArcadeDB has no checkpoint: the WAL housekeeping timer drops a WAL file once all its pages are flushed (`TransactionManager.java:176-200`). So:

- the projection persists **dirty pages ∪ pages applied to the overlay but not yet flushed** (the overlay dies in a crash);
- that set is fsynced **before** a WAL file is dropped, through a hook in housekeeping;
- on restart: load the manifest, load the persisted set, add every page WAL replay marks, re-read them. Cost is proportional to recent writes.
- with `setUseWAL(false)` there is nothing to replay; on an unclean open, buckets written without WAL since the last flush are rebuilt.

Each manifest stores, per covered bucket page, the page version it was built from. It detects corruption, validates recovery, and lets a rebuild skip unchanged pages.

### 6.6 HA and new replicas

The projection is node-local, like the vector graph file. Every node tracks dirty pages from its own apply paths, so no Raft traffic is added. A new replica, or a Raft snapshot install (which replaces whole files), rebuilds the projection of affected buckets in the background. During a rebuild, facets on those buckets use the fallback of 7.8.

## 7. Query execution

### 7.1 Match set

- The `WHERE` produces the complete match set as per-bucket bitmaps of positions; 2048-slot pages map to `long[32]` per page. RoaringBitmap (Apache 2.0, allowed; ATTRIBUTIONS.md and NOTICE updated) only if measurement favours it.
- With `FACETS` present, the planner disables every LIMIT pushdown and early termination for the match source, including the full-text top-K shortcut (#9895) and the HTTP handler's appended ` limit N` (`PostCommandHandler.java:299-304`).
- **Vector search** (my call): facets cover the vector search's returned set (the k neighbors after filters), as Elasticsearch's kNN search does. A vector search used only as a filter contributes its set like any other predicate.

### 7.2 Security

Before reading any bucket's projection, the executor checks READ access on that bucket, as record reads do (`SecurityDatabaseUser.java:68`). Polymorphic queries over subtypes with different permissions count only readable buckets. This applies to filter bitmaps and `INCLUDE` buckets too. `GROUP BY` acceleration declines when any bucket is unreadable, so the user gets the normal plan's error; `facet.searchValues` raises that error too when the user may read no bucket of the type.

Read events: a bucket whose type (or a super type, through its triggers) has a `BEFORE READ` or `AFTER READ` trigger or listener, or a database with a read listener of its own, is never answered from the projection, which would count the rows a trigger hides and the values a listener rewrites. The facets read that bucket's records (the fallback scan of 7.8, which fires the events like any read, so a vetoed record counts nowhere) and the match set of a query without `WHERE` takes it from a scan rather than from the projection; `GROUP BY` acceleration falls back to the normal plan. Decided per bucket at each execution, so a cached plan follows a trigger created or dropped after it was planned. The cost: no projection speedup on a type with read triggers; its facets take the fallback scan, so `arcadedb.columnar.maxFallbackScans` bounds the concurrent FACETS queries on it (beyond it every facet that needs the scan is `unavailable / fallback scans capped`). The events fire for the facet scan as well as for the rows: with a `WHERE`, twice per matched record (reusing the rows' records is a tuning item). The full-text match source (an index adding matches by RID without reading them) is not used when a bucket of the target fires read events, and a database-level read listener fires for every type, so it turns that source off for every FACETS query. The read triggers of a `HIERARCHY`'s vertex type are another matter: they run during the shared hierarchy build, with no user (tuning list).

### 7.3 Exactness by patching

Facets must agree with the results. The projection is current except for pages still dirty, so:

- for matches on dirty pages, the executor reads the records directly, extracts their values with the projection's own conversion, and counts those instead of the projected values;
- the cost is bounded by recent writes, not by the match count;
- a **patch budget** per query (records read) caps it. Beyond the budget, the facet is computed from the projection alone and marked `exact: false`, with `lagMs`. It never escalates into a full scan, so a node under heavy ingest is not pushed further behind.

Phase-5 `GROUP BY` acceleration uses the same rule, and additionally falls back to the normal plan when the caller's own transaction has modified the queried buckets.

### 7.4 Counting

- Per segment, per facet: count over `matches AND presence AND live`, dense scan or point reads by match density.
- Dense `int[]` per ordinal when matches are a large fraction of cardinality, otherwise a primitive open-addressing map; both drawn from the budget of 5.7.
- Merge partial counts across segments, buckets and parallel tasks, rank by count with a deterministic tie-break (count descending, then value ascending), then resolve only the top candidates to strings and compute their stats.
- Multi-valued fields count documents, not values (dedup per document is done at write time, 5.3).
- Counts are exact; the response also reports `otherCount` (the sum of the counts of the values not returned; for a multi-valued property or a HIERARCHY facet it can exceed the number of matches) and `missing`.

### 7.5 Disjunctive facets

Each facet may carry a `FILTER`. Semantics, like Elasticsearch `post_filter` plus per-facet filters and Solr `excludeTags`:

- **result rows** are the `WHERE` matches that pass every `FILTER`;
- **each facet** counts the `WHERE` matches that pass every `FILTER` except its own;
- facets without `FILTER` count the rows that pass all filters.

Execution is one pass, DrillSideways style: for each match, evaluate the F filters into a bitmask; a match passing all counts in every facet, a match failing exactly one counts only in that facet, others count nowhere.

Filter evaluation uses a regular index (LSM) on the property when one exists, otherwise the projection with the patching of 7.3, so filters stay exact.

### 7.6 Hierarchical facets

My call, borrowed from Lucene's taxonomy: documents store only their own tag ordinals. A `parents[]` array (ordinal to parent ordinal) is built from the tag graph declared in the index metadata, in memory, sized by tag cardinality. At query time, each match's tags expand to their ancestors through `parents[]`, deduplicated per document with a small reusable bitset, then counted. A change to the tag tree rebuilds `parents[]` (cheap) and needs no document re-apply. Cost per match is tags × depth, small for typical trees.

### 7.7 Range, histogram, distinct, stats

- Range: boundaries sorted once per query; overlapping ranges via a segment tree.
- Date histogram: boundary arrays computed once per query in the requested time zone, calendar-aware; values are located by binary search. DST transitions inside a month are handled by the boundary array, not by per-value rounding.
- Distinct count: exact popcount over an ordinal bitset for dictionary fields; HLL++ only for raw numeric fields above a precision threshold, flagged `exact: false`.
- Stats: min, max, sum, count, avg over numeric columns, with per-segment min/max used for skipping.

### 7.8 Fallback

If a projection is missing or rebuilding, the facet is computed by reading the matched records (today's `GROUP BY` cost), marked `"source": "scan"`. Concurrent fallback scans are capped per database; above the cap the facet returns `"source": "unavailable"` rather than overloading the node.

## 8. SQL syntax and response

### 8.1 Review of the originally proposed syntax

| Problem in `FACETS brand TOP 10, tags TOP 20, fileSize RANGES [..], publishDate HISTOGRAM 1 MONTH` | Fix |
|---|---|
| No disjunctive filters, the high-priority case | Per-facet `FILTER` (7.5) |
| No alias; the same field cannot be faceted twice | `alias: expression` |
| `RANGES [0, 1048576, 10485760]`: ends and labels unspecified | Half-open intervals, `*` for unbounded, labels, `DEFAULT` bucket |
| `HISTOGRAM 1 MONTH`: no time zone; inconsistent with `ts.timeBucket('1M', ...)`, which refuses DST zones | `CALENDAR`/`FIXED` interval, `TIMEZONE`, `OFFSET`, `MIN_COUNT`, `BOUNDS` |
| No ordering, minimum count, missing bucket, selected values, paging | `ORDER BY COUNT|VALUE`, `MIN_COUNT`, `MISSING`, `INCLUDE`, `AFTER` |
| `TOP`, `FACETS`, `HISTOGRAM`, `RANGES`, `TERMS` are new tokens; `TAGS` already is one | Every new keyword added to the `identifier` rule in `SQLParser.g4` |
| The HTTP handler appends ` limit N` at the end of the text | `FACETS` before `LIMIT`; handler LIMIT detection updated |
| Unstated that facets cover the full match set, not the LIMIT page | Documented |

### 8.2 Proposed syntax

```sql
SELECT @rid, title FROM Asset
WHERE search_fields(['title', 'body'], 'running shoes')
FACETS
  brand TERMS TOP 10 FILTER brand IN ['Nike', 'Adidas'] INCLUDE ['Nike', 'Adidas'],
  tags  TERMS TOP 20 MIN_COUNT 2 ORDER BY COUNT DESC,
  tagTree: tags HIERARCHY TOP 20,
  size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, 10485760) AS 'medium', [10485760, *) AS 'large'),
  pub:  publishDate HISTOGRAM CALENDAR '1M' TIMEZONE 'Europe/Rome' MIN_COUNT 0,
  authors: author DISTINCT,
  sizeStats: fileSize STATS
LIMIT 20
```

- `TOP` defaults to 10, capped by a server setting (default 1000).
- `AFTER <value>` pages through all values of a terms facet, ordered by value.
- Facet-value search (prefix search over a dictionary, for high-cardinality fields in a UI) is a separate function, `facet.searchValues('Asset', 'tags', 'run', 20)`, in phase 5.
- Keyword spelling is open to discussion; the semantics are not.

### 8.3 Response

```json
{
  "result": [ { "@rid": "#12:4051", "title": "..." } ],
  "facets": {
    "meta": { "exact": true, "patched": 12, "lagMs": 340 },
    "brand": { "source": "index", "buckets": [ { "value": "Nike", "count": 1240 } ], "otherCount": 310, "missing": 4 },
    "size":  { "source": "index", "buckets": [ { "key": "small", "from": 0, "to": 1048576, "count": 830 } ] }
  }
}
```

- `exact` is false only when the patch budget was exceeded or an approximate algorithm was requested; `patched` is the number of records read directly; `lagMs` is the age of the oldest undrained epoch on the queried buckets.
- ndjson streaming: the facets ride in the `stats` trailer, as its `facets` member (`{"stats": {"limit", "returned", "truncated", "facets"}}`), so a client that predates them meets no unknown event (plan 4 final review B-M2).
- Every transport encodes a facet value as it encodes a row's value of the same property, and the engine hands the values over in the Java type a row holds (plan 4 final review C-I8).
- gRPC proto, OpenAPI, `RemoteDatabase` and `RemoteGrpcDatabase` are updated together.
- Engine API: a `ResultSet` side channel for facets (`ExplainResultSet` is the precedent).
- `EXPLAIN` and `PROFILE` show facet steps: segments scanned and skipped, patched records, counting mode, budget used.

## 9. Observability

Per database and type: apply lag and throughput, re-read amplification, pages resolved through placeholders, overlay size, flush and merge counts and durations, segment count and dead ratio, dictionary size, cache hit rate, open files, query budget waits, patch-budget overflows, fallback scans, conversion failures. Micrometer, wired as the `engine-concurrency` skill describes for pools.

## 10. Testing

- **Equivalence oracle**: randomized operations (insert, update, delete, slot reuse, records growing into placeholders and chunks, external properties, schema changes, edges on vertices), then facets compared with the equivalent `GROUP BY`, at lag 0 and while lagging (patching must make them equal).
- **Crash**: kill -9 during flush, merge, dictionary append, and between a WAL file drop and the persist of 6.5; recovery must reach the oracle.
- **HA**: follower convergence and snapshot install mid-apply, using the chaos harness.
- **Security**: a user without access to a subtype's bucket gets no counts from it.
- **Memory**: concurrent queries on a high-cardinality field stay within the budget.
- **Calendar**: month histograms across DST changes in several zones.
- **Parser**: existing schemas with properties named after every new keyword still parse.
- Benchmarks tagged `@Tag("benchmark")`, measured with `StallAwareStopwatch`.

Test layers (team requirement), each planned with the phase that introduces the behaviour:

| Layer | Module | What |
|---|---|---|
| Unit | `engine` | Codecs, segments, live bitmaps, dictionary, manifest, dirty page sets, placeholder resolution, counting, patching, parser |
| Integration | `engine`, `server` | Oracle against `GROUP BY`, recovery, schema events, security, HTTP/ndjson/gRPC/remote clients, through the fixture base classes (`BaseGraphServerTest`) |
| HA integration | `ha-raft` (`BaseRaftHATest`) | Follower convergence through Raft apply, leader receiving forwarded writes, snapshot install triggering rebuild, facets queried on followers, failover mid-apply |
| HA resilience | `e2e-ha` (containers, `HaChaosIT`) | Facet invariant added to the chaos harness (facets equal `GROUP BY` on every node once converged) under partition, packet loss, delay, rolling restart, leader failover, split brain |
| Load | `load-tests` | Facet workload modeled on `SingleServerTimeSeriesLoadTestIT` / `ThreeNodesTimeSeriesLoadTestIT`: continuous ingest with updates plus concurrent search-with-facets queries, reporting apply lag, patch counts, query latency, memory budget waits |

## 11. Decisions

| Decision | Made by |
|---|---|
| Async projection outside the record's transaction, catching up within seconds | Team |
| Node-local, never replicated; new replicas rebuild in the background | Team (async); my call (rebuild accepted) |
| Off heap, on disk; heap proportional to cardinality | Team |
| Do not refactor the time-series store; extract codecs, block format and a server-wide cache | Team |
| Own append-only segment store per bucket | Team |
| Scope: all phases planned now, including hierarchical facets | Team |
| Spike go/no-go: faster than `GROUP BY` on the same data | Team |
| Global insert-ordered dictionary with a sorted rank view | My call |
| Change tracking by dirty bucket pages with placeholder reverse map | My call |
| Exact by default through patching, with a budget | My call |
| Disjunctive `FILTER` restricts result rows (post-filter semantics) | My call |
| Hierarchy by `parents[]` at query time | My call |
| Facets over a vector search's returned set | My call |

## 12. Plan

1. **Spike (throwaway):** one string field, one bucket, one segment with presence bitmap and global dictionary, terms top-N from a SQL `WHERE`, against `GROUP BY` on a dataset sized like a large content repository. Go/no-go: faster than `GROUP BY`. Also measures segment size and page re-read cost.
2. **Extraction:** codecs, block header and CRC, server-wide cache out of the time-series engine, guarded by its full test suite.
3. **Core:** `COLUMNAR` index type, dirty page tracking on all three paths, apply thread with placeholder resolution, overlay, segments, live bitmaps, merge, manifest, recovery, schema events, fallback, metrics. Terms, multi-valued, range, histogram.
4. **Query and API:** `FACETS` grammar, patching, disjunctive filters, budget and admission control, planner changes for full-text and vector, security, HTTP, ndjson, gRPC, OpenAPI, remote clients, `EXPLAIN`/`PROFILE`.
5. **Hierarchical facets, distinct count, stats, facet-value search, `GROUP BY` acceleration.**

## 13. Open questions

For the users driving this feature:

- Which facet types: terms, range, date histogram, hierarchical, disjunctive, nested?
- How many facets per request, with which top-N?
- Typical cardinality of facet fields, especially tags?
- Typical and maximum match count facets are computed over?
- Latency target for search plus facets?
- How often does the tag tree change?
- With vector search, should facets cover the returned neighbors or a larger candidate set?

Left to the builder:

- Segment size, flush and merge thresholds, bit-packing widths, patch budget default.
- Apply thread per database or per type.
- `long[]` page masks or RoaringBitmap.
- Memory-mapped or `FileChannel` reads.
- Which SIMD interface to extend.
- Exact `METADATA` key names and keyword spelling.
