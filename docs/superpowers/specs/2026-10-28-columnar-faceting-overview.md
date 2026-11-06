# Columnar faceting - design overview

Date: 2026-10-28
Audience: CTO
Branch: `feat/columnar-faceting` (uncommitted work, staged for review)
Status: plans 1-4 delivered, plan 5 in progress, plan 6, the tuning phase and the pre-merge steps planned

This document summarizes the design documents and the code delivered so far. It is meant to be read on its own; the source documents are listed in section 17 for anyone who wants the detail.

---

## 1. Executive summary

**What it is.** Faceting is the count-per-value panel next to search results ("brand: 1,240 / 830 / ...", "size: small / medium / large", "published per month"). ArcadeDB had none; users emulated it with one `GROUP BY` per facet, which reads every matched record once per facet. The feature adds a `FACETS` clause to SQL `SELECT`, so one request returns the result rows and every facet, computed over the full match set of any match source (SQL `WHERE`, full-text, vector).

**How it works.** A new index type, `COLUMNAR`, keeps a per-bucket, on-disk columnar copy (the *projection*) of the properties to facet on: strings as integer ordinals of a dictionary, numbers and dates bit-packed, lists as sorted ordinal sets. The projection is maintained **asynchronously**, outside the record's transaction: a commit only sets bits in a per-bucket dirty page set; a background pass re-reads those pages and updates the projection, normally within a fraction of a second. Facet counting runs over compact columns instead of records.

**Exact by default.** Because the projection can lag, a query corrects it on the fly: matches on pages not yet applied are read from the records ("patching"), as are the caller's own uncommitted writes. The answer carries `exact`, `patched` and `lagMs`; it is marked inexact only when the patch budget is exceeded, and never silently disagrees with the rows.

**Performance measured so far.**

| Scenario | Facets | Equivalent `GROUP BY` | Speedup |
|---|---|---|---|
| Spike, 1M records, one facet, broad predicate (70% match) | 4.7 ms | 32.2 ms | 6.8x |
| Spike, 1M records, 5-facet page, broad predicate | 24.8 ms | 868 ms | 35x |
| Plan 4, 200k records, 3 facets, broad `WHERE`, caught up | 26 ms | 293 ms | 11x |
| Plan 4, 200k records, 3 facets, broad, 5,000 pending writes, exact | 105 ms | 329 ms | 3.1x |

**Costs.** Inserts into a type with five `COLUMNAR` indexes are 6-10% slower, updates 4.5%. The commit path itself is clean (2-4% with the background apply paused); the remainder is the background apply competing for CPU. The spec goal is "ingest cost unchanged", so a tuning phase (#10662) is scheduled after plans 4-6. Query-time memory is bounded by a JVM-wide budget (256 MB default).

**Status.** Storage, change tracking, recovery, HA integration (plan 3) and the query language, exactness, security, budgets and all transports (plan 4) are delivered and green. Hierarchical facets, distinct count, stats, value search, the `AFTER` cursor and `GROUP BY` acceleration (plan 5) are in progress: only their grammar is in. Chaos and load testing (plan 6), the tuning phase and the Python client (separate repository) remain before the merge to main.

**Main risks.** The ingest overhead; patch amplification under heavy write load (pending changes are tracked per page, so a few thousand scattered writes can force a broad query to read a large share of its matches); the projection is a large new subsystem (about 15,000 lines in `index/columnar`, 8,000 in `query/facet`) whose resilience under faults is proven so far by unit, integration and `ha-raft` tests, not yet by the chaos harness.

---

## 2. Status at a glance

| Plan | Scope | State | Gate |
|---|---|---|---|
| 1 Spike | Throwaway benchmark: one string facet from a hand-built segment vs `GROUP BY` | Delivered | GO (second method, see 8.1) |
| 2 Extraction | Codecs, block header and CRC, server-wide block cache, moved out of the time-series engine | Delivered | Full time-series suite green |
| 3 Core | `COLUMNAR` index, segment store, dictionaries, manifest, dirty page tracking, apply, flush, merge, compaction, recovery, schema events, rebuild, metrics, HA | Delivered | Oracle equivalence at lag 0; `ha-raft` ITs green; final gate 590 columnar tests, 1,059 engine tests, server/HA/integration/metrics green |
| 4 Query and API | `FACETS` grammar, executor, patching, disjunctive `FILTER`, ranges, histograms, memory budget, security, fallback, HTTP, ndjson, gRPC, Java remote clients, OpenAPI, `EXPLAIN`/`PROFILE` | Delivered | 1,007 facet engine tests, 4,246 SQL regression tests, server, gRPC, HA follower ITs green |
| 5 Advanced | `HIERARCHY`, `DISTINCT`, `STATS`, `AFTER` cursor, `facet.searchValues`, `GROUP BY` acceleration | In progress: Task 1 of 15 (grammar and spec records) done | Unit and integration green |
| 6 Resilience and load | Facet invariant in the HA chaos harness, five `e2e-ha` scenarios, two load tests | Planned | No facet violation in chaos and load runs |
| Tuning (#10662) | Ingest overhead, patch granularity, query-side caching and pooling | Planned, after plans 4-6 | Ingest within noise of no-`COLUMNAR`, no latency or lag regression |
| Pre-merge (#10666) | Python bindings in `ArcadeData/arcadedb-python` | Planned | Same API surface as Java over HTTP and gRPC |

---

## 3. What the user sees

### 3.1 Declaring facetable properties

```sql
CREATE INDEX ON Asset (brand) COLUMNAR
CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR
CREATE INDEX ON Asset (fileSize) COLUMNAR
CREATE INDEX ON Asset (publishDate) COLUMNAR
```

- One index per property; lists use the existing `BY ITEM`.
- Supported property types and how they are stored:

| Property type | Column kind | Stored as |
|---|---|---|
| `STRING`, `BOOLEAN`, `LINK` | `ORDINAL` | One dictionary ordinal per record |
| `LIST` or `ARRAY_OF_*` with `BY ITEM` | `ORDINAL_LIST` | Sorted, deduplicated ordinal set per record |
| `BYTE`, `SHORT`, `INTEGER`, `LONG`, `DATE`, `DATETIME` (all precisions) | `LONG` | 64-bit integer (dates as epoch counts at the property's precision) |
| `FLOAT`, `DOUBLE` | `DOUBLE` | Raw double |
| `DECIMAL`, `MAP`, `EMBEDDED`, `BINARY`, `LIST` without `BY ITEM` | refused | - |

- A dictionary property with more distinct values than `arcadedb.columnar.maxCardinality` (1,000,000) is refused at creation (override with `METADATA {"maxCardinality": n}`); if it crosses the limit later it keeps working and raises the gauge `arcadedb.columnar.cardinality.exceeded`.
- At most one `COLUMNAR` index per property per type hierarchy (a supertype and a subtype cannot both index `brand`).
- The index is never used by the planner for `WHERE` or `ORDER BY`, and is not maintained inside the record's transaction.

### 3.2 The `FACETS` clause (delivered)

```sql
SELECT @rid, title FROM Asset
WHERE search_fields(['title', 'body'], 'running shoes')
FACETS
  brand TERMS TOP 10 FILTER brand IN ['brand-a', 'brand-b'] INCLUDE ['brand-a', 'brand-b'],
  tags  TERMS TOP 20 MIN_COUNT 2 ORDER BY COUNT DESC,
  size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, 10485760) AS 'medium', [10485760, *) AS 'large'),
  pub:  publishDate HISTOGRAM CALENDAR '1M' TIMEZONE 'America/New_York' MIN_COUNT 0
LIMIT 20
```

| Facet kind | Options | Notes |
|---|---|---|
| `TERMS` | `TOP n` (default 10, max `arcadedb.columnar.topMax` = 1000), `MIN_COUNT n` (default 1), `ORDER BY COUNT|VALUE [ASC|DESC]`, `INCLUDE [...]`, `MISSING`, `FILTER <condition>` | Default order: count descending, then value ascending (deterministic). `INCLUDE` values are always returned, even at count 0, and are the only ones returned. A multi-valued property counts documents, not values |
| `RANGES` | `([lo, hi) AS 'key', ...)`, `*` for unbounded, `DEFAULT 'label'`, `FILTER` | Half-open intervals; ranges may overlap; boundary values land in the upper range |
| `HISTOGRAM` | `CALENDAR '1m|1h|1d|1w|1M|1q|1y'` or `FIXED 'Ns|m|h|d|w'` (or a number for numeric properties), `TIMEZONE`, `OFFSET`, `MIN_COUNT`, `BOUNDS [lo, hi]`, `FILTER` | Calendar boundaries are computed once per query in the zone, so DST is exact. `FIXED '1M'` is refused (it would mean one minute) with a pointer to `CALENDAR`. More than 100,000 buckets is an error |
| `alias:` prefix | any facet | Lets the same property be faceted twice; `meta` is reserved |

Rules worth knowing:

- Facets always cover the **whole match set**, not the `LIMIT` page. With `FACETS` present the planner turns off every LIMIT pushdown and early termination for the match source, including the full-text top-K shortcut and the HTTP handler's appended ` limit N`.
- `FACETS` goes before `SKIP`/`LIMIT` (the reverse order gets a clear error), only on the outermost `SELECT`, and needs a `FROM`.
- Vector search: facets cover the returned neighbors, written as `SELECT FROM (SELECT expand(vector.neighbors(...))) FACETS ...`.
- Every new keyword (`FACETS`, `TERMS`, `TOP`, `RANGES`, `HISTOGRAM`, ...) is also a valid identifier, so existing schemas with properties named `top` or `range` keep parsing. Two aliases need back-ticks: `unwind` and `timeout`.

### 3.3 Disjunctive facets

A facet with a `FILTER` follows Elasticsearch `post_filter` / Solr `excludeTags` semantics:

- result rows are the `WHERE` matches that pass **every** `FILTER`;
- each facet counts the matches that pass every `FILTER` **except its own** (so selecting "brand-a" in the brand facet still shows the other brands' counts);
- a facet without `FILTER` counts the rows that pass all filters.

It is computed in one pass (Lucene DrillSideways style): each match gets a fail bitmask over the filters; a match failing exactly one filter counts only in that facet. Up to 31 facets may carry a `FILTER`. Each filter is evaluated by the cheapest exact means: an `LSM_TREE` index lookup (`=`/`IN`), the projection's column (comparisons with literals), or the record.

### 3.4 Response

```json
{
  "result": [ { "@rid": "#12:4051", "title": "..." } ],
  "facets": {
    "meta":  { "exact": true, "patched": 12, "lagMs": 340 },
    "brand": { "type": "terms", "source": "index", "buckets": [ { "value": "brand-a", "count": 1240 } ], "otherCount": 310, "missing": 4 },
    "size":  { "type": "range", "source": "index", "buckets": [ { "key": "small", "from": 0, "to": 1048576, "count": 830 } ] }
  }
}
```

- `meta.exact` is false only when the patch budget was exceeded (or, in plan 5, an approximate algorithm was explicitly requested); `patched` counts records read directly; `lagMs` is the age of the oldest pending change on the queried buckets.
- `source` per facet: `index` (projection), `scan` (fallback over the matched records, still exact) or `unavailable` (with a `reason`, no buckets).
- `otherCount` is the sum of the counts of the values not returned (Elasticsearch `sum_other_doc_count`); for a multi-valued property it can exceed the number of matches. `missing` counts matches without a value.
- Facet values come back with the same type and encoding a row value of the same property has (dates as dates, decimals as decimals), on every transport.

### 3.5 Plan 5 additions (in progress)

The grammar for these is in; the executors are not yet.

```sql
SELECT FROM Asset WHERE publishDate >= '2024-01-01'
FACETS
  tagTree:   tags HIERARCHY TOP 20,
  authors:   author DISTINCT,
  sizeStats: fileSize STATS,
  nextPage:  tags TERMS AFTER 'tag-120' TOP 50
```

| Addition | Design |
|---|---|
| `HIERARCHY` | Tag tree declared in the index `METADATA` (`{"hierarchy": {"vertexType": "Tag", "key": "path", "parentEdge": "ParentOf"}}`). Documents keep only their own tags; a CSR `parents` array is built in memory from the graph (DAGs allowed), and each match's tags expand to their ancestors deduplicated per document, so a document with two tags in one subtree counts once. Rebuilt when Tag or edge pages change (watched through the same page marks); never persisted; cycles terminate; orphan tags are their own root |
| `DISTINCT [PRECISION n]` | Exact by default: popcount over a bitset of merged ordinals, or exact hash sets for numerics. HyperLogLog (in-house, p = 14, 16 KB, about 0.8% error) only when the user asks with `PRECISION n` and the set exceeds `n`; then `exact: false` |
| `STATS` | `count`, `missing`, `min`, `max`, `sum`, `avg` on numeric and date properties (dates: no sum or avg); sums widen from `long` to `BigDecimal` exactly as `sum()` does |
| `AFTER <value>` | Composite-style cursor over a terms facet in value order |
| `facet.searchValues('Asset', 'tags', 'run', 20)` | Prefix search over the dictionaries with current counts, for high-cardinality pickers |
| `GROUP BY` acceleration | Eligible `GROUP BY` queries aggregate from the projection with patching; never inexact, falling back to the normal plan when in doubt; kill switch `arcadedb.columnar.groupByAcceleration` (default true, database scope) |

---

## 4. Architecture in one page

```
  commit / Raft apply / WAL replay
          |  (set bits: changed bucket pages)
          v
  DirtyPageTracker (per bucket, epoch based)  <-- the only commit-path cost
          |
          |  ColumnarApplyPool (JVM-wide, 2 threads, <= 1 pass per database, every 100-500 ms)
          v
  ColumnarApplier: re-read dirty pages from committed state, extract values,
                   extend dictionaries, write the in-memory overlay
          |
          |  ColumnarMaintenancePool (JVM-wide, 2 threads)
          v
  ColumnarFlusher: overlay -> immutable segment (every 1 s or 65,536 rows)
  MergePolicy / DictionaryCompactor / ColumnarRebuilder
          |
          v
  <db>/columnar/b<bucketFileId>/ : manifest.json, seg-*.acs, seg-*.dead.N, dict-*.dic, owner maps
          ^
          |  ColumnarSnapshot (ref-counted, consistent per query)
  SELECT ... FACETS: FacetCollectStep -> MatchSet (per-bucket bitmaps) -> FacetExecutor
                     -> collectors per segment + FacetPatcher (pending pages, own writes)
                     -> FacetResults (exact, patched, lagMs) -> HTTP / ndjson / gRPC / remote
```

Key properties:

- **Node-local.** Each node (leader or follower) builds its own projection from its own committed pages. Nothing is added to Raft traffic, snapshots or backups.
- **Per bucket.** One projection directory per bucket, holding all indexed properties of that bucket, to bound open files across tenants.
- **Off heap.** Data lives in files read through `FileChannel` and the OS page cache; decoded blocks go through one server-wide byte-budgeted cache. Heap holds dictionaries (proportional to cardinality), the overlay (bounded by flush thresholds) and small bookkeeping, never anything proportional to record count.

---

## 5. Storage: how facet data is kept

### 5.1 Positions and bitmaps

A record's position in a bucket is `pageId * maxRecordsInPage + slot`, with 2,048 slots per page (256 for external-property buckets). Real pages are sparse: a 64 KB page holds about 275 records of 200 bytes. All bitmaps in the feature are `PositionBitmap`: one stripe of `maxRecordsInPage / 64` longs per touched page (32 longs, 256 bytes, for a standard page), copy-on-write snapshots. RoaringBitmap was considered and not added: no measurement showed a need (the match bitmap is 0.93 MB for 1M records; counting is about 6% of a `GROUP BY`).

### 5.2 Directory layout

```
<databasePath>/columnar/                 outside FileManager, snapshots, backups and checksum scans
  REBUILD                                 rebuild-reason marker (for example snapshot_install)
  recovery.pages                          persisted recovery set (5.8)
  b<bucketFileId>/                        one directory per projected bucket
    manifest.json                         the published state, replaced atomically
    seg-<id>.acs                          immutable segment
    seg-<id>.dead.<version>               dead set of a segment (complement of its live bitmap)
    dict-<indexFileId>.<generation>.dic   one generation of one column's dictionary
    owners-<gen>.own, ext-owners-<gen>.own content-page to owner reverse maps
    NOWAL                                 marker of a no-WAL write not covered by a flush
    CHECK                                 marker of an unfinished unclean-open page check
```

A dropped projection's directory is renamed to a tombstone and deleted after its last reader; tombstones are swept at open.

### 5.3 Segment file format

A segment is immutable and covers a set of positions sorted ascending, with one column per indexed property. Every block is a self-describing `BlockHeader` (magic, version, column offsets and sizes, CRC32), the format extracted from the time-series engine:

| Section | Content |
|---|---|
| File header | segment id, `maxRecordsInPage`, block rows, row count, column definitions (JSON) |
| Row blocks | 16,384 rows each (`arcadedb.columnar.segmentBlockRows`); per column three sub-columns: value presence bits, values, list offsets |
| Presence blocks | up to 1,024 covered pages each: page numbers, per-page row start, slot stripes. Presence is stored once per segment, shared by all columns (the spike showed presence is 48-79% of a narrow column's bytes) |
| Page version blocks | the bucket page version each covered page was built from (recovery validation, rebuild skipping) |
| Footer | directory of every block, per-column min and max (segment skipping for ranges and stats), CRC |

A position maps to its row by rank (popcount) over the presence stripes. Corrupt data surfaces as `BlockCorruptionException` and triggers a `corrupt` rebuild; nothing is ever allocated from unverified sizes.

### 5.4 Codecs

| Field type | Encoding | Point access |
|---|---|---|
| String, boolean, link | Dictionary ordinal, bit-packed at the block's width | Yes |
| List | Offsets plus ordinals, sorted and deduplicated per document | Yes, via offsets |
| Integers, dates | Chosen per block by size: bit-packed with frame of reference, linear, or blockwise linear (512-value sub-blocks), all with GCD reduction | Yes, O(1) |
| Float, double | Raw | Yes |
| Missing vs null | Presence bits | Yes |

Counting picks dense decoding or point reads by match density, so a 1% query does not decode whole blocks; sub-columns 8 bits wide or narrower are never decoded. Value conversion (a `"12"` string in a long property, list items, date precision) follows exactly the rules `GROUP BY` uses, so the equivalence test holds; an unconvertible value counts as missing and increments `conversion.failures`.

### 5.5 Dictionaries, ordinals and the rank view

- **One dictionary per bucket and property**, mapping each distinct value to a dense `int` ordinal in insert order, never reused. Old segments stay valid without rewriting when new values arrive. (The spec originally said per type; it was changed to per bucket so every publish, compaction included, is a single manifest swap. With the default of one bucket per type the two are the same.)
- **In memory:** chunked append-only arrays plus an open-addressing lookup table; a volatile published size gives readers a consistent prefix lock-free; one writer (the apply pass).
- **On disk:** an append-only file `[varint length][type byte][payload]` per ordinal, valid up to the byte length the manifest names (a torn tail is truncated at open), fsynced before any segment referencing new ordinals is published.
- **Rank view:** a sorted view of the ordinals for value ordering (`ORDER BY VALUE`, string ranges, prefix search, `AFTER`). It is rebuilt in the background when the dictionary has grown by more than `max(1024, size/8)` values, at most every 10 s; ordinals newer than the view are ordered on the fly.
- **Cross-bucket ordinal map:** a type with several buckets, or a polymorphic query over subtypes, merges ordinals through a cached map (Lucene `OrdinalMap` style), extended incrementally as dictionaries grow, rebuilt for a bucket after its compaction. Size is proportional to cardinality times buckets, bounded to about twice the live values. A single-bucket type bypasses it. A user who cannot read every bucket gets a private, uncached map with only the readable buckets.
- **Compaction:** when more than `arcadedb.columnar.dictionaryCompactRatio` (0.5) of a generation's ordinals are referenced by no live row, a new generation holding only the referenced values is written, every segment and the live overlay are re-encoded, and everything is published with one manifest swap. Queries holding the old snapshot keep the old generation until they close; a query never waits on a compaction.

### 5.6 Live data: overlay, dead sets, merge

- **Overlay:** the newest, not-yet-flushed segment, in memory, one immutable block per page (copy-on-write publication). At most one live value per position exists across all segments and the overlay.
- **Dead sets:** when a newer value supersedes a row or the record is deleted, the row is marked dead in its segment. Dead sets are small versioned files rewritten on publish (like Lucene `.liv`). Counting runs over `matches AND presence AND live` per segment, with no per-position lookup across segments.
- **Flush:** overlay to segment every `flushInterval` (1 s) or `flushMaxRows` (65,536 rows).
- **Merge:** a segment over `mergeDeadRatio` (0.3) dead rows is rewritten alone; a projection over `mergeMaxSegments` (10) merges its cheapest adjacent run.

### 5.7 Memory and caches

| Item | Where | Bound |
|---|---|---|
| Segment data | Disk, OS page cache, positional `FileChannel` reads | Measured 1.2 to 8.7 bytes per record per column (spike, 1M records) |
| Decoded blocks | `BlockCache`, one per JVM, 16 stripes | `arcadedb.blockCache.maxBytes`, default heap/32, at least 16 MB |
| Dictionaries | Heap | Proportional to cardinality |
| Overlay | Heap | Flush thresholds (1 s or 65,536 rows) |
| Content owner maps | Heap, only for records behind a placeholder, chunk chain or external value | About 62 bytes per owner |
| Query counters and bitmaps | Heap, charged to `FacetMemoryBudget` | `arcadedb.columnar.queryMemoryBudget`, 256 MB per JVM |

The spec's target of "tens of MB of heap for 50M records and four facet fields" has not yet been measured at that scale; plan 6 load tests and the tuning phase will.

### 5.8 Durability and the WAL

The projection does not write to the WAL; it is derived data, durable through its own protocol:

- **Publish order** (every flush, merge, compaction and rebuild): data files (segments, dead sets, owner maps) fsynced, dictionaries fsynced, bucket directory fsynced, then the database WAL forced (so the projection is never more durable than the commits it reflects), then `manifest.json` written to a temp file and renamed atomically. A crash leaves the previous manifest valid. Files no manifest names and no reader holds are deleted afterwards.
- **What is persisted vs rebuilt:** segments, dead sets, dictionaries, owner maps and the manifest are persisted. The overlay is not: it dies in a crash. What covers it is the **recovery set** (`recovery.pages`): every page that is dirty or applied but not yet flushed. ArcadeDB has no checkpoint (a WAL file is dropped once its pages are flushed), so WAL housekeeping persists the recovery set before dropping any WAL file, and a WAL file is not droppable while a commit touching a projected bucket is between its two marks.
- **No-WAL writes** (`setUseWAL(false)`, a bulk-load mode): a `NOWAL` marker is written synchronously at most once per flush epoch per projection; an unclean open with the marker present rebuilds that bucket (`unclean_no_wal`). This is the one deliberate cost on the commit path, limited to no-WAL writers.

---

## 6. When the engine updates the projection

### 6.1 Commit path: what the writer pays

Change tracking is by **bucket pages**, not records, because every path that changes a bucket knows page ids, and only some know records: a local commit, a replicated commit applied on a follower (WAL pages, no record callback), and WAL replay at startup.

On commit, for a database with at least one `COLUMNAR` index:

1. The commit's page ids are packed into reused `long[]` keys and sorted (no allocation per commit once warmed).
2. **First mark**, before the pages are published: covers the window in which the flush thread could write a page and its WAL file become droppable.
3. Pages are published and page counts raised.
4. **Second mark**, once the pages are visible: the ordering-protocol mark; an apply pass that drains it reads the committed images.

A mark is lock-free: it registers as a writer of the current epoch, sets bits with atomic ORs and leaves. A database without `COLUMNAR` indexes pays one volatile read. Measured: bytes allocated by the committing thread per commit are unchanged (about 145 KB per insert transaction, 11.2 MB per update transaction of 500 records, with or without five `COLUMNAR` indexes). The spec described one mark after visibility; the delivered protocol uses two, closing a race found in the plan 3 final review.

### 6.2 The background apply pass

- **Pool:** `ColumnarApplyPool`, JVM-wide, `arcadedb.columnar.applyThreads` = 2 daemon threads; at most one pass per database at a time, so the pool is sized by threads, not tenants. A JVM-wide ticker wakes every 10 ms and honors each database's `applyInterval`.
- **Pacing:** base interval 100 ms. Under sustained ingest, while every pass leaves marks behind, the interval doubles up to 5x, never past `max(applyInterval, 500 ms)`; it returns to the base as soon as a pass drains everything or the oldest mark is older than 500 ms. At the defaults the lag bound is about 500 ms plus one pass.
- **Epochs:** a pass swaps in a fresh epoch, waits for writers of the sealed one, then reads its bits. Each epoch records its start time, which gives `lagMs` without per-page timestamps.
- **Work:** each dirty page is re-read at its newest committed version; every record's projected values are extracted in one header walk and compared with the live value. Equal is a no-op (adding an edge rewrites the vertex record but changes no projected value). Different values are staged in a new overlay block; the old row in a segment is marked dead.
- **Records that move:** a record that outgrows its page leaves a placeholder pointer; later updates write only the content page. The pass follows placeholders, chunk chains and external-property values, and keeps a **content owner map** (content page to owner positions), so a dirty content page re-reads its owners. No extra write on the commit path.
- **Unreadable records** keep their previous value, are counted (`apply.unreadable`), and their page is retried with exponential backoff; after repeated failures a `corrupt` rebuild is requested while the previous state keeps serving.
- **Cost:** measured 35-37 us per re-read 64 KB page, 0.13 us per record (warm). A full re-read of a 1M-record bucket (3,631 pages) is 127-134 ms single-threaded.

### 6.3 Schema and maintenance events

Schema changes do not reach the projection as dirty pages. A reconciler diffs the `COLUMNAR` columns of each projected bucket between schema states, on the leader and on followers alike (where they arrive as schema entries):

| Event | Delivered behavior |
|---|---|
| `CREATE INDEX ... COLUMNAR` | Background `create` rebuild; concurrent writers converge (tested) |
| Drop type, property or index | Projection dropped (directory tombstoned, deleted after the last reader) |
| Rename type or bucket | Definition update only; data unchanged (columns are keyed by index file id, not by name) |
| Rename an indexed property | Refused by DDL today, as for every index family; the reconciler already handles it (unit-tested), so lifting the refusal needs no projection change |
| Property type change | No `ALTER PROPERTY ... TYPE` DDL exists; the reconciler's rule (rebuild that column, `schema` reason) is unit-tested with a synthetic change. A retyped column is unusable until its rebuild publishes, and queries fall back meanwhile |
| `ALTER TYPE` adding buckets | Empty projections for the new buckets |
| Bucket truncate | Deliberate deviation from the spec: the projection empties itself through the ordinary deletes (exact, one apply pass over the bucket's pages) instead of a drop |
| `CHECK DATABASE FIX` | `check_fix` rebuild, only for buckets the fix actually repaired |
| Backup restore | Projections are not in backups; a restored bucket with records and no manifest rebuilds (`missing`) |
| Hierarchy source changes (plan 5) | `parents[]` rebuilt from the graph; no document re-apply |

### 6.4 Background rebuild

- Per bucket, covering all its columns, on `ColumnarMaintenancePool`, with its **own semaphore** (`arcadedb.columnar.rebuildConcurrency` = 1 per JVM), never queued behind the JVM-wide vector-index rebuild semaphore.
- While it runs, the previous valid state (if any) keeps serving and apply passes keep updating it; pages applied during the scan are re-marked after the publish. A projection is unusable only when no valid state exists, and then queries use the fallback scan.
- Requests coalesce per projection. A failed rebuild retries with backoff from 1 s doubling to 5 min.
- For `check_fix` and `schema` rebuilds, pages whose version is unchanged copy their rows from the old segments instead of being re-extracted (`rebuild.pages.reused`).
- Every rebuild increments `arcadedb.columnar.rebuilds{reason}` with one of `create`, `snapshot_install`, `missing`, `corrupt`, `unclean_no_wal`, `unclean_pages`, `schema`, `check_fix`. **A normal open and crash recovery never increment it**: plan 6 asserts this.

### 6.5 Restart and crash recovery

Anti-goal of the spec: a restart or crash must never trigger a full rebuild. Delivered:

1. Load the manifest of each bucket (O(1) per projection).
2. Load the persisted recovery set and add every page WAL replay marks, **including pages replay skips** as older versions.
3. Re-apply those pages. Cost is proportional to recent writes.
4. On an **unclean** open only, a background job checks each projection's page versions against the bucket (reading 4-byte page headers, no records); the projection is unusable until it finishes, so queries fall back to the exact scan meanwhile. Changed pages are re-applied; if more than 1,024 pages and more than a quarter of the bucket changed, the bucket rebuilds (`unclean_pages`). A `CHECK` marker makes an interrupted check resume at the next open.
5. A `READ_ONLY` open never writes or deletes anything under `columnar/` and never schedules work.

Crash testing uses named kill points (segment synced, dictionary synced, dictionary append half-written, between a WAL drop and the recovery-set persist, between page publication and the second mark) with simulated crashes; recovery must reach the oracle.

### 6.6 High availability

- The projection is **node-local and never replicated**. Followers mark pages from their own Raft apply path (`TransactionManager.applyChanges`); a follower's write forwarded to the leader is marked where it commits.
- A **Raft snapshot install** replaces whole files, so it writes a `snapshot_install` marker and the affected projections rebuild in the background; during the rebuild those buckets use the fallback scan. A **new replica** rebuilds the same way. This is the accepted exception to "no full rebuild".
- **Follower checkpoint guard:** follower changes bypass the WAL housekeeping hook, so the Raft state machine persists every database's recovery set before registering a snapshot and aborts the checkpoint if that fails; otherwise Ratis could purge log entries whose pages the projection had not yet covered.
- Facets are answered locally on followers (tested in `FacetsFollowerQueryIT`); a node-local count is exact because every node holds the whole database.

### 6.7 Staying exact while the projection lags

A query sees the projection as of its snapshot plus a set of **pending positions**: those on pages marked but not yet applied. For each bucket:

```
patch set = (matches AND pending)
          OR (matches AND NOT (pending OR live in any segment))
          OR (records the caller's open transaction wrote)
```

- Patched records are read once and their values extracted with the projection's own conversion, then handed to every facet that counts them; their projected values are excluded. A position whose record is gone counts nowhere.
- The second term means a match no segment explains is counted from the record: a projection defect degrades to a slower query, never a wrong count.
- **Own uncommitted writes are never "lag":** they are always read from the records. If they cannot be patched within the budget, those buckets go to the fallback scan, still exact.
- **Patch budget:** the answer stays exact while the patch set is at most `max(patchBudget, patchBudgetRatio x matches)`, the matches being those of the patched buckets. Defaults: `patchBudget` 10,000 and `patchBudgetRatio` 1.0, so by default a query patches whatever lags, bounded by the memory budget. Over budget, nothing is patched, counts come from the projection alone, and the response says `exact: false` with `lagMs`; it never escalates into a full scan that would push a busy node further behind. (The spec and plan 4 started with an absolute, all-or-nothing budget; the Task 19 measurement in 8.3 showed it answered inexact under moderate write load, and the final review made it relative with ratio 1.0.)
- **Why patching can be large:** pending marks are page-granular, so every matched record on a dirty page is patched. 5,000 scattered updates on 200,000 records made the patch set of a broad query about 120,000 positions (60% of its matches). Record-granular marks are a tuning item.

---

## 7. Query execution

1. **Match set.** `FacetCollectStep` sits after the match-source plan. It passes rows through (post-filtered by the facet `FILTER`s, so downstream `ORDER BY` and `LIMIT` keep their streaming behavior), and records every match's RID into a `MatchSet` of per-bucket bitmaps plus fail masks. When the upstream is exhausted, or `getFacets()` is asked, it drains the rest without forwarding and calls the executor. A `FACETS` query with no `WHERE` builds its match set from the live bitmaps plus pending positions, never from a record scan.
2. **Security.** Before reading a bucket's projection the executor checks READ access on the bucket, as record reads do. Buckets the user cannot read contribute nothing: no counts, no dictionary values in the merged map, no RIDs in index filters.
3. **Snapshots.** One ref-counted snapshot per readable bucket, acquired once, so all facets of the query read the same state. A commit racing the query may be counted on either side, as any read-committed scan.
4. **Counting.** Per segment, per facet, over `matches AND live`. Dense `int[]` per ordinal when cardinality is at most 100,000 and `matches x 8 >= cardinality`, otherwise an open-addressing primitive hash map; numeric terms in a primitive long-keyed map. Partial counts merge across segments, buckets and subtypes; ranking by count with a deterministic tie-break; only the top candidates are resolved to values.
5. **Ranges** use elementary intervals over the sorted bounds (O(log b) per value); a segment whose `[min, max]` falls in one interval is counted by cardinality without decoding. **Histograms** locate values by binary search in a boundary array built once per query.
6. **Fallback scan.** A bucket the projection cannot answer (missing or rebuilding projection, unpublished or retyped column, property without a `COLUMNAR` index, an expression facet) is counted from its matched records, with the values `GROUP BY` would key on, and marked `source: "scan"`. At most `maxFallbackScans` (2) concurrent scans per database; above that the facet is `unavailable` with reason `fallback scans capped`.
7. **Deadlines.** The patch loop and the fallback scan check the query's timeout and termination every few records.

---

## 8. Performance

All timings are stall-discounted (`StallAwareStopwatch`) medians on an Apple M3 Pro, 12 cores, OpenJDK 25, warm engine cache unless stated. Benchmarks are tagged `@Tag("benchmark")` and report only; none asserts a time.

### 8.1 Spike (plan 1): 1,000,000 records, one bucket

The first spike run was **NO-GO**: it built the match set with `SELECT @rid`, which materialized one result row per match (125 of 128 ms on the broad predicate) while counting itself cost 5-9% of a `GROUP BY`. The design was corrected to build the match set natively (index range or column scan into a bitmap, no result rows), and the second run was **GO**:

| Measurement | Selective (`fileSize < 10000`, 9,868 matches, indexed) | Broad (`status = 'published'`, 700,191 matches) |
|---|---|---|
| One facet: match + count | 2.67-3.04 ms | 4.72-4.85 ms |
| One `GROUP BY` | 4.44-4.78 ms | 32.2-34.9 ms |
| Speedup, one facet | 1.6-1.7x (1.3-1.4x with the 0.5 ms SQL front end) | 6.8-7.2x |
| 5-facet page (brand, status, mimeType, author ~96.5k values, tags multi-valued) | 4.3-4.7 ms vs 42.5-45.2 ms | 24.8-25.1 ms vs 868-914 ms |
| Speedup, 5-facet page | 9.6-9.9x | 35-36x |
| 8 concurrent clients, 5-facet page | 1,394-1,430 q/s vs 133-146 q/s | 263-267 q/s vs 3.0-3.4 q/s |
| Cold engine cache (DB reopened), one facet | 4.2-4.3 ms vs 99-109 ms | 4.9-5.2 ms vs 55-66 ms |

The multi-valued `tags` facet gains most: 12.5 ms against 607 ms for `GROUP BY ... UNWIND` (49x). Segment sizes were 1.2 (status, 3 values) to 8.7 (tags) bytes per record, 0.5% to 3.7% of the bucket's size; segment build ran at 4.6-8 M records/s.

Lesson carried into the design: **the match source decides the feature**. The projection must never route its match set through SQL result rows.

### 8.2 Plan 3: counting over real segments

`ColumnarCountBenchmark`, 1,048,576 records, counting only (no SQL), 10% dead rows:

| Facet, selectivity | One merged segment | 16 segments + overlay | Cold block cache | Cold cache + fresh segment open |
|---|---|---|---|---|
| brand, 1% | 0.37 ms | 0.54 ms | 3.64 ms | 3.89 ms |
| brand, 70% | 3.24 ms | 3.69 ms | 6.32 ms | 6.74 ms |
| tags, 1% | 0.38 ms | 1.18 ms | 3.37 ms | 3.55 ms |
| tags, 70% | 6.58 ms | 7.44 ms | 17.1 ms | 17.3 ms |

Many segments cost +13% to +47% on brand and up to +208% on a 1% tags query (fixed per-segment costs dominate); merge keeps the count bounded. A cold block cache adds 2-10 ms.

### 8.3 Plan 4: the shipped `FACETS` path vs `GROUP BY`

`FacetsVsGroupByBenchmark`: 200,000 records; query `FACETS brand TERMS TOP 1000, tags TERMS TOP 1000, size: fileSize RANGES (4 ranges) LIMIT 20` against the three equivalent `GROUP BY` queries (brand; tags through `UNWIND`; fileSize / 1 MiB), timed as the sum. Lag produced by suspending the apply pass and running 5,000 random mutations.

| Case (1 bucket unless noted) | Facets | 3 x `GROUP BY` | Ratio | Patched | Exact |
|---|---|---|---|---|---|
| Caught up, broad (199,028 matches) | 26 ms | 293 ms | 0.09 | 0 | yes |
| Caught up, no `WHERE` | 17 ms | 286 ms | 0.06 | 0 | yes |
| Caught up, selective (483 matches) | 1 ms | 2 ms | noise | 0 | yes |
| Caught up, broad, 3 buckets | 30 ms | 299 ms | 0.10 | 0 | yes |
| Lagging, broad, delivered defaults | 105 ms | 329 ms | 0.32 | 120,152 | yes |
| Lagging, no `WHERE`, delivered defaults | 97 ms | 320 ms | 0.30 | 120,723 | yes |
| Lagging, selective | 1 ms | 1 ms | noise | 296 | yes |
| Lagging, broad, `patchBudgetRatio = 0` (absolute budget only) | 27-30 ms | about 290 ms | about 0.1 | 0 | no |

Reading: caught up, facets are 10-17x faster than the `GROUP BY`s. While lagging, the default (exact) costs about a third of a `GROUP BY`; an operator who prefers latency to exactness under heavy ingest sets the ratio to 0 and gets inexact answers flagged as such. Bucket count (1 vs 3) changes nothing material.

### 8.4 Ingest overhead

`ColumnarIngestOverheadBenchmark`: 200,000 inserts then 100,000 updates in transactions of 500, without (A) and with (B) five `COLUMNAR` indexes, and B' = B with the apply pass held (commit path only):

| Phase | B vs A | B' vs A (commit path only) |
|---|---|---|
| Inserts, before tuning in plan 3 | +13% | +4.5% |
| Inserts, after adaptive pacing and single header walk | +6.0% / +10.1% (two runs) | +3.9% / +2.4% |
| Updates, after | +4.5% / +4.6% | -1.0% / +0.1% |

Bytes allocated by the committer and GC counts are unchanged. Catch-up after 100,000 updates: about 440 ms on the apply pool. Conclusion: the commit path meets the spec ("no extra page write, lock or retry"); the residual is background CPU. Accepted for now by user decision (2026-10-23), with a tuning phase planned.

### 8.5 What scales with what

| Cost | Scales with | Notes |
|---|---|---|
| Building the match set | Matches (native index range or column scan) | Plus the match source's own cost (full-text, vector) |
| Counting | Pages touched by matches, per segment; dense vs point reads chosen by density | Not with tenant size; fixed per-segment costs matter for very selective queries |
| Count memory | Cardinality (dense) or `min(matches, cardinality)` (hash) | Charged to the JVM-wide budget |
| Multi-bucket merge | Cardinality x buckets | Cached per type; private for restricted users |
| Patching | Matched records on dirty pages (page granular) | Bounded by the patch budget and the memory budget |
| Apply | Changed pages x records per page (about 275) | Off the commit path |
| Recovery at open | Recent writes | Never record count, except a rebuild |
| Heap at rest | Dictionary cardinality, overlay, owner map of moved records | Never record count |

### 8.6 Tuning phase levers (#10662, planned)

Baseline: the three benchmarks above plus the plan 6 load tests. Candidate levers, to be measured rather than assumed:

- **Apply:** fewer re-reads under sustained ingest (longer pacing with a configurable lag bound; skip pages whose change is outside projected properties); batch dictionary lookups; less allocation per re-read record.
- **Apply pool:** thread count and priority relative to committers; per-database fairness.
- **Commit path:** skip the second mark when no drain happened between the two; skip the sort when keys are already ordered (worth under 0.5 ms per 400 commits).
- **Flush and merge:** segment size, merge policy, fsync batching.
- **Patching:** record-granular pending marks, patching per page read, a budget expressed in pages read.
- **Query side:** counter pooling inside budget reservations, parallel counting per segment, ordinal-map caching for restricted users, rank-view refresh cadence.

Exit criterion: inserts and updates within noise of no-`COLUMNAR` at default settings, with facet latency and apply lag no worse than the plan 4 and plan 6 numbers; any remaining cost documented with its knob.

---

## 9. APIs and transports

| Transport | How facets travel | State |
|---|---|---|
| Embedded Java | `ResultSet.getFacets()` returns `FacetResults` (default `null` for every other result set) | Delivered |
| HTTP JSON | `facets` object next to `result`, exactly `FacetResults.toJSON()`; values encoded as row values; with `typeHints`, a `@props` `value:<type id>` hint | Delivered |
| HTTP ndjson streaming | In the `stats` trailer as `stats.facets` (`{"stats": {"limit", "returned", "truncated", "facets"}}`), so a client that predates facets meets no unknown event. Plan 4 first designed a separate line; it was moved into the trailer in the final review | Delivered |
| HTTP auto-limit | The handler's appended ` limit N` limits rows only; `returned`/`truncated` describe rows; facets count every match | Delivered |
| gRPC | `GrpcFacets` on `ExecuteQueryResponse`, `ExecuteCommandResponse` and the terminal streaming batch; values as `GrpcValue` typed like row values. `PAGED` retrieval with `FACETS` is refused (`INVALID_ARGUMENT`, would recompute facets per page): use `CURSOR` or `MATERIALIZE_ALL` | Delivered |
| `RemoteDatabase`, `RemoteGrpcDatabase` | Parse facets back into `FacetResults`, buffered and streamed, values decoded as rows | Delivered |
| Raft forwarded queries | The leader-answer parser keeps facets | Delivered |
| OpenAPI | Schemas for `FacetResults`, `FacetResult`, `FacetBucket`, ndjson event | Delivered; plan 5 adds attribute schemas |
| Python | `ArcadeData/arcadedb-python`, sub-issue #10666: `FACETS` usable, `facets` parsed from HTTP JSON and `stats.facets`, same shape, tested against a server built from this branch | Pre-merge step |
| Cypher, Gremlin | Not in scope for v1; the engine API does not preclude them | Non-goal |

---

## 10. Security

- READ permission is checked per bucket before any projection read, exactly as record reads check it. Polymorphic queries over subtypes with different permissions count only readable buckets.
- **No hidden counts:** an unreadable bucket contributes no count, no `missing`, no `otherCount`, no dictionary value (restricted users get a private ordinal map built only from readable buckets), and no RIDs in index-backed `FILTER`s. A query the plain `SELECT` would refuse is refused the same way, with one documented exception: with no `WHERE` and an index- or projection-backed `FILTER`, the facets are answered from the projection with the hidden buckets left out. Tested in `FacetsSubtypeAclIT`.
- `INCLUDE` accepts literals, parameters and side-effect-free expressions only; subqueries are refused at parse time (closes a write-through-query hole).
- The profiler output (`PROFILE`, stored query plans) keeps facet counts but drops every value, bound, key, cursor and stat value, so plans do not leak data.
- During the work a security issue was found in an unrelated area (full-text scoring); it is being handled privately and is not described here.

---

## 11. Resource governance

| Guard | Mechanism | Default |
|---|---|---|
| Query counting memory | `FacetMemoryBudget`: fair semaphore of KiB permits shared by all `FACETS` queries of the JVM. Dense counters try without waiting, then hash counters wait up to `queryBudgetWait`; refused, that facet is `unavailable` (`memory budget`). Match bitmaps reserve in 1 MiB steps; refused, every facet of the query is unavailable and the rows still flow. Patch sets and derived bitmaps are charged before allocation | 256 MB, 5 s wait per collector |
| Why a budget | The spec's example: 1M tag ordinals x 4 bytes x 16 tasks x 50 concurrent queries is about 3 GB of unbounded `int[]` | - |
| Patch work | Patch budget (6.7), never escalates to a full scan | 10,000 records or 100% of matches |
| Fallback scans | Per-database semaphore, `tryAcquire` without waiting | 2 concurrent |
| `TOP` | Parse error above the cap | 1,000 |
| Histogram buckets | Error above the cap | 100,000 |
| Background work | Two dedicated JVM-wide pools (apply, maintenance), never `ForkJoinPool.commonPool()`; structurally bounded queues (at most one pending pass per database, one maintenance job per projection); separate rebuild semaphore | 2 + 2 threads, 1 rebuild |
| Timeouts | Patch loop and fallback scan check the query deadline every few records | Query `TIMEOUT` |
| Kill switch | Drop the `COLUMNAR` index (queries then use the fallback scan). Plan 5 adds `arcadedb.columnar.groupByAcceleration` for `GROUP BY` | - |

---

## 12. Operations

### 12.1 Settings

| Key (`arcadedb.columnar.*` unless noted) | Scope | Default | Purpose |
|---|---|---|---|
| `flushInterval` | database | 1000 ms | Overlay flush by age |
| `flushMaxRows` | database | 65,536 | Overlay flush by size |
| `mergeDeadRatio` | database | 0.3 | Rewrite a segment above this dead ratio |
| `mergeMaxSegments` | database | 10 | Merge above this segment count |
| `maxCardinality` | database | 1,000,000 | Creation refusal and warning gauge |
| `segmentBlockRows` | database | 16,384 | Rows per column block |
| `applyInterval` | database | 100 ms | Apply check period (stretched up to 5x, max 500 ms at default, under ingest) |
| `applyThreads` | JVM | 2 | Apply pool size |
| `maintenanceThreads` | JVM | 2 | Flush, merge, compaction, rebuild pool size |
| `rebuildConcurrency` | JVM | 1 | Concurrent rebuilds |
| `dictionaryCompactRatio` | database | 0.5 | Unreferenced ordinal share that triggers compaction |
| `patchBudget` | server | 10,000 | Absolute patch budget (records) |
| `patchBudgetRatio` | server | 1.0 (0 to 1) | Relative patch budget; 0 = absolute only |
| `queryMemoryBudget` | JVM | 268,435,456 | Counting memory for all queries |
| `queryBudgetWait` | server | 5000 ms | Wait for counting memory, per collector |
| `topMax` | server | 1000 | `TOP` cap |
| `maxFallbackScans` | database | 2 | Concurrent fallback scans |
| `arcadedb.blockCache.maxBytes` | JVM | heap/32, at least 16 MB | Decoded block cache |
| `groupByAcceleration` (plan 5, not yet in code) | database | true | `GROUP BY` acceleration kill switch |

### 12.2 Metrics

Micrometer meters under `arcadedb.columnar.`, tagged `database` (and `type`, `property` where relevant), exposed by `GET /prometheus` (for example `arcadedb_columnar_rebuilds_total`). The block cache meters are JVM-wide and tagged `database="*"`.

| Area | Meters |
|---|---|
| Apply | `apply.lag.ms`, `apply.pages`, `apply.records.reread`, `apply.placeholders.resolved`, `apply.unreadable`, `overlay.rows` |
| Storage | `segments`, `segments.dead.ratio`, `flush.count`, `flush.duration`, `merge.count`, `merge.duration`, `files.open`, `dictionary.size`, `dictionary.compactions`, `dictionary.ordinals.reclaimed`, `cardinality.exceeded` |
| Recovery | `rebuilds{reason}`, `rebuild.pages.reused`, `recovery.pages`, `conversion.failures` |
| Cache | `cache.hits`, `cache.misses`, `cache.used.bytes` |
| Query | `query.patched`, `query.patch.overflows`, `query.budget.waits`, `query.fallback.scans`, `query.filter.fallbacks` |
| Pools | `pool=columnar_apply`, `pool=columnar_maintenance` through the standard pool metrics (`queueDepth` is the saturation signal) |

### 12.3 EXPLAIN and PROFILE

`EXPLAIN` shows a `COLLECT FACET MATCHES` step listing each facet and its `FILTER` evaluator kind (`INDEX`, `PROJECTION`, `RECORD`). `PROFILE` adds per facet `source`, segments scanned and skipped, counting mode (dense or hash), budget bytes and cardinality; per filter the column vs record answers or the index RID count; and for the query `matchSource`, matches, `exact`, `patched` and `lagMs`. `SHOW INDEXES` reports whether a `COLUMNAR` index is online.

---

## 13. Compatibility

- **Queries without `FACETS` are unchanged**: the clause is optional and none of the planner changes (pushdown suppression, collect step) apply without it. A database without `COLUMNAR` indexes pays one volatile read per commit.
- **Existing schemas keep parsing**: every new keyword is also an identifier, tested with properties named after each keyword (219 parameterized cases).
- **Old clients**: the HTTP `facets` member and the ndjson `stats.facets` member are additive; gRPC fields are new optional fields; old clients ignore them.
- **Data format**: the projection is new files only, under `<db>/columnar/`, outside the files the engine, snapshots and backups manage; bucket and WAL formats are unchanged. The `COLUMNAR` index itself is an ordinary schema entry backed by an empty LSM shell, so schema handling, file ids and HA DDL work unchanged.
- **HA**: the projection adds no Raft traffic; schema changes travel as today's schema entries.
- Statements built by the literal parameterizer (statement cache) keep every literal inside `FACETS`, so two queries with different ranges never share a plan.

---

## 14. Testing strategy

| Layer | What | State |
|---|---|---|
| Unit (engine) | Codecs, segments, dead sets, dictionaries, manifest, dirty page sets, placeholder and chunk resolution, counting, patching, parser, calendar/DST boundaries, budget | Delivered |
| Equivalence oracle | `FacetOracle` runs the equivalent `GROUP BY`; randomized inserts, updates, deletes, slot reuse, records growing into placeholders and chunks, external properties, duplicate tags in one record, schema changes, edges on vertices, subtypes and 3-bucket types; compared at lag 0 and **while lagging** (patching must make them equal) | Delivered |
| Crash | Named kill points during flush, merge, dictionary append, compaction, WAL drop vs recovery-set persist, commit between marks; recovery must reach the oracle and count no rebuild | Delivered |
| Integration (server) | HTTP, ndjson, gRPC, both remote clients, OpenAPI, Prometheus, ACL (`FacetsSubtypeAclIT`), memory budget under concurrency | Delivered |
| HA integration (`ha-raft`) | Follower convergence, forwarded writes, snapshot install triggering rebuild, projection never replicated, checkpoint guard, facets on followers including over-budget | Delivered |
| HA resilience (`e2e-ha`, chaos) | `FacetWorkload` beside the ledger workload; `FacetCheck` at each convergence checkpoint reports `FACET_DIVERGENCE`, `FACET_DRIFT`, `FACET_META`, `FACET_STALL`, `FACET_ERROR`, `FACET_SCAN`; facet methods in `LeaderFailoverIT` (new replica rebuilds), `RollingRestartIT` and `NetworkPartitionRecoveryIT` (restart recovers without rebuild), `SplitBrainIT`, `PacketLossIT` | Planned (plan 6) |
| Load | `SingleServerFacetLoadTestIT`, `ThreeNodesFacetLoadTestIT`: continuous ingest with updates plus concurrent search-with-facets; logs apply lag, patch counts, latency, budget waits; asserts only stable invariants | Planned (plan 6) |
| Benchmarks | Spike, count, ingest, facets vs `GROUP BY`; `@Tag("benchmark")`, report only | Delivered |

Each plan was executed task by task with an independent review per task and a final cross-cutting review in three areas (concurrency and lifecycle; durability, recovery and HA; schema, metrics, ingest and fixtures), followed by fix batches and a re-run of the gate.

---

## 15. Remaining work

| Item | Owner | Notes |
|---|---|---|
| Plan 5 Tasks 2-15 | Plan 5 | Hierarchy definition, view and manager; HIERARCHY, DISTINCT (HLL), STATS collectors; prefix search and `AFTER`; `facet.searchValues`; `GROUP BY` acceleration (eligibility, execution, equivalence); transports for the new kinds; regression run |
| Plan 6 | Plan 6 | Chaos invariant, five scenarios, two load tests |
| Tuning | #10662 | Section 8.6 |
| Python client | #10666 | Separate repository, before the merge |
| User documentation | Docs pass | Includes the back-tick note for `unwind` / `timeout` aliases |
| Product questions from the spec | Users driving the feature | Facet types actually used, facets per request and top-N, typical cardinality (especially tags), typical and maximum match counts, latency target, tag-tree change rate, vector candidate set |

---

## 16. Risks, open items and bugs found

### 16.1 Risks and open items

| Item | Impact | Mitigation / plan |
|---|---|---|
| Ingest overhead +6-10% inserts, +4.5% updates | Misses the spec goal "ingest cost unchanged" | Tuning phase #10662; commit path already clean |
| Page-granular pending marks | A lagging broad query can patch a large share of its matches (60% in the benchmark), costing about a third of a `GROUP BY` | Exact by default; ratio 0 for latency; record-granular marks in tuning |
| Selective queries have fixed per-segment and per-stripe costs | At very low selectivity (0.01%) or very large buckets the floor could approach an index-served `GROUP BY` | Point-read path delivered; to be measured at scale in plan 6 and tuning |
| Heap target at 50M records not yet measured | Spec target "tens of MB for 50M records, four fields" unverified | Plan 6 load tests |
| External-property bucket pages are not covered by the unclean-open page-version check | A crash-time change on an external page could be missed until that record is next written | Documented open item |
| One unexplained divergence seen once (1 of 75 runs) during plan 3 testing, with a rebuild concurrent with vertex, edge and external-body writes | Possible rare miscount in that mix | Two plausible mechanisms found and fixed in the final review; a stress test has not reproduced it since; plan 6 chaos runs exercise the area |
| Restricted users rebuild the multi-bucket ordinal merge per facet | O(values x buckets) per query for those users | Tuning |
| `DECIMAL` facet values equal `GROUP BY` by `compareTo`, not always by `equals` (scale stripped) | Cosmetic difference for values with trailing zeros | Documented; `STATS` refuses `DECIMAL` |
| `DATETIME_MICROS`/`NANOS` read at millisecond precision on the fallback scan path | Sub-millisecond bounds can differ between scan and index answers | Plan 5 carried item |
| No-WAL commits write a small marker synchronously | Deliberate spec deviation for bulk loads only | Accepted |
| Subsystem size (about 23,000 lines of new main code) | Review and maintenance load | Per-task and cross-cutting reviews; plan workspaces kept for the user's code review |

### 16.2 Pre-existing bugs found by the work

| Issue | What | Status |
|---|---|---|
| #10617 | `LocalBucket` placeholder spill wrote a 9-byte pointer into a slot smaller than 9 bytes, overwriting the next record (silent data corruption, reachable in production). Found by the plan 3 test-data generator | Filed as critical; fixed on main by #10639, not yet in this branch (it is based on an older main) |
| #10676 | `DateUtils.format(value, pattern, zone)` ignores the zone for `LocalDateTime`/`ZonedDateTime`, so a `GROUP BY` on a zoned date format groups by UTC month. Found while validating zoned histograms against `GROUP BY` | Filed, open |
| Full-text scoring | A security issue | Handled privately |
| Type rename left a stale index-map key, so `DROP INDEX` on a renamed index failed until reload | Affected every index family | Fixed in this branch (one call, covered by the type-rename regression tests) |

---

## 17. Alternatives considered

| Alternative | Why not chosen |
|---|---|
| Keep emulating with `GROUP BY` | One pass and one full record read per facet; no top-N, ranges, histograms, disjunctive or hierarchical facets; no combined response. Measured 7-49x slower |
| Lucene's facet module on the full-text index | Only usable where the full-text index keeps Lucene structures, and covers only full-text matches, not SQL filters or vector search |
| External OLAP store | Second infrastructure, consistency lost, contradicts the multi-model pitch |
| Transactional projection on the same snapshot (the original analysis) | Puts projection writes on the commit path and makes ingest pay; the team accepted an Elasticsearch-like lag of under a second, and exactness is restored by patching instead |
| Refactor the time-series sealed store into a shared store | It rewrites whole files outside the WAL (wrong model for upserts), has no position addressing, no update or delete, sequential codecs and a per-store heap cache. Only the codecs, the block header/CRC and a server-wide cache were extracted |
| Graph Analytical View storage | Uncompressed heap arrays sized to the whole graph; only its snapshot-plus-overlay update pattern was borrowed |
| Per-block dictionaries with global ordinals rebuilt at compaction | Costly remaps (Elasticsearch's first-query latency spike); replaced by one append-only dictionary per bucket and property with a sorted rank view |
| Record-level change tracking | Replicated commits and WAL replay know pages, not records; page marks cover all three paths with one bit |
| Approximate counts by default | A known source of user complaints in other engines; exact by default, approximate only when explicitly requested |
| `SELECT @rid` as the match source | Spike run 1: NO-GO, result-row materialization dominated |
| RoaringBitmap | No measured need; would be a new dependency |
| Memory-mapped segments | Positional `FileChannel` reads chosen; measured again by the count benchmark |
| DataSketches for distinct count | Large dependency for one estimator used only on explicit request; in-house HLL of about 150 lines |
| Deferred, not rejected | Per-value bitmap indexes for very low cardinality fields; star-tree pre-aggregation for filter-only dashboards; nested facets and pipeline aggregations; sharded top-N refinement |

---

## 18. Where the delivery differs from the original spec

| Spec point | Delivered |
|---|---|
| One dictionary per type and property | Per bucket and property, with generations (spec updated) |
| One mark after page visibility | Two marks around page publication, plus a WAL-drop guard |
| Apply thread per database or type | One JVM-wide pool of 2 threads, at most one pass per database |
| Patch budget absolute, all-or-nothing | Relative: `max(10000, 1.0 x matches)`, ratio configurable 0-1 |
| ndjson facets as a separate line | Inside the `stats` trailer |
| Bucket truncate drops the projection | Projection empties itself through ordinary deletes |
| Nothing on the commit path but a bit | Plus a synchronous `NOWAL` marker for no-WAL commits, once per flush epoch |
| Ingest cost unchanged | +6-10% inserts, +4.5% updates; tuning planned |
| Count arrays pooled per thread | Not pooled (a pooled array would sit outside its budget reservation); tuning item |
| Property rename and type change handled | Handled by the reconciler, but not reachable by DDL today |
| Unclean-open page validation | A background job after open, bounded, with a rebuild above 1,024 pages and 25% of the bucket |

---

## 19. Glossary

| Term | Meaning |
|---|---|
| Facet | Counts (or stats) per value, range or time bucket over the matches of a query |
| Projection | The columnar copy of a bucket's indexed properties, derived from committed pages |
| Segment | Immutable on-disk file of rows for a set of positions, one column per property |
| Overlay | The in-memory newest segment, not yet flushed |
| Position | `page x maxRecordsInPage + slot`, the record's address inside its bucket |
| Presence | Which positions a segment holds a row for |
| Dead set / live bitmap | Rows of a segment superseded or deleted / the rest |
| Ordinal | Dense integer standing for a dictionary value |
| Rank view | Sorted order of the ordinals, for value ordering and prefix search |
| Cross-bucket ordinal map | Merge of per-bucket ordinals into one space for multi-bucket or polymorphic facets |
| Manifest | The atomically replaced file naming the live files of a bucket's projection |
| Dirty page / mark | A bucket page changed by a commit and not yet applied to the projection |
| Epoch | A generation of the dirty page set; its start time gives `lagMs` |
| Apply pass | Background re-read of dirty pages into the overlay |
| Recovery set | Persisted dirty and unflushed pages, re-applied at open |
| Pending positions | Positions on dirty pages, input to patching |
| Patching | Reading lagging matches from the records so counts stay exact |
| Fallback scan | Counting a bucket from its records when the projection cannot answer |
| Disjunctive facet | A facet that ignores its own `FILTER` while counting |
| Oracle | The test helper that computes the expected counts with `GROUP BY` |

---

## 20. References

Design and plans (worktree `.worktrees/feat/columnar-faceting`):

- Spec: `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md`
- Roadmap and cross-plan contract: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md` (including "Plan N additions", "Tuning phase (#10662)", "Pre-merge steps (#10666)")
- Plans: `docs/superpowers/plans/2026-10-19-columnar-faceting-1-spike.md`, `-2-extraction.md`, `-3-core.md` (its "Spike results" section holds the plan 1 measurements and the plan 3 count and ingest benchmarks), `-4-query-api.md`, `-5-advanced.md`, `-6-resilience-load.md`
- Execution rulings and open items: `.superpowers/sdd/plan1-rulings.md` to `plan4-rulings.md`, `plan3-open-items.md`, `plan4-open-items.md`
- Plan 4 benchmark and final fixes: `.superpowers/sdd/2026-10-19-columnar-faceting-4-query-api/task-19-report.md`, `final-fix-batch2-report.md`
- Plan 3 final reviews and fixes: `.superpowers/sdd/2026-10-19-columnar-faceting-3-core/final-review-{A,B,C}.md`, `final-fix-batch{1,2,3}-report.md`
- Original analysis (Italian, superseded by the spec): `Faceting colonnare analisi.md` in the main checkout's `docs/` directory, dated 2026-10-08

Code:

- `engine/src/main/java/com/arcadedb/index/columnar/` (index type, projection, segments, dictionaries, tracker, applier, flusher, rebuilder, reconciler, recovery)
- `engine/src/main/java/com/arcadedb/query/facet/` (specs, compiler, executor, collectors, patcher, budget, fallback, results)
- `engine/src/main/java/com/arcadedb/engine/codec/`, `engine/src/main/java/com/arcadedb/engine/block/` (codecs, block format, block cache)
- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (`arcadedb.columnar.*`, `arcadedb.blockCache.maxBytes`)
- `server/src/main/java/com/arcadedb/server/monitor/ColumnarMetrics.java`
- `grpc/src/main/proto/arcadedb-server.proto` (`GrpcFacets`)

Issues: #10355 (umbrella) and #10356-#10361 (per plan), #10662 (tuning), #10666 (Python, pre-merge), #10617 and #10639 (placeholder spill bug and fix), #10676 (date format zone bug).
