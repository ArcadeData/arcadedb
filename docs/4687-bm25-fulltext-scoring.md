# Native BM25 full-text scoring (issue #4687)

## Summary

Full-text (`FULL_TEXT`) indexes can now rank results with **Okapi BM25** (term frequency + inverse document frequency + document
length normalization) instead of the legacy term-coordination (match-count) model. BM25 is the default for newly created
full-text indexes; pre-existing indexes keep CLASSIC scoring until rebuilt or explicitly switched.

## Usage (SQL)

```sql
-- New index: BM25 by default
CREATE INDEX ON Article (content) FULL_TEXT;

-- Explicit BM25 with tuned parameters and a per-field boost (multi-field index)
CREATE INDEX ON Article (title, body) FULL_TEXT
  METADATA {"similarity": "BM25", "bm25_k1": 1.2, "bm25_b": 0.75, "title_boost": 3.0};

-- Keep the legacy coordination scoring
CREATE INDEX ON Article (content) FULL_TEXT METADATA {"similarity": "CLASSIC"};

-- Query: $score is a float BM25 relevance score
SELECT title, $score FROM Article
  WHERE SEARCH_INDEX('Article[content]', 'java database') = true
  ORDER BY $score DESC;

-- Caret boost: weight a term (or a field-qualified term) more (Lucene/Elasticsearch idiom)
SELECT title, $score FROM Article
  WHERE SEARCH_INDEX('Article[title,body]', 'title:java^3 database') = true ORDER BY $score DESC;

-- Inspect the scoring with EXPLAIN / PROFILE: the full-text fetch step reports the BM25 similarity,
-- k1/b, corpus stats (N, avgdl) and each query term's df + idf.
EXPLAIN SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java database') = true;
```

### Metadata keys

| Key            | Default | Meaning                                                            |
|----------------|---------|--------------------------------------------------------------------|
| `similarity`   | `BM25`  | `BM25` or `CLASSIC`                                                 |
| `bm25_k1`      | `1.2`   | term-frequency saturation                                          |
| `bm25_b`       | `0.75`  | document-length normalization in [0,1] (0 disables length norm)    |
| `<field>_boost`| `1.0`   | multiplier applied to BM25 contributions of `field:term` matches   |

The Java builder mirrors these: `buildTypeIndex(...).withType(FULL_TEXT).withFullTextType().withBM25(k1, b).withFieldBoost("title", 3.0f).create()`.

### Boosts

Two composable mechanisms, both following the Lucene/Elasticsearch conventions:

- **Configured per-field boost** (`<field>_boost` metadata) - a default weight applied to BM25 contributions of `field:term`
  matches on that field.
- **Query-time caret boost** (`term^N`, e.g. `title:java^3`) - weights a specific term in a specific query. Parsed by the Lucene
  query syntax. The effective weight is `caret * field_boost`.

Field boosts apply to **field-qualified** query terms (e.g. `title:java`). Unqualified terms on a multi-field index score against
the field-agnostic token with boost 1.0.

### Inspecting scores with EXPLAIN / PROFILE

`EXPLAIN`/`PROFILE` of a query whose `WHERE` uses `SEARCH_INDEX(...)` annotates the full-text fetch step (`FETCH FROM INDEXED
FUNCTION`) with a `SCORING` line containing the BM25 similarity, the `k1`/`b` parameters, the corpus statistics (`totalDocs`,
`avgDocLength`) and, per query term, its document frequency (`df`), `idf` and applied `boost`. This is the query-level "why are
the scores what they are" view; per-document contributions are intentionally not included (a plan describes the query, not
individual rows). There is no separate explain function.

## Design

- **Formula** — `BM25Scorer` (pure, DB-free, unit-tested): `idf(N,df)=ln((N-df+0.5)/(df+0.5)+1)` (matches the sparse-vector
  index's IDF), `termScore = idf * tf*(k1+1) / (tf + k1*(1 - b + b*dl/avgdl))`.
- **Persisted statistics** — to avoid re-reading documents at query time, the per-posting term frequency (`tf`) and document
  length (`docLength`) are stored inline in the postings. A posting value is serialized as
  `compressedRID + tf(varint) + docLength(varint)` when the index uses BM25.
  - This is carried through the entire existing RID-typed pipeline (transaction staging, commit replay, compaction, page cursors)
    by `FullTextPostingRID extends DatabaseRID`: every `(RID)` cast, deletion-marker bucket-sign check and `RidHashSet`
    membership keeps working because `equals`/`hashCode` are inherited from `RID` (bucket id + offset only).
- **Scoring passes** — `df` (for IDF) is derived by counting a token's postings. The `SEARCH_INDEX` path (a candidate set is
  known) scores in a single pass, collecting only candidate postings. The direct index-lookup path (no candidate set) scans each
  token's postings twice (count `df`, then accumulate) to bound memory rather than materialize the full list; for a very common
  term this doubles that token's read I/O. Both keep memory bounded by the result/candidate set.
  - Only the value (de)serialization in `LSMTreeIndexAbstract` changes, gated by `storeTermFrequency`. Every non-full-text LSM
    index (graph edges, unique constraints, type indexes) keeps the byte-identical RID-only format.
  - `storeTermFrequency` is derived from the persisted `similarity` (BM25 ⇒ on). It is propagated to the mutable index, its
    compacted sub-index, and across compaction/split, so old RID-only files still parse and score as CLASSIC.
- **Corpus statistics** — `N` for IDF is the per-bucket live record count (consistent with the per-bucket `df`). The shared
  type-wide counters in `FullTextIndexMetadata` (document count + sum of document lengths) feed only `avgdl`, are maintained
  incrementally on put/remove, and are persisted. (Note: this mixes scopes deliberately - `N`/`df` are per-bucket while `avgdl`
  is type-wide, unlike Elasticsearch/Lucene where both are shard-local; `avgdl` is only a length normalizer so a type-wide
  estimate is fine and avoids per-bucket length bookkeeping.) They are **not** transactionally reversed on rollback and a removed document's
  length is recomputed (so it can drift after an analyzer change); since they affect only the `avgdl` length normalizer this
  degrades ranking gradually, not catastrophically. There is **no background recompute** - `recomputeBM25Counters()` (or a
  rebuild) repairs them exactly on demand. After a restart the persisted counters may lag the on-disk data (documents indexed
  after the last schema save); the first BM25 query validates them once with a cheap live document count and rebuilds only if
  they disagree, so a clean restart with fresh counters pays nothing while a stale one self-heals.
- **Metadata persistence fix** — `LSMTreeFullTextIndex.toJSON()` previously dropped analyzer/operator config, so a restart
  silently reverted custom analyzers to `StandardAnalyzer`. The metadata round-trip is now implemented (and restored in
  `LocalSchema` reload), which also persists the BM25 settings and counters.

## Backward compatibility

- Existing full-text index files open and score with CLASSIC (no `tf` on disk, no behavior change on upgrade).
- Getting BM25 on existing data requires **rebuilding** the full-text index (the build path re-analyzes documents and writes the
  `tf`/`docLength` postings + corpus counters). `tf` was never stored before and past compactions discarded posting multiplicity,
  so there is no in-place migration.
- **`$score` is now a float** (`Float`), including for CLASSIC indexes where it was previously an `Integer` (the coordination
  match count, now widened to e.g. `3.0`). Application code that read `$score` as an `Integer` must read it as a `Number`/`Float`.
  This is the one user-visible behavior change for existing CLASSIC indexes.

## Compaction fix (pre-existing bug, also affected CLASSIC)

Full-text index **compaction** previously dropped postings when a *single token's* posting list spanned multiple compacted leaf
pages: the compacted root is a positional sparse index that cannot index one leaf page under two keys, so a key's first values
left on a shared continuation page became unreachable on read. This was independent of BM25 (reproduced identically with CLASSIC).
Fixed here by forcing an overflowing key to start on a fresh page it fully owns; covered by `FullTextBM25CompactionTest` at tiny
page sizes.
