# ArcadeDB v.27.7.1 Release Highlights

This release adds native **BM25 relevance scoring** to `FULL_TEXT` indexes (issue
[#4687](https://github.com/ArcadeData/arcadedb/issues/4687)). It also includes two user-visible behaviour changes for full-text
indexes that require attention when upgrading - see **Breaking Changes** below.

### New Features

- Native **Okapi BM25** scoring for `FULL_TEXT` indexes: term frequency + inverse document frequency + document-length
  normalization, replacing the legacy term-coordination (match-count) model. BM25 is the default for newly created full-text
  indexes ([#4687](https://github.com/ArcadeData/arcadedb/issues/4687)).
- Configurable BM25 parameters (`bm25_k1`, `bm25_b`) and per-field boosts (`<field>_boost`) via index `METADATA`, plus the Java
  builder API (`withBM25(k1, b)`, `withFieldBoost(field, boost)`, `withSimilarity(...)`).
- Query-time caret boosts (`term^N`, `field:term^N`, group boosts) following the Lucene/Elasticsearch convention.
- `EXPLAIN` / `PROFILE` now surface BM25 scoring metadata (similarity, `k1`/`b`, corpus stats, and per-term `df`/`idf`/`boost`)
  on the full-text fetch step.

### Bug Fixes

- Fixed a pre-existing full-text **compaction** bug that silently dropped postings when a single token's posting list spanned
  multiple compacted leaf pages (affected CLASSIC scoring too) ([#4687](https://github.com/ArcadeData/arcadedb/issues/4687)).
- Fixed full-text index **metadata persistence**: custom analyzer/operator configuration was silently reverted to
  `StandardAnalyzer` on restart; it now round-trips correctly (and carries the BM25 settings and corpus counters).
- A property literally named `content` on a multi-property full-text index no longer collides with the query parser's default
  field, so `content:term` clauses (and `content_boost`) now resolve and score correctly.
- Fixed `REBUILD INDEX * WITH <setting> = <value>` silently dropping the **first** setting: the `*` (all-indexes) form has no
  index-name identifier, but the parser still skipped `identifier(0)` as if it were one. Any wildcard rebuild with a `WITH`
  clause (e.g. `REBUILD INDEX * WITH batchSize = 1000`) was therefore ignoring that setting; named-index rebuilds were unaffected.

## Breaking Changes (migration notes)

These two changes affect existing `FULL_TEXT` indexes (including indexes that keep CLASSIC scoring). Review them before upgrading.

### 1. `$score` is now a `Float` (was `Integer` for CLASSIC indexes)

`$score` is now always a floating-point relevance score. For CLASSIC (term-coordination) indexes it was previously an
`Integer` match count (e.g. `3`) and is now the same value widened to a `Float` (e.g. `3.0`). BM25 indexes return a genuine
fractional score.

- **Impact:** application code that reads `$score` and casts/expects a `java.lang.Integer` will fail (e.g. `ClassCastException`)
  or behave unexpectedly. This is silent at compile time for code using `getProperty("$score")`.
- **Migration:** read `$score` as a `Number` (or `Float`), e.g. `((Number) result.getProperty("$score")).floatValue()`. SQL that
  only sorts/filters on `$score` (`ORDER BY $score DESC`, `WHERE $score > ...`) is unaffected.

### 2. Full-text `get()` cursor order is now most-relevant-first (affects CLASSIC indexes)

The **CLASSIC** (term-coordination) full-text index cursor previously returned matches sorted by ascending score
(least-relevant first) and did not apply the result `limit`. It now returns most-relevant-first (descending score, with a stable
RID tiebreaker) and honors the `limit` - consistent with the BM25 path, which already sorted descending. This change therefore
affects CLASSIC indexes specifically (BM25 indexes are new in this release and were never ascending).

- **Impact:** applications that consumed the raw index cursor order (without an explicit SQL `ORDER BY`) will see the order
  reversed, and a `limit` now truncates to the top-N by relevance instead of being ignored. This includes callers of the
  low-level `Index.get(...)` API directly on a CLASSIC full-text index, not just SQL. Queries with an explicit
  `ORDER BY $score DESC` are unaffected (they already sorted).
- **Migration:** if you relied on the old ascending order, add an explicit `ORDER BY $score ASC`. The new default
  (most-relevant-first) is almost always what full-text callers want.

### 3. `IndexCursorEntry` identity no longer includes the score (extension/plugin API)

`IndexCursorEntry.equals()`/`hashCode()` now use only `(record, keys)`; the relevance `score` is no longer part of identity. This
lets the same document be deduplicated in a result `Set` regardless of score (needed for BM25, where a document can be scored more
than once). No in-tree caller relied on the old behaviour, but **any** caller - not only plugin/extension authors - that places
`IndexCursorEntry` objects (e.g. from `Index.get(...)`) into a `Set`, or uses them as map keys, expecting score-differentiated
entries will silently see different deduplication. Treat `(record, keys)` as the identity.

### Getting BM25 on existing data

Existing full-text index files open and continue to score with **CLASSIC** (no behaviour change on upgrade beyond the `$score`
type above). To switch existing data to BM25 you must **rebuild** the full-text index: term frequencies were never stored before
and past compactions discarded posting multiplicity, so there is no in-place migration. New indexes use BM25 automatically.

**Corpus counters and `REBUILD INDEX ... WITH statsOnly = true`.** BM25 keeps per-type corpus counters (document count + total
length) that feed the average-document-length normalizer. They are maintained incrementally and are self-corrected once per
session on the first query, but they are not transactionally reversed on rollback and the session check validates only the
document count. After a large bulk import, a workload with many rolled-back transactions, or migrating a pre-BM25 index, run
`REBUILD INDEX <name> WITH statsOnly = true` (or `REBUILD INDEX *`) to re-derive the counters exactly without a full reindex -
also useful to pre-warm a cold upgraded index before serving traffic (the first BM25 query otherwise does a one-time full scan).

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.0...27.7.1
