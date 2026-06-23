# ArcadeDB v.27.7.1 Release Highlights

This release adds native **BM25 relevance scoring** to `FULL_TEXT` indexes (issue
[#4687](https://github.com/ArcadeData/arcadedb/issues/4687)). It also includes two user-visible behaviour changes for full-text
indexes that require attention when upgrading - see **Breaking Changes** below.

### ✨ New Features

- Native **Okapi BM25** scoring for `FULL_TEXT` indexes: term frequency + inverse document frequency + document-length
  normalization, replacing the legacy term-coordination (match-count) model. BM25 is the default for newly created full-text
  indexes ([#4687](https://github.com/ArcadeData/arcadedb/issues/4687)).
- Configurable BM25 parameters (`bm25_k1`, `bm25_b`) and per-field boosts (`<field>_boost`) via index `METADATA`, plus the Java
  builder API (`withBM25(k1, b)`, `withFieldBoost(field, boost)`, `withSimilarity(...)`).
- Query-time caret boosts (`term^N`, `field:term^N`, group boosts) following the Lucene/Elasticsearch convention.
- `EXPLAIN` / `PROFILE` now surface BM25 scoring metadata (similarity, `k1`/`b`, corpus stats, and per-term `df`/`idf`/`boost`)
  on the full-text fetch step.

### 🐛 Bug Fixes

- Fixed a pre-existing full-text **compaction** bug that silently dropped postings when a single token's posting list spanned
  multiple compacted leaf pages (affected CLASSIC scoring too) ([#4687](https://github.com/ArcadeData/arcadedb/issues/4687)).
- Fixed full-text index **metadata persistence**: custom analyzer/operator configuration was silently reverted to
  `StandardAnalyzer` on restart; it now round-trips correctly (and carries the BM25 settings and corpus counters).
- A property literally named `content` on a multi-property full-text index no longer collides with the query parser's default
  field, so `content:term` clauses (and `content_boost`) now resolve and score correctly.

## ⚠️ Breaking Changes (migration notes)

These two changes affect existing `FULL_TEXT` indexes (including indexes that keep CLASSIC scoring). Review them before upgrading.

### 1. `$score` is now a `Float` (was `Integer` for CLASSIC indexes)

`$score` is now always a floating-point relevance score. For CLASSIC (term-coordination) indexes it was previously an
`Integer` match count (e.g. `3`) and is now the same value widened to a `Float` (e.g. `3.0`). BM25 indexes return a genuine
fractional score.

- **Impact:** application code that reads `$score` and casts/expects a `java.lang.Integer` will fail (e.g. `ClassCastException`)
  or behave unexpectedly. This is silent at compile time for code using `getProperty("$score")`.
- **Migration:** read `$score` as a `Number` (or `Float`), e.g. `((Number) result.getProperty("$score")).floatValue()`. SQL that
  only sorts/filters on `$score` (`ORDER BY $score DESC`, `WHERE $score > ...`) is unaffected.

### 2. Full-text `get()` cursor order is now most-relevant-first

The full-text index cursor previously returned matches sorted by ascending score (least-relevant first) and did not apply the
result `limit`. It now returns most-relevant-first (descending score, with a stable RID tiebreaker) and honors the `limit`.

- **Impact:** applications that consumed the raw index cursor order (without an explicit SQL `ORDER BY`) will see the order
  reversed, and a `limit` now truncates to the top-N by relevance instead of being ignored. Queries with an explicit
  `ORDER BY $score DESC` are unaffected (they already sorted).
- **Migration:** if you relied on the old ascending order, add an explicit `ORDER BY $score ASC`. The new default
  (most-relevant-first) is almost always what full-text callers want.

### Getting BM25 on existing data

Existing full-text index files open and continue to score with **CLASSIC** (no behaviour change on upgrade beyond the `$score`
type above). To switch existing data to BM25 you must **rebuild** the full-text index: term frequencies were never stored before
and past compactions discarded posting multiplicity, so there is no in-place migration. New indexes use BM25 automatically.

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.0...27.7.1
