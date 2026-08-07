# Issue #5650: JSONL import restores FULL_TEXT and LSM_SPARSE_VECTOR indexes without their metadata

## Root cause

`JsonlImporterFormat.loadSchema()` special-cased only `LSM_VECTOR` indexes to route through their
dedicated builder (`TypeLSMVectorIndexBuilder.withPersistedMetadata`), because that index type is the
only one whose metadata (dimensions, similarity, HNSW parameters, ...) lives entirely off the generic
`getOrCreateTypeIndex(idxType, unique, idxFields)` path. `FULL_TEXT` and `LSM_SPARSE_VECTOR` have the
exact same shape - their real configuration (analyzers/BM25 tuning/per-field boosts for FULL_TEXT;
dimensions/modifier/weightQuantization for LSM_SPARSE_VECTOR) lives only in their own metadata class,
not in anything `getOrCreateTypeIndex` can carry - but the importer fell through to the generic path for
them, silently dropping every one of those settings on restore even though the exporter (via
`LSMTreeFullTextIndex.toJSON()` / `LSMSparseVectorIndex.toJSON()`) writes all of it.

Unlike `LSM_VECTOR`, both affected types DO serialize `unique`/`nullStrategy` in their persisted JSON, so
the fix applies those explicitly rather than omitting them.

## Fix

Mirrors the shape #5643 established for the vector path (suggested by the issue itself):

1. **`TypeFullTextIndexBuilder.withPersistedMetadata(JSONObject)`** (new) - restores a `FullTextIndexMetadata`
   from a persisted definition via the existing (already-tolerant) `FullTextIndexMetadata.fromJSON()`, the
   same reader `LocalSchema` uses to reload the index from `schema.json`, as opposed to the strict
   `withMetadata(JSONObject)` used for the `METADATA` clause of `CREATE INDEX`.
2. **`TypeLSMSparseVectorIndexBuilder.withPersistedMetadata(JSONObject)`** (new) - same shape, backed by
   `LSMSparseVectorIndexMetadata.fromJSON()`.
3. **`JsonlImporterFormat.loadSchema()`** - routes `FULL_TEXT` and `LSM_SPARSE_VECTOR` index entries through
   two new private methods, `loadFullTextIndex()` and `loadSparseVectorIndex()`, which build the index via
   `buildTypeIndex(...).withType(...).withFullTextType()/.withSparseVectorType()`, apply `unique` and
   `nullStrategy` explicitly (present in these two types' persisted JSON, unlike LSM_VECTOR), then
   `withPersistedMetadata(idx)` to restore the rest, exactly as `loadVectorIndex()` already did for
   LSM_VECTOR.

`GEOSPATIAL` (also called out in the issue as affected "in principle") and the `getMetadata()` subtype gap
on `LSMTreeFullTextIndex`/`LSMSparseVectorIndex` (the issue's "Related" section) are out of scope for this
fix - the issue frames the first as lower severity (a rebuilt geo index still re-tokenizes consistently) and
the second as a separate, not-blocking observation. Both existing dedicated accessors
(`getFullTextMetadata()` / `getSparseMetadata()`) were used directly in the new test instead.

## Files changed

- `engine/src/main/java/com/arcadedb/schema/TypeFullTextIndexBuilder.java` - added `withPersistedMetadata(JSONObject)`
- `engine/src/main/java/com/arcadedb/schema/TypeLSMSparseVectorIndexBuilder.java` - added `withPersistedMetadata(JSONObject)`
- `integration/src/main/java/com/arcadedb/integration/importer/format/JsonlImporterFormat.java` - routes
  FULL_TEXT/LSM_SPARSE_VECTOR through the dedicated builders instead of `getOrCreateTypeIndex`
- `integration/src/test/java/com/arcadedb/integration/FullTextSparseVectorIndexExportImportIT.java` (new) -
  regression test

## Test

`FullTextSparseVectorIndexExportImportIT` (new), modeled on `VectorIndexExportImportIT`:

- Creates a source database with a `FULL_TEXT` index tuned away from every default (`bm25_k1: 1.7`,
  `bm25_b: 0.55`, `defaultOperator: AND`, `title_boost: 3.5`, `allowLeadingWildcard: true`) and an
  `LSM_SPARSE_VECTOR` index tuned away from every default (`dimensions: 64`, `modifier: IDF`,
  `weightQuantization: FP16`), with data in both types.
- Exports to JSONL, imports into a fresh database.
- Asserts the **restored metadata** (via `getFullTextMetadata()` / `getSparseMetadata()`), not just that
  the indexes exist - a test that only checks existence passes today, per the issue's own guidance.
- Asserts the full-text index is searchable post-import with the restored `AND` operator and title boost
  still contributing to ranking, and the sparse index has postings for all imported records.

### TDD verification

Confirmed the test reproduces the bug before the fix: with the three source changes stashed, the test
failed with `expected: 1.7f but was: 1.2f` (BM25 `k1` silently reverted to default), and the run's own log
line showed the sparse index restored as `dimensions:0, modifier:NONE, weightQuantization:INT8` against a
source of `dimensions:64, modifier:IDF, weightQuantization:FP16`. With the fix restored, the same test
passes and the log line shows every tuned setting preserved.

## Verification run

- `mvn -pl engine,integration -am compile` - clean compile.
- `FullTextSparseVectorIndexExportImportIT` - PASS (1/1), both red (pre-fix) and green (post-fix) confirmed.
- `VectorIndexExportImportIT`, `JsonlExporterIT` (4 tests) - PASS, no regression on the sibling/adjacent
  export-import paths.
- `Issue5639IndexMetadataKeysTest` (17), `Issue5765IndexSettingsIfNotExistsTest` (8),
  `Issue5607VectorIndexMetadataTest` (11), `FullTextBM25Test` (33), `FullTextMultiPropertyTest` (6),
  `FullTextRebuildOverExistingTest` (3) - all PASS, no regression on the METADATA-clause / rebuild /
  BM25-scoring paths the new builder methods sit beside.

## PR

https://github.com/ArcadeData/arcadedb/pull/5936

## Review cycles

**Cycle 1** - head SHA `6f3564b0471cb137865cc127faaa4b570754b0ef`

`claude[bot]` reviewed via an issue comment (not a formal PR review object) on 2026-08-07T17:07:53Z.
Outcome: approval, "No blocking issues found." Traced the field names end-to-end between the exporter's
`toJSON()` and the metadata's `fromJSON()`, confirmed the `unique`/`nullStrategy` re-application order has
no clobbering hazard, and called out the test's use of typed metadata assertions plus a live BM25 query as
good defense against "restored but not wired in" regressions.

One optional nitpick, explicitly flagged "Not a strong ask": `loadFullTextIndex`/`loadSparseVectorIndex`
are near-identical and could share a small helper "if a fourth index type ever needs this treatment."
**Declined (YAGNI):** there is no third occurrence of this pattern today - only these two plus the existing
`loadVectorIndex`, which already differs in whether `unique`/`nullStrategy` apply - and each method already
carries its own javadoc explaining why it diverges from the `LSM_VECTOR` baseline. Introducing a shared
helper for two call sites, each already justified independently, would be speculative generality ahead of
a genuine third caller.

No code changes were needed in response to this cycle. Working tree stayed empty - clean approval.

## Final state

**clean-approval** (1 review cycle, 0 code changes required after the initial PR).

## Deferred / out of scope

- `GEOSPATIAL` JSONL restore (issue calls it lower severity; not addressed here).
- `LSMTreeFullTextIndex.getMetadata()` / `LSMSparseVectorIndex.getMetadata()` returning the base
  `IndexMetadata` instead of the typed subtype (issue's "Related" section; the dedicated
  `getFullTextMetadata()`/`getSparseMetadata()` accessors already exist and were used in the test instead).
