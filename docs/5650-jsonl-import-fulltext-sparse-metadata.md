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

## Deferred / out of scope

- `GEOSPATIAL` JSONL restore (issue calls it lower severity; not addressed here).
- `LSMTreeFullTextIndex.getMetadata()` / `LSMSparseVectorIndex.getMetadata()` returning the base
  `IndexMetadata` instead of the typed subtype (issue's "Related" section; the dedicated
  `getFullTextMetadata()`/`getSparseMetadata()` accessors already exist and were used in the test instead).
