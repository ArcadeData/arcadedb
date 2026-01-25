# SEARCH_MORE Similar Document Search Design

**Date:** 2026-01-25
**Status:** Draft
**Prerequisites:** Full-text index improvements (2026-01-23 design)

## Overview

Add `SEARCH_INDEX_MORE()` and `SEARCH_FIELDS_MORE()` SQL functions to find documents similar to a given set of source documents. This implements Lucene's "More Like This" capability for content discovery, recommendations, and "related articles" use cases.

## Goals

1. **Similar document search** - Find documents similar to one or more source documents
2. **Consistent API** - Match existing `SEARCH_INDEX()` / `SEARCH_FIELDS()` naming pattern
3. **Full configurability** - Expose all Lucene MLT parameters
4. **Dual scoring** - Provide both raw `$score` and normalized `$similarity`

## Non-Goals

- Arbitrary text input (only RID-based similarity)
- `SEARCH_CLASS_MORE()` cross-index variant (future enhancement)
- Highlighting of similar terms (future enhancement)

---

## Design

### 1. Function Signatures

Two new SQL functions following existing naming conventions:

```sql
-- By index name
SEARCH_INDEX_MORE(indexName, sourceRIDs [, metadata])

-- By field names
SEARCH_FIELDS_MORE(fieldNames, sourceRIDs [, metadata])
```

**Examples:**

```sql
-- Find articles similar to #10:3 and #10:4
SELECT title, $score, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3, #10:4])

-- With configuration
SELECT title, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:3], {
  'minTermFreq': 2,
  'minDocFreq': 5,
  'maxSourceDocs': 50
})
```

**Return behavior:**
- Results ordered by similarity (highest first)
- `$score` - raw Lucene MLT score
- `$similarity` - normalized 0.0 to 1.0
- Source documents excluded by default

---

### 2. Configuration Parameters

All parameters are optional with sensible defaults:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `minTermFreq` | int | 2 | Term must appear this many times in source doc |
| `minDocFreq` | int | 5 | Term must exist in at least this many docs |
| `maxDocFreqPercent` | float | none | Ignore terms in more than X% of total docs |
| `maxQueryTerms` | int | 25 | Maximum terms used in similarity query |
| `minWordLen` | int | 0 | Minimum term length to consider |
| `maxWordLen` | int | 0 (unlimited) | Maximum term length to consider |
| `boostByScore` | boolean | true | Weight terms by their TF-IDF score |
| `excludeSource` | boolean | true | Exclude source RIDs from results |
| `maxSourceDocs` | int | 25 | Maximum source RIDs allowed (soft limit) |

**Example with multiple parameters:**

```sql
SELECT title, $similarity FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3, #10:4], {
  'minTermFreq': 1,
  'minDocFreq': 3,
  'maxQueryTerms': 50,
  'excludeSource': false
})
```

---

### 3. Error Handling

**Validation rules (fail fast):**

| Condition | Error |
|-----------|-------|
| Empty RID array | `"SEARCH_INDEX_MORE requires at least one source RID"` |
| RID doesn't exist | `"Record #10:999 not found"` |
| RID not in index | `"Record #10:3 is not indexed by 'Article[title,body]'"` |
| RID wrong type | `"Record #10:3 is not of type 'Article'"` |
| Index not found | `"Full-text index 'Article[title,body]' not found"` |
| Source limit exceeded | `"Source RIDs (100) exceeds maxSourceDocs limit (25)"` |

**Validation order:**
1. Check index exists
2. Check RID count within limit
3. For each RID: verify exists, correct type, and indexed
4. Execute similarity query

**Edge cases:**
- All source docs have no indexable content -> empty result set (not an error)
- Source docs share no common terms with corpus -> empty result set

---

### 4. Algorithm

**How "More Like This" works:**

1. **Extract terms** from source documents' indexed fields
2. **Filter terms** by configured thresholds (minTermFreq, minDocFreq, etc.)
3. **Select top N terms** (maxQueryTerms) ranked by TF-IDF score
4. **Build query** from selected terms (essentially an OR query)
5. **Execute against index** and score results
6. **Normalize scores** to produce `$similarity` (0.0 to 1.0)

**Score normalization for `$similarity`:**

```
$similarity = $score / maxScore
```

Where `maxScore` is the highest score in the result set, ensuring the most similar document has `$similarity = 1.0`.

**Multi-document handling:**

When multiple source RIDs are provided:
- Extract and merge terms from all source documents
- Term frequency = sum across all source docs
- Produces results similar to "any of these documents"

---

## Implementation Plan

### Phase 1: Core Infrastructure
- Create `MoreLikeThisQueryBuilder` class
- Add term extraction from indexed documents
- Implement term filtering logic (minTermFreq, minDocFreq, etc.)

### Phase 2: Index Integration
- Add `searchMoreLikeThis()` method to `LSMTreeFullTextIndex`
- Integrate with `FullTextQueryExecutor`
- Add `$similarity` to result pipeline alongside `$score`

### Phase 3: SQL Functions
- Implement `SearchIndexMoreFunction`
- Implement `SearchFieldsMoreFunction`
- Register in `DefaultSQLFunctionFactory`
- Parameter validation and error handling

### Phase 4: Testing & Documentation
- Unit tests for all components
- Integration tests for SQL queries
- Performance tests with large datasets
- Update documentation

---

## File Changes

### New Files
- `engine/src/main/java/com/arcadedb/query/sql/function/text/SearchIndexMoreFunction.java`
- `engine/src/main/java/com/arcadedb/query/sql/function/text/SearchFieldsMoreFunction.java`
- `engine/src/main/java/com/arcadedb/index/lsm/MoreLikeThisQueryBuilder.java`

### Modified Files
- `engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java`
- `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
- `engine/src/main/java/com/arcadedb/index/lsm/FullTextQueryExecutor.java`
- `engine/src/main/java/com/arcadedb/query/sql/executor/ResultInternal.java`
- `engine/src/main/java/com/arcadedb/query/sql/executor/ProjectionCalculationStep.java`

---

## Testing Strategy

### Unit Tests

| Test Class | Coverage |
|------------|----------|
| `MoreLikeThisQueryBuilderTest` | Term extraction, filtering, query building |
| `SearchIndexMoreFunctionTest` | Parameter validation, error cases |
| `SearchFieldsMoreFunctionTest` | Index resolution from field names |

### Integration Tests

| Scenario | Validation |
|----------|------------|
| Basic similarity | Returns documents with shared terms |
| Multi-source RIDs | Combines terms from multiple documents |
| Score ordering | Results sorted by similarity descending |
| `$score` and `$similarity` | Both values populated correctly |
| `excludeSource: true` | Source RIDs not in results |
| `excludeSource: false` | Source RIDs appear (should score highest) |
| Parameter tuning | `minTermFreq`, `maxQueryTerms` affect results |
| Invalid RID | Throws appropriate error |
| Empty results | No matches returns empty set, not error |
| Multi-property index | Field-specific terms extracted correctly |

### Performance Tests
- Large corpus (100k+ documents)
- Many source RIDs (approaching maxSourceDocs)
- Documents with high term count

---

## Prerequisites

This feature depends on completion of the main full-text index improvements:
- Multi-property indexes (for field-prefixed token storage)
- `$score` exposure in result pipeline
- `FullTextQueryExecutor` infrastructure

Should be implemented after the 2026-01-23 design is complete.

---

## References

- [OrientDB SEARCH_MORE Documentation](https://orientdb.dev/docs/3.2.x/indexing/Full-Text-Index.html)
- [Lucene MoreLikeThis](https://lucene.apache.org/core/9_0_0/queries/org/apache/lucene/queries/mlt/MoreLikeThis.html)
- Parent design: `docs/plans/2026-01-23-fulltext-index-improvements-design.md`
