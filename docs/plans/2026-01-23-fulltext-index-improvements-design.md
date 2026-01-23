# Full-Text Index Improvements Design

**Date:** 2026-01-23
**Status:** Draft
**Branch:** feat/improve-fulltext

## Overview

Enhance ArcadeDB's full-text index with configurable analyzers, Lucene query syntax support, new search functions, and multi-property indexes. This brings capabilities closer to OrientDB's full-text implementation while maintaining backward compatibility.

## Goals

1. **Analyzer configurability** - Support custom Lucene analyzers per index and per field
2. **Lucene query syntax** - Boolean operators, phrases, wildcards, field-specific queries
3. **Search functions** - `SEARCH_INDEX()` and `SEARCH_FIELDS()` SQL functions
4. **Score exposure** - `$score` projection variable in SQL queries
5. **Multi-property indexes** - Index multiple fields in a single full-text index
6. **Backward compatibility** - Existing `CONTAINSTEXT` operator unchanged

## Non-Goals

- `SEARCH_MORE()` similar-document function (future enhancement)
- `SEARCH_CLASS()` cross-index search (future enhancement)
- Highlighting support (future enhancement)
- Index-time field boosting (future enhancement)

---

## Design

### 1. Index Configuration & Creation

#### Metadata Schema

Full-text indexes accept a `METADATA {...}` JSON block:

```sql
CREATE INDEX ON Article(title, body) FULL_TEXT METADATA {
  "analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer",
  "index_analyzer": "...",      -- optional: separate analyzer for indexing
  "query_analyzer": "...",      -- optional: separate analyzer for queries
  "allowLeadingWildcard": true, -- default: false
  "defaultOperator": "AND"      -- default: "OR"
}
```

Per-field analyzers for multi-property indexes:

```sql
CREATE INDEX ON Article(title, body) FULL_TEXT METADATA {
  "analyzer": "org.apache.lucene.analysis.standard.StandardAnalyzer",
  "title_analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer",
  "body_analyzer": "org.apache.lucene.analysis.standard.StandardAnalyzer"
}
```

#### Default Behavior

| Setting | Default |
|---------|---------|
| analyzer | `org.apache.lucene.analysis.standard.StandardAnalyzer` |
| allowLeadingWildcard | `false` |
| defaultOperator | `OR` |

#### Implementation Components

- `FullTextIndexMetadata` - stores configuration, serializes to/from JSON
- `TypeFullTextIndexBuilder` - builder with `withMetadata(JSONObject)` method
- Extend `CreateIndexStatement.executeDDL()` for FULL_TEXT metadata
- Remove single-property restriction in `LSMTreeFullTextIndex`

---

### 2. Search Functions

#### New SQL Functions

```sql
-- Search by index name
SELECT title, $score FROM Article
WHERE SEARCH_INDEX('Article[title,body]', '+database -nosql')

-- Search by field names (resolves index automatically)
SELECT title, $score FROM Article
WHERE SEARCH_FIELDS(['title', 'body'], 'cloud AND scalable')
```

#### Lucene Query Syntax

| Syntax | Meaning |
|--------|---------|
| `database cloud` | OR search (default) |
| `+database +cloud` | AND (both required) |
| `database -nosql` | database but NOT nosql |
| `"multi model"` | Exact phrase |
| `datab*` | Prefix wildcard |
| `*base` | Leading wildcard (if enabled) |
| `title:database` | Field-specific |
| `database~` | Fuzzy match |
| `database~0.8` | Fuzzy with threshold |

#### Function Behavior

- Returns matching records with score populated
- Results ordered by score descending (highest relevance first)
- `LIMIT` applied after scoring

#### Implementation Components

- `SearchIndexFunction` - SQL function, uses Lucene `QueryParser`
- `SearchFieldsFunction` - SQL function, resolves index from field names
- Query planner integration for efficient index usage

---

### 3. Score Exposure

#### SQL Projection

```sql
-- Score in projection
SELECT title, body, $score FROM Article
WHERE SEARCH_INDEX('Article[title,body]', 'database')

-- Score in ORDER BY
SELECT title, $score FROM Article
WHERE SEARCH_INDEX('Article[title,body]', 'database')
ORDER BY $score DESC

-- Score with alias
SELECT title, $score AS relevance FROM Article
WHERE SEARCH_FIELDS(['title'], 'cloud')
```

#### Score Semantics

- Type: `float` (Lucene's native score type)
- Higher = more relevant
- 0 = no match (excluded)
- `null` or 0 when no search function used

#### Implementation Components

- Extend `ResultInternal` to carry score
- Add `$score` as projection variable in SQL parser
- `SelectExecutionPlanner` propagates score
- `ProjectionCalculationStep` resolves `$score`

#### Java API

```java
ResultSet rs = db.query("sql", "SELECT $score FROM Article WHERE SEARCH_INDEX(...)");
while (rs.hasNext()) {
    Result r = rs.next();
    float score = r.getProperty("$score");
}
```

---

### 4. Lucene Integration Architecture

#### Query Execution Flow

```
User Query: "SEARCH_INDEX('idx', '+database cloud')"
                    |
                    v
         Lucene QueryParser
                    |
                    v
     BooleanQuery(+database, cloud)
                    |
                    v
    LSMTreeFullTextIndex.search(Query)
                    |
                    v
    For each term: lookup in LSM-Tree -> RIDs
                    |
                    v
    Combine results per boolean logic
                    |
                    v
    Score based on term frequency + query structure
                    |
                    v
    Return scored IndexCursor
```

#### Key Classes

| Class | Responsibility |
|-------|----------------|
| `LSMTreeFullTextIndex` | Stores tokens, executes parsed queries |
| `FullTextQueryExecutor` | Translates Lucene Query to LSM lookups |
| `FullTextScorer` | Calculates relevance scores |
| `FullTextIndexMetadata` | Stores analyzer config, parser settings |

#### Analyzer Lifecycle

- Instantiated at index open time (cached)
- Index analyzer: used during `put()` operations
- Query analyzer: used during `search()` operations
- Custom analyzers loaded via reflection

---

### 5. Multi-Property Indexes

#### Token Storage Format

```
Key: "fieldName:token" (e.g., "title:database", "body:scalable")
Value: RID[]
```

Benefits:
- Field-specific queries work: `title:database`
- Combined queries work: `database` (searches all fields)
- Single LSM-Tree file per index

#### Index Naming

- Single property: `Article[title]`
- Multi-property: `Article[title,body]`

---

### 6. Backward Compatibility

| Feature | Behavior |
|---------|----------|
| `CONTAINSTEXT` operator | Unchanged |
| Existing full-text indexes | Work as before with defaults |
| Index without metadata | Uses `StandardAnalyzer`, OR, no leading wildcards |
| `SEARCH_INDEX` on old index | Works with default query analyzer |

#### Migration Path

```sql
-- Recreate with new configuration
DROP INDEX Article[title];
CREATE INDEX ON Article(title) FULL_TEXT METADATA {
  "analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer"
};
```

---

## Implementation Plan

### Phase 1: Infrastructure
1. Create `FullTextIndexMetadata` class
2. Create `TypeFullTextIndexBuilder` with metadata support
3. Extend `CreateIndexStatement` for FULL_TEXT metadata
4. Update `LSMTreeFullTextIndex` to use configurable analyzers

### Phase 2: Multi-Property Support
1. Remove single-property restriction
2. Implement field-prefixed token storage
3. Update indexing logic for multiple fields
4. Add tests for multi-property indexes

### Phase 3: Query Functions
1. Implement `SearchIndexFunction`
2. Implement `SearchFieldsFunction`
3. Integrate Lucene `QueryParser`
4. Create `FullTextQueryExecutor` for query translation

### Phase 4: Score Exposure
1. Add `$score` to SQL parser
2. Propagate scores through execution pipeline
3. Implement score projection resolution
4. Add comprehensive tests

### Phase 5: Testing & Documentation
1. Unit tests for all components
2. Integration tests for SQL queries
3. Performance tests with large datasets
4. Update documentation

---

## File Changes

### New Files
- `engine/src/main/java/com/arcadedb/schema/FullTextIndexMetadata.java`
- `engine/src/main/java/com/arcadedb/schema/TypeFullTextIndexBuilder.java`
- `engine/src/main/java/com/arcadedb/index/lsm/FullTextQueryExecutor.java`
- `engine/src/main/java/com/arcadedb/index/lsm/FullTextScorer.java`
- `engine/src/main/java/com/arcadedb/query/sql/function/text/SearchIndexFunction.java`
- `engine/src/main/java/com/arcadedb/query/sql/function/text/SearchFieldsFunction.java`

### Modified Files
- `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
- `engine/src/main/java/com/arcadedb/query/sql/parser/CreateIndexStatement.java`
- `engine/src/main/java/com/arcadedb/query/sql/parser/SqlParser.jjt` (for $score)
- `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java`
- `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`

---

## Open Questions

1. Should we support index-time vs query-time synonyms?
2. Should stop words be configurable per-index or use analyzer defaults?
3. Should we add a `REBUILD INDEX` command to re-index with new analyzer?

---

## References

- [OrientDB Full-Text Index Documentation](https://orientdb.dev/docs/3.2.x/indexing/Full-Text-Index.html)
- [Lucene QueryParser Syntax](https://lucene.apache.org/core/9_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html)
- Current implementation: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
