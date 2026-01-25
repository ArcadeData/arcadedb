# Full-Text Search

## Overview

ArcadeDB provides comprehensive full-text search capabilities using Apache Lucene. The system supports both keyword-based search and similarity-based document discovery through four SQL functions:

- **SEARCH_INDEX()** - Keyword search by index name
- **SEARCH_FIELDS()** - Keyword search by field names
- **SEARCH_INDEX_MORE()** - Similarity search by index name
- **SEARCH_FIELDS_MORE()** - Similarity search by field names

## Creating Full-Text Indexes

### Single-Field Index

```sql
CREATE INDEX ON Article (content) FULL_TEXT
```

### Multi-Field Index

Index multiple fields in a single index for cross-field search:

```sql
CREATE INDEX ON Article (title, body, tags) FULL_TEXT
```

### With Custom Analyzer

```sql
CREATE INDEX ON Article (content) FULL_TEXT
METADATA {
  "analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer"
}
```

## Keyword Search Functions

### SEARCH_INDEX()

Search a full-text index using Lucene query syntax:

```sql
SEARCH_INDEX(indexName, queryString)
```

**Parameters:**
- `indexName` - Name of the full-text index (e.g., `'Article[content]'` or `'Article[title,body]'`)
- `queryString` - Lucene query string with keywords and operators

**Basic Example:**
```sql
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'database programming')
ORDER BY $score DESC
```

**Query Syntax:**

```sql
-- Boolean operators
WHERE SEARCH_INDEX('Article[content]', '+java +programming')  -- Both required (AND)
WHERE SEARCH_INDEX('Article[content]', 'java OR python')      -- Either term (OR)
WHERE SEARCH_INDEX('Article[content]', 'java -deprecated')    -- Include java, exclude deprecated

-- Phrase queries
WHERE SEARCH_INDEX('Article[content]', '"machine learning"')  -- Exact phrase

-- Wildcards
WHERE SEARCH_INDEX('Article[content]', 'java*')               -- Starts with "java"
WHERE SEARCH_INDEX('Article[content]', 'te?t')                -- Single character wildcard

-- Field-specific search (multi-field indexes)
WHERE SEARCH_INDEX('Article[title,body]', 'title:java')       -- Search only in title
WHERE SEARCH_INDEX('Article[title,body]', 'title:java body:programming')  -- Mixed

-- Complex queries
WHERE SEARCH_INDEX('Article[content]', '(java OR python) AND programming -deprecated')
```

**Supported Operators:**
- `+` - Term must be present (required)
- `-` - Term must not be present (excluded)
- `OR` - Either term matches
- `AND` - Both terms must match (or use `+` prefix)
- `"..."` - Exact phrase match
- `*` - Multi-character wildcard
- `?` - Single character wildcard
- `field:term` - Field-specific search

### SEARCH_FIELDS()

Auto-discover the appropriate index by field names:

```sql
SEARCH_FIELDS(fieldNames, queryString)
```

**Parameters:**
- `fieldNames` - Array of field names (e.g., `['title', 'body']`)
- `queryString` - Lucene query string

**Example:**
```sql
SELECT title, $score
FROM Article
WHERE SEARCH_FIELDS(['title', 'body'], 'database programming')
ORDER BY $score DESC
```

**Benefits:**
- No need to know the exact index name
- Automatically finds the full-text index covering those fields
- More flexible when indexes are renamed

**Comparison:**

```sql
-- These are equivalent:
WHERE SEARCH_INDEX('Article[title,body]', 'java programming')
WHERE SEARCH_FIELDS(['title', 'body'], 'java programming')
```

### Keyword Search Scoring ($score)

Both SEARCH_INDEX() and SEARCH_FIELDS() expose a `$score` variable:

```sql
SELECT title, body, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'java programming')
ORDER BY $score DESC
```

**Score calculation:**
- Each matching term increments the score by 1
- Query: `"java programming"` → document with both terms scores 2
- Query: `"java OR python"` → document with "java" scores 1, with both scores 2
- Higher scores indicate more relevant matches

**Using scores:**

```sql
-- Top 10 most relevant articles
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'database optimization')
ORDER BY $score DESC
LIMIT 10

-- Only highly relevant results
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'java programming')
AND $score >= 2
ORDER BY $score DESC

-- Score in projections
SELECT title, $score AS relevance
FROM Article
WHERE SEARCH_FIELDS(['title', 'body'], 'machine learning')
ORDER BY relevance DESC
```

## Similarity Search Functions (More Like This)

### SEARCH_INDEX_MORE()

Find documents similar to source documents using TF-IDF analysis:

```sql
SEARCH_INDEX_MORE(indexName, sourceRIDs [, metadata])
```

**Parameters:**
- `indexName` - Name of the full-text index (e.g., `'Article[title,body]'`)
- `sourceRIDs` - Array of source document RIDs (e.g., `[#10:3, #10:4]`)
- `metadata` - Optional JSON object with configuration parameters

**Example:**
```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $similarity DESC
```

**Use Cases:**
- Content recommendations ("related articles")
- "Readers who liked this also enjoyed..."
- Duplicate detection
- Exploratory search

### SEARCH_FIELDS_MORE()

Similarity search auto-discovering index by field names:

```sql
SEARCH_FIELDS_MORE(fieldNames, sourceRIDs [, metadata])
```

**Parameters:**
- `fieldNames` - Array of field names (e.g., `['title', 'body']`)
- `sourceRIDs` - Array of source document RIDs (e.g., `[#10:3]`)
- `metadata` - Optional JSON object with configuration parameters

**Example:**
```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:3, #10:5])
ORDER BY $similarity DESC
```

**Comparison:**

```sql
-- These are equivalent:
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:3])
```

### Similarity Scoring ($score and $similarity)

More Like This functions expose two score variables:

#### $score (Raw Score)

The raw Lucene term-match score (accumulated):

```sql
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $score DESC
```

#### $similarity (Normalized Score)

Normalized score from 0.0 to 1.0, where 1.0 is the most similar:

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $similarity DESC
```

**Normalization:** `$similarity = $score / maxScore`

The highest-scoring result always has `$similarity = 1.0`, making it easy to set thresholds:

```sql
-- Only highly similar documents
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
AND $similarity > 0.7
ORDER BY $similarity DESC
```

### Configuration Parameters (More Like This)

Customize similarity search with optional metadata:

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
| `maxSourceDocs` | int | 25 | Maximum source RIDs allowed |

**Example with configuration:**

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3], {
  'minTermFreq': 1,
  'minDocFreq': 3,
  'maxQueryTerms': 50,
  'excludeSource': false
})
ORDER BY $similarity DESC
```

### How More Like This Works

1. **Extract Terms** - Analyze source documents to extract indexed terms
2. **Filter Terms** - Apply frequency thresholds (minTermFreq, minDocFreq)
3. **Score Terms** - Calculate TF-IDF scores for each term:
   ```
   score = termFreq * log10(totalDocs / docFreq)
   ```
4. **Select Top Terms** - Choose top N terms (maxQueryTerms) by score
5. **Build Query** - Create an OR query with selected terms
6. **Execute Search** - Run query against the index
7. **Normalize Scores** - Convert to 0.0-1.0 range

**TF-IDF Insight:** Common terms (high docFreq) receive lower scores, while rare distinctive terms receive higher scores.

## Complete Examples

### Basic Keyword Search

```sql
-- Simple keyword search
SELECT title, publishDate, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'database')
ORDER BY $score DESC
LIMIT 10

-- Multi-term search
SELECT title, $score
FROM Article
WHERE SEARCH_FIELDS(['title', 'body'], 'java programming')
ORDER BY $score DESC

-- Required terms
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', '+database +optimization')
ORDER BY $score DESC

-- Exclude terms
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'java -deprecated')
ORDER BY $score DESC
```

### Advanced Queries

```sql
-- Phrase search
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', '"machine learning"')
ORDER BY $score DESC

-- Wildcard search
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'prog*')
ORDER BY $score DESC

-- Field-specific search
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[title,body]', 'title:"Getting Started" body:tutorial')
ORDER BY $score DESC

-- Complex boolean query
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', '(java OR python) AND (database OR programming) -deprecated')
ORDER BY $score DESC
```

### Content Recommendations

```sql
-- Find similar articles
SELECT title, author, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:42])
ORDER BY $similarity DESC
LIMIT 5

-- Related content widget
SELECT title, excerpt, publishDate, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:100], {
  'minTermFreq': 1,
  'minDocFreq': 2,
  'maxQueryTerms': 30
})
AND publishDate > '2025-01-01'
ORDER BY $similarity DESC
LIMIT 3

-- Multi-source recommendations (user's reading history)
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:1, #10:5, #10:12], {
  'minTermFreq': 1,
  'minDocFreq': 3,
  'maxQueryTerms': 40
})
ORDER BY $similarity DESC
LIMIT 10
```

### Duplicate Detection

```sql
-- Find near-duplicates
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:50], {
  'minTermFreq': 1,
  'minDocFreq': 1,
  'maxQueryTerms': 100,
  'excludeSource': true
})
AND $similarity > 0.8
ORDER BY $similarity DESC
```

### Combined Search and Similarity

```sql
-- First, find articles about a topic
SELECT @rid, title FROM Article
WHERE SEARCH_INDEX('Article[content]', 'machine learning')
LIMIT 1

-- Then find similar articles (use the RID from above)
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[content]', [#10:3])
ORDER BY $similarity DESC
LIMIT 10
```

### With Filters and Joins

```sql
-- Search with category filter
SELECT title, category, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'database')
AND category = 'Technology'
ORDER BY $score DESC

-- Similarity search with filter
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
AND publishDate > '2025-01-01'
AND status = 'published'
ORDER BY $similarity DESC

-- With joins
SELECT a.title, a.$score, u.name AS authorName
FROM Article a
LEFT JOIN User u ON a.authorId = u.@rid
WHERE SEARCH_FIELDS(['title', 'body'], 'java programming')
ORDER BY a.$score DESC
LIMIT 10
```

### Pagination

```sql
-- Page 1 (items 1-10)
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'database')
ORDER BY $score DESC
LIMIT 10

-- Page 2 (items 11-20)
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[content]', 'database')
ORDER BY $score DESC
SKIP 10 LIMIT 10

-- Similarity search pagination
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $similarity DESC
SKIP 20 LIMIT 10
```

## Comparison: Keyword vs Similarity Search

| Feature | SEARCH_INDEX / SEARCH_FIELDS | SEARCH_INDEX_MORE / SEARCH_FIELDS_MORE |
|---------|------------------------------|----------------------------------------|
| **Input** | Query string (keywords) | Source document RIDs |
| **Algorithm** | Boolean keyword matching | TF-IDF similarity analysis |
| **Use Case** | User enters search terms | Find similar content automatically |
| **Scoring** | Term frequency ($score) | Normalized similarity ($similarity 0.0-1.0) |
| **Configuration** | Query syntax only | 9 configurable parameters |
| **Performance** | Fast, simple matching | More complex, analyzes term distribution |
| **Best For** | Explicit search queries | Content recommendations, duplicates |

## Multi-Property Indexes

All four functions work with multi-property indexes:

```sql
-- Create multi-property index
CREATE INDEX ON Article (title, body, tags) FULL_TEXT

-- Keyword search (searches all fields)
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[title,body,tags]', 'database')
ORDER BY $score DESC

-- Field-specific keyword search
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX('Article[title,body,tags]', 'title:tutorial tags:beginner')
ORDER BY $score DESC

-- Similarity search (uses all fields)
SELECT title, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body', 'tags'], [#10:3])
ORDER BY $similarity DESC
```

## Error Handling

All functions validate inputs and provide clear error messages:

```sql
-- Index not found
WHERE SEARCH_INDEX('NonExistent', 'query')
-- Error: "Index 'NonExistent' not found"

-- Invalid query syntax
WHERE SEARCH_INDEX('Article[content]', 'AND OR')
-- Error: "Invalid search query: AND OR"

-- Empty source RIDs (More Like This)
WHERE SEARCH_INDEX_MORE('Article[content]', [])
-- Error: "SEARCH_INDEX_MORE requires at least one source RID"

-- Non-existent RID (More Like This)
WHERE SEARCH_INDEX_MORE('Article[content]', [#99:999])
-- Error: "Record #99:999 not found"

-- Too many source RIDs (More Like This)
WHERE SEARCH_INDEX_MORE('Article[content]', [#10:0, ..., #10:30])
-- Error: "Source RIDs (31) exceeds maxSourceDocs limit (25)"
```

**Empty Results:** Both keyword and similarity searches return empty result sets (not errors) when no matches are found.

## Performance Considerations

### Caching

Results are cached per query execution within the same SQL statement. If the WHERE clause is evaluated multiple times (e.g., large result set), the search is only executed once.

### Indexing Strategy

- **Single-field indexes** - Use for dedicated search fields (e.g., content-only)
- **Multi-field indexes** - Use for cross-field search (title + body)
- **Separate indexes** - Create multiple indexes for different search patterns

```sql
-- Dedicated indexes for different use cases
CREATE INDEX ON Article (content) FULL_TEXT      -- Full content search
CREATE INDEX ON Article (title) FULL_TEXT        -- Title-only search
CREATE INDEX ON Article (title, body) FULL_TEXT  -- Cross-field search
```

### Query Optimization

**Keyword Search:**
- Use `+` prefix for required terms to narrow results early
- Avoid leading wildcards (`*java`) - they're slow
- Use field-specific searches to reduce scope
- Combine with non-full-text filters (category, date, etc.)

**Similarity Search:**
- Start with 1-3 source documents (more isn't always better)
- Use `minDocFreq` to filter common terms in large indexes
- Lower `maxQueryTerms` for faster queries
- Use `LIMIT` clause to restrict result count

### Index Size Impact

For very large indexes (100k+ documents):

**Keyword Search:**
- Performance remains consistent
- Query complexity is the main factor

**Similarity Search:**
- Increase `minDocFreq` to filter common terms
- Use `maxDocFreqPercent` to exclude very common terms
- Consider lowering `maxQueryTerms` for faster execution
- Monitor with `EXPLAIN` to understand performance

## Best Practices

### Keyword Search

1. **Use specific terms** - More specific queries return better results
2. **Combine operators** - Mix `+`, `-`, `OR` for precise results
3. **Use phrase queries** - `"exact phrase"` for multi-word concepts
4. **Field-specific when possible** - `title:term` is faster than searching all fields
5. **Filter early** - Combine with category, date, or other filters

### Similarity Search

1. **Start with defaults** - Default parameters work well for most cases
2. **Tune incrementally** - Adjust one parameter at a time
3. **Use descriptive sources** - Longer documents with more terms work better
4. **Exclude source by default** - Usually don't want "more of the same"
5. **Set similarity thresholds** - `WHERE $similarity > 0.6` filters weak matches
6. **Test with real data** - Parameter tuning depends on your content
7. **Monitor performance** - Use `EXPLAIN` for query planning

### General

1. **Index appropriate fields** - Only index searchable text fields
2. **Use appropriate analyzers** - Language-specific analyzers improve results
3. **Test queries** - Verify results match expectations
4. **Monitor index size** - Large indexes may need tuning
5. **Document search patterns** - Help future developers understand design

## Limitations

### Keyword Search

- **No stemming by default** - "run" doesn't match "running" (use EnglishAnalyzer)
- **No synonym matching** - "car" doesn't match "automobile"
- **Case-sensitive based on analyzer** - StandardAnalyzer lowercases, others may not
- **Phrase order matters** - `"java programming"` != `"programming java"`

### Similarity Search

- **Source must be indexed** - Cannot find similar docs if source isn't in index
- **Requires meaningful content** - Short docs with few terms produce limited results
- **Language-dependent** - Works best with language-appropriate analyzers
- **No semantic understanding** - Matches terms, not meaning (synonyms won't match)
- **Synchronous execution** - Blocking query execution (not async)

### Both

- **Synchronous** - Queries block until complete
- **No fuzzy matching** - Typos won't match (unless using FuzzyQuery explicitly)
- **Single language per index** - Mix languages carefully

## See Also

- [Full-Text Analyzers](FullTextAnalyzers.md)
- [Index Configuration](Indexes.md)
- [Lucene Query Syntax](https://lucene.apache.org/core/9_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html)
- [Lucene MoreLikeThis](https://lucene.apache.org/core/9_0_0/queries/org/apache/lucene/queries/mlt/MoreLikeThis.html)
- [SQL Functions Reference](SQLFunctions.md)
