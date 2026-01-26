# More Like This (MLT) - Similar Document Search

## Overview

The More Like This feature enables finding documents similar to one or more source documents based on content analysis. It uses Lucene's term frequency-inverse document frequency (TF-IDF) algorithm to identify shared terms and rank results by similarity.

## Use Cases

- **Content Recommendations** - "Readers who liked this article also enjoyed..."
- **Related Articles** - Find similar blog posts, news articles, or documentation
- **Duplicate Detection** - Identify near-duplicate content
- **Exploratory Search** - Discover content related to items of interest
- **Content Clustering** - Group similar documents together

## SQL Functions

ArcadeDB provides two SQL functions for similarity search:

### SEARCH_INDEX_MORE()

Search by index name with explicit source document RIDs:

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
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3, #10:4])
ORDER BY $similarity DESC
```

### SEARCH_FIELDS_MORE()

Search by field names - automatically finds the appropriate full-text index:

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
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:3])
ORDER BY $similarity DESC
```

## Scoring

Both functions expose two score values:

### $score (Raw Score)

The raw Lucene similarity score (accumulated term matches):

```sql
SELECT title, $score
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $score DESC
```

### $similarity (Normalized Score)

Normalized score from 0.0 to 1.0, where 1.0 is the most similar document:

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $similarity DESC
```

The `$similarity` score is calculated as: `$score / maxScore`, ensuring the highest-scoring result has a similarity of 1.0.

## Configuration Parameters

Customize the similarity algorithm using the optional metadata parameter:

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

### Example with Configuration

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3, #10:4], {
  'minTermFreq': 1,
  'minDocFreq': 3,
  'maxQueryTerms': 50,
  'excludeSource': false
})
ORDER BY $similarity DESC
```

## How It Works

The More Like This algorithm follows these steps:

1. **Extract Terms** - Analyze source documents to extract indexed terms
2. **Filter Terms** - Apply frequency thresholds (minTermFreq, minDocFreq)
3. **Score Terms** - Calculate TF-IDF scores for each term
4. **Select Top Terms** - Choose top N terms (maxQueryTerms) by score
5. **Build Query** - Create an OR query with selected terms
6. **Execute Search** - Run query against the index
7. **Normalize Scores** - Convert raw scores to 0.0-1.0 range

### TF-IDF Scoring

When `boostByScore: true` (default), terms are scored using:

```
score = termFreq * log10(totalDocs / docFreq)
```

Where:
- `termFreq` - How many times the term appears in source documents
- `docFreq` - How many documents in the index contain the term
- `totalDocs` - Total number of indexed documents

Common terms (high docFreq) receive lower scores, while rare distinctive terms receive higher scores.

## Multiple Source Documents

When multiple source RIDs are provided, the algorithm:

1. Extracts terms from ALL source documents
2. Aggregates term frequencies across sources
3. Selects top terms from the combined set
4. Returns documents similar to "any of these sources"

**Example:**
```sql
-- Find articles similar to BOTH Java and Python guides
SELECT title, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:3, #10:5], {
  'minTermFreq': 1,
  'minDocFreq': 1
})
ORDER BY $similarity DESC
```

## Including Source Documents

By default, source documents are excluded from results (`excludeSource: true`). To include them:

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3], {
  'excludeSource': false
})
ORDER BY $similarity DESC
```

When included, source documents typically have the highest similarity (often 1.0) since they match themselves perfectly.

## Tuning Similarity Search

### For Broad Results (More Matches)

Use permissive parameters to find more related content:

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3], {
  'minTermFreq': 1,      -- Accept terms appearing once
  'minDocFreq': 1,       -- Accept rare terms
  'maxQueryTerms': 50    -- Use more terms for matching
})
ORDER BY $similarity DESC
```

### For Precise Results (Fewer, More Similar Matches)

Use strict parameters to find only highly similar content:

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3], {
  'minTermFreq': 3,      -- Require terms appearing 3+ times
  'minDocFreq': 5,       -- Require terms in 5+ documents
  'maxQueryTerms': 10,   -- Use fewer, more distinctive terms
  'minWordLen': 4        -- Ignore short words
})
ORDER BY $similarity DESC
```

### For Common Content

Filter out very common terms using `maxDocFreqPercent`:

```sql
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3], {
  'maxDocFreqPercent': 50  -- Ignore terms in >50% of docs
})
ORDER BY $similarity DESC
```

## Multi-Property Indexes

More Like This works seamlessly with multi-property full-text indexes:

```sql
-- Create multi-property index
CREATE INDEX ON Article (title, body, tags) FULL_TEXT

-- Find similar articles using all three fields
SELECT title, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body', 'tags'], [#10:3])
ORDER BY $similarity DESC
```

Field-specific terms are properly extracted and weighted.

## Error Handling

The functions validate inputs and provide clear error messages:

```sql
-- Empty source RIDs
SELECT FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [])
-- Error: "SEARCH_INDEX_MORE requires at least one source RID"

-- Non-existent RID
SELECT FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#99:999])
-- Error: "Record #99:999 not found"

-- Too many source RIDs
SELECT FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:0, #10:1, ..., #10:30])
-- Error: "Source RIDs (31) exceeds maxSourceDocs limit (25)"

-- Index not found
SELECT FROM Article
WHERE SEARCH_INDEX_MORE('NonExistent', [#10:3])
-- Error: "Index 'NonExistent' not found"
```

## Empty Results

If no similar documents are found (no shared terms after filtering), the query returns an empty result set without error:

```sql
-- Document with no common terms - returns empty set
SELECT FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:999], {
  'minTermFreq': 10,
  'minDocFreq': 10
})
-- Returns: (empty)
```

This is expected behavior, not an error condition.

## Performance Considerations

### Caching

Results are cached per query execution within the same SQL statement. Multiple evaluations of the same More Like This query (e.g., when filtering a large result set) reuse cached results.

### Query Complexity

The `maxQueryTerms` parameter controls search complexity:
- Lower values (10-25): Faster queries, more focused results
- Higher values (50-100): Slower queries, broader results

### Source Document Limit

The `maxSourceDocs` parameter (default 25) prevents performance issues with excessive source documents. For most use cases, 1-5 source documents provide optimal results.

### Index Size

More Like This performance scales with index size. For very large indexes (100k+ documents):
- Consider increasing `minDocFreq` to filter common terms
- Use `maxDocFreqPercent` to exclude very common terms
- Limit results with SQL `LIMIT` clause

## Complete Examples

### Recommendation System

```sql
-- Find articles similar to what the user just read
SELECT title, author, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:42], {
  'minTermFreq': 2,
  'maxQueryTerms': 25,
  'excludeSource': true
})
ORDER BY $similarity DESC
LIMIT 5
```

### Related Content Widget

```sql
-- Show related articles at bottom of current article
SELECT title, excerpt, publishDate, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:100], {
  'minTermFreq': 1,
  'minDocFreq': 2,
  'maxQueryTerms': 30
})
AND publishDate > '2025-01-01'
ORDER BY $similarity DESC
LIMIT 3
```

### Duplicate Detection

```sql
-- Find potential duplicates (very high similarity)
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

### Multi-Source Recommendations

```sql
-- Find articles similar to user's recent reading history
SELECT title, $similarity
FROM Article
WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#10:1, #10:5, #10:12], {
  'minTermFreq': 1,
  'minDocFreq': 3,
  'maxQueryTerms': 40
})
ORDER BY $similarity DESC
LIMIT 10
```

## Integration with Other Features

### Combining with Regular Search

```sql
-- Find similar articles in specific category
SELECT title, category, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
AND category = 'Technology'
ORDER BY $similarity DESC
```

### Joining with Related Data

```sql
-- Find similar articles with author information
SELECT a.title, a.$similarity, u.name AS authorName
FROM Article a
LEFT JOIN User u ON a.authorId = u.@rid
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY a.$similarity DESC
LIMIT 10
```

### Pagination

```sql
-- Page 2 of similar articles (skip 10, take 10)
SELECT title, $similarity
FROM Article
WHERE SEARCH_INDEX_MORE('Article[title,body]', [#10:3])
ORDER BY $similarity DESC
SKIP 10 LIMIT 10
```

## Comparison with SEARCH_INDEX() / SEARCH_FIELDS()

| Feature | SEARCH_INDEX / SEARCH_FIELDS | SEARCH_INDEX_MORE / SEARCH_FIELDS_MORE |
|---------|------------------------------|----------------------------------------|
| **Input** | Query string (keywords) | Source document RIDs |
| **Algorithm** | Boolean keyword matching | TF-IDF similarity analysis |
| **Use Case** | User enters search terms | Find similar content automatically |
| **Scoring** | Term frequency | Normalized similarity (0.0-1.0) |
| **Configuration** | Limited | Extensive (9 parameters) |

Both features can be used together:

```sql
-- First, find articles about "machine learning"
SELECT @rid, title FROM Article
WHERE SEARCH_INDEX('Article[title,body]', 'machine learning')
-- Then use those RIDs with More Like This to find related articles
```

## Best Practices

1. **Start with Defaults** - Default parameters work well for most use cases
2. **Tune Incrementally** - Adjust one parameter at a time to see effects
3. **Use Descriptive Source Docs** - Longer source documents with more terms work better
4. **Limit Result Count** - Use SQL `LIMIT` to prevent returning too many results
5. **Exclude Source by Default** - Usually don't want to recommend "more of the same"
6. **Test with Real Data** - Parameter tuning depends on your content characteristics
7. **Monitor Performance** - Use `EXPLAIN` to understand query execution

## Limitations

- **Source documents must be indexed** - Cannot find similar documents if source isn't in the index
- **Requires meaningful content** - Short documents with few terms produce limited results
- **Language-dependent** - Works best with language-appropriate analyzers
- **No semantic understanding** - Matches on terms, not meaning (synonyms won't match)
- **Synchronous execution** - Blocking query execution (not async)

## See Also

- [Full-Text Search Documentation](FullTextSearch.md)
- [SEARCH_INDEX() Function](FullTextSearch.md#search_index)
- [SEARCH_FIELDS() Function](FullTextSearch.md#search_fields)
- [Full-Text Analyzers](FullTextAnalyzers.md)
- [Lucene MoreLikeThis](https://lucene.apache.org/core/9_0_0/queries/org/apache/lucene/queries/mlt/MoreLikeThis.html)
