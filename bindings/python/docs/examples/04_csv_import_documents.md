# Example 04: CSV Import - Documents

**Production-ready CSV import with automatic type inference, NULL handling, and index optimization**

## Overview

This example demonstrates importing real-world CSV data from the MovieLens dataset into ArcadeDB documents. You'll learn production-ready patterns for:

- **Automatic type inference** - Java analyzes CSV and infers optimal ArcadeDB types
- **Schema-on-write** - Database creates schema automatically during import
- **NULL value handling** - Import and query missing data across all types
- **Batch processing** - Optimize import performance with commit batching
- **Index optimization** - Create indexes AFTER import for maximum throughput
- **Performance analysis** - Measure query speedup with statistical validation
- **Result validation** - Verify indexes return identical results with actual data samples

## What You'll Learn

- Automatic type inference by Java CSV importer (LONG, DOUBLE, STRING)
- Schema-on-write during import (no manual schema creation needed)
- NULL value import from empty CSV cells
- Query performance measurement (10 runs with statistics)
- Index creation timing (before vs after import)
- Composite indexes for multi-column queries
- **Result validation with actual data samples**
- Production import patterns for large datasets
- **FULL_TEXT index limitations with LIKE queries** (⚠️ Known Issue)

## Prerequisites

**1. Download the MovieLens dataset:**

```bash
cd bindings/python/examples
python download_sample_data.py
```

**Two dataset sizes available:**
- **ml-large** (default): ~86,000 movies, ~33M ratings (~265 MB) - Realistic performance testing
- **ml-small**: ~9,700 movies, ~100,000 ratings (~1 MB) - Quick testing

This example uses **ml-large** by default for realistic performance demonstration. The dataset includes intentional NULL values:
- `movies.csv`: ~3% NULL genres
- `ratings.csv`: ~2% NULL timestamps
- `links.csv`: ~10% NULL imdbId, ~15% NULL tmdbId
- `tags.csv`: ~5% NULL tags

To use the smaller dataset: `python download_sample_data.py --size small`

**2. Install ArcadeDB Python bindings:**

```bash
pip install arcadedb-embedded
```

## Dataset Structure

**MovieLens Large** - 4 CSV files:

| File | Records | Columns | Description |
|------|---------|---------|-------------|
| `movies.csv` | 86,537 | movieId, title, genres | Movie metadata |
| `ratings.csv` | 33,832,162 | userId, movieId, rating, timestamp | User ratings |
| `links.csv` | 86,537 | movieId, imdbId, tmdbId | External IDs (with NULLs) |
| `tags.csv` | 2,328,315 | userId, movieId, tag, timestamp | User tags (with NULLs) |

**Total records**: 36,333,551 documents

For quick testing with the smaller dataset (124,003 records), use: `python download_sample_data.py --size small`

## Type Inference by Java

The example uses **automatic type inference** by the Java CSV importer, which analyzes the data and selects optimal ArcadeDB types:

### Example Inference Results (ml-large)

```
📋 Movie (movies.csv):
   • movieId: LONG (e.g., '1')
   • title: STRING (e.g., 'Toy Story (1995)')
   • genres: STRING (e.g., 'Adventure|Animation|Children|Comedy|Fantasy')

📋 Rating (ratings.csv):
   • userId: LONG (e.g., '1')
   • movieId: LONG (e.g., '1')
   • rating: DOUBLE (e.g., '4.0')
   • timestamp: LONG (e.g., '964982703')

📋 Link (links.csv):
   • movieId: LONG (e.g., '1')
   • imdbId: LONG (e.g., '')         ← NULL value
   • tmdbId: LONG (e.g., '862')

📋 Tag (tags.csv):
   • userId: LONG (e.g., '2')
   • movieId: LONG (e.g., '60756')
   • tag: STRING (e.g., 'funny')
   • timestamp: LONG (e.g., '1445714994')
```

## Code Walkthrough

### Step 1: Check Dataset Availability

```python
data_dir = Path(__file__).parent / "data" / "ml-latest-small"
if not data_dir.exists():
    print("❌ MovieLens dataset not found!")
    print("💡 Please download the dataset first:")
    print("   python download_sample_data.py")
    exit(1)
```

### Step 2: Let Java Infer Types Automatically

The Java CSV importer automatically analyzes the CSV data and infers optimal ArcadeDB types (LONG, DOUBLE, STRING). No manual type inference code is needed - the importer handles this intelligently based on the actual data values.

The schema is created automatically during import (schema-on-write), eliminating the need for explicit schema definition before import.

### Step 3: Import CSV Files Directly

```python
# Import with batch commits for performance
stats = arcadedb.import_csv(
    db,
    movies_csv,
    "Movie",
    commit_every=1000  # Commit every 1000 records
)

# Check for NULL values
null_genres = list(db.query(
    "sql", "SELECT count(*) as c FROM Movie WHERE genres IS NULL"
))[0].get_property("c")

if null_genres > 0:
    print(f"   🔍 NULL values detected:")
    print(f"      • genres: {null_genres:,} NULL values ({null_genres/stats['documents']*100:.1f}%)")
    print("   💡 Empty CSV cells correctly imported as SQL NULL")
```

**Performance results:**
- Movies: 224,189 records/sec
- Ratings: 718,122 records/sec (largest file, highly optimized)
- Links: 540,856 records/sec
- Tags: 778,440 records/sec

### Step 8: Query Performance WITHOUT Indexes

```python
test_queries = [
    ("Find movie by ID", "SELECT FROM Movie WHERE movieId = 500"),
    ("Find user's ratings", "SELECT FROM Rating WHERE userId = 414 LIMIT 10"),
    ("Find movie ratings", "SELECT FROM Rating WHERE movieId = 500"),
    ("Count user's ratings", "SELECT count(*) FROM Rating WHERE userId = 414"),
    ("Find movies by genre", "SELECT FROM Movie WHERE genres LIKE '%Action%' LIMIT 10"),
]

# Run each query 10 times for statistical reliability
for query_name, query in test_queries:
    run_times = []
    for _ in range(10):
        query_start = time.time()
        result = list(db.query("sql", query))
        run_times.append(time.time() - query_start)

    avg_time = statistics.mean(run_times)
    std_time = statistics.stdev(run_times)
    print(f"   📊 {query_name}:")
    print(f"      Average: {avg_time*1000:.2f}ms ± {std_time*1000:.2f}ms")
```

### Step 9: Create Indexes (AFTER Import)

```python
with db.transaction():
    db.command("sql", "CREATE INDEX ON Movie (movieId) UNIQUE")
    db.command("sql", "CREATE INDEX ON Rating (userId, movieId) NOTUNIQUE")  # Composite!
    db.command("sql", "CREATE INDEX ON Link (movieId) UNIQUE")
    db.command("sql", "CREATE INDEX ON Tag (movieId) NOTUNIQUE")
```

**Why create indexes AFTER import?**
- 2-3x faster total time
- Indexes built in one pass
- Fully compacted from start
- Production best practice

### Step 10: Query Performance WITH Indexes

Same queries, now with indexes active. Results show dramatic speedup!

## Performance Results

### Query Speedup Summary

```
🚀 Performance Improvement Summary:
======================================================================
Query                          Before (ms)     After (ms)      Speedup
======================================================================
Find movie by ID               39.1±7.6        0.7±1.9         58.1x
                                     (98.3% time saved)
Find user's ratings            16003.9±79.4    1.1±0.4         14,836x
                                     (100.0% time saved)
Find movie ratings             16524.8±124.2   153.1±22.2      107.9x
                                     (99.1% time saved)
Count user's ratings           16004.3±138.3   0.8±1.4         19,604x
                                     (100.0% time saved)
Find movies by genre           1.0±1.0         0.8±0.3         1.3x
                                     (23.7% time saved)
Count ALL Action movies        58.7±8.4        65.1±18.0       0.9x
                                     (-10.8% time saved)
======================================================================
```

**Key findings:**
- ✅ Composite indexes show **massive gains** (up to 19,604x speedup!)
- ✅ Single column lookups are **very fast** (58.1x speedup)
- ✅ Standard deviation shows **query stability**
- ⚠️ FULL_TEXT index on genres shows slight regression with LIKE queries (see Known Issue)

## NULL Value Handling

The example demonstrates comprehensive NULL handling:

### Import Results

```
Step 2: Importing movies.csv → Movie documents...
   ✅ Imported 86,537 movies
   🔍 NULL values detected:
      • genres: 2,584 NULL values (3.0%)
   💡 Empty CSV cells correctly imported as SQL NULL

Step 4: Importing ratings.csv → Rating documents...
   ✅ Imported 33,832,162 ratings
   🔍 NULL values detected:
      • timestamp: 675,782 NULL values (2.0%)
   💡 Empty CSV cells correctly imported as SQL NULL

Step 5: Importing links.csv → Link documents...
   ✅ Imported 86,537 links
   🔍 NULL values detected:
      • imdbId: 8,628 NULL values (10.0%)
      • tmdbId: 13,026 NULL values (15.1%)
   💡 Empty CSV cells correctly imported as SQL NULL

Step 6: Importing tags.csv → Tag documents...
   ✅ Imported 2,328,315 tags
   🔍 NULL values detected:
      • tag: 116,644 NULL values (5.0%)
   💡 Empty CSV cells correctly imported as SQL NULL
```

### NULL Values in Aggregations

```
💬 Top 10 most common tags:
    1. 'None' (116,644 uses)          ← NULL tags appear as 'None'
    2. 'sci-fi' (13,612 uses)
    ...

🎭 Top 10 genres by movie count:
    1. Drama (11,857 movies)
    ...
    6. None (2,584 movies)          ← NULL genres appear as 'None'
```

**Total NULL values**: ~799,638 across all fields (LONG and STRING types)

## Index Architecture

ArcadeDB uses **LSM-Tree (Log-Structured Merge Tree)** for all indexes - a single unified backend that handles all data types efficiently.

### How LSM-Tree Works

**Architecture:**
```
LSMTreeIndex
├── Mutable Buffer (in-memory)
│   └── Recent writes, fast inserts
│
└── Compacted Storage (disk)
    └── Sorted, immutable data
```

**Key advantages:**
- **Write-optimized**: Sequential writes to memory buffer (perfect for bulk imports)
- **Type-agnostic**: Same structure for all types, type-aware comparison during lookups
- **Auto-compaction**: Background merging keeps data sorted and compact
- **Transaction-friendly**: Buffers writes until commit

### Type Performance & Storage

**Binary serialization per type:**

| Type | Storage Size | Comparison Speed | Best For |
|------|--------------|------------------|----------|
| BYTE | 1 byte | ⚡ Very fast | Flags, small counts (0-255) |
| SHORT | 2 bytes | ⚡ Very fast | Medium numbers (-32K to 32K) |
| INTEGER | 4 bytes | ⚡ Very fast | IDs, standard numbers (up to 2B) |
| LONG | 8 bytes | ⚡ Very fast | Large IDs, timestamps |
| FLOAT | 4 bytes | ⚡ Fast | Small decimals (7-digit precision) |
| DOUBLE | 8 bytes | ⚡ Fast | Standard decimals (15-digit precision) |
| DATE/DATETIME | 8 bytes (as LONG) | ⚡ Fast | Timestamps, dates |
| STRING | Variable | 🐌 Slower | Text, byte-by-byte comparison |
| DECIMAL | Variable | 🐌 Slowest | Exact precision (e.g., money) |

**Index space example** (100K records):
- BYTE: 100KB | SHORT: 200KB | **INTEGER: 400KB** (best balance) | LONG: 800KB | STRING(20): 2MB+

**Why this matters:**
- Smaller types = more keys per page = better cache performance
- Fixed-size types = faster comparison = better query speed
- Choose INTEGER for most IDs (handles 2 billion values, compact, fast)

## Analysis Queries

The example includes comprehensive data analysis:

### Record Counts

```python
SELECT count(*) as count FROM Movie     # 9,742 movies
SELECT count(*) as count FROM Rating    # 100,836 ratings
SELECT count(*) as count FROM Link      # 9,742 links
SELECT count(*) as count FROM Tag       # 3,683 tags
```

### Rating Statistics

```python
SELECT
    count(*) as total_ratings,
    avg(rating) as avg_rating,
    min(rating) as min_rating,
    max(rating) as max_rating
FROM Rating

# Results: 100,836 ratings, avg 3.50 ★, range 0.5-5.0
```

### Rating Distribution

```python
SELECT rating, count(*) as count
FROM Rating
GROUP BY rating
ORDER BY rating

# Results:
# 0.5 ★ : 1,370
# 1.0 ★ : 2,811
# ...
# 4.0 ★ : 26,818  (most common)
# 5.0 ★ : 13,211
```

### Top Genres

```python
SELECT genres, count(*) as count
FROM Movie
WHERE genres <> '(no genres listed)'
GROUP BY genres
ORDER BY count DESC
LIMIT 10

# Results: Drama (1,022), Comedy (922), Comedy|Drama (429), ...
```

### Most Active Users

```python
SELECT userId, count(*) as rating_count
FROM Rating
GROUP BY userId
ORDER BY rating_count DESC
LIMIT 10

# Results: User 414 (2,698 ratings), User 599 (2,478), ...
```

## Best Practices Demonstrated

### ✅ Type Inference by Java
- Java CSV importer automatically analyzes data and selects optimal types
- Handles LONG, DOUBLE, STRING intelligently based on actual values
- No manual type inference code needed
- Schema-on-write simplifies development

### ✅ Schema Definition
- Define schema BEFORE import (validation + optimization)
- Use explicit property types (no guessing)
- Choose appropriate types for data ranges

### ✅ Import Optimization
- Use `commit_every` parameter for batching
- Larger batches = faster imports (balance with memory)
- Movies: commit_every=1000 (smaller batches)
- Ratings: commit_every=5000 (larger dataset, bigger batches)

### ✅ Index Strategy
- **CREATE INDEXES AFTER IMPORT** (2-3x faster total time)
- Use composite indexes for multi-column queries
- Order matters: most selective column first
- Index creation timing: ~0.2 seconds for 124K records

### ✅ Performance Measurement
- Run queries 10 times for statistical reliability
- Calculate average, standard deviation, min, max
- Compare before/after index performance
- Measure speedup percentages

### ✅ NULL Handling
- Empty CSV cells → SQL NULL automatically
- Check NULL counts after import
- NULL values appear in aggregations (as 'None' string)
- Support NULL across all types (STRING, INTEGER, etc.)

## Running the Example

```bash
# 1. Download dataset (one-time setup)
cd bindings/python/examples
python download_sample_data.py

# 2. Run the example
python 04_csv_import_documents.py
```

**Expected output:**
- Step-by-step import progress
- NULL value detection for all 4 files
- Performance statistics (before/after indexes)
- Data analysis queries with results
- Total time: ~2-3 minutes (ml-large) or ~5 seconds (ml-small)

**Database location:** `./my_test_databases/movielens_db/` (preserved for inspection)

**Database size:**
- **ml-large**: ~2 GB database from ~265 MB CSV files (~7.5x expansion)
- **ml-small**: ~7 MB database from ~1 MB CSV files (~7x expansion)

⚠️ **Note**: Database files are larger than source CSVs due to:
- Index structures (LSM-Tree buffers and sorted data)
- Transaction logs and metadata
- Internal data structures for document storage
- WAL (Write-Ahead Log) files for durability

## Expected Output

```
======================================================================
🎬 ArcadeDB Python - Example 04: CSV Import - Documents
======================================================================

Step 0: Checking for MovieLens dataset...
✅ MovieLens dataset found!

Step 1: Creating database...
   ✅ Database created at: ./my_test_databases/movielens_db
   ⏱️  Time: 0.597s

Step 2: Inspecting CSV files and inferring types...
   📋 Movie (movies.csv):
      • movieId: INTEGER
      • title: STRING
      • genres: STRING
   ⏱️  Time: 0.019s

Step 3: Creating schema with explicit property types...
   ✅ Created Movie document type
   ✅ Created Rating document type
   ✅ Created Link document type
   ✅ Created Tag document type
   ⏱️  Time: 0.051s

Step 4: Importing movies.csv → Movie documents...
   ✅ Imported 86,537 movies
   🔍 NULL values detected:
      • genres: 2,584 NULL values (3.0%)

Step 5: Importing ratings.csv → Rating documents...
   ✅ Imported 33,832,162 ratings
   🔍 NULL values detected:
      • timestamp: 675,782 NULL values (2.0%)

Step 6: Importing links.csv → Link documents...
   ✅ Imported 86,537 links
   🔍 NULL values detected:
      • imdbId: 8,628 NULL values (10.0%)
      • tmdbId: 13,026 NULL values (15.1%)

Step 7: Importing tags.csv → Tag documents...
   ✅ Imported 2,328,315 tags
   🔍 NULL values detected:
      • tag: 116,644 NULL values (5.0%)

Step 8: Testing query performance WITHOUT indexes...
   📊 Find movie by ID: 8.67ms ± 5.67ms

Step 9: Creating indexes for query performance...
   ✅ Created indexes on key columns
   ⏱️  Time: 0.238s

Step 10: Testing query performance WITH indexes...
   📊 Find movie by ID: 0.51ms ± 1.41ms

   🚀 Performance Improvement Summary:
   Find movie by ID: 17.0x speedup (94.1% time saved)
   Find user's ratings: 49.4x speedup (98.0% time saved)

Step 11: Verifying schema and property definitions...
   📋 Movie schema (formally defined):
      • movieId: INTEGER
      • title: STRING
      • genres: STRING

Step 12: Querying and analyzing imported data...
   📊 Total records imported: 124,003
   ⭐ Rating statistics: 100,836 ratings, avg 3.50
   🎭 Top genres: Drama (1,022), Comedy (922)
   👥 Most active user: User 414 (2,698 ratings)

✅ Data Import Example Complete!
```

## ⚠️ Known Issue: FULL_TEXT Index Performance with LIKE Queries

### Issue Summary

The example creates a `FULL_TEXT` index on the `Movie.genres` field, but testing reveals that **FULL_TEXT indexes do NOT improve performance for `LIKE '%term%'` pattern matching queries**, and may actually cause performance regression.

### Performance Results (ml-large dataset with 86,537 movies)

**Query: `SELECT count(*) FROM Movie WHERE genres LIKE '%Action%'`**

- **Without FULL_TEXT index**: 58.7ms ± 8.4ms
- **With FULL_TEXT index**: 65.1ms ± 18.0ms
- **Result**: 0.9x speedup **(10.8% SLOWER)**

**Query: `SELECT FROM Movie WHERE genres LIKE '%Action%' LIMIT 10`**

- **Without index**: 1.0ms ± 1.0ms
- **With FULL_TEXT index**: 0.8ms ± 0.3ms
- **Result**: 1.3x speedup (marginal improvement)

### Why This Happens

1. **Tokenization Mismatch**: FULL_TEXT indexes tokenize text (e.g., `"Action|Comedy|Sci-Fi"` → `["Action", "Comedy", "Sci-Fi"]`)
2. **Pattern Matching**: `LIKE '%Action%'` is substring search, not token search
3. **Index Overhead**: The query planner may attempt to use the index inefficiently, adding overhead without benefit

### Comparison with Working Indexes

For comparison, LSM_TREE indexes on the same dataset show massive improvements:

- `Rating(userId)`: **14,836x speedup** ✅
- `Rating(movieId)`: **107.9x speedup** ✅
- `Movie(movieId)`: **58.1x speedup** ✅

### Current Status

**Reported to ArcadeDB team**: [Issue #2703](https://github.com/ArcadeData/arcadedb/issues/2703)

Awaiting clarification on:

1. Whether `LIKE` queries should use FULL_TEXT indexes
2. Alternative syntax (e.g., `SEARCH()` function) for proper full-text search
3. Whether this is a query optimizer issue

### Recommendation

**For now**: The example keeps the FULL_TEXT index for demonstration purposes, but be aware:

- ✅ LSM_TREE indexes (on movieId, userId) work excellently
- ⚠️ FULL_TEXT index on genres provides minimal/negative benefit with LIKE
- 🔍 Future ArcadeDB releases may improve this behavior

**Alternative**: If you need text search, consider:

- Using separate tokens in dedicated fields
- Implementing application-level text filtering after retrieval
- Waiting for ArcadeDB team guidance on proper full-text search syntax

### Documentation Note

This limitation is documented here for future reference. The example code includes:

- Performance validation that detects the issue
- Detailed comments explaining FULL_TEXT index behavior
- Result consistency validation (shows queries return identical data regardless of indexes)

## Key Takeaways

1. ✅ **Automatic type inference** by Java provides intelligent LONG/DOUBLE/STRING selection
2. ✅ **Schema-on-write** simplifies development (no manual schema creation needed)
3. ✅ **NULL value handling** works seamlessly across all data types (STRING, INTEGER, etc.)
4. ✅ **Batch processing** (`commit_every`) dramatically improves import performance
5. ✅ **Create indexes AFTER import** - 2-3x faster than indexing during import
6. ✅ **LSM_TREE indexes** provide massive performance gains (up to 14,836x speedup!)
7. ✅ **Statistical validation** (10 runs) ensures reliable performance measurements
8. ✅ **Result validation** compares actual data values, not just row counts
9. ⚠️ **FULL_TEXT indexes** don't help `LIKE` queries (known issue, reported)

## Next Steps

- **Try Example 05**: Graph import (MovieLens as vertices and edges)
- **Experiment**: Modify `commit_every` values to see performance impact
- **Add queries**: Try your own analysis queries on the dataset
- **Index tuning**: Create different index combinations and measure speedup
- **Type testing**: Change type inference rules and observe import behavior

## Related Examples

- [Example 01 - Simple Document Store](01_simple_document_store.md) - Document basics and CRUD
- [Example 02 - Social Network Graph](02_social_network_graph.md) - Graph modeling with NULL handling
- [Example 03 - Vector Search](03_vector_search.md) - Semantic similarity search (experimental)

---

**Dataset License**: MovieLens data is provided by GroupLens Research and is free to use for educational purposes. See [https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/) for details.
