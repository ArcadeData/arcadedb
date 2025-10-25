# Vector Search - Semantic Similarity

**⚠️ EXPERIMENTAL FEATURE**: Vector search in ArcadeDB is under active development. This example demonstrates the API and concepts, but is not recommended for production use until the implementation stabilizes.

[View source code](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/examples/03_vector_search.py){ .md-button }

## Overview

This example demonstrates semantic similarity search using vector embeddings and HNSW (Hierarchical Navigable Small World) indexing. It covers:

- Storing 384-dimensional vector embeddings (mimicking sentence-transformers)
- Creating and populating HNSW indexes
- Performing nearest-neighbor searches
- Understanding indexing performance and architecture
- Best practices for filtering and production deployment

## Implementation Status

### Current: jelmerk/hnswlib

ArcadeDB currently uses [jelmerk/hnswlib](https://github.com/jelmerk/hnswlib), a Java port of the original C++ HNSW implementation.

**Characteristics:**
- ✅ Mature, proven algorithm
- ✅ Supports multiple distance functions (cosine, euclidean, inner product)
- ⚠️ Java port has performance overhead vs native implementations
- ⚠️ Limited to single-threaded indexing
- ⚠️ Memory management through JVM

### Future: datastax/jvector

The ArcadeDB team is planning migration to [datastax/jvector](https://github.com/datastax/jvector), a modern Java-native vector search library.

**Expected improvements:**
- 🚀 Better performance (native Java, no port overhead)
- 🚀 Multi-threaded indexing support
- 🚀 More efficient memory usage
- 🚀 Better integration with Java ecosystems
- 🚀 Active development and enterprise support

**Timeline:** Pending upstream ArcadeDB Java implementation changes.

!!! warning "Python Bindings Impact"
    The Python bindings are thin wrappers around Java APIs. When ArcadeDB migrates to jvector, the Python API will remain the same, but you'll automatically benefit from improved performance.

## Key Concepts

### Vector Embeddings

Vector embeddings represent text, images, or other data as points in high-dimensional space:

```python
# Example with sentence-transformers
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dimensions
embedding = model.encode("This is a sample document")
# embedding is now a 384D numpy array
```

**Common dimensions:**
- **384D**: sentence-transformers/all-MiniLM-L6-v2 (fast, good quality)
- **768D**: sentence-transformers/all-mpnet-base-v2 (higher quality)
- **1536D**: OpenAI text-embedding-3-small (best quality, paid)

### HNSW Index

Hierarchical Navigable Small World graphs enable fast approximate nearest-neighbor search:

```python
index = db.create_vector_index(
    vertex_type="Article",
    vector_property="embedding",
    dimensions=384,
    id_property="id",
    distance_function="cosine",  # or "euclidean", "inner_product"
    m=16,                         # connections per node
    ef=128,                       # search quality
    ef_construction=128,          # build quality
    max_items=10000              # capacity
)
```

**Parameters explained:**

- **dimensions**: Must match your embedding model
- **distance_function**:
  - `cosine`: Best for normalized vectors (text embeddings)
  - `euclidean`: Straight-line distance (image features)
  - `inner_product`: Dot product (when magnitude matters)
- **M**: Connections per node (16 typical, 12-48 range)
  - Higher = better accuracy, more memory
  - 16 is good balance for most use cases
- **ef**: Search beam width (100-200 typical)
  - Higher = better recall, slower search
- **ef_construction**: Build quality (100-200 typical)
  - Higher = better index, slower build
- **max_items**: Pre-allocated capacity

### Distance vs Similarity

**Cosine Distance** (used in this example):
- Formula: `distance = 1 - cosine_similarity`
- Range: [0, 2]
  - 0 = identical vectors (same direction)
  - 1 = orthogonal vectors (unrelated)
  - 2 = opposite vectors (negation)
- Best for normalized vectors where direction matters

**Cosine Similarity** (for reference):
- Formula: `similarity = dot(a, b) / (norm(a) * norm(b))`
- Range: [-1, 1]
  - 1 = identical
  - 0 = orthogonal
  - -1 = opposite

## Architecture & Performance

### Index Structure

When you create and populate a vector index, ArcadeDB stores:

**Files created** (for 10K documents, 384D, M=16):
```
Article_414002873519545.5.v0.hnswidx         4 KB   (metadata only)
Article_0.1.65536.v0.bucket                 24 MB   (vertices + embeddings)
Article_0_in_edges.3.65536.v0.bucket        22 MB   (incoming edges)
Article_0_out_edges.2.65536.v0.bucket       22 MB   (outgoing edges)
VectorProximity0_0.7.65536.v0.bucket        47 MB   (HNSW proximity edges)
─────────────────────────────────────────────────
Total:                                     115 MB
```

**Key insight**: The `.hnswidx` file is tiny (4KB) - it only stores metadata. The actual HNSW graph is stored as edges in the database!

### Indexing Performance

**Batch indexing** (10,000 documents):
- Total time: ~130 seconds
- Per document: ~13ms
- What happens:
  1. HNSW algorithm runs in RAM (graph construction)
  2. Each document connects to M≈16 neighbors
  3. Bidirectional edges created (~32 edge writes per doc)
  4. Transaction commits write to disk (~9KB edges per doc)

**Why it's expensive:**
- Complex graph operations (finding best insertion point)
- Distance calculations at each level
- Writing edges to disk (bulk of the time)
- Sequential processing (single-threaded in current implementation)

### Search Performance

**Query characteristics** (k=5 nearest neighbors):
- Visited vertices: ~log(N) × ef ≈ 1,500-2,000 (not all 10,000!)
- HNSW navigates intelligently through graph
- Vertices loaded on-demand from disk
- Hot vertices cached by ArcadeDB's page cache

**Memory during search:**
- Query vector: ~1.5 KB (384 floats)
- Visited vertices: ~4 MB working set
- Page cache: Varies (hot data stays in RAM)
- Total: Scales logarithmically, not linearly with dataset size

## Production Best Practices

### 1. Incremental Indexing (Recommended)

**❌ Don't do this** (batch re-indexing):
```python
# Bad: Index after inserting all documents
with db.transaction():
    for doc in documents:
        vertex = db.new_vertex("Article")
        vertex.set("embedding", embedding)
        vertex.save()

# Batch indexing (slow, expensive)
result = db.query("sql", "SELECT FROM Article")
with db.transaction():
    for record in result:
        index.add_vertex(record.asVertex())  # 130s for 10K docs!
```

**✅ Do this instead** (incremental indexing):
```python
# Good: Index during insertion
with db.transaction():
    for doc in documents:
        vertex = db.new_vertex("Article")
        vertex.set("embedding", embedding)
        vertex.save()

        # Index immediately
        index.add_vertex(vertex)  # Only pays cost once
```

**Why it's better:**
- Pay indexing cost only once per document
- No separate batch indexing step
- Natural for incremental data ingestion
- Simpler code

### 2. Filtering Strategies

Vector databases face a fundamental challenge: **HNSW doesn't support pre-filtering**.

#### Option A: Oversample + Post-filter (Recommended)

```python
def search_with_filters(index, query_embedding, k=5, filters=None,
                       oversample_factor=20):
    """
    Search with metadata filters using oversampling.

    Gets k × oversample_factor candidates, then filters to k results.
    """
    k_oversample = k * oversample_factor
    candidates = index.find_nearest(query_embedding, k=k_oversample)

    if not filters:
        return list(candidates)[:k]

    results = []
    for vertex, distance in candidates:
        matches = all(
            vertex.get(prop) == value
            for prop, value in filters.items()
        )
        if matches:
            results.append((vertex, distance))
            if len(results) >= k:
                break

    return results

# Usage
results = search_with_filters(
    index,
    query_embedding,
    k=5,
    filters={"category": "tech", "year": 2024},
    oversample_factor=20  # Get 100 candidates, filter to 5
)
```

**Pros:**
- Works with single index
- Simple implementation
- Good for moderately selective filters (>1% of data)

**Cons:**
- Wastes computation on filtered results
- May not find k results if filter is very selective
- Need to tune oversample_factor

#### Option B: Multiple Indexes (For Static Partitions)

```python
# Create separate indexes per category
tech_index = db.create_vector_index(
    vertex_type="TechArticle",
    vector_property="embedding",
    dimensions=384,
    ...
)

science_index = db.create_vector_index(
    vertex_type="ScienceArticle",
    vector_property="embedding",
    dimensions=384,
    ...
)

# Query specific index
results = tech_index.find_nearest(query_embedding, k=5)
```

**Pros:**
- No wasted computation
- Smaller indexes = faster search
- Perfect for static categories

**Cons:**
- Index management complexity
- Storage overhead (N × 115MB per index)
- Can't easily combine filters

#### Option C: Hybrid (For Highly Selective Filters)

```python
# If filter results in <1000 documents, brute force
filtered = db.query("sql",
    "SELECT FROM Article WHERE category = 'tech' AND year = 2024")

if len(filtered) < 1000:
    # Brute force distance calculation
    results = []
    for record in filtered:
        vertex = record.asVertex()
        embedding = vertex.get("embedding")
        distance = cosine_distance(query_embedding, embedding)
        results.append((vertex, distance))

    results.sort(key=lambda x: x[1])
    return results[:k]
else:
    # Use HNSW with oversampling
    return search_with_filters(index, query_embedding, k, filters)
```

### 3. Memory Considerations

**RAM usage formula:**
```
RAM ≈ 4 bytes × dimensions × num_vectors × (1 + M/2)
```

**Examples:**
- 10K vectors, 384D, M=16: ~37 MB
- 100K vectors, 384D, M=16: ~370 MB
- 1M vectors, 384D, M=16: ~3.7 GB
- 1M vectors, 1536D, M=16: ~14.7 GB

**Note:** This is working set, not total database size. ArcadeDB uses page caching, so hot data stays in RAM while cold data is read from disk on-demand.

### 4. Choosing Parameters

**Start with defaults:**
```python
M=16, ef=128, ef_construction=128
```

**Then tune based on needs:**

**Higher accuracy needed?**
- Increase M to 32-48 (more memory, better recall)
- Increase ef to 200-300 (slower search, better recall)

**Faster build needed?**
- Decrease ef_construction to 64-100 (faster build, slightly worse index)

**Faster search needed?**
- Decrease ef to 50-100 (faster search, slightly worse recall)
- Decrease M to 8-12 (less memory, faster but less accurate)

## Example Output

```
======================================================================
🔍 ArcadeDB Python - Example 03: Vector Search
======================================================================

⚠️  EXPERIMENTAL: Vector search is under active development
   This example demonstrates the API but may have known issues.
   Not recommended for production use yet.

Step 1: Creating database...
   ✅ Database created at: ./my_test_databases/vector_search_db
   💡 Using embedded mode - no server needed!
   ⏱️  Time: 0.234s

Step 2: Creating schema for document embeddings...
   ✅ Created Article vertex type with embedding property
   💡 Vector property type: ARRAY_OF_FLOATS (required for HNSW)
   ⏱️  Time: 0.012s

Step 3: Creating sample documents with mock embeddings...
   💡 Using 384D embeddings (like sentence-transformers)
   💡 Generating 10,000 documents across 100 categories...
   ✅ Generated 100 uniformly distributed category base vectors
      (Categories maximally separated on unit sphere)
   ✅ Inserted 10,000 documents with 384D embeddings
   ⏱️  Time: 1.347s

Step 4: Creating HNSW vector index...
   💡 HNSW Parameters:
      • dimensions: 384 (matches embedding size)
      • distance_function: cosine (best for normalized vectors)
      • m: 16 (connections per node)
      • ef: 128 (search quality)
      • max_items: 10000
   ✅ Created HNSW vector index
   ⏱️  Time: 0.163s

Step 5: Populating vector index with existing documents...
   ⚠️  BATCH INDEXING: One-time operation for existing data
   💡 Production Best Practices:
      • INDEX AS YOU INSERT: Call index.add_vertex() during creation
      • AVOID RE-INDEXING: Batch approach is for initial load only
      • FILTERING: Build ONE index, use oversampling for filters
      • PERFORMANCE: ~13ms per document (HNSW graph + disk writes)
   ✅ Indexed 10,000 documents in HNSW index
   ⏱️  Time: 129.847s
   ⏱️  Per-document indexing time: 12.98ms

Step 6: Performing semantic similarity searches...
   Running 10 queries on randomly sampled categories...

   🔍 Query 1: Find documents similar to Category 42
      Top 5 MOST similar documents (smallest distance):
      1. Category 42: Document 67
         Category: category_42, Distance: 0.7634
      2. Category 42: Document 12
         Category: category_42, Distance: 0.7698
      ...

   ⏱️  All queries time: 4.521s
```

## Testing Notes

The `test_vector_search` test in [`test_core.py`](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/tests/test_core.py) validates:

- ✅ Vector property creation (ARRAY_OF_FLOATS)
- ✅ HNSW index creation with parameters
- ✅ Vector insertion (NumPy arrays or Python lists)
- ✅ Nearest-neighbor search
- ✅ Result ranking by distance

**Key findings from testing:**
- Index creation is fast (~0.16s) - just metadata
- Index population is expensive (~13ms per document) - graph building
- Search is efficient (logarithmic, not linear in dataset size)
- Works with both NumPy arrays and plain Python lists
- Distance values correct (cosine distance = 1 - similarity)

## Related Documentation

- [Database API](../api/database.md) - Core database operations
- [Transactions](../api/transactions.md) - Transaction management
- [Query Language](../api/query.md) - SQL and Cypher
- [ArcadeDB Vector Search](https://docs.arcadedb.com/) - Official documentation

## Limitations & Roadmap

**Current limitations:**
- ⚠️ Single-threaded indexing (slow for large datasets)
- ⚠️ No native filtered search (requires oversampling)
- ⚠️ JVM memory overhead vs native implementations
- ⚠️ Limited to jelmerk/hnswlib performance characteristics

**Planned improvements:**
- 🚀 Migration to datastax/jvector (faster, native Java)
- 🚀 Multi-threaded indexing support
- 🚀 Better memory management
- 🚀 Potential filtered search support

**For production use:**
Until the implementation stabilizes, consider mature alternatives:
- [PostgreSQL + pgvector](https://github.com/pgvector/pgvector) - Stable, production-ready
- [Pinecone](https://www.pinecone.io/) - Managed vector database
- [Weaviate](https://weaviate.io/) - Open-source vector database
- [Qdrant](https://qdrant.tech/) - High-performance vector search

---

*This example is educational and demonstrates current capabilities. Monitor [ArcadeDB releases](https://github.com/humemai/arcadedb-embedded-python/releases) for vector search stability updates.*
