#!/usr/bin/env python3
"""
Example 03: Vector Search - Semantic Similarity

⚠️  EXPERIMENTAL FEATURE ⚠️
Vector search in ArcadeDB is currently under active development and may have bugs.
This example demonstrates the API and concepts, but is not recommended for
production use until the vector implementation stabilizes.

This example demonstrates vector embeddings and semantic similarity search using
ArcadeDB's HNSW (Hierarchical Navigable Small World) index.

Key Concepts:
- Storing vector embeddings (simulated sentence embeddings)
- Creating HNSW indexes for fast nearest-neighbor search
- Finding semantically similar documents using cosine similarity
- Understanding vector search parameters (dimensions, distance functions)
- Index population strategies and performance characteristics

Implementation Status:
- Current: Uses jelmerk/hnswlib (Java port)
- Future: Will migrate to datastax/jvector (better performance)
- See docs/examples/03_vector_search.md for detailed comparison

Potential Use Cases (when stable):
- Semantic document search (find similar articles, papers)
- RAG (Retrieval-Augmented Generation) for LLMs
- Recommendation systems (find similar products, content)
- Duplicate detection (find near-duplicate text)
- Question answering (find relevant context)

Requirements:
- arcadedb-embedded (any distribution)
- NumPy (for vector operations and mock embeddings)

Note: This example uses mock embeddings for demonstration. In production:
- Use real embedding models (OpenAI, sentence-transformers, etc.)
- Store higher-dimensional vectors (384, 768, 1536 dimensions)
- Index vectors incrementally as you insert documents
- Consider metadata filtering strategies (see documentation)
- Test thoroughly as vector features may have known issues

About Vector Search:
Vector embeddings represent text/images as points in high-dimensional space.
Similar items are close together, enabling semantic search beyond keyword matching.
HNSW (Hierarchical Navigable Small World) enables logarithmic search time without
loading all vectors into memory.
"""

import os
import shutil
import time

import arcadedb_embedded as arcadedb
import numpy as np

print("=" * 70)
print("🔍 ArcadeDB Python - Example 03: Vector Search")
print("=" * 70)
print()
print("⚠️  EXPERIMENTAL: Vector search is under active development")
print("   This example demonstrates the API but may have known issues.")
print("   Not recommended for production use yet.")
print()

# -----------------------------------------------------------------------------
# Step 1: Create Database
# -----------------------------------------------------------------------------
print("Step 1: Creating database...")
step_start = time.time()

db_dir = "./my_test_databases"
db_path = os.path.join(db_dir, "vector_search_db")

# Clean up any existing database
if os.path.exists(db_path):
    shutil.rmtree(db_path)

if os.path.exists("./log"):
    shutil.rmtree("./log")

db = arcadedb.create_database(db_path)

print(f"   ✅ Database created at: {db_path}")
print("   💡 Using embedded mode - no server needed!")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 2: Create Schema for Documents with Vectors
# -----------------------------------------------------------------------------
print("Step 2: Creating schema for document embeddings...")
step_start = time.time()

with db.transaction():
    # Create vertex type for documents
    # We use VERTEX (not DOCUMENT) so we can potentially create relationships
    db.command("sql", "CREATE VERTEX TYPE Article")

    # Properties
    db.command("sql", "CREATE PROPERTY Article.id STRING")
    db.command("sql", "CREATE PROPERTY Article.title STRING")
    db.command("sql", "CREATE PROPERTY Article.content STRING")
    db.command("sql", "CREATE PROPERTY Article.category STRING")

    # Vector property - MUST be ARRAY_OF_FLOATS for HNSW index
    # In production, this would be 384, 768, or 1536 dimensions
    db.command("sql", "CREATE PROPERTY Article.embedding ARRAY_OF_FLOATS")

    # Index for fast lookups
    db.command("sql", "CREATE INDEX ON Article (id) UNIQUE")

print("   ✅ Created Article vertex type with embedding property")
print("   💡 Vector property type: ARRAY_OF_FLOATS (required for HNSW)")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 3: Create Mock Document Embeddings
# -----------------------------------------------------------------------------
print("Step 3: Creating sample documents with mock embeddings...")
print()

# Using realistic dimensions like sentence-transformers models
# - all-MiniLM-L6-v2: 384 dimensions (most popular, good balance)
# - all-mpnet-base-v2: 768 dimensions (higher quality)
# - OpenAI text-embedding-3-small: 1536 dimensions
EMBEDDING_DIM = 384  # Matching sentence-transformers/all-MiniLM-L6-v2

print(f"   💡 Using {EMBEDDING_DIM}D embeddings (like sentence-transformers)")
print("   💡 In production, use real models: OpenAI, sentence-transformers, etc.")
print()

# Set random seed for reproducibility
np.random.seed(42)


# Pre-compute uniformly distributed category base vectors on unit sphere
# This ensures categories are maximally separated in embedding space
def generate_uniform_sphere_points(n_points, dimensions):
    """
    Generate points uniformly distributed on unit sphere using Gaussian method.

    Generates random points from standard normal distribution, then normalizes.
    This produces a uniform distribution on the sphere surface (Muller 1959).

    Args:
        n_points: Number of points to generate
        dimensions: Dimensionality of the sphere

    Returns:
        numpy.ndarray: Array of shape (n_points, dimensions) with unit vectors
    """
    # Generate from standard normal distribution
    points = np.random.randn(n_points, dimensions)

    # Normalize each point to unit length
    norms = np.linalg.norm(points, axis=1, keepdims=True)
    points = points / norms

    return points


# Generate category base vectors once (will be used by create_mock_embedding)
CATEGORY_BASE_VECTORS = None


def initialize_category_vectors(num_categories, dimensions):
    """Initialize uniformly distributed category base vectors."""
    global CATEGORY_BASE_VECTORS
    CATEGORY_BASE_VECTORS = generate_uniform_sphere_points(num_categories, dimensions)


def create_mock_embedding(category, doc_id):
    """
    Create a realistic mock embedding based on category.

    Simulates how real embeddings work:
    - Categories uniformly distributed on unit sphere (maximally separated)
    - Each document has unique but related embedding within category
    - Normalized to unit length (standard for cosine similarity)

    Args:
        category: Document category (e.g., "category_1", "category_2", ...)
        doc_id: Unique document ID (for variation within category)

    Returns:
        numpy.ndarray: Normalized 384D embedding vector
    """
    # Extract category number from "category_N" format
    category_num = int(category.split("_")[1]) - 1  # 0-indexed

    # Get pre-computed uniformly distributed base vector for this category
    category_vector = CATEGORY_BASE_VECTORS[category_num]

    # Add small document-specific variation
    doc_seed = hash(doc_id) % 1000000
    doc_rng = np.random.RandomState(doc_seed)

    # Mix 85% category vector + 15% random noise for realistic clustering
    # Real embeddings have ~0.7-0.95 similarity within same topic
    noise = doc_rng.randn(EMBEDDING_DIM) * 0.15
    embedding = category_vector + noise

    # Normalize to unit length (standard practice for cosine similarity)
    embedding = embedding / np.linalg.norm(embedding)

    return embedding


# Generate a realistic dataset of 10,000 documents across 10 categories
# This simulates a large knowledge base or documentation corpus
NUM_DOCUMENTS = 10000
NUM_CATEGORIES = 100
DOCS_PER_CATEGORY = NUM_DOCUMENTS // NUM_CATEGORIES

print(
    f"   💡 Generating {NUM_DOCUMENTS:,} documents "
    f"across {NUM_CATEGORIES} categories..."
)
print()

# Initialize uniformly distributed category vectors
initialize_category_vectors(NUM_CATEGORIES, EMBEDDING_DIM)
print(f"   ✅ Generated {NUM_CATEGORIES} uniformly distributed category base vectors")
print("      (Categories maximally separated on unit sphere)")
print()

documents = []
doc_counter = 1

for cat_num in range(1, NUM_CATEGORIES + 1):
    category = f"category_{cat_num}"

    for doc_num in range(1, DOCS_PER_CATEGORY + 1):
        doc_id = f"doc{doc_counter:05d}"
        title = f"Category {cat_num}: Document {doc_num}"
        content = f"This is a mock document about {category} topics..."

        documents.append(
            {
                "id": doc_id,
                "title": title,
                "content": content,
                "category": category,
            }
        )
        doc_counter += 1

# Insert documents with embeddings
print("   Inserting documents with embeddings...")
step_start = time.time()

with db.transaction():
    for doc in documents:
        # Generate realistic mock embedding
        embedding = create_mock_embedding(doc["category"], doc["id"])

        # Convert numpy array to Java float array
        java_embedding = arcadedb.to_java_float_array(embedding)

        # Create vertex
        vertex = db.new_vertex("Article")
        vertex.set("id", doc["id"])
        vertex.set("title", doc["title"])
        vertex.set("content", doc["content"])
        vertex.set("category", doc["category"])
        vertex.set("embedding", java_embedding)
        vertex.save()

print(f"   ✅ Inserted {len(documents):,} documents with {EMBEDDING_DIM}D embeddings")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 4: Create HNSW Vector Index
# -----------------------------------------------------------------------------
print("Step 4: Creating HNSW vector index...")
step_start = time.time()

print("   💡 HNSW Parameters:")
print(f"      • dimensions: {EMBEDDING_DIM} (matches embedding size)")
print("      • distance_function: cosine (best for normalized vectors)")
print("      • m: 16 (connections per node, higher = more accurate but slower)")
print("      • ef: 128 (search quality, higher = more accurate)")
print("      • max_items: 10000 (can index up to 10K documents)")
print()

with db.transaction():
    index = db.create_vector_index(
        vertex_type="Article",
        vector_property="embedding",
        dimensions=EMBEDDING_DIM,
        id_property="id",
        distance_function="cosine",  # Options: cosine, euclidean, inner_product
        m=16,
        ef=128,
        ef_construction=128,
        max_items=10000,
    )

print("   ✅ Created HNSW vector index")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 5: Populate Vector Index (Batch Indexing)
# -----------------------------------------------------------------------------
print("Step 5: Populating vector index with existing documents...")
print()
print("   ⚠️  BATCH INDEXING: One-time operation for existing data")
print()
print("   💡 Production Best Practices:")
print("      • INDEX AS YOU INSERT: Call index.add_vertex() during creation")
print("      • AVOID RE-INDEXING: Batch approach is for initial load only")
print("      • FILTERING: Build ONE index, use oversampling for filters")
print("      • PERFORMANCE: ~13ms per document (HNSW graph + disk writes)")
print()
print("   📊 What happens during indexing:")
print("      • HNSW graph built in RAM (algorithm execution)")
print("      • Edges persisted to disk (~9KB per document)")
print("      • Vertices updated with graph metadata")
print("      • All within transaction for consistency")
print()

step_start = time.time()

# Fetch all documents and add them to the index
result = db.query("sql", "SELECT FROM Article ORDER BY id")
indexed_count = 0

with db.transaction():
    for record in result:
        # Get the underlying Java vertex object
        java_vertex = record._java_result.getElement().get().asVertex()

        # Add to vector index
        index.add_vertex(java_vertex)
        indexed_count += 1

print(f"   ✅ Indexed {indexed_count:,} documents in HNSW index")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
per_doc_time = (time.time() - step_start) / indexed_count * 1000
print(f"   ⏱️  Per-document indexing time: {per_doc_time:.2f}ms")
print()
print("   ⚠️  Note: This step is slow because we're batch-indexing existing data.")
print("      In production, you'd typically index vectors as you insert them,")
print("      not re-index the entire dataset.")
print()

# -----------------------------------------------------------------------------
# Step 6: Perform Semantic Search
# -----------------------------------------------------------------------------
print("Step 6: Performing semantic similarity searches...")
step_start = time.time()

# Sample 10 random categories (or NUM_CATEGORIES if less than 10)
num_queries = min(10, NUM_CATEGORIES)
sampled_categories = np.random.choice(
    range(1, NUM_CATEGORIES + 1), size=num_queries, replace=False
)

print(f"   Running {num_queries} queries on randomly sampled categories...")
print()

for query_num, cat_num in enumerate(sampled_categories, 1):
    category = f"category_{cat_num}"

    print(f"   🔍 Query {query_num}: Find documents similar to Category {cat_num}")
    print()

    query_embedding = create_mock_embedding(category, f"query{query_num}")

    # Get top 5 most similar (smallest distances)
    most_similar = index.find_nearest(query_embedding, k=5)

    print("      Top 5 MOST similar documents (smallest distance):")
    for i, (vertex, distance) in enumerate(most_similar, 1):
        title = vertex.get("title")
        doc_category = vertex.get("category")
        print(f"      {i}. {title}")
        print(f"         Category: {doc_category}, Distance: {distance:.4f}")
    print()

    # Get all documents to find least similar
    # (HNSW doesn't have a "find_farthest" method, so we get more results)
    all_results = index.find_nearest(query_embedding, k=NUM_DOCUMENTS)
    least_similar = list(all_results)[-5:]  # Last 5 = farthest

    print("      Top 5 LEAST similar documents (largest distance):")
    for i, (vertex, distance) in enumerate(least_similar, 1):
        title = vertex.get("title")
        doc_category = vertex.get("category")
        print(f"      {i}. {title}")
        print(f"         Category: {doc_category}, Distance: {distance:.4f}")
    print()

print(f"   ⏱️  All queries time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------
print("=" * 70)
print("✅ Vector search example completed successfully!")
print("=" * 70)
print()

db.close()

print(f"💡 Database preserved at: {db_path}")
print()


"""Below is the output of running this script. We'll revisit it when we have a better
vector search implementation in ArcadeDB.

python 03_vector_search.py
======================================================================
🔍 ArcadeDB Python - Example 03: Vector Search
======================================================================

⚠️  EXPERIMENTAL: Vector search is under active development
   This example demonstrates the API but may have known issues.
   Not recommended for production use yet.

Step 1: Creating database...
[To redirect Truffle log output to a file use one of the following options:
* '--log.file=<path>' if the option is passed using a guest language launcher.
* '-Dpolyglot.log.file=<path>' if the option is passed using the host Java launcher.
* Configure logging using the polyglot embedding API.]
[engine] WARNING: The polyglot engine uses a fallback runtime that does not support runtime compilation to native code.
Execution without runtime compilation will negatively impact the guest application performance.
The following cause was found: JVMCI is not enabled for this JVM. Enable JVMCI using -XX:+EnableJVMCI.
For more information see: https://www.graalvm.org/latest/reference-manual/embed-languages/#runtime-optimization-support.
To disable this warning use the '--engine.WarnInterpreterOnly=false' option or the '-Dpolyglot.engine.WarnInterpreterOnly=false' system property.

2025-10-23 13:01:09.494 WARNI [PaginatedComponentFile] Unable to disable channel close on interrupt: Unable to make field private sun.nio.ch.Interruptible java.nio.channels.spi.AbstractInterruptibleChannel.interruptor accessible: module java.base does not "opens java.nio.channels.spi" to unnamed module @5b8bc155   ✅ Database created at: ./my_test_databases/vector_search_db
   💡 Using embedded mode - no server needed!
   ⏱️  Time: 0.657s

Step 2: Creating schema for document embeddings...
   ✅ Created Article vertex type with embedding property
   💡 Vector property type: ARRAY_OF_FLOATS (required for HNSW)
   ⏱️  Time: 0.061s

Step 3: Creating sample documents with mock embeddings...

   💡 Using 384D embeddings (like sentence-transformers)
   💡 In production, use real models: OpenAI, sentence-transformers, etc.

   💡 Generating 10,000 documents across 100 categories...

   ✅ Generated 100 uniformly distributed category base vectors
      (Categories maximally separated on unit sphere)

   Inserting documents with embeddings...
   ✅ Inserted 10,000 documents with 384D embeddings
   ⏱️  Time: 1.406s

Step 4: Creating HNSW vector index...
   💡 HNSW Parameters:
      • dimensions: 384 (matches embedding size)
      • distance_function: cosine (best for normalized vectors)
      • m: 16 (connections per node, higher = more accurate but slower)
      • ef: 128 (search quality, higher = more accurate)
      • max_items: 10000 (can index up to 10K documents)

   ✅ Created HNSW vector index
   ⏱️  Time: 0.138s

Step 5: Populating vector index with existing documents...

   ⚠️  BATCH INDEXING: One-time operation for existing data

   💡 Production Best Practices:
      • INDEX AS YOU INSERT: Call index.add_vertex() during creation
      • AVOID RE-INDEXING: Batch approach is for initial load only
      • FILTERING: Build ONE index, use oversampling for filters
      • PERFORMANCE: ~13ms per document (HNSW graph + disk writes)

   📊 What happens during indexing:
      • HNSW graph built in RAM (algorithm execution)
      • Edges persisted to disk (~9KB per document)
      • Vertices updated with graph metadata
      • All within transaction for consistency

   ✅ Indexed 10,000 documents in HNSW index
   ⏱️  Time: 122.899s
   ⏱️  Per-document indexing time: 12.29ms

   ⚠️  Note: This step is slow because we're batch-indexing existing data.
      In production, you'd typically index vectors as you insert them,
      not re-index the entire dataset.

Step 6: Performing semantic similarity searches...
   Running 10 queries on randomly sampled categories...

   🔍 Query 1: Find documents similar to Category 98

      Top 5 MOST similar documents (smallest distance):
      1. Category 98: Document 67
         Category: category_98, Distance: 0.7334
      2. Category 98: Document 14
         Category: category_98, Distance: 0.7718
      3. Category 98: Document 7
         Category: category_98, Distance: 0.7789
      4. Category 98: Document 74
         Category: category_98, Distance: 0.7841
      5. Category 98: Document 43
         Category: category_98, Distance: 0.7850

      Top 5 LEAST similar documents (largest distance):
      1. Category 20: Document 53
         Category: category_20, Distance: 1.1704
      2. Category 43: Document 20
         Category: category_43, Distance: 1.1712
      3. Category 6: Document 3
         Category: category_6, Distance: 1.1764
      4. Category 56: Document 15
         Category: category_56, Distance: 1.1880
      5. Category 51: Document 96
         Category: category_51, Distance: 1.1912

   🔍 Query 2: Find documents similar to Category 43

      Top 5 MOST similar documents (smallest distance):
      1. Category 43: Document 69
         Category: category_43, Distance: 0.7506
      2. Category 43: Document 60
         Category: category_43, Distance: 0.7778
      3. Category 43: Document 22
         Category: category_43, Distance: 0.7924
      4. Category 43: Document 70
         Category: category_43, Distance: 0.7929
      5. Category 43: Document 42
         Category: category_43, Distance: 0.7986

      Top 5 LEAST similar documents (largest distance):
      1. Category 36: Document 9
         Category: category_36, Distance: 1.1680
      2. Category 97: Document 20
         Category: category_97, Distance: 1.1736
      3. Category 27: Document 12
         Category: category_27, Distance: 1.1739
      4. Category 32: Document 17
         Category: category_32, Distance: 1.1760
      5. Category 16: Document 13
         Category: category_16, Distance: 1.1773

   🔍 Query 3: Find documents similar to Category 45

      Top 5 MOST similar documents (smallest distance):
      1. Category 45: Document 89
         Category: category_45, Distance: 0.7727
      2. Category 18: Document 10
         Category: category_18, Distance: 0.8148
      3. Category 32: Document 76
         Category: category_32, Distance: 0.8181
      4. Category 45: Document 1
         Category: category_45, Distance: 0.8195
      5. Category 45: Document 55
         Category: category_45, Distance: 0.8205

      Top 5 LEAST similar documents (largest distance):
      1. Category 86: Document 20
         Category: category_86, Distance: 1.1736
      2. Category 77: Document 50
         Category: category_77, Distance: 1.1773
      3. Category 8: Document 70
         Category: category_8, Distance: 1.1784
      4. Category 84: Document 89
         Category: category_84, Distance: 1.1826
      5. Category 89: Document 34
         Category: category_89, Distance: 1.1866

   🔍 Query 4: Find documents similar to Category 38

      Top 5 MOST similar documents (smallest distance):
      1. Category 38: Document 31
         Category: category_38, Distance: 0.7694
      2. Category 38: Document 74
         Category: category_38, Distance: 0.7716
      3. Category 38: Document 75
         Category: category_38, Distance: 0.7773
      4. Category 38: Document 48
         Category: category_38, Distance: 0.7931
      5. Category 38: Document 17
         Category: category_38, Distance: 0.8035

      Top 5 LEAST similar documents (largest distance):
      1. Category 95: Document 18
         Category: category_95, Distance: 1.1653
      2. Category 6: Document 26
         Category: category_6, Distance: 1.1673
      3. Category 82: Document 45
         Category: category_82, Distance: 1.1690
      4. Category 51: Document 86
         Category: category_51, Distance: 1.1742
      5. Category 9: Document 35
         Category: category_9, Distance: 1.1759

   🔍 Query 5: Find documents similar to Category 16

      Top 5 MOST similar documents (smallest distance):
      1. Category 16: Document 85
         Category: category_16, Distance: 0.7961
      2. Category 77: Document 59
         Category: category_77, Distance: 0.8135
      3. Category 16: Document 51
         Category: category_16, Distance: 0.8144
      4. Category 16: Document 71
         Category: category_16, Distance: 0.8203
      5. Category 16: Document 18
         Category: category_16, Distance: 0.8225

      Top 5 LEAST similar documents (largest distance):
      1. Category 15: Document 5
         Category: category_15, Distance: 1.1636
      2. Category 15: Document 53
         Category: category_15, Distance: 1.1658
      3. Category 56: Document 48
         Category: category_56, Distance: 1.1718
      4. Category 31: Document 41
         Category: category_31, Distance: 1.1840
      5. Category 59: Document 34
         Category: category_59, Distance: 1.1967

   🔍 Query 6: Find documents similar to Category 22

      Top 5 MOST similar documents (smallest distance):
      1. Category 22: Document 35
         Category: category_22, Distance: 0.7863
      2. Category 22: Document 27
         Category: category_22, Distance: 0.7937
      3. Category 22: Document 18
         Category: category_22, Distance: 0.8105
      4. Category 91: Document 18
         Category: category_91, Distance: 0.8267
      5. Category 20: Document 8
         Category: category_20, Distance: 0.8329

      Top 5 LEAST similar documents (largest distance):
      1. Category 57: Document 65
         Category: category_57, Distance: 1.1658
      2. Category 73: Document 78
         Category: category_73, Distance: 1.1763
      3. Category 64: Document 2
         Category: category_64, Distance: 1.1833
      4. Category 86: Document 28
         Category: category_86, Distance: 1.1837
      5. Category 83: Document 44
         Category: category_83, Distance: 1.1961

   🔍 Query 7: Find documents similar to Category 82

      Top 5 MOST similar documents (smallest distance):
      1. Category 82: Document 95
         Category: category_82, Distance: 0.7767
      2. Category 82: Document 69
         Category: category_82, Distance: 0.7817
      3. Category 14: Document 7
         Category: category_14, Distance: 0.7931
      4. Category 82: Document 81
         Category: category_82, Distance: 0.7991
      5. Category 82: Document 3
         Category: category_82, Distance: 0.8004

      Top 5 LEAST similar documents (largest distance):
      1. Category 98: Document 3
         Category: category_98, Distance: 1.1652
      2. Category 78: Document 4
         Category: category_78, Distance: 1.1660
      3. Category 26: Document 2
         Category: category_26, Distance: 1.1753
      4. Category 73: Document 51
         Category: category_73, Distance: 1.1898
      5. Category 60: Document 65
         Category: category_60, Distance: 1.1935

   🔍 Query 8: Find documents similar to Category 76

      Top 5 MOST similar documents (smallest distance):
      1. Category 76: Document 94
         Category: category_76, Distance: 0.7519
      2. Category 76: Document 98
         Category: category_76, Distance: 0.7728
      3. Category 76: Document 11
         Category: category_76, Distance: 0.7806
      4. Category 76: Document 72
         Category: category_76, Distance: 0.7810
      5. Category 76: Document 68
         Category: category_76, Distance: 0.7858

      Top 5 LEAST similar documents (largest distance):
      1. Category 58: Document 88
         Category: category_58, Distance: 1.1704
      2. Category 37: Document 65
         Category: category_37, Distance: 1.1719
      3. Category 45: Document 72
         Category: category_45, Distance: 1.1737
      4. Category 70: Document 22
         Category: category_70, Distance: 1.1946
      5. Category 28: Document 29
         Category: category_28, Distance: 1.1956

   🔍 Query 9: Find documents similar to Category 36

      Top 5 MOST similar documents (smallest distance):
      1. Category 36: Document 14
         Category: category_36, Distance: 0.7304
      2. Category 36: Document 88
         Category: category_36, Distance: 0.7478
      3. Category 36: Document 78
         Category: category_36, Distance: 0.7900
      4. Category 19: Document 25
         Category: category_19, Distance: 0.7953
      5. Category 36: Document 65
         Category: category_36, Distance: 0.8019

      Top 5 LEAST similar documents (largest distance):
      1. Category 47: Document 15
         Category: category_47, Distance: 1.1613
      2. Category 82: Document 83
         Category: category_82, Distance: 1.1620
      3. Category 70: Document 6
         Category: category_70, Distance: 1.1688
      4. Category 37: Document 37
         Category: category_37, Distance: 1.1770
      5. Category 70: Document 55
         Category: category_70, Distance: 1.1793

   🔍 Query 10: Find documents similar to Category 28

      Top 5 MOST similar documents (smallest distance):
      1. Category 66: Document 51
         Category: category_66, Distance: 0.8075
      2. Category 28: Document 41
         Category: category_28, Distance: 0.8170
      3. Category 28: Document 16
         Category: category_28, Distance: 0.8208
      4. Category 99: Document 38
         Category: category_99, Distance: 0.8228
      5. Category 99: Document 66
         Category: category_99, Distance: 0.8255

      Top 5 LEAST similar documents (largest distance):
      1. Category 49: Document 41
         Category: category_49, Distance: 1.1738
      2. Category 87: Document 11
         Category: category_87, Distance: 1.1747
      3. Category 51: Document 59
         Category: category_51, Distance: 1.1756
      4. Category 87: Document 13
         Category: category_87, Distance: 1.1990
      5. Category 51: Document 48
         Category: category_51, Distance: 1.1995

   ⏱️  All queries time: 4.225s

======================================================================
✅ Vector search example completed successfully!
======================================================================

💡 Database preserved at: ./my_test_databases/vector_search_db

"""
