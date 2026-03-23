#!/usr/bin/env python3
"""
Example 03: Vector Search - Semantic Similarity

This example demonstrates vector embeddings and semantic similarity search using
ArcadeDB's HNSW (JVector) index.

Key Concepts:
- Storing vector embeddings (simulated sentence embeddings)
- Creating HNSW (JVector) indexes for fast nearest-neighbor search
- Finding semantically similar documents using cosine similarity
- Understanding vector search parameters (dimensions, distance functions)
- Index population strategies and performance characteristics

Implementation Status:
- Current: Uses datastax/jvector (better performance)

Potential Use Cases (when stable):
- Semantic document search (find similar articles, papers)
- RAG (Retrieval-Augmented Generation) for LLMs
- Recommendation systems (find similar products, content)
- Duplicate detection (find near-duplicate text)
- Question answering (find relevant context)

Requirements:
- arcadedb-embedded
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
HNSW (JVector) enables logarithmic search time without loading all vectors into memory.
"""

import argparse
import os
import shutil
import time

import arcadedb_embedded as arcadedb
import numpy as np

# Parse command line arguments
parser = argparse.ArgumentParser(
    description="Example 03: Vector Search - Semantic Similarity"
)
args = parser.parse_args()

print("=" * 70)
print("🔍 ArcadeDB Python - Example 03: Vector Search (JVector)")
print("=" * 70)
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

with arcadedb.create_database(db_path) as db:

    print(f"   ✅ Database created at: {db_path}")
    print("   💡 Using embedded mode - no server needed!")
    print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
    print()

    # -----------------------------------------------------------------------------
    # Step 2: Define Schema
    # -----------------------------------------------------------------------------
    print("Step 2: Defining schema...")
    step_start = time.time()

    # Create vertex type for documents
    db.command("sql", "CREATE VERTEX TYPE Article")

    # Create properties
    # Note: Vector property MUST be ARRAY_OF_FLOATS
    db.command("sql", "CREATE PROPERTY Article.title STRING")
    db.command("sql", "CREATE PROPERTY Article.content STRING")
    db.command("sql", "CREATE PROPERTY Article.category STRING")
    db.command("sql", "CREATE PROPERTY Article.embedding ARRAY_OF_FLOATS")
    db.command("sql", "CREATE PROPERTY Article.id STRING")

    # Create standard index on ID for fast lookups
    db.command("sql", "CREATE INDEX ON Article (id) UNIQUE_HASH")

    print("   ✅ Schema created: Article vertex type")
    print("   💡 Vector property type: ARRAY_OF_FLOATS")
    print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
    print()

    # -----------------------------------------------------------------------------
    # Step 3: Generate Mock Data
    # -----------------------------------------------------------------------------
    print("Step 3: Generating mock data...")
    step_start = time.time()

    # Configuration
    NUM_DOCUMENTS = 10000
    EMBEDDING_DIM = 384  # Typical for small transformer models
    NUM_CATEGORIES = 50

    def create_mock_embedding(category_seed, doc_seed):
        """
        Create a deterministic mock embedding based on category and document seeds.
        Documents in the same category will be closer together.
        """
        # Use deterministic random state
        rng = np.random.RandomState(hash(category_seed + doc_seed) % 2**32)

        # Base vector for the category (random direction)
        cat_rng = np.random.RandomState(hash(category_seed) % 2**32)
        category_vector = cat_rng.randn(EMBEDDING_DIM)
        category_vector /= np.linalg.norm(category_vector)

        # Add noise for the specific document
        noise = rng.randn(EMBEDDING_DIM) * 0.2  # 20% noise

        # Combine and normalize
        embedding = category_vector + noise
        embedding /= np.linalg.norm(embedding)

        return embedding.astype(np.float32)

    # Generate documents
    documents = []
    for i in range(NUM_DOCUMENTS):
        cat_id = (i % NUM_CATEGORIES) + 1
        category = f"category_{cat_id}"
        doc_id = f"doc_{i}"

        embedding = create_mock_embedding(category, doc_id)

        documents.append(
            {
                "id": doc_id,
                "title": f"Article {i} about {category}",
                "content": f"This is the content for article {i} in {category}...",
                "category": category,
                "embedding": embedding.tolist(),  # Convert to list for insertion
            }
        )

    print(f"   ✅ Generated {NUM_DOCUMENTS} mock documents")
    print(f"   💡 Embedding dimensions: {EMBEDDING_DIM}")
    print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
    print()

    # -----------------------------------------------------------------------------
    # Step 4: Insert Data
    # -----------------------------------------------------------------------------
    print("Step 4: Inserting data...")
    step_start = time.time()

    # Insert in batches for better performance
    BATCH_SIZE = 1000
    total_inserted = 0

    with db.transaction():
        for i, doc in enumerate(documents):
            db.command(
                "sql",
                """
                INSERT INTO Article SET
                    id = :id,
                    title = :title,
                    content = :content,
                    category = :category,
                    embedding = :embedding
                """,
                {
                    "id": doc["id"],
                    "title": doc["title"],
                    "content": doc["content"],
                    "category": doc["category"],
                    "embedding": arcadedb.to_java_float_array(doc["embedding"]),
                },
            )

            total_inserted += 1

            # Commit batch
            if total_inserted % BATCH_SIZE == 0:
                db.commit()
                db.begin()
                print(f"      Inserted {total_inserted}/{NUM_DOCUMENTS} documents...")

    print(f"   ✅ Inserted {total_inserted} documents")
    print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
    print()

    # -----------------------------------------------------------------------------
    # Step 5: Create Vector Index
    # -----------------------------------------------------------------------------
    print("Step 5: Creating vector index...")
    step_start = time.time()

    print(f"   💡 JVector Parameters:")
    print(f"      • dimensions: {EMBEDDING_DIM} (matches embedding size)")
    print("      • distance_function: cosine (best for normalized vectors)")
    print(
        "      • max_connections: 32 (connections per node, higher = more accurate but slower)"
    )
    print("      • beam_width: 256 (search quality, higher = more accurate)")
    print()

    # Create vector index (JVector implementation - recommended)
    # Using new defaults: max_connections=32, beam_width=256
    # New options available:
    # - quantization: "INT8" or "BINARY" (reduces memory usage)
    # - store_vectors_in_graph: True (faster search, higher disk usage)
    index = db.create_vector_index(
        vertex_type="Article",
        vector_property="embedding",
        dimensions=EMBEDDING_DIM,
        distance_function="cosine",
    )

    print("   ✅ Created JVector vector index")
    print("   💡 LSM index automatically indexes existing records upon creation.")
    print("   ✅ Indexing handled by ArcadeDB engine.")
    print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
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
        # Note: For LSM, getting ALL documents might be slow or limited by k
        k_limit = min(NUM_DOCUMENTS, 1000)
        all_results = index.find_nearest(query_embedding, k=k_limit)
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
