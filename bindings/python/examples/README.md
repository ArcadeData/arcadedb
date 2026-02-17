# ArcadeDB Python Examples

This directory contains hands-on examples demonstrating ArcadeDB Python bindings in action.

## Quick Start

**⚠️ Important: Run examples from the `examples/` directory for proper file paths and database creation.**

```bash
# Navigate to the examples directory first
cd bindings/python/examples

# Then run the basic document store example
python 01_simple_document_store.py
```

See the dataset downloader guide: [docs/examples/download_data.md](../docs/examples/download_data.md).

## Available Examples

### 📄 [01_simple_document_store.py](./01_simple_document_store.py)
**Document Types | CRUD Operations | Rich Data Types | NULL Handling**

Perfect introduction to ArcadeDB basics:
- Creating embedded databases (no server needed)
- Document types with comprehensive schema (STRING, BOOLEAN, DATE, DATETIME, DECIMAL, FLOAT, INTEGER, LIST, etc.)
- CRUD operations with ArcadeDB SQL
- NULL value handling (INSERT, UPDATE, queries with IS NULL)
- Transactions and data validation
- Built-in functions (`date()`, `sysdate()`)
- Arrays/lists with type safety

**Learn:** Document storage, SQL dialect, schema design, NULL handling, data type diversity

---

### 🔗 [02_social_network_graph.py](./02_social_network_graph.py) ✅ **COMPLETE**
**Vertex Types | Edge Types | Graph Traversal | SQL MATCH vs OpenCypher | NULL Handling**

Complete social network modeling with graph database:
- Creating vertex types (Person) and edge types (FRIEND_OF) with rich properties
- NULL value handling for optional fields (email, phone, reputation)
- Bidirectional relationships with metadata (since, closeness)
- Graph traversal patterns (friends, friends-of-friends, mutual connections)
- Comparing SQL MATCH vs OpenCypher query languages
- Variable-length path queries (`*1..3`) and graph aggregations
- NULL filtering (IS NULL queries for missing contact info)
- Relationship property queries (closeness filtering)

**Learn:** Graph schema design, relationship modeling, multi-language querying, NULL handling in graphs

**Status:** ✅ Fully functional - 8 people, 24 bidirectional edges, comprehensive queries

### 🔍 [03_vector_search.py](./03_vector_search.py) ✅ **COMPLETE**
**Vector Embeddings | HNSW (JVector) Index | Semantic Search | Performance Analysis**

Semantic similarity search with AI/ML:
**Status:** ✅ Fully functional - Demonstrates vector search capabilities

---

### 📄 [04_csv_import_documents.py](./04_csv_import_documents.py) ✅ **COMPLETE**
**CSV Import | Schema Definition | Batch Processing | Type Inference**

High-performance CSV import for document data:
- Importing MovieLens dataset (movies.csv)
- Automatic schema creation with type inference
- Handling NULL values and data cleaning
- Batch processing for optimal performance
- Index creation strategies

**Note:** Download the MovieLens dataset first with `python download_data.py movielens-<size>`.

**Learn:** ETL patterns, bulk import, schema management, performance tuning

**Status:** ✅ Fully functional - Imports 100K+ records efficiently

---

### 🕸️ [05_csv_import_graph.py](./05_csv_import_graph.py) ✅ **COMPLETE**
**Graph Import | Edge Creation | Foreign Keys | Performance Benchmarking**

Complex graph construction from CSV data:
- Importing Users, Movies, and Ratings
- Creating edges (User-[RATED]->Movie) from foreign keys
- Handling large-scale edge creation (millions of edges)
- Benchmarking different import strategies (Sync vs Async vs Batch)
- Memory management for large graphs

**Learn:** Graph ETL, edge creation patterns, performance optimization, memory management

**Status:** ✅ Fully functional - Benchmarks show optimal import strategies

---

### 🎬 [06_vector_search_recommendations.py](./06_vector_search_recommendations.py) ✅ **COMPLETE**
**Hybrid Search | Recommendation Engine | Vector + Graph | Real-world Use Case**

Building a movie recommendation engine:
- Generating embeddings for movies (Title + Genres)
- Combining vector similarity with graph relationships
- "More like this" functionality
- Hybrid queries (Vector Search + SQL Filtering)
- Personalized recommendations based on user history

**Learn:** Recommendation systems, hybrid search, vector+graph integration

**Status:** ✅ Fully functional - Generates relevant movie recommendations

---



## 💡 Tips

- **Run from examples/ directory** - Always execute examples from `bindings/python/examples/` for correct file paths
- **Start with Example 01** - Foundation for all ArcadeDB concepts
- **Database files persist** - Examples preserve data for inspection
- **Output is educational** - Check console output to understand operations
- **Experiment freely** - Examples clean up and recreate on each run

## 🔗 Learn More

- **[ArcadeDB Documentation](https://docs.arcadedb.com/)**
- **[Python API Reference](../docs/api/)**
- **[GitHub Repository](https://github.com/ArcadeData/arcadedb)**

---

*Examples are designed to be self-contained and educational. Each includes detailed comments and step-by-step explanations.*
