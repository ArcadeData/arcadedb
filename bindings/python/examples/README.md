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

### 🔗 [02_social_network_graph.py](./02_social_network_graph.py)
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

### 🔍 [03_vector_search.py](./03_vector_search.py)
**Vector Embeddings | HNSW (JVector) Index | Semantic Search | Performance Analysis**

Semantic similarity search with AI/ML:
- Creating vector-ready schema (`ARRAY_OF_FLOATS`) for embeddings
- Generating deterministic mock embeddings for repeatable experiments
- Building a JVector (HNSW) index for nearest-neighbor search
- Running top-k similarity queries and inspecting distance scores
- Comparing "most similar" vs "least similar" result sets
- Measuring insertion, indexing, and query phases

**Learn:** Vector data modeling, index creation strategy, and practical semantic search behavior

---

### 📄 [04_csv_import_documents.py](./04_csv_import_documents.py)
**CSV Import | Schema Definition | Batch Processing | Type Inference**

High-performance CSV import for document data:
- Importing MovieLens dataset (movies.csv)
- Automatic schema creation with type inference
- Handling NULL values and data cleaning
- Batch processing for optimal performance
- Index creation strategies

**Note:** Download the MovieLens dataset first with `python download_data.py movielens-<size>`.

**Learn:** ETL patterns, bulk import, schema management, performance tuning

---

### 🕸️ [05_csv_import_graph.py](./05_csv_import_graph.py)
**Graph Import | Edge Creation | Foreign Keys | Performance Benchmarking**

Complex graph construction from CSV data:
- Importing Users, Movies, and Ratings
- Creating edges (User-[RATED]->Movie) from foreign keys
- Handling large-scale edge creation (millions of edges)
- Benchmarking different import strategies (Sync vs Async vs Batch)
- Memory management for large graphs

**Learn:** Graph ETL, edge creation patterns, performance optimization, memory management

---

### 🎬 [06_vector_search_recommendations.py](./06_vector_search_recommendations.py)
**Hybrid Search | Recommendation Engine | Vector + Graph | Real-world Use Case**

Building a movie recommendation engine:
- Generating embeddings for movies (Title + Genres)
- Combining vector similarity with graph relationships
- "More like this" functionality
- Hybrid queries (Vector Search + SQL Filtering)
- Personalized recommendations based on user history

**Learn:** Recommendation systems, hybrid search, vector+graph integration

---

### ⏱️ [14_lifecycle_timing.py](./14_lifecycle_timing.py)
**JVM Startup | DB Create/Open | Transaction Load | Query Phases | Reopen Timing**

Lifecycle benchmark for embedded ArcadeDB with mixed workloads:
- Measures JVM startup time in-process
- Creates schema and loads table/graph/vector data
- Runs query workload before and after reopen
- Prints per-run timings and final averages
- Uses random `/tmp` database path and always cleans up

**Learn:** Cold-start behavior, lifecycle costs, and realistic mixed-workload timing patterns

---

### 📊 [09_stackoverflow_graph_oltp.py](./09_stackoverflow_graph_oltp.py)
**Graph OLTP | Deterministic Verification | Cross-DB Benchmarking**

Stack Overflow property-graph OLTP benchmark with mixed CRUD operations:
- Supports deterministic single-thread verification through DB-scoped baselines
- `--verify-single-thread-series` checks repeatability for one DB/mode, not strict
  cross-DB equality
- Summary files report real post-run filesystem usage in `du_mib`, while `disk_after_*`
  fields are benchmark-reported logical sizes
- Per-operation latency is derived from `latency_summary.ops.{50,95,99}` and converted
  from seconds to milliseconds; counts come from `op_counts`

**Learn:** OLTP workload comparison, verification semantics, and benchmark result interpretation

---

### 📈 [10_stackoverflow_graph_olap.py](./10_stackoverflow_graph_olap.py)
**Graph OLAP | OpenCypher Query Suite | Cross-DB Benchmarking**

Fixed query-suite benchmark for Stack Overflow graph analytics:
- ArcadeDB remains on synchronous preload ingest for cross-database fairness
- ArcadeDB query execution is Cypher-only in this example path

**Learn:** OLAP graph query benchmarking and directed-edge traversal assumptions

---

### 🔀 [13_stackoverflow_hybrid_queries.py](./13_stackoverflow_hybrid_queries.py)
**Hybrid SQL + Graph + Vector | Standalone Workflow**

Standalone Stack Overflow workflow combining documents, graph edges, embeddings, and
hybrid queries:
- Preload uses synchronous transaction-batched ingest rather than `IMPORT DATABASE`
- Graph edge creation uses RID-based directed endpoints

**Learn:** End-to-end hybrid querying across SQL, OpenCypher, and vector search

---

### 🚚 [15_import_database_vs_transactional_table_ingest.py](./15_import_database_vs_transactional_table_ingest.py)
**Transactional SQL vs Async SQL vs SQL Import | Table Ingest Benchmark**

Synthetic multi-table ingest comparison harness:
- Runs three modes against the same generated dataset shape
- Modes: transactional SQL, async SQL, SQL `IMPORT DATABASE`
- Includes parity checks so final table counts must match before timing results should
  be trusted
- Current outcome is workload-dependent; SQL import can win on some table-heavy shapes

**Learn:** Table-ingest tradeoffs for embedded Python workloads

---

### 🌐 [16_import_database_vs_transactional_graph_ingest.py](./16_import_database_vs_transactional_graph_ingest.py)
**Transactional SQL vs Async SQL vs SQL Import | Graph Ingest Benchmark**

Synthetic graph ingest comparison harness:
- Runs transactional SQL, async SQL, and SQL import on equivalent vertex/edge data
- Includes parity checks on final vertex and edge counts
- Current outcome is workload-dependent; async SQL can outperform SQL import on
  graph-heavy shapes

**Learn:** Graph-ingest tradeoffs for embedded Python workloads

---

## 💡 Tips

- **Run from examples/ directory** - Always execute examples from `bindings/python/examples/` for correct file paths
- **Start with Example 01** - Foundation for all ArcadeDB concepts
- **Use directed graph assumptions** - Current graph examples explicitly define
  `UNIDIRECTIONAL` edge types unless a script says otherwise
- **Database files persist** - Examples preserve data for inspection
- **Output is educational** - Check console output to understand operations
- **Experiment freely** - Examples clean up and recreate on each run

## 🔗 Learn More

- **[ArcadeDB Documentation](https://docs.arcadedb.com/)**
- **[Python API Reference](../docs/api/)**
- **[GitHub Repository](https://github.com/ArcadeData/arcadedb)**

---

*Examples are designed to be self-contained and educational. Each includes detailed comments and step-by-step explanations.*
