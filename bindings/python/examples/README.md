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
**Vertex Types | Edge Types | Graph Traversal | SQL MATCH vs Cypher | NULL Handling**

Complete social network modeling with graph database:
- Creating vertex types (Person) and edge types (FRIEND_OF) with rich properties
- NULL value handling for optional fields (email, phone, reputation)
- Bidirectional relationships with metadata (since, closeness)
- Graph traversal patterns (friends, friends-of-friends, mutual connections)
- Comparing SQL MATCH vs Cypher query languages
- Variable-length path queries (`*1..3`) and graph aggregations
- NULL filtering (IS NULL queries for missing contact info)
- Relationship property queries (closeness filtering)

**Learn:** Graph schema design, relationship modeling, multi-language querying, NULL handling in graphs

**Status:** ✅ Fully functional - 8 people, 24 bidirectional edges, comprehensive queries

---

### 🔍 [03_vector_search.py](./03_vector_search.py) ⚠️ **EXPERIMENTAL**
**Vector Embeddings | HNSW Index | Semantic Search | Performance Analysis**

Semantic similarity search with AI/ML (under active development):
- Vector storage with 384D embeddings (mimicking sentence-transformers)
- HNSW indexing for nearest-neighbor search
- Cosine distance similarity queries
- Index population strategies (batch vs incremental)
- Filtering approaches (oversampling, multiple indexes, hybrid)
- Performance characteristics and best practices

**Learn:** Vector databases, HNSW algorithm, semantic search patterns, index architecture

**Implementation note:** Currently uses jelmerk/hnswlib. Future migration to datastax/jvector planned for better performance.

**Status:** ⚠️ API demonstration - not production-ready yet

---

### 📥 [04_csv_import_documents.py](./04_csv_import_documents.py) ✅ **COMPLETE**
**CSV → Documents | Type Inference | NULL Handling | Index Optimization**

Production-ready CSV import with MovieLens dataset (124,003 records):
- Custom type inference (BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL, STRING)
- Explicit schema definition BEFORE import (best practice)
- NULL value handling across all types (4,920 NULL values)
- Batch processing with `commit_every` parameter (up to 113K records/sec)
- Index creation AFTER import (2-3x faster)
- Performance analysis with statistical validation (10 runs per query)
- Composite indexes for multi-column queries (49x speedup)
- Comprehensive data analysis queries

**Learn:** CSV-to-Document import, type selection, import optimization, index strategy, NULL handling

**Status:** ✅ Fully functional - imports 4 CSV files with comprehensive NULL testing

**Prerequisites:** Run `python download_sample_data.py` first to download MovieLens dataset

---

### 🔗 [05_csv_import_graph.py](./05_csv_import_graph.py) 🚧 **COMING SOON**
**CSV → Vertices/Edges | Graph Import | Relationship Mapping**

Import CSV data as graph structures:
- CSV to Vertices (entities as nodes)
- CSV to Edges (relationships with from/to columns)
- Vertex type inference and schema
- Edge type definition with properties
- Bidirectional edge creation
- Foreign key resolution (movieId → Movie vertex)
- Graph queries on imported data

**Learn:** CSV-to-Graph import, vertex/edge mapping, relationship modeling

**Status:** 🚧 Planned - CSV graph import patterns

---

### 🏗️ 06_ecommerce_multimodel.py *(Coming Soon)*
**Multi-Model | Documents + Graph + Vectors**

Simplified e-commerce demonstration:
- Products as documents with full-text search
- Customer purchase graph
- Product similarity with vectors
- Integrated query patterns

**Learn:** Multi-model architecture, mixed queries

---

### 🌐 07_server_mode_rest_api.py *(Coming Soon)*
**HTTP Server | Studio UI | REST API**

Embedded server with remote access:
- Start ArcadeDB HTTP server
- Access Studio web interface
- REST API integration
- Simultaneous embedded + HTTP access

**Learn:** Server deployment, HTTP API, Studio UI

---

### 🔄 08_acid_transactions_banking.py *(Coming Soon)*
**ACID | Transactions | Concurrency**

Banking operations with guarantees:
- Atomic transfers
- Transaction isolation
- Concurrent access patterns
- Rollback and error handling

**Learn:** Transaction management, data integrity, concurrency

---

### ⚙️ 09_production_patterns.py *(Coming Soon)*
**Best Practices | Error Handling | Performance**

Production deployment patterns:
- Resource management and cleanup
- Thread safety
- Error handling and retries
- JVM memory tuning
- Backup and restore

**Learn:** Production readiness, reliability patterns

---

## 📚 Complete Documentation

For comprehensive guides, API reference, and advanced topics:

**🔗 [Full Python Documentation](../docs/)**

Includes:
- Installation & setup guides
- Complete API reference
- Advanced patterns & best practices
- Performance optimization
- Troubleshooting guides

## 🚀 Getting Started

1. **Install ArcadeDB Python bindings:**
   ```bash
   pip install arcadedb-embedded
   ```

2. **Navigate to examples directory:**
   ```bash
   cd bindings/python/examples
   ```

3. **Run an example:**
   ```bash
   python 01_simple_document_store.py
   ```

4. **Explore the results:**
   - Check `./my_test_databases/` for database files
   - Review output logs for operation details
   - Inspect the code to understand patterns

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
