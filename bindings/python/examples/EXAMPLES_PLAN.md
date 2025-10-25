# ArcadeDB Python Examples - Planning Document

## Target User Personas

1. **Data Scientists**: Using embeddings, vector search, analytics
2. **Backend Developers**: Building APIs, microservices with embedded DB
3. **Graph Analysts**: Working with relationships, networks, social graphs
4. **ETL Engineers**: Importing data from various sources
5. **Application Developers**: Need simple embedded database for desktop/CLI apps

## 10 Practical Examples

### 1. **Getting Started: Simple Document Store** ✅ (Priority: CRITICAL)
**File**: `01_simple_document_store.py`
**Target**: First-time users, application developers
**Concepts**: Database creation, basic CRUD, context managers, transactions
**Why**: Every user needs this - absolute basics, shows Pythonic API
```python
# Create DB, insert documents, query, close
# Like SQLite but with documents
# Perfect for config storage, logs, simple apps
```

### 2. **Graph Basics: Social Network** (Priority: HIGH)
**File**: `02_social_network_graph.py`
**Target**: Graph analysts, developers new to graph DBs
**Concepts**: Vertices, edges, graph traversals, relationships
**Why**: Graph DB is ArcadeDB's strength, shows core value proposition
```python
# Person vertices, FRIEND_OF edges
# Find friends, friends-of-friends
# Shortest path between people
```

### 3. **Vector Search: Semantic Similarity** (Priority: HIGH)
**File**: `03_vector_embeddings_search.py`
**Target**: Data scientists, AI/ML engineers
**Concepts**: Vector storage, HNSW index, nearest neighbor search
**Why**: Hot topic (RAG, LLMs), differentiates from traditional DBs
```python
# Store text embeddings (OpenAI/sentence-transformers)
# Build HNSW index
# Find similar documents by cosine similarity
```

### 4. **CSV Import: Documents** ✅ (Priority: HIGH)
**File**: `04_csv_import_documents.py`
**Target**: ETL engineers, data analysts
**Concepts**: CSV → Documents, type inference, NULL handling, batch processing, index optimization
**Why**: Most common import scenario, production patterns
**Status**: ✅ Complete - MovieLens dataset with 124K records
```python
# CSV → Documents: Product catalog, user data, any tabular data
# Custom type inference (BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL, STRING)
# Explicit schema definition BEFORE import
# NULL value handling (4,920 NULL values)
# Batch processing with commit_every parameter
# Index creation AFTER import (2-3x faster)
```

### 5. **CSV Import: Graph (Vertices & Edges)** 🚧 (Priority: HIGH)
**File**: `05_csv_import_graph.py`
**Target**: Graph analysts, ETL engineers
**Concepts**: CSV → Vertices, CSV → Edges, foreign key resolution, relationship mapping
**Why**: Graph import is different from documents, common migration scenario
**Status**: 🚧 Planned
```python
# CSV → Vertices: People/Users as graph nodes
# CSV → Edges: Relationships with from/to (friendships, follows, ratings)
# Foreign key resolution (movieId → Movie vertex)
# Bidirectional edge creation
# Graph queries on imported data
```

### 6. **Multi-Model: E-commerce System** (Priority: MEDIUM)
**File**: `06_ecommerce_multimodel.py`
**Target**: Backend developers, full-stack engineers
**Concepts**: Documents (products), graph (customers), vectors (similarity), integrated queries
**Why**: Demonstrates multi-model flexibility in simplified form
**Scope**: 200-300 lines (simplified from original complex plan)
```python
# Products as documents with properties
# Customer purchase graph
# Product similarity vectors
# Mixed query patterns (document + graph + vector)
```
**Note**: Kept simple to focus on multi-model concepts, not full e-commerce complexity

### 7. **Server Mode: HTTP API + Studio** (Priority: HIGH)
**File**: `07_server_mode_rest_api.py`
**Target**: Backend developers, DevOps engineers
**Concepts**: Server startup, HTTP API access, Studio UI, remote access
**Why**: Essential for production deployment, debugging with Studio
```python
# Start embedded server
# Access via HTTP REST API
# Show Studio URL for visual debugging
# Both embedded + HTTP access simultaneously
```

### 8. **ACID Transactions & Concurrency: Banking Operations** (Priority: MEDIUM)
**File**: `08_acid_transactions_banking.py`
**Target**: Backend developers, financial systems
**Concepts**: ACID guarantees, rollback, concurrent access, isolation, thread safety
**Why**: Shows database reliability and concurrency patterns
**Scope**: 150-200 lines (simplified to focus on core patterns)
```python
# Bank account transfers (ACID)
# Concurrent transactions from multiple threads
# Rollback on error
# Isolation demonstration
```
**Note**: Combines transactions + concurrency (was originally separate from time series)

### 9. **Production Patterns & Best Practices** (Priority: HIGH)
**File**: `09_production_patterns.py`
**Target**: Backend developers deploying to production
**Concepts**: Error handling, resource cleanup, threading, performance tuning, deployment
**Why**: Critical for production reliability, bridges development to deployment
```python
# Proper context managers
# Thread-safe patterns
# Error handling and retries
# JVM memory tuning
# Database backup/restore
# Health checks and monitoring
```

## Implementation Strategy

### Phase 1: Core Fundamentals (Examples 1-3) ✅ COMPLETE
- Simple document store ✅
- Graph basics ✅
- Vector search ✅ (experimental)

### Phase 2: Import Patterns (Examples 4-5)
- CSV → Documents ✅ (Example 4 complete)
- CSV → Graph 🚧 (Example 5 planned)

### Phase 3: Integration & Advanced (Examples 6-8)
- Multi-model e-commerce (simplified showcase) 🚧
- Server mode (essential for production) 🚧
- ACID + concurrency (reliability patterns) 🚧

### Phase 4: Production Readiness (Example 9)
- Production patterns (deployment best practices) 🚧

## Documentation Structure

Each example should have:

1. **Code file** (`examples/0X_name.py`):
   - Clear comments explaining each step
   - Self-contained (runs without other files)
   - Includes sample data generation
   - Cleanup at the end
   - ~100-300 lines (simplified examples at lower end)

2. **Documentation page** (`docs/examples/0X_name.md`):
   - What this example demonstrates
   - Real-world use case
   - Key concepts explained
   - Full code with detailed explanations
   - Expected output
   - "Try it yourself" modifications
   - Link to related API docs

3. **MkDocs navigation** (`mkdocs.yml`):
   - New "Examples" section in nav
   - Ordered by complexity
   - Tagged by user persona

## Success Criteria

- ✅ Each example runs without errors
- ✅ Examples are self-contained (no external dependencies except arcadedb)
- ✅ Clear progression from simple to advanced
- ✅ Cover all major features (document, graph, vector, import)
- ✅ Real-world use cases users can adapt
- ✅ Proper resource cleanup (no leaked DB handles)
- ✅ Documented in MkDocs with explanations

## Notes

- Focus on **thin wrapper** nature - we're wrapping Java APIs, not reimplementing
- Show **Java interop** where relevant (e.g., when Java objects are exposed)
- Emphasize **embedded** benefits - no network, fast, self-contained
- Include **performance tips** specific to JPype/JVM integration
- Make examples **copy-pasteable** for quick starts

## Related Test Files (for reference)

- `test_core.py` - Basic operations, transactions, graph, vector
- `test_server.py` - Server mode, HTTP API
- `test_importer.py` - CSV, JSON, Neo4j import
- `test_concurrency.py` - Threading, file locking
- `test_server_patterns.py` - Access patterns, embedded vs HTTP
- `test_gremlin.py` - Gremlin query language (full distribution)
