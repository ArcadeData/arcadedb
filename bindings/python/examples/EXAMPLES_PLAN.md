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
**Status**: ✅ Complete - MovieLens dataset with 124K records, automatic dataset download
```python
# CSV → Documents: Product catalog, user data, any tabular data
# Automatic dataset download (--size small|large)
# Custom type inference (BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL, STRING)
# Explicit schema definition BEFORE import
# NULL value handling (4,920 NULL values)
# Batch processing with commit_every parameter
# Index creation AFTER import (2-3x faster)
```

### 5. **CSV Import: Graph (Vertices & Edges)** 🚧 (Priority: HIGH)
**File**: `05_csv_import_graph.py`
**Target**: Graph analysts, ETL engineers
**Concepts**: CSV → Vertices, CSV → Edges, foreign key resolution, relationship mapping, graph traversal
**Why**: Graph import is different from documents, common migration scenario
**Status**: 🚧 Planned
**Dataset**: MovieLens (reuses data from Example 04)
```python
# CSV → Vertices: Movie and User nodes from MovieLens dataset
# CSV → Edges: User RATED Movie relationships (from ratings.csv)
# CSV → Edges: Movie HAS_TAG Tag relationships (from tags.csv)
# Foreign key resolution (userId → User vertex, movieId → Movie vertex)
# Edge properties (rating score, timestamp)
# Graph queries: "Users who rated similar movies", "Movie recommendations"
```
**Note**: Builds on Example 04's dataset, showing how same data can be modeled as graph

### 6. **Vector Search: Movie Recommendations** 🚧 (Priority: MEDIUM)
**File**: `06_vector_search_movies.py`
**Target**: Data scientists, AI/ML engineers
**Concepts**: Document embeddings, vector similarity, semantic search on real data
**Why**: Practical vector search application using familiar dataset
**Status**: 🚧 Planned
**Dataset**: MovieLens (reuses data from Example 04)
```python
# Generate embeddings from movie titles + genres
# Store vectors in ArcadeDB with metadata
# Find similar movies using HNSW index
# Combine vector similarity with rating data
# "Movies similar to X that users also liked"
```
**Note**: Demonstrates vector search with real-world use case (recommendations)

### 7. **Multi-Model: Stack Overflow Q&A** 🚧 (Priority: HIGH)
**File**: `07_stackoverflow_multimodel.py`
**Target**: Backend developers, full-stack engineers, data scientists
**Concepts**: Documents + Graph + Vectors in one system, complex queries, multi-model integration
**Why**: Comprehensive showcase of ArcadeDB's multi-model capabilities with rich dataset
**Status**: 🚧 Planned
**Dataset**: Stack Exchange data dump (Python/JavaScript tags, converted from XML to CSV)
**Scope**: 300-400 lines (comprehensive multi-model example)
```python
# Documents: Questions/Answers with full text, searchable content
# Graph: User → ASKED/ANSWERED → Question, Question → TAGGED_WITH → Tag
# Vectors: Question embeddings for duplicate detection
# Multi-model queries:
#   - "Find Python experts" (graph traversal + aggregation)
#   - "Similar unanswered questions" (vector + document filtering)
#   - "Trending topics by tag relationships" (graph analytics)
# XML → CSV conversion (provide script or use existing tools)
```
**Dataset Options**:
- Stack Exchange data dump (8-10 CSV files after conversion)
- Alternative: E-commerce dataset (products, reviews, customers)
- Decision: To be determined based on conversion ease and dataset availability
**Note**: Largest example, demonstrates all three models working together

### 8. **Server Mode: HTTP API + Studio** (Priority: HIGH)
**File**: `08_server_mode_rest_api.py`
**Target**: Backend developers, DevOps engineers
**Concepts**: Server startup, HTTP API access, Studio UI, remote access, embedded + HTTP simultaneously
**Why**: Essential for production deployment, debugging with Studio, remote access patterns
**Status**: 🚧 Planned
```python
# Start embedded ArcadeDB server
# Access via HTTP REST API (queries, commands)
# Open Studio web interface for visual debugging
# Demonstrate both embedded + HTTP access to same database
# Show server configuration and security settings
# Use databases from previous examples (01-07) for demonstration
```
**Note**: Final example - shows production deployment patterns

## Implementation Strategy

### Phase 1: Core Fundamentals (Examples 1-3) ✅ COMPLETE
- Simple document store ✅
- Graph basics ✅
- Vector search ✅ (experimental)

### Phase 2: Import Patterns with MovieLens (Examples 4-6)
- CSV → Documents ✅ (Example 4 complete - MovieLens dataset)
- CSV → Graph 🚧 (Example 5 planned - MovieLens as graph)
- Vector search on documents 🚧 (Example 6 planned - MovieLens movie similarity)

### Phase 3: Multi-Model Integration (Example 7)
- Stack Overflow Q&A multi-model 🚧 (Documents + Graph + Vectors)
- Alternative dataset options available

### Phase 4: Production Deployment (Example 8)
- Server mode & HTTP API 🚧 (Production patterns, Studio UI)

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
- `test_gremlin.py` - Gremlin query language
