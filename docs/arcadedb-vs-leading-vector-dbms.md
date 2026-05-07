# ArcadeDB vs Qdrant and Milvus - Vector Search Comparison

> Honest, side-by-side analysis of ArcadeDB against the two leaders in the
> open-source vector database space (Qdrant and Milvus) for vector and hybrid
> search workloads. Triggered by community discussion
> [#4044](https://github.com/ArcadeData/arcadedb/discussions/4044) and the
> follow-up issues
> [#4065](https://github.com/ArcadeData/arcadedb/issues/4065),
> [#4066](https://github.com/ArcadeData/arcadedb/issues/4066),
> [#4067](https://github.com/ArcadeData/arcadedb/issues/4067),
> [#4068](https://github.com/ArcadeData/arcadedb/issues/4068).
>
> Last updated: 2026-05-06 (ArcadeDB main, Qdrant 1.16.x, Milvus 2.5.x).

**Recent updates**

- 2026-05-06 - **#4087 partition pruning shipped** ([#4119](https://github.com/ArcadeData/arcadedb/pull/4119)).
  `SelectExecutionPlanner.derivePartitionPrunedClusters` stashes a partition-pruned bucket set on
  `CommandContext`; `SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint` consumes it
  before invoking the per-bucket sub-index walk on both `vector.neighbors` and
  `vector.sparseNeighbors`. A query like `SELECT vector.sparseNeighbors('Doc[embedding]', :q, k)
  FROM Doc WHERE tenant_id = 'X'` on a `partitioned('tenant_id')` type now hits exactly one
  bucket's segment set instead of fanning out across every bucket. Without a partition-key WHERE,
  behavior is unchanged (full fan-out). This closes the bulk of the "partition key for
  multi-tenancy" gap (P2) for the most performance-sensitive workload (vector retrieval).
- 2026-05-05 - **Bolt protocol coverage extended** ([#4079](https://github.com/ArcadeData/arcadedb/pull/4079)).
  `BoltNetworkExecutor` now recognizes `LSM_SPARSE_VECTOR` so Neo4j-driver clients can read the
  new index alongside the dense `LSM_VECTOR`. No format changes; pure protocol surface.
- 2026-05-05 - **#4068 shipped and closed** (sparse-vector-v2 branch). The MVP
  composite-key wrapper has been deleted; the `LSM_SPARSE_VECTOR` index is now
  served by a purpose-built `.sparseseg` page-component segment format with
  BlockMax-WAND DAAT, VarInt-delta RID compression, INT8/FP16/FP32 weight
  quantization, size-tiered auto-compaction, and full HA replication via
  `runWithCompactionReplication`. End-to-end benchmark on `LSMSparseVectorIndexLargeBenchmark`
  lands at **10M docs / 30k-dim SPLADE-shape: 62.98 ms/query vs 48,299 ms brute force = 766.9x speedup**
  (post `payloadScratch` sizing fix; matches the design doc's size-tiered baseline within
  ~7%). Per-segment parallel scoring (Step 5) deferred to follow-up
  [#4085](https://github.com/ArcadeData/arcadedb/issues/4085) and decode-buffer pooling to
  [#4086](https://github.com/ArcadeData/arcadedb/issues/4086) - the serial path is
  already inside network noise for any real workload, so the dedicated
  `SparseVectorScoringPool` is allocated and instrumented but the dispatch
  wiring is intentionally not in the v2 PR.
- 2026-05-03 - **#4065, #4066, #4067 shipped together** (sparse-vector-index branch). The
  full Qdrant migration use case from discussion
  [#4044](https://github.com/ArcadeData/arcadedb/discussions/4044) is now a single SQL
  statement:
  ```sql
  SELECT expand(`vector.fuse`(
      `vector.neighbors`('Doc[dense]', :denseVec, 50),
      `vector.sparseNeighbors`('Doc[tokens,weights]', :qIdx, :qVal, 50),
      { fusion: 'RRF', groupBy: 'source_file', groupSize: 1 }
  ))
  ```
  - **#4065**: native `LSM_SPARSE_VECTOR` index, posting-list inverted structure on the
    LSM-Tree backbone with composite key `(int dim_id, RID rid, float weight)`,
    document-at-a-time WAND scoring with per-dim `max_weight` upper bounds, IDF modifier
    (Robertson-Sparck-Jones), `vector.sparseNeighbors` SQL function, Studio UI.
  - **#4066**: `vector.fuse(source1, source2, ..., [options])` server-side hybrid fusion
    operator with three strategies (RRF default, DBSF, LINEAR), composes with
    `vector.neighbors`, `vector.sparseNeighbors`, `SEARCH_INDEX`, plain `SELECT ... ORDER BY`.
  - **#4067**: `groupBy` + `groupSize` options on `vector.neighbors` and
    `vector.sparseNeighbors`, mirroring Qdrant's `query_points_groups` semantics.
  - The MVP composite-key storage shipped here was treated as throwaway and replaced
    by the v2 page-component format under
    [#4068](https://github.com/ArcadeData/arcadedb/issues/4068) (now closed).

---

## TL;DR

ArcadeDB, Qdrant, and Milvus solve different problems and live in different
parts of the design space.

- **Qdrant** is a purpose-built vector database with deep specialisation in
  similarity search, dense+sparse hybrid retrieval, payload-aware filtering,
  and a clean Query API for fusion and multi-stage retrieval. Sweet spot:
  vector-first workloads in the 1M-1B range with rich filter pushdown.
- **Milvus** is the most "industrial scale" vector database in the
  open-source landscape. Its design assumption is that vectors are the only
  workload that matters, and a deployment will eventually need GPU indexing,
  storage-compute separation, and tens of billions of vectors across a
  Kubernetes cluster. Wide catalogue of vector index types (HNSW, HNSW_SQ,
  HNSW_PQ, HNSW_PRQ, IVF_*, DiskANN, SCANN, GPU_CAGRA, GPU_IVF_*, sparse
  inverted, BM25-as-sparse, etc.) and a coordinator/worker distributed
  architecture. Sweet spot: vector-first, GPU-friendly, >100M vectors.
- **ArcadeDB** is a multi-model DBMS (Graph, Document, KV, Time Series,
  Search) with a strong native vector index and ACID transactions, where
  embeddings are first-class citizens of the SQL/Cypher/Gremlin engine.
  Sweet spot: vectors plus relationships, plus ACID writes, plus multiple
  query languages and wire protocols, in one process.

If your workload is "store and search billions of embeddings, fast" with
nothing else attached, Qdrant or Milvus are the more focused tools (Qdrant
for clean retrieval semantics, Milvus for raw scale). If your workload is
"vectors plus graph traversal, plus relational joins, plus full-text, plus
ACID writes, in one engine", neither vector specialist has a counterpart -
you would otherwise glue Qdrant/Milvus + Postgres + Neo4j + Elasticsearch
together, which is exactly the integration cost ArcadeDB removes.

This document makes ArcadeDB's gaps explicit and ranks them so contributors
can see what to work on next.

---

## 1. Side-by-side feature matrix

Legend: ✅ Full, ⚠️ Partial, ❌ Missing.

### 1.1 Vector index types

| Index family                                        | ArcadeDB                                         | Qdrant                                       | Milvus 2.5                                   |
|-----------------------------------------------------|--------------------------------------------------|----------------------------------------------|----------------------------------------------|
| Dense vector index: HNSW                            | ✅ via JVector (`LSMVectorIndex`)                | ✅ native HNSW                               | ✅                                           |
| HNSW + scalar quantization (HNSW_SQ)                | ⚠️ INT8 quantization on HNSW path                | ✅                                           | ✅                                           |
| HNSW + product quantization (HNSW_PQ / PRQ)         | ✅ PQ via JVector                                | ⚠️ scalar+binary preferred                   | ✅ HNSW_PQ + HNSW_PRQ                        |
| FLAT (brute force) as a first-class index           | ⚠️ via SQL `vector.cosineSimilarity()` scan      | ⚠️ via "exact" search flag                   | ✅                                           |
| IVF_FLAT                                            | ❌                                               | ❌                                           | ✅                                           |
| IVF_SQ8                                             | ❌                                               | ❌                                           | ✅                                           |
| IVF_PQ                                              | ❌                                               | ❌                                           | ✅                                           |
| DiskANN (Vamana, on-disk)                           | ❌                                               | ❌                                           | ✅                                           |
| SCANN                                               | ❌                                               | ❌                                           | ✅                                           |
| GPU_BRUTE_FORCE                                     | ❌                                               | ❌                                           | ✅                                           |
| GPU_IVF_FLAT / GPU_IVF_PQ                           | ❌                                               | ❌                                           | ✅                                           |
| GPU_CAGRA (NVIDIA cuVS)                             | ❌                                               | ⚠️ in roadmap                                | ✅                                           |
| HNSW mutability (incremental insert/delete)         | ⚠️ with rebuild thresholds                       | ✅ continuous                                | ✅                                           |
| Sparse vector index (inverted / WAND)               | ✅ `LSM_SPARSE_VECTOR` + BlockMax-WAND DAAT, IDF modifier, INT8/FP16/FP32 quantization, size-tiered auto-compaction ([#4065](https://github.com/ArcadeData/arcadedb/issues/4065) + [#4068](https://github.com/ArcadeData/arcadedb/issues/4068) shipped) | ✅ inverted index, IDF modifier | ✅ SPARSE_INVERTED_INDEX + SPARSE_WAND       |
| Multi-vector / ColBERT (late-interaction MaxSim)    | ⚠️ scoring functions only, no native index       | ✅ MaxSim native                             | ✅ MAX_SIM_* metrics + native multivector    |
| Binary vector index (BIN_FLAT / BIN_IVF_FLAT)       | ⚠️ BINARY quantization, not native binary type   | ✅ binary quantization                       | ✅ first-class binary type                   |

### 1.2 Vector data types and distances

| Capability                                          | ArcadeDB                                         | Qdrant                                       | Milvus 2.5                                   |
|-----------------------------------------------------|--------------------------------------------------|----------------------------------------------|----------------------------------------------|
| Float32 vectors                                     | ✅                                               | ✅                                           | ✅                                           |
| Float16 vectors                                     | ❌                                               | ❌                                           | ✅                                           |
| BFloat16 vectors                                    | ❌                                               | ❌                                           | ✅                                           |
| INT8 vector type (pre-quantized ingest)             | ⚠️ INT8 quantization, not ingest type            | ✅                                           | ✅                                           |
| Scalar (INT8) quantization at index build           | ✅                                               | ✅                                           | ✅                                           |
| Binary quantization (1-bit)                         | ✅                                               | ✅                                           | ✅                                           |
| Product quantization (PQ)                           | ✅                                               | ⚠️ scalar+binary preferred                   | ✅                                           |
| Sparse vectors data type                            | ✅ (`SparseVector`, in-memory map)               | ✅                                           | ✅                                           |
| Distance: cosine                                    | ✅                                               | ✅                                           | ✅                                           |
| Distance: euclidean (L2)                            | ✅                                               | ✅                                           | ✅                                           |
| Distance: dot product / inner product               | ✅                                               | ✅                                           | ✅                                           |
| Distance: manhattan (L1)                            | ✅ via `vector.l1Distance` / `vector.manhattanDistance` SQL functions (brute-force only - no L1 index)   | ✅                                           | ❌                                           |
| Distance: hamming (binary)                          | ❌                                               | ❌                                           | ✅                                           |
| Distance: jaccard (binary)                          | ❌                                               | ❌                                           | ✅                                           |

### 1.3 Hybrid retrieval and result shaping

| Capability                                          | ArcadeDB                                         | Qdrant                                       | Milvus 2.5                                   |
|-----------------------------------------------------|--------------------------------------------------|----------------------------------------------|----------------------------------------------|
| Multiple named vectors per record                   | ⚠️ multiple indexes, not first-class             | ✅                                           | ✅ up to 10 per collection                   |
| Built-in BM25 (text -> sparse vector function)      | ⚠️ Lucene-based full-text, not unified           | ⚠️ via sparse vectors                        | ✅ `Function` schema, unified hybrid         |
| Server-side hybrid fusion (RRF, DBSF, weighted)     | ✅ `vector.fuse(...)` with RRF / DBSF / LINEAR ([#4066](https://github.com/ArcadeData/arcadedb/issues/4066) shipped) | ✅ Query API | ✅ `hybrid_search()` + `WeightedRanker` / `RRFRanker` |
| Multi-stage retrieval (prefetch + rerank)           | ✅ via `vector.rerank(source, query, embeddingProperty, k)` (declarative)               | ✅                                           | ✅                                           |
| Score boosting / formula reranking                  | ✅ via `vector.boost(source, { boosts: [{field, weight}, ...] })`                  | ✅                                           | ⚠️ via custom Function / external           |
| Group-by in vector search                           | ✅ `groupBy` / `groupSize` options on `vector.neighbors` and `vector.sparseNeighbors` ([#4067](https://github.com/ArcadeData/arcadedb/issues/4067) shipped); also exposed on `vector.fuse` post-fusion | ✅                          | ✅                                           |
| MMR (Maximal Marginal Relevance / diversity)        | ✅ via `vector.mmr(source, embeddingProperty, {lambda, k})` | ✅                                           | ⚠️ via custom Function / external            |
| Recommendation API (positive/negative examples)     | ✅ via `vector.recommend(indexSpec, positives, negatives, k[, opts])` | ✅                                           | ⚠️ build via filter + vector ops             |
| Discovery / context search API                      | ✅ via `vector.discover(indexSpec, pairs, k)` (re-rank by per-pair margin sum) | ✅                                           | ❌                                           |
| Range search (radius / score threshold)             | ✅ via `maxDistance` (dense) / `minScore` (sparse) options on the neighbors functions  | ✅                                           | ✅ first-class param                         |
| Search iterator (large result sets)                 | ⚠️ via SQL paging / streaming                    | ✅                                           | ✅ first-class iterator                      |

### 1.4 Filtering and payload indexes

| Capability                                          | ArcadeDB                                         | Qdrant                                       | Milvus 2.5                                   |
|-----------------------------------------------------|--------------------------------------------------|----------------------------------------------|----------------------------------------------|
| Boolean filter expressions on payload               | ✅ via SQL/Cypher                                | ✅                                           | ✅ purpose-built syntax                      |
| Pre-filtering during graph traversal                | ⚠️ `RIDBitsFilter`, post-filter by JVector       | ✅ filterable HNSW + ACORN                   | ✅ filtered HNSW with attr-aware             |
| Geo payload index                                   | ✅ via `LSMTreeGeoIndex` (Lucene spatial4j prefix-tree)  | ✅                                           | ⚠️ via scalar index                          |
| UUID payload index                                  | ❌                                               | ✅                                           | ⚠️ via scalar index                          |
| Datetime range payload index                        | ⚠️ generic range index                           | ✅                                           | ⚠️ via scalar index                          |
| Tenant index (multi-tenant locality)                | ❌                                               | ✅                                           | ⚠️ via partition key                         |
| Principal index (sorted-field locality)             | ❌                                               | ✅                                           | ❌                                           |
| Partition key / payload-based partitioning          | ⚠️ schema-level buckets + query-time pruning on `vector.neighbors` / `vector.sparseNeighbors` ([#4119](https://github.com/ArcadeData/arcadedb/pull/4119) shipped) | ✅                                           | ✅ first-class                               |
| Dynamic field / schemaless payload                  | ⚠️ requires schema or `*` properties             | ✅                                           | ✅                                           |
| JSON field with path index                          | ⚠️ JSON property, no path index                  | ✅                                           | ✅ JSON path + JSON flat index               |
| Array field with index                              | ⚠️ list property                                 | ✅                                           | ✅                                           |
| Bitmap scalar index (low cardinality)               | ❌                                               | ❌                                           | ✅                                           |
| Inverted scalar index                               | ⚠️ via Lucene full-text                          | ✅                                           | ✅                                           |

### 1.5 Operational, API, and durability

| Capability                                          | ArcadeDB                                         | Qdrant                                       | Milvus 2.5                                   |
|-----------------------------------------------------|--------------------------------------------------|----------------------------------------------|----------------------------------------------|
| ACID transactions on vector writes                  | ✅                                               | ⚠️ WAL, no general ACID                      | ❌ eventually consistent                     |
| Persistent on-disk vector index                     | ✅ page-based, transactional                     | ✅ memmap-based                              | ✅                                           |
| HA replication                                      | ✅ Raft                                          | ✅ Raft                                      | ✅                                           |
| Cloud-native horizontal sharding                    | ❌ (per-bucket within a node)                    | ✅                                           | ✅                                           |
| Storage-compute separation                          | ❌                                               | ❌                                           | ✅                                           |
| Custom shard key (user-defined sharding)            | ❌                                               | ✅                                           | ✅                                           |
| Snapshots / backup                                  | ✅ general backup                                | ✅ per-collection                            | ✅                                           |
| Collection aliases (zero-downtime swap)             | ❌                                               | ✅                                           | ✅                                           |
| Embedded mode                                       | ✅ JVM in-process                                | ❌                                           | ⚠️ Milvus Lite (Python only)                 |
| Standalone single-binary mode                       | ✅                                               | ✅                                           | ✅ Standalone                                |
| GPU-accelerated index build                         | ❌                                               | ✅                                           | ✅                                           |
| HTTP/REST API for vector ops                        | ⚠️ via generic `/sql` and `/command`             | ✅ purpose-built endpoints                   | ✅                                           |
| gRPC API for vector ops                             | ⚠️ generic gRPC wire, no vector-specific API     | ✅                                           | ✅                                           |
| Strict mode / collection-level guardrails           | ⚠️ schema-level constraints                      | ✅                                           | ⚠️ per-collection knobs                      |

---

## 2. Where ArcadeDB beats both Qdrant and Milvus

The features below have no equivalent in either Qdrant or Milvus. They are
the reason a user would pick ArcadeDB even if a few pure-vector capabilities
are missing:

- **Multi-model storage**: vectors share the same database, transaction, and
  schema as graph vertices/edges, JSON documents, time-series, and key/value.
- **Native graph engine**: vector hits are vertices that can be traversed via
  `out()`, `in()`, `shortestPath()`, etc., in the same query. Critical for
  GraphRAG, agent memory graphs, and RAG-with-citations.
- **Full ACID transactions** spanning vectors, graph, and documents in a
  single commit. Qdrant offers eventual consistency with WAL durability per
  point; Milvus is eventually consistent across the distributed pipeline.
  ArcadeDB can be used as a system of record.
- **Multiple query languages**: SQL, Cypher, Gremlin, GraphQL, MongoDB query
  language, Polyglot scripting (GraalVM). Vectors are accessible from all
  of them. Qdrant exposes the Query API + SDKs; Milvus exposes its own
  filter expression DSL + SDKs.
- **Multiple wire protocols**: HTTP/JSON, PostgreSQL, MongoDB, Redis, Neo4j
  Bolt, gRPC. Drop-in replacement for several existing systems.
- **Embedded JVM mode**: runs in-process inside any Java application, no
  separate service. Milvus Lite is Python-only and explicitly not for
  production; Qdrant has no embedded mode at all.
- **Lucene-backed full-text search** (analyzers, tokenizers, stemmers)
  integrated with the same record store. Qdrant has lighter analyzers;
  Milvus 2.5 implements BM25 via sparse vectors with a built-in tokenizer
  and is less flexible than Lucene.
- **Schema-level constraints**, indexes, mandatory properties, type
  hierarchy. Qdrant collections and Milvus collections are flatter.
- **Apache 2.0 license**, single-binary distribution, no Pulsar / Kafka /
  etcd / MinIO dependency stack. Milvus Standalone packages these in
  Docker; Distributed requires operating them.

For RAG, agentic, and GraphRAG workloads where retrieved chunks have
semantic relationships (citations, dependencies, ownership, authorship),
ArcadeDB's graph + vector + document combination removes a layer of
integration that neither vector specialist alone can.

---

## 3. Gap analysis - features missing from ArcadeDB

Each gap is scored on three axes:

- **Priority** (P0..P3): how important it is to close, weighing user demand,
  competitive pressure, and how often it appears in real workloads.
- **Risk** (Low / Medium / High): cost of NOT having it. "High" means we
  routinely lose evaluations or block migrations because of it.
- **Popularity** (Low / Medium / High): how common this feature is in
  typical Qdrant or Milvus usage today.

For each gap, the **Source** column says where the comparable feature lives:
"Q" for Qdrant, "M" for Milvus, "Q+M" for both. Implementation effort is
intentionally a non-axis: ranking is by user value first.

### 3.1 P0 - Must close to be a credible vector DB alternative

#### ~~Indexed sparse vectors (inverted index)~~ - shipped
- **Source:** Q+M - **Priority:** P0 - **Risk:** High - **Popularity:** High
- **Status:** **Implemented in [#4065](https://github.com/ArcadeData/arcadedb/issues/4065).**
  Native `LSM_SPARSE_VECTOR` index, posting-list inverted structure on the
  LSM-Tree backbone, document-at-a-time WAND scoring with per-dim
  `max_weight` upper bounds, IDF modifier (Robertson-Sparck-Jones),
  `vector.sparseNeighbors` SQL function, Studio UI for index creation.
  Inherits ACID, WAL, replication, and compaction from the LSM-Tree
  backbone. Scaling work past ~10M sparse vectors (BlockMax-WAND with
  per-page bounds, weight quantization, size-tiered auto-compaction) shipped
  in [#4068](https://github.com/ArcadeData/arcadedb/issues/4068) (closed); the
  per-segment parallel scoring deliverable is tracked separately in
  [#4085](https://github.com/ArcadeData/arcadedb/issues/4085).
- **Why it mattered:** Sparse vectors (BM25-like, SPLADE, BGE-M3) are how
  modern hybrid retrieval handles exact-term and OOV matches that dense
  embeddings miss. The discussion #4044 use case ("freeze card after theft"
  vs "unfreeze card after travel") collapses to identical dense embeddings;
  only sparse can separate them.

#### ~~Server-side hybrid search (single-call fusion with rerankers)~~ - shipped
- **Source:** Q+M - **Priority:** P0 - **Risk:** High - **Popularity:** High
- **Status:** **Implemented in [#4066](https://github.com/ArcadeData/arcadedb/issues/4066).**
  New SQL function `vector.fuse(source1, source2, ..., sourceN [, options])` accepts any
  sub-pipeline yielding `(@rid, $score)` rows. Three fusion strategies wired:
  - `RRF` (default) with configurable `k` and per-source `weights`,
  - `DBSF` (Qdrant 1.11+ mean ± 3σ normalization),
  - `LINEAR` (per-source min-max normalization).
  Composes with `vector.neighbors`, `vector.sparseNeighbors`, `SEARCH_INDEX`, and any plain
  `SELECT ... ORDER BY ... LIMIT N`. Auto-flips `distance` → similarity at extract time so
  dense and sparse sources compose without manual rescaling.
- **Why it mattered:** Hybrid (dense + sparse + optional reranker) is the default retrieval
  pattern for modern RAG. Forcing two round-trips plus client-side fusion added latency,
  complicated clients, and prevented prefetch-style planner optimisations.

#### ~~Group-by in vector search~~ - shipped
- **Source:** Q+M - **Priority:** P0 - **Risk:** Medium-High - **Popularity:** High
- **Status:** **Implemented in [#4067](https://github.com/ArcadeData/arcadedb/issues/4067).**
  `groupBy` + `groupSize` options on both `vector.neighbors` and `vector.sparseNeighbors`,
  with the same options exposed at the fusion layer in `vector.fuse`. Defaults: `groupSize=1`.
  The third positional arg of `vector.neighbors` becomes the max number of *distinct groups*
  when `groupBy` is set (mirroring Qdrant's `query_points_groups`). Composes with the existing
  `filter` option. MVP uses post-traversal grouping with adaptive over-fetch (`k * groupSize * 5`);
  integration into the HNSW traversal proper is a follow-up.
- **Why it mattered:** Document-chunk RAG is the canonical example: many chunks per document,
  want top-K *documents* with the best chunk per document. Native grouping pushes the dedup
  out of the application code.

#### Built-in BM25 / full-text-as-sparse-vector
- **Source:** M - **Priority:** P0 - **Risk:** High - **Popularity:** High
- **Status:** ArcadeDB has Lucene full-text and (now) sparse vectors, but
  they are still separate query paths. Milvus 2.5 unifies full-text and
  vector by storing BM25 weights as a sparse vector and reranking together
  with dense via the same `hybrid_search` call.
- **Why it matters:** This is the model the industry is converging on
  (Vespa, Weaviate, Elastic followed similar paths). It removes the need
  for users to glue full-text + vector results manually and lets the
  planner optimise across both. The building blocks now exist (Lucene
  + `LSM_SPARSE_VECTOR`); they need to be unified.
- **Design suggestion:** Expose a schema-level `Function` (Milvus
  terminology) that converts a text property into a sparse vector and
  registers a sparse index, then make this compose with the hybrid search
  construct from the previous item.

### 3.2 P1 - Closes major competitive gaps

#### ~~Sparse-index scaling: BlockMax-WAND, quantization, parallel segments~~ - shipped (parallel scoring deferred)
- **Source:** Q+M - **Priority:** P1 - **Risk:** Medium-High - **Popularity:** Medium-High
- **Status:** **Shipped via [#4068](https://github.com/ArcadeData/arcadedb/issues/4068) (closed).**
  The MVP composite-key wrapper has been deleted; the new backend is the only
  `LSM_SPARSE_VECTOR` storage, with per-transaction durability via the regular
  ArcadeDB page WAL (each segment is a `PaginatedComponent` so flush + compaction
  land inside normal transactions). Benchmark scaling on
  `LSMSparseVectorIndexBenchmark` / `LSMSparseVectorIndexLargeBenchmark`:
  - **100k corpus, 5k vocab, 30 nnz/doc**: index 8.21 ms vs brute force 670 ms = 82x
  - **1M corpus, 30k vocab**: index **10.09 ms** vs brute force 4885 ms = **484x**
  - **10M corpus, 30k vocab**: 2026-05-05 rerun (post `payloadScratch` sizing fix +
    today's reliability hardening) lands at **62.98 ms** vs brute force 48,299 ms
    = **766.9x**, with bulk-load 465 s. Size-tiered baseline (Phase 5 step 9) was
    59.01 ms / 829x with bulk-load 503 s; today is +7% on latency, -7.5% on speedup
    ratio, -7.5% on bulk-load - acceptable cost for the day's reliability fixes.
    An earlier "1.20 ms / 41,613x" measurement was withdrawn: it came from the
    silent-failure path where high-vocab flushes overflowed `payloadScratch`, the
    orphan-drop in `flush()` swallowed the failure, and the index path scanned a
    small in-memory memtable instead of real on-disk segments.
  - The size-tiered auto-compaction gate (design doc Phase 5 step 9) caps
    steady-state segment count at `(fanout - 1) * log_fanout(N / base)`
    (≈5 at 10M, ≈15 at 1B) and amortizes write amplification at
    `O(log_fanout(N / base))` per posting, which is the LSM-Tree STCS
    sweet spot. An earlier count-tiered policy ran 764 s for the same
    10M bulk-load; switching to size-tiered cut that by 34%.

  Storage drops from a composite `(dim, RID, weight)` LSM-Tree key to a
  purpose-built `.sparseseg` format with VarInt-delta RID compression and
  int8 weight quantization (3.41 bytes per posting on a 10M-posting
  roundtrip). Per-block `max_weight` + skip-list metadata drive BlockMax-WAND
  DAAT pruning. HA replication is end-to-end: the engine's flush +
  compaction run inside `database.getWrappedDatabaseInstance().runWithCompactionReplication(...)`,
  the same hook `LSMTreeIndex` uses; new `SparseSegmentComponent` files (and
  retired ones during compaction) ride a Raft `SCHEMA_ENTRY` with a synthetic
  page-WAL so followers materialize the file atomically with the leader's
  commit, validated by `RaftSparseVectorReplicationIT` (3-server, every
  server must serve top-K). Backup integration is inherited from
  `PaginatedComponent` for free. **Per-segment parallel scoring** is
  deferred to follow-up [#4085](https://github.com/ArcadeData/arcadedb/issues/4085)
  - the serial 10M number is already well inside the noise of network +
  JSON-serialization overhead, so the absolute gain from parallelism is
  small. The dedicated `SparseVectorScoringPool` is allocated and
  instrumented (Studio "Executor Pools" card + Micrometer gauges) so the
  dispatch is a one-line change once #4085 lands.

#### Filterable HNSW (graph-aware pre-filtering)
- **Source:** Q+M - **Priority:** P1 - **Risk:** High - **Popularity:** High
- **Status:** Today we have a `RIDBitsFilter` Bits implementation; JVector
  treats it as a per-point skip during traversal. Qdrant builds payload-aware
  subgraphs and merges them, plus the v1.16 ACORN algorithm explores
  second-hop neighbours when direct neighbours are filtered out. Milvus
  pushes Boolean expressions into the index traversal.
- **Why it matters:** Strict filters (e.g., `tenant_id = X AND lang = 'en' AND status = 'active'`)
  collapse recall on a vanilla HNSW because most candidates get skipped
  before reaching K. The #1 production complaint about vector DBs.
- **Design suggestion:** Investigate ACORN integration in JVector (already
  on their roadmap). Independently, payload-aware index sub-partitioning
  per high-cardinality filter field (cf. Qdrant tenant index).

#### Named vectors / multiple embedding spaces per record
- **Source:** Q+M - **Priority:** P1 - **Risk:** Medium-High - **Popularity:** High
- **Status:** Achievable today by creating multiple indexes on multiple
  properties, but the model is not first-class in SQL syntax and there's
  no shared point identity for "title vector + body vector + image vector".
  Milvus allows up to 10 vector fields per collection with independent
  index types and per-field metrics; Qdrant has named vectors as a
  collection concept.
- **Why it matters:** Common pattern: each item has dense (English),
  multilingual, sparse, and image embeddings. Querying together with hybrid
  fusion needs first-class support to be ergonomic. Pre-requisite for
  ColBERT and recommendation APIs.

#### Multi-vector / ColBERT native index (late interaction)
- **Source:** Q+M - **Priority:** P1 - **Risk:** Medium - **Popularity:** Medium-High
- **Status:** `SQLFunctionMultiVectorScore` provides MAX/AVG/MIN/WEIGHTED
  fusion at scoring time, but there's no native MaxSim index for token-level
  vectors. Qdrant exposes `multivector_config`; Milvus exposes native
  `MAX_SIM_*` metrics.
- **Why it matters:** ColBERT/ColPali late-interaction models give
  significantly higher recall than single-vector dense for long-form text
  and document images. Becoming a default for high-quality RAG.

#### True distributed sharding / horizontal scale-out
- **Source:** M (primary) / Q (some) - **Priority:** P1 - **Risk:** High - **Popularity:** High
- **Status:** ArcadeDB indexes are per-bucket within a single node; HA is
  replicated, not sharded. Milvus's biggest selling point is the
  coordinator/worker model and storage-compute separation; Qdrant supports
  custom shard keys.
- **Why it matters:** Single-node ceilings make ArcadeDB uncompetitive
  past a few hundred million vectors. Multi-quarter project that affects
  the engine, not just the vector index, but the lack is felt most
  acutely on vector workloads (which scale fastest).

#### MMR (Maximal Marginal Relevance) for result diversity
- **Source:** Q - **Priority:** P1 - **Risk:** Medium - **Popularity:** Medium
- **Status:** Not implemented as a first-class operator. Milvus has it via
  custom Functions or external scripts; Qdrant has a dedicated path.
- **Why it matters:** RAG systems typically want diverse top-K, not
  near-duplicates. MMR is the standard re-ranking step for this. Trivial
  to add as a SQL function operating on a result set, harder to integrate
  into the index traversal.

#### IVF index family (IVF_FLAT, IVF_SQ8, IVF_PQ)
- **Source:** M - **Priority:** P1 - **Risk:** Medium - **Popularity:** Medium-High
- **Status:** Not implemented. JVector is HNSW-centric; IVF is a different
  family.
- **Why it matters:** IVF clusters vectors and probes only N clusters per
  query. It scales differently to HNSW: more memory-efficient at huge
  cardinalities, easier to shard, faster to build. Many production
  Milvus users pick IVF over HNSW.
- **Design suggestion:** A separate index implementation (not via JVector)
  or wait for JVector roadmap. Lower priority than HNSW improvements.

#### DiskANN (Vamana, on-disk graph)
- **Source:** M - **Priority:** P1 - **Risk:** Medium - **Popularity:** Medium
- **Status:** Not implemented. ArcadeDB's HNSW has on-disk pages but is
  not DiskANN.
- **Why it matters:** DiskANN is the standard for "billions of vectors on
  one machine, mostly on SSD". JVector has a roadmap item for DiskANN-style
  indexes; ArcadeDB would benefit directly.

#### Range search (radius / score threshold) - shipped
- **Source:** Q+M - **Priority:** P1 - **Risk:** Medium - **Popularity:** Medium
- **Status:** **Implemented** as a post-filter option on the neighbors
  functions: `maxDistance` on `vector.neighbors`, `minScore` on
  `vector.sparseNeighbors`. The result list breaks early once a row crosses
  the threshold (the inputs are already sorted), so the cost is bounded by
  the upstream top-K. Verified by `VectorRangeSearchTest`.
  ```sql
  -- "All docs with cosine distance <= 0.05 from the query":
  SELECT expand(`vector.neighbors`('Doc[embedding]', :q, 100, { maxDistance: 0.05 }))
  -- "All sparse hits with BM25 score >= 0.5":
  SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', :qIdx, :qVal, 100, { minScore: 0.5 }))
  ```
  **Limitation:** JVector's HNSW does not expose a native range mode, so the
  function still pays for the K candidates the upstream search returns. Set a
  generous `k`/`limit` alongside a tight threshold to cover the radius
  semantics; a true unbounded radius scan remains a follow-up.

#### Search iterator for large result sets
- **Source:** M (primary) - **Priority:** P1 - **Risk:** Medium - **Popularity:** Medium
- **Status:** ArcadeDB streams via SQL but the HNSW search itself returns
  top-K, not an iterator.
- **Why it matters:** Some workloads (deduplication, mass evaluation, data
  export) need to scan millions of results without re-running ANN.

### 3.3 P2 - Important but workaroundable

#### Recommendation API (positive/negative examples) - shipped
- **Source:** Q (primary) / M (workaround) - **Priority:** P2 - **Risk:** Medium - **Popularity:** Medium
- **Status:** **Implemented** as `vector.recommend(indexSpec, positiveRids,
  negativeRids, k[, options])`. Looks up each example record, reads the
  embedding property (parsed from the `Type[property]` index spec), averages
  positives and negatives independently, takes the difference
  (`centroid(positives) - centroid(negatives)`), and runs the existing
  `vector.neighbors` against that query vector. Examples themselves are
  excluded from the result by post-filtering. Forwarded options:
  `efSearch`, `groupBy`, `groupSize`. Verified by
  `SQLFunctionVectorRecommendTest`.
  ```sql
  SELECT expand(`vector.recommend`('Doc[embedding]',
      [#1:1, #1:2],   -- positives (required, non-empty)
      [#1:9],         -- negatives (optional)
      10))
  ```

#### Discovery / context search API - shipped
- **Source:** Q - **Priority:** P2 - **Risk:** Low-Medium - **Popularity:** Low-Medium
- **Status:** **Implemented** as `vector.discover(indexSpec, pairs, k[, options])`. Each
  pair `[positive_rid, negative_rid]` contributes a margin
  `cos(c, positive) - cos(c, negative)` to the candidate's discovery score; the
  function picks the top-K by score sum. Candidates come from a
  `vector.neighbors` call against a seed vector built from the per-pair
  differences (sum of `pos - neg`), then re-ranked by the per-pair score so
  the result reflects the discovery semantics rather than the seed's geometry.
  `efSearch`, `overfetch` (default 5x) options. Verified by
  `SQLFunctionVectorDiscoverTest`.
  ```sql
  SELECT expand(`vector.discover`('Doc[embedding]',
      [[#1:1, #1:9], [#1:2, #1:8]],   -- (positive, negative) pairs
      10))
  ```

#### Multi-stage retrieval (declarative prefetch + rerank) - shipped
- **Source:** Q+M - **Priority:** P2 - **Risk:** Medium - **Popularity:** Medium
- **Status:** **Implemented** as `vector.rerank(source, queryVector,
  embeddingProperty, k)`. Takes a stage-1 candidate set (typically the
  output of a cheap-but-coarse search like `vector.neighbors` on a
  quantized index), looks up each record's full-precision embedding via
  `embeddingProperty`, computes cosine similarity to the new query, and
  returns the top-K reranked. Server-side and declarative; the application
  no longer needs to round-trip between stages.
  ```sql
  SELECT expand(`vector.rerank`(
      `vector.neighbors`('Doc[binary_embedding]', :q_binary, 1000),
      :q_full,
      'embedding',
      100))
  ```
  Verified by `SQLFunctionVectorRerankTest`.

#### Score-boosting / formula expressions over result set - shipped
- **Source:** Q (primary) - **Priority:** P2 - **Risk:** Low-Medium - **Popularity:** Medium
- **Status:** **Implemented** as `vector.boost(source, { boosts: [{field,
  weight}, ...][, limit] })`. Computes
  `boostedScore = base_similarity + Σ(weight_i * row[field_i])` and re-sorts
  by the new score descending. Auto-flips upstream `distance` to similarity
  so the output is consistently higher-is-better. Composes with
  `vector.neighbors` / `vector.sparseNeighbors` / `vector.fuse`.
  ```sql
  SELECT expand(`vector.boost`(
      `vector.neighbors`('Doc[embedding]', :q, 100),
      { boosts: [
          { field: 'popularity', weight: 0.1 },
          { field: 'recency_decay', weight: 0.05 }
      ], limit: 10 }
  ))
  ```
  Verified by `SQLFunctionVectorBoostTest`.

#### Pre-quantized ingest types (uint8, INT8, Float16, BFloat16)
- **Source:** M (full) / Q (partial) - **Priority:** P2 - **Risk:** Low-Medium - **Popularity:** Medium
- **Status:** ArcadeDB stores float32 and quantizes internally. Milvus
  accepts FLOAT16, BFLOAT16, and INT8 ingest types directly; Qdrant accepts
  uint8.
- **Why it matters:** Saves 4x bandwidth and storage when the embedding
  provider already produces half-precision (Cohere, OpenAI newer endpoints,
  self-hosted Sentence Transformers). Small additive feature.

#### Manhattan (L1) distance - shipped
- **Source:** Q - **Priority:** P2 - **Risk:** Low - **Popularity:** Low-Medium
- **Status:** **Implemented as brute-force SQL.** `vector.l1Distance` and
  `vector.manhattanDistance` (alias) wrap the existing
  `VectorUtils.manhattanDistance` (already used by Cypher's
  `vector.distance.manhattan`). Use cases:
  ```sql
  SELECT @rid, `vector.l1Distance`(:q, embedding) AS d FROM Doc
    ORDER BY d ASC LIMIT 10
  ```
  **Limitation:** JVector's HNSW only supports EUCLIDEAN / COSINE / DOT_PRODUCT
  similarity functions, so an indexed L1 path is not available. Brute-force is
  the only option for now; OK for small corpora and exploratory use.

#### Binary vector type with hamming / jaccard distances
- **Source:** M - **Priority:** P2 - **Risk:** Low-Medium - **Popularity:** Low-Medium
- **Status:** ArcadeDB has BINARY quantization but no native binary vector
  type, and only cosine/L2/dot.
- **Why it matters:** Cheminformatics, image fingerprinting, and binary
  embeddings (Cohere binary, BinaryNet) need hamming/jaccard. Niche but
  cheap to add.

#### Partition key for multi-tenancy - partially shipped
- **Source:** M - **Priority:** P2 - **Risk:** Medium - **Popularity:** Medium-High
- **Status:** **Partial via [#4119](https://github.com/ArcadeData/arcadedb/pull/4119) (issue #4087, closed).**
  ArcadeDB has had `partitioned(<key>)` types for a while (schema-level
  buckets keyed by a property hash). Query-time pruning was the missing half:
  the planner now derives the bucket set from `WHERE` predicates on the
  partition key and stashes it on `CommandContext`, and the vector functions
  (`vector.neighbors`, `vector.sparseNeighbors`) narrow their per-bucket
  fan-out to that subset. **Still missing:** end-to-end pruning for
  non-vector reads (regular `SELECT ... FROM Doc WHERE tenant_id = 'X'`)
  through the same plan-level path - covered in part by `FetchFromTypeStep`
  but not as a unified planner concept.
- **Why it matters:** SaaS multi-tenant workloads want "this query touches
  only tenant X's data" without writing the filter manually. The vector path
  (most performance-sensitive) is now covered; the general SQL path is the
  remaining work.

#### Dynamic field with indexed JSON paths
- **Source:** M - **Priority:** P2 - **Risk:** Low-Medium - **Popularity:** Medium
- **Status:** ArcadeDB has untyped properties, but no `dynamic_field`
  semantics with JSON path indexes. Milvus indexes specific JSON paths or
  whole subtrees.
- **Why it matters:** Flexible payload schemas with fast filter on a few
  hot paths. Common in agent/event-store use cases.

#### Bitmap scalar index for low-cardinality fields
- **Source:** M - **Priority:** P2 - **Risk:** Low - **Popularity:** Medium
- **Status:** Not implemented. ArcadeDB has LSM-Tree indexes which are
  general but not optimal for low cardinality.
- **Why it matters:** Filters like `category = 'electronics' AND status = 'active'`
  benefit from bitmap intersection. Often paired with vector filtering.

### 3.4 P3 - Nice to have

#### GPU-accelerated index build (CAGRA, GPU_IVF_*)
- **Source:** Q+M - **Priority:** P3 - **Risk:** Low - **Popularity:** Low (high-end users)
- **Status:** Not implemented; tied to JVector's roadmap and JVM-GPU
  support.
- **Why it matters:** Order-of-magnitude faster bulk ingestion at >100M
  vectors. Mostly a competitive signal at the high end. Most ArcadeDB
  workloads do not need it.

#### Geo / UUID / dedicated datetime payload indexes
- **Source:** Q - **Priority:** P3 - **Risk:** Low - **Popularity:** Medium (geo) / Low (uuid)
- **Status:** Geo: **already shipped** as `LSMTreeGeoIndex` (Lucene spatial4j
  prefix-tree based; `point.withinBBox`, `point.withinDistance`, etc. SQL
  functions). The original matrix entry of ❌ here was wrong; corrected.
  UUID and datetime are handled by generic LSM-Tree indexes (works for
  range queries) but not optimised at the index-implementation level.
- **Why it matters:** Geo + vector ("nearest 10 things, but inside this
  bounding box") is a common combined filter and is supported today by
  composing the geo index in WHERE with vector functions in projection.

#### Tenant index / principal index (locality optimisations)
- **Source:** Q - **Priority:** P3 - **Risk:** Low-Medium - **Popularity:** Medium
- **Status:** Not implemented.
- **Why it matters:** Heavy multi-tenant SaaS workloads benefit from
  on-disk locality of one tenant's data. Becomes important only past
  ~1B vectors and thousands of tenants.

#### Collection aliases (zero-downtime swaps)
- **Source:** Q+M - **Priority:** P3 - **Risk:** Low - **Popularity:** Medium
- **Status:** Not implemented.
- **Why it matters:** Common operational pattern when re-embedding a
  corpus with a new model: build the new index in a side type, then
  atomically swap. Easy to layer on top of schema rename.

#### SCANN index
- **Source:** M - **Priority:** P3 - **Risk:** Low - **Popularity:** Low-Medium
- **Status:** Not implemented; Google-originated index, integrated in Milvus.
- **Why it matters:** Slightly different recall/latency profile than HNSW
  or IVF. Niche choice.

#### HNSW_PRQ (product residual quantization)
- **Source:** M - **Priority:** P3 - **Risk:** Low - **Popularity:** Low-Medium
- **Status:** Not implemented. ArcadeDB has PQ; PRQ is a refinement.
- **Why it matters:** Higher recall at same compression than PQ. Only
  matters at very large scale.

#### SUBSTRUCTURE / SUPERSTRUCTURE distances (binary)
- **Source:** M - **Priority:** P3 - **Risk:** Low - **Popularity:** Low (cheminformatics niche)
- **Status:** Not implemented.
- **Why it matters:** Required by cheminformatics workloads. Very narrow
  audience.

#### Storage-compute separation (object-storage backed)
- **Source:** M - **Priority:** P3 - **Risk:** Medium - **Popularity:** Medium (cloud users)
- **Status:** ArcadeDB uses local pages. Milvus Distributed uses
  MinIO/S3 + Pulsar/Kafka.
- **Why it matters:** Cost efficiency at huge scale; not a fit for the
  embedded / single-binary workloads ArcadeDB targets.

#### Strict mode / collection-level guardrails
- **Source:** Q - **Priority:** P3 - **Risk:** Low - **Popularity:** Low
- **Status:** Not implemented in the same form. ArcadeDB has schema-level
  constraints, but no Qdrant-style collection knobs (e.g., max points,
  required filters).

---

## 4. Where Qdrant and Milvus struggle vs ArcadeDB

For symmetry, the gaps in the other direction. None of these are "Qdrant or
Milvus could add this if they wanted" - they are deliberate scope choices.
A team that needs them ends up gluing a vector DB + Postgres + Neo4j +
Elasticsearch together, which is exactly the integration cost ArcadeDB
removes.

### 4.1 Common to Qdrant and Milvus

- **No graph traversal**: cannot follow relationships from a result vertex.
  Required for GraphRAG and retrieval-with-citations.
- **No SQL / Cypher / Gremlin / GraphQL / MongoDB query languages**: only
  the proprietary Query API or filter DSL plus client SDKs.
- **No multi-protocol wire compatibility** (Postgres, Mongo, Redis, Bolt).
- **No transactional joins** across collections.
- **No native document or KV model**: payloads are JSON blobs without
  schema in Qdrant, and flat-collection columns in Milvus.

### 4.2 Specific to Qdrant

- **No native full-text search engine** (analyzers exist but are lighter
  than Lucene's).
- **No embedded mode**: must run as a separate service.

### 4.3 Specific to Milvus

- **No ACID transactions**: writes are eventually consistent across the
  distributed pipeline. A delete and a re-insert can be visible in any
  order. Hard to use as a source of truth.
- **No JVM embedded mode**: Milvus Lite is Python-only and explicitly
  not recommended for production.
- **Operationally heavy at scale**: Milvus Distributed requires etcd,
  Pulsar (or Kafka), MinIO (or S3), and Kubernetes. ArcadeDB ships as
  a single binary.
- **Schema is flat**: no inheritance, no edges-as-types, no schema-level
  constraints beyond the collection.

---

## 5. Recommended roadmap (priority-ordered)

The set below is what would close the most-felt gaps for the smallest amount
of work, in the order they should be tackled. Tiers 1-3 are sparse-vector
*engine-internal* follow-ups from
[`docs/sparse-vector-storage-design.md`](sparse-vector-storage-design.md) -
small, low-risk, and they keep the engine production-ready before the
larger competitive features land. Tiers 4+ are the broader feature gaps
versus Qdrant/Milvus.

### Already shipped

1. ~~**Indexed sparse vectors** (#4065)~~ - **shipped**.
2. ~~**Server-side hybrid fusion with rerankers** (#4066)~~ - **shipped**.
3. ~~**Group-by in vector search** (#4067)~~ - **shipped**.
4. ~~**Sparse-index scaling** (#4068)~~ - **shipped** (BlockMax-WAND, INT8/FP16/FP32
   quantization, size-tiered auto-compaction, HA replication via `runWithCompactionReplication`).
5. ~~**Bolt protocol coverage for `LSM_SPARSE_VECTOR`** (#4079)~~ - **shipped**.
6. ~~**Partition pruning on vector functions** (#4119, issue #4087)~~ - **shipped**.

### Tier 1 - engine cleanup + ops visibility (small, no format change)

7. ~~**#4073 - Replace `isOriginalCall` shape-sniffing with a typed replay
   marker**~~ - **shipped.** New `SparsePostingReplayKey` record replaces the
   3-tuple shape; `LSMSparseVectorIndex.put/remove` now dispatch via
   `instanceof SparsePostingReplayKey`. Dedup semantics carried by
   `TransactionIndexContext` are preserved. Verified by
   `SparsePostingReplayKeyTest` plus the existing sparse-vector + LSM-Tree +
   HA-replication tests.
8. ~~**Studio per-index segment-count card**~~ - **shipped.** Server tab now
   shows a "Sparse Vector Indexes" card with one row per logical
   `LSMSparseVectorIndex` aggregating `memtablePostings`, `segmentCount`,
   `totalPostings` across per-bucket sub-indexes. Backed by
   `LSMSparseVectorIndexMetrics.buildJSON` (engine) +
   `GetServerHandler.buildSparseVectorIndexesJSON` (server). Hidden when no
   database has any sparse-vector indexes. Verified by
   `LSMSparseVectorIndexMetricsTest`.

### Tier 2 - production safety

9. ~~**Memtable backpressure under sustained write rate**~~ - **shipped.**
   `PaginatedSparseVectorEngine.put` / `remove` soft-block on `mutatorLock`
   once `memtable.totalPostings() >= 2 * memtableFlushThreshold`. The lock
   take/release is essentially free when no flush is running; under
   contention the put waits exactly for the in-progress flush to swap the
   memtable. We deliberately do not call `flush()` from `put` (the
   commit-replay path runs inside an outer `database.transaction(...)` and
   nesting a transaction would deadlock). Verified by
   `PaginatedSparseVectorEngineBackpressureTest`.
10. ~~**Tombstone-ratio compaction trigger**~~ - **shipped.** Each segment
    persists `totalTombstones` in the manifest's previously-reserved 8-byte
    slot (no format-version bump - older segments read 0L which is a safe
    under-report). `compactSizeTiered` now has a secondary trigger after
    the size-tier overflow path: when `active.length >= tierFanout` and any
    segment carries `tombstones / totalPostings >= 0.30`, the offender is
    paired with the oldest neighbors so the merge collapses file count.
    Runs with `dropAllTombstones=false` to preserve user-visible state when
    matching inserts are in older segments outside the input set. Verified
    by `PaginatedSparseVectorEngineTombstoneTriggerTest`.

### Tier 3 - sparse-vector performance lever

11. ~~**#4085 - Per-bucket parallel top-K fan-out**~~ - **shipped.**
    `SQLFunctionVectorSparseNeighbors.executeWithIndexes` now submits each
    per-bucket `LSMSparseVectorIndex.topK(...)` call onto the dedicated
    `SparseVectorScoringPool` when the type has more than one bucket;
    single-bucket types take the inline path. Per-bucket sub-indexes are
    independent so the dispatch is correctness-trivial. The
    `SPARSE_VECTOR_SCORING_POOL_THREADS` and
    `SPARSE_VECTOR_SCORING_QUEUE_SIZE` operator knobs landed in
    `GlobalConfiguration` alongside the wiring. Verified by
    `SparseVectorParallelFanoutTest` (correctness + dispatch-actually-fires
    + single-bucket inline-path). **Per-segment within-index parallelism
    (RID-range partitioning) deferred** as a future enhancement; the
    practical bar is higher now that per-bucket fan-out covers the typical
    multi-bucket case.

### Tier 4 - broader competitive gaps (priority order)

12. **Unify Lucene full-text with sparse vector path** - expose
    `text -> sparse vector` as a schema-level function so BM25 and dense
    compose in the same hybrid call. Mirrors Milvus 2.5's `Function` schema.
    The building blocks now exist (Lucene + `LSM_SPARSE_VECTOR`); they need
    to be wired together.
13. **Filterable HNSW improvements** - investigate ACORN in JVector;
    payload-aware sub-partitioning for high-cardinality filters. The #1
    production complaint about vector DBs.
14. **Named vectors as a first-class schema concept** - clean up
    multi-embedding ergonomics; pre-requisite for ColBERT and recommendation
    APIs.
15. **#4071 - Traversal-integrated groupBy for vector search** (sparse-vector
    companion). The MVP groupBy implementation does post-traversal grouping
    with adaptive over-fetch; rewriting it against the v2 cursor pushes the
    dedup into the index walk. Depends on Tier 3 cursor design being stable
    (it is).
16. ~~**Range search**~~ - **shipped** as `maxDistance` (`vector.neighbors`)
    and `minScore` (`vector.sparseNeighbors`) options. Post-filter today
    with a fast early-break on the sorted upstream result; native HNSW range
    mode remains an open follow-up tied to JVector. **Search iterator** for
    very large result sets still open.
17. ~~**MMR**~~ - **shipped** (`vector.mmr`).
    ~~**Recommendation API**~~ - **shipped** (`vector.recommend`).
    ~~**Discovery API**~~ - **shipped** (`vector.discover`).
    All three compose with the existing `@rid` / `score` row shape that
    `vector.neighbors`, `vector.sparseNeighbors`, and `vector.fuse` produce.
    Verified by `SQLFunctionVectorMmrTest`, `SQLFunctionVectorRecommendTest`,
    and `SQLFunctionVectorDiscoverTest`.
18. **Multi-vector (ColBERT) native index** - larger investment, depends
    on JVector roadmap.
19. **Pre-quantized ingest types** (uint8, INT8, Float16, BFloat16) -
    schema-level change, deferred.
    ~~**manhattan distance**~~ (shipped as brute-force SQL function;
    indexed path blocked on JVector adding L1).
    ~~**declarative prefetch**~~ - **shipped** (`vector.rerank`).
20. **Bitmap scalar index, JSON path index** -
    operational and product quality-of-life features.
    ~~**Score boosting**~~ - **shipped** (`vector.boost`).
21. ~~**Generalize partition pruning beyond vector functions.**~~ - already
    shipped (precedes #4119). `SelectExecutionPlanner.derivePartitionPrunedClusters`
    runs in `handleTypeAsTarget` and narrows the cluster set fed into every
    fetch path (indexed function, indexed lookup, `FetchFromTypeWithFilterStep`,
    `FetchFromTypeExecutionStep`). #4119 was specifically the per-bucket
    fan-out leak in vector functions, where the function's per-bucket walk
    bypassed the planner's narrowed cluster list. Verified by
    `PartitionPruningPlannerTest`. Originally listed here in error.

### Tier 5 - low-priority polish (pull when pain shows up)

22. **Block-size empirical sweep** on a SPLADE-style corpus to validate
    or move the 128 default. Pure tuning study; no code change unless
    results dictate.
23. **fp16 default re-evaluation** once we have customer data on int8
    quantization loss for non-uniform weight distributions (Cohere v4,
    OpenAI 3-large). The mode already ships; just changing the default.
24. **Inline `WAL_SPARSE_PUT/REMOVE/FLUSH` records.** Closes the
    microsecond durability window mentioned in the design doc; today's
    per-page WAL is already fully durable.
25. **MVP rebuild migration tool.** Only build if the field reports it.

### Below the line - large, mostly-orthogonal initiatives

These should be planned independently of the vector roadmap above:

- **Distributed sharding / horizontal scale-out** - the biggest
  competitive gap with Milvus, but a multi-quarter cross-cutting project.
- **GPU index build (CAGRA, GPU_IVF_*)** - bound to JVector and JVM-GPU
  ecosystem; only relevant past 100M vectors.
- **IVF / DiskANN / SCANN index families** - alternative ANN algorithms;
  pursue when HNSW + quantization no longer suffice.
- **Storage-compute separation (object storage backend)** - relevant for
  cloud-scale deployments and partly contradicts the embedded-first design.
- **Geo / UUID / dedicated datetime payload indexes**, **tenant /
  principal indexes**, **collection aliases** - operational features
  tracked separately.

---

## 6. Sources

- [ArcadeDB Discussion #4044 - Migration from Qdrant](https://github.com/ArcadeData/arcadedb/discussions/4044)
- [Issue #4065 - Sparse vector indexing (shipped)](https://github.com/ArcadeData/arcadedb/issues/4065)
- [Issue #4066 - Server-side hybrid search (shipped)](https://github.com/ArcadeData/arcadedb/issues/4066)
- [Issue #4067 - Group-by in vector search (shipped)](https://github.com/ArcadeData/arcadedb/issues/4067)
- [Issue #4068 - WAND/BlockMax-WAND scaling for sparse index (shipped, closed)](https://github.com/ArcadeData/arcadedb/issues/4068)
- [Issue #4071 - Traversal-integrated groupBy for vector search (open follow-up)](https://github.com/ArcadeData/arcadedb/issues/4071)
- [Issue #4073 - LSMSparseVectorIndex replay-marker cleanup (open)](https://github.com/ArcadeData/arcadedb/issues/4073)
- [Issue #4085 - Per-segment parallel top-K scoring (Step 5 follow-up)](https://github.com/ArcadeData/arcadedb/issues/4085)
- [Issue #4087 / PR #4119 - Partition-aware planner pruning for vector functions (shipped, closed)](https://github.com/ArcadeData/arcadedb/issues/4087)
- [PR #4079 - Bolt protocol coverage for `LSM_SPARSE_VECTOR` (shipped)](https://github.com/ArcadeData/arcadedb/pull/4079)
- [`docs/sparse-vector-storage-design.md`](sparse-vector-storage-design.md) - design doc for the v2 sparse-vector engine and source of the engine-internal Tier 1-3 follow-ups

### Qdrant references

- [Qdrant - Collections concepts](https://qdrant.tech/documentation/concepts/collections/)
- [Qdrant - Indexing](https://qdrant.tech/documentation/concepts/indexing/)
- [Qdrant - Hybrid Queries](https://qdrant.tech/documentation/concepts/hybrid-queries/)
- [Qdrant - Filterable HNSW (ACORN)](https://qdrant.tech/course/essentials/day-2/filterable-hnsw/)
- [Qdrant 2025 Recap](https://qdrant.tech/blog/2025-recap/)
- [Qdrant 1.10 - Universal Query, IDF, ColBERT](https://qdrant.tech/blog/qdrant-1.10.x/)
- [Qdrant - Multivectors and Late Interaction](https://qdrant.tech/documentation/tutorials-search-engineering/using-multivector-representations/)

### Milvus references

- [Milvus - What is Milvus](https://milvus.io/docs/overview.md)
- [Milvus - Architecture Overview](https://milvus.io/docs/architecture_overview.md)
- [Milvus - In-memory Index](https://milvus.io/docs/index.md)
- [Milvus - GPU Index](https://milvus.io/docs/gpu_index.md)
- [Milvus - Multi-Vector Hybrid Search](https://milvus.io/docs/multi-vector-search.md)
- [Milvus - Full Text Search (BM25)](https://milvus.io/docs/full-text-search.md)
- [Milvus - Sparse Vector](https://milvus.io/docs/sparse_vector.md)
- [Milvus - Metric Types](https://milvus.io/docs/metric.md)
- [Milvus - Index Scalar Fields](https://milvus.io/docs/index-scalar-fields.md)
- [Milvus - Bitmap Index](https://milvus.io/docs/bitmap.md)
- [Milvus - Inverted Index](https://milvus.io/docs/inverted.md)
- [Milvus - Use Partition Key](https://milvus.io/docs/use-partition-key.md)
- [Milvus - Filtering Explained](https://milvus.io/docs/boolean.md)
- [Milvus - Deployment Options](https://milvus.io/docs/install-overview.md)
- [Zilliz - CAGRA GPU index](https://zilliz.com/blog/Milvus-introduces-GPU-index-CAGRA)

### ArcadeDB code references

- `engine/src/main/java/com/arcadedb/index/vector/LSMVectorIndex.java`
- `engine/src/main/java/com/arcadedb/index/vector/VectorQuantizationType.java`
- `engine/src/main/java/com/arcadedb/function/sql/vector/SparseVector.java`
- `engine/src/main/java/com/arcadedb/index/sparsevector/LSMSparseVectorIndex.java`
- `engine/src/main/java/com/arcadedb/function/sql/vector/SQLFunctionVectorSparseNeighbors.java`
- `engine/src/main/java/com/arcadedb/function/sql/vector/SQLFunctionVectorFuse.java`
- `engine/src/main/java/com/arcadedb/function/sql/vector/SQLFunctionVectorNeighbors.java`
- `engine/src/main/java/com/arcadedb/function/sql/vector/` (40+ SQL functions)
