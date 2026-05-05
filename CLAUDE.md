# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcadeDB is a Multi-Model DBMS (Database Management System) built for extreme performance. It's a Java-based project that supports multiple data models (Graph, Document, Key/Value, Search Engine, Time Series, Vector Embedding) and query languages (SQL, Cypher, Gremlin, GraphQL, MongoDB Query Language).

## Response Formatting
- Never use the em dash character (`—`) in responses. Use a normal dash (`-`), a comma, or rephrase instead.

## Project Instructions

Before writing any code:
- State how you will verify this change works (e.g., unit tests, integration tests, manual testing)
- Write the tests first (TDD approach) whenever possible
- Ensure code adheres to existing coding standards and styles
- Then implement the code
- Run verification and iterate until it passes
- Run all the connected tests could be affected by the change to ensure nothing is broken (no need to run the whole suite, it would take too long)

General design principles:
- reuse existing components whenever is possible
- don't use fully qualified names if possible, always import the class and just use the name
- don't include a new dependency unless is strictly necessary, and they MUST be Apache 2.0 compatible:
  - ✅ ALLOWED: Apache 2.0, MIT, BSD (2/3-Clause), EPL 1.0/2.0, UPL 1.0, EDL 1.0, LGPL 2.1+ (for libraries only), CC0/Public Domain
  - ❌ FORBIDDEN: GPL, AGPL, proprietary licenses without explicit permission, SSPL, Commons Clause
  - When adding a dependency, you MUST update ATTRIBUTIONS.md and, if Apache-licensed with a NOTICE file, incorporate required notices into the main NOTICE file
- for Studio (webapp), limit to jquery and bootstrap 5. If necessary use 3rd party libs, but they must be Apache 2.0 compatible (see allowed licenses above)
- always bear in mind PERFORMANCE. It must be always your mantra: performance and lightweight on garbage collector. If you can, prefer using arrays of primitives to List of Objects
- if you need to use JSON, use the class com.arcadedb.serializer.json.JSONObject. Leverage the getter methods that accept the default value as 2nd argument, so you don't need to check if they present or not null = less boilerplate code
- same thing for JSON arrays: use com.arcadedb.serializer.json.JSONArray class
- code styles:
 - adhere to the existing code
 - if statements with only one child sub-statement don't require a curly brace open/close, keep it simple
 - use the final keyword when possible on variables and parameters
- all new server-side code must be tested with a test case. Check existing test case to see the framework and style to use
- write a regression test
- after every change in the backend (Java), compile the project and fix all the issues until the compilation passes
- test all the new and old components you've modified before considering the job finished. Please do not provide something untested
- always keep in mind speed and security with ArcadeDB, do not introduce security hazard or code that could slow down other parts unless requested/approved
- do not commit on git, I will do it after a review
- remove any System.out you used for debug when you have finished
- For test cases, prefer this syntax: `assertThat(property.isMandatory()).isTrue();`
- Annotate performance/benchmark tests so they're skipped from regular CI builds:
  - Use `@Tag("benchmark")` for pure microbenchmarks (e.g., JMH-style or comparison runs)
  - Use `@Tag("slow")` for functional regression tests that take noticeably long (large batches, multi-second elapsed time, big payloads)
  - Apply at the class level when every method in the class is slow; at the method level when only some methods are slow
  - Required imports: `import org.junit.jupiter.api.Tag;`
- don't add Claude as author of any source code

## Build and Development Commands

### Maven (Java)
- **Build entire project**: `mvn clean install`
- **Build without tests**: `mvn clean install -DskipTests`
- **Run unit tests**: `mvn test`
- **Run integration tests**: `mvn test -DskipITs=false`
- **Build specific module**: `cd <module> && mvn clean install`

### Studio Frontend (Node.js)
- **Build frontend**: `cd studio && npm run build`
- **Development mode**: `cd studio && npm run dev`
- **Security audit**: `cd studio && npm run security-audit`

### Server Operations
- **Start server**: Use packaged scripts in `package/src/main/scripts/server.sh` (Unix) or `server.bat` (Windows)
- **Console**: Use `package/src/main/scripts/console.sh` or `console.bat`

### Distribution Builder

The modular distribution builder (`package/arcadedb-builder.sh`) creates custom ArcadeDB distributions:

**Production builds** (download from releases):
```bash
cd package
./arcadedb-builder.sh --version=26.1.0 --modules=gremlin,studio
```

**Development builds** (use local Maven repository):
```bash
# Build modules first
mvn clean install -DskipTests

# Create distribution with local modules
cd package
VERSION=$(mvn -f ../pom.xml help:evaluate -Dexpression=project.version -q -DforceStdout)
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=console,gremlin,studio \
    --local-repo \
    --skip-docker
```

**Testing the builder**:
```bash
cd package
./test-builder-local.sh
```

### Testing Commands
- **Run specific test class**: `mvn test -Dtest=ClassName`
- **Run tests with specific pattern**: `mvn test -Dtest="*Pattern*"`
- **Performance tests**: Located in `src/test/java/performance/` packages

## Codebase Navigation Map

### ANTLR Grammars
- `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4` — SQL lexer
- `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4` — SQL parser
- `engine/src/main/antlr4/com/arcadedb/query/opencypher/grammar/Cypher25Lexer.g4` — Cypher lexer
- `engine/src/main/antlr4/com/arcadedb/query/opencypher/grammar/Cypher25Parser.g4` — Cypher parser

### SQL Engine Key Files
- **Parser AST nodes** (170+ classes): `engine/src/main/java/com/arcadedb/query/sql/parser/`
  - `SuffixIdentifier.java` — property access (e.g., `record.field`)
  - `BaseIdentifier.java`, `LevelZeroIdentifier.java` — identifier resolution
  - `Expression.java`, `BaseExpression.java`, `MathExpression.java` — expression evaluation
  - `Projection.java`, `ProjectionItem.java` — SELECT projection handling
  - `SelectStatement.java`, `MatchStatement.java` — statement AST roots
  - `NestedProjection.java`, `NestedProjectionItem.java` — nested projection (e.g., `{*}`)
  - `FunctionCall.java`, `MethodCall.java` — function/method invocation
  - `Modifier.java` — chained modifiers (array selectors, method calls, suffix identifiers)
  - `WhereClause.java`, `BooleanExpression.java` — filter conditions
  - `LetClause.java`, `LetItem.java` — LET variable bindings
- **Executor steps** (158 classes): `engine/src/main/java/com/arcadedb/query/sql/executor/`
  - `SelectExecutionPlanner.java` — main SELECT execution planner
  - `ProjectionCalculationStep.java` — projection evaluation step
  - `LetExpressionStep.java`, `GlobalLetExpressionStep.java` — LET evaluation
  - `FetchFromTypeStep.java`, `FetchFromIndexStep.java` — data source steps
  - `FilterStep.java`, `FilterByClustersStep.java` — filtering steps
- **SQL methods** (50+ classes): `engine/src/main/java/com/arcadedb/query/sql/method/`
  - `string/` — toLowerCase, toUpperCase, trim, split, etc.
  - `collection/` — size, keys, values, sort, etc.
  - `conversion/` — asInteger, asString, asList, asJSON, etc.
- **SQL functions**: `engine/src/main/java/com/arcadedb/function/sql/`
  - `graph/` — out, in, both, outE, inE, bothE, shortestPath, dijkstra, etc.
  - `coll/` — difference, intersect, symmetricDifference
  - `fulltext/` — search field/index functions

### OpenCypher Engine Key Files
- **AST** (40+ classes): `engine/src/main/java/com/arcadedb/query/opencypher/ast/`
  - `CypherStatement.java`, `MatchClause.java`, `ReturnClause.java`, `WhereClause.java`
  - `CreateClause.java`, `MergeClause.java`, `DeleteClause.java`, `SetClause.java`
  - `PatternElement.java`, `NodePattern.java`, `RelationshipPattern.java`
- **Executor**: `engine/src/main/java/com/arcadedb/query/opencypher/executor/`
- **Optimizer**: `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/`
- **Planner**: `engine/src/main/java/com/arcadedb/query/opencypher/planner/`
- **Tests**: `engine/src/test/java/com/arcadedb/query/opencypher/`

### Graph Engine
- `engine/src/main/java/com/arcadedb/graph/`
  - `Vertex.java`, `MutableVertex.java`, `ImmutableVertex.java` — vertex types
  - `Edge.java`, `MutableEdge.java`, `ImmutableEdge.java` — edge types
  - `GraphEngine.java` — core graph operations
  - `EdgeSegment.java`, `MutableEdgeSegment.java` — edge storage segments
  - `EdgeLinkedList.java` — edge linked list structure
  - `EdgeIterator.java`, `VertexIterator.java` — traversal iterators

### Server / HTTP
- **HTTP handlers**: `server/src/main/java/com/arcadedb/server/http/handler/`
  - `DatabaseAbstractHandler.java` — base handler (wraps commands in transactions)
  - `PostCommandHandler.java` — POST /command endpoint
  - `PostQueryHandler.java`, `GetQueryHandler.java` — query endpoints
- **HA**: `server/src/main/java/com/arcadedb/server/ha/`
- **Security**: `server/src/main/java/com/arcadedb/server/security/`

### Test Locations (by module)
- `engine/src/test/java/` — 746 test files (SQL, Cypher, graph, storage, schema, indexing)
- `server/src/test/java/` — 114 test files (HTTP API, HA, security)
- `gremlin/src/test/java/` — 29 test files
- `integration/src/test/java/` — 22 test files
- `bolt/src/test/java/` — 10 test files
- `graphql/src/test/java/` — 9 test files

## Architecture Overview

### Core Modules
- **engine/**: Core database engine, storage, indexing, query execution (SQL, OpenCypher, Polyglot)
- **server/**: HTTP/REST API, WebSocket support, clustering/HA, MCP server
- **network/**: Network communication layer
- **console/**: CLI console for interactive database operations
- **studio/**: Web-based administration interface (JavaScript/Node.js)
- **metrics/**: Server metrics collection and reporting
- **integration/**: Integration utilities
- **test-utils/**: Shared test utilities

#### Wire Protocol Modules
- **gremlin/**: Apache Tinkerpop Gremlin support
- **graphql/**: GraphQL API support
- **mongodbw/**: MongoDB wire protocol compatibility
- **redisw/**: Redis wire protocol compatibility
- **postgresw/**: PostgreSQL wire protocol compatibility
- **bolt/**: Neo4j Bolt wire protocol compatibility
- **grpc/**: gRPC protocol definitions
- **grpcw/**: gRPC wire protocol module
- **grpc-client/**: gRPC client library

### Key Engine Components
- **Database Management**: `com.arcadedb.database.*` - Database lifecycle, transactions, ACID compliance
- **Storage Engine**: `com.arcadedb.engine.*` - Low-level storage, page management, WAL
- **SQL Query Engine**: `com.arcadedb.query.sql.*` - SQL query parsing, execution planning
- **OpenCypher Engine**: `com.arcadedb.query.opencypher.*` - Native Cypher implementation with ANTLR parser, AST, optimizer (filter pushdown, index selection, expand-into, join ordering), and step-based execution. Has both optimizer and legacy execution paths — changes to clause handling may need updates in multiple paths
- **Polyglot Engine**: `com.arcadedb.query.polyglot.*` - GraalVM-based scripting support
- **Schema Management**: `com.arcadedb.schema.*` - Type definitions, property management
- **Index System**: `com.arcadedb.index.*` - LSM-Tree indexes, full-text, vector indexes
- **Graph Engine**: `com.arcadedb.graph.*` - Vertex/Edge management, graph traversals
- **Serialization**: `com.arcadedb.serializer.*` - Binary serialization, JSON handling

### Server Components
- **HTTP API**: `com.arcadedb.server.http.*` - REST endpoints, request handling
- **High Availability**: `com.arcadedb.server.ha.*` - Clustering, replication, leader election
- **Security**: `com.arcadedb.server.security.*` - Authentication, authorization
- **Monitoring**: `com.arcadedb.server.monitor.*` - Metrics, query profiling, health checks
- **MCP**: `com.arcadedb.server.mcp.*` - Model Context Protocol server support

## Development Guidelines

### Java Version
- **Required**: Java 21+ (main branch)
- **Legacy**: Java 17 support on `java17` branch

### Code Structure
- Uses Maven multi-module project structure
- Low-level Java optimization for performance ("LLJ: Low Level Java")
- Minimal garbage collection pressure design
- Thread-safe implementations throughout

### Concurrency and Parallelism

**Core principle:** ArcadeDB avoids the JDK common ForkJoinPool (`ForkJoinPool.commonPool()`) for engine-internal parallelism. The common pool is shared with user-supplied code (Gremlin, Polyglot, custom SQL functions, application JVM) and JDK internals (parallel GC, reference handler), so long-running engine work submitted there starves user code and JDK housekeeping. Engine code that needs parallelism submits to one of the dedicated pools below; the rule is documented at the head of `com.arcadedb.query.QueryEngineManager`'s class Javadoc.

**Existing JDK-common-pool callers** (tagged in source with `NOTE (concurrency)` comments referencing the rule, will migrate as workloads justify it):
- `GraphBatch.parallelSort` (`engine/...graph/GraphBatch.java`)
- `ArcadeStateMachine.notifyInstallSnapshotFromLeader` (`ha-raft/...`)

**Dedicated thread pools:**

| Pool | Module | Purpose | Sizing | Saturation policy |
|---|---|---|---|---|
| `QueryEngineManager` JVM-wide pool | `engine` | Query-time parallelism: graph algorithms (`parallelForRange`), parallel index scans, anything that forks query work | `arcadedb.queryParallelismPoolThreads` (default `max(2, CPU)`) | Bounded queue (`arcadedb.queryParallelismQueueSize`, default 1024), caller-runs rejection, throttled WARNING (60s window) |
| `SparseVectorScoringPool` | `engine` | Reserved for per-segment parallel scoring of `LSM_SPARSE_VECTOR` top-K (dispatch wiring deferred to issue #4085) | Hardcoded `max(2, CPU)` threads, 1024 queue; lazy-init via Holder idiom | Bounded queue, caller-runs, throttled WARNING |
| `DatabaseAsyncExecutor` | `engine` | Per-database async ops (background scheduled tasks, async commit) | `arcadedb.asyncWorkerThreads` (default `CPU - 1`, min 1) | Bounded queue (`arcadedb.asyncOperationsQueueSize`, default 1024) |
| `PageManagerFlushThread` | `engine` | Dedicated single thread for paginated-component page writes | 1 thread | Backpressure via `arcadedb.maxRAMForPageRamUsageInMB` |
| `TransactionManager` Timer | `engine` | Periodic WAL housekeeping | Timer thread | n/a |
| `TimeSeriesEngine` pool | `engine` | Time-series rollup work | configurable | n/a |
| `BackupScheduler` | `server` | Cron-style backup jobs | scheduled executor | n/a |
| `MaterializedViewScheduler` | `server` | Cron-style MV refresh | scheduled executor | n/a |
| Raft HA pools | `ha-raft` | Leader election, log replication, snapshot install | configurable in Raft conf | n/a |
| Undertow IO + worker | `server` | HTTP request handling | hardcoded 500 worker threads | Undertow built-in |
| `ServerMonitor` | `server` | Periodic metric collection | scheduled executor | n/a |

**Lock-free read patterns** (used on the read hot paths):
- `AtomicReference<T[]>` swaps for the current snapshot of immutable artifacts. Example: `PaginatedSparseVectorEngine.segments` is an `AtomicReference<PaginatedSegmentReader[]>`; readers grab the snapshot at query start and stay on it for the whole query, while flush/compaction publishes a new array atomically.
- Per-dim `ConcurrentSkipListMap` / per-key `ConcurrentHashMap` for live mutable state. Example: `Memtable.postings` is `ConcurrentHashMap<Integer, ConcurrentSkipListMap<RID, Float>>` - lock-free `put`, weakly-consistent ordered iteration during flush.
- `AtomicLong` version probes for O(1) "has this changed since I last looked?" gates. Example: `FileManager.modificationCount` bumps on every `registerFile` / `dropFile`; `PaginatedSparseVectorEngine.refreshSegmentsFromFileManager` reads it on every `topK` and short-circuits without walking the file list when unchanged.
- Sealed segments are immutable after `b.finish()`; multiple readers share file handles via `PaginatedComponent`'s page cache without per-segment locks.

**Locks** (used sparingly, only for publish-side serialization):
- `ReentrantLock` per engine instance for the publish-side mutex. Example: `PaginatedSparseVectorEngine.mutatorLock` orders flush + compaction segment-array publications so a query never sees an inconsistent intermediate state. Readers do not take this lock - they read the `AtomicReference` snapshot.
- `PageManager`'s per-page latches for read/write coordination on mutable pages.
- HA replication uses `database.getWrappedDatabaseInstance().runWithCompactionReplication(...)` recording sessions; the recording itself is single-threaded per session and the resulting `SCHEMA_ENTRY` ships atomically.

**Metrics (Micrometer):**
- `PoolMetrics` MeterBinder registers six gauges per pool with a `pool=<name>` tag: `size`, `active`, `queue_depth`, `queue_capacity_remaining`, `completed_tasks`, `caller_run_fallbacks`.
- Studio surfaces these via the "Executor Pools" card on the Server tab (live numbers).
- A sustained non-zero `caller_run_fallbacks` indicates pool saturation; the engine logs a throttled WARNING the first time and at most once per 60 s afterward.

**When adding new parallelism to engine code:**
1. Pick the right home pool (`QueryEngineManager` for query-time, `SparseVectorScoringPool` if scoping to sparse vectors, etc.). If none fits, justify a new pool and add a `PoolMetrics` binding for it.
2. Bounded queue + caller-runs rejection so the system degrades to single-threaded under load instead of throwing `RejectedExecutionException` to callers.
3. Throttled WARNING on saturation (60-second window matching the existing pools) so operators notice without getting spammed.
4. Wire the pool into `PoolMetrics` so the Studio "Executor Pools" card shows it.
5. If using the JDK common ForkJoinPool is unavoidable in the short term, tag the call site with a `NOTE (concurrency)` comment pointing at this section so the migration stays tracked.

### Testing Approach
- **Framework**: JUnit 5 (Jupiter) with AssertJ assertions
- Unit tests in each module's `src/test/java`
- Integration tests with `IT` suffix
- Performance tests in `performance/` packages
- TestContainers used in `e2e/` and `load-tests/` modules for containerized testing
- Separate test databases in `databases/` for isolation

### Database Features to Consider
- **ACID Transactions**: Full transaction support with isolation levels
- **Multi-Model**: Single database can store graphs, documents, key/value pairs
- **Query Languages**: SQL (OrientDB-compatible), Cypher, Gremlin, MongoDB queries
- **Indexing**: LSM-Tree indexes, full-text (Lucene), vector embeddings
- **High Availability**: Leader-follower replication, automatic failover
- **Wire Protocols**: HTTP/JSON, PostgreSQL, MongoDB, Redis, Neo4j Bolt, gRPC compatibility

### Common Development Tasks

#### Adding New Features
1. Create tests first (TDD approach)
2. Implement in appropriate module
3. Update schema if needed
4. Add integration tests
5. Update documentation

#### Working with Indexes
- LSM-Tree implementation in `com.arcadedb.index.lsm.*`
- Index creation via Schema API
- Performance testing with large datasets recommended

#### Query Development
- SQL parsing in `com.arcadedb.query.sql.*`
- SQL execution plans in `com.arcadedb.query.sql.executor.*`
- OpenCypher engine in `com.arcadedb.query.opencypher.*` — has `ast/`, `parser/`, `executor/`, `optimizer/`, `planner/`, `rewriter/` sub-packages
- OpenCypher tests in `engine/src/test/java/com/arcadedb/query/opencypher/`
- Test with various query patterns and data sizes

#### Server Development
- HTTP handlers in `com.arcadedb.server.http.handler.*`
- Security integration required for new endpoints
- WebSocket support for real-time features

#### Wire Protocol Module Dependencies
- **Standard**: All wire protocol modules (gremlin, graphql, mongodbw, redisw, postgresw, bolt, grpcw) must use `provided` scope for `arcadedb-server` dependency
- **Rationale**: Server remains the assembly point; prevents dependency duplication in distributions
- **Pattern**:
  - Main server dependency → scope: `provided`
  - Server test-jar → scope: `test`
  - Cross-module test dependencies → scope: `test` only (e.g., postgresw should not depend on gremlin for compilation)
  - Integration/format handlers → scope: `compile` only if in `src/main/java` (e.g., gremlin's GraphML/GraphSON handlers)
- **Enforcement**: Code review process ensures:
  - Protocol modules do NOT depend on other protocol modules in compile scope
  - Each protocol module has arcadedb-server in `provided` scope only (not compile)
  - Only the server assembly (package module) and coverage reporting modules can aggregate protocol modules
- **Example**:
  ```xml
  <dependency>
      <groupId>com.arcadedb</groupId>
      <artifactId>arcadedb-server</artifactId>
      <scope>provided</scope>
  </dependency>
  <dependency>
      <groupId>com.arcadedb</groupId>
      <artifactId>arcadedb-server</artifactId>
      <version>${project.parent.version}</version>
      <scope>test</scope>
      <type>test-jar</type>
  </dependency>
  ```

## Important Notes

- **Pre-commit hooks**: This project uses pre-commit for code quality checks (trailing whitespace, Prettier for Java/XML formatting, etc.)
- **Code formatting**: Prettier with `requirePragma: true` and `printWidth: 160` — only formats files with a `@format` pragma
- **Security**: Never log or expose sensitive data (passwords, tokens, etc.)
- **Performance**: Always consider memory and CPU impact of changes
- **Compatibility**: Maintain backward compatibility for API changes
- **Licensing**: All code must comply with Apache 2.0 license
- **Modular Builder**: Script to create custom distributions with selected modules (see `package/README-BUILDER.md`)
