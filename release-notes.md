# ArcadeDB 26.2.1 Release Notes

We're excited to announce ArcadeDB 26.2.1, a massive release that brings **484 commits** and **200+ closed issues** with significant advancements across the platform. This release focuses on hardening the native [OpenCypher](https://docs.arcadedb.com/#cypher-introduction) engine with official TCK compliance, introducing the **Bolt protocol** for Neo4j driver compatibility, a new **plugin architecture**, a powerful **backup scheduler**, and major performance optimizations.

## 🎯 Major New Features

### Bolt Protocol (Neo4j Driver Compatibility)

ArcadeDB now supports the **Neo4j Bolt wire protocol** ([#3250](https://github.com/ArcadeData/arcadedb/pull/3250)), allowing applications to connect using standard Neo4j drivers. This opens ArcadeDB to the vast ecosystem of Neo4j-compatible tools and libraries:

- First version of the Bolt wire protocol implementation
- Compatible with the official Neo4j Java driver
- Correct protocol version encoding and handshake negotiation ([#3413](https://github.com/ArcadeData/arcadedb/pull/3413))
- Proper field name preservation for single-element Cypher RETURN over Bolt ([#3286](https://github.com/ArcadeData/arcadedb/pull/3286))

### Cypher LOAD CSV

Implemented the OpenCypher **LOAD CSV** clause ([#3450](https://github.com/ArcadeData/arcadedb/pull/3450)), enabling bulk data import from CSV files directly in Cypher queries, including context functions `file()` and `linenumber()`.

### New SQL Parser (ANTLR4)

Replaced the legacy JavaCC-based SQL parser with a modern **ANTLR4 implementation** ([#3195](https://github.com/ArcadeData/arcadedb/pull/3195)), providing:

- More robust and maintainable parsing infrastructure
- Better error messages and diagnostics
- Foundation for future SQL language extensions

### Plugin Architecture with Isolated Class Loaders

Introduced a new **modular plugin architecture** ([#3260](https://github.com/ArcadeData/arcadedb/pull/3260)) that supports isolated class loaders, enabling clean separation of plugin code from the core engine, dynamic loading/unloading at runtime, and reduced risk of dependency conflicts.

### Backup Scheduler Plugin

A new **backup scheduler plugin** ([#3263](https://github.com/ArcadeData/arcadedb/pull/3263)) allows automated, scheduled database backups with security validation — a highly requested feature for production deployments.

### SQL Triggers

Added support for **SQL triggers** ([#3222](https://github.com/ArcadeData/arcadedb/pull/3222)) that can execute SQL, JavaScript, and Java code, enabling powerful event-driven automation within the database.

### HTTP Session Management

Introduced **HTTP session support** with token-based authentication, including configurable session expiration, new `/login` and `/logout` APIs, and a list of active connections in Studio.

### Database Transient Variables via Redis

Added support for **transient database global variables** accessible through the Redis wire protocol ([#3248](https://github.com/ArcadeData/arcadedb/pull/3248)), and Redis can now be used as a query language.

## 🔐 OpenCypher Engine Hardening

This release brings massive improvements to the native OpenCypher engine, achieving the compliance with the OpenCypher TCK (Technology Compatibility Kit)**:

### TCK Compliance
- Integrated the official OpenCypher TCK test suite ([#3358](https://github.com/ArcadeData/arcadedb/pull/3358))
- Multiple rounds of TCK-driven fixes ([#3362](https://github.com/ArcadeData/arcadedb/pull/3362), [#3363](https://github.com/ArcadeData/arcadedb/pull/3363), [#3366](https://github.com/ArcadeData/arcadedb/pull/3366), [#3367](https://github.com/ArcadeData/arcadedb/pull/3367), [#3368](https://github.com/ArcadeData/arcadedb/pull/3368))
- Completed 100% temporal OpenCypher TCK compliance ([#3369](https://github.com/ArcadeData/arcadedb/pull/3369))

### New Cypher Functions
- Added many new Cypher functions ([#3275](https://github.com/ArcadeData/arcadedb/pull/3275))
- Math functions: `cosh()`, `sinh()`, `tanh()`, `cot()`, `coth()`, `pi()`, `e()`
- Utility functions: `isempty()`, `isnan()`, `randomuuid()`, `allReduce()`, `collflatten()`
- Spatial functions: `distance(point1, point2)` and `distance(point1, point2, unit)`
- User-defined functions in Cypher language with cross-language invocation
- Mega refactor consolidating all functions under `com.arcadedb.function` with merged SQL/Cypher function framework

### Cypher Bug Fixes
- **MERGE**: Label-only patterns now correctly find existing nodes instead of always creating new ones ([#3255](https://github.com/ArcadeData/arcadedb/pull/3255))
- **Relationship direction**: Relations created with MERGE and CREATE now respect arrow direction ([#3353](https://github.com/ArcadeData/arcadedb/pull/3353))
- **Label filtering**: Fixed label filtering in MATCH relationship patterns ([#3252](https://github.com/ArcadeData/arcadedb/pull/3252))
- **RETURN clause**: Single element results are now correctly unwrapped ([#3268](https://github.com/ArcadeData/arcadedb/pull/3268))
- **head(collect())**: Fixed returning null in WITH clauses ([#3309](https://github.com/ArcadeData/arcadedb/pull/3309), [#3310](https://github.com/ArcadeData/arcadedb/pull/3310))
- **Backtick handling**: Fixed backtick stripping in map literal keys ([#3322](https://github.com/ArcadeData/arcadedb/pull/3322))
- **PROFILE/EXPLAIN**: Fixed corrupting returned values ([#3406](https://github.com/ArcadeData/arcadedb/pull/3406))
- **CREATE path variables**: Fixed `CreateStep.createPath()` not building `TraversalPath` objects for path variables
- **Serialization**: Fixed result field mapping for non-Studio serializers ([#3392](https://github.com/ArcadeData/arcadedb/pull/3392))
- Fixed arithmetic operations, polymorphism handling, multiple parenthesized expressions, FOREACH, UNWIND with nested lists, list comprehension, pattern comprehension, zero-length paths, shortestPath, subquery, OPTIONAL MATCH with pattern predicates, and sort with null values
- Function fixes: `trim()`, `length(null)`, `avg()`, `round()` precision, `stdev()`/`stdevp()`, `toBoolean()` from integer, `timestamp()`, `collect()` on Studio

## ⚡ Performance Optimizations

Significant performance work across the OpenCypher engine and core:

- **Batch CREATE**: Implemented batch create in OpenCypher with configurable batch size (default 20K entries per transaction)
- **Chunked streaming**: Implemented chunked streaming with limit usage in ORDER BY
- **Edge traversal**: Skipped loading of edges when not necessary, loading connected vertices directly; inverted traversal optimization
- **Aggregation**: Optimized aggregation and GROUP BY when there is a LIMIT; optimized count of edges
- **Count optimization**: Improved performance for `count()` in OpenCypher
- **Function caching**: Cached function calls in OpenCypher for repeated invocations
- **Graph algorithms**: Dijkstra and A* now return list of RIDs instead of whole vertex objects; avoided unnecessary overhead
- **Spatial**: Optimized point insertion with a lightweight version; removed dynamic dependency with spatial4j for improved performance
- **Index selection**: Improved index selection; distinct is used to normalize avoiding cartesian product when possible
- **Memory**: Reduced allocation on common paths; using optimized `SingletonMap` when possible; replaced `new Random()` with `ThreadLocalRandom.current()`
- **Unsafe removal**: Replaced usage of `sun.misc.Unsafe` with `VarHandle` for modern Java compatibility

## 🚀 Vector Index Enhancements

- **Diagnostics logging**: Added vector graph build diagnostics for better observability ([#3305](https://github.com/ArcadeData/arcadedb/pull/3305))
- **Preload vectors API**: New API to preload vectors for faster search
- **OpenCypher integration**: Fixed vector functions accessibility from OpenCypher queries
- Upgraded to jvector 4.0.0-rc.7

## 🔧 Improvements & Bug Fixes

### Core Engine
- **Concurrent multi-page record reads**: Added automatic retry mechanism during concurrent modifications ([#3396](https://github.com/ArcadeData/arcadedb/pull/3396))
- **CREATE INDEX**: Now waits for any running async tasks before proceeding ([#3408](https://github.com/ArcadeData/arcadedb/pull/3408))
- **Index edge removal**: Fixed index remove edge for non-unique indexes
- **SQL CREATE EDGE**: Fixed argument expression resolution ([#3323](https://github.com/ArcadeData/arcadedb/pull/3323))
- **SQL concatenation**: Fixed second concatenation operator failing without parenthesis
- **Full-text index improvements** ([#3221](https://github.com/ArcadeData/arcadedb/pull/3221))
- **Memory leak fix**: Removed strong reference of `Thread` in `DatabaseContext.CONTEXTS`
- **Page isolation**: Fixed page isolation bug with concurrent access
- **Index rebuild**: Fixed async rebuild while another rebuild is in progress
- **Schema concurrency**: Fixed concurrency issue with schema
- **Export**: Fixed database export
- **Shapes support**: Added support for shapes in SQL
- **Truncate bucket**: Implemented `TRUNCATE BUCKET` command
- **JSON array in UPDATE**: Supported JSON array in SQL UPDATE
- **SQL buckets/page size**: SQL now supports setting buckets and page size at type creation
- **NULL index strategy**: Return NULL when index null strategy is set to skip
- **lookupByRID**: Ensured it always returns a record or exception, never null

### PostgreSQL Wire Protocol
- Fixed DATETIME serialization to use ISO 8601 format ([#3245](https://github.com/ArcadeData/arcadedb/pull/3245))
- Fixed PostgreSQL protocol issues with Node.js driver

### gRPC
- Tests and refactoring of gRPC client ([#3306](https://github.com/ArcadeData/arcadedb/pull/3306), [#3317](https://github.com/ArcadeData/arcadedb/pull/3317))
- Fixed gRPC context management and transaction management
- Fixed record creation for edges
- Fixed server not found exception handling

### HTTP Server
- Upgraded Undertow to 2.3.22.Final for large content handling ([#3319](https://github.com/ArcadeData/arcadedb/pull/3319))
- Added `autoCommit` parameter on `/command` and `/query` HTTP API endpoints
- `/server` endpoint now returns the available query languages installed
- Disabled HTML escaping in JSON content

### Gremlin
- Upgraded Apache TinkerPop Gremlin from 3.7.4 to 3.8.0 ([#3209](https://github.com/ArcadeData/arcadedb/pull/3209))
- Gremlin now supports multiple databases

### Studio
- Improved security level ([#3356](https://github.com/ArcadeData/arcadedb/pull/3356))
- Improved layout of query panel
- History browser with keyboard shortcuts
- New `/login` and `/logout` APIs with list of active connections
- Fixed settings dropdown
- Fixed resultset with UNWIND collapsed with Studio serializer (same RID)

### Python Bindings
- Synced Python bindings with upstream ([#3270](https://github.com/ArcadeData/arcadedb/pull/3270))
- Updated documentation links ([#3272](https://github.com/ArcadeData/arcadedb/pull/3272))

### Scripting
- Added `time` object to JavaScript (GraalVM) scope
- Upgraded GraalVM from 24.2.2 to 25.0.2

### Testing & Quality
- Added LDBC-inspired graph benchmark ([#3410](https://github.com/ArcadeData/arcadedb/pull/3410))
- Integrated test coverage upload to Codecov ([#3308](https://github.com/ArcadeData/arcadedb/pull/3308))
- License review and compliance with automatic check action ([#3364](https://github.com/ArcadeData/arcadedb/pull/3364))
- Migrated assertions to AssertJ ([#3283](https://github.com/ArcadeData/arcadedb/pull/3283), [#3312](https://github.com/ArcadeData/arcadedb/pull/3312))
- Replaced SLF4J logback with JUL bridge
- Enhanced OpenCypher profiling with step-by-step output and inter-step timing
- Significantly increased code coverage across multiple modules

## 📦 Dependencies Updated

Updated 130+ dependencies including security patches. Notable upgrades:

- **Apache TinkerPop Gremlin** 3.7.4 → 3.8.0
- **GraalVM** 24.2.2 → 25.0.2
- **Undertow** 2.3.20.Final → 2.3.23.Final
- **Netty** 4.2.9.Final → 4.2.10.Final
- **Jackson Databind** 2.20.1 → 2.21.0
- **gRPC** 1.78.0 → 1.79.0
- **Logback** 1.5.22 → 1.5.31
- **Protobuf Java** 4.33.2 → 4.33.5
- **PostgreSQL JDBC** 42.7.8 → 42.7.10
- **Cucumber** 7.22.1 → 7.34.2
- **Micrometer** 1.16.1 → 1.16.3
- **jQuery** 3.7.1 → 4.0.0 (Studio)
- Various GitHub Actions, Docker base images, and Studio frontend dependencies

## 📝 Full Changelog

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.1.1...26.2.1

**Closed Issues**: 200+ issues resolved — see [milestone 26.2.1](https://github.com/ArcadeData/arcadedb/milestone/50?closed=1) for details
