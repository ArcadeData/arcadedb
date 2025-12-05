# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcadeDB is a Multi-Model DBMS (Database Management System) built for extreme performance. It's a Java-based project that supports multiple data models (Graph, Document, Key/Value, Search Engine, Time Series, Vector Embedding) and query languages (SQL, Cypher, Gremlin, GraphQL, MongoDB Query Language).

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

### Testing Commands
- **Run specific test class**: `mvn test -Dtest=ClassName`
- **Run tests with specific pattern**: `mvn test -Dtest="*Pattern*"`
- **Performance tests**: Located in `src/test/java/performance/` packages

## Architecture Overview

### Core Modules
- **engine/**: Core database engine, storage, indexing, query execution
- **server/**: HTTP/REST API, WebSocket support, clustering/HA functionality
- **studio/**: Web-based administration interface (JavaScript/Node.js)
- **gremlin/**: Apache Tinkerpop Gremlin support
- **mongodbw/**: MongoDB wire protocol compatibility
- **redisw/**: Redis wire protocol compatibility
- **graphql/**: GraphQL API support
- **postgresw/**: PostgreSQL wire protocol compatibility
- **network/**: Network communication layer

### Key Engine Components
- **Database Management**: `com.arcadedb.database.*` - Database lifecycle, transactions, ACID compliance
- **Storage Engine**: `com.arcadedb.engine.*` - Low-level storage, page management, WAL
- **Query Processing**: `com.arcadedb.query.sql.*` - SQL query parsing, execution planning
- **Schema Management**: `com.arcadedb.schema.*` - Type definitions, property management
- **Index System**: `com.arcadedb.index.*` - LSM-Tree indexes, full-text, vector indexes
- **Graph Engine**: `com.arcadedb.graph.*` - Vertex/Edge management, graph traversals
- **Serialization**: `com.arcadedb.serializer.*` - Binary serialization, JSON handling

### Server Components
- **HTTP API**: `com.arcadedb.server.http.*` - REST endpoints, request handling
- **High Availability**: `com.arcadedb.server.ha.*` - Clustering, replication, leader election
- **Security**: `com.arcadedb.server.security.*` - Authentication, authorization
- **Monitoring**: `com.arcadedb.server.monitor.*` - Metrics, health checks

## Development Guidelines

### Java Version
- **Required**: Java 21+ (main branch)
- **Legacy**: Java 17 support on `java17` branch

### Code Structure
- Uses Maven multi-module project structure
- Low-level Java optimization for performance ("LLJ: Low Level Java")
- Minimal garbage collection pressure design
- Thread-safe implementations throughout

### Testing Approach
- Unit tests in each module's `src/test/java`
- Integration tests with `IT` suffix
- Performance tests in `performance/` packages
- Use TestContainers for external service testing
- Separate test databases in `databases/` for isolation

### Database Features to Consider
- **ACID Transactions**: Full transaction support with isolation levels
- **Multi-Model**: Single database can store graphs, documents, key/value pairs
- **Query Languages**: SQL (OrientDB-compatible), Cypher, Gremlin, MongoDB queries
- **Indexing**: LSM-Tree indexes, full-text (Lucene), vector embeddings
- **High Availability**: Leader-follower replication, automatic failover
- **Wire Protocols**: HTTP/JSON, PostgreSQL, MongoDB, Redis compatibility

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
- Execution plans in `com.arcadedb.query.sql.executor.*`
- Test with various query patterns and data sizes

#### Server Development
- HTTP handlers in `com.arcadedb.server.http.handler.*`
- Security integration required for new endpoints
- WebSocket support for real-time features

#### Wire Protocol Module Dependencies
- **Standard**: All wire protocol modules (gremlin, graphql, mongodbw, redisw, postgresw, grpcw) must use `provided` scope for `arcadedb-server` dependency
- **Rationale**: Server remains the assembly point; prevents dependency duplication in distributions
- **Pattern**:
  - Main server dependency → scope: `provided`
  - Server test-jar → scope: `test`
  - Cross-module test dependencies → scope: `test` only (e.g., postgresw should not depend on gremlin for compilation)
  - Integration/format handlers → scope: `compile` only if in `src/main/java` (e.g., gremlin's GraphML/GraphSON handlers)
- **Enforcement**: Maven Enforcer rules in root pom.xml prevent:
  - Protocol modules depending on other protocol modules in compile scope
  - Any protocol module using compile scope for arcadedb-server (must be provided)
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

- **Pre-commit hooks**: This project uses pre-commit for code quality checks
- **Security**: Never log or expose sensitive data (passwords, tokens, etc.)
- **Performance**: Always consider memory and CPU impact of changes
- **Compatibility**: Maintain backward compatibility for API changes
- **Licensing**: All code must comply with Apache 2.0 license
