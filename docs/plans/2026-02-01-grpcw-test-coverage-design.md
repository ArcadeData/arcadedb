# gRPC Wire Protocol Module Test Coverage Design

## Overview

This document describes the test strategy for adding comprehensive test coverage to the `grpcw` module, which currently has no tests.

## Current State

The grpcw module contains:
- **ArcadeDbGrpcService** - Main database service (~15 RPC methods)
- **ArcadeDbGrpcAdminService** - Admin service (~8 RPC methods)
- **GrpcServerPlugin** - Server plugin that starts the gRPC server
- **Interceptors** - GrpcAuthInterceptor, GrpcLoggingInterceptor, GrpcMetricsInterceptor, GrpcCompressionInterceptor

## Test Strategy

**Approach:** Unit tests + Integration tests

- Integration tests follow the `BaseGraphServerTest` pattern (same as MongoDBServerTest)
- Unit tests for interceptors and plugin configuration using mocks and gRPC testing utilities
- Priority: Core service operations first, then admin service, then interceptors

## File Organization

```
grpcw/src/test/java/com/arcadedb/server/grpc/
├── GrpcServerIT.java              # Integration tests for ArcadeDbGrpcService
├── GrpcAdminServiceIT.java        # Integration tests for ArcadeDbGrpcAdminService
├── GrpcAuthInterceptorTest.java   # Unit tests for authentication
├── GrpcServerPluginTest.java      # Unit tests for plugin configuration
└── GrpcCompressionInterceptorTest.java  # Unit tests for compression
```

## Dependencies to Add

Add to `grpcw/pom.xml`:

```xml
<dependency>
    <groupId>com.arcadedb</groupId>
    <artifactId>arcadedb-server</artifactId>
    <version>${project.parent.version}</version>
    <scope>test</scope>
    <type>test-jar</type>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-testing</artifactId>
    <version>${grpc.version}</version>
    <scope>test</scope>
</dependency>
```

## Integration Test Base Pattern

```java
public class GrpcServerIT extends BaseGraphServerTest {
    private static final int GRPC_PORT = 50051;
    private ManagedChannel channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blockingStub;

    @Override
    public void setTestConfiguration() {
        super.setTestConfiguration();
        GlobalConfiguration.SERVER_PLUGINS.setValue(
            "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    }

    @BeforeEach
    void setupGrpcClient() {
        channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT)
            .usePlaintext()
            .build();
        blockingStub = ArcadeDbServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardownGrpcClient() {
        if (channel != null) channel.shutdownNow();
    }
}
```

## Test Coverage Matrix

### GrpcServerIT - Main Service Tests

| Method | Test Cases |
|--------|------------|
| `executeCommand` | Insert document, DDL command, with/without credentials, invalid SQL |
| `executeQuery` | Select query, query with parameters, empty result, large result set |
| `createRecord` | Create vertex, create document, duplicate key handling |
| `lookupByRid` | Found record, not found, invalid RID format |
| `updateRecord` | Full replacement, partial update, non-existent record |
| `deleteRecord` | Delete existing, delete non-existent |
| `beginTransaction` | Start transaction, get transaction ID |
| `commitTransaction` | Commit changes visible |
| `rollbackTransaction` | Rollback changes discarded |
| `streamQuery` | Streaming large results, batch size handling, cancellation |
| `bulkInsert` | Batch insert, conflict modes (error, update, ignore) |

### GrpcAdminServiceIT - Admin Service Tests

| Method | Test Cases |
|--------|------------|
| `ping` | Successful ping with valid credentials, ping without credentials (should fail) |
| `getServerInfo` | Returns version, ports, database count, uptime |
| `listDatabases` | Lists existing databases |
| `existsDatabase` | Returns true for existing db, false for non-existent |
| `createDatabase` | Create new database, create with "graph" type, idempotent |
| `dropDatabase` | Drop existing database, idempotent |
| `getDatabaseInfo` | Returns record count, class count, index count, type |
| `createUser` / `deleteUser` | User management |

### Authentication Tests (Cross-cutting)

- Valid credentials → success
- Invalid credentials → `UNAUTHENTICATED` status
- Missing credentials → `UNAUTHENTICATED` status

### Unit Tests

**GrpcAuthInterceptorTest:**
- Valid credentials allows request
- Missing credentials rejects request
- Invalid credentials rejects request
- Malformed auth header rejects request

**GrpcCompressionInterceptorTest:**
- Force compression sets compressor
- Respects min size threshold
- Supports gzip compression

**GrpcServerPluginTest:**
- Default port is 50051
- Disabled plugin does not start server
- Custom port configuration
- TLS configuration with missing cert falls back to insecure
- XDS mode starts both servers

## Implementation Order

1. Update pom.xml with test dependencies
2. GrpcAdminServiceIT - ping, listDatabases, existsDatabase tests
3. GrpcServerIT - Core CRUD operations (createRecord, lookupByRid, executeQuery)
4. GrpcServerIT - Transaction tests (begin, commit, rollback)
5. GrpcServerIT - Streaming and bulk insert tests
6. Unit tests - Interceptors and plugin configuration

## Helper Methods

```java
private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
}

private GrpcValue stringValue(String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
}

private GrpcValue intValue(int i) {
    return GrpcValue.newBuilder().setInt32Value(i).build();
}
```

## Estimated Test Count

- GrpcAdminServiceIT: ~12 tests
- GrpcServerIT: ~25 tests
- Unit tests: ~15 tests
- **Total: ~52 tests**

## Success Criteria

- All tests pass
- Tests follow existing project conventions (AssertJ assertions, `assertThat().isTrue()` style)
- No System.out statements in test code
- Tests clean up after themselves (databases, connections)
- Integration tests can run independently or as part of the full suite
