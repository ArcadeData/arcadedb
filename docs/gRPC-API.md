# gRPC API

## Overview

ArcadeDB provides a high-performance gRPC API for database operations. The gRPC interface offers several advantages over REST:

- **Streaming**: Stream large result sets with minimal latency (0-3ms to first row vs 175ms+ for batch JSON)
- **Binary Protocol**: Efficient Protobuf serialization reduces payload sizes
- **Multiplexing**: Single HTTP/2 connection handles multiple concurrent requests
- **Type Safety**: Generated clients with compile-time type checking

## Quick Start

### Server Configuration

Enable gRPC in your server configuration:

```json
{
  "grpc": {
    "enabled": true,
    "port": 50051,
    "useSSL": false
  }
}
```

Or via command line:

```bash
./bin/server.sh -Darcadedb.grpc.enabled=true -Darcadedb.grpc.port=50051
```

### Proto File

The service definitions are in `grpc/src/main/proto/arcadedb-server.proto`. Generate clients using:

```bash
# Java (Maven)
mvn compile -pl grpc

# Other languages - use protoc
protoc --java_out=. --grpc-java_out=. arcadedb-server.proto
```

## Authentication

ArcadeDB gRPC supports two authentication methods:

### 1. Username/Password (per-request)

Include credentials in gRPC metadata headers:

```
x-arcade-user: root
x-arcade-password: yourpassword
x-arcade-database: mydb
```

**Java Example:**

```java
Metadata headers = new Metadata();
headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), "password");
headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "mydb");

Channel channel = ClientInterceptors.intercept(
    ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build(),
    new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, options)) {
                @Override
                public void start(Listener<RespT> listener, Metadata md) {
                    md.merge(headers);
                    super.start(listener, md);
                }
            };
        }
    });

ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(channel);
```

### 2. Bearer Token Authentication

Obtain a session token via HTTP login, then use it for gRPC requests. This is useful for:

- Long-running connections that shouldn't resend credentials
- Unified authentication across HTTP and gRPC
- Session management with automatic expiration

**Step 1: Login via HTTP**

```bash
curl -X POST http://localhost:2480/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"root","password":"yourpassword"}'
```

Response:
```json
{
  "token": "AU-550e8400-e29b-41d4-a716-446655440000"
}
```

**Step 2: Use token for gRPC**

Include the token in the `Authorization` header:

```
Authorization: Bearer AU-550e8400-e29b-41d4-a716-446655440000
x-arcade-database: mydb
```

**Java Example:**

```java
String token = "AU-550e8400-e29b-41d4-a716-446655440000";

ClientInterceptor tokenInterceptor = new ClientInterceptor() {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(
            next.newCall(method, options)) {
            @Override
            public void start(Listener<RespT> listener, Metadata headers) {
                headers.put(
                    Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                    "Bearer " + token);
                headers.put(
                    Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER),
                    "mydb");
                super.start(listener, headers);
            }
        };
    }
};

Channel channel = ClientInterceptors.intercept(baseChannel, tokenInterceptor);
```

**Token Lifecycle:**

| Event | Behavior |
|-------|----------|
| Login | Token created with configurable timeout |
| Activity | Token timeout resets on each request |
| Logout | Token invalidated for both HTTP and gRPC |
| Expiration | Requests return `UNAUTHENTICATED` |

**Logout:**

```bash
curl -X POST http://localhost:2480/api/v1/logout \
  -H "Authorization: Bearer AU-550e8400-e29b-41d4-a716-446655440000"
```

### Authentication Errors

| gRPC Status | Description |
|-------------|-------------|
| `UNAUTHENTICATED` | No credentials, invalid token, or expired session |
| `PERMISSION_DENIED` | Valid credentials but insufficient permissions |

## Services

### ArcadeDbService

Main service for database operations.

#### StreamQuery

Stream query results with server-side cursor. Ideal for large result sets.

```protobuf
rpc StreamQuery (StreamQueryRequest) returns (stream QueryResult);
```

**Example:**

```java
StreamQueryRequest request = StreamQueryRequest.newBuilder()
    .setDatabase("mydb")
    .setQuery("SELECT FROM Person")
    .setLanguage("sql")
    .setBatchSize(100)
    .build();

Iterator<QueryResult> results = stub.streamQuery(request);
while (results.hasNext()) {
    QueryResult batch = results.next();
    for (GrpcRecord record : batch.getRecordsList()) {
        System.out.println(record.getRid() + ": " + record.getPropertiesMap());
    }
}
```

**Retrieval Modes:**

| Mode | Description |
|------|-------------|
| `CURSOR` | Stream as database iterates (default, lowest memory) |
| `MATERIALIZE_ALL` | Load all results, then stream in batches |
| `PAGED` | Re-execute query with LIMIT/SKIP per batch |

#### ExecuteCommand

Execute DDL, INSERT, UPDATE, DELETE commands.

```protobuf
rpc ExecuteCommand (ExecuteCommandRequest) returns (ExecuteCommandResponse);
```

**Example:**

```java
ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
    .setDatabase("mydb")
    .setCommand("INSERT INTO Person SET name = :name, age = :age")
    .putParameters("name", GrpcValue.newBuilder().setStringValue("Alice").build())
    .putParameters("age", GrpcValue.newBuilder().setInt32Value(30).build())
    .build();

ExecuteCommandResponse response = stub.executeCommand(request);
System.out.println("Affected: " + response.getAffectedRecords());
```

#### ExecuteQuery

Execute a query and return all results (non-streaming).

```protobuf
rpc ExecuteQuery(ExecuteQueryRequest) returns (ExecuteQueryResponse);
```

#### CRUD Operations

| RPC | Description |
|-----|-------------|
| `CreateRecord` | Create a new record |
| `UpdateRecord` | Update existing record (full or partial) |
| `LookupByRid` | Fetch record by RID |
| `DeleteRecord` | Delete record by RID |

**Create Example:**

```java
GrpcRecord record = GrpcRecord.newBuilder()
    .setType("Person")
    .putProperties("name", GrpcValue.newBuilder().setStringValue("Bob").build())
    .putProperties("age", GrpcValue.newBuilder().setInt32Value(25).build())
    .build();

CreateRecordRequest request = CreateRecordRequest.newBuilder()
    .setDatabase("mydb")
    .setType("Person")
    .setRecord(record)
    .build();

CreateRecordResponse response = stub.createRecord(request);
System.out.println("Created: " + response.getRid());
```

#### Bulk Insert

Three insert patterns for different use cases:

| RPC | Pattern | Use Case |
|-----|---------|----------|
| `BulkInsert` | Unary | Small batches (<1000 records) |
| `InsertStream` | Client streaming | Large imports, memory-efficient |
| `InsertBidirectional` | Bidi streaming | Per-batch acknowledgments, progress tracking |

**Conflict Handling:**

```java
InsertOptions options = InsertOptions.newBuilder()
    .setTargetClass("Person")
    .addKeyColumns("email")
    .setConflictMode(InsertOptions.ConflictMode.CONFLICT_UPDATE)
    .build();
```

| Mode | Behavior |
|------|----------|
| `CONFLICT_ERROR` | Fail on duplicate (default) |
| `CONFLICT_UPDATE` | Upsert - update existing |
| `CONFLICT_IGNORE` | Skip duplicates silently |
| `CONFLICT_ABORT` | Abort entire batch on conflict |

#### Transactions

Explicit transaction control:

```java
// Begin
BeginTransactionRequest beginReq = BeginTransactionRequest.newBuilder()
    .setDatabase("mydb")
    .setIsolation(TransactionIsolation.READ_COMMITTED)
    .build();
BeginTransactionResponse beginResp = stub.beginTransaction(beginReq);
String txId = beginResp.getTransactionId();

// Execute operations with transaction context
TransactionContext txCtx = TransactionContext.newBuilder()
    .setTransactionId(txId)
    .build();

ExecuteCommandRequest cmdReq = ExecuteCommandRequest.newBuilder()
    .setDatabase("mydb")
    .setCommand("INSERT INTO Person SET name = 'Test'")
    .setTransaction(txCtx)
    .build();
stub.executeCommand(cmdReq);

// Commit
CommitTransactionRequest commitReq = CommitTransactionRequest.newBuilder()
    .setTransaction(txCtx)
    .build();
stub.commitTransaction(commitReq);
```

### ArcadeDbAdminService

Server and database administration.

| RPC | Description |
|-----|-------------|
| `Ping` | Health check |
| `GetServerInfo` | Server version, ports, uptime |
| `ListDatabases` | List all databases |
| `ExistsDatabase` | Check if database exists |
| `CreateDatabase` | Create new database |
| `DropDatabase` | Delete database |
| `GetDatabaseInfo` | Database statistics |
| `CreateUser` | Create user account |
| `DeleteUser` | Delete user account |

**Note:** Admin service uses credentials in the request body, not headers.

```java
ListDatabasesRequest request = ListDatabasesRequest.newBuilder()
    .setCredentials(DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword("password")
        .build())
    .build();

ListDatabasesResponse response = adminStub.listDatabases(request);
System.out.println("Databases: " + response.getDatabasesList());
```

## Data Types

### GrpcValue

Universal value container supporting all ArcadeDB types:

| Type | Proto Field | Java Type |
|------|-------------|-----------|
| Boolean | `bool_value` | `boolean` |
| Integer | `int32_value` | `int` |
| Long | `int64_value` | `long` |
| Float | `float_value` | `float` |
| Double | `double_value` | `double` |
| String | `string_value` | `String` |
| Binary | `bytes_value` | `byte[]` |
| Timestamp | `timestamp_value` | `Date` |
| Decimal | `decimal_value` | `BigDecimal` |
| List | `list_value` | `List<Object>` |
| Map | `map_value` | `Map<String, Object>` |
| Embedded | `embedded_value` | Embedded document |
| Link | `link_value` | RID reference |

### GrpcRecord

Represents a vertex, edge, or document:

```protobuf
message GrpcRecord {
  string rid  = 1;  // e.g., "#12:0"
  string type = 2;  // ArcadeDB type name
  map<string, GrpcValue> properties = 3;
}
```

## Error Handling

gRPC status codes used by ArcadeDB:

| Status | Meaning |
|--------|---------|
| `OK` | Success |
| `UNAUTHENTICATED` | Authentication failed |
| `PERMISSION_DENIED` | Insufficient permissions |
| `NOT_FOUND` | Database or record not found |
| `INVALID_ARGUMENT` | Bad request parameters |
| `INTERNAL` | Server error |
| `UNAVAILABLE` | Server overloaded or shutting down |

**Handling errors:**

```java
try {
    response = stub.executeQuery(request);
} catch (StatusRuntimeException e) {
    switch (e.getStatus().getCode()) {
        case UNAUTHENTICATED:
            // Re-authenticate
            break;
        case NOT_FOUND:
            // Handle missing resource
            break;
        default:
            // Log and retry or fail
    }
}
```

## Performance Considerations

### Connection Pooling

Reuse channels - they're thread-safe and multiplex requests:

```java
// Create once, reuse everywhere
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 50051)
    .usePlaintext()
    .build();

// Multiple stubs can share a channel
ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub1 = ArcadeDbServiceGrpc.newBlockingStub(channel);
ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub2 = ArcadeDbServiceGrpc.newBlockingStub(channel);
```

### Streaming vs Unary

| Use Case | Recommended Pattern |
|----------|---------------------|
| < 100 rows | `ExecuteQuery` (unary) |
| 100 - 10,000 rows | `StreamQuery` with `batchSize=100` |
| > 10,000 rows | `StreamQuery` with `batchSize=500-1000` |
| Large imports | `InsertStream` or `InsertBidirectional` |

### Compression

Enable gzip compression for large payloads:

```java
stub.withCompression("gzip").executeQuery(request);
```

## TLS Configuration

Enable TLS for production:

```json
{
  "grpc": {
    "enabled": true,
    "port": 50051,
    "useSSL": true,
    "sslCertPath": "/path/to/cert.pem",
    "sslKeyPath": "/path/to/key.pem"
  }
}
```

Client configuration:

```java
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 50051)
    .useTransportSecurity()
    .build();
```

## See Also

- [REST API Documentation](https://docs.arcadedb.com/api/rest)
- [gRPC Support Case](../grpc/gRPC-Support-Case.md) - Performance benchmarks and business case
- [Proto File](../grpc/src/main/proto/arcadedb-server.proto) - Full service definitions
