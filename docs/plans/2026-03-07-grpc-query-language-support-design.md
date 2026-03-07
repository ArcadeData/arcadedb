# gRPC Query Language Support

## Problem

The gRPC client (`RemoteGrpcDatabase`) ignores the `language` parameter for query operations. Only `command()` correctly propagates the language. This prevents using Cypher, Gremlin, or any non-SQL language through the gRPC query and streaming query paths.

The bug spans three layers:

1. **Proto**: `ExecuteQueryRequest` is missing a `language` field entirely
2. **Client**: `query()` can't pass language (proto missing it); `queryStream()` and `streamQuery()` never call `.setLanguage()` despite the proto supporting it
3. **Server**: `executeQuery()` hardcodes `db.query("sql", ...)`; all three `streamQuery` modes (`streamCursor`, `streamMaterialized`, `streamPaged`) hardcode `db.query("sql", ...)`

## Design Decisions

- Add a `language` field to `ExecuteQueryRequest` (additive, backward-compatible)
- Keep using `db.query(language, ...)` on the server for the Query RPC (caller chose read-only)
- Default to `"sql"` when the language field is empty/unset (backward-compatible)

## Changes

### 1. Proto (`grpc/src/main/proto/arcadedb-server.proto`)

Add `string language = 9;` to `ExecuteQueryRequest`:

```protobuf
message ExecuteQueryRequest {
  string database = 1;
  string query = 2;
  map<string, GrpcValue> parameters = 3;
  DatabaseCredentials credentials = 4;
  TransactionContext transaction = 5;
  int32 limit = 6;
  int32 timeout_ms = 7;
  ProjectionSettings projectionSettings = 8;
  string language = 9; // "sql" if empty (default)
}
```

### 2. Server (`grpcw/.../ArcadeDbGrpcService.java`)

**`executeQuery()`**: Replace hardcoded `"sql"` with language from request, defaulting to `"sql"` when empty.

**`streamQuery()`**: Extract language from `StreamQueryRequest.getLanguage()` (proto field 7, already exists), resolve default, and pass to `streamCursor`/`streamMaterialized`/`streamPaged`. Each mode method gains a `String language` parameter.

### 3. Client (`grpc-client/.../RemoteGrpcDatabase.java`)

- `query()` path (line 556): Add `.setLanguage(language)` to `ExecuteQueryRequest` builder
- `queryStream()` path (line 780): Add `.setLanguage(language)` to `StreamQueryRequest` builder
- Private `streamQuery()` (line 1767): Add `.setLanguage("sql")` since it's SQL-only by design

### Testing

- Existing gRPC e2e tests verify backward compatibility (SQL still works)
- Add test that runs a query with a non-SQL language through `query()` and `queryStream()` to verify language propagation
