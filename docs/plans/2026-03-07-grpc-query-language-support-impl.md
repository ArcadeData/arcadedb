# gRPC Query Language Support - Implementation Plan

## Step 1: Proto change

- File: `grpc/src/main/proto/arcadedb-server.proto`
- Add `string language = 9;` to `ExecuteQueryRequest` (after `projectionSettings`)
- Rebuild proto: `cd grpc && mvn clean install -DskipTests`

## Step 2: Server - `executeQuery()` language support

- File: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`
- In `executeQuery()` (~line 823): extract language from `request.getLanguage()`, default to `"sql"`
- Replace `db.query("sql", ...)` with `db.query(language, ...)`

## Step 3: Server - `streamQuery()` language support

- File: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`
- In `streamQuery()`: extract language from `request.getLanguage()`, default to `"sql"`
- Add `String language` parameter to `streamCursor()`, `streamMaterialized()`, `streamPaged()`
- Replace hardcoded `"sql"` in each mode's `db.query()` call
- Build server: `cd grpcw && mvn clean install -DskipTests`

## Step 4: Client - wire language through query paths

- File: `grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java`
- `query()` at line 556: add `.setLanguage(language)` to `ExecuteQueryRequest` builder
- `queryStream()` at line 780: add `.setLanguage(language)` to `StreamQueryRequest` builder
- Private `streamQuery()` at line 1767: add `.setLanguage("sql")` to `StreamQueryRequest` builder
- Build client: `cd grpc-client && mvn clean install -DskipTests`

## Step 5: Test

- Add e2e test verifying a non-SQL query (e.g. Cypher `MATCH (n) RETURN n LIMIT 1`) works via gRPC `query()`
- Run existing gRPC e2e tests to verify no regressions
