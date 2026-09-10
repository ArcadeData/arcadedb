# #7308 - gRPC control plane: restore backup, restore database, import database

Issue: https://github.com/ArcadeData/arcadedb/issues/7308
Branch: `feat/7308-grpc-restore-import-rpcs`

Follow-up to #7304, which moved every *exchange-free* control-plane operation into
`com.arcadedb.server.ServerControlPlane` and exposed it on `ArcadeDbAdminService`. The three
operations left behind were the ones whose HTTP handler writes progress straight to the
`HttpServerExchange`.

## Invariant

**Restore and import run one transport-independent implementation, and every progress event that
implementation emits reaches the caller through a sink the transport supplies - an SSE frame over
HTTP, a message on a server-streaming RPC over gRPC - with the same authorization, leader and
URL-safety gates applied on both.**

The corollary that matters for security: a caller-supplied URL and a caller-supplied database name
are validated inside the shared implementation, so the new transport cannot ship a fresh SSRF or
path-traversal sink by forgetting a pre-check the HTTP handler happened to do first.

## Completeness

### Enumeration (commands run in this session)

```
$ grep -n "exchange)" server/.../PostServerCommandHandler.java | grep -i "restore\|import\|performRestore\|isSSE"
183  restoreBackup(..., payload, exchange)      459  private restoreBackup(...)
187  restoreDatabase(..., exchange)             422  private restoreDatabase(...)
189  importDatabase(..., exchange)              694  private importDatabase(...)
449/496 performRestore(...)                     510  private performRestore(...)
523/734 isSSERequested(exchange)                824  private static isSSERequested(...)
                                       -> exactly 3 commands, 2 SSE branches, 1 shared performRestore
```

```
$ sed -n '138,144p' server/.../PostServerCommandHandler.java     # leader-forwarded set
CREATE_DATABASE DROP_DATABASE CREATE_USER DROP_USER RESTORE_BACKUP RESTORE_DATABASE IMPORT_DATABASE
                                       -> all three of this issue's commands are leader-only over HTTP
```

```
$ grep -n "returns (stream" grpc/src/main/proto/arcadedb-server.proto
157 StreamQuery   172 InsertBidirectional   196 TimeSeriesQuery
                                       -> no streaming RPC on ArcadeDbAdminService before this change
```

```
$ grep -rn "isBlockedHost\|PostServerCommandHandler\." server/src/test grpcw/src/test
PostServerCommandHandlerSsrfTest.java: 12 assertions on PostServerCommandHandler.isBlockedHost
                                       -> the static must keep working; it now delegates to the control plane
```

```
$ grep -rn "restore database\|import database\|restore backup" server console engine network --include=*.java | grep -v /target/ | grep -v src/test
ArcadeDBServer.java:1431          startup '-Darcadedb.server.defaultDatabases' import - SQL, not this command
Console.java:1081                 console autocompletion list - HTTP client, unchanged
GlobalConfiguration.java:1445     SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS doc
ImportDatabaseStatement.java:82   SQL 'import database', reached via the sync import branch
CoreApiSpec.java:122              OpenAPI description of POST /api/v1/server - unchanged, routes already exist
```

```
$ grep -n "checkDatabaseNameIsValid" server/.../PostServerCommandHandler.java
439   restoreDatabase only
                                       -> 'restore backup ... as <target>' never validated its target name
```

### Entry-point coverage table

| Entry point | gRPC RPC / disposition | Covered by fix? | Covered by a test? |
|---|---|---|---|
| `restore backup <db> <file> as <target>`, synchronous | `RestoreBackup` | fixed here | yes |
| `restore backup`, SSE progress | `RestoreBackup` stream frames | fixed here | yes |
| `restore backup` `overwrite` flag | `RestoreBackupRequest.overwrite` | fixed here | yes |
| `restore database <db> <url>`, synchronous | `RestoreDatabase` | fixed here | yes |
| `restore database`, SSE progress | `RestoreDatabase` stream frames | fixed here | yes |
| `import database <db> <url>`, synchronous | `ImportDatabase` | fixed here | yes |
| `import database`, SSE progress + polled counters | `ImportProgress.parsed/vertices/edges` | fixed here | yes |
| root-user gate on all three | `requireServerAdmin(authenticate(...))` | fixed here | yes |
| leader-only gate (HTTP forwards to leader) | `requireLeader` + leader trailers | fixed here | yes |
| SSRF / local-URL guard (`restore database`, `import database`) | shared `ServerControlPlane` validator | fixed here | yes |
| path traversal via database name | `checkDatabaseNameIsValid` inside the shared implementation, now also on `restore backup`'s target | fixed here | yes |
| `GrpcAuthInterceptor` "all admin RPCs are unary" claim | comment corrected; server-streaming still delivers one request message | fixed here | yes |
| client API | `RemoteGrpcServer.restoreBackup/restoreDatabase/importDatabase` | fixed here | yes |
| OpenAPI spec | **argued** - the HTTP routes and their command grammar are unchanged, so `CoreApiSpec` needs no edit | - | - |
| console `restore database` | **argued** - the console speaks HTTP through `RemoteServer`; it is not a control-plane entry point this issue names, and its behaviour is unchanged | - | - |
| a *client*-streaming import (payload uploaded over the wire) | **not implemented** - the issue raises it only as the case that would break `GrpcAuthInterceptor.onMessage`; both transports still take a URL, so the interceptor's one-request-message assumption continues to hold. Recorded in residual risk | - | - |

### Behaviour changes outside the strict adaptation

1. `restore backup ... as <target>` now validates the **target** database name with
   `ArcadeDBServer.checkDatabaseNameIsValid`, which only `restore database` did before. Without it
   the new `RestoreBackup` RPC would ship a fresh path-traversal sink into
   `SERVER_DATABASE_DIRECTORY + File.separator + target`. Names it rejects (`/`, `\`, `\0`, `..`)
   could never have produced a usable database anyway.
2. The drop that `swapRestoredDatabase` performs before moving the restored directory into place no
   longer increments the `http.drop-database` Micrometer counter. That counter was being charged for
   a restore, and charging it from a gRPC call would be plainly wrong. The `drop database` command
   itself still increments it.

## Residual risk

- The import payload is still fetched by the **server** from a URL on both transports. Uploading an
  archive to the server as a client stream is a different feature and would need the
  `GrpcAuthInterceptor` rework the issue describes; it is not attempted here.
- Cancellation: a gRPC client that cancels a restore stops receiving progress, but the restore
  itself runs to completion server-side, exactly as an HTTP client that drops an SSE connection
  does. Neither transport can interrupt `Restore.restoreDatabase()` mid-flight - the underlying
  integration API has no cancellation hook.

## What changed

| File | Change |
|---|---|
| `server/.../ServerControlPlane.java` | Gains `ProgressListener`, `restoreDatabase`, `restoreBackup`, `importDatabase`, and the private `performRestore` / `swapRestoredDatabase` / `replicateRestoredDatabase` / `runImport` / URL-guard machinery moved out of the HTTP handler. Plus `RestoreImportUrlNotAllowedException`, a `SecurityException` subtype so gRPC can answer `PERMISSION_DENIED` where a failed login answers `UNAUTHENTICATED` |
| `server/.../PostServerCommandHandler.java` | Now parses the command, picks a progress sink and delegates. `SSEProgressSink` starts the stream lazily, on the first event, so a request rejected before the operation starts is still an HTTP status. `isBlockedHost` stays as a delegating static, because a unit test names it |
| `grpc/src/main/proto/arcadedb-server.proto` | Three server-streaming RPCs plus `RestoreBackupRequest`, `RestoreDatabaseRequest`, `ImportDatabaseRequest`, `RestoreProgress`, `ImportProgress` |
| `grpcw/.../GrpcProgressStream.java` | New. The streaming counterpart of `GrpcUnaryCall`: a `ProgressListener` that writes to a `StreamObserver`, terminates the call exactly once, and stops writing to a cancelled call rather than failing the operation |
| `grpcw/.../ArcadeDbGrpcAdminService.java` | The three handlers, each `requireServerAdmin` + `requireLeader` |
| `grpcw/.../GrpcAuthInterceptor.java` | The "all admin RPCs are unary" comment was the issue's own question. Corrected: server-streaming still delivers exactly one request message, so the `onMessage` gate is unaffected; a *client*-streaming admin RPC would still break it, and the comment now says why |
| `grpc-client/.../RemoteGrpcServer.java` | `restoreBackup` / `restoreDatabase` / `importDatabase`, blocking for the length of the operation, with an optional progress callback and no client deadline |
| `grpcw/pom.xml`, `grpc-client/pom.xml` | `arcadedb-integration` in **test** scope: the restore/import path reaches it reflectively, so without it the ITs would only exercise the "libs not found in classpath" arm |

## Tests

| Class | Module | What it drives |
|---|---|---|
| `Issue7308GrpcRestoreImportIT` | grpcw | 9 tests. Real backup -> `RestoreBackup` -> restored row count; overwrite on and off; both traversal guards; `RestoreDatabase` from a `file://` URL with progress before the terminator; `ImportDatabase` with the importer report on the completed message |
| `Issue7308GrpcRestoreImportUrlGuardIT` | grpcw | 5 tests. The SSRF guard on the new transport, with the flag at its default. `PERMISSION_DENIED`, never `UNAUTHENTICATED`; and no database left behind by a refused import |
| `Issue7308GrpcRestoreImportAuthorizationIT` | grpcw | 7 tests. Root-only gate on all three, non-root and bad password, plus the check that a denied caller created nothing |
| `Issue7308GrpcRestoreImportLeaderRoutingIT` | grpcw | 2 tests, `@Tag("slow")`, 3-node Raft. Follower refuses with `FAILED_PRECONDITION` and the leader trailers; the leader gets past the gate and fails on its arguments instead |
| `Issue7308RemoteGrpcServerRestoreImportIT` | grpc-client | 5 tests. Each client method against a live server, the report `importDatabase` returns, and a failed restore throwing rather than returning the progress so far |
| `Issue7308RestoreTargetNameIT` | server | 5 tests. The HTTP side of the target-name guard, the control that a plain name still gets through, and an SSE client still receiving an HTTP status for a request refused before the restore starts |

### Results

```
server      Issue7308RestoreTargetNameIT                     5/5
server      http/handler + backup packages                 561/561   (regression sweep)
grpcw       whole module                                   216/216
grpc-client whole module                                   150/150
```

The guard tests were falsified before being trusted: removing
`server.checkDatabaseNameIsValid(targetDatabase)` turns 2 of the 5 `Issue7308RestoreTargetNameIT`
tests red, and restoring it turns them green again.

One flake was seen and is not this change's: `Issue6875SetServerSettingHttpIT` answered 403 instead
of 400 on one package-wide run and passed on the re-run, alone, and paired with the new class. It is
the shared-JVM authentication-lockout interaction the repo already knows about.

### Behaviour changes beyond the adaptation, in full

1. `restore backup ... as <target>` now validates the **target** name (see above).
2. The pre-restore drop no longer charges `http.drop-database`.
3. The synchronous HTTP `import database` now runs the importer directly rather than through the SQL
   `IMPORT DATABASE` statement, because that is the one implementation both transports share. The two
   things the SQL path contributed are preserved explicitly in `runImport`: the database is unwrapped
   with `getWrappedDatabaseInstance()` so commits still replicate in HA, and the operation is
   registered with `OperationProgressRegistry` so `GET /api/v1/progress/{database}` still reports it.
   The SSE branch had neither before; it does now.
4. `Metrics.counter("http.restore-database")` is incremented before the URL guard rather than after,
   so a refused URL now counts as a received command. Counting the request rather than the requests
   that passed validation is the more useful of the two, and it is the only ordering the shared
   implementation allows.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not been persuaded by the author's reasoning.
**No `Task` tool was available in this session**, so the pass was run by the author against the diff
instead, which is a weaker instrument: it cannot be surprised. Recorded here rather than skipped
silently. Each finding below was verified with a command, and both real ones were filed before the
PR opened.

| Finding | Verdict | Disposition |
|---|---|---|
| A restore is serialised against nothing - two concurrent restores of one target both pass the existence pre-check and both reach `swapRestoredDatabase`, and a restore can drop the database directory underneath a running backup. `BackupCoordinator.begin/end` guards `triggerBackup` and nothing else | **real, out of scope** | filed as **#7384**. Not new - two HTTP callers could always race - but #7308 makes the racers reachable from two transports |
| A running restore publishes no `OperationProgress`, so `GET /api/v1/progress/{database}`, the console and Studio show an idle database while it is being replaced. Import publishes one; restore never did on any path | **real, out of scope** | filed as **#7385** |
| `importDatabase` does not pre-check that the database exists, so a duplicate name would surface as a server fault over gRPC | **not real** | `ArcadeDBServer.createDatabase:1005` throws `IllegalArgumentException("Database '...' already exists")`, which `toStatus` maps to `INVALID_ARGUMENT` |
| A restore blocks a gRPC handler thread for its whole duration and could starve other RPCs | **not real** | `GrpcServerPlugin` never calls `.executor(...)` on the builder, so grpc-java's default cached pool applies and grows on demand. The HTTP path blocks a worker thread for exactly as long |
| A cancelled gRPC client would fail the restore | **not real** | `GrpcProgressStream.send` checks `ServerCallStreamObserver.isCancelled()` and stops writing rather than throwing; the restore runs to its end, matching an HTTP client that drops its SSE connection. Documented in the proto |
| The SSRF guard would be bypassed on the new transport | **not real** | `validateClientRestoreImportUrl` is inside the shared implementation, not in either handler, and `Issue7308GrpcRestoreImportUrlGuardIT` drives it through the gRPC entry point with the flag at its default |
