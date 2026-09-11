# #7304 - gRPC control plane: database lifecycle, backup, security and profiler RPCs

Issue: https://github.com/ArcadeData/arcadedb/issues/7304
PR: https://github.com/ArcadeData/arcadedb/pull/7324
Branch: `feat/7304-grpc-control-plane-admin-service`
Final state: clean approval on cycle 2. The merge is the developer's.

## Correction to the issue's premise

The issue says `ArcadeDbService` "has no control-plane operation of any kind". That is true of
`ArcadeDbService`, but the proto already carries a second service:

```
$ sed -n '/^service ArcadeDbAdminService/,/^}/p' grpc/src/main/proto/arcadedb-server.proto | grep -o 'rpc [A-Za-z]*'
rpc Ping
rpc GetServerInfo
rpc ListDatabases
rpc ExistsDatabase
rpc CreateDatabase
rpc DropDatabase
rpc GetDatabaseInfo
rpc CreateUser
rpc DeleteUser
```

So `ArcadeDbAdminService` exists with 9 RPCs, of which `CreateUser` and `DeleteUser` are declared
but answer `UNIMPLEMENTED` at runtime
(`ArcadeDbGrpcAdminService.createUser`/`deleteUser`, before this change). The issue's keyword
counts are accurate and unchanged by that:

```
$ for k in backup health cluster profil; do printf "%-8s %s\n" "$k" "$(grep -ic "$k" grpc/src/main/proto/arcadedb-server.proto)"; done
backup   0
health   0
cluster  0
profil   0
```

The real gap is therefore narrower than "no control plane" and wider than "a few RPCs": database
lifecycle is half-covered, and backup / security / profiler / probes / settings / server events
are absent entirely.

## Invariant

**Every exchange-free control-plane operation the HTTP layer exposes is reachable over gRPC, and
both protocols reach it through one implementation, gated by the same root-user check.**

"Exchange-free" is the boundary of this PR: the three HTTP operations that stream progress over
the `HttpServerExchange` (restore backup, restore database, import database) need a
server-streaming RPC design of their own and are filed as follow-ups rather than half-adapted.

## Completeness

### Enumeration (commands run in this session)

```
$ grep -n 'private static final String [A-Z_]* *=' server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
LIST_DATABASES SHUTDOWN CREATE_DATABASE DROP_DATABASE CLOSE_DATABASE OPEN_DATABASE
CREATE_USER DROP_USER CONNECT_CLUSTER DISCONNECT_CLUSTER SET_DATABASE_SETTING
SET_SERVER_SETTING GET_SERVER_EVENTS ALIGN_DATABASE GET_BACKUP_CONFIG SET_BACKUP_CONFIG
LIST_BACKUPS TRIGGER_BACKUP RESTORE_BACKUP DELETE_BACKUP RESTORE_DATABASE IMPORT_DATABASE
PROFILER                                                              (23 commands)

$ grep -n '"/server/\|"/health"\|"/ready"\|"/databases"\|"/exists/\|"/progress/\|"/login"\|"/logout"\|"/sessions"' server/src/main/java/com/arcadedb/server/http/HttpServer.java
/databases /exists/{db} /progress/{db} /login /logout /sessions /ready /health
/server/api-tokens {GET,POST,DELETE}  /server/users {GET,POST,PUT,DELETE}
/server/groups {GET,POST,DELETE}                                      (18 routes)

$ grep -rn "checkRootUser" server/src/main/java/com/arcadedb/server/http/handler/ | wc -l
14      # one definition in AbstractServerHttpHandler + 13 call sites

$ grep -rn 'requireServerAdmin\|"root"' grpcw/src/main/java/
ArcadeDbGrpcAdminService.java:122,152  (createDatabase, dropDatabase)
ArcadeDbGrpcAdminService.java:297-298  (definition, mirrors checkRootUser)
```

### Entry-point coverage table

| HTTP control-plane entry point | gRPC RPC | Disposition | Test |
|---|---|---|---|
| `list databases`, `GET /databases` | `ListDatabases` | pre-existing RPC, **narrowed here** to the caller's authorized databases | yes |
| `GET /exists/{db}` | `ExistsDatabase` | pre-existing RPC, **gated here** on the caller's grant | yes |
| `create database` | `CreateDatabase` | pre-existing | pre-existing |
| `drop database` | `DropDatabase` | pre-existing | pre-existing |
| `GET /server` | `GetServerInfo` | pre-existing RPC, `databases_count` **narrowed here** | covered by the `ListDatabases` filter test |
| `GET /server` (per-database view) | `GetDatabaseInfo` | pre-existing RPC, **gated here** on the caller's grant | yes |
| `open database` | `OpenDatabase` | fixed here | yes |
| `close database` | `CloseDatabase` | fixed here | yes |
| `align database` | `AlignDatabase` | fixed here | yes |
| `set server setting` | `SetServerSetting` | fixed here | yes |
| `set database setting` | `SetDatabaseSetting` | fixed here | yes |
| `get backup config` | `GetBackupConfig` | fixed here | yes |
| `set backup config` | `SetBackupConfig` | fixed here | yes |
| `list backups` | `ListBackups` | fixed here | yes |
| `trigger backup` | `TriggerBackup` | fixed here | yes |
| `delete backup` | `DeleteBackup` | fixed here | yes |
| `profiler start/stop/reset/results/list/load` | `ProfilerStart/Stop/Reset/Results/List/Load` | fixed here | yes |
| `create user`, `POST /server/users` | `CreateUser` (was `UNIMPLEMENTED`) | fixed here | yes |
| leader forwarding of create/drop database and user | leader gate on the four RPCs | fixed here | yes (HA, 3 nodes) |
| `drop user`, `DELETE /server/users` | `DeleteUser` (was `UNIMPLEMENTED`) | fixed here | yes |
| `GET /server/users` | `ListUsers` | fixed here | yes |
| `get server events` | `GetServerEvents` | fixed here | yes |
| `shutdown` | `Shutdown` | fixed here | yes (authz only; a test may not stop the JVM) |
| `disconnect cluster` | `DisconnectCluster` | fixed here | yes (authz + non-HA rejection) |
| `GET /health` | `Health` | fixed here | yes |
| `GET /ready` | `Ready` | fixed here | yes |
| `connect cluster` | `ConnectCluster` | **argued here, reversed by #7400** | yes, in #7400 (authz + non-HA rejection) |
| `restore backup` | none | **filed** #7308 | - |
| `restore database` | none | **filed** #7308 | - |
| `import database` | none | **filed** #7308 | - |
| `PUT /server/users` (update) | none | **filed** #7309 | - |
| `GET/POST/DELETE /server/groups` | none | **filed** #7309 | - |
| `GET/POST/DELETE /server/api-tokens` | none | **filed** #7309 | - |
| `GET /progress/{db}` | none | **filed** #7310 | - |
| `POST /login`, `POST /logout`, `GET /sessions` | none | **argued** | - |

### Arguments

- **`connect cluster`** - *this argument was reversed by #7400; the RPC exists as of that issue and
  the row above is updated. Kept here because the reasoning is the record of what was decided, and
  what was wrong with it.* The argument was: the HTTP implementation has no behaviour to adapt - it
  is a single unconditional throw - so a gRPC RPC would reproduce an error, not an operation. What
  it missed is that the *contract* is the thing the two transports must agree on, not just the
  behaviour: a verb HTTP accepts and answers with a reasoned refusal, and gRPC answers
  `UNIMPLEMENTED`, is a difference a client can see and has to code around. #7400 added the RPC as
  the same thin adapter every other row uses, so the refusal now arrives as `FAILED_PRECONDITION`
  carrying the shared implementation's own message, and a later real join lands in one place for
  both transports. The shared method has since moved to `ServerControlPlane.connectCluster` and
  raises `OperationNotAvailableException` (a `CommandExecutionException` subtype), which is the arm
  that maps to `FAILED_PRECONDITION`. Whether to implement the join at all is #7401.
- **`POST /login` / `POST /logout` / `GET /sessions`** - HTTP sessions exist because HTTP is
  stateless per request and the browser needs a bearer token. gRPC authenticates every admin RPC
  from the `DatabaseCredentials` on the request body, enforced centrally in
  `GrpcAuthInterceptor.authenticateAdminRequest`. There is no gRPC session to create, destroy, or
  list. `GET /sessions` as a *read-only administrative view of HTTP sessions* is a real gap and is
  filed with the discovery group (#7310).

## Design

`PostServerCommandHandler` held the control-plane semantics as ~450 lines of private methods
returning `ExecutionResponse`. Reimplementing them in the gRPC service is exactly the drift the
issue warns against, so they move to a new shared class instead:

`server/src/main/java/com/arcadedb/server/ServerControlPlane.java`

- constructed from an `ArcadeDBServer`, with no HTTP types on its surface;
- returns `JSONObject`/`JSONArray`/void and throws, so each protocol maps errors its own way;
- `PostServerCommandHandler` keeps command-string parsing, leader forwarding, the
  `ExecutionResponse` status mapping, and the three exchange-bound operations, and delegates the
  semantics.

`ArcadeDbGrpcAdminService` gains the RPCs, each a thin adapter over the same object, gated by the
existing `requireServerAdmin` (the mirror of HTTP's `checkRootUser`) and, for the four operations
HTTP forwards to the leader, by `requireLeader` (see the adversarial pass below).

Two things deliberately did NOT move into the shared class, because they are properties of the
transport rather than of the operation: the `http.*` metric counters, and leader routing.

`Health` and `Ready` are unauthenticated on HTTP (`isRequireAuthentication()` returns `false` on
both handlers), so `GrpcAuthInterceptor` exempts those two method names from the admin
authentication choke point to keep the two protocols' probe semantics equal - a probe that needs
credentials is not usable by a Kubernetes probe.

## What the tests caught

`ProfilerList` first modelled a saved run as a bare file name (`repeated string file_names`). The
gRPC test passed while the profiler directory was empty and failed the moment a run existed:
`ServerQueryProfiler.listSavedRuns` returns `{fileName, size, lastModified}` objects, not strings,
so the adapter raised `JSONArray[0] is not a string`. The proto now carries `ProfilerRunInfo` and
the test stops a recording first so the listing is non-empty when its shape is asserted - an
empty-list assertion could not have failed.

## Adversarial pass

The orchestrator's Phase 1.5 spawns an uninvolved subagent to write the follow-up issue it would
file against this patch. The `Task` tool is disabled in this session, so that pass was run by the
author instead - weaker, and recorded as such. Four findings, all fixed here:

1. **The leader gate was not reproduced.** The HTTP handler forwards create/drop database and
   create/drop user to the leader (`PostServerCommandHandler.forwardToLeaderIfReplica`), so those
   commands never run on a follower. The gRPC admin service ran them wherever the call landed,
   which would have taken `createUserClusterWide` into `HAServerPlugin.replicateSecurityUsers` on a
   follower - a state the HTTP path cannot reach. The issue named this explicitly ("leader proxying
   via `LeaderProxy` ... must be reproduced rather than assumed"). gRPC has no request proxy, so the
   gate is the refusal this transport already uses for `graphBatchLoad` (#6091/#6183):
   `ServerIsNotTheLeaderException` routed through `GrpcErrorMapper`, so the answer is
   `FAILED_PRECONDITION` carrying the leader address on the `LeaderRedirectProtocol` trailers.
   Driven against a real 3-node cluster by `Issue7304GrpcAdminLeaderRoutingIT`, with a
   leader-succeeds positive control and a read-only RPC that must still answer on a follower - a
   gate that refused everywhere would otherwise pass.
2. **gRPC admin calls were inflating the HTTP metrics.** Moving the operations into
   `ServerControlPlane` took the `Metrics.counter("http.*")` increments with them, so a gRPC
   `TriggerBackup` would have incremented `http.trigger-backup`. The counters are back on the HTTP
   side of the split (`PostServerCommandHandler.count`); gRPC admin calls are counted per method by
   `GrpcMetricsInterceptor`, as they already were.
3. **`ListDatabases` and `GetDatabaseInfo` disclosed more over gRPC than over HTTP.** Listing is
   the one control-plane read that is not root-only, so on HTTP the gate is the answer's contents:
   `list databases` and `GET /api/v1/databases` both narrow it through
   `filterAuthorizedDatabases`. The gRPC `ListDatabases` handed every authenticated caller every
   database name on the server, `GetDatabaseInfo` reported the schema shape and record counts of
   any database to any account, and `GetServerInfo.databases_count` counted them all. All three now
   narrow to the caller through the same filter, which moved to `ServerControlPlane` with
   `AbstractServerHttpHandler.filterAuthorizedDatabases` delegating to it. `ExistsDatabase` was left
   unfiltered at first, on a misreading of `GetExistsDatabaseHandler` that review cycle 1 below
   corrected. Pre-existing RPCs, but this is the parity the issue asks for.
4. **`CreateUser` could only make an ungranted account.** `CreateUserRequest` carried `user`,
   `password` and a `role` the security model has no concept of, while the HTTP `create user`
   document carries a `databases` map of per-database groups - which is where a user's authority
   actually comes from. The request now carries that map, `role` is marked deprecated rather than
   silently ignored, and `RemoteGrpcServer.createUser` has an overload for it.

## Contract surfaces

1. **Proto** - `grpc/src/main/proto/arcadedb-server.proto`
2. **OpenAPI** - no new HTTP route is introduced, so no spec contributor changes; the existing
   routes these RPCs mirror are already described. `OpenApiSpecGenerationIT` is run to prove it.
3. **Client** - `grpc-client/.../RemoteGrpcServer.java` gains a method per new RPC.

## Verification

Every suite below was run in this session, on a machine where an unrelated ArcadeDB server has been
listening on `*:2480` for days. That matters, because the `server`-module HTTP tests bind port 2480
too and some of them build their URLs as `localhost:2480`:

```
$ lsof -nP -iTCP:2480 -sTCP:LISTEN     # with no test running
java 93797 *:2480

$ curl -s -o /dev/null -w '%{http_code} (remote %{remote_ip})\n' http://localhost:2480/api/v1/server
401 (remote ::1)
$ curl -s -o /dev/null -w '%{http_code} (remote %{remote_ip})\n' http://127.0.0.1:2480/api/v1/server
401 (remote 127.0.0.1)
```

`localhost` resolves to `::1` and reaches that server; the test server binds `127.0.0.1:2480`
alongside it. So a test that posts to `localhost:2480` is answered by a server that has none of the
fixture's state, which reads as an assertion about a value that was never stored - or, in
`Issue6753ConcurrentBackupIT`, as `expected 409 but was 200`, a coordinator with no backup in
progress because it is a different process's coordinator.

The same suites were therefore run against pristine `main` to establish that an intercepted run
fails there identically, and each suite was retried until it ran in a window where it was not
intercepted. Counts are from those runs.

| Suite | Result |
|---|---|
| `grpcw` unit tests | green (211) |
| `server` unit tests | green (954) |
| `server` control-plane ITs (`PostServerCommandHandlerIT`, `Issue6875SetServerSettingHttpIT`, `Issue7124BooleanSettingHttpIT`, `BackupApiCommandsIT`, `BackupRestoreDeleteApiIT`, `Issue6753ConcurrentBackupIT`, `RestoreImportSecurityDurabilityIT`, `HealthProbesIT`, `UngatedHandlerCrossDatabaseIT`, `HTTPGraphIT`, `OpenApiSpecGenerationIT`) | green (85) |
| `grpcw` admin ITs, including the new `Issue7304GrpcControlPlaneIT`, `Issue7304GrpcControlPlaneAuthorizationIT` and the 3-node `Issue7304GrpcAdminLeaderRoutingIT` | green (136) |
| `grpc-client` ITs, including the new `Issue7304RemoteGrpcServerControlPlaneIT` | green (25) |

`Issue7304GrpcAdminLeaderRoutingIT` inherits `BaseRaftHATest`'s teardown, which compares every
node's copy of the database after each test method. That comparison was seen to fail once, as
`Types: DB1 7 <> DB2 8`, on a machine running several builds at once - a follower one schema entry
behind at the moment `checkDatabasesAreIdentical` looked, after
`waitForReplicationIsCompleted` had given up its 30 s budget. It is not an assertion this test
makes, and no test method here writes a type. Measured afterwards: the class passed 3 of 3 runs in
isolation, and the pre-existing `Issue6183FollowerCommandRoutingIT` on the same fixture passed 3 of
3 alongside it. The class was then reduced from four test methods to two, halving the number of
three-node cluster restarts (51 s to 28 s) without dropping an assertion, which halves the exposure
to it.

## Review cycles

### Cycle 1 - `f65ff26`

The scheduled `claude-review` run for this commit completed successfully (18 turns, 9 permission
denials) and posted nothing, so a review was requested explicitly with an `@claude` comment. Two
findings, both correct, both fixed:

1. **The `http.*` counters moved from "after this command's validation" to "before it".** The first
   attempt wrote `count("http.create-user").createUser(...)`, and Java evaluates the receiver before
   the argument - so the counter fired before the JSON was parsed, let alone before the shared
   password policy ran. Every one of these counters used to sit inside the moved method, past that
   method's own validation, so a command rejected for an empty database name or a password the
   policy refuses was never counted. The chained form silently turned nine of them from successes
   into attempts, in a PR that claimed the split changed no behaviour. Each command now has a small
   wrapper in the handler that runs the shared implementation and increments afterwards.
   `connect cluster` is the one exception and says so: it always throws, so there is no success to
   count after, which is what the moved implementation did too.
2. **`ExistsDatabase` disclosed the existence of databases the caller has no grant on.** The first
   attempt left it unfiltered and this document claimed that as parity with
   `GetExistsDatabaseHandler`. That was a misreading of the handler's comment, which explains why it
   does not build the whole authorized set to answer a one-name question - not that it skips
   authorization:

   ```java
   final boolean existsDatabase = server.getDatabaseNames().contains(requested)
       && (user == null || user.canAccessToDatabase(requested));
   ```

   The gRPC RPC carried only the first conjunct, so any account could enumerate over gRPC the
   database names HTTP hides from it - the same disclosure this PR narrows for `ListDatabases`,
   `GetDatabaseInfo` and `GetServerInfo`, missed on the fourth RPC. It now carries both, with
   `existsDatabaseAnswersFalseForADatabaseTheCallerMayNotAccess` covering it: the granted database
   still reads `true`, the ungranted one reads `false`, and it cannot be told apart from a name that
   does not exist at all.

Two more changes went into the same commit that the review did not raise, one from the Codacy report
and one from reading it:

3. `RemoteGrpcServer` wrapped every admin failure in a bare `RuntimeException` carrying a rendered
   message. It now goes through `GrpcClientErrorMapper`, the mapper the data plane already uses, so a
   follower's leader refusal arrives as a `ServerIsNotTheLeaderException` holding the leader's
   address from the trailers rather than a string with that address thrown away - which was the whole
   point of adding those trailers.
4. The gRPC service mapped every `CommandExecutionException` to `FAILED_PRECONDITION`. That is right
   for "HA is not enabled" and "connect cluster is unsupported", and wrong for a backup archive that
   could not be deleted, which is a server-side fault. A new
   `ServerControlPlane.OperationNotAvailableException` (a `CommandExecutionException` subtype, so the
   HTTP status stays 500) now carries the first meaning, and everything else stays `INTERNAL`.

Codacy reported 5 new "avoid throwing raw exception types" against 5 solved. Four are the
`RuntimeException` throws inside `executeImmediateBackup` and `listBackups`, moved verbatim out of
`PostServerCommandHandler` - the same ones Codacy counts as solved there. They are left alone on
purpose: converting them to `CommandExecutionException` would change the HTTP error body's
`exception` field and, through the mapping above, would have made a failed backup report as a
precondition failure over gRPC. The fifth was `RemoteGrpcServer`, fixed by (3).

### Cycle 2 - `5753212`

Both cycle-1 findings confirmed fixed, and the two changes the review had not raised confirmed as
well: "No new issues found in this incremental diff." Two observations, neither actionable:

- the reviewer noted the `mapped.getMessage() == null || isBlank()` fallback to `RemoteException` in
  `RemoteGrpcServer.call` is unreachable in practice, because `GrpcClientErrorMapper.toException`
  always derives a message from the status description or the status code name. Correct, and it is
  kept as a defensive guard: it costs one branch and it is what stops a future mapper change from
  handing a caller an exception with no message at all.
- it could not run Maven in that session, so cycle 2 is a diff-level read rather than a compiled
  one. The suites listed above were run locally against exactly `5753212`.

Codacy went from 5 new "avoid throwing raw exception types" to 4, the remaining ones being the
verbatim-moved backup throws argued about in cycle 1.

No deferred items: nothing in either review was left unaddressed or unanswered.

## Residual risk

Listed in the table above as **filed**: restore/import (#7308), user update + groups + API
tokens (#7309), and progress/sessions discovery (#7310). Nothing else in the HTTP control plane
is uncovered.
