# #7309 - gRPC control plane: groups, API tokens and user update

Follow-up to #7304, which put `ListUsers`/`CreateUser`/`DeleteUser` on `ArcadeDbAdminService` and
left the remaining rows of the `/server/*` security table HTTP-only.

## Finding ledger

Read from the issue body; one row per HTTP route it names.

- [x] 1. `PUT /server/users` (`PutUserHandler`) - update password and/or per-database groups
- [x] 2. `GET /server/groups` (`GetGroupsHandler`) - read the group/permission document
- [x] 3. `POST /server/groups` (`PostGroupHandler`) - create/replace a group
- [x] 4. `DELETE /server/groups` (`DeleteGroupHandler`) - drop a group
- [x] 5. `GET /server/api-tokens` (`GetApiTokensHandler`) - list issued tokens
- [x] 6. `POST /server/api-tokens` (`PostApiTokenHandler`) - mint a token, **returns secret material**
- [x] 7. `DELETE /server/api-tokens` (`DeleteApiTokenHandler`) - revoke a token

The issue attaches two security questions to row 6, tracked as sub-items:

- [x] 6a. Nothing refuses to *receive* a minted token over a plaintext channel to a non-loopback
  host, though `RemoteGrpcServer` refuses to *send* credentials over one.
- [x] 6b. `GrpcLoggingInterceptor` and `GrpcMetricsInterceptor` see every message; a token-bearing
  response must be proved not to reach a log sink.

## Completeness

### 1. The invariant

Two, because rows 1-5 and 7 are adapters and row 6 is not:

> **Parity.** Every `checkRootUser`-gated route under `/server/*` is reachable as a
> `requireServerAdmin`-gated RPC on `ArcadeDbAdminService`, and both transports run one
> implementation of the operation rather than two that can drift.

> **Secrecy.** Plaintext API-token material minted by the server leaves the process only towards a
> peer whose transport protects it, and reaches no log sink or metric tag on the way out.

### 2. Every way to violate it - found by command, not by memory

**The `/server/*` security surface is exactly nine routes** - so the issue's seven-row table plus
#7304's three is the whole of it, with `GET /server/users` counted in #7304:

```
$ grep -rnE "UserHandler|GroupHandler|GroupsHandler|ApiToken" \
    server/src/main/java/com/arcadedb/server/http/HttpServer.java
249:        .get("/server/api-tokens", new GetApiTokensHandler(this))
250:        .post("/server/api-tokens", new PostApiTokenHandler(this))
251:        .delete("/server/api-tokens", new DeleteApiTokenHandler(this))
253:        .post("/server/users", new PostUserHandler(this))
254:        .put("/server/users", new PutUserHandler(this))
255:        .delete("/server/users", new DeleteUserHandler(this))
256:        .get("/server/groups", new GetGroupsHandler(this))
257:        .post("/server/groups", new PostGroupHandler(this))
258:        .delete("/server/groups", new DeleteGroupHandler(this))
```

(`.get("/server/users", new GetUsersHandler(this))` is line 252, covered by #7304.)

**Writers and readers of the group document - the HTTP handlers are the only non-test callers**, so
moving the body into `ServerControlPlane` cannot orphan a second caller:

```
$ grep -rnE "\.saveGroup\(|\.deleteGroup\(|\.groupsToJSON\(" --include="*.java" . \
    | grep -v /target/ | grep -v /src/test/
server/.../handler/DeleteGroupHandler.java:55:    final boolean deleted = security.deleteGroup(database, name);
server/.../handler/GetGroupsHandler.java:40:    final JSONObject groups = ...getSecurity().groupsToJSON();
server/.../handler/PostGroupHandler.java:62:    security.saveGroup(database, name, groupConfig);
```

**Writers and readers of the token store - same, three HTTP handlers and the store itself:**

```
$ grep -rnE "createToken\(|deleteToken\(|listTokens\(|getApiTokenConfiguration\(" --include="*.java" . \
    | grep -v /target/ | grep -v /src/test/
server/.../security/ServerSecurity.java:1112          (the accessor)
server/.../security/ApiTokenConfiguration.java:132,162,188   (the store)
server/.../handler/PostApiTokenHandler.java:57,60
server/.../handler/GetApiTokensHandler.java:41,42
server/.../handler/DeleteApiTokenHandler.java:47,48
```

**Writer of a user update - one HTTP handler:**

```
$ grep -rnE "updateUserClusterWide\(" --include="*.java" . | grep -v /target/ | grep -v /src/test/
server/.../security/ServerSecurity.java:431   (the definition)
server/.../security/ServerSecurity.java:568   (internal re-entry)
server/.../handler/PutUserHandler.java:72
```

**Sibling of the same shape - what else returns secret material in a response body?** Only the
token mint:

```
$ grep -rn "put(\"token\"" --include="*.java" server/src/main/java | grep -v /target/
server/.../security/ApiTokenConfiguration.java:  response.put("token", tokenValue);
```

**Is the group/token state replicated on HA, as user state is?** No - and this is a
*pre-existing* asymmetry, not one this change introduces:

```
$ grep -rnE "saveGroup|deleteGroup|groupRepository|ApiToken" --include="*.java" \
    server/src/main/java/com/arcadedb/server/ha/
(no matches)
$ grep -rnE "replicateSecurityUsers" --include="*.java" server/src/main/java/ | head -4
HAServerPlugin.java:262:  default void replicateSecurityUsers(...)
ServerSecurity.java:422,448,475     (createUserClusterWide / updateUserClusterWide / dropUserClusterWide)
```

`ServerSecurity.saveGroup`/`deleteGroup` write through `groupRepository` to a local file; nothing
submits a Raft entry. Same for `ApiTokenConfiguration`, which owns `server-api-tokens.json`.

**Do the interceptors touch message content?** Read both:

- `GrpcLoggingInterceptor` logs `methodName`, elapsed millis, the two compression header values and,
  on failure, `status`. It never holds a reference to a request or response message - it overrides
  `sendHeaders` and `close`, not `sendMessage`.
- `GrpcMetricsInterceptor` overrides `sendMessage`, but only to test
  `message instanceof ExecuteCommandResponse response && !response.getSuccess()`. It never
  stringifies the message; the meters carry `method` and `status` tags only.

So 6b holds on today's tree. It is an invariant nothing enforces, which is what the regression test
added here is for.

**Does the client already refuse a plaintext non-loopback channel?** Yes, for every admin RPC,
because the admin stub attaches call credentials:

```
RemoteGrpcServer.adminServiceBlockingV2Stub()
  -> createCallCredentials(userName, userPassword)
     -> ensureCredentialsAllowedOverChannel()   // throws SecurityException
```

That covers 6a on ArcadeDB's own Java client, but only there: `allowInsecureCredentials=true` opts
out of it, and a grpcurl or Python caller never runs it at all. The gate that actually holds the
invariant has to be server-side, at the point the secret leaves the process.

### 3. Coverage table

| # | Entry point | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | `UpdateUser` RPC -> `ServerControlPlane.updateUser` | yes | yes |
| 1b | `PUT /server/users` -> same `ServerControlPlane.updateUser` | yes (refactored onto it) | yes |
| 2 | `ListGroups` RPC -> `ServerControlPlane.listGroups` | yes | yes |
| 2b | `GET /server/groups` -> same | yes (refactored) | yes |
| 3 | `SaveGroup` RPC -> `ServerControlPlane.saveGroup` | yes | yes |
| 3b | `POST /server/groups` -> same | yes (refactored) | yes |
| 4 | `DeleteGroup` RPC -> `ServerControlPlane.deleteGroup` | yes | yes |
| 4b | `DELETE /server/groups` -> same | yes (refactored) | yes |
| 5 | `ListApiTokens` RPC -> `ServerControlPlane.listApiTokens` | yes | yes |
| 5b | `GET /server/api-tokens` -> same | yes (refactored) | yes |
| 6 | `CreateApiToken` RPC -> `ServerControlPlane.createApiToken` | yes | yes |
| 6b | `POST /server/api-tokens` -> same | yes (refactored) | yes |
| 6a | secret material over an unprotected transport | yes - server-side gate | yes |
| 6b' | secret material into a log sink / metric tag | yes - regression test pins it | yes |
| 7 | `DeleteApiToken` RPC -> `ServerControlPlane.deleteApiToken` | yes | yes |
| 7b | `DELETE /server/api-tokens` -> same | yes (refactored) | yes |
| C1 | `RemoteGrpcServer` client methods for rows 1-7 | yes | yes |
| C2 | `RemoteServer` (HTTP Java client) methods for rows 1-7 | **filed as #7372** | no |
| C3 | groups + API tokens are node-local on an HA cluster | **filed as #7373** | no |

Rows C2 and C3 are the two the sweep found that the issue does not name. Neither is a regression
introduced here; both are recorded as follow-up issues before this PR opens.

### 5. Reachability

- `ArcadeDbGrpcAdminService` is constructed by `GrpcServerPlugin.configureServer`, which both
  `startStandardServer` and `startXdsServer` call - so the new RPCs are live on either builder.
- The new `GrpcTransportSecurityInterceptor` is registered unconditionally in the same method,
  beside the logging and metrics interceptors. `CreateApiToken` **fails closed** when its context
  key is absent, so a future refactor that drops the registration turns the mint off rather than
  turning the gate off silently.
- The refactored HTTP handlers are the ones `HttpServer` registers at lines 249-258, unchanged.

### 7. Residual risk

- **A token minted over gRPC still lands in a node-local file** (row C3). On an HA cluster the token
  authenticates only against the node that minted it. That was already true over HTTP; this change
  makes it reachable from one more transport, which is why it is filed rather than left implicit.
- **The secrecy gate reads the transport, not the operator's intent.** A TLS-terminating proxy in
  front of a plaintext gRPC port on a loopback interface is indistinguishable, from inside the
  process, from a local client - and is allowed. That is the same assumption
  `RemoteGrpcServer.isLoopbackHost` already makes on the client side.
- **`permissions` travels as a JSON string** in `CreateApiTokenRequest`/`ApiTokenInfo`, not as proto
  fields, for the reason `GetBackupConfigResponse.config_json` gives: it is the same free-form
  document the HTTP body carries, and a second declaration of its shape here would be one more thing
  to keep in step with it.
- Nothing else. The coverage table above is the evidence: every row is fixed here or filed.

## What was built

**One implementation per operation, two transports.** Seven methods moved into
`ServerControlPlane` - `updateUser`, `listGroups`, `saveGroup`, `deleteGroup`, `listApiTokens`,
`createApiToken`, `deleteApiToken` - and the six HTTP handlers were rewritten onto them. What moved
is not only the happy path: the group-permission cache refresh, the admin-group refusal, the
permission-document validation and the plaintext-token refusal all moved too, because each is a rule
a second transport would otherwise have silently lacked.

Two exception types carry the outcomes both transports need to tell apart:
`ServerControlPlane.NotFoundException` (HTTP 404 / gRPC `NOT_FOUND`) and `AlreadyExistsException`
(HTTP 409 / gRPC `ALREADY_EXISTS`).

**The mint's transport gate.** `GrpcTransportSecurityInterceptor` publishes, per call, whether the
transport protects the response - TLS session present, or loopback peer - and `CreateApiToken`
refuses with `FAILED_PRECONDITION` unless it does. It fails closed on an absent key, so removing the
interceptor stops tokens being minted rather than stops them being protected.

## Test results

| Suite | Result |
|---|---|
| `Issue7309GrpcSecurityControlPlaneIT` (new, 17 tests) | 17/17 |
| `GrpcTransportSecurityInterceptorTest` (new, 7 tests) | 7/7 |
| `grpcw` full surefire suite | 223/223 |
| `GroupManagementIT` + `ApiTokenAuthenticationIT` + `UserManagementIT` (HTTP regression) | 27/27 |
| adjacent security ITs (`Issue6806`, `SchemaMutationAuthorization`, `CrossDatabaseAccess`, `Issue6808`, `Issue5269`, `OpenApiSpecGeneration`) | 35/35 |

### The secrecy test was wrong first, and that is worth recording

`aMintedTokenReachesNoLogSinkAndNoMetricTag` was written first against a `java.util.logging` handler
attached to the root logger. A deliberate leak injected into `GrpcLoggingInterceptor.sendMessage` -
logging the whole response message at INFO - **did not fail it**. The engine logs through its own
pluggable `com.arcadedb.log.Logger`, so a JUL handler sees only what that implementation forwards, at
whatever level it is configured for.

Rewritten to capture at `LogManager.setLogger`, the engine's own seam, the same injected leak fails
the test. Only then was the mutation reverted and the test confirmed green. The captured text on a
clean run is the evidence for 6b:

```
gRPC call started: %s (request compression: %s, client accepts: %s) null
  com.arcadedb.grpc.ArcadeDbAdminService/CreateApiToken none gzip null null ...
gRPC call completed: %s (%sms, req-compression: %s, resp-compression: %s) null
  com.arcadedb.grpc.ArcadeDbAdminService/CreateApiToken 1 none none null ...
```

Method name, compression, elapsed millis. No payload.

### Unrelated red seen while testing

The `server` module's full surefire run showed failures in `AutoCommitParameterTest`,
`Issue6220TruncateHttpDefaultTest`, `QueryEndpointReadOnlyTest` and `ClusterInternalAuthTest`. None
of them touch anything this branch changes, and the cause is the one `CLAUDE.md` documents:

- `ClusterInternalAuthTest` targets `http://localhost:2480`. On this machine a Homebrew ArcadeDB has
  held `*:2480` for six days (`lsof -nP -iTCP:2480 -sTCP:LISTEN` -> pid 93797, `ELAPSED 06-04:13`),
  `localhost` resolves to `::1`, and that stranger answers the cluster token with 401. The assertion
  reads `Expecting actual: 401 not to be equal to: 401`, which looks like an auth bug and is a port
  conflict.
- The other three target `127.0.0.1:2480`, reach the test's own server, and **passed when re-run in
  isolation** - they fail only in a full run sharing the machine with another agent's `server` build.

Neither is this branch's, and neither is filed here: the first is a local environment condition, the
second is the known cost of concurrent builds.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen the author's reasoning and asks it
to write the follow-up issue it would file against the patch. **No `Task` tool was available in this
session**, so no such subagent could be spawned. Per the skill's error table that is not a hard gate;
it is recorded here rather than passed off as done, and the pass was run by re-reading the diff
against the issue with the same three questions. Findings:

| Finding | Disposition |
|---|---|
| The three REST `/server/users` routes reach `*ClusterWide` on a follower with no leader gate, while the `POST /server` command path forwards to the leader - so gRPC's `requireLeader` on `UpdateUser` is stricter than the HTTP route it mirrors | Real, out of scope - **filed as #7380** |
| `POST /server/api-tokens` with an explicitly empty `"database"` used to store a token scoped to the database named `""`; it now normalizes to `"*"` | Real, in scope - behaviour change, deliberate, documented on `ServerControlPlane.createApiToken`. A token scoped to `""` matched no database and could never be used |
| `GrpcTransportSecurityInterceptor` might not run before the handler, leaving the gate to fail closed on every call and disabling the mint outright | **Not real.** `createApiTokenReturnsTheMaterialOnceAndListsTheTokenWithout` mints successfully over a real channel. Since the gate refuses when the key is absent, a successful mint is proof the interceptor ran and published a verdict - the positive control doubles as the registration and ordering test |
| The token listing might carry the plaintext by copying the stored document | **Not real**, and now prevented by construction: `listApiTokens` names each field it copies rather than removing one from a copy. `createApiTokenReturnsTheMaterialOnceAndListsTheTokenWithout` asserts `etl.toString()` does not contain the minted token |
| `PutUserHandler` could have changed which of "user not found" and "password too short" wins | **Not real.** Both old handler and `ServerControlPlane.updateUser` look the user up first and validate the password second; `updateUserAnswersNotFoundForAnAbsentUser` pins the order from the gRPC side and `UserManagementIT` from the HTTP side |
