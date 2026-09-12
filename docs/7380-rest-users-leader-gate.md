# #7380 - the REST `/server/users` routes reach `*ClusterWide` on a follower with no leader gate

## Problem

`POST`, `PUT` and `DELETE /api/v1/server/users` call `ServerSecurity.createUserClusterWide` /
`updateUserClusterWide` / `dropUserClusterWide` on whichever node served the request. On an HA
cluster that reaches `HAServerPlugin.replicateSecurityUsers` -> `RaftTransactionBroker` ->
`RaftGroupCommitter.submitAndWait`, i.e. a Raft entry submitted from a follower.

The same three operations reached over the other two transports are gated:

- `POST /api/v1/server` with `create user` / `drop user` -> `PostServerCommandHandler.execute`
  hands the command to `forwardToLeaderIfReplica` before executing it (line 124).
- gRPC `CreateUser` / `UpdateUser` / `DeleteUser` -> `ArcadeDbGrpcAdminService.requireLeader`
  (issues #7304 and #7309).

So the REST routes were the only ungated way in.

## Decision: forward, do not refuse

The two candidate behaviours were "forward to the leader, as `POST /api/v1/server` does" and
"refuse and name the leader, as gRPC does".

Forwarding wins because the question is about *one* transport being self-consistent. An HTTP
client that asks this server to create a user already gets the request forwarded when it phrases
it as `POST /api/v1/server {"command":"create user ..."}`; getting a refusal for the same
operation phrased as `POST /api/v1/server/users` is the disagreement the issue reports. gRPC
refuses only because it has no request proxy - that is a property of the transport, not a policy
the HTTP API should copy.

## Invariant

> A `POST`, `PUT` or `DELETE /api/v1/server/users` request never reaches
> `ServerSecurity.createUserClusterWide` / `updateUserClusterWide` / `dropUserClusterWide` on a
> node whose HA plugin reports `isLeader() == false`: it is forwarded to the leader, or refused
> with `ServerIsNotTheLeaderException` when it cannot be forwarded.

## Completeness

### 2. Every way to violate it

Writers - everything that calls a `*ClusterWide` security mutator:

```
$ grep -rn "ClusterWide(" --include='*.java' */src/main/java | grep -v ServerSecurity.java
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:323:    dropDatabaseClusterWide(server.getDatabase(databaseName), databaseName);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:331:  private void dropDatabaseClusterWide(final ServerDatabase database, final String databaseName) {
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:484:    server.getSecurity().createUserClusterWide(json);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:492:    if (!server.getSecurity().dropUserClusterWide(userName))
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:532:    security.updateUserClusterWide(updatedConfig);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1638:    dropDatabaseClusterWide(server.getDatabase(databaseName), databaseName);
server/src/main/java/com/arcadedb/server/http/handler/DeleteDropUserHandler.java:56:    final boolean result = httpServer.getServer().getSecurity().dropUserClusterWide(userName);
server/src/main/java/com/arcadedb/server/http/handler/DeleteUserHandler.java:50:    final boolean deleted = httpServer.getServer().getSecurity().dropUserClusterWide(name);
server/src/main/java/com/arcadedb/server/http/handler/PostUserHandler.java:72:    security.createUserClusterWide(userConfig);
```

Callers of the `ServerControlPlane` user operations:

```
$ grep -rn "controlPlane\.\(createUser\|dropUser\|updateUser\)" --include='*.java' */src/main/java
grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcAdminService.java:341:      controlPlane.createUser(user);
grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcAdminService.java:353:      controlPlane.dropUser(req.getUser());
grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcAdminService.java:410:      controlPlane.updateUser(req.getUser(), ...);
server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:214:    controlPlane.createUser(new JSONObject(payload));
server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:219:    controlPlane.dropUser(userName);
server/src/main/java/com/arcadedb/server/http/handler/PutUserHandler.java:59:      controlPlane.updateUser(name, password, databases);
```

Routed HTTP entry points (`HttpServer.setupRoutes`):

```
$ grep -n "server/users\|server/groups\|server/api-tokens" server/src/main/java/com/arcadedb/server/http/HttpServer.java
266:        .post("/server/api-tokens", new PostApiTokenHandler(this))
267:        .delete("/server/api-tokens", new DeleteApiTokenHandler(this))
268:        .get("/server/users", new GetUsersHandler(this))
269:        .post("/server/users", new PostUserHandler(this))
270:        .put("/server/users", new PutUserHandler(this))
271:        .delete("/server/users", new DeleteUserHandler(this))
273:        .post("/server/groups", new PostGroupHandler(this))
274:        .delete("/server/groups", new DeleteGroupHandler(this))
```

Siblings - the deprecated `DeleteDropUserHandler` has the same shape and is registered on no route:

```
$ grep -rn "DeleteDropUserHandler" . --include='*.java' --include='*.json' --include='*.yaml' --include='*.js' | grep -v handler/DeleteDropUserHandler.java
(no output, exit 1)
```

Only users replicate; groups do not:

```
$ grep -rn "replicateSecurity" --include='*.java' */src/main/java
ha-raft/.../RaftTransactionBroker.java:441:  public void replicateSecurityUsers(final String usersJson) {
ha-raft/.../RaftHAPlugin.java:204:  public void replicateSecurityUsers(final String usersJsonArray) {
ha-raft/.../PostAddPeerHandler.java:70:      plugin.replicateSecurityUsers(usersPayload);
server/.../ServerSecurity.java:422,448,475      ha.replicateSecurityUsers(...)
server/.../HAServerPlugin.java:293:  default void replicateSecurityUsers(final String usersJsonArray) {
```

`ServerControlPlane.saveGroup` calls `server.getSecurity().saveGroup(...)` with no HA branch at
all, so the group routes cannot submit a Raft entry from a follower - a different (pre-existing)
defect, not this invariant.

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/server/users` -> `createUserClusterWide` | yes | yes - `Issue7380RestUserRoutesLeaderForwardingIT` |
| `PUT /api/v1/server/users` -> `updateUserClusterWide` | yes | yes - same IT |
| `DELETE /api/v1/server/users` -> `dropUserClusterWide` | yes | yes - same IT |
| `POST /api/v1/server` `create user` / `drop user` | already gated (unchanged behaviour, re-implemented on the shared forwarder) | yes - same IT drives `create user` through a follower |
| gRPC `CreateUser` / `UpdateUser` / `DeleteUser` | already gated by `requireLeader` (#7304, #7309) | `Issue7304GrpcAdminLeaderRoutingIT` (existing) |
| `DeleteDropUserHandler` (deprecated) -> `dropUserClusterWide` | **argued** - unreachable: the class is registered on no route and referenced nowhere outside its own file (grep above, exit 1) | n/a |
| `POST` / `DELETE /api/v1/server/groups` | **filed** - out of the invariant: `saveGroup`/`dropGroup` never call HA, so no Raft entry is submitted from anywhere. That groups do not replicate at all is tracked by #7373 | n/a |
| `POST` / `DELETE /api/v1/server/api-tokens` | **filed** - they mutate a user's token list through `ServerSecurity` without a `*ClusterWide` mutator (no hit in the writers grep above); their node-locality is the other half of #7373 | n/a |

### 5. Reachability

`LeaderCommandForwarder` is constructed once per `HttpServer` (`HttpServer` constructor) and
reached from `PostUserHandler`, `PutUserHandler`, `DeleteUserHandler` and
`PostServerCommandHandler`, all four of which are registered routes (`HttpServer.setupRoutes`
lines 258, 269-271). The branch it adds runs only when `server.getHA() != null &&
!ha.isLeader()`, which is why the test is a real three-node Raft cluster rather than a unit test.

### 7. Residual risk

- A request that arrives already authenticated with the cluster token but carrying no
  `X-ArcadeDB-Forwarded-To-Leader` marker (only possible when `arcadedb.ha.clusterToken` is
  blank, since every forward this code emits sets the marker alongside the token) is forwarded
  with no `Authorization` header and the leader answers 401. That behaviour is inherited
  unchanged from `PostServerCommandHandler.forwardToLeaderIfReplica`; this change does not widen
  or narrow it.
- Groups and API tokens still do not replicate across an HA cluster at all, so no leader gate would
  help them. Out of scope here and already tracked by #7373.
- `POST /api/v1/server` `create user` / `drop user` keeps its behaviour bit for bit; what changed is
  where the code lives. The regression guard is the command-path leg of
  `theThreeRestUserRoutesCalledOnAFollowerAreExecutedOnTheLeader`, which is the only assertion that
  reaches the URL-building the refactor touched.

## Changes

| File | What |
|---|---|
| `server/.../http/handler/LeaderCommandForwarder.java` (new) | The follower-to-leader forward, lifted verbatim out of `PostServerCommandHandler` and generalised over method, path and body. Loop protection, the self-address check and the auth-header conversion are unchanged |
| `server/.../http/HttpServer.java` | One forwarder per server, so the "a peer forwarded this here and I am not the leader either" notice stays logged once per node rather than once per route |
| `server/.../http/handler/PostServerCommandHandler.java` | `forwardToLeaderIfReplica` is now a two-line delegation; 90 lines of mechanics moved out. Behaviour unchanged |
| `server/.../http/handler/PostUserHandler.java` | Forwards after `checkRootUser` and before any validation, as the command path does |
| `server/.../http/handler/PutUserHandler.java` | Same |
| `server/.../http/handler/DeleteUserHandler.java` | Same; no body, and the query string travels with the path so the leader reads the same `name` |
| `ha-raft/src/test/.../Issue7380RestUserRoutesLeaderGateIT.java` (new) | Two methods on a three-node Raft cluster: the one-hop refusal on all four routes, and the happy path through a follower on all four |

## Verification

Run:

```
mvn -o verify -pl ha-raft -DskipITs=false \
    -Dit.test=Issue7380RestUserRoutesLeaderGateIT -Dtest=Issue7380RestUserRoutesLeaderGateIT \
    -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
=> Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
```

### That the test can fail

The refusal assertions look for the body text `already forwarded` from `POST`, `PUT` and
`DELETE /api/v1/server/users`. On `main` that string exists in three files, none of which a
request to those routes touches:

```
$ git grep -n "already forwarded" main -- '*.java' | grep -v test
main:ha-raft/.../RaftReplicatedDatabase.java:3404   (the SQL write path)
main:server/.../handler/PostBatchHandler.java:1554  (the bulk-load route)
main:server/.../handler/PostServerCommandHandler.java:664,678
```

and the three handlers themselves carry no HA symbol at all:

```
$ for f in PostUserHandler PutUserHandler DeleteUserHandler; do
    git show main:server/src/main/java/com/arcadedb/server/http/handler/$f.java \
      | grep -n "getHA\|isLeader\|Leader\|forward"
  done
(no output for any of the three)
```

So the pre-fix routes answer 201/200/200 where the test demands 400, whatever the Raft group
committer does with an entry submitted from a follower - which is the question the issue
deliberately left open, and which this test therefore does not have to answer.

### Not verified here

`server`'s `UserManagementIT` - the non-HA regression suite for these same three routes - cannot
run on this machine: it dials a hard-coded `127.0.0.1:2480`, and an unrelated ArcadeDB 26.9.1
installed by Homebrew has been listening there for hours, so the test's own server never binds the
port and the requests are answered by the wrong process (observed as `expected 201 but was 500/503`,
and `curl -u root:... http://127.0.0.1:2480/api/v1/server` answering `User/Password not valid`).
The non-HA path is a single branch - `forwardIfReplica` returns null the moment `getHA()` is null -
and the leader leg of the new IT exercises the same early return with HA active. CI runs
`UserManagementIT` on a clean port.
