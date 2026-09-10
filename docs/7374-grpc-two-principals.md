# #7374 - gRPC call metadata and request body can carry two different principals

Issue: https://github.com/ArcadeData/arcadedb/issues/7374

## Problem

`RemoteGrpcDatabase` takes its own `userName`/`userPassword` and puts them in every request BODY
(`buildCredentials()`), but the stubs it issues calls on are built by `RemoteGrpcServer`, which
attaches call METADATA built from the *server's* own `userName`/`userPassword`
(`RemoteGrpcServer.createCredentials(String)` reads its own fields).

Server side, `GrpcAuthInterceptor` authenticates from the metadata and puts that name in
`USER_CONTEXT_KEY`; `ArcadeDbGrpcService.resolvedUsername()` then *prefers* the context user over the
body credentials. So when server user A and database user B differ, every gRPC call authenticates
and authorizes as A, and the B the caller passed is silently discarded - while the HTTP half of the
same object (`RemoteDatabase`) still speaks as B.

## Root cause

`RemoteGrpcDatabase.createBlockingStub()` / `createAsyncStub()` call
`remoteGrpcServer.newBlockingStub(timeout, databaseName)` / `newAsyncStub(timeout, databaseName)`,
which have no way to say *which principal*. Same for `getProgress()`, which calls
`newAdminBlockingStub(timeout)`.

## Fix

Option 1 from the issue ("the smaller surprise"): the database's stubs carry the database's own
credentials. `RemoteGrpcServer` gains principal-carrying overloads
(`newBlockingStub`/`newAsyncStub`/`newAdminBlockingStub` taking `userName`/`userPassword`, and
`createCredentials(database, user, password)`); `RemoteGrpcDatabase` passes its own pair to all three.
A `null`/blank database user falls back to the server's pair, so the pre-existing behaviour of a
database constructed without credentials is unchanged (and no `Metadata.put(null)` NPE is introduced).

## Completeness

### Invariant

> Every gRPC call issued by a `RemoteGrpcDatabase` carries in its call metadata the username and
> password that `RemoteGrpcDatabase` was constructed with, so the principal the server authenticates
> and the principal in the request body are the same one.

### Enumeration

Every stub the client builds (`grep -rn --include='*.java' -E "newBlockingStub\(|newAsyncStub\(|newAdminBlockingStub\("`,
main sources only):

```
grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java:199:  remoteGrpcServer.newBlockingStub(getTimeout(), databaseName)
grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java:206:  remoteGrpcServer.newAsyncStub(getTimeout(), databaseName)
grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java:247:  remoteGrpcServer.newAdminBlockingStub(getTimeout())          (getProgress)
grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcServer.java:266/280/291/298/315  (the factories themselves)
```

Everything else that matched is a test building a raw `ArcadeDbServiceGrpc` stub against its own
channel, not through `RemoteGrpcServer`.

Every field/stub `RemoteGrpcDatabase` issues calls on
(`grep -n "blockingStub\|asyncStub" RemoteGrpcDatabase.java`): 32 call sites, all of them
`blockingStub.withDeadlineAfter(...)` or `asyncStub.withDeadlineAfter(...)` on the two fields assigned
in the constructor - `withDeadlineAfter` copies the `CallOptions`, credentials included, so fixing the
two factory methods covers all 32.

No other class in `grpc-client/src/main` builds a stub
(`grep -rn "Stub\b" grpc-client/src/main/java/.../grpc/ | grep -v "RemoteGrpcServer.java\|RemoteGrpcDatabase.java"` -> no output),
so `QueryBatch`, `BatchedStreamingResultSet`, `GraphBatchLoadStream`, `RemoteGrpcGraphBatch`,
`StreamingResultSet` and `RemoteGrpcTransactionExplicitLock` all issue through the database's stubs.

Subclasses (`grep -rn "extends RemoteGrpcDatabase"`): `RemoteGrpcDatabaseWithCompression` only, which
delegates to the fixed constructor and does not override either factory.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `RemoteGrpcDatabase.createBlockingStub()` - query/command/CRUD/streaming/vector/timeseries (all 30 blocking call sites) | yes | yes - `Issue7374GrpcDatabaseCredentialsTest.blockingStubCarriesTheDatabaseUser`, `Issue7374GrpcPrincipalIT` (query, per-type ACL, foreign-database refusal) |
| `RemoteGrpcDatabase.createAsyncStub()` - bidi ingestion / graph batch (2 async call sites) | yes | yes - `Issue7374GrpcDatabaseCredentialsTest.asyncStubCarriesTheDatabaseUser` |
| `RemoteGrpcDatabase.getProgress()` -> `newAdminBlockingStub` | yes | yes - `Issue7374GrpcDatabaseCredentialsTest.adminStubCarriesTheDatabaseUser` |
| `RemoteGrpcDatabaseWithCompression` (subclass) | yes, inherited | yes - `Issue7374GrpcDatabaseCredentialsTest.compressionSubclassInheritsTheDatabaseUser` |
| `RemoteGrpcDatabase` constructed with a null/blank user | yes - falls back to the server's pair, unchanged behaviour | yes - `Issue7374GrpcDatabaseCredentialsTest.blankDatabaseUserFallsBackToTheServerUser` |
| `RemoteGrpcServer`'s own RPCs (`listDatabases`, `getProgress(String)`, `createDatabase`, ...) | argued: not in scope | n/a |
| Server-side `resolvedUsername()` preferring the context user over the body credentials | argued: not in scope | n/a |

### Arguments

- **`RemoteGrpcServer`'s own RPCs.** `RemoteGrpcServer` is the object that holds the server account;
  when it calls `listDatabases()` or `getProgress(String)` on its own behalf, its user *is* the
  principal and metadata and body already agree (`buildCredentials()` reads the same two fields as
  `createCredentials()`). There is no second principal to disagree with.
- **Server-side precedence.** `ArcadeDbGrpcService.resolvedUsername()` preferring the interceptor's
  context user is what makes the mismatch invisible, but it is not itself wrong: the metadata
  credentials are the ones the server actually verified a password for, and the body credentials are
  unverified when a context user exists. Changing that precedence would mean re-authenticating the
  body on every call. Once the client sends one principal the question does not arise, and hardening
  the server against a *deliberately* mismatched client is a separate concern from this client bug.

### Residual risk

A caller that reaches into `RemoteGrpcServer.newBlockingStub(timeout, database)` directly still gets
the server's principal - that overload is unchanged and is the right one for the server's own calls.
Nothing in `src/main` does so other than the two fixed factories.

## Changes

- `grpc-client/.../RemoteGrpcServer.java` - added `newBlockingStub(int, String, String, String)`,
  `newAsyncStub(int, String, String, String)`, `newAdminBlockingStub(int, String, String)` and
  `createCredentials(String, String, String)`. The pre-existing overloads now delegate to them with a
  null principal, so they keep meaning "this server's own account" and nothing that called them changes.
- `grpc-client/.../RemoteGrpcDatabase.java` - `createBlockingStub()` and `createAsyncStub()` pass this
  database's `userName`/`userPassword`; the admin stub `getProgress()` builds moved into a new
  protected `createAdminBlockingStub()` that does the same (and is what makes it testable).

Purely additive on the public API: no signature changed, no method removed.

## Reachability

`createBlockingStub()`/`createAsyncStub()` are called from `RemoteGrpcDatabase`'s constructor, which is
`new`-ed in the `e2e` and `load-tests` modules and is the documented client entry point; the 32 call
sites listed above all issue on the two fields those calls assign. `createAdminBlockingStub()` is
called by `getProgress()`. Nothing here is behind a flag.

## Test results

```
mvn -o -pl grpc-client -DskipITs=true test
  Tests run: 161, Failures: 0, Errors: 0, Skipped: 0   BUILD SUCCESS
    (includes Issue7374GrpcDatabaseCredentialsTest: 7, Issue7320GrpcDatabaseHeaderTest: 4,
     RemoteGrpcServerInsecureCredentialsTest: 6)

mvn -o -pl grpc-client -DskipITs=false -Dit.test=Issue7374GrpcPrincipalIT verify
  Tests run: 3, Failures: 0, Errors: 0                 BUILD SUCCESS

mvn -o -pl e2e,load-tests -DskipTests test-compile     BUILD SUCCESS
```

Both suites were run against the pre-fix wiring first, to prove they can fail:

```
Issue7374GrpcDatabaseCredentialsTest   Tests run: 7, Failures: 5   (expected "scoped7374")
Issue7374GrpcPrincipalIT#theDatabaseUserIsTheOneTheEngineAuthorizes        FAILED
Issue7374GrpcPrincipalIT#aForeignDatabaseUserIsRefusedEvenOnARootServer    FAILED
```

The two that still pass pre-fix are the two that should: `blankDatabaseUserFallsBackToTheServerUser`
and `serverScopedStubStillCarriesTheServerUser` pin behaviour this change deliberately leaves alone.

### Pre-existing failures on this machine, not caused by this change

`Issue7320ScopedUserGrpcIT` (3 errors) and `Issue7305TimeSeriesGrpcAclIT` (1 failure) fail here because
`BaseGraphServerTest.command(int, String)` posts to a hardcoded `http://127.0.0.1:2480`, and another
ArcadeDB server on this machine owns that port - so their DDL lands on a foreign server and the types
are missing:

```
Issue7320ScopedUserGrpcIT ... IOException: Server returned HTTP response code: 503 for URL: http://127.0.0.1:2480/api/v1/command/graph
Issue7305TimeSeriesGrpcAclIT ... RecordNotFoundException: Type 'SecretMetrics' does not exist
$ lsof -nP -iTCP:2480 -sTCP:LISTEN
java  50517 frank  264u  IPv6  TCP 127.0.0.1:2480 (LISTEN)
java  93797 frank  263u  IPv6  TCP *:2480 (LISTEN)
```

The new `Issue7374GrpcPrincipalIT` avoids that trap on purpose: it runs its DDL against
`getServer(0).getDatabase(...)` and reaches HTTP only on `getServer(0).getHttpServer().getPort()`.

## Impact

A caller that built a `RemoteGrpcServer` and a `RemoteGrpcDatabase` with the SAME user sees no change -
which is every test and every documented example. A caller that passed different users now has its gRPC
calls run as the database's user, which is what it asked for and what the HTTP half of the same object
already did. That is a behaviour change in the direction of the argument the caller passed, and it can
newly refuse calls that previously rode in on the server's account - which is the point of the issue.

## Residual risk

See the Completeness section: only `RemoteGrpcServer`'s own server-scoped overloads still carry the
server's principal, deliberately, and nothing in `src/main` uses them for a database's calls.

## Adversarial pass

Run inline rather than through a subagent: no `Task` tool was available in this session, so the pass was
done by re-reading the tree against the issue text rather than by an agent that had not been convinced.
That is a weaker version of the check and is recorded as such.

| Finding | Verified by | Disposition |
|---|---|---|
| The data-plane stubs are `final` fields built once in the constructor, so a `RemoteGrpcServer.close()`/`start()` cycle leaves them bound to a terminated channel - while `getProgress()`, built per call, survives it | `grep -n "private final    ArcadeDbServiceGrpc" RemoteGrpcDatabase.java` (lines 158-159, assigned at 185-186, never reassigned); `RemoteGrpcServer.start()` lines 203-237 build a NEW channel after `close()` nulls it | Real, out of scope (channel lifetime, not principal identity). Filed as **#7416** |
| Credentials could go stale in the cached stubs if a caller changed them after construction | `grep -rn "setUserName\|setUserPassword\|this.userName *=" network/src/main/java/com/arcadedb/remote/ grpc-client/src/main/java/` - the only assignments are the three constructors; there are no setters | Not real. The pair is assign-once, so a stub built in the constructor cannot go stale |
| `buildCredentials()` (request body) reads the inherited `getUserName()` while the stubs read this class's own `userName` field, so the two could diverge | Both are assigned from the same constructor parameter (`super(...)` at line 180, `this.userName =` at 183). Pinned by `Issue7374GrpcDatabaseCredentialsTest.bodyAndMetadataNameTheSamePrincipal`, which compares them | Not real, and now regression-tested |
| `Issue7374GrpcDatabaseCredentialsTest` depends on nothing listening on its `DEAD_HTTP_PORT` | `RemoteHttpComponent.requestClusterConfiguration()` swallows every failure except a `SecurityException`, so only a real ArcadeDB server answering 401/403 on that exact port would break it | Accepted risk, documented on the constant |

## Follow-up issues

- **#7416** - `RemoteGrpcDatabase` caches its data-plane stubs on a channel a server restart replaces.
