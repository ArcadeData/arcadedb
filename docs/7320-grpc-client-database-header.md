# #7320 - gRPC client sends no database header, so every call authenticates against "default"

## Problem

`GrpcAuthInterceptor` authenticates every data-plane call against the database named in the
`x-arcade-database` request metadata, falling back to the literal name `"default"` when the header
is absent:

```java
// grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java:154
String database = headers.get(DATABASE_HEADER);
if (database == null || database.isEmpty())
  database = "default";
...
if (!validateCredentials(username, password, database))
  call.close(Status.UNAUTHENTICATED.withDescription("Invalid credentials"), new Metadata());
```

`ServerSecurity.authenticate` enforces the grant when `databaseName != null`:

```java
// server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:236
if (databaseName != null) {
  final Set<String> allowedDatabases = su.getAuthorizedDatabases();
  if (!allowedDatabases.contains(SecurityManager.ANY) && !su.getAuthorizedDatabases().contains(databaseName))
    throw new ServerSecurityException("User has not access to database '" + databaseName + "'");
}
```

`RemoteGrpcServer` never sends that header, so every call from `RemoteGrpcDatabase` was authenticated
against `"default"` - a database the caller never named and, on almost every deployment, one that
does not exist. Only a principal granted `"*"` (in practice `root`) could connect at all.

## Root cause

Two independent defects, one on each side of the wire:

1. **Client half.** `RemoteGrpcServer.createCredentials()` puts four keys on every call
   (`username`, `password`, `x-arcade-user`, `x-arcade-password`) and no database key. The class is
   server-scoped and never knew the target database; `RemoteGrpcDatabase`, which does, had no way to
   pass it.
2. **Server half.** The `"default"` fallback authenticates against a name the caller never sent.
   Failing this way is not even a conservative default: it refuses every correctly-granted principal
   while granting nothing extra, because per-database authorization is separately enforced downstream
   against the *request-body* database.

## Invariant the fix establishes

A gRPC call is authenticated against the database it actually targets, or against no database at
all - never against a database name the caller never sent.

Two halves:
- (a) the shipped client puts `x-arcade-database` on every data-plane call, set to the database the
  `RemoteGrpcDatabase` targets;
- (b) when a call carries no database header, the interceptor authenticates the credentials at server
  level (`database == null`) instead of against the literal name `"default"`.

## Completeness

### Readers of the header (server side)

```
$ grep -rn "DATABASE_HEADER" --include="*.java" grpcw/src/main grpc-client/src/main server/src/main
grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java:51:  private static final Metadata.Key<String> DATABASE_HEADER      =
grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java:154:      String database = headers.get(DATABASE_HEADER);
```

One reader, one use site.

### Writers of call metadata in the shipped client

```
$ grep -rn "Metadata.Key.of" --include="*.java" grpc-client/src/main
grpc-client/.../GrpcClientErrorMapper.java:48:  static final Metadata.Key<String> EXCEPTION_CLASS_KEY = ...
grpc-client/.../GrpcClientErrorMapper.java:50:  static final Metadata.Key<String> DUP_INDEX_KEY        = ...
grpc-client/.../GrpcClientErrorMapper.java:52:  static final Metadata.Key<String> DUP_KEYS_KEY         = ...
grpc-client/.../RemoteGrpcServer.java:593-596:  (createCallCredentials: username/password/x-arcade-user/x-arcade-password)
grpc-client/.../RemoteGrpcServer.java:613-616:  (createCredentials:     username/password/x-arcade-user/x-arcade-password)
```

`GrpcClientErrorMapper`'s three keys are *trailer* keys read off a failed response, not request
metadata - they cannot carry a database. The two `CallCredentials` factories are the whole set of
request-metadata writers.

### Stub construction in the shipped client

```
$ grep -rn "ArcadeDbServiceGrpc.new\|ArcadeDbAdminServiceGrpc.new" --include="*.java" grpc-client/src/main grpcw/src/main
grpc-client/.../RemoteGrpcServer.java:224:    return ArcadeDbServiceGrpc.newBlockingV2Stub(channel())        -> createCredentials()
grpc-client/.../RemoteGrpcServer.java:232:    return ArcadeDbServiceGrpc.newStub(channel())                  -> createCredentials()
grpc-client/.../RemoteGrpcServer.java:242:    ... = ArcadeDbAdminServiceGrpc.newBlockingV2Stub(channel())    -> createCallCredentials()
```

### Callers of the data-plane stub factories

```
$ grep -rn "createBlockingStub\|createAsyncStub" --include="*.java" . | grep -v target
grpc-client/.../RemoteGrpcDatabase.java:156:    this.blockingStub = createBlockingStub();
grpc-client/.../RemoteGrpcDatabase.java:157:    this.asyncStub = createAsyncStub();
grpc-client/.../RemoteGrpcDatabase.java:164:  protected ... createBlockingStub() {
grpc-client/.../RemoteGrpcDatabase.java:168:  protected ... createAsyncStub() {
```

`RemoteGrpcDatabase` is the only in-tree caller, and the only in-tree subclass
(`RemoteGrpcDatabaseWithCompression`) overrides neither. Both factories stay `protected` so an
out-of-tree subclass that overrides them keeps working.

### Sibling clients in other languages

```
$ grep -rln "x-arcade-user" --include="*.py" --include="*.go" --include="*.js" --include="*.ts" --include="*.cs" . | grep -v target
(no output)
```

No other in-repo client authenticates over gRPC, so the client half of the fix has exactly one site.
Third-party clients that send no header are covered by the server half instead.

### Does dropping the `"default"` check widen access?

No. The check it removes was never the authorization gate, and the callers it now lets past the
interceptor are authorized per-RPC one layer down.

Every data-plane RPC resolves its target database through one of two gates, both of which call
`ArcadeDbGrpcService.validateCredentials(credentials, databaseName)`:

```
$ grep -n "^  public void \|^  public StreamObserver" grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java
665 executeCommand   985 createRecord    1125 lookupByRid     1197 updateRecord   1388 deleteRecord
1492 executeQuery    1723 beginTransaction  1863 commitTransaction  1949 rollbackTransaction
2026 streamQuery     2610 bulkInsert      2685 insertStream    3036 graphBatchLoad
3404 insertBidirectional
```

- `getDatabase(databaseName, credentials)` (line 4637) - `validateDatabaseName` then
  `validateCredentials(credentials, databaseName)` at line 4644, before `arcadeServer.getDatabase`.
- `authorizeTransactionAccess(txCtx, credentials)` (line 466) - `validateCredentials(credentials,
  txCtx.db.getName())` at line 468, against the transaction's REAL database.

The streaming RPCs reach the first gate through `InsertContext`, which is easy to miss because the
constructor is 1000 lines away from the RPC:

```
$ grep -n "InsertContext(InsertOptions opts)" -A 5 grpcw/.../ArcadeDbGrpcService.java
4406:    InsertContext(InsertOptions opts) {
4410:      this.db = getDatabase(opts.getDatabase(), opts.getCredentials());
```

Before the fix the interceptor authorized `"default"` - never the RPC's actual target - so the only
principals it ever admitted were those granted `"*"`, who are authorized everywhere anyway. Removing
it therefore grants no principal access to any database the layer below would not already grant.

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `RemoteGrpcServer.newBlockingStub` -> `createCredentials` (data-plane blocking stub) | yes | yes - `Issue7320GrpcDatabaseHeaderTest.blockingStubCredentialsCarryTheTargetDatabase`, `Issue7320ScopedUserGrpcIT.scopedUserCanQueryOverGrpc` |
| `RemoteGrpcServer.newAsyncStub` -> `createCredentials` (data-plane async/streaming stub) | yes | yes - `Issue7320GrpcDatabaseHeaderTest.asyncStubCredentialsCarryTheTargetDatabase`, `Issue7320ScopedUserGrpcIT.scopedUserCanIngestOverTheAsyncStub` |
| `RemoteGrpcServer.adminServiceBlockingV2Stub` -> `createCallCredentials` (admin plane) | **argued** - admin RPCs take the `com.arcadedb.grpc.ArcadeDbAdminService/` branch of `interceptCall`, which returns before line 154 and never reads `DATABASE_HEADER`; it authenticates from the request body via `authenticateAdminRequest` -> `validateCredentials(user, pass, null)`, already database-less | n/a |
| `GrpcAuthInterceptor` basic-auth branch with **no** database header (any third-party client) | yes - authenticates at server level instead of against `"default"` | yes - `Issue7320GrpcAuthInterceptorDatabaseHeaderTest.absentHeaderAuthenticatesAtServerLevel` |
| `GrpcAuthInterceptor` basic-auth branch **with** a database header | yes - unchanged behaviour, plus the refusal now names the database | yes - `Issue7320GrpcAuthInterceptorDatabaseHeaderTest.presentHeaderStillAuthenticatesAgainstThatDatabase` / `...refusalNamesTheDatabaseThatWasChecked` |
| `GrpcAuthInterceptor` bearer-token branch | **argued** - `getValidSession(token, database)` uses `database` only in two `Level.FINE` log messages; no grant check ever ran there, so the fallback value was never load-bearing on this path | n/a (existing `GrpcAuthInterceptorTest` token cases stay green) |

No blank rows, so no follow-up issue was filed from this sweep.

### Reachability

- `RemoteGrpcServer.newBlockingStub` / `newAsyncStub` are called from `RemoteGrpcDatabase`'s
  constructor (lines 156-157), which every gRPC client path goes through - not behind a flag.
- `GrpcAuthInterceptor` is installed by `GrpcServerPlugin` on the live server; the changed line is on
  the basic-auth path every data-plane RPC takes when security is enabled.
- Both halves are exercised against a real server by `Issue7320ScopedUserGrpcIT`, which fails on the
  pre-fix tree.

## Residual risk

- The header is attached per **stub**, captured when `RemoteGrpcDatabase` is constructed. A caller
  that obtains a stub from `RemoteGrpcServer.newBlockingStub(timeout)` directly (the no-database
  overload, kept for source compatibility) still sends no header - and is then authenticated at
  server level by the server half rather than refused. That is the intended pairing, not a gap.
- Per-database authorization is unchanged: it is enforced in
  `ArcadeDbGrpcService.validateCredentials(credentials, databaseName)` against the **request-body**
  database (`getDatabase` chokepoint, line 4644), which is what `Issue4794GrpcPerDbAuthorizationIT`
  pins. Dropping the `"default"` fallback removes a check that was never the authorization gate.

## Adversarial pass

The orchestrator's Phase 1.5 spawns an isolated subagent to write the follow-up issue it would file
against this patch. No `Task` tool was available in this session, so the pass was run by the author
against the diff instead - weaker, because it was already convinced. Recorded here so the gap is
visible rather than silently skipped.

| Objection | Verdict | Evidence |
|---|---|---|
| "Dropping the `"default"` grant check widens who reaches the service layer." | Real but not a defect - see *Does dropping the `"default"` check widen access?* above. Every one of the 14 data-plane RPCs authorizes the request-body database via `getDatabase` or `authorizeTransactionAccess`, and the removed check only ever admitted `"*"` principals. | audit above |
| "`insertBidirectional` never calls the authorization gate." | Not real. It reaches it through `new InsertContext(opts)` -> `getDatabase(opts.getDatabase(), opts.getCredentials())` (line 4410), 1000 lines from the RPC. | `grep -n "InsertContext(InsertOptions opts)" -A 5` |
| "The header is captured once at stub construction, so a `RemoteGrpcDatabase` re-pointed at another database would send a stale name." | Not real. `RemoteDatabase.databaseName` is assigned once in the constructor (`network/.../RemoteDatabase.java:100`) and there is no setter; `RemoteGrpcDatabase.databaseName` is `final`. | `grep -rn "setDatabase\b\|databaseName =" network/.../RemoteDatabase.java` |
| "`e.getMessage()` can be null, and a null failure reads as success." | Real, and fixed in this branch before the PR opened: `authenticationFailure` falls back to `"Invalid credentials"` on a null or blank message rather than returning null. | `GrpcAuthInterceptor.authenticationFailure` |
| "The end-to-end IT passes with only the server half applied, so it does not prove the client sends anything." | Real, and answered by construction: `Issue7320GrpcDatabaseHeaderTest` reads the metadata back off the very stub `RemoteGrpcDatabase` builds, so the client half has its own assertion independent of the IT. | `Issue7320GrpcDatabaseHeaderTest` |

Nothing survived as an out-of-scope defect, so no follow-up issue was filed.

## Drive-by

`RemoteGrpcServer.createCredentials()` carried a commented-out `curl`-style example containing a
password-shaped literal (`x-arcade-password: oY9uU2uJ8nD8iY7t`). The comment described the very header
this issue is about, and the replacement code names the key explicitly, so the comment went with it.
