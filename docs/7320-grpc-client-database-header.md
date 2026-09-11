# #7320 - gRPC client sends no database header, so every call authenticates against "default"

## Problem

`GrpcAuthInterceptor` authenticates every data-plane call against the database named in the
`x-arcade-database` request metadata, falling back to the literal name `"default"` when the header
is absent:

```java
// grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java (pre-fix)
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
grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java:163:      final String database = normalizeDatabase(headers.get(DATABASE_HEADER));
```

One reader, one use site.

### Writers of call metadata in the shipped client

```
$ grep -rn "Metadata.Key.of" --include="*.java" grpc-client/src/main
grpc-client/.../GrpcClientErrorMapper.java:48:  static final Metadata.Key<String> EXCEPTION_CLASS_KEY = ...
grpc-client/.../GrpcClientErrorMapper.java:50:  static final Metadata.Key<String> DUP_INDEX_KEY        = ...
grpc-client/.../GrpcClientErrorMapper.java:52:  static final Metadata.Key<String> DUP_KEYS_KEY         = ...
grpc-client/.../RemoteGrpcServer.java (createCallCredentials -> credentials(): username/password/x-arcade-user/x-arcade-password)
grpc-client/.../RemoteGrpcServer.java (createCredentials     -> credentials(): the same four, plus x-arcade-database)
```

`GrpcClientErrorMapper`'s three keys are *trailer* keys read off a failed response, not request
metadata - they cannot carry a database. The two `CallCredentials` factories are the whole set of
request-metadata writers.

### Stub construction in the shipped client

```
$ grep -rn "ArcadeDbServiceGrpc.new\|ArcadeDbAdminServiceGrpc.new" --include="*.java" grpc-client/src/main grpcw/src/main
grpc-client/.../RemoteGrpcServer.java:259:    return ArcadeDbServiceGrpc.newBlockingV2Stub(channel())        -> createCredentials(database)
grpc-client/.../RemoteGrpcServer.java:277:    return ArcadeDbServiceGrpc.newStub(channel())                  -> createCredentials(database)
grpc-client/.../RemoteGrpcServer.java:287:    ... = ArcadeDbAdminServiceGrpc.newBlockingV2Stub(channel())    -> createCallCredentials()
```

### Callers of the data-plane stub factories

```
$ grep -rn "createBlockingStub\|createAsyncStub" --include="*.java" . | grep -v target
grpc-client/.../RemoteGrpcDatabase.java:181:    this.blockingStub = createBlockingStub();
grpc-client/.../RemoteGrpcDatabase.java:182:    this.asyncStub = createAsyncStub();
grpc-client/.../RemoteGrpcDatabase.java:194:  protected ... createBlockingStub() {
grpc-client/.../RemoteGrpcDatabase.java:201:  protected ... createAsyncStub() {
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

No. The check it removes was never the authorization gate, and every caller it now lets past the
interceptor is authorized per-RPC one layer down, against the database the request body names.

The gate itself, read in full:

```
$ sed -n '/private .*void validateCredentials(/,/^  }/p' grpcw/.../ArcadeDbGrpcService.java
private void validateCredentials(final DatabaseCredentials credentials, final String databaseName) {
  final String authenticatedUser = resolvedUsername(credentials);
  if (authenticatedUser == null)
    throw Status.UNAUTHENTICATED...
  ...
    final Set<String> allowedDatabases = user.getAuthorizedDatabases();
    if (!allowedDatabases.contains(SecurityManager.ANY) && !allowedDatabases.contains(databaseName))
      throw Status.PERMISSION_DENIED.withDescription("User has not access to database '" + databaseName + "'")
  ...
    } else {  // no interceptor context: authenticate AND authorize from the body credentials
      security.authenticate(authenticatedUser, password, databaseName);
```

Both of its branches enforce the per-database grant. What had to be checked is that every RPC
reaches it. There are 22 data-plane RPCs on this service - eight more than when this branch was cut,
because #7305 and #7306 landed in between - so the audit was scripted rather than eyeballed:

```
$ python3 - <<'EOF'   # every "public void|StreamObserver" member of ArcadeDbGrpcService, and its gate
...
EOF
executeCommand           line   677  getDatabase
createRecord             line   997  getDatabase
lookupByRid              line  1137  getDatabase
updateRecord             line  1209  getDatabase
deleteRecord             line  1400  getDatabase
executeQuery             line  1504  getDatabase
beginTransaction         line  1735  getDatabase
commitTransaction        line  1875  authorizeTransactionAccess
rollbackTransaction      line  1961  authorizeTransactionAccess
streamQuery              line  2038  getDatabase
bulkInsert               line  2622  InsertContext->getDatabase
insertStream             line  2697  getDatabase+InsertContext->getDatabase
graphBatchLoad           line  3048  getDatabase
timeSeriesWrite          line  3272  getDatabase
timeSeriesWriteStream    line  3299  getDatabase
timeSeriesQuery          line  3400  getDatabase
timeSeriesLatest         line  3631  getDatabase
insertBidirectional      line  3841  getDatabase+InsertContext->getDatabase
vectorSearch             line  5085  getDatabase
hybridSearch             line  5095  getDatabase
fullTextSearch           line  5105  getDatabase+validateCredentials
```

No row is ungated. `getDatabase(databaseName, credentials)` calls `validateDatabaseName` and then
`validateCredentials(credentials, databaseName)` before it touches the server;
`authorizeTransactionAccess(txCtx, credentials)` calls it against the transaction's REAL database.
The streaming RPCs reach the first through `InsertContext`, whose constructor is a thousand lines
from the RPC:

```
$ grep -n "InsertContext(InsertOptions opts)" -A 5 grpcw/.../ArcadeDbGrpcService.java
4406:    InsertContext(InsertOptions opts) {
4410:      this.db = getDatabase(opts.getDatabase(), opts.getCredentials());
```

Before the fix the interceptor authorized `"default"` - never the RPC's actual target - so the only
principals it ever admitted were those granted `"*"`, who are authorized everywhere anyway. Removing
it therefore grants no principal access to any database the layer below would not already grant.
`Issue4794GrpcPerDbAuthorizationIT` pins that layer and stays green (4/4, run below).

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `RemoteGrpcServer.newBlockingStub` -> `createCredentials` (data-plane blocking stub) | yes | yes - `Issue7320GrpcDatabaseHeaderTest.blockingStubCredentialsCarryTheTargetDatabase`, `Issue7320ScopedUserGrpcIT.scopedUserCanQueryOverGrpc` |
| `RemoteGrpcServer.newAsyncStub` -> `createCredentials` (data-plane async/streaming stub) | yes | yes - `Issue7320GrpcDatabaseHeaderTest.asyncStubCredentialsCarryTheTargetDatabase`, `Issue7320ScopedUserGrpcIT.scopedUserCanIngestOverTheAsyncStub` |
| `RemoteGrpcServer.adminServiceBlockingV2Stub` -> `createCallCredentials` (admin plane) | **argued** - admin RPCs take the `com.arcadedb.grpc.ArcadeDbAdminService/` branch of `interceptCall`, which returns before the `DATABASE_HEADER` read and authenticates from the request body via `authenticateAdminRequest` -> `validateCredentials(user, pass, null)`, already database-less | n/a - `Issue7304GrpcControlPlaneAuthorizationIT` (49/49) and `Issue5039GrpcAdminAuthorizationIT` (3/3) stay green |
| `GrpcAuthInterceptor` basic-auth branch with **no** database header (any third-party client) | yes - authenticates at server level instead of against `"default"` | yes - `Issue7320GrpcAuthInterceptorDatabaseHeaderTest.absentHeaderAuthenticatesAtServerLevel` / `.emptyHeaderAuthenticatesAtServerLevel` |
| `GrpcAuthInterceptor` basic-auth branch **with** a database header | yes - unchanged behaviour, plus the refusal now names the database | yes - `...presentHeaderStillAuthenticatesAgainstThatDatabase`, `...refusalNamesTheDatabaseThatWasChecked`, `...wrongPasswordIsStillRefused`, and end-to-end `Issue7320ScopedUserGrpcIT.userGrantedAnotherDatabaseIsRefusedAndTheRefusalNamesIt` |
| `GrpcAuthInterceptor` bearer-token branch | **argued** - verified by reading `getValidSession(token, database)` in full: `database` appears in exactly three `Level.FINE` log messages and nowhere else, so no grant check ever ran on this path and the fallback value was never load-bearing | n/a - the existing `GrpcAuthInterceptorTest` token cases stay green (9/9) |

No blank rows.

### Reachability

- `RemoteGrpcServer.newBlockingStub` / `newAsyncStub` are called from `RemoteGrpcDatabase`'s
  constructor (lines 181-182), which every gRPC client path goes through - not behind a flag. Both
  stub fields are `final` and every one of the class's RPC call sites uses one of them.
- The only in-tree subclass, `RemoteGrpcDatabaseWithCompression`, overrides neither factory. Both
  stay `protected`, so an out-of-tree subclass that does override them keeps compiling.
- `GrpcAuthInterceptor` is installed by `GrpcServerPlugin` on the live server; the changed line is on
  the basic-auth path every data-plane RPC takes when security is enabled.
- Both halves are exercised against a real server by `Issue7320ScopedUserGrpcIT`.

### Proof the tests fail without the fix

The three production files were restored to their pre-fix content (`git show HEAD~1:<path>`) and the
suites re-run:

```
grpcw unit tests, pre-fix:
[ERROR] Issue7320GrpcAuthInterceptorDatabaseHeaderTest.absentHeaderAuthenticatesAtServerLevel:108
[ERROR] Issue7320GrpcAuthInterceptorDatabaseHeaderTest.emptyHeaderAuthenticatesAtServerLevel:123
[ERROR] Issue7320GrpcAuthInterceptorDatabaseHeaderTest.refusalNamesTheDatabaseThatWasChecked:159
[ERROR] Issue7320GrpcAuthInterceptorDatabaseHeaderTest.wrongPasswordIsStillRefused:177
[ERROR] Tests run: 221, Failures: 4, Errors: 0, Skipped: 0

Issue7320ScopedUserGrpcIT, pre-fix:
[ERROR] Tests run: 3, Failures: 1, Errors: 2
com.arcadedb.remote.RemoteException: gRPC error: Invalid credentials
Caused by: io.grpc.StatusRuntimeException: UNAUTHENTICATED: Invalid credentials
```

The IT fails with the exact symptom the issue reports. The fifth interceptor case,
`presentHeaderStillAuthenticatesAgainstThatDatabase`, passes before and after by design: it is the
unchanged-behaviour guard, and a test that changed colour there would mean the fix had moved
something it should not.

### Test results (with the fix)

```
grpcw  unit:   Tests run: 221, Failures: 0, Errors: 0, Skipped: 0
grpc-client unit: Tests run: 154, Failures: 0, Errors: 0, Skipped: 0
Issue7320ScopedUserGrpcIT:  Tests run: 3, Failures: 0, Errors: 0
Authorization regression ITs (Issue4794GrpcPerDbAuthorizationIT, Issue5039GrpcAdminAuthorizationIT,
  Issue5040GrpcTransactionHijackIT, Issue7304GrpcControlPlaneAuthorizationIT,
  GrpcTransactionScriptingAuthorizationIT, Issue4793GrpcGetDatabaseSecurityIT):
  Tests run: 70, Failures: 0, Errors: 0
```

One caveat on how that last line was reached: the gRPC server port is a fixed 50051 with no range,
so two agents running gRPC ITs on this machine collide, and the collision reads as
`ServerException: Error starting plugin: GrpcServerPlugin` / `IOException: Failed to bind to address
0.0.0.0/0.0.0.0:50051`, not as a port message in the summary. Thirteen errors in the first run were
all that, confirmed with `lsof -nP -iTCP:50051` (another JVM held it) and cleared by re-running once
the port was free. Nothing was loosened to get there.

## Residual risk

- The header is attached per **stub**, captured when `RemoteGrpcDatabase` is constructed. A caller
  that obtains a stub from `RemoteGrpcServer.newBlockingStub(timeout)` directly (the no-database
  overload, kept for source compatibility) still sends no header - and is then authenticated at
  server level by the server half rather than refused. That is the intended pairing, not a gap.
  `RemoteDatabase.databaseName` is assigned once in the constructor and has no setter, so a stub
  cannot go stale against a re-pointed database.
- Per-database authorization is unchanged: it is enforced in
  `ArcadeDbGrpcService.validateCredentials(credentials, databaseName)` against the **request-body**
  database, which is what `Issue4794GrpcPerDbAuthorizationIT` pins.
- The refusal now repeats the message `ServerSecurity` produced. Reading `ServerSecurity.authenticate`,
  the three it can produce are `"User/Password not valid"` (one message for both a missing user and a
  wrong password, so no user enumeration), `"User has not access to database 'X'"` (X is the name the
  caller itself sent) and the lockout message. HTTP already reports the same strings, so this is
  parity, not new disclosure.
- **#7374** - a `RemoteGrpcDatabase` built with a different user than its `RemoteGrpcServer` sends the
  server's user on the metadata and its own in the request body. Pre-existing and independent: wrong
  the same way before and after this fix. Filed, not fixed here.
- **#7375** - `Issue7305TimeSeriesGrpcAclIT` still grants its scoped user `"*"` databases, with a
  comment pointing at this issue. The workaround is now unnecessary, but narrowing it means editing
  an existing test, which this workflow does not do. Filed, not fixed here.

## Adversarial pass

Phase 1.5 asks for an isolated subagent that has not been convinced by the author's reasoning. **No
`Task` tool was available in this session**, so no such subagent could be spawned, and the pass was
run by the author against the diff instead. That is weaker, and is recorded here rather than
silently skipped. Each objection below was checked with a command, not from memory.

| Objection | Verdict | Evidence |
|---|---|---|
| "Dropping the `"default"` grant check widens who reaches the service layer." | Real question, not a defect. All 22 data-plane RPCs reach `validateCredentials(credentials, databaseName)`, whose both branches enforce the grant; the removed check only ever admitted `"*"` principals. | scripted RPC-gate audit + `validateCredentials` body, above; `Issue4794GrpcPerDbAuthorizationIT` 4/4 |
| "The audit was written before #7305/#7306 landed, so the eight RPCs they added are unaudited." | Real, and fixed: the branch was rebased onto current `main` and the audit re-run over all 22 RPCs, including `timeSeries*`, `vectorSearch`, `hybridSearch`, `fullTextSearch`. | table above |
| "The bearer-token branch loses a grant check when `database` becomes null." | Not real. `getValidSession(token, database)` uses `database` in three `Level.FINE` log messages and nowhere else - read in full, not grepped. | `sed -n '/getValidSession/,/^  }/p'` |
| "The end-to-end IT passes with only the server half applied, so it does not prove the client sends anything." | Real, and answered by construction: `Issue7320GrpcDatabaseHeaderTest` reads the metadata back off the very stub `RemoteGrpcDatabase` builds, so the client half has an assertion that does not depend on the IT. | `Issue7320GrpcDatabaseHeaderTest` |
| "`e.getMessage()` can be null, and a null failure reads as success." | Real, fixed in this branch: `authenticationFailure` falls back to `"Invalid credentials"` on a null or blank message rather than returning null. | `GrpcAuthInterceptor.authenticationFailure` |
| "Echoing the security message to an unauthenticated caller leaks whether a user exists." | Not real. `ServerSecurity.authenticate` answers a missing user and a wrong password with the same `"User/Password not valid"`. | `ServerSecurity.authenticate` lines 226-231 |
| "The IT hardcodes port 2480, so whatever already listens there answers it." | Real, fixed before the PR: the IT now derives the HTTP port from `getServer(0).getHttpServer().getPort()` and creates its users through `POST /api/v1/server/users`, the way `Issue7305TimeSeriesGrpcAclIT` does. gRPC's own port stays 50051 because the plugin has no range. | rewritten `Issue7320ScopedUserGrpcIT` |
| "The user the caller passes to `RemoteGrpcDatabase` is never the one on the wire." | Real, out of scope. Filed as **#7374**. | `RemoteGrpcDatabase.buildCredentials` vs `RemoteGrpcServer.createCredentials` |
| "A test that works around this bug still carries the workaround and a comment saying the bug is open." | Real, out of scope (editing an existing test). Filed as **#7375**. | `Issue7305TimeSeriesGrpcAclIT:182-194` |

## Drive-by

`RemoteGrpcServer.createCredentials()` carried a commented-out `curl`-style example containing a
password-shaped literal (`x-arcade-password: oY9uU2uJ8nD8iY7t`). The comment described the very header
this issue is about, and the replacement code names the key explicitly, so the comment went with it.

## Note on provenance

An earlier session left this worktree with the fix and its tests uncommitted, unrebased (cut before
#7305/#7306 merged) and unverified - no commit, no branch on `origin`, no PR. This run adopted that
work rather than discarding it, then rebased it onto `main`, re-ran the completeness sweep over the
grown RPC surface, rewrote the IT off the hardcoded port, proved the tests fail without the fix, and
filed the two follow-ups. Nothing here is inherited on trust.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7377

## Review cycles

- **cycle 1 - `432716f5`** - `claude` reviewed the diff and re-derived the doc's claims against the
  source rather than taking them: it re-read `ServerSecurity.authenticate` and confirmed the bounded
  set of three messages (so echoing `e.getMessage()` adds no user-enumeration vector), confirmed the
  blank-vs-absent normalization is symmetric on both sides of the wire, confirmed
  `userName`/`userPassword` are `Objects.requireNonNull`'d in the `RemoteGrpcServer` constructor so
  the unconditional `headers.put` cannot NPE, and confirmed `RemoteGrpcDatabase.databaseName` is
  `final`. Verdict: **no bugs, no blocking issues**, security a net narrowing. No code changes were
  applied, so no second cycle was needed. Two non-blocking observations, both declined with reasons:

  1. *"`Issue7320ScopedUserGrpcIT.createUser` duplicates the `HttpURLConnection` POST helper from
     `Issue7305TimeSeriesGrpcAclIT` almost verbatim - worth lifting into a shared helper if a third
     gRPC IT needs it."* Declined here, and the reviewer's own condition says why: there are two
     copies, not three. Extracting a shared helper means editing `Issue7305TimeSeriesGrpcAclIT`, an
     existing test this workflow does not modify, and #7375 is already queued to touch that file -
     which is the right moment to extract it, with a third caller in sight or not.
  2. *"The bearer-token branch's `database` parameter is effectively dead (used only in `FINE` log
     messages); consider wiring it into a real check or dropping it."* Declined: the parameter is not
     dead, it is diagnostic - it is what tells an operator reading FINE logs which database a
     rejected token was aimed at. Wiring it into a real grant check would be a behaviour change to
     the token path, which is not what this issue reports and would want its own review. Worth
     knowing: after this fix those three log lines print `database: null` for a header-less call,
     which is honest (no database was named) but reads oddly. Left alone rather than churning a PR
     the reviewer passed.

## Final state

`clean-approval` - one cycle, no review-driven code changes, no deferred items.
