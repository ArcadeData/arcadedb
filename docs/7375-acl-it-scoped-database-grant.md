# #7375 — `Issue7305TimeSeriesGrpcAclIT` can drop its `"*"` database grant now that #7320 is fixed

Issue: https://github.com/ArcadeData/arcadedb/issues/7375 (follow-up to #7320)

## Finding ledger

- [x] 1. **Fixed.** The scoped user in `Issue7305TimeSeriesGrpcAclIT` is granted its group on `"*"` rather than on
  the database under test, with a paragraph of comment explaining that gRPC could not authenticate a
  database-scoped principal. That gap is closed by #7320; the grant is now `getDatabaseName()` and the
  stale paragraph is replaced by a shorter one stating what the narrowed grant does and does not guard.

## Analysis

### What #7320 changed

`RemoteGrpcServer` now attaches the target database on the `x-arcade-database` metadata key, and
`GrpcAuthInterceptor` authenticates the call against it instead of falling back to the literal name
`"default"`.

```
$ grep -rn "arcade-database" --include="*.java" grpc-client/src/main grpc/src/main grpcw/src/main
grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcServer.java:142:      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);
grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcServer.java:271:   * A data-plane stub whose every call carries {@code database} on the {@code x-arcade-database}
grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java:53:      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);
```

`RemoteGrpcDatabase` is what the test drives, and both of its stub factories pass the database:

```
$ grep -n "newBlockingStub\|newAsyncStub" grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java
199:    return this.remoteGrpcServer.newBlockingStub(getTimeout(), databaseName);
206:    return this.remoteGrpcServer.newAsyncStub(getTimeout(), databaseName);
```

So the interceptor's `normalizeDatabase(headers.get(DATABASE_HEADER))` resolves to the real database
name on every RPC this test makes, and a principal holding a grant only on that name authenticates.

### Invariant

> The scoped user this test creates holds its group grant on exactly the database under test, and every
> TimeSeries RPC it drives still authenticates and is still refused per-type on `SecretMetrics`.

Narrowing the grant makes the test a second guard on #7320 — but a narrower one than it first appears,
and the difference was found by trying to falsify it rather than by reading the code. See "Proving the
narrowed grant can fail" below.

### Note on the "never modify existing tests" constraint

`resolve-issue/constraints.md` forbids modifying existing tests. This issue *is* a request to modify one,
filed by the repository owner, and the modification narrows a grant rather than weakening an assertion —
strictly more restrictive, so no assertion can start passing for a new reason. Recorded here rather than
applied silently.

## Proving the narrowed grant can fail

A test that stays green is not yet evidence. Two falsification attempts, both run against the narrowed
grant:

**Attempt 1 — client stops sending the header.** Patched `RemoteGrpcDatabase.createBlockingStub` /
`createAsyncStub` to pass `null` instead of `databaseName`, rebuilt, re-ran the IT.

> Result: **still green.** The first draft of the code comment claimed this would go red. It was wrong.
> `GrpcAuthInterceptor.normalizeDatabase` maps a null/blank header to `null`, and
> `authenticationFailure(user, pass, null)` then authenticates at *server* level without a grant check —
> which any valid principal passes. Per-database authorization is enforced downstream in
> `ArcadeDbGrpcService.validateCredentials` against the database in the request body, so the call still
> succeeds and the per-type ACL still refuses `SecretMetrics`.

**Attempt 2 — the actual pre-#7320 shape.** Header dropped *and* the interceptor's old substitution
restored (`normalizeDatabase` returning the literal `"default"` for an absent header).

> Result: **red**, on the first RPC:
> ```
> [querying a denied TimeSeries type over gRPC must be refused]
> Expecting actual throwable to be an instance of: java.lang.SecurityException
> but was: com.arcadedb.remote.RemoteException: gRPC error: User has not access to database 'default'
> ```

Both production files were restored from backup afterwards and `grpcw` reinstalled clean; `git status`
shows the test file and this doc as the only changes.

So the verified claim, and the one the code comment now makes, is narrow: **the narrowed grant catches
the interceptor authenticating against a substituted database name.** It does not catch the header alone
going missing. `Issue7320ScopedUserGrpcIT` stays the primary guard. The first draft of the comment
overstated this and was corrected before commit.

## Completeness

### Entry points that can violate the invariant

Every place a test grants a group on `"*"` because a database-scoped grant would not work over gRPC.

```
$ grep -rn 'put("databases"' --include="*.java" . | grep -v /target/ | grep '"\*"'
ha-raft/.../RaftUserManagement3NodesIT.java:114,161,176,221
ha-raft/.../RaftUserSeedOnPeerAdd3NodesIT.java:100
grpc-client/src/test/java/com/arcadedb/server/security/Issue7305TimeSeriesGrpcAclIT.java:194
server/src/test/java/com/arcadedb/server/security/ServerSecurityUsersConcurrencyTest.java:71,141
server/src/test/java/com/arcadedb/server/security/SecurityUserFileRepositoryTest.java:65
```

```
$ grep -rln 'falls back to the literal name\|pre-existing gRPC authentication gap' --include="*.java" . | grep -v /target/
grpc-client/src/test/java/com/arcadedb/server/security/Issue7305TimeSeriesGrpcAclIT.java
grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java
```

The second grep is the decisive one: the `"*"`-as-#7320-workaround rationale appears in exactly one test
file, the one this issue names. The interceptor hit is the fix's own explanatory comment, not a workaround.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `Issue7305TimeSeriesGrpcAclIT.createScopedUser` → `"*"` grant (the reported site) | yes | yes — the class itself; the four RPCs it drives now run as a database-scoped principal |
| `ha-raft` `RaftUserManagement3NodesIT`, `RaftUserSeedOnPeerAdd3NodesIT` → `"*"` grant | n/a — argued | these are HTTP/HA user-replication tests whose subject IS the `"*"` grant being replicated across nodes; no gRPC client, no #7320 workaround comment. Narrowing them would change what they test |
| `ServerSecurityUsersConcurrencyTest`, `SecurityUserFileRepositoryTest` → `"*"` grant | n/a — argued | unit tests of the user store's concurrency and file round-trip; the grant value is payload, never authenticated over any wire protocol |
| `Issue7320ScopedUserGrpcIT` → per-database grant | already correct | yes — grants `admin` on one named database, the primary guard the issue points at |
| `grpcw` ITs (`Issue4794GrpcPerDbAuthorizationIT`, `Issue5039…`, `Issue7304…`, `Issue7308…`, `Issue7309…`, `Issue7310…`) | n/a — argued | inspected via the grep in section C: none carries a `"*"` databases grant, so none carries this workaround |

No blank rows. No follow-up issue needed.

### Reachability

The changed code is a test. It runs in the `grpc-client` module's Failsafe lane (`*IT`), and it was
observed both passing and — under the injected regression above — failing, so it is genuinely executed
and genuinely able to fail.

## Test results

```
mvn -o -pl grpc-client verify -Dit.test='Issue7305TimeSeriesGrpcAclIT,Issue7320ScopedUserGrpcIT' \
    -DfailIfNoTests=false -DskipITs=false

Tests run: 154, Failures: 0, Errors: 0, Skipped: 0     (surefire, grpc-client unit tests)
Tests run: 1,   Failures: 0, Errors: 0, Skipped: 0  -- Issue7305TimeSeriesGrpcAclIT
Tests run: 3,   Failures: 0, Errors: 0, Skipped: 0  -- Issue7320ScopedUserGrpcIT
BUILD SUCCESS
```

## Residual risk

None beyond the test itself. This change touches no production code, so it cannot regress runtime
behaviour; the only way it can be wrong is by being red, which the run below rules out. It does not
extend #7320's coverage into new RPCs, and it does not catch a missing `x-arcade-database` header on its
own (attempt 1 above) — `Issue7320ScopedUserGrpcIT` remains the primary guard. What it adds is a second,
independent tripwire on the substituted-database-name regression.

## Adversarial pass

The `Task` tool is disabled in this session, so the independent subagent this workflow normally spawns
could not run. The checks it would have been asked for were run directly instead — which is weaker,
because the same person who wrote the patch ran them. Recorded as a limitation, not as an equivalent.

The one finding that mattered was not found here at all; it was found by the falsification attempt above,
which is why that attempt is documented in full: the first draft of the code comment asserted something
the tree does not do.

| Check | Evidence read | Verdict |
|---|---|---|
| Does `getDatabaseName()` name the database the gRPC calls actually target? | Line 156 passes `getDatabaseName()` as `RemoteGrpcDatabase`'s `databaseName`; line 172 keys the group config on the same call; line 195 now grants on the same call. All three agree | not a finding |
| Is `x-arcade-database` really attached? | `RemoteGrpcServer.credentials(...)`: `if (targetDatabase != null) headers.put(ARCADE_DATABASE_KEY, targetDatabase)` | not a finding — attached whenever non-blank |
| Does `normalizeDatabase` behave as the new comment says? | `GrpcAuthInterceptor:282-284` — `return database == null \|\| database.isBlank() ? null : database;` | not a finding — the corrected comment matches |
| Does the test reach an ADMIN RPC, whose auth carries no database? | `newAdminBlockingStub` has exactly one caller in `RemoteGrpcDatabase` (line 247, `getProgress`), which this test never calls. `close()` only rolls back | not a finding |
| Other `"*"`-because-gRPC workarounds left behind | The two greps in the Completeness section: the rationale appears in one test file only | not a finding |
| Test-isolation hazard from narrowing | `createScopedUser` drops-then-creates `SCOPED_USER`, a name unique to this class; teardown leaves it, exactly as before. Narrowing strictly reduces the authority the leaked principal carries | not a finding — a small improvement |

No follow-up issues filed: no coverage-table row is unaccounted for.
