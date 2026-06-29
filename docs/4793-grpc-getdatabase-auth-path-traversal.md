# Issue #4793 - gRPC getDatabase: unauth DB create + path traversal

## Summary

`ArcadeDbGrpcService.getDatabase()` resolved a database from a request-supplied
`databaseName` after only checking that a username was *resolvable*
(`validateCredentials` never called `ServerSecurity.authenticate`). Two security
problems:

1. **Path traversal** - `databaseName` flowed unsanitized into
   `new DatabaseFactory(databasePath + "/" + databaseName)` (fallback path) and
   into `arcadeServer.getDatabase(databaseName)`. A name containing `..`, `/` or
   `\` escapes the configured databases directory.
2. **Weak authentication / missing authorization** - `validateCredentials` only
   required a non-empty username. It never authenticated the password nor
   authorized the user for the named database, so any path reaching the service
   could open/create databases.

## Root cause

- `validateCredentials(DatabaseCredentials)` only resolved a username and threw
  only when it was null. No call to `ServerSecurity.authenticate`, no
  per-database authorization.
- `getDatabase(String, DatabaseCredentials)` never validated the database name
  before touching the filesystem.

## Fix

`grpcw/.../ArcadeDbGrpcService.java`:

- New `validateDatabaseName(String)`: rejects null/blank names and any name
  containing `/`, `\` or `..` (mirrors the established pattern in
  `PostServerCommandHandler.resolveBackupFile` / `ServerQueryProfiler`). Called
  at the top of `getDatabase` before any filesystem access.
- `validateCredentials` now takes the database name and, when server security is
  active (at least one user configured), enforces real authentication +
  per-database authorization:
  - If the auth interceptor already set the connection's context user, authorize
    that user for the requested database.
  - Otherwise authenticate the request-payload username/password and authorize
    the requested database via `ServerSecurity.authenticate`.

## Tests

`grpcw/.../Issue4793GrpcGetDatabaseSecurityIT.java` (new):
- path-traversal database name (`../`, nested `/`, backslash) is rejected and no
  directory escapes the databases folder.
- legitimate database access by an authorized user still works (positive
  control / no regression).

## Verification (results)

- New IT `Issue4793GrpcGetDatabaseSecurityIT`: 3 of 5 tests FAIL before the fix
  (traversal names produced "...does not exist" instead of being rejected); all
  5 PASS after the fix.
- `grpcw` surefire unit tests (104, incl. `ArcadeDbGrpcServiceExtendedTest`
  authenticated happy path) and `GrpcAuthInterceptorTest` (8): pass.
- `grpcw` failsafe ITs `GrpcServerIT` + `GrpcFollowerForwardingIT` (31, incl.
  HA leader-forwarding using the interceptor context user): pass.
- Pre-existing unrelated failure: `grpc-client` `Issue4562RollbackDeleteTest`
  HTTP-path variants fail on `RemoteSchema.getType` for a gRPC-created type. Verified
  this fails identically on the unmodified base code, so it is not caused by this
  change (HTTP path is untouched).

## Impact

- Closes the path-traversal vector (request `databaseName` can no longer escape the
  databases directory) for every gRPC entry point that resolves a database.
- Enforces per-database authorization of the authenticated user and real
  authentication of request-payload credentials when server security is active.
- No behavioral change when security is disabled beyond the database-name sanitization.
