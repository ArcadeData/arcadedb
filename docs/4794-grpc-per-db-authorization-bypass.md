# Issue #4794 - gRPC per-DB authorization bypass

URL: https://github.com/ArcadeData/arcadedb/issues/4794
Type: bug (security, HIGH)

## Summary

`GrpcAuthInterceptor` authenticates the caller against the database named in the
`x-arcade-database` metadata header (defaulting to `"default"` when absent), but the
service methods resolve the database they actually operate on from the request body
(`req.getDatabase()` -> `ArcadeDbGrpcService.getDatabase(...)`). The two are never
reconciled, so an authenticated user can present a header naming a database they may
access (or omit it entirely) and then read/write a *different* database named in the
request body. This is a per-database access-control bypass.

## Security boundary

- Authentication (who are you + correct password): handled by `GrpcAuthInterceptor`
  via `ServerSecurity.authenticate(user, pass, headerDb)`.
- Authorization (may this user touch *this* database): must be enforced against the
  database the operation actually uses, i.e. the request-body database resolved in
  `ArcadeDbGrpcService.getDatabase(databaseName, credentials)`.

The bug is that authorization was bound to the header database, not the body database.

## Root cause

`ArcadeDbGrpcService.getDatabase(databaseName, credentials)` opened/returned the body
database and set the current security user without ever verifying the authenticated
user is authorized for `databaseName`.

## Fix

The authorization gap is already closed by `validateCredentials(credentials, databaseName)`,
which `getDatabase(...)` invokes as its first step (introduced by #4815). That method
authorizes the resolved user against the request-body `databaseName`:

- interceptor-context users are checked against `getAuthorizedDatabases()`;
- request-payload credentials go through `ServerSecurity.authenticate(user, pass, databaseName)`,

both throwing `PERMISSION_DENIED` ("User has not access to database '<db>'") when the user
may not access the body database. Because this runs before any database work, the bypass is
closed at the single chokepoint every RPC funnels through, regardless of what the header
claimed. No additional gate is required; this issue contributes the cross-database
regression coverage below.

## Tests

- `Issue4794GrpcPerDbAuthorizationIT` (grpcw): a user authorized only for a restricted
  database authenticates with a header naming that allowed database, then issues a
  query/command whose body targets a different database the user cannot access. Expect
  `PERMISSION_DENIED`. A positive-control test confirms the same user can still operate
  on the database it is authorized for.
