# Issue #5039 - gRPC admin RPCs authenticate but never authorize

## Symptom
The gRPC admin service (`ArcadeDbGrpcAdminService`) validated the caller's credentials but never
checked for an administrative (root) role. Any user with a valid server account could
`CreateDatabase` (resource abuse) or `DropDatabase` on any database (destructive privilege
escalation), even one they had no rights over.

## Root cause
`authenticate(DatabaseCredentials)` called `ServerSecurity.authenticate(user, pass, null)`, which
returns a valid `ServerSecurityUser` for any correct credential pair, then discarded the result.
`createDatabase` / `dropDatabase` proceeded to `createDatabasePhysical` / `dropDatabasePhysical`
with no role check. The central `GrpcAuthInterceptor` intentionally enforces authentication only
(not admin-role authorization), delegating authorization to the handlers.

## Fix
- `grpcw/.../ArcadeDbGrpcAdminService.java`:
  - `authenticate(...)` now returns the resolved `ServerSecurityUser`.
  - Added `requireServerAdmin(ServerSecurityUser)` which throws a dedicated
    `AdminAuthorizationException` unless the user is `root` (mirrors HTTP
    `PostServerCommandHandler` / `AbstractServerHttpHandler.checkRootUser`).
  - `createDatabase` and `dropDatabase` now call `requireServerAdmin(authenticate(...))`.
  - Both RPCs map `AdminAuthorizationException` to `Status.PERMISSION_DENIED` (distinct from the
    `UNAUTHENTICATED` used for authentication failures), fail-closed.
- `grpcw/.../GrpcAuthInterceptor.java`: updated the choke-point comment to state that mutating admin
  handlers now enforce role authorization.

Scope: only the mutating destructive RPCs (`createDatabase`, `dropDatabase`) are gated, matching the
acceptance criteria. Read-only info RPCs are left unchanged to avoid altering legitimate non-root
read behavior.

## Tests
`grpcw/src/test/java/com/arcadedb/server/grpc/Issue5039GrpcAdminAuthorizationIT.java`:
- `nonAdminCreateDatabaseIsDenied` - non-admin `CreateDatabase` -> `PERMISSION_DENIED`, db not created.
- `nonAdminDropDatabaseIsDenied` - non-admin `DropDatabase` on existing db -> `PERMISSION_DENIED`, db preserved.
- `adminCreateAndDropDatabaseStillSucceeds` - positive control, root still succeeds.

TDD confirmed: both denial tests fail before the fix, pass after. All existing gRPC admin/auth
tests (GrpcAdminServiceIT, GrpcAdminAuthInterceptorIT, Issue4794GrpcPerDbAuthorizationIT,
GrpcAuthInterceptorTest = 29 tests) still pass.

## Impact
Closes a critical privilege-escalation + data-destruction vulnerability on the gRPC admin plane.
No behavior change for root/admin callers.

## Pull request
https://github.com/ArcadeData/arcadedb/pull/5101

## Review cycles
- cycle 1: `7b904a0` - initial fix (role gate on createDatabase/dropDatabase, PERMISSION_DENIED).
  Gemini reviewed with one security-critical suggestion: fail closed if `ServerSecurity.authenticate`
  returns `null` (defense-in-depth; the current impl throws, but the null path would otherwise map to
  PERMISSION_DENIED instead of UNAUTHENTICATED and weaken the discarded-return RPCs). Claude bot did
  not respond within the 15-minute per-iteration timeout.
- cycle 2: `4f7ccae` - addressed Gemini: `authenticate(...)` now null-checks the returned user and
  throws `SecurityException` (-> UNAUTHENTICATED), hardening all admin RPCs. All grpcw admin/auth
  tests pass. Gemini re-posted the same (now already-applied) suggestion; no new actionable items.
  Claude bot again did not respond within the timeout.

## Final state
timeout - the Claude bot reviewer never responded within the per-iteration budget across both cycles;
Gemini's substantive feedback was fully addressed and the working tree is clean. Merge remains the
developer's responsibility.
