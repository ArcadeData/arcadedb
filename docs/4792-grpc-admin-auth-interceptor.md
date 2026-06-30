# Issue #4792 - gRPC auth interceptor blanket-skips the entire Admin service

## Problem

`GrpcAuthInterceptor.interceptCall` returned `next.startCall(...)` for any method
whose name starts with `com.arcadedb.grpc.ArcadeDbAdminService/`, on the premise
that each admin method "handles its own authentication via request body".

Consequences:
- No central choke point: authorization hinged on every admin RPC remembering to
  call `authenticate(req.getCredentials())`.
- The interceptor's `securityEnabled` gate never applied to admin methods at all.
- Any admin method added in the future without a body-auth call would be silently,
  fully open at the transport layer.

## Root cause

The blanket `startsWith("com.arcadedb.grpc.ArcadeDbAdminService/")` early-return in
the interceptor bypassed all authentication for the whole admin service.

## Fix

Replace the blanket skip with central enforcement. When security is enabled, the
interceptor now wraps the admin call listener and, on the inbound request message,
extracts the `DatabaseCredentials` from the message's `credentials` field (present
on every admin request, field #1) and validates it against `ServerSecurity` before
the request is dispatched to the handler. A missing `credentials` field, missing or
blank username, or failed validation closes the call with `UNAUTHENTICATED` and the
request never reaches the handler (fail closed).

This is defense-in-depth: the per-method `authenticate()` calls remain, but a
forgotten call can no longer open a hole because the interceptor is now the central
choke point. When security is disabled (no users configured), admin methods remain
reachable, matching the behavior for all other gRPC methods.

## Tests

`GrpcAdminAuthInterceptorIT` (grpcw): mock-free unit test that drives
`GrpcAuthInterceptor.interceptCall` for an admin method with a real `ServerSecurity`
obtained from a running server and hand-written `ServerCall`/`ServerCallHandler`
test doubles. Five cases:
- invalid credentials: call closed UNAUTHENTICATED, request NOT delivered to handler
  (fails before the fix because the blanket skip delivered the message)
- missing credentials: call closed UNAUTHENTICATED, request NOT delivered
  (fails before the fix)
- blank username: call closed UNAUTHENTICATED, request NOT delivered (fail closed)
- security disabled: request passes through to the handler
- valid credentials: request delivered to handler

Existing `GrpcAdminServiceIT` end-to-end tests continue to pass (clients send body
credentials, which the central check accepts).

## Verification results

- `GrpcAdminAuthInterceptorIT`: 5/5 pass after fix. Before the fix the invalid- and
  missing-credentials tests failed (the request reached the handler), proving the
  regression is real.
- `GrpcAdminServiceIT` (existing, 12 tests): pass, no regression on the real admin path.
- `GrpcServerIT` (existing data-plane path, 30 tests): 29 pass; the single error was an
  environmental `Address already in use` on the fixed gRPC port 50051 caused by a
  concurrent test run, not by this change.

## Impact

- Admin RPCs are now authenticated at a single transport-level choke point. A future
  admin method that forgets its own `authenticate()` call can no longer be invoked
  unauthenticated.
- Scope note: this enforces *authentication* (valid credentials), not admin-role
  *authorization*. As today, any user that authenticates can pass the check; the
  handlers do not require a server-root role. Adding an admin-role authorization gate
  is a worthwhile follow-up but is out of scope for this fix (pre-existing behavior).
- No behavior change for authenticated clients (valid body credentials still pass).
- No data-plane change; only the admin-service branch of the interceptor was modified.
