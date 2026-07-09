# Issue #5023 - X-Request-Id idempotency cache: wrong-response replay, skipped writes, unbounded growth

## Symptom
The HTTP idempotency cache keys solely on the raw client `X-Request-Id` header (also reused as the
log-correlation id). Clients/proxies that propagate one id across a logical operation get:
- wrong-response replay / silently skipped writes (different method/path/db/body, same id);
- no in-flight dedup (concurrent retries both execute -> duplicate write);
- caching of work done inside an open client session transaction;
- `/begin` replay that drops the `arcadedb-session-id` header.
Plus O(n) per-insert eviction scan and an entry-count-only bound (unbounded bytes).

## Root cause
- `AbstractServerHttpHandler` keyed the cache on the raw request id alone.
- `IdempotencyCache.evictOldest` did a full-map scan on every overflow insert; bound was entry count only.

## Fix
- Key idempotency on a SHA-256 of `requestId \0 method \0 relativePath \0 database \0 body`.
- Skip idempotency when the request carries an incoming `arcadedb-session-id` (session-scoped work),
  and skip caching when the response sets a session header (e.g. `/begin`).
- Add reserve / complete / abort with a PENDING marker so concurrent identical retries execute once.
- Replace the O(n) eviction scan with O(1) FIFO eviction and add a total-byte bound plus a per-body
  size cap (skip caching oversized bodies). New config keys with backward-compatible defaults.
- Keep the existing periodic TTL sweeper untouched.

## Tests
- `IdempotencyCacheTest` additions: reserve/complete/abort contract, in-flight dedup, byte bound,
  per-body cap, O(1) eviction correctness.
- `IdempotencyKeyTest`: composite key distinguishes method/path/db/body; stable for identical inputs.

## Impact
`server` module only. Backward-compatible constructor and get/putSuccess retained. Two new
`SCOPE.SERVER` config keys (`arcadedb.ha.idempotencyCacheMaxBytes` default 64 MB,
`arcadedb.ha.idempotencyCacheMaxBodyBytes` default 1 MB). The cache is now guarded by a monitor
(one lock per idempotent POST); non-idempotent requests never touch it.

## Verification
- `IdempotencyCacheTest` (15), `IdempotencyKeyTest` (7), `Issue5023IdempotencyKeyReplayTest` (2) pass;
  reproduction test run 4x for stability.
- Regression batch green: AutoCommitParameterTest, RequestIdSanitizationTest, CorrelationIdGenerationTest,
  AbstractServerHttpHandlerErrorBodyTest/TokenTest, ExecutionResponseTest, Issue5037BeginHandlerIT,
  HTTPAuthSessionIT, HTTPTransactionIT, Issue4141SessionManagementIT, HttpObservationIT.
