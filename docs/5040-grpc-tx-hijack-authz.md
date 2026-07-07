# Issue #5040 - gRPC transaction hijack: predictable/colliding tx IDs + tx-scoped RPCs skip authorization

## Symptom
Server-side gRPC transactions use a predictable, monotonic id (`"tx_" + System.nanoTime()`) and every
transaction-scoped RPC acts on the transaction found by that id without re-checking per-database
authorization or transaction ownership. An authenticated user can hijack another user's in-flight
transaction on a database they have no rights to. The weak id scheme also allows silent id collisions
between two concurrent `beginTransaction` calls (`activeTransactions.put` overwrites).

## Root cause
1. `generateTransactionId()` returns `"tx_" + System.nanoTime()` (low entropy, enumerable, collidable).
2. The transaction-scoped handlers resolve `TransactionContext` by id and run against `txCtx.db`
   directly, skipping the `getDatabase()` -> `validateCredentials()` authn/per-db-authz gate used on the
   non-transactional path. The context is not bound to the authenticating principal.
3. `activeTransactions.put(id, ctx)` silently overwrites on id collision.

## Fix (grpcw `ArcadeDbGrpcService`)
- `generateTransactionId()` now returns `"tx_" + UUID.randomUUID()` (CSPRNG-backed, collision-free).
- `TransactionContext` gains an immutable `owner` field, bound to the authenticated principal at
  `beginTransaction` (`resolvedUsername(credentials)`).
- `beginTransaction` registers with `putIfAbsent`; a non-null return (impossible with UUIDs) is treated
  as an internal error, rolling back the just-started transaction instead of silently overwriting.
- New `authorizeTransactionAccess(txCtx, credentials)` re-runs `validateCredentials` against the
  transaction's REAL database (`txCtx.db.getName()`, never a request-supplied name) and enforces that
  the caller is the transaction owner; otherwise `PERMISSION_DENIED`.
- New `resolveAuthorizedTransaction(txId, credentials)` = lookup + authorize; used by `executeCommand`,
  `createRecord`, `lookupByRid`, `updateRecord`, `deleteRecord`, `executeQuery`.
- `commitTransaction` / `rollbackTransaction` peek (not remove) first, authorize, then atomically
  `remove(key, value)` so the double-commit race protection and the idempotent no-op for unknown ids are
  preserved and a denied caller cannot evict the real owner's transaction.

## Tests
- New IT `Issue5040GrpcTransactionHijackIT`: cross-tenant commit/rollback/executeCommand denied; an
  authorized-but-non-owner user denied ("owned by another user"); owner still succeeds; tx ids are
  `tx_<uuid>` and unique.

## Impact
Closes the cross-tenant hijack and the id-collision data-integrity race on the gRPC transaction path.
No change to the non-transactional path or to public API. Fail-closed: unknown owner (open/no-security
mode) leaves ownership unenforced, matching the interceptor's existing `securityEnabled` gate.
