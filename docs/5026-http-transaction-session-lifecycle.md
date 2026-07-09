# Issue #5026 - HTTP transaction session lifecycle

## Symptom
Three related defects in server-side HTTP transaction-session lifecycle:
1. REST `/commit` and `/rollback` never remove the server-side `HttpSession`; a stale
   session id keeps resolving, so follow-up writes silently auto-commit and a retried
   `/commit`/`/rollback` returns HTTP 500.
2. Idle-timeout sweep vs in-flight request race: the sweep can roll back and remove a
   session between `getSessionById()` and `HttpSession.execute()` acquiring the session
   lock, leaking an active transaction.
3. Session ownership is checked by user name only, and after attach: `getSessionById`
   ignores the user; a dropped-and-recreated same-name principal can adopt the prior
   principal's still-open session.

## Root cause
- `PostCommitHandler`/`PostRollbackHandler` only strip the response header; `removeSession`
  is only called by GraphQL `SESSION CLOSE`.
- `HttpSession.execute()` re-validates nothing after acquiring the lock.
- `HttpSessionManager.getSessionById` ignores the `user` param; `ServerSecurityUser.equals`
  compares by name; user drop/password change does not invalidate live HTTP sessions.

## Fix
- `PostCommitHandler`/`PostRollbackHandler`: commit/rollback only when a tx is active,
  then `removeSession(header id)`. Idempotent (retry -> 204).
- `DatabaseAbstractHandler.setTransactionInThreadLocal`: an unresolved session id on a
  write-capable handler (`requiresTransaction()`) throws an explicit
  `HttpSessionException` (HTTP 404) instead of silently auto-committing; on read/commit/
  rollback/begin handlers it degrades to a session-less request (keeps idempotency and
  read-after-commit working).
- `HttpSession.execute()`: re-validate the session is still registered under the lock
  before running the callback; refuse (throw) otherwise so a swept transaction is never
  operated on.
- `HttpSessionManager.getSessionById`: enforce ownership (return null for a non-owner).
  Add `removeSessionsForUser(name)`; `ServerSecurity.dropUser`/`setUserPassword`/
  `updateUser` invalidate the principal's live HTTP sessions.

## Tests
- `Issue5026HttpSessionLifecycleTest` (unit): execute re-validation, ownership,
  removeSessionsForUser, recreated-same-name adoption.
- `Issue5026TransactionSessionLifecycleIT` (HTTP): commit/rollback remove the session,
  stale-session follow-up write is rejected, retried commit/rollback idempotent,
  recreated-same-name user cannot adopt.

## Behavior changes (for the changelog)
- A stale/unresolved session id now surfaces differently by handler kind. Write-capable
  handlers (e.g. `POST /command`) return an explicit **404** (previously the code intended
  401 but fell through to 500) instead of silently auto-committing. Read handlers
  (`GET /query`) and the transaction endpoints (`/begin`, `/commit`, `/rollback`) degrade
  to a session-less request: a read returns 200 against committed data (read-after-commit
  preserved) and a retried commit/rollback is an idempotent 204. A client that passed an
  expired session id to a read no longer learns it expired - the read simply runs outside
  the session.
- Dropping a user or changing its password rolls back and removes that principal's live HTTP
  transaction sessions. A plain metadata/grant update (`updateUser`) does NOT - per-request
  authorization is still re-checked on every command.

## Impact
Server module only. No wire-format change. Existing session-mgmt (#4141) semantics preserved.
