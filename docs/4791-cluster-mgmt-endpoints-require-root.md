# Issue #4791 — Cluster-management HTTP endpoints require root, not just any authenticated user

## Problem

The five Raft cluster-management HTTP handlers authenticate the caller but only via
the base class, which guarantees *some* authenticated user, not root:

- `PostAddPeerHandler`  — `POST /api/v1/cluster/peer`
- `DeletePeerHandler`   — `DELETE /api/v1/cluster/peer/<id>`
- `PostTransferLeaderHandler` — `POST /api/v1/cluster/leader`
- `PostStepDownHandler` — `POST /api/v1/cluster/stepdown`
- `PostLeaveHandler`    — `POST /api/v1/cluster/leave`

None of them call `checkRootUser(user)`. Every other admin handler does
(`PutUserHandler`, `DeleteGroupHandler`, `PostServerCommandHandler`, and the
sibling raft handlers `PostResyncDatabaseHandler` / `PostVerifyDatabaseHandler`).

### Impact (privilege escalation)

Any authenticated, non-root tenant user (or any cluster-token holder on the
forwarded-auth path) can remove peers until quorum is lost, force permanent
election churn via repeated step-down, transfer leadership, or evict a node from
the cluster.

## Security boundary

`AbstractServerHttpHandler.checkRootUser(user)` throws `ServerSecurityException`
when `user.getName()` is not `"root"`. That exception is mapped to HTTP **403**
by the base handler. The fix is to invoke `checkRootUser(user)` as the **first**
statement of each handler's `execute(...)` so the root check runs before any
side effect (and before the `raftHAServer == null` short-circuit).

## Fix

Add `checkRootUser(user);` as the first line of `execute(...)` in all five
handlers. The resync/verify handlers already enforce it and are unchanged.

## Tests

`ClusterManagementAuthorizationIT` (ha-raft):
- A non-root (but otherwise admin) tenant user receives **403** on all five
  cluster-management endpoints. Because `checkRootUser` runs first, no cluster
  mutation occurs.
- The root user passes the root check (a malformed add-peer request returns
  **400 validation**, not 403), proving the fix does not over-block root.

The non-root assertions fail before the fix (the operation would otherwise run)
and pass after it.

## Verification

- `mvn -q -pl ha-raft -am test -Dtest=ClusterManagementAuthorizationIT`
