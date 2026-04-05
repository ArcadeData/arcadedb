# Replica→Leader Auth Forwarding Design

**Date:** 2026-02-23
**Branch:** ha-redesign
**Status:** Approved

## Problem

When a client (including the Studio UI) authenticates against a **replica** node using session-based auth (Bearer `AU-...` token), the replica's `PostCommandHandler` correctly detects it is not the leader and forwards the command to the leader's HTTP endpoint. However, the forwarded request carries the original `Authorization: Bearer AU-<uuid>` header. The session token is stored only in the **replica's** `HttpAuthSessionManager` — the leader has never seen it and returns 401.

Basic auth and API tokens (`Bearer at-...`) are stateless and work across nodes without this problem. Only session tokens (`AU-...`) are affected.

## Solution: Cluster-Internal Trust Token (Approach A)

All cluster nodes share a secret `HA_CLUSTER_TOKEN`. When the replica forwards a command to the leader, it replaces session-token auth with two internal headers identifying the user and proving cluster membership. The leader validates the token and resolves the user directly, bypassing session lookup.

## Design

### 1. Configuration

New entry in `GlobalConfiguration`:

```java
HA_CLUSTER_TOKEN("arcadedb.ha.clusterToken", SCOPE.SERVER,
    "Shared secret for inter-node request forwarding. Must be identical on all cluster nodes. " +
    "If empty, a random token is auto-generated and stored in raft-storage at startup.",
    String.class, "")
```

If blank at `RaftHAServer.start()`, a UUID is generated and written to `<raft-storage-dir>/cluster-token.txt`. Subsequent restarts read the file so the token is stable. Operators may also set the token explicitly in config (must be identical on all nodes).

The token is never logged at INFO level.

### 2. Header Names

| Header | Value |
|--------|-------|
| `X-ArcadeDB-Forwarded-User` | Username resolved from the local session |
| `X-ArcadeDB-Cluster-Token` | Shared cluster secret |

### 3. Forwarding Logic (`PostCommandHandler`)

`forwardToLeader()` gains the resolved `ServerSecurityUser` as a parameter.

```
if Authorization header starts with "Bearer AU-":
    add X-ArcadeDB-Forwarded-User: <username>
    add X-ArcadeDB-Cluster-Token:  <clusterToken>
    omit original Authorization header
else:
    forward original Authorization header unchanged
    (Basic and API token auth are stateless; the leader validates them normally)
```

### 4. Leader Authentication (`AbstractServerHttpHandler`)

Before the existing auth block, check for internal headers:

```
if X-ArcadeDB-Cluster-Token header present:
    if token != configured HA_CLUSTER_TOKEN → return 401
    resolve user by name from server security
    if user not found → return 401
    use resolved user as authenticated principal
    skip normal auth entirely
```

Both internal headers must be present for this path to activate. A request with only one header falls through to normal auth (and will fail if the session token is unknown).

### 5. Error Handling

- Token mismatch (misconfigured cluster): leader returns 401; replica passes it to the client.
- Valid token, unknown username: leader returns 401.
- Leader unreachable: existing `IOException` forwarding error path, unchanged.

### 6. Security Considerations

- The cluster token is a shared secret; nodes should communicate on a trusted network (same LAN/VPC). Raft gRPC is also unencrypted in the current design, so this is consistent.
- External clients cannot forge the internal headers without knowing the token.
- The internal auth path only activates when both headers are present with a valid token.

### 7. Tests

| Test | What it verifies |
|------|-----------------|
| `PostCommandHandler` — session token forwarding | `AU-...` auth is replaced with internal headers; original `Authorization` is dropped |
| `PostCommandHandler` — Basic/API token forwarding | Original `Authorization` header forwarded unchanged |
| `AbstractServerHttpHandler` — valid cluster token + known user | Auth succeeds, correct user resolved |
| `AbstractServerHttpHandler` — invalid cluster token | Returns 401 |
| `AbstractServerHttpHandler` — valid token + unknown username | Returns 401 |
| `RaftHAServer` — token auto-generation | UUID written to file on first call; same value returned on second call |

## Files to Change

| File | Change |
|------|--------|
| `engine/.../GlobalConfiguration.java` | Add `HA_CLUSTER_TOKEN` |
| `ha-raft/.../RaftHAServer.java` | Auto-generate and persist cluster token at `start()` |
| `server/.../handler/PostCommandHandler.java` | Replace session-token auth with internal headers when forwarding |
| `server/.../handler/AbstractServerHttpHandler.java` | Validate internal headers before normal auth |
| `ha-raft/.../RaftHAServerTest.java` | Token auto-generation tests |
| `server/.../handler/PostCommandHandlerTest.java` | Forwarding auth header tests |
| `server/.../handler/AbstractServerHttpHandlerTest.java` | Internal auth path tests |
