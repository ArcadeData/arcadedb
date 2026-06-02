# Fix #4456 - Bolt follower forwarding: no authenticated user in security context

## Issue

Bolt protocol write queries (`CREATE`, `MERGE`, etc.) fail with
`Cannot forward command to leader: no authenticated user in the current security context`
when the connection lands on a Raft HA follower instead of the leader.

REST/HTTP is unaffected. Reported against an EKS deployment with the Helm chart.

## Root Cause

`BoltNetworkExecutor.ensureDatabase()` resolves the database via `server.getDatabase(targetName)`
but never calls `DatabaseContext.INSTANCE.init(...).setCurrentUser(...)`.

On a follower write, `RaftReplicatedDatabase.forwardCommandToLeaderViaRaft` reads
`proxied.getCurrentUserName()` (line 1523) to build the `X-ArcadeDB-Forwarded-User` header.
Because the current user was never bound to the thread-local `DatabaseContext`, this returns
`null` and a `SecurityException` is thrown (line 1526).

### Identical bug fixed previously in gRPC

`GrpcFollowerForwardingIT` tests the same fix for the gRPC module. Recorded guidance:
> "any new wire-protocol module (Bolt, MongoDB, Redis) MUST set current user on DatabaseContext
> after init or HA follower writes fail silently under load."

Bolt never received that treatment.

### Also affected: mongodbw and redisw

Both `mongodbw` and `redisw` also have zero `DatabaseContext`/`setCurrentUser` references.
Whether their write paths hit the same `.command()` forwarding code needs verification
during implementation; follow-up issues will be filed if their failure mode differs.

## Fix

In `BoltNetworkExecutor.ensureDatabase()`, after `database = server.getDatabase(targetName)`,
bind the already-authenticated `ServerSecurityUser user` onto the thread-local context:

```java
DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user.getDatabaseUser(database));
```

This mirrors `PostgresNetworkExecutor.openDatabase()` line 1509 and the gRPC fix.

## Files Changed

- `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` - bind user in `ensureDatabase()`
- `bolt/src/test/java/com/arcadedb/bolt/BoltFollowerForwardingIT.java` - new 3-node HA regression test
- `bolt/pom.xml` - add arcadedb-ha-raft test+test-jar deps (mirrors grpcw/pom.xml)

## Test Plan

1. `BoltFollowerForwardingIT`: 3-node Raft cluster, Neo4j driver writes to a follower,
   asserts write succeeds and replicates to all 3 nodes.
2. Existing Bolt tests: `mvn -pl bolt -am test` - must all pass.

## PR

https://github.com/ArcadeData/arcadedb/pull/4457

## Review Cycles

### Cycle 1 - SHA 6ea4b08fa

**Gemini (COMMENTED):** Flagged critical security vulnerability: LOGOFF/LOGON re-authentication on
the same connection changes `user` but `ensureDatabase()` returns early (database already open),
so the DatabaseContext is NOT updated with the new user - privilege escalation risk.

**Claude:** Did not review within 15-minute window.

**Changes applied:** In the early-return block of `ensureDatabase()`, use `getContextIfExists()`
to update the current user without disturbing any open transactions (calling `init()` again would
rollback transactions, so `getContextIfExists().setCurrentUser()` is the correct approach).

### Cycle 2 - SHA 0985a30bd (+ docs SHA a85a1cbf6)

**Gemini:** No new findings on the updated commit within the 15-minute window. Its original
inline security comment persists (GitHub re-points `commit_id` to the latest SHA as the line
still exists) but the issue is resolved by the cycle-1 change.

**claude-review CI check:** PASS (in this repo "claude" reviews via a GitHub Actions check named
`claude-review`, not as a PR review author - so the orchestrator's poll for a "claude" review
author never matches by design).

**CI status:** All checks pass except `Meterian client scan`, which fails on an UNRELATED
pre-existing tooling error - `Unable to generate lockfile in folder /github/workspace/e2e-python`
(Python uv lockfile generation). This PR changes only `bolt/` Java files and a test-scoped
`arcadedb-ha-raft` pom dependency; no Python dependencies are touched.

## Final State

- **timeout** on bot re-review (no new actionable feedback); the one substantive review item
  (Gemini's re-authentication security issue) was addressed in cycle 1.
- `claude-review` CI check: PASS. Java build + all e2e CI checks: PASS.
- Merge is the developer's responsibility.
