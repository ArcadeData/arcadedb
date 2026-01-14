# HA Production Hardening Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix replica connection failures by improving the replica status transition flow and adding diagnostic capabilities.

**Architecture:** The main issue is timing between cluster formation and replica ONLINE status. Full resync path requires `ReplicaReadyRequest` before setting ONLINE, but tests timeout waiting. We'll add diagnostic logging, validate the handshake flow, and ensure proper status transitions.

**Tech Stack:** Java 21, Maven, JUnit 5, Awaitility

---

## Background Investigation

### Root Cause Analysis

The `waitForClusterStable()` check fails because `getOnlineReplicas()` returns 0 even when:
1. Leader election succeeded
2. Cluster table shows replicas connected
3. Replication appears to be working

The issue is in the handshake flow:

```
Leader                                Replica
  |                                     |
  |<--- ReplicaConnectRequest ----------|
  |                                     |
  |--- ReplicaConnectFullResyncResponse->|  (status still JOINING)
  |                                     |
  |    [Replica processes full resync]  |
  |                                     |
  |<--- ReplicaReadyRequest ------------|  (only NOW set to ONLINE)
  |                                     |
```

For **hot resync**, the leader sets ONLINE immediately (line 330 in `Leader2ReplicaNetworkExecutor`).
For **full resync**, the leader waits for `ReplicaReadyRequest` (line 27 in `ReplicaReadyRequest.java`).

**Problem**: In tests with fresh databases, full resync is always required. If `ReplicaReadyRequest` is delayed or lost, replica never becomes ONLINE.

### Files Involved

- `HAServer.java` - `getOnlineReplicas()`, `setReplicaStatus()`
- `Leader2ReplicaNetworkExecutor.java` - STATUS enum, connection handling
- `Replica2LeaderNetworkExecutor.java` - `installDatabases()`, sends ReplicaReadyRequest
- `ReplicaConnectRequest.java` - Initial handshake
- `ReplicaConnectFullResyncResponse.java` - Full resync trigger
- `ReplicaReadyRequest.java` - Sets replica ONLINE
- `BaseGraphServerTest.java` - `waitForClusterStable()`, `areAllReplicasAreConnected()`

---

## Task 1: Add Diagnostic Logging to Replica Handshake

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java`
- Modify: `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`

**Step 1: Add logging to Leader2ReplicaNetworkExecutor when sending resync response**

In `Leader2ReplicaNetworkExecutor.java`, after line 328 (after sending response), add FINE-level logging:

```java
if (response instanceof ReplicaConnectHotResyncResponse resyncResponse) {
  LogManager.instance().log(this, Level.FINE,
      "Hot resync response sent to '%s', setting ONLINE immediately", remoteServer);
  server.resendMessagesToReplica(resyncResponse.getMessageNumber(), remoteServer);
  server.setReplicaStatus(remoteServer, true);
} else if (response instanceof ReplicaConnectFullResyncResponse) {
  LogManager.instance().log(this, Level.FINE,
      "Full resync response sent to '%s', waiting for ReplicaReadyRequest before ONLINE",
      remoteServer);
}
```

**Step 2: Run a simple test to verify logging works**

Run: `mvn test -Dtest=SimpleReplicationServerIT#testReplication -pl server 2>&1 | grep -i "resync\|ONLINE"`
Expected: See "Full resync response sent" messages in output

**Step 3: Add logging to Replica2LeaderNetworkExecutor when sending ReplicaReadyRequest**

In `Replica2LeaderNetworkExecutor.java`, in `installDatabases()` method, before sending `ReplicaReadyRequest`, add logging:

```java
LogManager.instance().log(this, Level.INFO,
    "Full resync complete, sending ReplicaReadyRequest to leader '%s'",
    getRemoteServerName());
```

**Step 4: Commit diagnostic logging**

```bash
git add server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java
git commit -m "$(cat <<'EOF'
fix: add diagnostic logging to HA handshake flow

- Log when hot resync vs full resync response is sent
- Log when ReplicaReadyRequest is sent after full resync
- Helps diagnose replica connection failures in tests

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Trace ReplicaReadyRequest Flow

**Files:**
- Read: `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`
- Read: `server/src/main/java/com/arcadedb/server/ha/message/ReplicaReadyRequest.java`

**Step 1: Find where ReplicaReadyRequest is sent in Replica2LeaderNetworkExecutor**

Search for `ReplicaReadyRequest` in `Replica2LeaderNetworkExecutor.java` and understand when it's called.

Run: `grep -n "ReplicaReadyRequest" server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`
Expected: Find the line where it's created and sent

**Step 2: Verify the installDatabases flow completes**

Search for the method that calls `ReplicaReadyRequest`:
- Should be at end of `installDatabases()` or similar
- Verify no exceptions can prevent it from being sent

**Step 3: Add logging to verify ReplicaReadyRequest.execute() is called on leader**

In `ReplicaReadyRequest.java`, add logging to the execute method:

```java
@Override
public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
  LogManager.instance().log(this, Level.INFO,
      "ReplicaReadyRequest received from '%s', setting replica ONLINE",
      remoteServerName.alias());
  server.setReplicaStatus(remoteServerName, true);
  return null;
}
```

**Step 4: Run test and verify ReplicaReadyRequest flow**

Run: `mvn test -Dtest=SimpleReplicationServerIT#testReplication -pl server 2>&1 | grep -i "ReplicaReadyRequest\|ONLINE"`
Expected: See "ReplicaReadyRequest received" for each replica

**Step 5: Commit tracing additions**

```bash
git add server/src/main/java/com/arcadedb/server/ha/message/ReplicaReadyRequest.java
git commit -m "$(cat <<'EOF'
fix: add logging to ReplicaReadyRequest execution

- Log when ReplicaReadyRequest is received on leader
- Confirms the full resync handshake completes
- Helps diagnose why replicas don't become ONLINE

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Identify Full Resync Completion Issue

**Files:**
- Read: `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`
- Modify: `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`

**Step 1: Find installDatabases method and trace to end**

Read the `installDatabases()` method and trace all code paths to understand when `ReplicaReadyRequest` is sent.

**Step 2: Check for exception handling that might swallow errors**

Look for `catch (Exception)` blocks that might prevent `ReplicaReadyRequest` from being sent.

**Step 3: Add try-finally to ensure ReplicaReadyRequest is always sent**

If `ReplicaReadyRequest` is only sent on success, consider if it should be sent in a `finally` block or after exception handling.

**Step 4: Run test to verify fix**

Run: `mvn test -Dtest=SimpleReplicationServerIT -pl server`
Expected: Tests pass more reliably

**Step 5: Commit fix if found**

```bash
git add server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java
git commit -m "$(cat <<'EOF'
fix: ensure ReplicaReadyRequest is sent after full resync

- [Description of the fix based on what was found]
- Prevents replicas from being stuck in JOINING state

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Improve Status Transition Visibility

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ha/HAServer.java`
- Modify: `server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java`

**Step 1: Add logging to setReplicaStatus**

In `HAServer.java`, update `setReplicaStatus()` to log state transitions:

```java
public void setReplicaStatus(final String remoteServerName, final boolean online) {
  final Leader2ReplicaNetworkExecutor c = replicaConnections.get(remoteServerName);
  if (c == null) {
    LogManager.instance().log(this, Level.SEVERE,
        "setReplicaStatus: Replica '%s' was not registered", remoteServerName);
    return;
  }

  final Leader2ReplicaNetworkExecutor.STATUS oldStatus = c.getStatus();
  c.setStatus(online ? Leader2ReplicaNetworkExecutor.STATUS.ONLINE : Leader2ReplicaNetworkExecutor.STATUS.OFFLINE);

  LogManager.instance().log(this, Level.INFO,
      "Replica '%s' status changed: %s -> %s (online replicas now: %d)",
      remoteServerName, oldStatus, c.getStatus(), getOnlineReplicas());
  // ... rest of method
}
```

**Step 2: Run test and verify status transitions are logged**

Run: `mvn test -Dtest=SimpleReplicationServerIT#testReplication -pl server 2>&1 | grep "status changed"`
Expected: See status transitions for each replica

**Step 3: Add diagnostic method to print replica status summary**

Add method to HAServer for debugging:

```java
public void logReplicaStatusSummary() {
  LogManager.instance().log(this, Level.INFO,
      "=== Replica Status Summary (total: %d, online: %d) ===",
      replicaConnections.size(), getOnlineReplicas());

  for (Map.Entry<String, Leader2ReplicaNetworkExecutor> entry : replicaConnections.entrySet()) {
    LogManager.instance().log(this, Level.INFO,
        "  %s: %s", entry.getKey(), entry.getValue().getStatus());
  }
}
```

**Step 4: Call diagnostic method in test helpers**

In `BaseGraphServerTest.waitForClusterStable()`, call `logReplicaStatusSummary()` on timeout for debugging.

**Step 5: Commit visibility improvements**

```bash
git add server/src/main/java/com/arcadedb/server/ha/HAServer.java server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java
git commit -m "$(cat <<'EOF'
fix: improve replica status transition visibility

- Log status changes in setReplicaStatus
- Add logReplicaStatusSummary for debugging
- Call summary on waitForClusterStable timeout
- Helps diagnose cluster formation issues

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Run Baseline Tests and Document Results

**Files:**
- Create: `docs/testing/ha-phase2-baseline.md`

**Step 1: Run full HA test suite with verbose output**

```bash
cd /Users/frank/projects/arcade/arcadedb
mvn test -Dtest="*HA*IT,*Replication*IT" -pl server 2>&1 | tee /tmp/ha-test-output.txt
```

**Step 2: Count passes and failures**

```bash
grep -E "Tests run:|Failures:|Errors:" /tmp/ha-test-output.txt | tail -20
```

**Step 3: Analyze failure patterns**

```bash
grep -B5 "FAILURE\|ERROR" /tmp/ha-test-output.txt | head -100
```

**Step 4: Create baseline document**

Create `docs/testing/ha-phase2-baseline.md` with:
- Test counts (pass/fail/error)
- Common failure patterns
- Comparison to Phase 1 baseline
- Next steps based on findings

**Step 5: Commit baseline document**

```bash
git add docs/testing/ha-phase2-baseline.md
git commit -m "$(cat <<'EOF'
docs: add Phase 2 HA test baseline

- Document test results after diagnostic logging
- Identify remaining failure patterns
- Track improvement from Phase 1

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Fix Identified Issue (Based on Task 2-3 Findings)

This task will be refined after Task 2-3 completes and identifies the specific issue.

**Expected scenarios:**

**Scenario A: ReplicaReadyRequest never sent**
- Fix: Ensure it's sent even if database install has errors

**Scenario B: ReplicaReadyRequest sent but not received**
- Fix: Check network executor message handling, ensure response is processed

**Scenario C: ReplicaReadyRequest received but setReplicaStatus fails**
- Fix: Check replica registration timing, ensure replica is in replicaConnections map

**Files:**
- Modify: Based on findings from Task 2-3
- Test: `server/src/test/java/com/arcadedb/server/ha/SimpleReplicationServerIT.java`

**Step 1: Implement fix based on findings**

[To be determined after Task 2-3]

**Step 2: Write test to verify fix**

Add test case that specifically verifies the scenario:

```java
@Test
@Timeout(value = 2, unit = TimeUnit.MINUTES)
void testReplicaBecomesOnlineAfterFullResync() {
  // Start fresh cluster (forces full resync)
  waitForClusterStable();

  // Verify all replicas are ONLINE
  ArcadeDBServer leader = getLeader();
  int onlineReplicas = leader.getHA().getOnlineReplicas();

  assertThat(onlineReplicas)
      .as("Expected %d replicas to be ONLINE", getServerCount() - 1)
      .isEqualTo(getServerCount() - 1);
}
```

**Step 3: Run test multiple times**

```bash
for i in {1..10}; do mvn test -Dtest=SimpleReplicationServerIT#testReplicaBecomesOnlineAfterFullResync -pl server; done
```

**Step 4: Commit fix**

```bash
git add [modified files]
git commit -m "$(cat <<'EOF'
fix: [description of fix]

[Explanation of the issue and solution]

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Add State Machine to Network Executors (Optional Enhancement)

This task implements the enhanced state machine from the design document, if diagnostic tasks reveal state transition issues.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java`

**Step 1: Enhance STATUS enum with transition validation**

```java
public enum STATUS {
  JOINING,      // Initial connection
  ONLINE,       // Healthy, processing messages
  RECONNECTING, // Connection lost, attempting recovery
  DRAINING,     // Shutdown requested
  FAILED;       // Unrecoverable error

  private static final Map<STATUS, Set<STATUS>> VALID_TRANSITIONS = Map.of(
      JOINING, Set.of(ONLINE, FAILED, DRAINING),
      ONLINE, Set.of(RECONNECTING, DRAINING, OFFLINE),
      RECONNECTING, Set.of(ONLINE, FAILED, DRAINING),
      DRAINING, Set.of(FAILED),
      FAILED, Set.of()
  );

  public boolean canTransitionTo(STATUS newStatus) {
    return VALID_TRANSITIONS.getOrDefault(this, Set.of()).contains(newStatus);
  }
}
```

**Step 2: Update setStatus to validate transitions**

```java
public void setStatus(final STATUS newStatus) {
  synchronized (this) {
    if (!status.canTransitionTo(newStatus)) {
      LogManager.instance().log(this, Level.WARNING,
          "Invalid state transition: %s -> %s for replica '%s'",
          status, newStatus, remoteServer);
      // Allow anyway for backward compatibility, but log
    }

    STATUS oldStatus = this.status;
    this.status = newStatus;

    LogManager.instance().log(this, Level.FINE,
        "Replica '%s' state: %s -> %s", remoteServer, oldStatus, newStatus);
  }
}
```

**Step 3: Run tests to verify no regressions**

```bash
mvn test -Dtest="*HA*IT" -pl server
```

**Step 4: Commit state machine enhancement**

```bash
git add server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java
git commit -m "$(cat <<'EOF'
feat: add state transition validation to replica executor

- Define valid state transitions
- Log invalid transitions for debugging
- Backward compatible (allows invalid transitions with warning)

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Create Cluster Health Diagnostic Command

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/GetClusterHealthHandler.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/HttpServer.java`

**Step 1: Create ClusterHealth response class**

Create a simple record for health response:

```java
public record ClusterHealthResponse(
    String serverName,
    String role,
    int configuredServers,
    int onlineReplicas,
    boolean quorumAvailable,
    Map<String, String> replicaStatuses
) {}
```

**Step 2: Create health endpoint handler**

```java
public class GetClusterHealthHandler extends AbstractServerHttpHandler {
  @Override
  public ExecutionResponse execute(...) {
    HAServer ha = server.getHA();
    if (ha == null) {
      return response(404, "HA not enabled");
    }

    Map<String, String> replicaStatuses = new HashMap<>();
    // Get status of each replica

    ClusterHealthResponse health = new ClusterHealthResponse(
        server.getServerName(),
        ha.isLeader() ? "Leader" : "Replica",
        ha.getConfiguredServers(),
        ha.getOnlineReplicas(),
        ha.isQuorumAvailable(),
        replicaStatuses
    );

    return response(200, health);
  }
}
```

**Step 3: Register handler in HttpServer**

Add route for `/api/v1/cluster/health`

**Step 4: Write test for health endpoint**

```java
@Test
void testClusterHealthEndpoint() throws Exception {
  waitForClusterStable();

  String response = fetchUrl(getLeaderHttpAddress() + "/api/v1/cluster/health");
  JSONObject health = new JSONObject(response);

  assertThat(health.getString("role")).isEqualTo("Leader");
  assertThat(health.getInt("onlineReplicas")).isEqualTo(getServerCount() - 1);
}
```

**Step 5: Commit health endpoint**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/GetClusterHealthHandler.java server/src/main/java/com/arcadedb/server/http/HttpServer.java
git commit -m "$(cat <<'EOF'
feat: add cluster health diagnostic endpoint

- GET /api/v1/cluster/health returns cluster status
- Shows leader/replica role, online replicas, quorum status
- Useful for debugging and monitoring

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Run Full Validation Suite

**Files:**
- Modify: `docs/testing/ha-phase2-baseline.md`

**Step 1: Run full test suite multiple times**

```bash
for i in {1..5}; do
  echo "=== Run $i ==="
  mvn test -Dtest="*HA*IT,*Replication*IT" -pl server 2>&1 | grep -E "Tests run:|BUILD"
done
```

**Step 2: Compare results to Phase 1 baseline**

Calculate improvement in pass rate.

**Step 3: Document any remaining issues**

List tests that still fail and their failure patterns.

**Step 4: Update baseline document with final results**

**Step 5: Commit final documentation**

```bash
git add docs/testing/ha-phase2-baseline.md
git commit -m "$(cat <<'EOF'
docs: update Phase 2 baseline with final results

- Document test pass rate improvement
- List remaining issues for Phase 3
- Track progress against reliability targets

Part of Phase 2: HA Production Hardening

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Create Phase 3 Planning Document

**Files:**
- Create: `docs/plans/2026-01-14-ha-advanced-resilience-phase3-placeholder.md`

**Step 1: Document Phase 2 completion status**

Summarize what was completed and what remains.

**Step 2: Identify Phase 3 priorities based on findings**

From the design document, Phase 3 includes:
- Circuit breaker for slow replicas
- Background consistency monitor
- Enhanced observability

Prioritize based on remaining test failures.

**Step 3: Create placeholder with high-level tasks**

**Step 4: Commit Phase 3 placeholder**

```bash
git add docs/plans/2026-01-14-ha-advanced-resilience-phase3-placeholder.md
git commit -m "$(cat <<'EOF'
docs: add Phase 3 planning placeholder

- Document Phase 2 completion status
- Outline Phase 3 priorities
- Provide roadmap for advanced resilience features

Part of Phase 2: HA Production Hardening completion

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Summary

**Total Tasks:** 10

**Core diagnostic tasks (1-4):** Add logging and tracing to understand the replica handshake flow
**Analysis tasks (5):** Document baseline and identify patterns
**Fix task (6):** Implement fix based on diagnostic findings
**Enhancement tasks (7-8):** Add state machine validation and health endpoint
**Validation tasks (9-10):** Run tests and document results

**Expected Outcome:**
- Clear visibility into replica status transitions
- Identification of root cause for "all replicas connected" failures
- Fix implemented and validated
- Test pass rate improved from 25% toward 95% target
