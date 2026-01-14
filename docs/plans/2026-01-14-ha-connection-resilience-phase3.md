# HA Phase 3: Connection Resilience Implementation

**Date:** 2026-01-14
**Previous Phase:** Phase 2 - Diagnostic Logging (21% pass rate)
**Target:** 50%+ pass rate
**Focus:** Connection timing, backoff/retry, graceful error handling

## Overview

Phase 3 Priority 1 addresses connection reliability issues that cause ~40% of test failures. The diagnostic logging from Phase 2 revealed that connection failures occur during:
1. Initial cluster formation (Connection refused)
2. Rapid server restarts (Connection reset, EOFException)
3. Network timing races (partial handshake completion)

## Root Causes Identified

From Phase 2 baseline analysis:
- **Connection refused:** Replicas connecting before leader is ready to accept connections
- **EOFException:** Connection closed mid-handshake due to timing
- **Connection reset:** Abrupt disconnections during rapid restart scenarios
- **No retry logic:** Single connection attempt without backoff/retry

## Tasks

### Task 1: Add Connection Retry with Exponential Backoff

**Objective:** Replace single connection attempt with retry logic using exponential backoff

**Files to Modify:**
- `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`

**Current Behavior:**
```java
connect() {
  try {
    channel = new Channel(...);
    channel.connect();
  } catch (ConnectionException e) {
    // Throws immediately, no retry
  }
}
```

**Target Behavior:**
```java
connect() {
  int maxAttempts = 5;
  long baseDelayMs = 100;

  for (int attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      channel = new Channel(...);
      channel.connect();
      return; // Success
    } catch (ConnectionException e) {
      if (attempt == maxAttempts) throw e;

      long delayMs = baseDelayMs * (1 << (attempt - 1)); // Exponential: 100, 200, 400, 800, 1600ms
      Thread.sleep(delayMs);
    }
  }
}
```

**Implementation Steps:**

1. Add configuration parameters to `GlobalConfiguration`:
   - `HA_REPLICA_CONNECT_RETRY_MAX_ATTEMPTS` (default: 5)
   - `HA_REPLICA_CONNECT_RETRY_BASE_DELAY_MS` (default: 100)
   - `HA_REPLICA_CONNECT_RETRY_MAX_DELAY_MS` (default: 5000)

2. Implement `connectWithRetry()` method in `Replica2LeaderNetworkExecutor`:
   - Use exponential backoff with jitter
   - Log each retry attempt with delay
   - Track total time spent in retries
   - Exit early if shutdown requested

3. Update `connect()` to use `connectWithRetry()`

4. Add metrics for connection retry statistics

**Verification:**
```bash
# Test that passes with retry but would fail without
mvn test -Dtest=ReplicationServerQuorumMajorityIT -pl server
```

### Task 2: Handle EOFException During Handshake

**Objective:** Gracefully handle connection closure during message exchange

**Files to Modify:**
- `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`
- `server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java`

**Current Behavior:**
```java
try {
  channel.readMessage();
} catch (EOFException e) {
  // Propagates as fatal error
  throw new ReplicationException(e);
}
```

**Target Behavior:**
```java
try {
  channel.readMessage();
} catch (EOFException e) {
  LogManager.log(FINE, "Connection closed during handshake, will retry");
  throw new ConnectionException("Handshake interrupted", e); // Triggers retry
}
```

**Implementation Steps:**

1. Create `HandshakeInterruptedException` exception class
2. Wrap EOFException in handshake methods
3. Ensure retry logic catches and retries HandshakeInterruptedException
4. Add handshake timeout to prevent hanging

**Verification:**
Test rapid server restart scenarios

### Task 3: Improve Leader Connection Acceptance

**Objective:** Ensure leader can accept connections even during startup

**Files to Modify:**
- `server/src/main/java/com/arcadedb/server/ha/LeaderNetworkListener.java`

**Current Behavior:**
Leader listener starts but may not be ready to process connections immediately

**Target Behavior:**
- Leader signals readiness before replicas attempt connection
- Replicas wait for leader ready signal
- Connection pool pre-warmed

**Implementation Steps:**

1. Add `isReadyToAcceptConnections()` check in `LeaderNetworkListener`
2. Signal readiness after initialization complete
3. Add connection queue for early arrivals
4. Process queued connections once ready

### Task 4: Add Connection Health Monitoring

**Objective:** Detect and recover from connection degradation

**Files to Modify:**
- `server/src/main/java/com/arcadedb/server/ha/Leader2ReplicaNetworkExecutor.java`

**Implementation Steps:**

1. Add periodic heartbeat messages (every 5 seconds)
2. Track heartbeat response latency
3. Mark connection as degraded if latency > threshold
4. Attempt reconnection if heartbeat fails
5. Log connection health metrics

### Task 5: Improve Test Connection Timing

**Objective:** Make tests more resilient to connection timing variations

**Files to Modify:**
- `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`

**Implementation Steps:**

1. Increase default timeouts in `waitForClusterStable()`:
   - All replicas connected: 30s → 60s
   - All servers ONLINE: 60s → 120s

2. Add exponential backoff in wait loops

3. Add detailed logging on timeout with cluster state dump

4. Implement `waitForLeaderReady()` helper method

**Verification:**
Run full test suite and measure improvement

## Expected Outcomes

**Test Improvements:**
- Connection refused errors: ~15 occurrences → ~2 occurrences
- EOFException errors: ~8 occurrences → ~1 occurrence
- Pass rate: 21% → 50%+

**New Passing Tests (Expected):**
- ReplicationServerQuorumMajorityIT
- ReplicationServerQuorumAllIT
- ReplicationChangeSchemaIT
- Several others currently failing on connection issues

## Configuration Additions

```java
// GlobalConfiguration additions
HA_REPLICA_CONNECT_RETRY_MAX_ATTEMPTS(5, "Max connection retry attempts"),
HA_REPLICA_CONNECT_RETRY_BASE_DELAY_MS(100, "Base delay between retries (ms)"),
HA_REPLICA_CONNECT_RETRY_MAX_DELAY_MS(5000, "Max delay between retries (ms)"),
HA_CONNECTION_HEALTH_CHECK_INTERVAL_MS(5000, "Heartbeat interval (ms)"),
HA_CONNECTION_HEALTH_CHECK_TIMEOUT_MS(15000, "Heartbeat timeout (ms)")
```

## Testing Strategy

1. **Unit Tests:** Test retry logic in isolation
2. **Integration Tests:** Run existing HA tests with new retry logic
3. **Stress Tests:** Rapid start/stop cycles
4. **Network Chaos:** Simulate connection drops, delays
5. **Full Suite:** Measure improvement in pass rate

## Commit Structure

Each task will have its own commit:
```
feat: add connection retry with exponential backoff
feat: handle EOFException during replica handshake
feat: improve leader connection acceptance
feat: add connection health monitoring
test: improve connection timing resilience
```

## Next Steps After Phase 3 Priority 1

If we achieve 50%+ pass rate:
- Move to Priority 2: Database Lifecycle Management (target 70%)
- Document remaining failures for priority assessment
- Update Phase 3 plan based on findings

## References

- [Phase 2 Baseline](../testing/ha-phase2-baseline.md)
- [Phase 3 Overview](./2026-01-14-ha-advanced-resilience-phase3-placeholder.md)
- Connection failure logs from Phase 2 testing
