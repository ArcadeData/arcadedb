# HA System Reliability Improvements - Design Document

**Date:** 2026-01-13
**Author:** System Architecture Team
**Status:** Proposed
**Target Release:** TBD

## Executive Summary

This document outlines a comprehensive, phased approach to improving the reliability and stability of ArcadeDB's High Availability (HA) system. The improvements address both test infrastructure and production code, focusing on eliminating race conditions, improving observability, and strengthening failure recovery mechanisms.

### Key Outcomes

- **Test Reliability:** Increase HA test pass rate from ~85% to 99%
- **Production Stability:** Eliminate split-brain scenarios and improve leader election success rate to 99.9%
- **Developer Experience:** Reduce test flakiness and improve debugging capabilities
- **Operational Excellence:** Add real-time cluster health monitoring and automated consistency repair

### Implementation Timeline

- **Phase 1:** Test Infrastructure (Weeks 1-2) - High impact, low effort
- **Phase 2:** Production Hardening (Weeks 3-5) - High impact, medium effort
- **Phase 3:** Advanced Resilience (Weeks 6-8) - Medium impact, medium effort

---

## 1. Problem Statement

### 1.1 Current State Analysis

Through comprehensive analysis of the HA codebase, test suite, and existing analysis documents, we identified three main categories of reliability issues:

#### Test Infrastructure Issues

**Timing Anti-Patterns:**
- ~18 instances of `Thread.sleep()` and `CodeUtils.sleep()` should be replaced with Awaitility-based condition waits
- Tests race ahead before cluster state is stable, leading to false failures
- No explicit timeout protection allows tests to hang indefinitely

**Race Conditions:**
- Server lifecycle transitions (startup/shutdown) proceed before full initialization
- Tests don't consistently wait for: (a) all servers ONLINE, (b) replication queues empty, (c) cluster fully connected
- Missing `@Timeout` annotations on most tests

**Example Problem:**
```java
// Current problematic pattern
getServer(serverId).stop();
Thread.sleep(5000);  // Hope it's shut down by now
getServer(serverId).start();
// Start test assertions - server may not be ready!
```

#### Production Code Issues

**Incomplete Server Identity Migration:**
- Transitioning from string-based to `ServerInfo`-based server identification
- Migration incomplete, causing alias resolution bugs in Docker/K8s environments
- Multiple servers reporting "localhost" cannot be distinguished

**Synchronization Gaps:**
- Network executors have complex threading with potential race conditions during shutdown/reconnection
- Generic `catch (Exception)` blocks mask specific failure modes
- Channel/connection cleanup during abnormal shutdown needs hardening

**Limited Observability:**
- No real-time cluster health API
- Tests and operators lack "is cluster stable?" query capability
- Difficult to distinguish transient from persistent failures

#### Architecture Strengths (to preserve)

- Excellent timeout constants in `HATestTimeouts`
- Good Awaitility patterns where implemented (e.g., `HARandomCrashIT`)
- Solid exponential backoff and retry strategies
- Comprehensive chaos testing coverage

### 1.2 Impact Assessment

**Development Impact:**
- Flaky tests slow down CI/CD pipeline
- Developers lose confidence in test suite
- Difficult to distinguish real failures from test infrastructure issues

**Production Impact:**
- Incomplete error categorization makes debugging difficult
- Reconnection race conditions can cause temporary cluster instability
- Split-brain scenarios, while rare, have high impact

**Operational Impact:**
- Manual intervention required for consistency issues
- Limited visibility into cluster health
- Difficult to predict when issues will self-resolve vs. require intervention

---

## 2. Solution Design

### 2.1 Design Principles

1. **Incremental over Big Bang:** Small, validated changes over large refactorings
2. **Test-First:** Fix test infrastructure before production code
3. **Feature Flags:** All production changes deployable with killswitch
4. **Preserve Strengths:** Maintain existing good patterns and comprehensive test coverage
5. **Observable:** Every change must improve observability

### 2.2 Priority 1: Critical Test Infrastructure

**Objective:** Eliminate test flakiness through systematic pattern replacement.

**Impact:** High | **Effort:** Low | **Timeline:** Weeks 1-2

#### Changes

**1. Create Reusable Test Helpers**

```java
/**
 * Common HA test utilities for ensuring cluster stability.
 */
public class HATestHelpers {

    /**
     * Waits for cluster to be fully stable before proceeding with test assertions.
     *
     * Ensures: (1) All servers ONLINE, (2) Replication queues empty, (3) All replicas connected
     *
     * @param test the test instance
     * @param serverCount number of servers in cluster
     */
    public static void waitForClusterStable(BaseGraphServerTest test, int serverCount) {
        // Phase 1: All servers ONLINE
        await().atMost(HATestTimeouts.CLUSTER_STABILIZATION_TIMEOUT)
               .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
               .until(() -> {
                   for (int i = 0; i < serverCount; i++) {
                       if (test.getServer(i).getStatus() != ArcadeDBServer.Status.ONLINE) {
                           return false;
                       }
                   }
                   return true;
               });

        // Phase 2: Replication queues empty
        for (int i = 0; i < serverCount; i++) {
            test.waitForReplicationIsCompleted(i);
        }

        // Phase 3: All replicas connected
        await().atMost(HATestTimeouts.REPLICA_RECONNECTION_TIMEOUT)
               .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
               .until(() -> test.areAllReplicasAreConnected());
    }

    /**
     * Waits for server shutdown with explicit timeout.
     */
    public static void waitForServerShutdown(ArcadeDBServer server, int serverId) {
        await().atMost(HATestTimeouts.SERVER_SHUTDOWN_TIMEOUT)
               .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL_LONG)
               .until(() -> server.getStatus() != ArcadeDBServer.Status.SHUTTING_DOWN);
    }

    /**
     * Waits for server startup and cluster joining.
     */
    public static void waitForServerStartup(ArcadeDBServer server, int serverId) {
        await().atMost(HATestTimeouts.SERVER_STARTUP_TIMEOUT)
               .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL_LONG)
               .until(() -> server.getStatus() == ArcadeDBServer.Status.ONLINE);
    }
}
```

**2. Systematic Test Conversion**

Replace all timing anti-patterns following these rules:

**Rule 1: No bare sleep statements**
```java
// BEFORE - Anti-pattern
Thread.sleep(2000);
assertThat(index.countEntries()).isEqualTo(TOTAL_RECORDS);

// AFTER - Condition-based wait
await().atMost(Duration.ofSeconds(30))
       .pollInterval(Duration.ofMillis(500))
       .untilAsserted(() ->
           assertThat(index.countEntries()).isEqualTo(TOTAL_RECORDS)
       );
```

**Rule 2: Replace retry loops with Awaitility**
```java
// BEFORE - Manual retry loop
for (int retry = 0; retry < maxRetry; ++retry) {
    try {
        resultSet = db.command("SQL", "CREATE VERTEX...");
        break;
    } catch (RemoteException e) {
        LogManager.instance().log(this, Level.SEVERE, "Retrying...", e);
        CodeUtils.sleep(500);
    }
}

// AFTER - Awaitility handles retries
await().atMost(Duration.ofSeconds(15))
       .pollInterval(Duration.ofMillis(500))
       .ignoreException(RemoteException.class)
       .until(() -> {
           ResultSet resultSet = db.command("SQL", "CREATE VERTEX...");
           return resultSet != null && resultSet.hasNext();
       });
```

**Rule 3: All tests must have @Timeout**
```java
// Simple tests (1-2 servers, < 1000 operations)
@Test
@Timeout(value = 5, unit = TimeUnit.MINUTES)
void simpleReplicationTest() { ... }

// Complex tests (3+ servers, > 1000 operations)
@Test
@Timeout(value = 15, unit = TimeUnit.MINUTES)
void complexHAScenarioTest() { ... }

// Chaos tests (random crashes, split brain)
@Test
@Timeout(value = 20, unit = TimeUnit.MINUTES)
void chaosEngineeringTest() { ... }
```

**3. Conversion Priority**

Convert tests in order of complexity (simple → complex):
1. `SimpleReplicationServerIT` - Establishes basic patterns
2. `ReplicationServerIT` - Base class affects all subclasses
3. `HARandomCrashIT` - Already partially improved
4. Remaining ~20 tests following established patterns

**Validation Criteria:**
- Each converted test must pass 100 consecutive runs locally
- Must pass 98/100 runs in CI (98% reliability minimum)
- No increase in average execution time
- Code review confirms pattern consistency

### 2.3 Priority 2: Production Code Hardening

**Objective:** Complete critical migrations and harden failure recovery paths.

**Impact:** High | **Effort:** Medium | **Timeline:** Weeks 3-5

#### Changes

**1. Complete ServerInfo Migration**

**Current Issue:** Mixed usage of string-based and `ServerInfo`-based server identification causes alias resolution failures in Docker/K8s.

**Solution:** Ensure all server identification uses stable server names (aliases) consistently.

```java
// HAServer.java - Already correct pattern, ensure consistency everywhere
private final Map<String, Leader2ReplicaNetworkExecutor> replicaConnections = new ConcurrentHashMap<>();
private final Map<String, ServerInfo> serverInfoByName = new ConcurrentHashMap<>();

// Key insight: Use server NAME (alias) as stable key, not entire ServerInfo
// ServerInfo addresses may change (Docker networking), but name is stable

// Update all lookups to use server name consistently
public Leader2ReplicaNetworkExecutor getReplica(String serverName) {
    return replicaConnections.get(serverName);
}

public ServerInfo getServerInfo(String serverName) {
    return serverInfoByName.get(serverName);
}
```

**Changes Required:**
- Audit all `Map<String, X>` to ensure keys are server names, not addresses
- Update `UpdateClusterConfiguration` to propagate `ServerInfo` properly
- Fix alias resolution in discovery mechanisms (Consul, K8s DNS)
- Add validation that server names are unique in cluster

**2. Network Executor State Machine**

**Current Issue:** State transitions during reconnection have subtle race conditions.

**Solution:** Explicit state machine with validated transitions.

```java
public class Leader2ReplicaNetworkExecutor extends Thread {

    public enum STATUS {
        CONNECTING,    // Initial connection establishment
        ONLINE,        // Healthy, processing messages
        RECONNECTING,  // Connection lost, attempting recovery
        DRAINING,      // Shutdown requested, processing remaining queue
        FAILED         // Unrecoverable error, manual intervention needed
    }

    // Valid state transitions
    private static final Map<STATUS, Set<STATUS>> VALID_TRANSITIONS = Map.of(
        CONNECTING, Set.of(ONLINE, FAILED, DRAINING),
        ONLINE, Set.of(RECONNECTING, DRAINING),
        RECONNECTING, Set.of(ONLINE, FAILED, DRAINING),
        DRAINING, Set.of(FAILED),
        FAILED, Set.of()  // Terminal state
    );

    private volatile STATUS status = STATUS.CONNECTING;

    /**
     * Thread-safe state transition with validation and logging.
     */
    private void transitionTo(STATUS newStatus, String reason) {
        synchronized (this) {
            if (!VALID_TRANSITIONS.get(status).contains(newStatus)) {
                LogManager.instance().log(this, Level.SEVERE,
                    "Invalid state transition: %s -> %s (reason: %s)",
                    status, newStatus, reason);
                return;
            }

            STATUS oldStatus = this.status;
            this.status = newStatus;

            LogManager.instance().log(this, Level.INFO,
                "Replica '%s' state: %s -> %s (%s)",
                remoteServer, oldStatus, newStatus, reason);

            // Emit lifecycle event for monitoring
            server.lifecycleEvent(
                ReplicationCallback.Type.REPLICA_STATE_CHANGED,
                new StateChangeEvent(remoteServer, oldStatus, newStatus, reason)
            );
        }
    }
}
```

**3. Enhanced Reconnection Logic**

**Current Issue:** Generic exception handling doesn't distinguish between transient network failures, leadership changes, and unrecoverable errors.

**Solution:** Categorize failures and apply appropriate recovery strategies.

```java
/**
 * Categorizes exceptions and applies appropriate reconnection strategy.
 */
private void reconnect(final Exception e) {
    if (Thread.currentThread().isInterrupted() || shutdown) {
        transitionTo(STATUS.DRAINING, "Shutdown requested");
        return;
    }

    if (isTransientNetworkFailure(e)) {
        // Network blip - quick retry with exponential backoff
        transitionTo(STATUS.RECONNECTING, "Transient network failure");
        reconnectWithBackoff(3, 1000, 2.0);  // 3 retries, 1s base, 2x multiplier

    } else if (isLeadershipChange(e)) {
        // Leader changed - find new leader immediately (no backoff needed)
        transitionTo(STATUS.RECONNECTING, "Leader changed");
        closeChannel();
        findAndConnectToNewLeader();

    } else if (isProtocolError(e)) {
        // Protocol version mismatch or corruption - fail fast
        transitionTo(STATUS.FAILED, "Protocol error: " + e.getMessage());
        server.lifecycleEvent(ReplicationCallback.Type.REPLICA_FAILED, this);

    } else {
        // Unknown error - log and attempt recovery with longer backoff
        LogManager.instance().log(this, Level.SEVERE,
            "Unknown error during replication to '%s'", e, remoteServer);
        transitionTo(STATUS.RECONNECTING, "Unknown error");
        reconnectWithBackoff(5, 2000, 2.0);  // 5 retries, 2s base, longer delays
    }
}

private boolean isTransientNetworkFailure(Exception e) {
    return e instanceof SocketTimeoutException ||
           e instanceof SocketException ||
           (e instanceof IOException && e.getMessage().contains("Connection reset"));
}

private boolean isLeadershipChange(Exception e) {
    return e instanceof ServerIsNotTheLeaderException ||
           e instanceof ConnectionException && e.getMessage().contains("not the Leader");
}

private boolean isProtocolError(Exception e) {
    return e instanceof NetworkProtocolException ||
           e instanceof IOException && e.getMessage().contains("Protocol");
}

/**
 * Reconnects with exponential backoff.
 */
private void reconnectWithBackoff(int maxAttempts, long baseDelayMs, double multiplier) {
    long delay = baseDelayMs;

    for (int attempt = 1; attempt <= maxAttempts && !shutdown; attempt++) {
        try {
            Thread.sleep(delay);
            connect();
            startup();
            transitionTo(STATUS.ONLINE, "Reconnection successful");
            return;  // Success

        } catch (Exception e) {
            LogManager.instance().log(this, Level.WARNING,
                "Reconnection attempt %d/%d failed (next retry in %dms)",
                e, attempt, maxAttempts, delay);
            delay = Math.min((long)(delay * multiplier), 30000);  // Cap at 30s
        }
    }

    // All attempts failed
    transitionTo(STATUS.FAILED, "Max reconnection attempts exceeded");
    server.startElection(true);  // Trigger new leader election
}
```

**Rollout Strategy for Phase 2:**

All changes deployed with feature flags:
```java
// Example: Enhanced reconnection logic
if (GlobalConfiguration.HA_ENHANCED_RECONNECTION.getValueAsBoolean()) {
    reconnectEnhanced(e);  // New code path
} else {
    reconnect(e);  // Legacy code path
}
```

**Validation Gates:**
1. Deploy with flag OFF by default
2. Run chaos tests 100 times - establish baseline
3. Enable flag in test environment, monitor 24 hours
4. Enable flag in all test environments, monitor 48 hours
5. If metrics acceptable, enable by default
6. After 2 weeks stable, remove flag and legacy code

### 2.4 Priority 3: Enhanced Observability

**Objective:** Add real-time cluster health monitoring and improve debugging capabilities.

**Impact:** Medium | **Effort:** Medium | **Timeline:** Weeks 6-7

#### Changes

**1. Cluster Health API**

```java
/**
 * Real-time cluster health status.
 */
public class ClusterHealth {
    private final boolean quorumAvailable;
    private final long maxReplicationLagMs;
    private final List<String> disconnectedReplicas;
    private final long leaderEpoch;
    private final long lastElectionAgeMs;
    private final Map<String, ReplicaHealth> replicaHealth;

    public boolean isFullyStable() {
        return quorumAvailable &&
               maxReplicationLagMs < 1000 &&
               disconnectedReplicas.isEmpty() &&
               lastElectionAgeMs > 30000;  // Leader stable for 30s+
    }

    public boolean isOperational() {
        return quorumAvailable;
    }
}

/**
 * Per-replica health status.
 */
public class ReplicaHealth {
    private final String serverName;
    private final Leader2ReplicaNetworkExecutor.STATUS status;
    private final long replicationLagMs;
    private final int queueSize;
    private final long lastMessageAgeMs;
    private final int consecutiveFailures;
}

/**
 * Cluster health checker.
 */
public class ClusterHealthChecker {

    public ClusterHealth checkHealth(HAServer server) {
        // Collect metrics from all replica connections
        Map<String, ReplicaHealth> replicaHealth = new HashMap<>();
        long maxLag = 0;
        List<String> disconnected = new ArrayList<>();

        for (Map.Entry<String, Leader2ReplicaNetworkExecutor> entry :
                server.getReplicaConnections().entrySet()) {

            String replicaName = entry.getKey();
            Leader2ReplicaNetworkExecutor replica = entry.getValue();

            ReplicaHealth health = new ReplicaHealth(
                replicaName,
                replica.getStatus(),
                replica.getReplicationLag(),
                replica.getQueueSize(),
                replica.getLastMessageAge(),
                replica.getConsecutiveFailures()
            );

            replicaHealth.put(replicaName, health);

            if (health.status != Leader2ReplicaNetworkExecutor.STATUS.ONLINE) {
                disconnected.add(replicaName);
            }

            maxLag = Math.max(maxLag, health.replicationLagMs);
        }

        return new ClusterHealth(
            server.isQuorumAvailable(),
            maxLag,
            disconnected,
            server.getLeaderEpoch(),
            System.currentTimeMillis() - server.getLastElectionTime(),
            replicaHealth
        );
    }
}
```

**Usage in Tests:**
```java
// Replace sleep-based waits with health-based waits
await().atMost(Duration.ofSeconds(30))
       .until(() -> {
           ClusterHealth health = healthChecker.checkHealth(server.getHA());
           return health.isFullyStable();
       });
```

**Usage in Production:**
```java
// HTTP endpoint: GET /api/v1/cluster/health
{
  "quorumAvailable": true,
  "maxReplicationLagMs": 245,
  "disconnectedReplicas": [],
  "leaderEpoch": 5,
  "lastElectionAgeMs": 3600000,
  "replicas": {
    "ArcadeDB_1": {
      "status": "ONLINE",
      "replicationLagMs": 120,
      "queueSize": 5,
      "lastMessageAgeMs": 50
    },
    "ArcadeDB_2": {
      "status": "ONLINE",
      "replicationLagMs": 245,
      "queueSize": 12,
      "lastMessageAgeMs": 100
    }
  }
}
```

**2. Structured Error Categorization**

Replace generic exception handlers with specific error types:

```java
// Create specific exception types
public class ReplicationTransientException extends ReplicationException {
    // Network timeouts, temporary unavailability
}

public class ReplicationPermanentException extends ReplicationException {
    // Protocol errors, version mismatches
}

public class LeadershipChangeException extends ReplicationException {
    // Leader election in progress, leader changed
}

// Update catch blocks
try {
    sendMessageToReplica(message);
} catch (ReplicationTransientException e) {
    // Retry with backoff
    retryWithBackoff(message);
} catch (LeadershipChangeException e) {
    // Wait for new leader
    waitForLeaderElection();
} catch (ReplicationPermanentException e) {
    // Fail fast, alert operator
    markReplicaFailed(e);
}
```

**3. Message Sequence Validation Enhancement**

```java
/**
 * Enhanced message ordering validation with gap detection.
 */
public class ReplicationLogFile {

    /**
     * Checks for message ordering issues and identifies gaps.
     *
     * @return Gap information if messages are missing, null if ordering is correct
     */
    public MessageGap detectGap(ReplicationMessage message) {
        long expected = getLastMessageNumber() + 1;
        long actual = message.messageNumber;

        if (actual == expected) {
            return null;  // In order
        }

        if (actual < expected) {
            // Duplicate - already processed
            return MessageGap.duplicate(actual, expected - 1);
        }

        if (actual > expected) {
            // Gap detected - missing messages
            return MessageGap.missing(expected, actual - 1);
        }

        return null;
    }

    /**
     * Requests missing messages from leader.
     */
    public void requestMissingMessages(MessageGap gap) {
        LogManager.instance().log(this, Level.WARNING,
            "Gap detected in replication log: expected %d, got %d. Requesting missing messages.",
            gap.expectedStart, gap.actualReceived);

        // Send request to leader for messages in range [gap.start, gap.end]
        server.getLeader().requestMessageRange(gap.start, gap.end);
    }
}

public record MessageGap(long start, long end, GapType type) {
    enum GapType { MISSING, DUPLICATE }

    static MessageGap missing(long start, long end) {
        return new MessageGap(start, end, GapType.MISSING);
    }

    static MessageGap duplicate(long messageNum, long lastProcessed) {
        return new MessageGap(messageNum, lastProcessed, GapType.DUPLICATE);
    }
}
```

### 2.5 Priority 4: Advanced Resilience Features

**Objective:** Add self-healing capabilities and proactive consistency monitoring.

**Impact:** Medium | **Effort:** High | **Timeline:** Week 8

#### Changes

**1. Circuit Breaker for Slow Replicas**

```java
/**
 * Circuit breaker to handle consistently slow/failing replicas.
 *
 * States:
 * - CLOSED: Normal operation, all messages sent to replica
 * - OPEN: Too many failures, replica temporarily excluded from quorum
 * - HALF_OPEN: Testing if replica has recovered
 */
public class ReplicaCircuitBreaker {

    enum State { CLOSED, OPEN, HALF_OPEN }

    private volatile State state = State.CLOSED;
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicInteger consecutiveSuccesses = new AtomicInteger(0);
    private volatile long openedAt = 0;

    private static final int FAILURE_THRESHOLD = 5;      // Open after 5 failures
    private static final int SUCCESS_THRESHOLD = 3;      // Close after 3 successes
    private static final long RETRY_TIMEOUT_MS = 30000;  // Try again after 30s

    /**
     * Records a successful operation.
     */
    public void recordSuccess() {
        consecutiveFailures.set(0);

        if (state == State.HALF_OPEN) {
            if (consecutiveSuccesses.incrementAndGet() >= SUCCESS_THRESHOLD) {
                transitionTo(State.CLOSED, "Replica recovered");
            }
        }
    }

    /**
     * Records a failed operation.
     *
     * @return true if message should be retried, false if replica is circuit-broken
     */
    public boolean recordFailure() {
        consecutiveSuccesses.set(0);

        if (state == State.CLOSED) {
            if (consecutiveFailures.incrementAndGet() >= FAILURE_THRESHOLD) {
                transitionTo(State.OPEN, "Too many consecutive failures");
                return false;  // Don't retry - circuit open
            }
        }

        return state != State.OPEN;
    }

    /**
     * Checks if replica should receive messages.
     */
    public boolean shouldAttempt() {
        if (state == State.CLOSED) {
            return true;
        }

        if (state == State.OPEN) {
            // Check if retry timeout has elapsed
            if (System.currentTimeMillis() - openedAt > RETRY_TIMEOUT_MS) {
                transitionTo(State.HALF_OPEN, "Retry timeout elapsed");
                return true;
            }
            return false;
        }

        // HALF_OPEN - allow attempts
        return true;
    }

    private void transitionTo(State newState, String reason) {
        State oldState = this.state;
        this.state = newState;

        if (newState == State.OPEN) {
            openedAt = System.currentTimeMillis();
        }

        LogManager.instance().log(this, Level.WARNING,
            "Circuit breaker state change: %s -> %s (%s)",
            oldState, newState, reason);
    }
}

// Integration with Leader2ReplicaNetworkExecutor
public void sendMessage(Binary message) throws IOException {
    if (!circuitBreaker.shouldAttempt()) {
        // Circuit open - don't wait for timeout, fail fast
        throw new ReplicationTransientException("Circuit breaker open for replica " + remoteServer);
    }

    try {
        // Send message
        actualSendMessage(message);
        circuitBreaker.recordSuccess();
    } catch (Exception e) {
        if (!circuitBreaker.recordFailure()) {
            LogManager.instance().log(this, Level.WARNING,
                "Circuit breaker opened for replica %s - temporarily excluded from quorum",
                remoteServer);
        }
        throw e;
    }
}
```

**2. Background Consistency Monitor**

```java
/**
 * Lightweight background consistency checker using sampling.
 *
 * Periodically samples records across replicas and compares checksums.
 * If drift detected above threshold, triggers automatic alignment.
 */
public class ConsistencyMonitor extends Thread {

    private final HAServer server;
    private final long checkIntervalMs;
    private final double samplePercentage;
    private final int driftThreshold;
    private volatile boolean shutdown = false;

    public ConsistencyMonitor(HAServer server) {
        this.server = server;
        this.checkIntervalMs = server.getConfiguration()
            .getValueAsLong(GlobalConfiguration.HA_CONSISTENCY_CHECK_INTERVAL);
        this.samplePercentage = server.getConfiguration()
            .getValueAsDouble(GlobalConfiguration.HA_CONSISTENCY_SAMPLE_PERCENTAGE);
        this.driftThreshold = server.getConfiguration()
            .getValueAsInteger(GlobalConfiguration.HA_CONSISTENCY_DRIFT_THRESHOLD);

        setDaemon(true);
        setName("HA-ConsistencyMonitor");
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Thread.sleep(checkIntervalMs);

                if (server.isLeader() && allReplicasOnline()) {
                    checkConsistency();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void checkConsistency() {
        for (String dbName : server.getDatabaseNames()) {
            try {
                ConsistencyReport report = sampleDatabaseConsistency(dbName);

                if (report.driftCount > driftThreshold) {
                    LogManager.instance().log(this, Level.WARNING,
                        "Consistency drift detected in database '%s': %d records differ (threshold: %d)",
                        dbName, report.driftCount, driftThreshold);

                    // Emit metric
                    server.lifecycleEvent(
                        ReplicationCallback.Type.CONSISTENCY_DRIFT_DETECTED,
                        report
                    );

                    // Auto-trigger alignment if configured
                    if (server.getConfiguration().getValueAsBoolean(
                            GlobalConfiguration.HA_CONSISTENCY_AUTO_ALIGN)) {
                        triggerAlignment(dbName);
                    }
                }

            } catch (Exception e) {
                LogManager.instance().log(this, Level.SEVERE,
                    "Error during consistency check for database '%s'", e, dbName);
            }
        }
    }

    private ConsistencyReport sampleDatabaseConsistency(String dbName) {
        Database db = server.getDatabase(dbName);
        ConsistencyReport report = new ConsistencyReport(dbName);

        // Get total record count
        long totalRecords = db.countType("V", true);  // Count all vertices
        long sampleSize = (long)(totalRecords * samplePercentage / 100.0);

        // Sample random records
        Random random = new Random();
        Set<RID> sampledRIDs = new HashSet<>();

        // Simple random sampling
        Iterator<Record> iterator = db.iterateType("V", true);
        long skipInterval = totalRecords / sampleSize;
        long count = 0;

        while (iterator.hasNext() && sampledRIDs.size() < sampleSize) {
            Record record = iterator.next();
            if (count % skipInterval == 0) {
                sampledRIDs.add(record.getIdentity());
            }
            count++;
        }

        // Compare sampled records across replicas
        for (RID rid : sampledRIDs) {
            Map<String, byte[]> checksums = new HashMap<>();

            // Get checksum from each replica
            for (Leader2ReplicaNetworkExecutor replica : server.getReplicaConnections().values()) {
                byte[] checksum = getRecordChecksum(replica, dbName, rid);
                checksums.put(replica.getRemoteServerName(), checksum);
            }

            // Compare checksums
            if (!allChecksumsMatch(checksums)) {
                report.recordDrift(rid, checksums);
            }
        }

        return report;
    }

    private void triggerAlignment(String dbName) {
        LogManager.instance().log(this, Level.INFO,
            "Auto-triggering database alignment for '%s'", dbName);

        Database db = server.getDatabase(dbName);
        db.command("sql", "ALIGN DATABASE");
    }
}

/**
 * Consistency check report.
 */
public class ConsistencyReport {
    public final String databaseName;
    public final long sampleSize;
    public int driftCount = 0;
    public final List<RecordDrift> drifts = new ArrayList<>();

    public void recordDrift(RID rid, Map<String, byte[]> checksums) {
        driftCount++;
        drifts.add(new RecordDrift(rid, checksums));
    }
}

public record RecordDrift(RID rid, Map<String, byte[]> checksumsByReplica) {}
```

---

## 3. Implementation Strategy

### 3.1 Rollout Phases

**Phase 1: Test Infrastructure (Weeks 1-2)**

**Week 1:**
- Create `HATestHelpers` utility class
- Convert 5-7 simple tests (SimpleReplicationServerIT, etc.)
- Validate pattern with team review
- Run converted tests 100 times each

**Week 2:**
- Convert `ReplicationServerIT` base class
- Convert remaining tests using established patterns
- Add `@Timeout` annotations to all tests
- Final validation: Full suite 100 runs

**Success Criteria:**
- All tests have `@Timeout` annotations
- Zero test hangs observed
- 95%+ pass rate on all converted tests
- No increase in average execution time

**Phase 2: Production Hardening (Weeks 3-5)**

**Week 3:**
- Complete ServerInfo migration
- Add state machine to network executors
- Deploy behind feature flags (OFF by default)

**Week 4:**
- Implement enhanced reconnection logic
- Add exception categorization
- Enable feature flags in test environments
- Monitor for 48 hours

**Week 5:**
- Enhanced message sequence validation
- Fix any issues found in monitoring
- Enable feature flags by default
- Full chaos test validation (100 runs)

**Success Criteria:**
- All chaos tests pass 98/100 runs
- Zero split-brain incidents in testing
- Leader election success rate >99%
- Metrics show improved reconnection behavior

**Phase 3: Advanced Features (Weeks 6-8)**

**Week 6:**
- Implement cluster health API
- Add HTTP endpoints for monitoring
- Update tests to use health checks

**Week 7:**
- Implement circuit breaker for replicas
- Add structured error types
- Deploy in observe-only mode

**Week 8:**
- Implement consistency monitor
- Enable circuit breaker (if metrics look good)
- Enable auto-alignment (if metrics look good)
- Final validation and documentation

**Success Criteria:**
- Health API available and accurate
- Circuit breaker prevents cascade failures
- Consistency monitor detects drift (validated with injected inconsistencies)

### 3.2 Validation Gates

Each phase must pass validation before proceeding:

**Gate 1: Baseline Metrics (Before Phase 1)**
```bash
# Capture baseline
mvn test -P integration -Dtest="*HA*IT,*Replication*IT"

# Track:
# - Pass rate per test
# - Execution time per test
# - Timeout frequency
# - Flakiness score (failures over 100 runs)
```

**Gate 2: Phase 1 Validation**
- Run converted test suite 100 times
- Pass rate must be ≥95% for each test
- Zero timeout failures
- Code review approval

**Gate 3: Phase 2 Validation**
- Run chaos tests 100 times with new code
- Compare metrics to baseline
- No regressions in pass rate
- Feature flags confirmed working (can disable and revert to baseline behavior)

**Gate 4: Phase 3 Validation**
- Health API returns accurate data
- Circuit breaker prevents cascade failures (validated with fault injection)
- Consistency monitor detects injected inconsistencies
- No performance degradation (p99 latency)

### 3.3 Feature Flag Strategy

All production changes use feature flags for safe rollout:

```java
// GlobalConfiguration additions
public static final Setting HA_ENHANCED_RECONNECTION =
    new Setting("ha.reconnection.enhanced", true, ...);

public static final Setting HA_CIRCUIT_BREAKER_ENABLED =
    new Setting("ha.circuitBreaker.enabled", false, ...);

public static final Setting HA_CONSISTENCY_AUTO_ALIGN =
    new Setting("ha.consistency.autoAlign", false, ...);
```

**Rollout Pattern:**
1. Deploy with flag OFF (uses legacy code)
2. Enable in one test environment, monitor 24h
3. Enable in all test environments, monitor 48h
4. Enable by default (can still disable if issues found)
5. After 2 weeks stable, remove flag and legacy code

### 3.4 Risk Mitigation

**Risk 1: Test changes mask production bugs**

*Mitigation:*
- Keep original tests as `*IT_Original.java` for 1 release
- Run both old and new tests in parallel during transition
- Production changes must pass BOTH test suites

**Risk 2: Production changes break existing clusters**

*Mitigation:*
- Feature flags allow instant rollback
- Wire protocol versioning for old/new server interop
- Rolling upgrade testing (mixed version clusters)
- Canary deployments in production

**Risk 3: Performance degradation**

*Mitigation:*
- Benchmark before/after each change
- Replication lag monitoring
- Automatic rollback if p99 latency increases >10%
- Circuit breaker prevents slow replicas from affecting cluster

**Risk 4: Incomplete migration**

*Mitigation:*
- Comprehensive code audit before starting
- Static analysis to find remaining string-based server IDs
- Integration tests for Docker/K8s scenarios
- Gradual migration with backward compatibility

---

## 4. Monitoring & Validation

### 4.1 Test Suite Monitoring

**Daily CI Job:**
```bash
# Run HA test suite with extended iterations
mvn test -P integration -Dtest="*HA*IT" -Dsurefire.rerunFailingTestsCount=20

# Track over time:
# - Pass rate per test (trend chart)
# - Flakiness score (failures / total runs)
# - Execution time (detect slowdowns)
# - Resource usage (memory, CPU)
```

**Dashboard Metrics:**
- Test reliability trends (7-day, 30-day rolling average)
- Top 10 flaky tests
- Test execution time distribution
- Failure categorization (timeout vs assertion vs exception)

**Alerts:**
- Any test drops below 95% pass rate
- Average execution time increases >20%
- New timeouts detected

### 4.2 Production Metrics

**Key Metrics to Collect:**

```java
// Leader Election
- election_count (counter)
- election_duration_ms (histogram)
- election_failures (counter)
- leader_tenure_seconds (gauge)

// Replication
- replication_queue_size (gauge per replica)
- replication_lag_ms (gauge per replica)
- replication_failures (counter per replica)
- hot_resync_count (counter)
- full_resync_count (counter)

// Connection Health
- replica_connections_total (gauge)
- replica_reconnection_count (counter)
- replica_offline_duration_ms (histogram)
- circuit_breaker_state (gauge per replica)

// Cluster Health
- quorum_status (gauge: 1 = available, 0 = lost)
- cluster_size_configured (gauge)
- cluster_size_online (gauge)
- split_brain_detected (counter - should be 0)

// Consistency
- consistency_check_count (counter)
- consistency_drift_detected (counter)
- consistency_auto_align_triggered (counter)
```

**Metric Collection:**
```java
// MicroMeter or similar metrics library
MeterRegistry registry = server.getMetricRegistry();

// Example metric registration
Gauge.builder("ha.replica.queue.size", replica,
    r -> r.getQueueSize())
    .tag("replica", replica.getRemoteServerName())
    .register(registry);

Counter.builder("ha.election.count")
    .register(registry)
    .increment();
```

### 4.3 Alerting Strategy

**Critical Alerts (Page On-Call):**
- Quorum lost for >30 seconds
- Split brain detected
- Leader election stuck for >2 minutes
- Any server offline for >5 minutes
- Circuit breaker trips on majority of replicas

**Warning Alerts (Create Ticket):**
- Replication lag >10 seconds
- Hot resync triggered (indicates queue overflow)
- Leader election >3 times in 1 hour
- Replica reconnection >10 times in 1 hour
- Consistency drift detected

**Info Alerts (Trending Only):**
- Full resync triggered
- Quorum lost for <30 seconds (transient)
- Circuit breaker opened (single replica)

### 4.4 Long-term Maintenance

**Monthly:**
- Review test reliability dashboard
- Analyze flaky tests that appeared
- Update timeout values if CI environment changed
- Review production HA metrics for trends

**Quarterly:**
- Run extended chaos tests (24-48 hours)
- Review and update HA documentation
- Conduct "game day" exercises (simulated failures)
- Benchmark performance vs. previous quarter

**Per Release:**
- Run test suite 100 times before release
- Verify no new flaky tests introduced
- Update CLAUDE.md with new patterns
- Document known issues/limitations

---

## 5. Success Metrics

### 5.1 Quantitative Targets

| Metric | Baseline | Phase 1 Target | Phase 2 Target | Phase 3 Target |
|--------|----------|----------------|----------------|----------------|
| HA test pass rate | ~85% | 95% | 98% | 99% |
| Test timeout rate | ~5% | 0% | 0% | 0% |
| Test execution time | Baseline | +0% | +0% | +5% |
| Leader election success | ~95% | - | 99% | 99.9% |
| Split-brain incidents | Rare | - | Zero in testing | Zero |
| Mean time to detect failure | Manual | <1min | <30sec | <10sec |
| False positive alerts | High | - | <5/week | <2/week |

### 5.2 Qualitative Targets

**Developer Experience:**
- Developers trust test results (no "run it again" culture)
- Flaky tests identified and fixed within 1 week
- CI/CD pipeline reliability >99%
- Test failures are always actionable

**Operational Excellence:**
- Cluster health visible in real-time
- Failures self-heal without intervention in >90% of cases
- Root cause of issues identifiable from metrics
- Runbooks cover all common scenarios

**Production Stability:**
- Zero unplanned downtime due to HA issues
- Planned maintenance with zero downtime (rolling restarts)
- Recovery from failures fully automated
- Consistency guaranteed (periodic validation)

---

## 6. Open Questions & Future Work

### 6.1 Open Questions

1. **Consistency monitor performance:** What's the acceptable overhead for sampling? Need to benchmark with production-sized databases.

2. **Circuit breaker thresholds:** Are default thresholds (5 failures, 30s timeout) appropriate? May need tuning based on production workloads.

3. **Auto-alignment safety:** Should auto-alignment require confirmation for production databases? Consider adding approval workflow.

4. **Metrics retention:** How long should we retain detailed HA metrics? Balance between debugging capability and storage cost.

### 6.2 Future Enhancements (Out of Scope)

**Advanced Leader Election:**
- Priority-based leader election (prefer certain nodes)
- Leader affinity (minimize leader changes)
- Pre-voting phase to prevent election storms

**Multi-Region Support:**
- Cross-region replication with lag tolerance
- Region-aware quorum (prefer local replicas)
- Disaster recovery across regions

**Performance Optimizations:**
- Batch replication for small transactions
- Compression for replication messages
- Parallel replication to multiple replicas

**Enhanced Monitoring:**
- Distributed tracing for replication flow
- Anomaly detection using ML
- Predictive alerting (detect issues before failure)

**Testing Infrastructure:**
- Toxiproxy integration for network fault injection
- Automated performance regression detection
- Chaos testing in production (controlled experiments)

---

## 7. Conclusion

This design provides a comprehensive, phased approach to improving ArcadeDB's HA system reliability. By starting with test infrastructure improvements (high impact, low risk), we establish a solid foundation for validating production changes. The use of feature flags, validation gates, and incremental rollout minimizes risk while allowing rapid iteration.

**Key Success Factors:**

1. **Discipline:** Follow the phased approach, don't skip validation gates
2. **Metrics:** Measure everything, make data-driven decisions
3. **Reversibility:** All changes must be reversible (feature flags)
4. **Team Buy-in:** Regular reviews, shared ownership of reliability

**Next Steps:**

1. Review and approval of this design document
2. Create implementation tasks in issue tracker
3. Assign owners for each phase
4. Establish baseline metrics (Week 0)
5. Begin Phase 1 implementation

---

## Appendix A: Code Review Checklist

Use this checklist when reviewing HA-related changes:

**Test Code:**
- [ ] No bare `Thread.sleep()` or `CodeUtils.sleep()` statements
- [ ] All async operations use Awaitility with explicit timeouts
- [ ] Test has `@Timeout` annotation with appropriate duration
- [ ] Test uses `HATestHelpers` for cluster stabilization
- [ ] Test waits for replication completion before assertions
- [ ] Resource cleanup in `@AfterEach` method
- [ ] Test is deterministic (no timing-dependent assertions)

**Production Code:**
- [ ] Server identification uses stable server names (not addresses)
- [ ] State transitions are validated and logged
- [ ] Exceptions are categorized (not generic `catch (Exception)`)
- [ ] Resource cleanup in finally blocks or try-with-resources
- [ ] Thread-safe access to shared state
- [ ] Lifecycle events emitted for observability
- [ ] Feature flags used for risky changes
- [ ] Backward compatibility maintained (or explicitly versioned)

**Documentation:**
- [ ] Javadoc for public APIs
- [ ] Timeout rationale documented (if non-standard)
- [ ] State machine transitions documented
- [ ] Metrics documented (name, type, purpose)

## Appendix B: Test Conversion Examples

### Example 1: Simple Sleep Replacement

**Before:**
```java
@Test
void testReplication() {
    // Insert data on leader
    insertData(leaderDb, 1000);

    // Wait for replication
    Thread.sleep(5000);

    // Verify on replica
    assertThat(replicaDb.countType("V", true)).isEqualTo(1000);
}
```

**After:**
```java
@Test
@Timeout(value = 5, unit = TimeUnit.MINUTES)
void testReplication() {
    // Insert data on leader
    insertData(leaderDb, 1000);

    // Wait for replication to complete
    HATestHelpers.waitForClusterStable(this, getServerCount());

    // Verify on replica with condition-based wait
    await().atMost(Duration.ofSeconds(30))
           .pollInterval(Duration.ofMillis(500))
           .untilAsserted(() ->
               assertThat(replicaDb.countType("V", true)).isEqualTo(1000)
           );
}
```

### Example 2: Retry Loop Replacement

**Before:**
```java
ResultSet resultSet = null;
for (int retry = 0; retry < 10; ++retry) {
    try {
        resultSet = db.command("SQL", "CREATE VERTEX...");
        if (resultSet != null && resultSet.hasNext()) {
            break;
        }
    } catch (RemoteException e) {
        LogManager.instance().log(this, Level.SEVERE, "Retrying...", e);
        CodeUtils.sleep(500);
    }
}
assertThat(resultSet).isNotNull();
```

**After:**
```java
ResultSet resultSet = await()
    .atMost(Duration.ofSeconds(15))
    .pollInterval(Duration.ofMillis(500))
    .ignoreException(RemoteException.class)
    .until(() -> {
        ResultSet rs = db.command("SQL", "CREATE VERTEX...");
        return rs != null && rs.hasNext() ? rs : null;
    });

assertThat(resultSet).isNotNull();
```

### Example 3: Server Lifecycle Management

**Before:**
```java
@Test
void testServerRestart() {
    getServer(0).stop();

    while (getServer(0).getStatus() == ArcadeDBServer.Status.SHUTTING_DOWN) {
        CodeUtils.sleep(300);
    }

    getServer(0).start();

    Thread.sleep(5000);  // Hope it's started

    // Run test...
}
```

**After:**
```java
@Test
@Timeout(value = 5, unit = TimeUnit.MINUTES)
void testServerRestart() {
    // Stop server
    getServer(0).stop();
    HATestHelpers.waitForServerShutdown(getServer(0), 0);

    // Start server
    getServer(0).start();
    HATestHelpers.waitForServerStartup(getServer(0), 0);

    // Wait for cluster to stabilize
    HATestHelpers.waitForClusterStable(this, getServerCount());

    // Run test...
}
```

## Appendix C: Glossary

**Awaitility:** Testing library that provides fluent API for waiting on asynchronous conditions with timeouts.

**Circuit Breaker:** Design pattern that prevents cascading failures by temporarily excluding failing components.

**Chaos Engineering:** Practice of injecting failures into systems to test resilience.

**Feature Flag:** Configuration toggle that enables/disables features without code deployment.

**Hot Resync:** Synchronization of replica with leader while cluster remains operational (no full database copy).

**Quorum:** Minimum number of servers that must agree for operation to succeed.

**Split Brain:** Network partition where multiple leaders are elected simultaneously.

**State Machine:** Model where system transitions between well-defined states with validated transitions.

---

*End of Design Document*
