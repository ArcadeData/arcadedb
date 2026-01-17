# Phase 2: Enhanced Reconnection & State Machine Design

**Date:** 2026-01-17
**Branch:** feature/2043-ha-test
**Phase:** 2 - Production Hardening
**Status:** Design Complete, Ready for Implementation

## Executive Summary

Implement intelligent reconnection logic with explicit state machine to fix leader election/failover test failures and improve production HA reliability.

**Problem Statement:**
- Baseline test pass rate: 61% (target: 90%+)
- Leader transition tests failing: ReplicationServerLeaderDownIT, ReplicationServerLeaderChanges3TimesIT
- Current reconnection logic treats all failures the same way
- No visibility into replica connection state during failures

**Solution:**
- Explicit state machine for replica connections (5 states, validated transitions)
- Exception classification (4 categories: transient, leadership, protocol, unknown)
- Category-specific recovery strategies (exponential backoff, immediate reconnect, fail-fast)
- Observable lifecycle events and metrics

**Expected Impact:**
- Fix 39% test failure rate (improve from 61% to 80-90%)
- Faster recovery from leadership changes (no unnecessary backoff)
- Better production observability (clear state, failure categorization)
- Foundation for future HA improvements (circuit breakers, health monitoring)

## Architecture Overview

### 3-Layer Design

**Layer 1: Explicit State Machine**
- Replace implicit state with `STATUS` enum in `Leader2ReplicaNetworkExecutor`
- Validated state transitions with logging
- Terminal `FAILED` state for unrecoverable errors

**Layer 2: Intelligent Exception Classification**
- Categorize exceptions: Transient Network, Leadership Change, Protocol Error, Unknown
- Each category drives different recovery strategy
- Reduces inappropriate retries and delays

**Layer 3: Observable Recovery**
- Lifecycle events for every state transition
- Metrics: reconnection attempts, failure categories, recovery times
- Health API exposing replica status and lag

### Benefits

1. **Fixes Test Failures** - Proper leadership change handling fixes failing tests
2. **Reduces Flakiness** - Transient network blips handled with quick retry
3. **Fail Fast** - Protocol errors don't waste time retrying
4. **Observable** - Clear visibility into what's happening during failures
5. **Safe Rollout** - Feature-flagged, can disable if issues arise

## Component 1: State Machine

### State Enum

```java
public class Leader2ReplicaNetworkExecutor extends Thread {

    public enum STATUS {
        CONNECTING,    // Initial connection establishment to leader
        ONLINE,        // Healthy, actively processing replication messages
        RECONNECTING,  // Connection lost, attempting recovery with backoff
        DRAINING,      // Shutdown requested, processing remaining queue
        FAILED         // Unrecoverable error, requires manual intervention
    }
}
```

### State Lifecycle

**Happy Path:**
```
CONNECTING → ONLINE → [operate normally] → DRAINING → FAILED (terminal)
```

**Failure and Recovery Path:**
```
ONLINE → RECONNECTING → ONLINE (recovered)
RECONNECTING → FAILED (max retries exceeded)
```

**Startup Failure Path:**
```
CONNECTING → FAILED (connection refused, leader not available)
```

### Valid State Transitions

```java
private static final Map<STATUS, Set<STATUS>> VALID_TRANSITIONS = Map.of(
    CONNECTING, Set.of(ONLINE, FAILED, DRAINING),
    ONLINE, Set.of(RECONNECTING, DRAINING),
    RECONNECTING, Set.of(ONLINE, FAILED, DRAINING),
    DRAINING, Set.of(FAILED),
    FAILED, Set.of()  // Terminal state, no transitions out
);
```

### State Transition Method

```java
/**
 * Thread-safe state transition with validation and logging.
 *
 * @param newStatus the target state
 * @param reason human-readable reason for transition
 * @throws IllegalStateException if transition is invalid
 */
private void transitionTo(STATUS newStatus, String reason) {
    synchronized (this) {
        // Validate transition
        if (!VALID_TRANSITIONS.get(status).contains(newStatus)) {
            String msg = String.format(
                "Invalid state transition: %s -> %s (reason: %s, replica: %s)",
                status, newStatus, reason, remoteServerName);

            LogManager.instance().log(this, Level.SEVERE, msg);
            throw new IllegalStateException(msg);
        }

        STATUS oldStatus = this.status;
        this.status = newStatus;

        // Log transition
        LogManager.instance().log(this, Level.INFO,
            "Replica '%s' state: %s -> %s (%s)",
            remoteServerName, oldStatus, newStatus, reason);

        // Emit lifecycle event for monitoring
        server.lifecycleEvent(
            ReplicationCallback.Type.REPLICA_STATE_CHANGED,
            new StateChangeEvent(remoteServerName, oldStatus, newStatus, reason)
        );

        // Update metrics
        metrics.recordStateChange(oldStatus, newStatus);
    }
}
```

### Why This Fixes Test Failures

**Current Problem:** During leader transitions in tests, replicas can get into inconsistent states:
- Connection fails, but replica thinks it's still ONLINE
- Reconnection succeeds, but state never updates
- Multiple threads try to transition state concurrently

**Solution:** Explicit state machine catches invalid transitions immediately:
- Logs exactly what went wrong with stack trace
- Prevents concurrent state corruption (synchronized)
- Makes test failures debuggable (clear state history in logs)

## Component 2: Exception Classification

### Exception Categories

**Category 1: Transient Network Failures**

Temporary network issues that should recover quickly:

```java
private boolean isTransientNetworkFailure(Exception e) {
    return e instanceof SocketTimeoutException ||
           e instanceof SocketException ||
           (e instanceof IOException &&
            e.getMessage() != null &&
            e.getMessage().contains("Connection reset"));
}
```

**Examples:**
- Network congestion causing timeout
- TCP connection reset by peer
- Temporary firewall blocking

**Recovery:** Quick retry with exponential backoff (3 attempts, 1s base delay)

**Category 2: Leadership Changes**

Leader is no longer the leader, need to find new leader:

```java
private boolean isLeadershipChange(Exception e) {
    return e instanceof ServerIsNotTheLeaderException ||
           (e instanceof ConnectionException &&
            e.getMessage() != null &&
            e.getMessage().contains("not the Leader")) ||
           (e instanceof ReplicationException &&
            e.getMessage() != null &&
            e.getMessage().contains("election in progress"));
}
```

**Examples:**
- Replica connected to old leader after election
- Leader stepped down due to network partition
- New election in progress

**Recovery:** Immediate leader discovery, no backoff delay

**Category 3: Protocol Errors**

Incompatible protocol versions or corrupted data:

```java
private boolean isProtocolError(Exception e) {
    return e instanceof NetworkProtocolException ||
           (e instanceof IOException &&
            e.getMessage() != null &&
            e.getMessage().contains("Protocol"));
}
```

**Examples:**
- Server version mismatch
- Corrupted message on wire
- Unexpected message type

**Recovery:** Fail fast, no retry, alert operators

**Category 4: Unknown Errors**

Any exception not matching above patterns:

```java
private boolean isUnknownError(Exception e) {
    return !isTransientNetworkFailure(e) &&
           !isLeadershipChange(e) &&
           !isProtocolError(e);
}
```

**Examples:**
- Out of memory
- Disk full
- Unexpected runtime exceptions

**Recovery:** Conservative retry with longer delays, log full stack trace

### Exception Classification Flow

```java
private void handleConnectionFailure(Exception e) {
    // Categorize the exception
    ExceptionCategory category;

    if (isTransientNetworkFailure(e)) {
        category = ExceptionCategory.TRANSIENT_NETWORK;
        metrics.transientNetworkFailures.incrementAndGet();

    } else if (isLeadershipChange(e)) {
        category = ExceptionCategory.LEADERSHIP_CHANGE;
        metrics.leadershipChanges.incrementAndGet();

    } else if (isProtocolError(e)) {
        category = ExceptionCategory.PROTOCOL_ERROR;
        metrics.protocolErrors.incrementAndGet();

    } else {
        category = ExceptionCategory.UNKNOWN;
        metrics.unknownErrors.incrementAndGet();
    }

    // Emit event with category
    server.lifecycleEvent(
        ReplicationCallback.Type.REPLICA_FAILURE_CATEGORIZED,
        new FailureEvent(remoteServerName, e, category)
    );

    // Apply category-specific recovery
    applyRecoveryStrategy(category, e);
}
```

## Component 3: Recovery Strategies

### Strategy 1: Transient Network Recovery

For network blips, quick recovery without overwhelming leader:

```java
/**
 * Handles transient network failures with exponential backoff.
 *
 * Attempts: 3
 * Base delay: 1000ms
 * Multiplier: 2.0x
 * Max delay: 8000ms
 * Total time: ~7 seconds
 */
private void recoverFromTransientFailure(Exception e) {
    transitionTo(STATUS.RECONNECTING, "Transient network failure: " + e.getMessage());

    reconnectWithBackoff(
        3,      // maxAttempts
        1000,   // baseDelayMs
        2.0,    // multiplier
        8000    // maxDelayMs
    );
}

/**
 * Exponential backoff reconnection.
 *
 * Delay sequence: 1s, 2s, 4s (capped at 8s)
 */
private void reconnectWithBackoff(int maxAttempts, long baseDelayMs,
                                   double multiplier, long maxDelayMs) {
    long delay = baseDelayMs;
    long recoveryStartTime = System.currentTimeMillis();

    for (int attempt = 1; attempt <= maxAttempts && !shutdown; attempt++) {
        try {
            // Wait before retry
            Thread.sleep(delay);

            // Emit reconnection attempt event
            server.lifecycleEvent(
                ReplicationCallback.Type.REPLICA_RECONNECT_ATTEMPT,
                new ReconnectAttemptEvent(remoteServerName, attempt, maxAttempts, delay)
            );

            // Attempt reconnection
            connect();
            startup();

            // Success!
            long recoveryTime = System.currentTimeMillis() - recoveryStartTime;
            transitionTo(STATUS.ONLINE, "Reconnection successful after " + attempt + " attempts");

            server.lifecycleEvent(
                ReplicationCallback.Type.REPLICA_RECOVERY_SUCCEEDED,
                new RecoverySuccessEvent(remoteServerName, attempt, recoveryTime)
            );

            metrics.recordSuccessfulRecovery(attempt, recoveryTime);
            metrics.consecutiveFailures.set(0);

            return;  // Success, exit retry loop

        } catch (Exception e) {
            LogManager.instance().log(this, Level.WARNING,
                "Reconnection attempt %d/%d failed for replica '%s' (next retry in %dms): %s",
                null, attempt, maxAttempts, remoteServerName, delay, e.getMessage());

            // Calculate next delay (exponential backoff, capped)
            delay = Math.min((long)(delay * multiplier), maxDelayMs);
        }
    }

    // All attempts exhausted
    long totalRecoveryTime = System.currentTimeMillis() - recoveryStartTime;
    transitionTo(STATUS.FAILED, "Max reconnection attempts exceeded (" + maxAttempts + ")");

    server.lifecycleEvent(
        ReplicationCallback.Type.REPLICA_RECOVERY_FAILED,
        new RecoveryFailedEvent(remoteServerName, maxAttempts, totalRecoveryTime)
    );

    metrics.consecutiveFailures.incrementAndGet();

    // Trigger new leader election
    server.startElection(true);
}
```

### Strategy 2: Leadership Change Recovery

When leader changes, connect to new leader immediately:

```java
/**
 * Handles leadership changes by finding and connecting to new leader.
 *
 * No exponential backoff - leadership changes are discrete events.
 */
private void recoverFromLeadershipChange(Exception e) {
    transitionTo(STATUS.RECONNECTING, "Leadership change detected: " + e.getMessage());

    server.lifecycleEvent(
        ReplicationCallback.Type.REPLICA_LEADERSHIP_CHANGE_DETECTED,
        new LeadershipChangeEvent(remoteServerName, currentLeaderName)
    );

    // Close current connection
    closeChannel();

    // Find new leader (blocks until election completes or timeout)
    String newLeaderName = server.findLeader(30_000);  // 30 second timeout

    if (newLeaderName == null) {
        transitionTo(STATUS.FAILED, "No leader found after election timeout");
        server.startElection(true);  // Trigger new election
        return;
    }

    if (newLeaderName.equals(remoteServerName)) {
        // We were trying to connect to ourselves (shouldn't happen)
        transitionTo(STATUS.FAILED, "Attempted to connect to self as replica");
        return;
    }

    // Update target leader and connect
    currentLeaderName = newLeaderName;

    try {
        connect();
        startup();
        transitionTo(STATUS.ONLINE, "Connected to new leader: " + newLeaderName);

        server.lifecycleEvent(
            ReplicationCallback.Type.REPLICA_RECOVERY_SUCCEEDED,
            new RecoverySuccessEvent(remoteServerName, 1, 0)
        );

    } catch (Exception connectException) {
        // Failed to connect to new leader - apply transient recovery
        LogManager.instance().log(this, Level.SEVERE,
            "Failed to connect to new leader '%s'", connectException, newLeaderName);
        recoverFromTransientFailure(connectException);
    }
}
```

### Strategy 3: Protocol Error Recovery

For protocol errors, fail fast and alert:

```java
/**
 * Handles protocol errors by failing immediately.
 *
 * Protocol errors are not retryable - version mismatch or data corruption.
 */
private void failFromProtocolError(Exception e) {
    transitionTo(STATUS.FAILED, "Protocol error: " + e.getMessage());

    // Log full stack trace
    LogManager.instance().log(this, Level.SEVERE,
        "PROTOCOL ERROR: Replica '%s' encountered unrecoverable protocol error. " +
        "Manual intervention required.", e, remoteServerName);

    server.lifecycleEvent(
        ReplicationCallback.Type.REPLICA_FAILED,
        new ProtocolErrorEvent(remoteServerName, e)
    );

    // Do NOT trigger election - this is a configuration/version issue
}
```

### Strategy 4: Unknown Error Recovery

For unknown errors, be conservative:

```java
/**
 * Handles unknown errors with conservative retry strategy.
 *
 * Attempts: 5
 * Base delay: 2000ms  (longer than transient)
 * Multiplier: 2.0x
 * Max delay: 30000ms  (30 seconds)
 * Total time: ~60 seconds
 */
private void recoverFromUnknownError(Exception e) {
    // Log full stack trace for investigation
    LogManager.instance().log(this, Level.SEVERE,
        "Unknown error during replication to '%s' - applying conservative recovery",
        e, remoteServerName);

    transitionTo(STATUS.RECONNECTING, "Unknown error: " + e.getClass().getSimpleName());

    reconnectWithBackoff(
        5,       // maxAttempts (more than transient)
        2000,    // baseDelayMs (longer initial delay)
        2.0,     // multiplier
        30000    // maxDelayMs (longer max delay)
    );
}
```

### Recovery Strategy Selection

```java
private void applyRecoveryStrategy(ExceptionCategory category, Exception e) {
    // Check for shutdown first
    if (Thread.currentThread().isInterrupted() || shutdown) {
        transitionTo(STATUS.DRAINING, "Shutdown requested");
        return;
    }

    switch (category) {
        case TRANSIENT_NETWORK:
            recoverFromTransientFailure(e);
            break;

        case LEADERSHIP_CHANGE:
            recoverFromLeadershipChange(e);
            break;

        case PROTOCOL_ERROR:
            failFromProtocolError(e);
            break;

        case UNKNOWN:
            recoverFromUnknownError(e);
            break;
    }
}
```

## Component 4: Observability & Monitoring

### Lifecycle Events

New event types for monitoring:

```java
public interface ReplicationCallback {
    enum Type {
        // Existing events
        REPLICA_MSG_RECEIVED,
        REPLICA_HOT_RESYNC,
        REPLICA_FULL_RESYNC,

        // New events for Phase 2
        REPLICA_STATE_CHANGED,              // State transition occurred
        REPLICA_FAILURE_CATEGORIZED,        // Exception categorized
        REPLICA_RECONNECT_ATTEMPT,          // Reconnection attempt starting
        REPLICA_RECOVERY_SUCCEEDED,         // Recovery completed successfully
        REPLICA_RECOVERY_FAILED,            // Recovery failed after max attempts
        REPLICA_LEADERSHIP_CHANGE_DETECTED, // Leadership change detected
        REPLICA_FAILED                      // Transitioned to FAILED state
    }
}
```

### Event Payloads

```java
/**
 * State change event payload.
 */
public class StateChangeEvent {
    private final String replicaName;
    private final Leader2ReplicaNetworkExecutor.STATUS oldStatus;
    private final Leader2ReplicaNetworkExecutor.STATUS newStatus;
    private final String reason;
    private final long timestampMs;
}

/**
 * Failure categorization event payload.
 */
public class FailureEvent {
    private final String replicaName;
    private final Exception exception;
    private final ExceptionCategory category;
    private final long timestampMs;
}

/**
 * Reconnection attempt event payload.
 */
public class ReconnectAttemptEvent {
    private final String replicaName;
    private final int attemptNumber;
    private final int maxAttempts;
    private final long delayMs;
    private final long timestampMs;
}

/**
 * Recovery success event payload.
 */
public class RecoverySuccessEvent {
    private final String replicaName;
    private final int totalAttempts;
    private final long totalRecoveryTimeMs;
    private final long timestampMs;
}

/**
 * Recovery failure event payload.
 */
public class RecoveryFailedEvent {
    private final String replicaName;
    private final int totalAttempts;
    private final long totalRecoveryTimeMs;
    private final ExceptionCategory lastFailureCategory;
    private final long timestampMs;
}
```

### Metrics

```java
/**
 * Per-replica connection metrics.
 */
public class ReplicaConnectionMetrics {
    // Connection health
    private final AtomicLong totalReconnections = new AtomicLong(0);
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private final AtomicLong lastSuccessfulMessageTime = new AtomicLong(0);
    private volatile Leader2ReplicaNetworkExecutor.STATUS currentStatus;

    // Failure categorization counts
    private final AtomicLong transientNetworkFailures = new AtomicLong(0);
    private final AtomicLong leadershipChanges = new AtomicLong(0);
    private final AtomicLong protocolErrors = new AtomicLong(0);
    private final AtomicLong unknownErrors = new AtomicLong(0);

    // Recovery performance
    private final AtomicLong totalRecoveryTimeMs = new AtomicLong(0);
    private final AtomicLong fastestRecoveryMs = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong slowestRecoveryMs = new AtomicLong(0);
    private final AtomicLong successfulRecoveries = new AtomicLong(0);
    private final AtomicLong failedRecoveries = new AtomicLong(0);

    // State transition history (last 10 transitions)
    private final ConcurrentLinkedDeque<StateTransition> recentTransitions =
        new ConcurrentLinkedDeque<>();

    public void recordStateChange(STATUS oldStatus, STATUS newStatus) {
        currentStatus = newStatus;

        StateTransition transition = new StateTransition(
            oldStatus, newStatus, System.currentTimeMillis()
        );

        recentTransitions.addFirst(transition);
        if (recentTransitions.size() > 10) {
            recentTransitions.removeLast();
        }
    }

    public void recordSuccessfulRecovery(int attempts, long recoveryTimeMs) {
        successfulRecoveries.incrementAndGet();
        totalRecoveryTimeMs.addAndGet(recoveryTimeMs);

        fastestRecoveryMs.updateAndGet(current ->
            Math.min(current, recoveryTimeMs));
        slowestRecoveryMs.updateAndGet(current ->
            Math.max(current, recoveryTimeMs));
    }
}
```

### Health API Endpoint

New HTTP endpoint: `GET /api/v1/server/ha/cluster-health`

**Response Schema:**
```json
{
  "status": "HEALTHY",
  "leader": {
    "name": "ArcadeDB_0",
    "epoch": 42,
    "electionAgeMs": 3600000
  },
  "quorumAvailable": true,
  "replicas": [
    {
      "name": "ArcadeDB_1",
      "status": "ONLINE",
      "replicationLagMs": 45,
      "queueSize": 0,
      "lastMessageAgeMs": 120,
      "consecutiveFailures": 0,
      "metrics": {
        "totalReconnections": 3,
        "transientNetworkFailures": 2,
        "leadershipChanges": 1,
        "protocolErrors": 0,
        "unknownErrors": 0,
        "avgRecoveryTimeMs": 1234,
        "fastestRecoveryMs": 890,
        "slowestRecoveryMs": 2100
      },
      "recentTransitions": [
        {
          "from": "RECONNECTING",
          "to": "ONLINE",
          "timestampMs": 1705507200000
        },
        {
          "from": "ONLINE",
          "to": "RECONNECTING",
          "timestampMs": 1705507198000
        }
      ]
    },
    {
      "name": "ArcadeDB_2",
      "status": "RECONNECTING",
      "replicationLagMs": 5000,
      "queueSize": 150,
      "lastMessageAgeMs": 5000,
      "consecutiveFailures": 2,
      "metrics": {
        "totalReconnections": 5,
        "transientNetworkFailures": 3,
        "leadershipChanges": 2,
        "protocolErrors": 0,
        "unknownErrors": 0
      }
    }
  ]
}
```

**Health Status Levels:**
- `HEALTHY` - All replicas ONLINE, lag < 1s, quorum available
- `DEGRADED` - Some replicas RECONNECTING, quorum available
- `CRITICAL` - Some replicas FAILED, or quorum unavailable
- `DOWN` - Leader not available

## Feature Flag Configuration

### GlobalConfiguration Settings

```java
/**
 * Enable enhanced reconnection logic with exception classification.
 *
 * When true: Uses new state machine and intelligent recovery strategies
 * When false: Uses legacy reconnection logic
 *
 * Default: false (legacy behavior)
 */
public static final Setting HA_ENHANCED_RECONNECTION =
    new Setting("ha.enhancedReconnection", false, SettingType.BOOLEAN);

/**
 * Transient failure retry attempts.
 *
 * Default: 3 attempts
 */
public static final Setting HA_TRANSIENT_FAILURE_MAX_ATTEMPTS =
    new Setting("ha.transientFailure.maxAttempts", 3, SettingType.INTEGER);

/**
 * Transient failure base delay in milliseconds.
 *
 * Default: 1000ms (1 second)
 */
public static final Setting HA_TRANSIENT_FAILURE_BASE_DELAY_MS =
    new Setting("ha.transientFailure.baseDelayMs", 1000L, SettingType.LONG);

/**
 * Unknown error retry attempts.
 *
 * Default: 5 attempts
 */
public static final Setting HA_UNKNOWN_ERROR_MAX_ATTEMPTS =
    new Setting("ha.unknownError.maxAttempts", 5, SettingType.INTEGER);

/**
 * Unknown error base delay in milliseconds.
 *
 * Default: 2000ms (2 seconds)
 */
public static final Setting HA_UNKNOWN_ERROR_BASE_DELAY_MS =
    new Setting("ha.unknownError.baseDelayMs", 2000L, SettingType.LONG);
```

### Usage in Code

```java
public void reconnect(final Exception e) {
    if (GlobalConfiguration.HA_ENHANCED_RECONNECTION.getValueAsBoolean()) {
        // New enhanced reconnection logic
        handleConnectionFailure(e);
    } else {
        // Legacy reconnection logic (existing code)
        reconnectLegacy(e);
    }
}
```

## Testing Strategy

### Unit Tests

**Test: State Machine Transitions**
- Valid transitions succeed
- Invalid transitions throw IllegalStateException
- Concurrent transitions are serialized
- State history is recorded

**Test: Exception Classification**
- SocketTimeoutException → TRANSIENT_NETWORK
- ServerIsNotTheLeaderException → LEADERSHIP_CHANGE
- NetworkProtocolException → PROTOCOL_ERROR
- Generic IOException → UNKNOWN

**Test: Recovery Strategies**
- Transient failure retries 3 times with backoff
- Leadership change connects to new leader immediately
- Protocol error fails fast without retry
- Unknown error retries 5 times with longer delays

### Integration Tests

**Test: Leader Failover Recovery**
```java
@Test
void testLeaderFailoverWithEnhancedReconnection() {
    // Enable feature flag
    GlobalConfiguration.HA_ENHANCED_RECONNECTION.setValue(true);

    // Start 3-server cluster
    startCluster(3);

    // Write data to leader
    writeData(1000);

    // Kill leader
    stopServer(0);

    // Verify:
    // 1. Replicas detect leadership change (not transient failure)
    // 2. Replicas connect to new leader immediately (no backoff delay)
    // 3. All data replicated correctly
    // 4. Metrics show LEADERSHIP_CHANGE category

    assertLeadershipChangeHandled();
    assertNoUnnecessaryBackoff();
    assertDataIntegrity();
}
```

**Test: Network Partition Recovery**
```java
@Test
void testNetworkPartitionRecovery() {
    GlobalConfiguration.HA_ENHANCED_RECONNECTION.setValue(true);

    startCluster(3);

    // Simulate network partition (block traffic to replica 2)
    networkPartition(2);

    // Verify:
    // 1. Replica 2 transitions to RECONNECTING
    // 2. Exponential backoff applied (1s, 2s, 4s)
    // 3. After partition heals, replica recovers
    // 4. Metrics show TRANSIENT_NETWORK failures

    assertTransientFailureHandling();
    assertExponentialBackoff();
    assertEventualRecovery();
}
```

**Test: Protocol Error Handling**
```java
@Test
void testProtocolErrorFailsFast() {
    GlobalConfiguration.HA_ENHANCED_RECONNECTION.setValue(true);

    startCluster(3);

    // Inject protocol error
    injectProtocolError(1);

    // Verify:
    // 1. Replica 1 transitions to FAILED immediately
    // 2. No retry attempts
    // 3. PROTOCOL_ERROR metric incremented
    // 4. Lifecycle event emitted

    assertFailedStatus();
    assertNoRetries();
    assertProtocolErrorLogged();
}
```

### Chaos Testing

Run 100 iterations of chaos scenarios:
- Random server kills
- Network partitions
- Leader elections
- Protocol version mismatches

**Success Criteria:**
- 90%+ of scenarios recover successfully
- Average recovery time < 10 seconds
- No stuck RECONNECTING states > 60 seconds
- All failures properly categorized in metrics

## Rollout Plan

### Phase 1: Deploy with Flag OFF (Week 1)

**Objective:** Deploy code to production with feature flag disabled

**Actions:**
1. Merge code to main branch
2. Deploy to staging environment
3. Run existing test suite (100 iterations)
4. Deploy to production with `HA_ENHANCED_RECONNECTION=false`

**Success Criteria:**
- All existing tests pass
- No regressions in production
- Code paths verified working with flag OFF

### Phase 2: Enable in Test Environment (Week 2)

**Objective:** Enable feature flag in non-production

**Actions:**
1. Enable `HA_ENHANCED_RECONNECTION=true` in staging
2. Monitor metrics for 24 hours
3. Run chaos tests (100 iterations)
4. Compare metrics: legacy vs. enhanced

**Success Criteria:**
- Test pass rate improves from 61% to 80%+
- Leader failover tests pass consistently
- Recovery times < 10 seconds average
- No new errors introduced

### Phase 3: Gradual Production Rollout (Week 3)

**Objective:** Enable feature flag in production clusters

**Actions:**
1. Enable in 10% of production clusters
2. Monitor for 48 hours
3. If stable, increase to 50%
4. Monitor for 48 hours
5. If stable, enable for 100%

**Success Criteria:**
- No increase in production errors
- Reduced MTTR (Mean Time To Recovery)
- Reduced false alarms from transient failures
- Positive metrics on leadership change handling

### Phase 4: Make Default & Cleanup (Week 4)

**Objective:** Make enhanced reconnection the default behavior

**Actions:**
1. Change default: `HA_ENHANCED_RECONNECTION=true`
2. Monitor for 1 week
3. Remove feature flag code
4. Remove legacy reconnection logic
5. Update documentation

**Success Criteria:**
- No issues with default enabled
- Legacy code safely removed
- Documentation updated

## Validation Criteria

### Test Pass Rate

**Before:**
- Baseline: 61% (17/28 tests passing)
- Failing: ReplicationServerLeaderDownIT, ReplicationServerLeaderChanges3TimesIT

**Target:**
- Phase 2 completion: 80%+ (23+/28 tests passing)
- Leader transition tests must pass

### Recovery Performance

**Metrics to Track:**
- Average recovery time from network partition: < 10 seconds
- Average recovery time from leadership change: < 5 seconds
- False positive rate (unnecessary retries): < 5%
- Protocol error detection rate: 100%

### Production Metrics

**Before enabling in production:**
- Establish baseline MTTR (Mean Time To Recovery)
- Establish baseline false alarm rate
- Establish baseline leadership change frequency

**After enabling in production:**
- MTTR reduced by 30%+
- False alarms reduced by 50%+
- Zero increase in unhandled errors

## Risk Mitigation

### Risk 1: New Code Introduces Bugs

**Mitigation:**
- Feature flag allows immediate rollback
- Gradual rollout (10% → 50% → 100%)
- Comprehensive test coverage before production
- Monitoring dashboards for new metrics

### Risk 2: State Machine Deadlocks

**Mitigation:**
- All state transitions are synchronized (no concurrent modifications)
- State machine unit tests verify thread safety
- Timeout protection on all blocking operations
- Dead thread detection in monitoring

### Risk 3: Classification is Wrong

**Mitigation:**
- Conservative classification (unknown errors get longer retry)
- Comprehensive exception mapping in unit tests
- Ability to adjust classification via config
- Logs include full exception details for investigation

### Risk 4: Performance Degradation

**Mitigation:**
- State machine operations are O(1)
- Metrics use lock-free atomic counters
- Event emission is asynchronous
- Performance benchmarks before/after

## Success Metrics

### Phase 2 Complete When:

1. ✅ Test pass rate ≥ 80% (up from 61%)
2. ✅ Leader transition tests passing consistently
3. ✅ Health API returning cluster status
4. ✅ All 4 exception categories tracked in metrics
5. ✅ Recovery strategies implemented and tested
6. ✅ Feature flag working (ON/OFF tested)
7. ✅ Production deployment successful
8. ✅ Documentation updated

### Long-Term Success (After 1 Month):

1. ✅ Test pass rate ≥ 90%
2. ✅ Production MTTR reduced 30%+
3. ✅ False alarms reduced 50%+
4. ✅ Zero protocol errors in production
5. ✅ Leadership changes handled < 5s average
6. ✅ Legacy code removed

## Next Steps After Phase 2

**Phase 3: Advanced Resilience (Weeks 8-12)**
- Circuit breakers for slow replicas
- Automatic replica removal/rejoin
- Background consistency checks
- Split-brain prevention

**Phase 4: Production Hardening (Ongoing)**
- Load balancing across replicas
- Read-only replica support
- Multi-datacenter replication
- Disaster recovery automation

## References

- **Original Design Doc:** `docs/plans/2026-01-13-ha-reliability-improvements-design.md`
- **Baseline Test Results:** `docs/plans/2026-01-17-ha-baseline-test-results.md`
- **Phase 1 Summary:** `docs/plans/2026-01-17-ha-test-infrastructure-summary.md`

---

**Status:** ✅ Design Complete
**Next Step:** Create implementation plan
**Estimated Effort:** 2-3 weeks (development + testing + rollout)
