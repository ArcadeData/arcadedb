# Enhanced Reconnection with Exception Classification

**Status:** Phase 2 Implementation Complete
**Version:** Added in v26.1.1
**Feature Flag:** `HA_ENHANCED_RECONNECTION`

## Overview

Enhanced reconnection adds intelligent exception classification and category-specific recovery strategies to improve HA reliability during network failures and leader transitions.

## Features

**4 Exception Categories:**
1. **Transient Network** - Temporary timeouts, connection resets
2. **Leadership Change** - Leader elections, failovers
3. **Protocol Error** - Version mismatches, corrupted data
4. **Unknown** - Uncategorized errors

**Category-Specific Recovery:**
- Transient: Quick retry (3 attempts, 1s base delay, exponential backoff)
- Leadership: Immediate leader discovery (no backoff)
- Protocol: Fail fast (no retry, alert operators)
- Unknown: Conservative retry (5 attempts, 2s base delay)

**Observability:**
- 7 new lifecycle events (state changes, recovery attempts, failures)
- Per-replica metrics (failure counts by category, recovery times)
- Health API endpoint: `/api/v1/cluster/health`

## Configuration

### Enable Enhanced Reconnection

```properties
# Default: false (legacy behavior)
arcadedb.ha.enhancedReconnection=true
```

### Tuning Parameters

```properties
# Transient failure retry
arcadedb.ha.transientFailure.maxAttempts=3
arcadedb.ha.transientFailure.baseDelayMs=1000

# Unknown error retry
arcadedb.ha.unknownError.maxAttempts=5
arcadedb.ha.unknownError.baseDelayMs=2000
```

## Monitoring

### Health API

```bash
curl http://localhost:2480/api/v1/cluster/health
```

**Response:**
```json
{
  "status": "HEALTHY",
  "serverName": "ArcadeDB_0",
  "clusterName": "arcade",
  "isLeader": true,
  "leaderName": "ArcadeDB_0",
  "electionStatus": "DONE",
  "onlineReplicas": 2,
  "configuredServers": 3,
  "replicas": {}
}
```

### Metrics

Access via programmatic API:
```java
Leader2ReplicaNetworkExecutor executor = ...;
ReplicaConnectionMetrics metrics = executor.getMetrics();

long transientFailures = metrics.transientNetworkFailuresCounter().get();
long leadershipChanges = metrics.leadershipChangesCounter().get();
long protocolErrors = metrics.protocolErrorsCounter().get();
long unknownErrors = metrics.unknownErrorsCounter().get();
long failedRecoveries = metrics.failedRecoveriesCounter().get();
long successfulRecoveries = metrics.successfulRecoveriesCounter().get();
```

## Rollout Strategy

1. **Deploy with flag OFF** (default)
2. **Enable in test environment**, monitor 24 hours
3. **Enable in 10% production**, monitor 48 hours
4. **Enable in 50% production**, monitor 48 hours
5. **Enable in 100% production**
6. **After 2 weeks stable**, make default `true`

## Troubleshooting

### High Transient Failure Count
- Check network quality between nodes
- May indicate infrastructure issues

### High Leadership Change Count
- Check cluster stability
- May indicate leader instability or network partitions

### Protocol Errors
- Check server versions match
- Indicates version mismatch or corruption

### High Unknown Error Count
- Review server logs for uncategorized exceptions
- May need to add new exception categories

## References

- Design: `docs/plans/2026-01-17-phase2-enhanced-reconnection-design.md`
- Implementation Plan: `docs/plans/2026-01-17-phase2-enhanced-reconnection-impl-plan.md`
- Tests: `server/src/test/java/com/arcadedb/server/ha/EnhancedReconnectionIT.java`
