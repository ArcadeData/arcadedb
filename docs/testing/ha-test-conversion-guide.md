# HA Test Conversion Guide - Phase 1

## Overview

This guide documents the patterns for converting HA integration tests from timing-based waits (Thread.sleep) to condition-based Awaitility patterns. The goal is to achieve 95%+ test reliability by eliminating timing assumptions.

**Target**: Convert all ~26 HA integration tests to use centralized stabilization helpers and Awaitility patterns.

## What Was Completed

### Core Infrastructure (Tasks 1-6)

**Task 1: HA Test Helper Methods** (Commit: eac8a23ec)
- Added `waitForClusterStable(int)` to BaseGraphServerTest
- Added `waitForServerShutdown(ArcadeDBServer, int)` to BaseGraphServerTest
- Added `waitForServerStartup(ArcadeDBServer, int)` to BaseGraphServerTest
- All methods use Awaitility with HATestTimeouts constants

**Task 2: SimpleReplicationServerIT** (Commit: 56130c7e3)
- Created reference implementation showing all Phase 1 patterns
- Template for other test conversions

**Task 3: ReplicationServerIT Base Class** (Commit: 69c10514e)
- **Critical**: Converted base class used by ~15 subclass tests
- All subclasses automatically inherit `waitForClusterStable()` usage

**Task 4: HARandomCrashIT** (Commit: e9bfb769a)
- Replaced manual 3-phase stabilization with `waitForClusterStable()`
- Replaced `Thread.sleep(5000)` with condition-based count verification

**Task 5: HASplitBrainIT** (Commit: c677623aa)
- Replaced custom queue drain with `waitForClusterStable()`
- Replaced `Thread.sleep(10000)` with condition-based count verification

**Task 6: ReplicationChangeSchemaIT** (Commit: c7a02f248)
- Removed custom `waitForReplicationQueueDrain()` method
- Uses centralized `waitForClusterStable()`

## Conversion Patterns

### Pattern 1: Add @Timeout Annotation

**Before:**
```java
@Test
public void myTest() throws Exception {
  // test code
}
```

**After:**
```java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void myTest() throws Exception {
  // test code
}
```

**Rationale:**
- Simple tests (1-2 servers, <1000 ops): 5 minutes
- Complex tests (3+ servers, >1000 ops): 10-15 minutes
- Chaos tests (random crashes): 15-20 minutes

### Pattern 2: Replace Thread.sleep with Condition Waits

**Before:**
```java
// Create data
db.transaction(() -> { /* ... */ });

// Wait for replication
Thread.sleep(5000);

// Verify
for (int i = 0; i < serverCount; i++) {
  // check data
}
```

**After:**
```java
// Create data
db.transaction(() -> { /* ... */ });

// Wait for cluster stability
waitForClusterStable(getServerCount());

// Verify
for (int i = 0; i < serverCount; i++) {
  // check data
}
```

### Pattern 3: Replace Manual Queue Drain Loops

**Before:**
```java
for (int i = 0; i < getServerCount(); i++) {
  waitForReplicationIsCompleted(i);
}
```

**After:**
```java
waitForClusterStable(getServerCount());
```

**Why:** `waitForClusterStable()` performs 3-phase check:
1. All servers ONLINE
2. All replication queues empty
3. All replicas connected to leader

### Pattern 4: Wait for Data Consistency

**Before:**
```java
Thread.sleep(10000);  // Wait for data to settle
checkDatabasesAreIdentical();
```

**After:**
```java
// Wait for all servers to report consistent counts
await("data consistency")
    .atMost(Duration.ofSeconds(30))
    .pollInterval(Duration.ofSeconds(1))
    .until(() -> {
      long expectedCount = /* expected count */;
      for (int i = 0; i < getServerCount(); i++) {
        Database serverDb = getServerDatabase(i, getDatabaseName());
        serverDb.begin();
        try {
          long count = serverDb.countType(TYPE_NAME, true);
          if (count != expectedCount) {
            return false;
          }
        } finally {
          serverDb.rollback();
        }
      }
      return true;
    });

checkDatabasesAreIdentical();
```

### Pattern 5: Server Lifecycle Operations

**Before:**
```java
stopServer(3);
Thread.sleep(5000);
startServer(3);
Thread.sleep(10000);
```

**After:**
```java
ArcadeDBServer server = getServer(3);
stopServer(3);
waitForServerShutdown(server, 3);

startServer(3);
server = getServer(3);
waitForServerStartup(server, 3);

// Wait for cluster to stabilize after topology change
waitForClusterStable(getServerCount());
```

## Conversion Checklist

For each test file, follow this checklist:

### 1. Add Required Imports

```java
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import java.time.Duration;
```

### 2. Add @Timeout Annotations

- [ ] Find all `@Test` methods
- [ ] Add `@Timeout(value = X, unit = TimeUnit.MINUTES)` above each
- [ ] Choose appropriate timeout based on test complexity

### 3. Replace Timing Assumptions

- [ ] Search for `Thread.sleep(`
- [ ] Search for `CodeUtils.sleep(`
- [ ] Replace each with appropriate Awaitility pattern
- [ ] Use `waitForClusterStable()` after server operations

### 4. Simplify Custom Wait Logic

- [ ] Look for custom queue drain loops
- [ ] Replace with `waitForClusterStable(getServerCount())`
- [ ] Remove duplicate stabilization code

### 5. Test Reliability

- [ ] Run test once: `mvn test -Dtest=MyTestIT -pl server`
- [ ] Run test 10 times: `for i in {1..10}; do mvn test -Dtest=MyTestIT -pl server -q || break; done`
- [ ] Target: 9/10 or 10/10 passes (90%+ reliability)

### 6. Commit

```bash
git add server/src/test/java/com/arcadedb/server/ha/MyTestIT.java
git commit -m "test: convert MyTestIT to use Awaitility patterns

- Add @Timeout(X minutes) annotation
- Replace Thread.sleep() with waitForClusterStable()
- [specific changes for this test]

Verified: 10/10 successful runs

Part of Phase 1: HA Test Infrastructure Improvements

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

## Tests Remaining for Conversion

### Inheriting from ReplicationServerIT (Benefit from Task 3)

These tests inherit `testReplication()` which already uses `waitForClusterStable()`:

- ReplicationServerQuorumMajorityIT
- ReplicationServerQuorumNoneIT
- ReplicationServerQuorumAllIT
- ReplicationServerLeaderDownIT
- ReplicationServerLeaderChanges3TimesIT
- ReplicationServerReplicaHotResyncIT

**Action:** Review for additional test methods beyond `replication()` that may need conversion.

### Standalone Tests (Need Full Review)

- ServerDatabaseAlignIT
- HTTP2ServersIT
- HTTPGraphConcurrentIT
- IndexOperations3ServersIT
- ReplicationServerWriteAgainstReplicaIT
- ServerDatabaseBackupIT
- HAConfigurationIT
- And ~13 more

**Action:** Full conversion following checklist above.

## Common Pitfalls

### 1. Variable Shadowing

**Wrong:**
```java
Database db = getServerDatabase(0, getName());
await().until(() -> {
  Database db = getServerDatabase(0, getName());  // ❌ Shadows outer variable
  // ...
});
```

**Right:**
```java
Database db = getServerDatabase(0, getName());
await().until(() -> {
  Database serverDb = getServerDatabase(0, getName());  // ✅ Different name
  // ...
});
```

### 2. Not Closing Transactions in Lambdas

**Wrong:**
```java
await().until(() -> {
  Database db = getServerDatabase(i, getName());
  db.begin();
  long count = db.countType(TYPE, true);
  return count == expected;
  // ❌ Transaction never closed
});
```

**Right:**
```java
await().until(() -> {
  Database db = getServerDatabase(i, getName());
  db.begin();
  try {
    long count = db.countType(TYPE, true);
    return count == expected;
  } finally {
    db.rollback();  // ✅ Always close
  }
});
```

### 3. Magic Numbers Instead of Constants

**Wrong:**
```java
await().atMost(Duration.ofSeconds(60))  // ❌ Magic number
```

**Right:**
```java
await().atMost(HATestTimeouts.CLUSTER_STABILIZATION_TIMEOUT)  // ✅ Named constant
```

## Available Timeout Constants

From `HATestTimeouts.java`:

```java
SCHEMA_PROPAGATION_TIMEOUT = Duration.ofSeconds(10)
CLUSTER_STABILIZATION_TIMEOUT = Duration.ofSeconds(60)
SERVER_SHUTDOWN_TIMEOUT = Duration.ofSeconds(90)
SERVER_STARTUP_TIMEOUT = Duration.ofSeconds(90)
CHAOS_TRANSACTION_TIMEOUT = Duration.ofSeconds(300)
REPLICA_RECONNECTION_TIMEOUT = Duration.ofSeconds(30)
AWAITILITY_POLL_INTERVAL = Duration.ofMillis(500)
AWAITILITY_POLL_INTERVAL_LONG = Duration.ofSeconds(2)
```

## Success Metrics

### Phase 1 Goals

- ✅ Zero `Thread.sleep()` or `CodeUtils.sleep()` in converted tests
- ✅ All test methods have `@Timeout` annotations
- ✅ 95%+ pass rate on converted tests (9/10 or 10/10 runs)
- ✅ Centralized stabilization logic in BaseGraphServerTest
- ⏳ All 26 HA tests converted and validated

### Current Status

- **Completed**: 6/26 tests (23%)
- **Base Class Impact**: ~15 additional tests benefit from ReplicationServerIT conversion
- **Remaining**: ~5-10 standalone tests need full conversion

## References

- **Design Document**: `docs/plans/2026-01-13-ha-reliability-improvements-design.md`
- **Implementation Plan**: `docs/plans/2026-01-13-ha-test-infrastructure-phase1.md`
- **HATestTimeouts**: `server/src/test/java/com/arcadedb/server/ha/HATestTimeouts.java`
- **BaseGraphServerTest**: `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`

## Example: Complete Conversion

See `SimpleReplicationServerIT.java` (Commit: 56130c7e3) for a complete reference implementation demonstrating all Phase 1 patterns.
