# HA Test Infrastructure - Current Status

## Commits on feature/2043-ha-test

1. `bf42e2179` - docs: add HA test infrastructure improvements plan
2. `2a535ce53` - test: update BaseGraphServerTest to delegate to HATestHelpers
3. `129b849b6` - test: convert ServerDatabaseSqlScriptIT to use HATestHelpers
4. `af4eeff38` - test: convert SimpleReplicationServerIT to use HATestHelpers
5. `9745165da` - test: add @Timeout annotations to HA tests
6. `34e099505` - test: add HATestHelpers utility class for HA tests

## Current Issue

**Problem:** Tests failing during cluster setup with:
```
Cluster failed to stabilize: expected 3 servers, only 1 connected
```

**Root Cause:** Replicas not connecting to leader within 60s REPLICA_RECONNECTION_TIMEOUT during test initialization.

**Why This Happens:**
1. Phase 3 connection retry with exponential backoff may delay initial connections
2. When running multiple tests together, cleanup timing issues
3. HATestHelpers.waitForClusterStable() has more phases than original implementation

## Observed Behavior

- **Individual tests pass:** SimpleReplicationServerIT runs successfully alone (183.6s)
- **Multiple tests fail:** Running SimpleReplicationServerIT + ServerDatabaseSqlScriptIT fails in setup
- **Slower execution:** Tests take longer than before (3+ minutes vs ~2 minutes previously)

## Recommended Fixes

### Option 1: Increase Test Timeouts (Quick Fix)
- Increase REPLICA_RECONNECTION_TIMEOUT from 60s to 120s
- Increase CLUSTER_STABILIZATION_TIMEOUT to 180s
- **Pros:** Simple, might fix immediate issue
- **Cons:** Masks underlying problem, makes tests even slower

### Option 2: Optimize Connection Retry for Tests (Better)
- Add GlobalConfiguration option to disable exponential backoff in tests
- Use faster retry intervals during test setup
- **Pros:** Faster test execution, targets root cause
- **Cons:** Requires modifying connection retry logic

### Option 3: Improve HATestHelpers Performance (Best Long-term)
- Add short-circuit logic to skip phases when conditions already met
- Reduce poll intervals from 1s to 200ms for faster detection
- Optimize waitForClusterStable to check all phases in parallel
- **Pros:** Better performance, maintains reliability
- **Cons:** More complex implementation

### Option 4: Revert and Refine (Conservative)
- Revert BaseGraphServerTest delegation changes
- Keep HATestHelpers for new tests only
- Gradually migrate tests after performance tuning
- **Pros:** Keeps existing tests working
- **Cons:** Delays benefit of centralized test infrastructure

## Next Steps

1. **Immediate:** Push to CI to see if issue is local or consistent
2. **Short-term:** Implement Option 2 or 3 to fix performance
3. **Long-term:** Complete Task 6 (verify full test suite reliability)

## Performance Comparison

| Test | Before | After | Change |
|------|--------|-------|--------|
| SimpleReplicationServerIT | ~120s | 183.6s | +53% slower |
| ServerDatabaseSqlScriptIT | ~120s | 183.1s | +53% slower |
| Both together | ~4 min | FAIL | Test setup timeout |

## Files Changed

- `server/src/test/java/com/arcadedb/server/ha/HATestHelpers.java` (NEW)
- `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java` (MODIFIED)
- `server/src/test/java/com/arcadedb/server/ha/SimpleReplicationServerIT.java` (MODIFIED)
- `server/src/test/java/com/arcadedb/server/ha/ServerDatabaseSqlScriptIT.java` (MODIFIED)
- 8 test files with @Timeout annotations added
