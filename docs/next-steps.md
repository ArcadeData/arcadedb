# Next Steps for HA Test Infrastructure

## Branch Status

**Branch:** `feature/2043-ha-test`
**Commits:** 7 commits ready to push
**Status:** All changes committed, ready for CI testing

## What's Been Done

### Phase 3 Connection Resilience (Commits 1-1)
- Connection retry with exponential backoff
- EOFException handling during handshake
- Leader connection acceptance improvements
- Connection health monitoring
- Test timeout adjustments

### Test Infrastructure Improvements (Commits 2-7)
1. HATestHelpers utility class (centralized test helpers)
2. @Timeout annotations on 8 HA tests
3. SimpleReplicationServerIT conversion (pattern example)
4. ServerDatabaseSqlScriptIT conversion
5. BaseGraphServerTest delegation to HATestHelpers
6. Performance optimization (single-phase checks, faster polling)
7. Documentation (plan + status tracking)

## Current Issue

**Root Cause Identified:** Phase 3 exponential backoff is delaying cluster startup in tests.

When tests start, servers try to connect to non-existent leaders with exponential backoff:
- Attempt 1: immediate
- Attempt 2: +200ms
- Attempt 3: +400ms
- Attempt 4: +800ms
- Attempt 5: +1600ms
- Attempt 6: +3200ms
- Total: ~6 seconds before giving up and electing own leader

With 3 servers starting simultaneously, connection attempts overlap, causing significant delay.

## Immediate Actions

### 1. Push to CI
```bash
git push origin feature/2043-ha-test
```

**Purpose:** See if issue is consistent across environments or local-only.

### 2. Monitor CI Results

Watch for:
- Which tests fail (setup vs execution)
- Timing patterns (consistent vs intermittent)
- Resource usage (CPU, memory)

## Short-Term Fixes (Choose One)

### Option A: Test-Specific Connection Config (Recommended)
```java
// In test setup
GlobalConfiguration.HA_CONNECTION_RETRY_DELAY.setValue(100);  // Faster retry
GlobalConfiguration.HA_CONNECTION_RETRY_MAX_DELAY.setValue(500);  // Lower cap
```

**Pros:**
- Targets root cause
- Doesn't affect production behavior
- Easy to implement

**Implementation:** Add to BaseGraphServerTest.setTestConfiguration()

### Option B: Skip Initial Connection Attempts
```java
// In test startup
if (System.getProperty("arcadedb.test") != null) {
  // Skip connection retry, go straight to leader election
}
```

**Pros:**
- Fastest test startup
- Clean separation of test vs production behavior

**Cons:**
- Doesn't test connection retry logic
- Requires code changes in production classes

### Option C: Increase Timeouts Further
```java
CLUSTER_STABILIZATION_TIMEOUT = Duration.ofSeconds(180);
REPLICA_RECONNECTION_TIMEOUT = Duration.ofSeconds(120);
```

**Pros:**
- Simplest fix
- No code changes

**Cons:**
- Makes tests even slower
- Masks underlying issue

## Long-Term Improvements

### 1. Parallel Server Startup
Instead of sequential startup, start all servers in parallel and wait for cluster formation.

### 2. Test-Aware Connection Strategy
Add a test mode that:
- Uses localhost-only connections (no DNS)
- Skips retry on first attempt if port not listening
- Falls back to leader election immediately

### 3. Connection Pool Warming
Pre-establish connections before running tests to avoid cold-start delays.

### 4. Test Isolation Improvements
- Ensure complete cleanup between tests
- Use different port ranges per test class
- Add delay between test classes

## Recommended Path Forward

**Phase 1: Verify (Today)**
1. Push to CI
2. Review test results
3. Identify failure patterns

**Phase 2: Quick Fix (This Week)**
1. Implement Option A (test-specific connection config)
2. Run full HA test suite
3. Measure pass rate improvement

**Phase 3: Optimization (Next Week)**
1. Implement parallel server startup
2. Add test-aware connection strategy
3. Complete Task 6 (10 consecutive runs >90% pass rate)

**Phase 4: Rollout (Following Week)**
1. Migrate remaining HA tests to HATestHelpers
2. Document patterns for future tests
3. Add CI job for HA test reliability monitoring

## Success Criteria

- **Immediate:** Tests pass on CI
- **Short-term:** <5% test failure rate
- **Long-term:** >90% pass rate across 10 consecutive full suite runs
- **Performance:** Average test time <2 minutes (back to baseline)

## Files to Watch

Key files that may need adjustments:
- `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`
- `server/src/test/java/com/arcadedb/server/ha/HATestHelpers.java`
- `server/src/test/java/com/arcadedb/server/ha/HATestTimeouts.java`
- `server/src/main/java/com/arcadedb/server/ha/Replica2LeaderNetworkExecutor.java`

## Questions to Answer

1. **Do tests pass on CI?** (Different environment, resources)
2. **Is the issue reproducible?** (Run same tests 3 times locally)
3. **Which approach to fix?** (A, B, or C above)
4. **Should we revert anything?** (If issues persist)

## Contact Points

All changes are well-documented with:
- Commit messages explaining what and why
- Code comments in HATestHelpers
- This summary document
- Status tracking in docs/test-infrastructure-status.md
