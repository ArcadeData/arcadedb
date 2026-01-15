# Test-Specific Connection Configuration Fix

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix cluster stabilization timeouts in tests by configuring faster connection retry delays

**Architecture:** Override `setTestConfiguration()` in `BaseGraphServerTest` to set test-friendly connection retry parameters that reduce cluster startup time without affecting production behavior.

**Tech Stack:** Java, JUnit 5, GlobalConfiguration

---

## Background

CI tests are failing with "Cluster failed to stabilize" errors due to exponential backoff connection retry delays. During test startup, when servers attempt to connect to not-yet-started leaders, the default retry configuration causes significant delays:

**Production defaults (too slow for tests):**
- Base delay: 200ms
- Max delay: 10000ms (10 seconds)
- Max attempts: 10
- Total potential delay: ~6-35 seconds per server

**Test-optimized values (this plan):**
- Base delay: 100ms
- Max delay: 500ms
- Max attempts: 5
- Total potential delay: ~1-2 seconds per server

This targets the root cause identified in CI failures without affecting production behavior.

---

## Task 1: Override setTestConfiguration() in BaseGraphServerTest

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`

**Step 1: Add setTestConfiguration() override**

Add this method to `BaseGraphServerTest` class (after line 88, before the `@BeforeEach` method):

```java
@Override
public void setTestConfiguration() {
  // Call parent to set base test configuration
  super.setTestConfiguration();

  // Override HA connection retry settings for faster test execution
  // These values reduce cluster startup time by using faster retry intervals
  // Production defaults (200ms base, 10s max) cause 6-35s delays during test startup
  GlobalConfiguration.HA_REPLICA_CONNECT_RETRY_BASE_DELAY_MS.setValue(100L);  // Faster initial retry
  GlobalConfiguration.HA_REPLICA_CONNECT_RETRY_MAX_DELAY_MS.setValue(500L);   // Lower cap on exponential backoff
  GlobalConfiguration.HA_REPLICA_CONNECT_RETRY_MAX_ATTEMPTS.setValue(5);      // Fewer attempts before giving up
}
```

**Step 2: Verify the change compiles**

Run: `mvn clean compile -DskipTests -pl server`

Expected: BUILD SUCCESS

**Step 3: Commit the change**

```bash
git add server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java
git commit -m "fix: configure faster connection retry for HA tests

Reduce connection retry delays during test execution to prevent
cluster stabilization timeouts. Production values (200ms base, 10s max)
cause excessive delays when servers start simultaneously in tests.

Test-optimized values: 100ms base, 500ms max, 5 attempts
Expected impact: ~1-2s startup vs ~6-35s with production defaults

Fixes failing CI tests:
- HTTP2ServersIT (5 failures)
- HTTP2ServersCreateReplicatedDatabaseIT
- ReplicationServerFixedClientConnectionIT
- ReplicationServerQuorumMajority1ServerOutIT
- ReplicationServerReplicaRestartForceDbInstallIT"
```

---

## Task 2: Test with previously failing test suite

**Files:**
- Test: `server/src/test/java/com/arcadedb/server/ha/HTTP2ServersIT.java`
- Test: `server/src/test/java/com/arcadedb/server/ha/HTTP2ServersCreateReplicatedDatabaseIT.java`

**Step 1: Run HTTP2ServersIT (was failing with 5 errors)**

Run: `mvn test -Dtest=HTTP2ServersIT -pl server`

Expected: All 5 tests PASS (previously failed with "Cluster failed to stabilize")

**Step 2: Run HTTP2ServersCreateReplicatedDatabaseIT**

Run: `mvn test -Dtest=HTTP2ServersCreateReplicatedDatabaseIT -pl server`

Expected: Test PASSES (previously failed with "Cluster failed to stabilize")

**Step 3: Run ReplicationServerQuorumMajority1ServerOutIT**

Run: `mvn test -Dtest=ReplicationServerQuorumMajority1ServerOutIT -pl server`

Expected: Test PASSES (previously timed out after 2 minutes)

**Step 4: Document test results**

If any tests still fail, note the failure mode. If different from "cluster failed to stabilize", there may be additional issues to address.

---

## Task 3: Run full HA test suite validation

**Files:**
- All HA tests in `server/src/test/java/com/arcadedb/server/ha/`

**Step 1: Run complete HA test suite**

Run: `mvn test -Dtest="*HA*IT,*Replication*IT" -pl server`

Expected: Pass rate >95% (up from ~62% in CI)

**Step 2: Identify any remaining failures**

Note any tests that still fail. Expected categories:
- ✅ "Cluster failed to stabilize" - SHOULD BE FIXED
- ⚠️  Other failures (data consistency, etc.) - may need separate fixes

**Step 3: Update status document**

Create summary in `docs/test-connection-config-results.md`:

```markdown
# Test Connection Config Fix - Results

## Configuration Changes
- HA_REPLICA_CONNECT_RETRY_BASE_DELAY_MS: 200ms → 100ms
- HA_REPLICA_CONNECT_RETRY_MAX_DELAY_MS: 10000ms → 500ms
- HA_REPLICA_CONNECT_RETRY_MAX_ATTEMPTS: 10 → 5

## Test Results

**Before Fix (CI):** 15/24 passing (~62%)

**After Fix (Local):** X/24 passing (X%)

### Fixed Tests
- [ ] HTTP2ServersIT (5 tests)
- [ ] HTTP2ServersCreateReplicatedDatabaseIT
- [ ] ReplicationServerFixedClientConnectionIT
- [ ] ReplicationServerQuorumMajority1ServerOutIT
- [ ] ReplicationServerReplicaRestartForceDbInstallIT

### Remaining Failures
[List any tests that still fail with error details]

## Impact
- Average cluster startup time: [before]s → [after]s
- Pass rate improvement: 62% → X%
```

**Step 4: Commit status document**

```bash
git add docs/test-connection-config-results.md
git commit -m "docs: add test connection config fix results"
```

---

## Task 4: Push to CI for validation

**Step 1: Push branch to remote**

Run: `git push origin feature/2043-ha-test`

Expected: Push succeeds, CI pipeline triggered

**Step 2: Monitor CI test results**

Watch GitHub Actions for test results. Focus on:
- Previously failing tests (should now pass)
- Overall HA test pass rate (target: >90%)
- Test execution times (should be faster)

**Step 3: Document CI results**

Update `docs/test-connection-config-results.md` with CI results:

```markdown
## CI Validation

**Environment:** GitHub Actions
**Run Date:** 2026-01-15

### Results
- Pass rate: X/24 (X%)
- Total execution time: Xm Xs
- Improvement: +X% pass rate, -X% execution time

### Status
- ✅ Fix validated in CI
- ⚠️  Partial improvement (list remaining issues)
- ❌ Fix ineffective (analyze why)
```

---

## Success Criteria

✅ **Minimum:** HTTP2ServersIT and other "cluster failed to stabilize" tests pass
✅ **Target:** HA test pass rate >90% (up from 62%)
✅ **Ideal:** All HA tests pass, average startup time reduced by 50%+

## Rollback Plan

If this change causes issues:

```bash
# Revert the commit
git revert HEAD

# Or reset to previous state
git reset --hard HEAD~1
```

The change is isolated to test configuration and doesn't affect production.

---

## Notes

- This fix addresses test infrastructure only (no production code changes)
- Production connection retry behavior unchanged
- If tests still fail after this fix, may need to investigate other issues (network, timing, etc.)
- Task 6 from test infrastructure plan (10 consecutive runs at >90% pass rate) should be completed after this fix
