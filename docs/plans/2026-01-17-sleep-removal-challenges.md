# HA Test Infrastructure - Sleep Removal Challenges

**Date:** 2026-01-17
**Branch:** feature/2043-ha-test
**Status:** Blocked - Test Infrastructure Fragility

## Objective

Remove remaining ~10 sleep statements from 7 HA tests to complete test infrastructure improvements.

## Attempted Work

### Test: ReplicationServerWriteAgainstReplicaIT

**Original Code (lines 61-67):**
```java
// Additional wait to ensure connection is stable
CodeUtils.sleep(2000);

// Ensure all servers have empty replication queues
for (int i = 0; i < getServerCount(); i++) {
  waitForReplicationIsCompleted(i);
}
```

**Attempted Conversion #1: Aggressive**
```java
// Replace sleep + loop with single call
waitForClusterStable(getServerCount());
```

**Result:** Test failed during cluster initialization with:
- Socket timeout exceptions
- Connection reset errors
- Database installation failures
- Timeout after 10 minutes

**Attempted Conversion #2: Conservative**
```java
// Remove sleep, keep loop
// The waitForReplicationIsCompleted already uses Awaitility with proper timeout,
// so no additional sleep is needed before checking queue status
for (int i = 0; i < getServerCount(); i++) {
  waitForReplicationIsCompleted(i);
}
```

**Result:** Same failures - test failed during cluster initialization, not during actual test execution.

## Root Cause Analysis

### Issues Identified

1. **Test Infrastructure Fragility**
   - HA tests fail during `beginTest()` cluster setup
   - Failures occur before any test-specific code runs
   - Errors: "Connection reset", "Socket timeout", "Error on installing database"

2. **Timing Sensitivity**
   - Even minimal changes (removing a 2-second sleep) cause failures
   - Failures are environmental, not related to test logic changes

3. **Non-Deterministic Failures**
   - Connection attempts exhausted (8/8 attempts failed)
   - Multiple servers failing to connect to leader
   - Database installation failing mid-transfer

### Test Execution Evidence

**Test run 1 (aggressive change):**
```
Timeout after 10 minutes
Socket timeouts during cluster formation
Failed to connect to leader after 8 attempts in 249580ms
```

**Test run 2 (conservative change):**
```
Still running after 10 minutes
Connection reset during database installation
Error: "Error on installing database 'graph'"
Caused by: java.net.SocketException: Connection reset
```

## Implications

### Current Status

The HA test infrastructure appears to be in a fragile state where:
- Baseline test reliability is unclear (full suite validation still running)
- Small timing changes cause cascading failures
- Cluster initialization is unreliable even without sleep removal

### Questions Raised

1. **Baseline Pass Rate Unknown**
   - Full test suite (task b2fb835) still running
   - Cannot determine if 90%+ reliability target is met
   - Cannot isolate sleep-related failures from infrastructure issues

2. **Sleep Removal Value Unclear**
   - Is the 2-second sleep actually problematic, or is it working around real issues?
   - Are the ~10 remaining sleeps causing test failures, or are they masking infrastructure problems?
   - Would 74% sleep removal (20/27 tests) be "good enough"?

3. **Test Infrastructure Needs**
   - May need port cleanup between runs
   - May need better cluster initialization logic
   - May need more robust error handling in test setup

## Recommendations

### Immediate Actions

1. **Wait for Full Suite Validation**
   - Let task b2fb835 complete to establish baseline pass rate
   - Identify which tests are currently failing (with sleeps intact)
   - Categorize failures: infrastructure vs. sleep-related

2. **Prioritize by Data**
   - If pass rate ≥90%: Sleep removal is optional, not required
   - If pass rate <90% AND failures are sleep-related: Focus on those specific tests
   - If pass rate <90% AND failures are NOT sleep-related: Fix infrastructure first

3. **Conservative Approach**
   - Do NOT remove sleeps until baseline is stable
   - Consider that some sleeps may be intentional (chaos testing, realistic timing)
   - Focus on tests that are actually failing, not theoretical improvements

### Decision Matrix

| Baseline Pass Rate | Failure Type | Action |
|--------------------|--------------|--------|
| ≥90% | N/A | **STOP** - Sleep removal not needed, document success |
| 85-89% | Sleep-related | Remove sleeps from failing tests only |
| 85-89% | Infrastructure | Fix test infrastructure, not sleeps |
| <85% | Mixed | Debug root causes, sleep removal is secondary |
| <85% | Infrastructure | **STOP** - Production code issues, not test improvements |

## Lessons Learned

### What Went Wrong

1. **Assumed Baseline Stability**
   - Started sleep removal before validating current test reliability
   - Baseline pass rate is still unknown

2. **Over-Optimistic about Sleep Impact**
   - Assumed sleeps were the primary cause of test issues
   - Reality: Infrastructure fragility is the bigger problem

3. **Insufficient Test Isolation**
   - Individual test runs may have port conflicts or leftover state
   - Full suite run may behave differently than individual tests

### What Worked

1. **Vector Index Fix Validation**
   - IndexCompactionReplicationIT tests passed after clean rebuild (4/4)
   - Confirmed critical production bug fix

2. **HATestHelpers Integration**
   - All 27 tests use HATestHelpers via BaseGraphServerTest
   - Centralized pattern is working well

3. **@Timeout Annotations**
   - 100% coverage prevents hanging tests
   - Tests fail fast rather than hanging indefinitely

## Revised Next Steps

1. **Immediate: Wait for baseline data**
   ```bash
   # Check status of full suite
   tail -f /private/tmp/claude/-Users-frank-projects-arcade-arcadedb/tasks/b2fb835.output | grep -E "Tests run:|BUILD"
   ```

2. **After baseline data available:**
   - Analyze pass/fail metrics
   - Identify root causes of failures
   - Decide whether to continue sleep removal or focus on infrastructure

3. **If continuing sleep removal:**
   - Only target tests that are actually failing
   - Make minimal changes
   - Validate each change independently
   - Consider that some sleeps may be legitimate

## Conclusion

**Blocked:** Cannot proceed with sleep removal until baseline test reliability is established.

**Current Assessment:**
- ✅ HATestHelpers integrated (100%)
- ✅ @Timeout annotations complete (100%)
- ✅ Vector index bug fixed and validated
- ⏸️ Sleep removal blocked by infrastructure fragility
- ❓ Baseline pass rate unknown

**Recommendation:** Pause sleep removal work and wait for full test suite results to make data-driven decisions.

---

**Status: Blocked pending test suite validation**
**Next Action: Analyze results from task b2fb835 when complete**
