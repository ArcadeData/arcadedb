# HA Test Infrastructure Continuation Plan

**Date:** 2026-01-17
**Branch:** feature/2043-ha-test
**Status:** Ready to Execute
**Prerequisites:** Test suite validation (task b2fb835)

## Executive Summary

This plan provides actionable steps to complete HA test infrastructure improvements, focusing on:
1. Analyzing current test suite results
2. Removing remaining sleep statements (if needed)
3. Achieving 90%+ test reliability
4. Documenting completion

## Current Status

✅ **Completed:**
- HATestHelpers utility created and integrated
- All 27 tests have @Timeout annotations (100%)
- All 27 tests use HATestHelpers (via BaseGraphServerTest)
- Vector index replication bug fixed and validated
- Build classpath issues resolved
- Connection retry implemented and configured

🟡 **In Progress:**
- Full test suite validation (task b2fb835 running)

⏳ **Remaining:**
- Remove ~10 sleep statements from 7 tests
- Full suite reliability validation
- Final documentation

## Task 1: Analyze Test Suite Results

**Objective:** Understand current test reliability and identify issues

**Prerequisites:** Wait for task b2fb835 to complete

### Step 1: Get test results summary
```bash
# Check if test suite completed
cat /private/tmp/claude/-Users-frank-projects-arcade-arcadedb/tasks/b2fb835.output | grep -E "Tests run:|BUILD"
```

### Step 2: Extract pass/fail metrics
```bash
# Get final summary
tail -50 /private/tmp/claude/-Users-frank-projects-arcade-arcadedb/tasks/b2fb835.output

# Count passing vs failing tests
grep "Tests run:" /private/tmp/claude/-Users-frank-projects-arcade-arcadedb/tasks/b2fb835.output | \
  awk '{total+=$3; fail+=$5; err+=$7} END {print "Total:", total, "Passed:", total-fail-err, "Failed:", fail+err}'
```

### Step 3: Identify failing tests
```bash
# List all failures
grep -B5 "FAILURE\|ERROR" /private/tmp/claude/-Users-frank-projects-arcade-arcadedb/tasks/b2fb835.output | \
  grep "Running\|Test.*FAILURE\|Test.*ERROR" | sort -u
```

### Step 4: Categorize failures
- **Sleep-related:** Timeouts, race conditions, flaky behavior
- **Connection-related:** "Cluster failed to stabilize", connection refused
- **Data-related:** Assertion failures, data inconsistency
- **Other:** Unexpected errors

**Decision Point:**
- If pass rate ≥90%: → Skip to Task 3 (Documentation)
- If pass rate <90% and sleep-related: → Continue to Task 2
- If pass rate <90% and not sleep-related: → Investigate root causes first

## Task 2: Remove Remaining Sleep Statements (Conditional)

**Objective:** Eliminate remaining Thread.sleep() calls from 7 tests

**Execute Only If:** Pass rate <90% AND failures are sleep-related

### Tests to Convert (in priority order):

1. **ReplicationServerReplicaHotResyncIT** (2 sleeps) - Hot resync timing
2. **ReplicationServerLeaderDownNoTransactionsToForwardIT** (2 sleeps) - Leader failure
3. **ReplicationServerWriteAgainstReplicaIT** (1 sleep) - Write-to-replica handling
4. **ReplicationServerReplicaRestartForceDbInstallIT** (1 sleep) - Force install
5. **ReplicationServerQuorumNoneIT** (1 sleep) - No quorum scenario
6. **ReplicationServerLeaderChanges3TimesIT** (1 sleep) - Multiple elections
7. **HARandomCrashIT** (1 sleep) - Chaos testing (may be intentional)

### Conversion Pattern for Each Test:

**Step 1: Locate sleep statement**
```bash
grep -n "Thread\.sleep\|CodeUtils\.sleep" server/src/test/java/com/arcadedb/server/ha/<TestFile>.java
```

**Step 2: Understand context**
- What is the sleep waiting for?
- Server state change?
- Replication completion?
- Leader election?
- Data propagation?

**Step 3: Replace with condition-based wait**

**Pattern A: Waiting for cluster state**
```java
// BEFORE
Thread.sleep(5000);

// AFTER
waitForClusterStable();  // Inherited from BaseGraphServerTest
```

**Pattern B: Waiting for specific server state**
```java
// BEFORE
Thread.sleep(2000);
// Hope server is ready

// AFTER
await().atMost(Duration.ofSeconds(30))
       .pollInterval(Duration.ofMillis(500))
       .until(() -> getServer(i).getStatus() == ArcadeDBServer.Status.ONLINE);
```

**Pattern C: Intentional delay (chaos testing)**
```java
// If truly intentional, document with comment:
// Intentional delay to simulate realistic failure timing
Thread.sleep(1000);
```

**Step 4: Test the conversion**
```bash
# Run test 10 times
for i in {1..10}; do
  mvn test -Dtest=<TestName> -pl server -q || echo "FAILED on run $i"
done
```

**Step 5: Commit individually**
```bash
git add server/src/test/java/com/arcadedb/server/ha/<TestFile>.java
git commit -m "test: remove sleep from <TestName>

Replaced Thread.sleep(<N>ms) with condition-based waiting for <state>.
Improves test reliability by waiting for actual state instead of fixed delay.

Verified: 10/10 consecutive successful runs

Part of HA test infrastructure improvements

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

### Batch Processing Approach

For efficiency, group related tests:

**Batch 1: Replica scenarios** (hot resync, restart, force install)
```bash
# Convert 3 tests together
# - ReplicationServerReplicaHotResyncIT
# - ReplicationServerReplicaRestartForceDbInstallIT
# - ReplicationServerWriteAgainstReplicaIT
```

**Batch 2: Leader scenarios** (leader down, leader changes)
```bash
# Convert 2 tests together
# - ReplicationServerLeaderDownNoTransactionsToForwardIT
# - ReplicationServerLeaderChanges3TimesIT
```

**Batch 3: Edge cases** (quorum none, chaos)
```bash
# Handle individually (likely special cases)
# - ReplicationServerQuorumNoneIT
# - HARandomCrashIT
```

**Estimated Time:** 2-4 hours total (20-30 minutes per test)

## Task 3: Full Suite Validation

**Objective:** Measure final test reliability

### Step 1: Run full suite 5 times
```bash
cd server
SUCCESS=0
for i in {1..5}; do
  echo "=== Run $i/5 ==="
  if mvn test -Dtest="*HA*IT,*Replication*IT" -q; then
    SUCCESS=$((SUCCESS + 1))
  fi
done
echo "Pass rate: $SUCCESS/5"
```

### Step 2: Collect metrics
- Total execution time
- Pass rate percentage
- Tests that failed (if any)
- Common failure patterns

### Step 3: Document results

Create: `docs/testing/2026-01-17-ha-test-final-validation.md`

```markdown
# HA Test Infrastructure - Final Validation

**Date:** 2026-01-17
**Status:** [PASS/PARTIAL/FAIL]

## Results

- **Pass Rate:** X/5 runs (X%)
- **Total Tests:** 27
- **Execution Time:** ~XX minutes per run
- **Reliability Target:** 90%+ ✅/❌

## Test Breakdown

| Category | Tests | Pass Rate | Notes |
|----------|-------|-----------|-------|
| Index tests | 4 | 100% | Vector replication validated |
| Replication tests | ~15 | X% | [notes] |
| HA tests | ~8 | X% | [notes] |

## Improvements from Baseline

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Pass rate | ~85% | X% | +X% |
| Tests with @Timeout | 0% | 100% | +100% |
| Sleep statements | ~18 | 0-10 | -XX% |
| Vector tests passing | 0/4 | 4/4 | +100% |

## Remaining Issues

[List any tests still failing with root cause analysis]

## Production Readiness

- ✅/❌ Vector index replication validated
- ✅/❌ 90%+ reliability achieved
- ✅/❌ Zero hanging tests
- ✅/❌ All critical paths tested

## Conclusion

[Summary and recommendations]
```

## Task 4: Documentation and Handoff

**Objective:** Create comprehensive final documentation

### Step 1: Update status documents
```bash
git add docs/plans/2026-01-17-test-infrastructure-current-state.md
git add docs/testing/2026-01-17-ha-test-final-validation.md
```

### Step 2: Create summary commit
```bash
git commit -m "docs: HA test infrastructure improvements complete

Test reliability improved from ~85% to X%+.

Achievements:
- All 27 tests have @Timeout annotations (100%)
- All tests use HATestHelpers via BaseGraphServerTest
- Vector index replication bug fixed and validated
- Connection retry with exponential backoff implemented
- Remaining sleeps: X/27 tests (X%)

Results:
- Pass rate: X/5 runs (X%)
- Execution time: ~XX minutes
- Critical bugs fixed: 2 (vector replication, classpath)

Production ready for HA deployments.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

### Step 3: Create Phase 2 placeholder (if continuing)
```markdown
# HA Production Hardening - Phase 2 Plan

**Prerequisites:** Test infrastructure at 90%+ reliability

## Objectives
1. Implement circuit breaker for slow replicas
2. Add cluster health monitoring API
3. Enhanced observability and metrics
4. Background consistency monitor

See: `docs/plans/2026-01-13-ha-reliability-improvements-design.md` Section 2.4-2.5
```

## Decision Matrix

Based on test suite results, choose next action:

| Pass Rate | Action | Priority |
|-----------|--------|----------|
| ≥95% | → Document success, move to Phase 2 | High value |
| 90-94% | → Optional: Remove remaining sleeps | Medium value |
| 85-89% | → Investigate failures, remove sleeps | Required |
| <85% | → Debug root causes first | Critical |

## Success Criteria

**Must Have:**
- [ ] Test suite validation complete
- [ ] Pass rate ≥90%
- [ ] No hanging tests
- [ ] Vector index tests passing (4/4)
- [ ] Documentation complete

**Should Have:**
- [ ] Pass rate ≥95%
- [ ] All sleeps removed (0/27 tests)
- [ ] Execution time <20 minutes

**Nice to Have:**
- [ ] 100% pass rate
- [ ] Execution time <15 minutes
- [ ] Zero flaky tests

## Rollback Plan

If issues arise:
```bash
# Revert recent changes
git log --oneline -10
git revert <commit-sha>

# Return to known good state
git reset --hard <last-good-commit>

# Re-run validation
mvn test -Dtest="*HA*IT" -pl server
```

## Estimated Timeline

- **Task 1** (Analysis): 30 minutes
- **Task 2** (Sleep removal): 2-4 hours (if needed)
- **Task 3** (Validation): 1-2 hours (mostly waiting)
- **Task 4** (Documentation): 1 hour

**Total:** 4-8 hours depending on path

## Next Steps After Completion

1. **If test infrastructure ≥90% reliable:**
   - Move to Phase 2 (production hardening)
   - Implement circuit breakers
   - Add health monitoring
   - Enhanced observability

2. **If test infrastructure <90% reliable:**
   - Deep dive into failure patterns
   - May need production code fixes (not just test improvements)
   - Revisit design document priorities

## References

- Current State: `docs/plans/2026-01-17-test-infrastructure-current-state.md`
- Design Doc: `docs/plans/2026-01-13-ha-reliability-improvements-design.md`
- Phase 1 Plan: `docs/plans/2026-01-13-ha-test-infrastructure-phase1.md`
- Recent Fix: `docs/plans/2026-01-17-index-compaction-test-fix.md`

---

**Ready to Execute:** Wait for test suite completion, then proceed with Task 1
