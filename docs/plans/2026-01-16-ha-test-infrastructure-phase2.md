# HA Test Infrastructure Improvements - Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix critical vector index replication bug and complete test infrastructure improvements for production-grade HA reliability.

**Architecture:** Address production blocker (vector index replication), then complete systematic conversion of remaining HA tests to use HATestHelpers and condition-based waits.

**Tech Stack:** JUnit 5, Awaitility, Java 21+, Maven

**Context:** Building on Phase 1 work. HATestHelpers utility exists, all tests have @Timeout annotations, and 2 tests converted. This phase prioritizes fixing the vector index replication bug discovered during test improvements, then completes remaining test conversions.

---

## Current Status Review

### ✅ Completed (Phase 1)

- **Task 1**: HATestHelpers utility class created with 6 helper methods
- **Task 2**: All 27 HA tests have @Timeout annotations (100% coverage)
- **Task 3**: SimpleReplicationServerIT converted to use HATestHelpers
- **Task 5**: BaseGraphServerTest delegates to HATestHelpers
- **Partial Task 4**: 2/27 tests converted (SimpleReplicationServerIT, ServerDatabaseSqlScriptIT)

### 🔴 Critical Issue Discovered

**Vector Index Replication Bug** (Production Blocker):
- **Symptom**: Vector index entries don't replicate to replicas (only 127/5000 entries)
- **Scope**: Specific to LSMVectorIndex - regular LSM indexes replicate correctly
- **Impact**: Data loss in HA deployments using vector indexes
- **Test**: `IndexCompactionReplicationIT.lsmVectorReplication()`
- **Status**: Test infrastructure fixed, production bug identified

### 🟡 Remaining Work

- **Task 4**: Convert remaining 25/27 tests to HATestHelpers
- **Task 6**: Full test suite reliability validation
- **NEW**: Investigate and fix vector index replication bug

---

## Task 1: Fix Vector Index Replication Bug (CRITICAL)

**Objective:** Investigate and fix the production bug preventing vector index entries from replicating to replicas.

**Priority:** HIGHEST - This is a data loss bug blocking production HA for vector indexes

**Files:**
- Investigate: `engine/src/main/java/com/arcadedb/index/vector/LSMVectorIndex.java`
- Investigate: `engine/src/main/java/com/arcadedb/database/TransactionIndexContext.java`
- Investigate: `server/src/main/java/com/arcadedb/server/ha/message/*.java` (replication protocol)
- Reference: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeIndex.java` (working example)
- Test: `server/src/test/java/com/arcadedb/server/ha/IndexCompactionReplicationIT.java`

### Background: What We Know

**Confirmed Facts:**
1. Leader has all 5000 entries ✓
2. Replica only has 127 entries (missing 4873 = 97.5%)
3. Regular LSM indexes replicate correctly
4. Both leader and replicas have unlimited cache (`VECTOR_INDEX_LOCATION_CACHE_SIZE=-1`)
5. Replication queues empty (not a queue backup issue)
6. The number 127 (0x7F = max signed byte) suggests serialization issue

**Test Infrastructure Fixes Already Applied:**
1. Fixed transaction batching (explicit begin/commit)
2. Fixed vector generation (unique vectors instead of duplicates)
3. Fixed cache configuration (unlimited on all servers)

**Step 1: Compare vector vs regular LSM index transaction handling**

Analyze how `put()` operations differ:

```bash
# Compare put() method signatures and transaction handling
diff -u \
  <(grep -A30 "public void put(final Object" engine/src/main/java/com/arcadedb/index/lsm/LSMTreeIndex.java) \
  <(grep -A30 "public void put(final Object" engine/src/main/java/com/arcadedb/index/vector/LSMVectorIndex.java)
```

Expected: Identify differences in how vector indexes handle `addIndexOperation()`

**Step 2: Trace vector index operation serialization**

Find how vector index operations are serialized for replication:

```bash
# Search for ComparableVector in transaction context
grep -rn "ComparableVector" engine/src/main/java/com/arcadedb/database/
```

Key question: Does `ComparableVector` equality/hashCode cause deduplication in `TransactionIndexContext`?

**Step 3: Check if there's a batch size limit**

Search for limits that might cap at 127:

```bash
# Look for byte-sized limits or 127/128 constants
grep -rn "127\|128\|0x7F\|0x80\|Byte.MAX\|batch.*size\|chunk.*size" \
  server/src/main/java/com/arcadedb/server/ha/
```

Expected: Find if replication protocol has a batch limit

**Step 4: Add detailed logging to LSMVectorIndex.put()**

Modify `LSMVectorIndex.put()` to log every operation:

```java
@Override
public void put(final Object[] keys, final RID[] values) {
  // ... existing code ...

  if (txStatus == TransactionContext.STATUS.BEGUN) {
    LogManager.instance().log(this, Level.INFO, "TX ADD: vector index operation for RID %s (id=%d)", rid, id);
    getDatabase().getTransaction()
            .addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.ADD,
                    new Object[]{new ComparableVector(vector)}, rid);
  } else {
    LogManager.instance().log(this, Level.INFO, "DIRECT ADD: vector index operation for RID %s (id=%d)", rid, id);
    // ... existing code ...
  }
}
```

**Step 5: Add logging to TransactionIndexContext**

Track how many unique vector operations are registered:

```java
// In TransactionIndexContext.addIndexOperation()
public void addIndexOperation(...) {
  // ... existing code ...
  LogManager.instance().log(this, Level.INFO,
    "TX: Added %s operation for index %s (total unique ops: %d)",
    operation, index.getName(), indexChanges.size());
}
```

**Step 6: Run test with detailed logging**

```bash
mvn test -Dtest=IndexCompactionReplicationIT#lsmVectorReplication 2>&1 | \
  grep -E "TX ADD|DIRECT ADD|total unique ops" | head -100
```

Expected: See if operations are being deduplicated during transaction

**Step 7: Compare with regular LSM index behavior**

Add same logging to `LSMTreeIndex.put()` and run comparison test:

```bash
# Run regular LSM test
mvn test -Dtest=IndexCompactionReplicationIT#lsmTreeCompactionReplication 2>&1 | \
  grep -E "TX ADD|DIRECT ADD" > /tmp/regular_lsm.log

# Run vector LSM test
mvn test -Dtest=IndexCompactionReplicationIT#lsmVectorReplication 2>&1 | \
  grep -E "TX ADD|DIRECT ADD" > /tmp/vector_lsm.log

# Compare
diff /tmp/regular_lsm.log /tmp/vector_lsm.log
```

Expected: Identify where behavior diverges

**Step 8: Hypothesis - Check ComparableVector equality**

The issue may be that `ComparableVector.equals()` compares vector values, causing identical vectors to deduplicate in `TransactionIndexContext`'s `TreeMap`.

Read the code:

```bash
grep -A20 "class ComparableVector" engine/src/main/java/com/arcadedb/index/vector/LSMVectorIndex.java
```

If `equals()` compares vector arrays by value, then vectors with same values would be treated as duplicates.

**Step 9: Propose fix**

If the hypothesis is correct, the fix is to make `ComparableVector` use identity-based equality or add a unique identifier:

```java
// OPTION 1: Add RID to ComparableVector for uniqueness
private static class ComparableVector implements Comparable<ComparableVector> {
    final float[] vector;
    final RID rid; // NEW - ensures uniqueness
    final int hashCode;

    ComparableVector(final float[] vector, final RID rid) {
        this.vector = vector;
        this.rid = rid;
        // Hash both vector and RID
        this.hashCode = Objects.hash(Arrays.hashCode(vector), rid);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ComparableVector other)) return false;
        // Compare both vector AND RID for uniqueness
        return Arrays.equals(vector, other.vector) && Objects.equals(rid, other.rid);
    }
}

// Update put() to pass RID:
new Object[]{new ComparableVector(vector, rid)}
```

**OPTION 2: Use identity-based comparison**
```java
@Override
public boolean equals(final Object o) {
    return this == o; // Identity only
}

@Override
public int hashCode() {
    return System.identityHashCode(this); // Identity hash
}
```

**Step 10: Implement and test fix**

Apply chosen fix and run tests:

```bash
# Rebuild
mvn clean install -DskipTests

# Test vector replication
mvn test -Dtest=IndexCompactionReplicationIT#lsmVectorReplication

# Verify regular indexes still work
mvn test -Dtest=IndexCompactionReplicationIT#lsmTreeCompactionReplication
```

Expected: All 3 servers show 5000/5000 entries

**Step 11: Run all vector index tests**

```bash
mvn test -Dtest=IndexCompactionReplicationIT
```

Expected: All 3 tests pass:
- lsmTreeCompactionReplication ✓
- lsmVectorReplication ✓ (was failing)
- lsmVectorCompactionReplication ✓

**Step 12: Commit fix**

```bash
git add engine/src/main/java/com/arcadedb/index/vector/LSMVectorIndex.java
git commit -m "fix: vector index entries not replicating to replicas

Vector index operations were being deduplicated in TransactionIndexContext
because ComparableVector.equals() compared vector values instead of
treating each operation as unique.

Fix: [describe chosen approach - either add RID to equality or use identity]

This caused only ~127 out of thousands of vector entries to replicate,
resulting in 97%+ data loss on replicas.

Verified:
- lsmVectorReplication test now passes (5000/5000 on all servers)
- lsmTreeCompactionReplication still passes (regular indexes unaffected)
- lsmVectorCompactionReplication passes

Fixes a critical production blocker for HA deployments with vector indexes.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Fix Other Vector Index Tests

**Objective:** Apply same vector generation fixes to other vector tests.

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ha/IndexCompactionReplicationIT.java`

**Step 1: Fix lsmVectorCompactionReplication test**

The `lsmVectorCompactionReplication()` test has the same `(i+j)%100f` pattern. Apply the same fix:

```java
// Change from:
vector[j] = (i + j) % 100f;

// To:
vector[j] = i + (j * 0.1f);
```

**Step 2: Run test**

```bash
mvn test -Dtest=IndexCompactionReplicationIT#lsmVectorCompactionReplication
```

Expected: PASS with 5000/5000 entries on all servers

**Step 3: Clean up diagnostic logging**

Remove the verbose commit logging added during debugging:

```java
// Remove these lines:
LogManager.instance().log(this, Level.INFO, "After commit #%d (i=%d): %d entries in index", ...);
```

Keep only essential logging:
```java
LogManager.instance().log(this, Level.INFO, "Inserting %d records into vector index...", TOTAL_RECORDS);
LogManager.instance().log(this, Level.INFO, "Completed inserting %d records", TOTAL_RECORDS);
```

**Step 4: Run all 3 tests**

```bash
mvn test -Dtest=IndexCompactionReplicationIT
```

Expected: All 3 tests PASS

**Step 5: Commit cleanup**

```bash
git add server/src/test/java/com/arcadedb/server/ha/IndexCompactionReplicationIT.java
git commit -m "test: complete IndexCompactionReplicationIT fixes

Apply unique vector generation to lsmVectorCompactionReplication.
Remove verbose diagnostic logging now that root cause is fixed.

All 3 tests now pass consistently:
- lsmTreeCompactionReplication ✓
- lsmVectorReplication ✓
- lsmVectorCompactionReplication ✓

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Convert Top 5 Flakiest Tests to HATestHelpers

**Objective:** Identify and convert the tests with the most sleep statements and timing issues.

**Files:**
- Identify: Tests with most `Thread.sleep` or timing issues
- Modify: Top 5 flakiest tests

**Step 1: Identify flakiest tests by sleep count**

```bash
echo "Sleep count | Filename" > /tmp/flaky_tests.txt
echo "------------|----------" >> /tmp/flaky_tests.txt
for file in server/src/test/java/com/arcadedb/server/ha/*IT.java; do
  count=$(grep -c "Thread.sleep\|CodeUtils.sleep" "$file" 2>/dev/null || echo 0)
  basename=$(basename "$file")
  printf "%3d         | %s\n" "$count" "$basename"
done | sort -rn >> /tmp/flaky_tests.txt

cat /tmp/flaky_tests.txt
```

Expected: Ranked list of tests by sleep count

**Step 2: Convert first flaky test**

For each test in the top 5:

1. Add import:
```java
import static com.arcadedb.server.ha.HATestHelpers.*;
```

2. Replace patterns:
```java
// BEFORE
Thread.sleep(2000);
checkConsistency();

// AFTER
waitForClusterStable(this, getServerCount());
checkConsistency();
```

3. Replace server lifecycle:
```java
// BEFORE
server.stop();
while (server.getStatus() == Status.SHUTTING_DOWN) Thread.sleep(100);
server.start();
Thread.sleep(5000);

// AFTER
server.stop();
waitForServerShutdown(this, server, i);
server.start();
waitForServerStartup(this, server, i);
waitForClusterStable(this, getServerCount());
```

**Step 3: Run test 10 times to verify**

```bash
TEST_NAME="<TestClassName>"
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -Dtest=$TEST_NAME -q || echo "FAILED on run $i"
done
```

Expected: At least 9/10 passes

**Step 4: Commit each test**

```bash
git add server/src/test/java/com/arcadedb/server/ha/<TestFile>.java
git commit -m "test: convert <TestName> to use HATestHelpers

Replaced <N> sleep statements with condition-based waits.
Improves reliability from flaky to 90%+ pass rate.

Specific changes:
- Thread.sleep() → waitForClusterStable()
- Manual shutdown loops → waitForServerShutdown()
- Manual startup waits → waitForServerStartup()

Verified: 9+/10 consecutive successful runs

Part of HA Test Infrastructure Improvements Phase 2

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Step 5: Repeat for remaining 4 tests**

Continue pattern for each of the top 5 flakiest tests.

---

## Task 4: Convert Remaining HA Tests (Batch Conversion)

**Objective:** Convert remaining ~20 tests in efficient batches.

**Strategy:** Group tests by complexity and convert in batches of 5

**Step 1: Group remaining tests**

```bash
# Get all HA test files
ls server/src/test/java/com/arcadedb/server/ha/*IT.java > /tmp/all_tests.txt

# Subtract already converted
grep -l "HATestHelpers" server/src/test/java/com/arcadedb/server/ha/*IT.java > /tmp/converted.txt

# Get remaining
comm -23 <(sort /tmp/all_tests.txt) <(sort /tmp/converted.txt) > /tmp/remaining_tests.txt

# Count
echo "Remaining tests: $(wc -l < /tmp/remaining_tests.txt)"
```

**Step 2: Convert Batch 1 (5 tests)**

Select first 5 from remaining list and apply conversion pattern.

**Step 3: Run batch verification**

```bash
# List first batch
BATCH_1=$(head -5 /tmp/remaining_tests.txt | xargs basename -a | sed 's/\.java$//' | tr '\n' ',' | sed 's/,$//')

# Run all 5 tests
mvn test -Dtest="$BATCH_1"
```

Expected: All 5 PASS

**Step 4: Commit batch**

```bash
git add server/src/test/java/com/arcadedb/server/ha/*IT.java
git commit -m "test: convert batch 1 of HA tests to HATestHelpers (5 tests)

Converted to condition-based waits:
- [list test names]

Each test verified individually before batch commit.

Part of HA Test Infrastructure Improvements Phase 2

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Step 5: Repeat for remaining batches**

Continue in batches of 5 until all ~20 remaining tests are converted.

---

## Task 5: Full Test Suite Reliability Validation

**Objective:** Measure improvement and document production readiness.

**Step 1: Baseline - Run full suite once**

```bash
cd server
mvn test -Dtest="*HA*IT,*Replication*IT" 2>&1 | tee /tmp/ha_final_validation.log

# Extract summary
grep -E "Tests run:|Failures:|Errors:|Skipped:" /tmp/ha_final_validation.log | tail -1
```

**Step 2: Reliability - Run 10 times**

```bash
cd server
SUCCESS=0
TOTAL=10

for i in $(seq 1 $TOTAL); do
  echo "=== Run $i/$TOTAL ===" | tee -a /tmp/ha_reliability.log
  if mvn test -Dtest="*HA*IT,*Replication*IT" -q 2>&1 | tee -a /tmp/ha_reliability.log | grep -q "BUILD SUCCESS"; then
    SUCCESS=$((SUCCESS + 1))
    echo "✓ PASS" | tee -a /tmp/ha_reliability.log
  else
    echo "✗ FAIL" | tee -a /tmp/ha_reliability.log
  fi
done

echo "Pass rate: $SUCCESS/$TOTAL ($(( SUCCESS * 100 / TOTAL ))%)"
```

Expected: ≥90% pass rate

**Step 3: Identify remaining issues**

```bash
# Find tests that failed in any run
grep -B3 "FAILURE\|ERROR" /tmp/ha_reliability.log | \
  grep -E "test[A-Z]" | \
  sort | uniq -c | sort -rn > /tmp/remaining_flaky_tests.txt

cat /tmp/remaining_flaky_tests.txt
```

**Step 4: Document results**

Create: `docs/testing/2026-01-16-ha-phase2-validation.md`

```markdown
# HA Test Infrastructure Phase 2 Validation Results

**Date:** 2026-01-16
**Branch:** feature/2043-ha-test
**Objective:** Production-grade HA reliability

## Critical Bug Fixes

### Vector Index Replication Bug
- **Issue**: Vector index entries not replicating (97% data loss)
- **Cause**: [describe root cause from Task 1]
- **Fix**: [describe solution]
- **Impact**: Production blocker resolved ✓

## Test Infrastructure Improvements

### Coverage Metrics

| Metric | Phase 1 | Phase 2 | Improvement |
|--------|---------|---------|-------------|
| Tests with @Timeout | 27/27 (100%) | 27/27 (100%) | Maintained |
| Tests using HATestHelpers | 2/27 (7%) | 27/27 (100%) | +93% |
| Sleep-based waits | ~15 | 0 | -100% |
| Pass rate (10 runs) | ~85% | ≥90% | +5%+ |
| Vector index tests passing | 0/3 | 3/3 | +100% |

### Test Conversion Summary

**Converted (27/27):**
- SimpleReplicationServerIT ✓
- ServerDatabaseSqlScriptIT ✓
- IndexCompactionReplicationIT ✓ (+ bug fix)
- [List remaining 24 tests]

**All tests now:**
- Use condition-based waits (no sleep)
- Have explicit timeout protection
- Follow consistent patterns
- Include detailed failure logging

## Production Readiness Assessment

### ✅ Ready for Production
- Vector index replication fixed
- Zero hanging tests
- 90%+ reliability in 10-run validation
- Comprehensive timeout coverage

### 🟡 Known Issues (Non-Blocking)
[List any tests that still fail occasionally with < 10% failure rate]

### 📋 Recommended Next Steps
1. Deploy to staging for extended testing
2. Monitor HA metrics in production
3. Address remaining flaky tests in Phase 3
4. Implement advanced resilience features (circuit breaker, health API)

## Validation Checklist

- [x] All vector index tests pass
- [x] Zero data loss in replication scenarios
- [x] No hanging tests in 10 consecutive runs
- [x] All tests have timeout protection
- [x] All tests use condition-based waits
- [x] 90%+ pass rate achieved
- [x] Backward compatibility maintained

## Conclusion

HA system is **PRODUCTION READY** for:
- Multi-server deployments
- Vector index workloads
- Automated failover scenarios
- CI/CD integration

**Production Blocker (vector replication) resolved.**
**Test reliability improved from ~85% to ≥90%.**
**Zero technical debt in test infrastructure.**
```

**Step 5: Commit validation results**

```bash
git add docs/testing/2026-01-16-ha-phase2-validation.md
git commit -m "docs: HA Phase 2 validation results - PRODUCTION READY

Validation confirms:
✓ Vector index replication bug fixed (was 97% data loss)
✓ 27/27 tests use HATestHelpers (was 2/27)
✓ Zero sleep-based waits (was 15+)
✓ ≥90% pass rate in 10-run validation
✓ All 3 vector index tests passing
✓ No hanging tests observed

Production blocker resolved. HA system ready for production deployment.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Final Verification Steps

**1. Clean build**
```bash
mvn clean install -DskipTests
```
Expected: BUILD SUCCESS

**2. Full suite single run**
```bash
cd server && mvn test -Dtest="*HA*IT,*Replication*IT"
```
Expected: PASS with high success rate

**3. Reliability test (critical)**
```bash
cd server
for i in {1..10}; do
  mvn test -Dtest="*HA*IT,*Replication*IT" -q || echo "RUN $i FAILED"
done | grep -c "FAILED"
```
Expected: ≤1 failure (≥90% success)

**4. No hanging tests**
```bash
timeout 90m mvn test -Dtest="*HA*IT,*Replication*IT"
echo "Exit code: $?"
```
Expected: Completes successfully (exit 0) within timeout

**5. Vector index tests specifically**
```bash
mvn test -Dtest=IndexCompactionReplicationIT
```
Expected: All 3 tests PASS consistently

---

## Success Criteria

### Must Have (Blocking)
- ✅ Vector index replication bug FIXED
- ✅ All 3 vector tests PASS (lsmTree, lsmVector, lsmVectorCompaction)
- ✅ Zero data loss in any replication scenario
- ✅ 100% of tests have @Timeout annotations
- ✅ 100% of tests use HATestHelpers (no sleep-based waits)
- ✅ ≥90% pass rate in 10-run validation
- ✅ No test hangs in validation runs

### Should Have (Quality)
- ✅ Detailed validation documentation
- ✅ Clear remaining issues list
- ✅ Backward compatibility maintained
- ✅ All commits follow conventional format

### Nice to Have (Future)
- 🟡 95%+ pass rate (stretch goal)
- 🟡 Advanced resilience features (circuit breaker, health API)
- 🟡 Performance metrics (test execution time)

---

## Rollback Plan

If issues arise after deployment:

1. **Revert vector index fix:**
```bash
git revert <commit-sha-of-vector-fix>
mvn clean install
```

2. **Revert test conversions:**
```bash
git revert <commit-range-of-test-conversions>
```

3. **Monitor specific tests:**
```bash
# Run problematic test in loop
while true; do
  mvn test -Dtest=<ProblematicTest>
  sleep 5
done
```

---

## Estimated Timeline

- **Task 1** (Vector bug fix): 4-8 hours (investigation heavy)
- **Task 2** (Other vector tests): 1 hour
- **Task 3** (Top 5 flaky tests): 3-4 hours
- **Task 4** (Remaining ~20 tests): 6-8 hours (in batches)
- **Task 5** (Final validation): 2-3 hours

**Total: 16-24 hours** (2-3 days with testing)

**Critical path: Task 1 (vector bug) must complete before production deployment**

---

## Notes for Execution

1. **Priority Order:**
   - Task 1 is CRITICAL - must complete first
   - Tasks 3-4 can be parallelized if needed
   - Task 5 validates everything

2. **Commit Hygiene:**
   - One logical change per commit
   - Include test results in commit messages
   - Reference issue numbers if applicable

3. **Testing Discipline:**
   - Always run test 10 times before committing conversion
   - If test fails >1/10, investigate before proceeding
   - Document any workarounds or known issues

4. **Communication:**
   - Update team after Task 1 completion (critical bug fix)
   - Share final validation results
   - Document any discovered issues for future work
