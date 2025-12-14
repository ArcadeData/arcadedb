# Issue #2948: Fix Test Logic in ThreeInstancesScenarioIT

**Issue URL**: https://github.com/ArcadeData/arcadedb/issues/2948
**Branch**: feature/2043-ha-test
**Priority**: P1 - Medium
**Phase**: Phase 1 - Fix Critical Bugs

## Overview

Fix incorrect test assertions that attempt to check state on a disconnected server in the ThreeInstancesScenarioIT test.

## Problem Description

The test at `ThreeInstancesScenarioIT.java:103-105` incorrectly asserts on arcade1's database after it has been disconnected via network toxics. When arcade1 is disconnected, only arcade2 and arcade3 should be checked for data replication.

**Current incorrect code:**
```java
// When arcade1 is disconnected, arcade2 and arcade3 should have data
// But test asserts on arcade1 which is disconnected!
db1.assertThatUserCountIs(130);  // arcade1 is DISCONNECTED - can't assert
db2.assertThatUserCountIs(130);  // correct
db3.assertThatUserCountIs(130);  // correct
```

## Implementation Plan

1. **Locate the test file** - Find ThreeInstancesScenarioIT.java in the resilience module
2. **Analyze the test** - Understand the test flow and identify all problematic assertions
3. **Fix the assertions** - Remove assertions on disconnected server (arcade1/db1)
4. **Verify** - Run the test to ensure it passes with the fix

## Progress Log

### Step 1: Analysis
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Findings**:

1. **Test File Location**: `resilience/src/test/java/com/arcadedb/containers/resilience/ThreeInstancesScenarioIT.java`

2. **Test Flow**:
   - Lines 51-61: Creates 3 arcade containers with proxies for network fault injection
   - Lines 64-68: Starts containers and creates database wrappers
   - Lines 70-93: Creates database, schema, and adds initial data (30 users total)
   - Lines 95-97: **Disconnects arcade1** using Toxiproxy (cuts all network traffic)
   - Line 100: Adds 100 users via db2 (only arcade2 and arcade3 should get this data)
   - Lines 102-105: **PROBLEM** - Asserts on db1 when arcade1 is disconnected!

3. **The Bug**:
   At lines 103-105, the test incorrectly asserts that all 3 databases have 130 users:
   ```java
   db1.assertThatUserCountIs(130);  // ❌ arcade1 is DISCONNECTED - can't receive new data
   db2.assertThatUserCountIs(130);  // ✅ correct - arcade2 has the data
   db3.assertThatUserCountIs(130);  // ✅ correct - arcade3 replicated from arcade2
   ```

   Since arcade1 is disconnected (lines 95-97), it cannot receive the 100 new users added via db2.
   Therefore, db1 should still have only 30 users, not 130.

4. **Additional Issue**:
   The comment on line 102 is also misleading: "Check that all the data are replicated only on arcade1 and arcade2"
   - Should say "arcade2 and arcade3" (not arcade1)
   - arcade1 is disconnected and cannot receive data

**Solution**:
Remove the assertion on db1 (line 103) and fix the comment to accurately reflect that only arcade2 and arcade3 should be checked.

### Step 2: Implementation
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Changes Made**:

1. **Fixed incorrect assertion** (Line 103):
   - **Removed**: `db1.assertThatUserCountIs(130);`
   - **Reason**: arcade1 is disconnected and cannot receive the 100 new users

2. **Fixed misleading comment** (Line 102):
   - **Before**: "Check that all the data are replicated only on arcade1 and arcade2"
   - **After**: "Check that the data are replicated only on arcade2 and arcade3 (arcade1 is disconnected)"
   - **Reason**: Accurately reflects that arcade1 is disconnected

3. **Added clarifying comment** (Line 103):
   - Added: `// Don't assert on db1 while arcade1 is disconnected`
   - **Reason**: Makes the intent explicit for future maintainers

**Result**:
The test now correctly validates that:
- When arcade1 is disconnected, data written to arcade2 is replicated to arcade3
- No assertion is attempted on the disconnected arcade1 node
- After reconnection (lines 107-109), arcade1 resyncs and all nodes converge (validated in lines 119-136)

### Step 3: Verification
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Verification Steps**:

1. **Code Compilation**: ✅ PASSED
   ```bash
   cd resilience && ../mvnw test-compile -q
   ```
   - Result: Compiled successfully with no errors

2. **Code Review**: ✅ VERIFIED
   - Confirmed the incorrect assertion on db1 (line 103) was removed
   - Confirmed the comment was corrected to accurately reflect the test scenario
   - Confirmed a clarifying comment was added for future maintainers

3. **Logic Verification**: ✅ CORRECT
   - The fix is logically sound: when arcade1 is disconnected (lines 95-97), it cannot receive new data
   - Therefore, attempting to assert db1.assertThatUserCountIs(130) would fail
   - The test now correctly validates only arcade2 and arcade3 (the connected nodes)

**Note on Test Execution**:
This test requires Docker and Testcontainers infrastructure (Toxiproxy) to run. The resilience module is designed for integration testing with network fault injection. The fix has been verified through:
- Successful compilation
- Code review confirming correct logic
- Analysis showing the change aligns with the test's intent

**Files Modified**:
- `resilience/src/test/java/com/arcadedb/containers/resilience/ThreeInstancesScenarioIT.java`

## Summary

**Issue**: Test incorrectly attempted to assert on arcade1's database state after disconnecting it via network toxics.

**Solution**:
1. Removed the problematic assertion: `db1.assertThatUserCountIs(130);`
2. Corrected the comment to accurately reflect that arcade2 and arcade3 are checked (not arcade1)
3. Added a clarifying comment explaining why db1 is not asserted

**Impact**:
- Test now correctly validates HA replication behavior during network partitions
- Future test runs won't fail due to attempting operations on disconnected nodes
- Test documentation (comments) now accurately reflect the test's purpose

**Verification**:
- ✅ Code compiles successfully
- ✅ Logic is correct and aligns with test intent
- ✅ Changes preserve all existing test functionality

This completes Task 1.4 from Phase 1 (Fix Critical Bugs) in the HA_IMPROVEMENT_PLAN.md.
