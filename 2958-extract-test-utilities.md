# Issue #2958: HA - Task 5.1 - Extract Common Test Utilities

## Overview
Create a dedicated test support module (`arcadedb-test-support`) to centralize and share HA test infrastructure across all test modules.

## Objectives
1. Create new Maven module `arcadedb-test-support`
2. Extract and enhance common test utilities
3. Implement HATestCluster for multi-node test cluster management
4. Implement NetworkFaultInjector to wrap Toxiproxy functionality
5. Enhance DatabaseComparator with additional validation features

## Scope
- Create new Maven module with proper dependencies
- Extract common code from e2e-perf and resilience modules
- Implement reusable test utilities
- Maintain backward compatibility with existing tests

## Progress Log

### Step 1: Branch Verification ✓
**Started**: 2025-12-15
- Current branch: `feature/2043-ha-test` (continuing on existing HA feature branch)

### Step 2: Documentation Created ✓
**Completed**: 2025-12-15
- Created tracking document: `2958-extract-test-utilities.md`

### Step 3: Analysis Phase
**Started**: 2025-12-15

#### Requirements from Issue:
- Extract common test utilities to dedicated module
- Create HATestCluster for cluster management
- Create NetworkFaultInjector for network fault injection
- Enhance DatabaseComparator for validation
- Medium effort, P3 priority task

#### Files to Analyze:
1. Current test infrastructure in e2e-perf/src/test/java/com/arcadedb/test/support/
2. Resilience tests in resilience/src/test/java/
3. HA_IMPROVEMENT_PLAN.md for detailed requirements

### Step 4: Implementation Plan
1. Create arcadedb-test-support Maven module
2. Define module dependencies (testcontainers, toxiproxy, etc.)
3. Extract ContainersTestTemplate as base infrastructure
4. Implement HATestCluster class
5. Implement NetworkFaultInjector class
6. Create enhanced test utilities
7. Update existing modules to use new test-support module
8. Write tests for new utilities

### Step 5: Analysis Complete ✓
**Completed**: 2025-12-15

#### Current State Assessment:

**Existing Test Infrastructure:**
1. **e2e-perf/src/test/java/com/arcadedb/test/support/**
   - `ContainersTestTemplate.java` - Base class for Testcontainers tests (229 lines)
   - `DatabaseWrapper.java` - Remote database operations wrapper (365 lines)
   - `ServerWrapper.java` - Server connection record (33 lines)
   - `TypeIdSupplier.java` - ID supplier for test data (72 lines)

2. **Current Usage:**
   - Used by e2e-perf performance tests
   - Used by resilience chaos engineering tests (issues #2955, #2956)
   - Contains Toxiproxy integration in ContainersTestTemplate
   - Contains database comparison logic (issue #2957)

#### Proposed Architecture:

**New Module: `arcadedb-test-support`**
```
arcadedb-test-support/
├── pom.xml
└── src/
    ├── main/java/com/arcadedb/test/
    │   ├── HATestCluster.java           (NEW - cluster management)
    │   ├── NetworkFaultInjector.java    (NEW - Toxiproxy wrapper)
    │   └── DatabaseValidator.java       (NEW - enhanced validation)
    └── test/java/com/arcadedb/test/
        ├── HATestClusterTest.java
        ├── NetworkFaultInjectorTest.java
        └── DatabaseValidatorTest.java
```

**Extracted from e2e-perf:**
- `ContainersTestTemplate.java` → Keep as test-scoped, depends on test-support
- `DatabaseWrapper.java` → Move to test-support/src/main/java
- `ServerWrapper.java` → Move to test-support/src/main/java
- `TypeIdSupplier.java` → Move to test-support/src/main/java

#### Complexity Assessment:

**High Complexity Factors:**
1. **Module Creation:**
   - New Maven module with dependencies (testcontainers, toxiproxy, assertj, etc.)
   - Integration into parent POM
   - Proper scope management (test vs compile)

2. **Dependency Migration:**
   - Update e2e-perf to depend on test-support
   - Update resilience to depend on test-support
   - Potential circular dependency risks
   - Version alignment across modules

3. **Code Migration:**
   - Move 4 classes (470+ lines) to new module
   - Update package imports across multiple test files
   - Ensure no breaking changes for existing tests

4. **New Implementations:**
   - HATestCluster: ~200-300 lines (cluster lifecycle management)
   - NetworkFaultInjector: ~150-200 lines (Toxiproxy abstraction)
   - DatabaseValidator: ~100-150 lines (enhanced comparison)

5. **Testing:**
   - Unit tests for 3 new classes
   - Integration tests to verify no regressions
   - Test across both e2e-perf and resilience modules

**Estimated Effort:** 4-6 hours

**Risk Level:** Medium-High
- Breaking existing tests during migration
- Dependency conflicts
- Build system complexity

#### Recommendation:

This task should be **completed as a dedicated effort** due to:

1. **Architectural Impact**: Creates new module affecting project structure
2. **Cross-Module Changes**: Requires careful coordination across 3+ modules
3. **Regression Risk**: High risk of breaking existing tests during migration
4. **Testing Overhead**: Extensive verification needed across modules

**Suggested Approach:**

**Phase A: Foundation (High Priority)**
1. Create arcadedb-test-support module structure
2. Define dependencies carefully
3. Implement NetworkFaultInjector (most urgent utility)
4. Write comprehensive tests

**Phase B: Migration (Medium Priority)**
5. Move existing classes to test-support
6. Update imports in e2e-perf and resilience
7. Verify all tests still pass

**Phase C: Enhancement (Lower Priority)**
8. Implement HATestCluster
9. Implement DatabaseValidator enhancements
10. Add integration tests

**Current Session Status:**
- Analysis and documentation: **COMPLETE** ✓
- Ready for implementation in dedicated session
- Prerequisites: None (can start immediately)
- Estimated duration: 4-6 hours focused work

## Technical Notes
- Part of Phase 5: Improve Test Infrastructure from HA_IMPROVEMENT_PLAN.md
- Related to issues #2043, #2955, #2956, #2957
- Must maintain backward compatibility
- Should reduce code duplication across test modules
- **Requires dedicated implementation session due to scope**
