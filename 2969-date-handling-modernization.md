# Issue #2969: Modernize Date Handling with Java 21 Pattern Matching

## Status
In Progress

## Objective
Modernize date handling code in `DateUtils.java` and `BinaryTypes.java` using Java 21 pattern matching switch expressions.

## Target: 43% Complexity Reduction
- Replace verbose if-else chains with exhaustive switch expressions
- Migrate from wildcard to explicit imports
- Improve code clarity and maintainability

## Methods to Refactor

### DateUtils.java
1. `getNanos(Object obj)` - Already modernized (switch expr)
2. `dateTime()` (lines 49-105) - Needs refactoring: if-else chain based on class comparison
3. `date()` (lines 107-121) - Needs refactoring: if-else chain
4. `getDate()` (lines 377-398) - Needs refactoring: if-else chain
5. `dateTimeToTimestamp()` (lines 123-200) - Already uses pattern matching, may optimize further

### BinaryTypes.java
- Current imports: `java.lang.reflect.*`, `java.math.*`, `java.time.*`, `java.util.*`, `java.util.logging.*`
- Action: Make explicit imports

## Implementation Plan

### Phase 1: Analyze & Test
1. Read current DateUtils and BinaryTypes code
2. Identify exact methods needing modernization
3. Review existing test coverage

### Phase 2: Refactor DateUtils Methods
1. `dateTime()` - Switch expression with nested precision switches
2. `date()` - Switch expression for class-based routing
3. `getDate()` - Switch expression with pattern matching
4. Compile and run tests after each change

### Phase 3: Update BinaryTypes Imports
1. Replace wildcard imports with explicit ones
2. Verify no functionality changes

### Phase 4: Validation
1. Compile engine module
2. Run DateUtils-related tests
3. Run RemoteDateIT integration tests
4. Check for regressions

## Changes Made
(To be updated)

## Test Results
(To be updated)

## Commits
(To be updated)
