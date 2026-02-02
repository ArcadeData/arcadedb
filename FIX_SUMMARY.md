# Fix for head(collect()) Issue in OpenCypher Engine

## Problem Description
When using wrapped aggregation functions like `head(collect(...))` in WITH clauses, the functions were returning null instead of the expected values.

Example failing query:
```cypher
MATCH (c:CHUNK) WHERE ID(c) = "#1:2051"
MATCH (c:CHUNK)-->(doc:DOCUMENT)
WITH head(collect(ID(doc))) as document_id,
     head(collect(ID(c))) as original_chunk_id,
     head(collect(doc.name)) as document_name
RETURN document_id, document_name
```

This returned null for all values instead of the actual data.

## Root Cause
The issue was in the query execution planner's logic for detecting aggregations:

1. `WithClause.hasAggregations()` used `Expression.isAggregation()` to check if expressions were aggregations
2. `isAggregation()` only returned true for direct aggregation functions (collect, count, sum, etc.)
3. For wrapped aggregations like `head(collect(...))`, `head()` is not an aggregation function, so `isAggregation()` returned false
4. Therefore, the execution planner used `WithStep` instead of `GroupByAggregationStep`
5. `WithStep` evaluates expressions row-by-row without aggregating across rows
6. This caused `collect()` to be evaluated per-row instead of across all rows, resulting in null values

## Solution
Added a new method `containsAggregation()` to the `Expression` interface that recursively checks if an expression contains any aggregation functions, even if wrapped in non-aggregation functions.

## Files Modified

### 1. Expression.java (interface)
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/Expression.java`

**Changes:**
- Added `containsAggregation()` default method that delegates to `isAggregation()`
- This provides the base implementation for all Expression implementations

### 2. FunctionCallExpression.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/FunctionCallExpression.java`

**Changes:**
- Overrode `containsAggregation()` to:
  - Return true if the function itself is an aggregation
  - Otherwise recursively check if any argument contains an aggregation
- This detects patterns like `head(collect(...))` where collect is wrapped

### 3. WithClause.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/WithClause.java`

**Changes:**
- Updated `hasAggregations()` to use `containsAggregation()` instead of `isAggregation()`
- Updated `hasNonAggregations()` to use `containsAggregation()` instead of `isAggregation()`
- This ensures wrapped aggregations are properly detected during query planning

### 4. ReturnClause.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/ReturnClause.java`

**Changes:**
- Updated `hasAggregations()` to use `containsAggregation()` instead of the private helper method
- Updated `hasNonAggregations()` to use `containsAggregation()` instead of the private helper method
- Removed the now-redundant private `containsAggregation()` method

### 5. ListExpression.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/ListExpression.java`

**Changes:**
- Overrode `containsAggregation()` to recursively check all list elements
- Ensures patterns like `[collect(x), collect(y)]` are properly detected

### 6. CaseExpression.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/CaseExpression.java`

**Changes:**
- Overrode `containsAggregation()` to check:
  - The case expression (if present)
  - All WHEN expressions
  - All THEN expressions
  - The ELSE expression (if present)

### 7. ListComprehensionExpression.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/ListComprehensionExpression.java`

**Changes:**
- Overrode `containsAggregation()` to check:
  - The list expression
  - The WHERE filter expression (if present)
  - The map expression (if present)

### 8. ArithmeticExpression.java
**Location:** `engine/src/main/java/com/arcadedb/query/opencypher/ast/ArithmeticExpression.java`

**Changes:**
- Overrode `containsAggregation()` to check both left and right operands

### 9. HeadCollectTest.java (NEW)
**Location:** `engine/src/test/java/com/arcadedb/query/opencypher/HeadCollectTest.java`

**Changes:**
- Created comprehensive regression test suite for wrapped aggregations
- Tests include:
  - `testHeadCollectInWith()` - Basic head(collect()) pattern
  - `testHeadCollectMultipleFields()` - Multiple wrapped aggregations
  - `testCollectWithoutHead()` - Control test for plain collect()
  - `testLastCollect()` - Test with last() wrapper
  - `testSizeCollect()` - Test with size() wrapper

## How the Fix Works

### Before Fix:
```
Query: WITH head(collect(x)) as result
  ↓
WithClause.hasAggregations() calls Expression.isAggregation()
  ↓
head(...).isAggregation() returns false (head is not an aggregation)
  ↓
Execution planner uses WithStep (per-row evaluation)
  ↓
collect() evaluated per row → returns empty/null
  ↓
head() of empty/null → returns null
```

### After Fix:
```
Query: WITH head(collect(x)) as result
  ↓
WithClause.hasAggregations() calls Expression.containsAggregation()
  ↓
head(...).containsAggregation() checks arguments
  ↓
collect(...).containsAggregation() returns true (collect is an aggregation)
  ↓
head(...).containsAggregation() returns true
  ↓
Execution planner uses GroupByAggregationStep (cross-row aggregation)
  ↓
GroupByAggregationStep detects wrapped aggregation using findInnerAggregation()
  ↓
collect() accumulates values across all rows → returns [value1, value2, ...]
  ↓
head() applied to aggregated result → returns value1
```

## Testing
The fix includes a comprehensive test suite (`HeadCollectTest.java`) that covers:
- Basic wrapped aggregation patterns
- Multiple wrapped aggregations in the same query
- Different wrapper functions (head, last, size)
- Control tests to ensure non-wrapped aggregations still work

## Backward Compatibility
This change is fully backward compatible:
- All existing queries continue to work as before
- Only queries with wrapped aggregations (which were previously broken) are affected
- The default implementation of `containsAggregation()` ensures all existing Expression implementations work correctly without modification

## Additional Notes
- The `GroupByAggregationStep` class already had logic to handle wrapped aggregations via `findInnerAggregation()`, so no changes were needed there
- The fix ensures proper execution plan selection by correctly identifying wrapped aggregations during the planning phase
