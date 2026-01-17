# ArcadeDB SQL Grammar Refactoring Analysis

**Author**: Claude Code with Happy
**Date**: 2026-01-16
**Grammar File**: `engine/src/main/grammar/SQLGrammar.jjt`
**Grammar Size**: 4,102 lines, 148 LOOKAHEAD declarations, ~211 generated AST classes

---

## Executive Summary

The ArcadeDB SQL grammar (inherited from OrientDB) is functionally correct but has accumulated technical debt that impacts maintainability and parsing performance. This analysis identifies specific pain points and provides actionable recommendations for incremental improvement.

**Key Findings:**
- ✅ Grammar works correctly - all tests pass
- ⚠️ 148 LOOKAHEAD declarations create parsing overhead
- ⚠️ Verbose case-insensitive token definitions (150+ lines can be reduced to 50)
- ⚠️ Statement variant explosion (4 CreateVertex variants, similar patterns elsewhere)
- ⚠️ 12 trivial operator productions can be consolidated

**Recommendation**: Incremental refactoring with careful testing after each phase.

---

## Current Architecture

### Token Layer
```
Lines 115-263: Case-insensitive keywords (verbose pattern)
  Example: <SELECT: ("s"|"S")("e"|"E")("l"|"L")("e"|"E")("c"|"C")("t"|"T")>
  Impact: 150+ lines of repetitive code

Lines 424-425: Compound tokens reference simple tokens
  <BUCKET_IDENTIFIER: <BUCKET> <COLON> <IDENTIFIER>>
  Impact: Creates dependencies preventing bulk token changes
```

### Statement Dispatch Layer
```
Lines 745-878: StatementInternal() with 47 alternatives
  - 30+ with LOOKAHEAD (expensive)
  - 17 without LOOKAHEAD (efficient)

Current pattern:
  LOOKAHEAD(CreateVertexStatement()) result = CreateVertexStatement()

Impact: O(n) linear scan through alternatives
```

### Statement Implementation Layer
```
211 AST node classes generated from grammar
  - Base hierarchy: Statement → QueryStatement/DDLStatement/etc.
  - Execution via createExecutionPlan() → ExecutionStep chain
```

---

## Identified Pain Points

### 1. LOOKAHEAD Complexity

**Location**: `StatementInternal()` lines 745-878

**Issue**: 30 out of 47 statement alternatives use LOOKAHEAD, each requiring parser state exploration.

**Example**:
```java
LOOKAHEAD(CreateVertexStatementNoTarget())
result = CreateVertexStatementNoTarget()
|
LOOKAHEAD(CreateVertexStatement())
result = CreateVertexStatement()
|
LOOKAHEAD(CreateVertexStatementEmpty())
result = CreateVertexStatementEmpty()
```

**Impact**:
- Parsing overhead proportional to number of LOOKAHEADs
- Difficult to add new statement types without careful LOOKAHEAD ordering
- No token-based O(1) dispatch

---

### 2. Statement Variant Explosion

**CreateVertex Variants** (lines 1299-1365):
```
CreateVertexStatementEmptyNoTarget  # CREATE VERTEX
CreateVertexStatementEmpty          # CREATE VERTEX Type
CreateVertexStatement               # CREATE VERTEX Type ... (full syntax)
CreateVertexStatementNoTarget       # CREATE VERTEX SET ...
```

**Why it exists**: Attempting to disambiguate optional clauses at parse time

**Better approach**: Single production with optional clauses, runtime validation if needed

**Similar patterns found in**:
- SELECT statements (SelectStatement vs SelectWithoutTargetStatement)
- CREATE TYPE statements (CreateDocumentTypeStatement, CreateVertexTypeStatement, CreateEdgeTypeStatement)

---

### 3. Verbose Token Definitions

**Current** (lines 115-120):
```javacc
<ALIASES: ("a"|"A")("l"|"L")("i"|"I")("a"|"A")("s"|"S")("e"|"E")("s"|"S")>
<ALIGN: ("a"|"A")("l"|"L")("i"|"I")("g"|"G")("n"|"N")>
<BATCH: ("b"|"B")("a"|"A")("t"|"T")("c"|"C")("h"|"H")>
<SELECT: ("s"|"S")("e"|"E")("l"|"L")("e"|"E")("c"|"C")("t"|"T")>
```

**With IGNORE_CASE**:
```javacc
TOKEN [IGNORE_CASE]:
{
  <ALIASES: "aliases">
  | <ALIGN: "align">
  | <BATCH: "batch">
  | <SELECT: "select">
}
```

**Challenge discovered**: Compound tokens like `<BUCKET_IDENTIFIER: <BUCKET> <COLON> <IDENTIFIER>>` reference simple tokens, creating interdependencies.

**Solution**: Convert tokens in batches, leaving compound tokens for later phase.

---

### 4. Trivial Operator Productions

**Current** (lines 2218-2304):
```javacc
LtOperator LtOperator():
{}
{ <LT> {return jjtThis;} }

GtOperator GtOperator():
{}
{ <GT> {return jjtThis;} }

// ... 10 more similar single-line productions
```

**Impact**:
- 12 separate AST node classes for simple operators
- More files to maintain
- No semantic value - could use enum

**Recommendation**: Consolidate into single `BinaryCompareOperator` production with operator type field.

---

### 5. Inline Semantic Actions

**Example** (lines 2072-2080):
```javacc
<OR>
{
  // Validate that we have a proper condition after OR
  if (getToken(1).kind == AND || getToken(1).kind == OR || ...) {
    throw new ParseException("Syntax error: OR operator...");
  }
}
```

**Issue**: Grammar validation mixed with parsing logic

**Better approach**: Separate validation into post-parse visitor or validator class

---

## Attempted Refactorings & Learnings

### Phase 1: IGNORE_CASE Token Conversion (BLOCKED)

**Approach**: Bulk replacement of verbose case-insensitive tokens with `TOKEN [IGNORE_CASE]`

**Blocker**: Compound tokens (`BUCKET_IDENTIFIER`, `INDEXVALUES_IDENTIFIER`) reference simple tokens, creating circular dependencies when converted.

**Lesson**: Token changes must be done incrementally, compound tokens separately from simple tokens.

**Next steps if pursued**:
1. Convert simple keywords first (SELECT, FROM, WHERE, etc.)
2. Test thoroughly
3. Handle compound tokens in separate phase with new token definitions

---

### Phase 2: CreateVertex Consolidation (NOT ATTEMPTED)

**Plan**: Merge 4 CreateVertex variants into single production with optional clauses

**Risk**: Medium - changes AST node hierarchy, affects existing code

**Next steps if pursued**:
1. Create single unified `CreateVertexStatement` production
2. Update execution layer to handle null optional fields
3. Add tests for all syntax variants
4. Update parser cache if needed

---

### Phase 3: Token-Based Dispatch (BLOCKED - TOO LARGE)

**Approach**: Replace LOOKAHEAD-heavy StatementInternal() with token-based dispatch

**Plan**:
```javacc
Statement StatementInternal() {
  <SELECT> result = SelectStatementBody()
  | <CREATE> result = CreateStatementDispatch()
  | <ALTER> result = AlterStatementDispatch()
  // ... etc
}
```

**Blocker**: Requires creating 25+ "Body" variants of existing statements (statements without leading token since dispatcher consumes it)

**Effort**: High - each statement needs Body variant

**Example needed transformations**:
- `SelectStatement()` → `SelectStatementBody()` (without consuming <SELECT>)
- `CreateDocumentTypeStatement()` → `CreateDocumentTypeStatementBody()` (without <CREATE> <DOCUMENT> <TYPE>)
- And 23 more...

**Lesson**: Full token-based dispatch is a major refactoring requiring careful planning and extensive testing.

**Alternative**: Selective LOOKAHEAD removal (see Quick Wins below)

---

## Recommended Approach

### Strategy: Incremental Improvement

Avoid "big bang" refactorings. Instead, improve the grammar through small, testable changes.

### Priority 1: Quick Wins (Low Risk, High Value)

#### 1.1: Remove Unnecessary LOOKAHEADs

Many statements at the end of StatementInternal() don't need LOOKAHEAD:

**Before**:
```javacc
LOOKAHEAD(BeginStatement())
result = BeginStatement()
|
LOOKAHEAD(CommitStatement())
result = CommitStatement()
```

**After**:
```javacc
result = BeginStatement()  // BEGIN is unique token
|
result = CommitStatement()  // COMMIT is unique token
```

**Statements that can drop LOOKAHEAD** (already start with unique tokens):
- BeginStatement (BEGIN)
- CommitStatement (COMMIT)
- RollbackStatement (ROLLBACK)
- LockStatement (LOCK)
- ReturnStatement (RETURN)
- SleepStatement (SLEEP)
- ConsoleStatement (CONSOLE)
- IfStatement (IF)
- InsertStatement (INSERT)
- MoveVertexStatement (MOVE)
- RebuildIndexStatement (REBUILD)

**Impact**: Eliminates ~11 LOOKAHEAD evaluations with zero risk

**Effort**: 30 minutes

---

#### 1.2: Consolidate Trivial Operator Productions

**Replace 12 operator classes with single enum-based production.**

**Before** (12 separate productions):
```javacc
LtOperator LtOperator():
{}
{ <LT> {return jjtThis;} }

GtOperator GtOperator():
{}
{ <GT> {return jjtThis;} }
// ... 10 more
```

**After** (single production with enum):
```javacc
BinaryCompareOperator CompareOperator():
{
  int op;
}
{
  (
    <LT> { op = BinaryCompareOperator.LT; }
    | <GT> { op = BinaryCompareOperator.GT; }
    | <LE> { op = BinaryCompareOperator.LE; }
    | <GE> { op = BinaryCompareOperator.GE; }
    | <EQ> { op = BinaryCompareOperator.EQ; }
    | <NE> { op = BinaryCompareOperator.NE; }
    | <NEQ> { op = BinaryCompareOperator.NEQ; }
    | <LIKE> { op = BinaryCompareOperator.LIKE; }
    | <ILIKE> { op = BinaryCompareOperator.ILIKE; }
    | <CONTAINSKEY> { op = BinaryCompareOperator.CONTAINSKEY; }
    | <NEAR> { op = BinaryCompareOperator.NEAR; }
    | <WITHIN> { op = BinaryCompareOperator.WITHIN; }
  )
  { jjtThis.setOperator(op); return jjtThis; }
}
```

**Update BinaryCompareOperator class**:
```java
public class BinaryCompareOperator extends SimpleNode {
  public static final int LT = 1;
  public static final int GT = 2;
  // ... etc

  private int operator;

  public void setOperator(int op) {
    this.operator = op;
  }

  public int getOperator() {
    return operator;
  }
}
```

**Impact**: Reduces from 12 classes to 1, cleaner codebase

**Effort**: 2-3 hours (need to update all operator usage sites)

---

### Priority 2: Medium-Term Improvements (Medium Risk, High Value)

#### 2.1: Incremental IGNORE_CASE Conversion

**Phase A: Convert simple keywords** (no compound token dependencies):
```javacc
TOKEN [IGNORE_CASE]:
{
  <SELECT: "select">
  | <FROM: "from">
  | <WHERE: "where">
  | <INSERT: "insert">
  | <UPDATE: "update">
  | <DELETE: "delete">
  | <CREATE: "create">
  | <ALTER: "alter">
  | <DROP: "drop">
  // ... all keywords except BUCKET, INDEX (used in compound tokens)
}
```

**Phase B: Handle compound tokens separately** (after Phase A succeeds):
```javacc
// Keep original verbose tokens that are referenced:
TOKEN:
{
  <BUCKET: ("B"|"b")("U"|"u")("C"|"c")("K"|"k")("E"|"e")("T"|"t")>
  | <INDEX: ("I"|"i")("N"|"n")("D"|"d")("E"|"e")("X"|"x")>
}

// Compound tokens still reference the above:
TOKEN:
{
  <BUCKET_IDENTIFIER: <BUCKET> <COLON> <IDENTIFIER>>
  | <INDEX_COLON: <INDEX> ":">
}
```

**Impact**: Reduces ~120 lines of verbose token definitions

**Effort**: 4-6 hours (test thoroughly after each batch)

**Risk**: Medium - requires careful testing of all SQL syntax

---

#### 2.2: Consolidate Statement Variants

**Target**: CreateVertex, Select, and other multi-variant statements

**Approach**:
1. Choose one statement variant to expand (e.g., CreateVertexStatement)
2. Make all clauses optional
3. Remove other variants
4. Update StatementInternal() dispatch
5. Test all syntax combinations

**Example for CreateVertex**:

**Before** (4 variants):
- CreateVertexStatement
- CreateVertexStatementEmpty
- CreateVertexStatementNoTarget
- CreateVertexStatementEmptyNoTarget

**After** (1 variant):
```javacc
CreateVertexStatement CreateVertexStatement():
{}
{
  <CREATE> <VERTEX>
  [
    // Parse target only if not immediately followed by InsertBody
    LOOKAHEAD( { getToken(1).kind != SET && getToken(1).kind != CONTENT && getToken(1).kind != LPAREN } )
    (
      LOOKAHEAD( Bucket() )
      jjtThis.targetBucket = Bucket()
      |
      jjtThis.targetType = Identifier()
      [
        <BUCKET>
        jjtThis.targetBucketName = Identifier()
      ]
    )
  ]
  [ <RETURN> jjtThis.returnStatement = Projection() ]
  [ LOOKAHEAD(InsertBody()) jjtThis.insertBody = InsertBody() ]
  {return jjtThis;}
}
```

**Impact**: Reduces 4 classes to 1, simpler maintenance

**Effort**: 8-10 hours per statement group

**Risk**: Medium-High - changes AST class hierarchy

---

### Priority 3: Long-Term (High Risk, High Reward)

#### 3.1: Full Token-Based Dispatch

**Only pursue if**:
- Team has bandwidth for large refactoring
- Comprehensive test suite exists
- Performance profiling shows dispatch as bottleneck

**Required work**:
1. Create 25+ "Body" variants of statements
2. Update all statement dispatchers
3. Extensive testing
4. Update documentation

**Estimated effort**: 40-60 hours

**Benefit**: O(1) token dispatch vs O(n) LOOKAHEAD scan

**Alternative**: Keep current structure, focus on selective LOOKAHEAD removal

---

## Implementation Checklist

### Before Any Change
- [ ] Run full test suite: `mvn test -pl engine`
- [ ] Note baseline metrics (if measuring performance)
- [ ] Create feature branch: `git checkout -b grammar-refactor-phaseX`

### After Each Change
- [ ] Compile: `mvn compile -pl engine`
- [ ] Fix ALL compilation errors before proceeding
- [ ] Run parser tests: `mvn test -pl engine -Dtest="*Parser*,*Sql*"`
- [ ] Run full engine tests: `mvn test -pl engine`
- [ ] Commit: `git commit -m "grammar: <description of change>"`

### Final Checks
- [ ] All tests pass
- [ ] No new warnings introduced
- [ ] Grammar file compiles cleanly
- [ ] Parser cache cleared if needed
- [ ] Documentation updated if syntax changed

---

## Performance Considerations

### Current Bottlenecks (Theoretical)

1. **LOOKAHEAD Evaluation**: Each LOOKAHEAD can scan multiple tokens ahead
2. **Statement Dispatch**: O(n) linear scan through 47 alternatives
3. **Token Manager**: Verbose case-insensitive patterns

### Measurement Needed

Before claiming "performance improvement", measure actual impact:

```java
// In QueryLanguageBenchmark.java or similar
long start = System.nanoTime();
for (int i = 0; i < 10000; i++) {
  database.command("sql", "SELECT * FROM V WHERE name = 'test'");
}
long duration = System.nanoTime() - start;
System.out.println("Avg parse time: " + (duration / 10000) + " ns");
```

**Hypothesis**: Parser dispatch is likely NOT the bottleneck. Execution planning and actual query execution dominate.

**Recommendation**: Profile before optimizing. Grammar clarity may be more valuable than micro-optimization.

---

## Alternative: Keep Current Structure

### Arguments for Minimal Changes

1. **Grammar works**: All tests pass, syntax is correct
2. **Proven stability**: Inherited from OrientDB, battle-tested
3. **Low risk**: Fewer chances to introduce bugs
4. **Team familiarity**: Developers know the current structure

### When to Refactor

- [ ] Performance profiling shows parser as bottleneck (evidence needed)
- [ ] Adding new features is consistently difficult due to grammar complexity
- [ ] Grammar has grown significantly (e.g., >6000 lines)
- [ ] Team has dedicated time for careful refactoring + testing

### When NOT to Refactor

- [ ] "It could be better" without concrete pain points
- [ ] No bandwidth for thorough testing
- [ ] Active development cycle (wait for quiet period)
- [ ] No performance evidence

---

## Conclusion

The ArcadeDB SQL grammar is **functionally correct but has technical debt**. The recommended approach is **incremental improvement**:

**Start here** (lowest risk, immediate value):
1. Remove unnecessary LOOKAHEADs (30 min)
2. Consolidate operator productions (2-3 hours)

**Then consider** (if time permits):
3. Incremental IGNORE_CASE conversion (4-6 hours)
4. Statement variant consolidation (8-10 hours per group)

**Avoid for now** (unless critical need):
5. Full token-based dispatch (40-60 hours)

**Key principle**: Test after every change, commit incrementally, maintain backward compatibility.

---

## References

- JavaCC IGNORE_CASE: [JavaCC Grammar Documentation](https://javacc.github.io/javacc/documentation/grammar.html)
- ANTLR4 SQL Grammars: [grammars-v4 repository](https://github.com/antlr/grammars-v4/tree/master/sql)
- ArcadeDB Grammar: `engine/src/main/grammar/SQLGrammar.jjt`
- ArcadeDB Parser Tests: `engine/src/test/java/com/arcadedb/query/sql/`

---

**Created with**: [Claude Code](https://claude.ai/code) via [Happy](https://happy.engineering)
