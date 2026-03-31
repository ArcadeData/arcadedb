# Seal JavaCC SQL Parser Escape Hatches — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make it impossible to accidentally invoke the legacy JavaCC `SqlParser` from any live code path, without deleting its source files.

**Architecture:** Three-part migration. First remove the `SQL_PARSER_IMPLEMENTATION` config switch so neither `StatementCache` nor `SQLScriptQueryEngine` can route to JavaCC. Then replace the two hardwired production usages (`AbstractProperty`, `SQLFunctionEval`) with equivalent `SQLAntlrParser` calls that already exist. Finally migrate 11 test files from inline `SqlParser` instantiation to `AbstractParserTest` / `SQLAntlrParser` — running each one after migration to catch any grammar gaps.

**Tech Stack:** Java 21, ANTLR4, JUnit 5 / AssertJ, Maven

**Working directory:** `/Users/frank/projects/arcade/worktrees/worktrees/seal-javacc`
**Run tests from:** `engine/` subdirectory (`cd engine && mvn test ...`)

---

## Part 1 — Remove the config switch

### Task 1: Simplify `StatementCache.parse()` — always use ANTLR

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/parser/StatementCache.java`

**Step 1: Remove `useAntlrParser()` and `parseWithJavaCC()`, simplify `parse()`**

Replace the block from `useAntlrParser()` through `parseWithJavaCC()` with a direct ANTLR call.

Current code (lines ~84–129):
```java
private boolean useAntlrParser() {
  String parserType = GlobalConfiguration.SQL_PARSER_IMPLEMENTATION.getValueAsString();
  if (db != null)
    parserType = db.getConfiguration().getValueAsString(GlobalConfiguration.SQL_PARSER_IMPLEMENTATION);
  return !"javacc".equalsIgnoreCase(parserType);
}

protected Statement parse(final String statement) throws CommandSQLParsingException {
  try {
    final Statement result;
    if (useAntlrParser()) {
      result = antlrParser.parse(statement);
    } else {
      result = parseWithJavaCC(statement);
    }
    result.originalStatementAsString = statement;
    return result;
  } catch (final CommandSQLParsingException e) {
    throw e;
  } catch (final Throwable e) {
    throwParsingException(e, statement);
  }
  return null;
}

private Statement parseWithJavaCC(final String statement) throws ParseException {
  final InputStream is = new ByteArrayInputStream(statement.getBytes(StandardCharsets.UTF_8));
  final SqlParser parser = new SqlParser(db, is);
  return parser.Parse();
}
```

Replace with:
```java
protected Statement parse(final String statement) throws CommandSQLParsingException {
  try {
    final Statement result = antlrParser.parse(statement);
    result.originalStatementAsString = statement;
    return result;
  } catch (final CommandSQLParsingException e) {
    throw e;
  } catch (final Throwable e) {
    throwParsingException(e, statement);
  }
  return null;
}
```

Also remove any now-unused imports: `SqlParser`, `ParseException`, `ByteArrayInputStream`, `StandardCharsets`, `GlobalConfiguration` (only if no longer used elsewhere in the file — check first).

**Step 2: Compile**

```bash
cd engine && mvn compile -q --no-transfer-progress
```
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/parser/StatementCache.java
git commit -m "refactor: remove JavaCC fallback from StatementCache"
```

---

### Task 2: Remove JavaCC branch from `SQLScriptQueryEngine.parseScript()`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/SQLScriptQueryEngine.java`

**Step 1: Simplify `parseScript()`**

Current code (lines ~133–151):
```java
public static List<Statement> parseScript(final String script, final DatabaseInternal database) {
  try {
    final String parserType = database.getConfiguration().getValueAsString(GlobalConfiguration.SQL_PARSER_IMPLEMENTATION);
    if ("javacc".equalsIgnoreCase(parserType)) {
      final InputStream is = new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8));
      final SqlParser parser = new SqlParser(database, is);
      return parser.ParseScript();
    } else {
      final SQLAntlrParser parser = new SQLAntlrParser(database);
      return parser.parseScript(script);
    }
  } catch (final CommandSQLParsingException e) {
    throw e.setCommand(script);
  } catch (final ParseException e) {
    throw new CommandSQLParsingException(e.getMessage(), e, script);
  }
}
```

Replace with:
```java
public static List<Statement> parseScript(final String script, final DatabaseInternal database) {
  try {
    return new SQLAntlrParser(database).parseScript(script);
  } catch (final CommandSQLParsingException e) {
    throw e.setCommand(script);
  }
}
```

Remove now-unused imports: `SqlParser`, `ParseException`, `ByteArrayInputStream`, `StandardCharsets`, `GlobalConfiguration` (check each — only remove if unused).

**Step 2: Compile**

```bash
cd engine && mvn compile -q --no-transfer-progress
```
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/SQLScriptQueryEngine.java
git commit -m "refactor: remove JavaCC fallback from SQLScriptQueryEngine"
```

---

### Task 3: Deprecate `SQL_PARSER_IMPLEMENTATION` in `GlobalConfiguration`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`

**Step 1: Mark the config entry as deprecated**

Find the `SQL_PARSER_IMPLEMENTATION` entry (~line 261):
```java
SQL_PARSER_IMPLEMENTATION("arcadedb.sql.parserImplementation", SCOPE.DATABASE,
    """
    SQL parser implementation to use. 'antlr' (default) uses the new ANTLR4-based parser with improved error messages. \
    'javacc' uses the legacy JavaCC-based parser for backward compatibility.""",
    String.class, "antlr", Set.of("antlr", "javacc")),
```

Replace the description to make it clear the option is deprecated and has no effect:
```java
SQL_PARSER_IMPLEMENTATION("arcadedb.sql.parserImplementation", SCOPE.DATABASE,
    "@Deprecated - this setting has no effect. The ANTLR4-based SQL parser is always used.",
    String.class, "antlr"),
```

**Step 2: Compile**

```bash
cd engine && mvn compile -q --no-transfer-progress
```
Expected: BUILD SUCCESS

**Step 3: Run Part 1 tests**

```bash
cd engine && mvn test -Dtest="*ParserTest*,*StatementTest*,*ScriptTest*" --no-transfer-progress 2>&1 | tail -15
```
Expected: All tests pass (238+ tests, 0 failures)

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java
git commit -m "deprecate: SQL_PARSER_IMPLEMENTATION config — ANTLR is always used now"
```

---

## Part 2 — Migrate direct production usages

### Task 4: Migrate `AbstractProperty.getDefaultValue()` to ANTLR

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/AbstractProperty.java`

**Step 1: Replace JavaCC call with `SQLAntlrParser.parseExpression()`**

Find the `getDefaultValue()` method (~line 100):
```java
expr = new SqlParser(database, new ByteArrayInputStream(defaultValue.toString().getBytes())).ParseExpression();
```

Replace:
```java
expr = new SQLAntlrParser(database).parseExpression(defaultValue.toString());
```

Update imports: remove `SqlParser`, `ParseException`, `ByteArrayInputStream`; add `com.arcadedb.query.sql.antlr.SQLAntlrParser`.
Update the catch clause: `catch (ParseException e)` → `catch (Exception e)` (both are ignored, behavior unchanged).

**Step 2: Compile**

```bash
cd engine && mvn compile -q --no-transfer-progress
```
Expected: BUILD SUCCESS

**Step 3: Run schema tests**

```bash
cd engine && mvn test -Dtest="*Property*,*Schema*,*Default*" --no-transfer-progress 2>&1 | tail -10
```
Expected: BUILD SUCCESS (or note any failures for investigation)

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/schema/AbstractProperty.java
git commit -m "refactor: migrate AbstractProperty default value parsing to SQLAntlrParser"
```

---

### Task 5: Migrate `SQLFunctionEval.execute()` to ANTLR

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionEval.java`

**Step 1: Replace JavaCC call with `SQLAntlrParser.parseCondition()`**

Find the `execute()` method (~line 50):
```java
try (final ByteArrayInputStream is = new ByteArrayInputStream(params[0].toString().getBytes())) {
  predicate = new SqlParser(context.getDatabase(), is).ParseCondition();
} catch (IOException e) {
  throw new CommandSQLParsingException("Error on parsing expression in eval() function", e);
} catch (ParseException e) {
  throw new CommandSQLParsingException("Error on parsing expression for the eval()", e);
}
```

Replace with:
```java
try {
  predicate = new SQLAntlrParser(context.getDatabase()).parseCondition(params[0].toString());
} catch (final CommandSQLParsingException e) {
  throw e;
} catch (final Exception e) {
  throw new CommandSQLParsingException("Error on parsing expression for the eval()", e);
}
```

Update imports: remove `SqlParser`, `ParseException`, `ByteArrayInputStream`, `IOException`; add `com.arcadedb.query.sql.antlr.SQLAntlrParser`.

**Step 2: Compile**

```bash
cd engine && mvn compile -q --no-transfer-progress
```
Expected: BUILD SUCCESS

**Step 3: Run eval() function tests**

```bash
cd engine && mvn test -Dtest="*Eval*,*Function*,*SQLFunction*" --no-transfer-progress 2>&1 | tail -10
```
Expected: BUILD SUCCESS

**Step 4: Verify no `new SqlParser` in production code**

```bash
grep -r "new SqlParser" engine/src/main/java/
```
Expected: zero results

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionEval.java
git commit -m "refactor: migrate SQLFunctionEval eval() parsing to SQLAntlrParser"
```

---

## Part 3 — Migrate the 11 test files

**Pattern for each file:** These tests have their own inline `checkRightSyntax` / `checkWrongSyntax` / `checkSyntax` / `getParserFor` methods that duplicate `AbstractParserTest`. The migration for most files is:
1. Add `extends AbstractParserTest` to the class declaration
2. Delete the duplicated helper methods
3. Run the test — fix any grammar/AST gaps that surface

`BatchScriptTest` and `ProjectionTest` have different internal patterns and need targeted fixes.

---

### Task 6: Migrate `UpdateStatementTest`, `InsertStatementTest`, `DeleteStatementTest`, `CreateVertexStatementTest`, `CreateEdgeStatementTest`, `TraverseStatementTest`

These 6 files all follow the same pattern as `SelectStatementTest` but are smaller (74–306 lines). Do them together.

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/UpdateStatementTest.java`
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/InsertStatementTest.java`
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/DeleteStatementTest.java`
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/CreateVertexStatementTest.java`
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/CreateEdgeStatementTest.java`
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/TraverseStatementTest.java`

**Step 1: For each file, apply the same change**

a) Change `class FooStatementTest {` → `class FooStatementTest extends AbstractParserTest {`
b) Delete the duplicated `checkRightSyntax`, `checkWrongSyntax`, `checkSyntax`, `getParserFor` methods
c) Remove imports that become unused: `SqlParser`, `ByteArrayInputStream`, `InputStream`, `fail` (if it came from a static import — check if `AbstractParserTest` brings it in already)

**Step 2: Compile**

```bash
cd engine && mvn compile -t src/test/java --no-transfer-progress 2>&1 | grep -E "ERROR|SUCCESS"
```

Or simply:
```bash
cd engine && mvn test-compile -q --no-transfer-progress
```
Expected: BUILD SUCCESS

**Step 3: Run these 6 tests**

```bash
cd engine && mvn test -Dtest="UpdateStatementTest,InsertStatementTest,DeleteStatementTest,CreateVertexStatementTest,CreateEdgeStatementTest,TraverseStatementTest" --no-transfer-progress 2>&1 | tail -15
```
Expected: All pass. If a test fails, the ANTLR grammar has a gap — fix it using the same approach from #3738 (update `SQLParser.g4` and/or `SQLASTBuilder.java`).

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/query/sql/parser/UpdateStatementTest.java \
        engine/src/test/java/com/arcadedb/query/sql/parser/InsertStatementTest.java \
        engine/src/test/java/com/arcadedb/query/sql/parser/DeleteStatementTest.java \
        engine/src/test/java/com/arcadedb/query/sql/parser/CreateVertexStatementTest.java \
        engine/src/test/java/com/arcadedb/query/sql/parser/CreateEdgeStatementTest.java \
        engine/src/test/java/com/arcadedb/query/sql/parser/TraverseStatementTest.java
git commit -m "refactor: migrate 6 statement tests to AbstractParserTest (ANTLR)"
```

---

### Task 7: Migrate `MatchStatementTest`

`MatchStatementTest` is similar to the above but uses the `MatchStatement` AST type — migrate it separately so failures are isolated.

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/MatchStatementTest.java`

**Step 1: Apply the same change as Task 6**

a) `class MatchStatementTest {` → `class MatchStatementTest extends AbstractParserTest {`
b) Delete `checkRightSyntax`, `checkWrongSyntax`, `checkSyntax`, `getParserFor`
c) Remove unused imports

**Step 2: Run the test**

```bash
cd engine && mvn test -Dtest="MatchStatementTest" --no-transfer-progress 2>&1 | tail -15
```

**Step 3: Fix any grammar gaps** — refer to `SQLParser.g4` MATCH rules if failures occur.

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/query/sql/parser/MatchStatementTest.java
git commit -m "refactor: migrate MatchStatementTest to AbstractParserTest (ANTLR)"
```

---

### Task 8: Migrate `SelectStatementTest` (720 lines — highest risk)

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/SelectStatementTest.java`

**Step 1: Extend AbstractParserTest and remove duplicates**

a) `class SelectStatementTest {` → `class SelectStatementTest extends AbstractParserTest {`
b) Delete `checkRightSyntax`, `checkWrongSyntax`, `checkSyntax`, `getParserFor` methods (lines ~32–68)
c) Remove unused imports: `SqlParser`, `ByteArrayInputStream`, `InputStream`
d) The tests that cast `SimpleNode` to `SelectStatement` and inspect structure are fine — `AbstractParserTest.checkRightSyntax` returns `SimpleNode`, and the tests already cast it:
   ```java
   final SimpleNode stm = checkRightSyntax("select from Foo");
   assertThat(stm instanceof SelectStatement).isTrue();
   final SelectStatement select = (SelectStatement) stm;
   ```
   This pattern works unchanged.

**Step 2: Run the test — expect failures, fix them**

```bash
cd engine && mvn test -Dtest="SelectStatementTest" --no-transfer-progress 2>&1 | grep -E "FAIL|ERROR|Tests run" | head -30
```

For each failure:
- Read the failing query
- Test it manually: does ANTLR reject it? Is it a grammar gap or a legitimate syntax error?
- If grammar gap: fix `SQLParser.g4` and/or `SQLASTBuilder.java` using the same process as #3738
- After any grammar fix, run `rm -rf engine/target/generated-sources/antlr4 && mvn compile -q --no-transfer-progress` to force regeneration

Repeat until all tests pass.

**Step 3: Commit (even partial — commit grammar fixes separately)**

```bash
# Grammar fixes (if any):
git add engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4
git add engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java
git commit -m "fix: ANTLR grammar gaps exposed by SelectStatementTest"

# Test migration:
git add engine/src/test/java/com/arcadedb/query/sql/parser/SelectStatementTest.java
git commit -m "refactor: migrate SelectStatementTest to AbstractParserTest (ANTLR)"
```

---

### Task 9: Migrate `OperationTypeTest`

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/OperationTypeTest.java`

**Step 1: Replace the inline parse helper**

Find the helper (~line 33–36):
```java
return new SqlParser(null, query).Parse();
```

Replace with:
```java
return new SQLAntlrParser(null).parse(query);
```

Where `query` is a `String`. Update the surrounding method signature if needed (the parameter type may be `InputStream` — if so, change callers to pass the string directly).

Remove unused imports: `SqlParser`, `ByteArrayInputStream`, `InputStream`. Add: `import com.arcadedb.query.sql.antlr.SQLAntlrParser;`

**Step 2: Run the test**

```bash
cd engine && mvn test -Dtest="OperationTypeTest" --no-transfer-progress 2>&1 | tail -10
```

**Step 3: Commit**

```bash
git add engine/src/test/java/com/arcadedb/query/sql/parser/OperationTypeTest.java
git commit -m "refactor: migrate OperationTypeTest to SQLAntlrParser"
```

---

### Task 10: Migrate `ProjectionTest`

`ProjectionTest` uses `getParserFor(query)` to get a `SqlParser` and then calls `.Parse()` directly to inspect `Projection` — it does not use `checkRightSyntax`. It also expects `select expand(foo), bar from V` to throw `CommandSQLParsingException`.

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/ProjectionTest.java`

**Step 1: Replace `getParserFor(query).Parse()` with `new SQLAntlrParser(null).parse(query)`**

Current pattern:
```java
final SqlParser parser = getParserFor("select expand(foo)  from V");
final SelectStatement stm = (SelectStatement) parser.Parse();
```

Replace with:
```java
final SelectStatement stm = (SelectStatement) new SQLAntlrParser(null).parse("select expand(foo)  from V");
```

Apply same replacement for all three occurrences. Remove the `getParserFor` helper method entirely.

For the `validate()` test that expects an exception:
```java
try {
  getParserFor("select expand(foo), bar  from V").Parse();
  fail("");
} catch (final CommandSQLParsingException ex) {
} catch (final Exception x) {
  fail("");
}
```

Replace with:
```java
try {
  new SQLAntlrParser(null).parse("select expand(foo), bar  from V");
  fail("");
} catch (final CommandSQLParsingException ex) {
  // expected
}
```

Remove unused imports: `SqlParser`, `ByteArrayInputStream`, `InputStream`. Add: `import com.arcadedb.query.sql.antlr.SQLAntlrParser;`

**Step 2: Run the test**

```bash
cd engine && mvn test -Dtest="ProjectionTest" --no-transfer-progress 2>&1 | tail -10
```

Note: The `validate()` test expects ANTLR to reject `select expand(foo), bar from V`. Verify this is the case. If ANTLR accepts it (parse succeeds but `Projection.validate()` throws), the test logic may need adjusting — wrap the `.parse()` call and also call `.validate()` on the projection.

**Step 3: Commit**

```bash
git add engine/src/test/java/com/arcadedb/query/sql/parser/ProjectionTest.java
git commit -m "refactor: migrate ProjectionTest to SQLAntlrParser"
```

---

### Task 11: Migrate `BatchScriptTest`

`BatchScriptTest` uses `ParseScript()` instead of `Parse()`. Replace with `SQLAntlrParser.parseScript()`.

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/query/sql/parser/BatchScriptTest.java`

**Step 1: Replace the `checkSyntax` and `getParserFor` helpers**

Current `checkSyntax` (lines ~98–117):
```java
protected List<Statement> checkSyntax(final String query, final boolean isCorrect) {
  final SqlParser osql = getParserFor(query);
  try {
    final List<Statement> result = osql.ParseScript();
    if (!isCorrect) fail("");
    return result;
  } catch (final Exception e) {
    if (isCorrect) { e.printStackTrace(); fail(""); }
  }
  return null;
}

protected SqlParser getParserFor(final String string) {
  final InputStream is = new ByteArrayInputStream(string.getBytes());
  final SqlParser osql = new SqlParser(null, is);
  return osql;
}
```

Replace both with:
```java
protected List<Statement> checkSyntax(final String query, final boolean isCorrect) {
  try {
    final List<Statement> result = new SQLAntlrParser(null).parseScript(query);
    if (!isCorrect) fail("");
    return result;
  } catch (final Exception e) {
    if (isCorrect) { e.printStackTrace(); fail(""); }
  }
  return null;
}
```

Remove unused imports: `SqlParser`, `ByteArrayInputStream`, `InputStream`. Add: `import com.arcadedb.query.sql.antlr.SQLAntlrParser;`

**Step 2: Run the test**

```bash
cd engine && mvn test -Dtest="BatchScriptTest" --no-transfer-progress 2>&1 | tail -10
```

**Step 3: Commit**

```bash
git add engine/src/test/java/com/arcadedb/query/sql/parser/BatchScriptTest.java
git commit -m "refactor: migrate BatchScriptTest to SQLAntlrParser.parseScript()"
```

---

## Final Verification

### Task 12: Confirm zero `new SqlParser` references in live code

**Step 1: Grep production and test source**

```bash
grep -r "new SqlParser" engine/src/main/java/ engine/src/test/java/
```
Expected: **zero results**

**Step 2: Run the full parser/statement/script test suite**

```bash
cd engine && mvn test -Dtest="*ParserTest*,*StatementTest*,*ScriptTest*" --no-transfer-progress 2>&1 | tail -10
```
Expected: All tests pass, 0 failures

**Step 3: Push**

```bash
git push origin fix/seal-javacc-escape-hatches
```
