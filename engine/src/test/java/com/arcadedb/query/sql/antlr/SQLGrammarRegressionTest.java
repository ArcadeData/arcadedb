/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.antlr;

import com.arcadedb.query.sql.parser.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for ANTLR SQL grammar fixes.
 * Tests parser-level acceptance of various SQL syntax patterns that were previously failing.
 * These tests focus on PARSING only - not execution.
 */
public class SQLGrammarRegressionTest {

  private final SQLAntlrParser parser = new SQLAntlrParser(null);

  // ============================================================================
  // Reserved Keywords as Identifiers
  // ============================================================================

  @Test
  public void testReservedKeywordsAsIdentifiers() {
    // Test that common reserved keywords can be used as field/property names
    assertParses("SELECT value FROM MyType");
    assertParses("SELECT values FROM MyType");
    assertParses("SELECT type FROM MyType");
    assertParses("SELECT status FROM MyType");
    assertParses("SELECT count FROM MyType");
    assertParses("SELECT date FROM MyType");
    assertParses("SELECT time FROM MyType");
    assertParses("SELECT timestamp FROM MyType");
    assertParses("SELECT name FROM MyType");
    assertParses("SELECT start FROM MyType");
    assertParses("SELECT content FROM MyType");
    assertParses("SELECT rid FROM MyType");
    assertParses("SELECT add FROM MyType");
    assertParses("SELECT set FROM MyType");
  }

  @Test
  public void testReservedKeywordsInInsert() {
    assertParses("INSERT INTO MyType SET value = 'test'");
    assertParses("INSERT INTO MyType SET status = 'active'");
    assertParses("INSERT INTO MyType SET start = 10");
    assertParses("INSERT INTO MyType SET content = 'data'");
  }

  @Test
  public void testReservedKeywordsInUpdate() {
    assertParses("UPDATE MyType SET value = 'new'");
    assertParses("UPDATE MyType SET status = 'pending'");
  }

  // ============================================================================
  // UPDATE REMOVE Clause
  // ============================================================================

  @Test
  public void testUpdateRemoveSimple() {
    assertParses("UPDATE MyType REMOVE field1");
    assertParses("UPDATE MyType REMOVE field1, field2, field3");
  }

  @Test
  public void testUpdateRemoveWithEquals() {
    // REMOVE field = value (removes specific value from collection)
    assertParses("UPDATE MyType REMOVE items = 'item1'");
    assertParses("UPDATE MyType REMOVE tags = 'obsolete'");
  }

  @Test
  public void testUpdateRemoveMixed() {
    assertParses("UPDATE MyType SET x = 1 REMOVE field1");
    assertParses("UPDATE MyType REMOVE field1 SET x = 1");
    assertParses("UPDATE MyType SET x = 1 REMOVE field1, field2 WHERE id = 5");
  }

  // ============================================================================
  // RETURN Statement Without Value
  // ============================================================================

  @Test
  public void testReturnEmpty() {
    final List<Statement> statements = parser.parseScript("RETURN;");
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof ReturnStatement);
    final ReturnStatement ret = (ReturnStatement) statements.get(0);
    assertNull(ret.expression, "RETURN without value should have null expression");
  }

  @Test
  public void testReturnWithValue() {
    final List<Statement> statements = parser.parseScript("RETURN $result;");
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof ReturnStatement);
    final ReturnStatement ret = (ReturnStatement) statements.get(0);
    assertNotNull(ret.expression, "RETURN with value should have expression");
  }

  @Test
  public void testReturnInScript() {
    final String script = """
        LET $x = 10;
        IF ($x > 5) {
          RETURN;
        }
        RETURN $x;
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(3, statements.size());
  }

  // ============================================================================
  // TRUNCATE POLYMORPHIC
  // ============================================================================

  @Test
  public void testTruncateTypePolymorphic() {
    final Statement stmt = assertParses("TRUNCATE TYPE MyType POLYMORPHIC");
    assertTrue(stmt instanceof TruncateTypeStatement);
    final TruncateTypeStatement truncate = (TruncateTypeStatement) stmt;
    assertTrue(truncate.polymorphic, "POLYMORPHIC flag should be set");
  }

  @Test
  public void testTruncateTypeUnsafe() {
    final Statement stmt = assertParses("TRUNCATE TYPE MyType UNSAFE");
    assertTrue(stmt instanceof TruncateTypeStatement);
    final TruncateTypeStatement truncate = (TruncateTypeStatement) stmt;
    assertTrue(truncate.unsafe, "UNSAFE flag should be set");
  }

  @Test
  public void testTruncateTypePolymorphicUnsafe() {
    final Statement stmt = assertParses("TRUNCATE TYPE MyType POLYMORPHIC UNSAFE");
    assertTrue(stmt instanceof TruncateTypeStatement);
    final TruncateTypeStatement truncate = (TruncateTypeStatement) stmt;
    assertTrue(truncate.polymorphic && truncate.unsafe, "Both flags should be set");
  }

  // ============================================================================
  // Method Calls on Expressions
  // ============================================================================

  @Test
  public void testMethodCallsOnLiterals() {
    assertParses("SELECT [{'x':1,'y':2}].keys() AS keys");
    assertParses("SELECT [{'x':1,'y':2}].values() AS values");
    assertParses("SELECT [1,2,3].size() AS count");
  }

  @Test
  public void testMethodCallsOnFields() {
    assertParses("SELECT name.toLowerCase() FROM User");
    assertParses("SELECT name.toUpperCase() FROM User");
    assertParses("SELECT name.trim() FROM User");
  }

  @Test
  public void testConversionMethods() {
    assertParses("SELECT number.asDecimal() FROM MyType");
    assertParses("SELECT number.asInteger() FROM MyType");
    assertParses("SELECT number.asLong() FROM MyType");
    assertParses("SELECT number.asFloat() FROM MyType");
    assertParses("SELECT string.asString() FROM MyType");
    assertParses("SELECT number.asDate() FROM MyType");
    assertParses("SELECT number.asDateTime() FROM MyType");
  }

  @Test
  public void testMethodCallsWithParameters() {
    assertParses("SELECT name.substring(0, 5) FROM User");
    assertParses("SELECT dateAsString.asDate('yyyy-MM-dd') FROM MyType");
  }

  @Test
  public void testChainedMethodCalls() {
    assertParses("SELECT name.trim().toLowerCase() FROM User");
    assertParses("SELECT data.keys().size() FROM MyType");
  }

  // ============================================================================
  // INSERT FROM SELECT
  // ============================================================================

  @Test
  public void testInsertFromSelectSimple() {
    // Simple INSERT FROM SELECT works (basic SELECT without WHERE/LIMIT/etc)
    assertParses("INSERT INTO dst FROM SELECT * FROM src");
    assertParses("INSERT INTO dst FROM SELECT a, b FROM src");
  }

  @Test
  public void testInsertFromSelectWithParentheses() {
    // Parenthesized SELECT works for simple queries
    assertParses("INSERT INTO dst FROM (SELECT * FROM src)");
    assertParses("INSERT INTO dst (SELECT * FROM src)");

    // Note: Complex SELECT statements (with WHERE, ORDER BY, etc.) currently have
    // grammar limitations. This is a known issue being addressed.
    // For now, complex queries should be executed as separate steps.
  }

  @Test
  public void testInsertFromSelectWithReturn() {
    assertParses("INSERT INTO dst RETURN @rid FROM SELECT * FROM src");
  }

  // ============================================================================
  // BREAK Statement (Script-only)
  // ============================================================================

  @Test
  public void testBreakInForeach() {
    final String script = """
        FOREACH ($i IN [1, 2, 3]) {
          IF ($i = 2) {
            BREAK;
          }
        }
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof ForEachBlock);
  }

  @Test
  public void testBreakInWhile() {
    final String script = """
        WHILE ($x < 10) {
          IF ($x = 5) {
            BREAK;
          }
          LET $x = $x + 1;
        }
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof WhileBlock);
  }

  @Test
  public void testNestedBreak() {
    final String script = """
        FOREACH ($i IN [1, 2, 3]) {
          FOREACH ($j IN ['A', 'B']) {
            IF ($i = 2 AND $j = 'B') {
              BREAK;
            }
          }
        }
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(1, statements.size());
  }

  // ============================================================================
  // BEGIN ISOLATION and COMMIT RETRY
  // ============================================================================

  @Test
  public void testBeginIsolation() {
    final Statement stmt = assertParses("BEGIN ISOLATION REPEATABLE_READ");
    assertTrue(stmt instanceof BeginStatement);
    final BeginStatement begin = (BeginStatement) stmt;
    assertNotNull(begin.isolation, "Isolation level should be set");
    assertEquals("REPEATABLE_READ", begin.isolation.getStringValue());
  }

  @Test
  public void testBeginWithoutIsolation() {
    final Statement stmt = assertParses("BEGIN");
    assertTrue(stmt instanceof BeginStatement);
    final BeginStatement begin = (BeginStatement) stmt;
    assertNull(begin.isolation, "Isolation level should be null");
  }

  @Test
  public void testCommitRetry() {
    final Statement stmt = assertParses("COMMIT RETRY 10");
    assertTrue(stmt instanceof CommitStatement);
    final CommitStatement commit = (CommitStatement) stmt;
    assertNotNull(commit.retry, "Retry count should be set");
    assertEquals(10, commit.retry.getValue().intValue());
  }

  @Test
  public void testCommitWithoutRetry() {
    final Statement stmt = assertParses("COMMIT");
    assertTrue(stmt instanceof CommitStatement);
    final CommitStatement commit = (CommitStatement) stmt;
    assertNull(commit.retry, "Retry count should be null");
  }

  @Test
  public void testTransactionScript() {
    final String script = """
        BEGIN ISOLATION REPEATABLE_READ;
        INSERT INTO MyType SET x = 1;
        COMMIT RETRY 10;
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(3, statements.size());
    assertTrue(statements.get(0) instanceof BeginStatement);
    assertTrue(statements.get(1) instanceof InsertStatement);
    assertTrue(statements.get(2) instanceof CommitStatement);
  }

  // ============================================================================
  // LET with Statements (not just expressions)
  // ============================================================================

  @Test
  public void testLetWithInsert() {
    final String script = "LET $a = INSERT INTO MyType SET x = 1";
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof LetStatement);
  }

  @Test
  public void testLetWithSelect() {
    final String script = "LET $result = SELECT * FROM MyType";
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof LetStatement);
  }

  @Test
  public void testLetWithParenthesizedStatement() {
    final String script = "LET $result = (SELECT * FROM MyType)";
    final List<Statement> statements = parser.parseScript(script);
    assertEquals(1, statements.size());
    assertTrue(statements.get(0) instanceof LetStatement);
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Assert that a SQL string parses successfully without errors.
   * @param sql The SQL statement to parse
   * @return The parsed Statement
   */
  private Statement assertParses(final String sql) {
    try {
      final Statement stmt = parser.parse(sql);
      assertNotNull(stmt, "Parser should return a statement for: " + sql);
      return stmt;
    } catch (final Exception e) {
      fail("Failed to parse SQL: " + sql + "\nError: " + e.getMessage(), e);
      return null;
    }
  }

  /**
   * Assert that a SQL string fails to parse with an error.
   * @param sql The SQL statement that should fail
   */
  private void assertFails(final String sql) {
    try {
      parser.parse(sql);
      fail("Should have failed to parse: " + sql);
    } catch (final Exception e) {
      // Expected - test passes
    }
  }
}
