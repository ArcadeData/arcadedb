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

import static org.assertj.core.api.Assertions.*;

/**
 * Regression tests for ANTLR SQL grammar fixes.
 * Tests parser-level acceptance of various SQL syntax patterns that were previously failing.
 * These tests focus on PARSING only - not execution.
 */
class SQLGrammarRegressionTest {

  private final SQLAntlrParser parser = new SQLAntlrParser(null);

  // ============================================================================
  // Reserved Keywords as Identifiers
  // ============================================================================

  @Test
  void reservedKeywordsAsIdentifiers() {
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
    assertParses("SELECT metadata FROM MyType");
    assertParses("SELECT limit FROM MyType");
    // Test VERTEX and EDGE as type names
    assertParses("SELECT * FROM Vertex");
    assertParses("SELECT * FROM Edge");
  }

  @Test
  void limitAsParameterName() {
    // Test that 'limit' can be used as a named parameter (issue from RandomTestMultiThreadsTest)
    assertParses("SELECT FROM Transaction LIMIT :limit");
    assertParses("SELECT FROM MyType WHERE id = :limit");
  }

  @Test
  void reservedKeywordsInInsert() {
    assertParses("INSERT INTO MyType SET value = 'test'");
    assertParses("INSERT INTO MyType SET status = 'active'");
    assertParses("INSERT INTO MyType SET start = 10");
    assertParses("INSERT INTO MyType SET content = 'data'");
  }

  @Test
  void reservedKeywordsInUpdate() {
    assertParses("UPDATE MyType SET value = 'new'");
    assertParses("UPDATE MyType SET status = 'pending'");
  }

  // ============================================================================
  // UPDATE REMOVE Clause
  // ============================================================================

  @Test
  void updateRemoveSimple() {
    assertParses("UPDATE MyType REMOVE field1");
    assertParses("UPDATE MyType REMOVE field1, field2, field3");
  }

  @Test
  void updateRemoveWithEquals() {
    // REMOVE field = value (removes specific value from collection)
    assertParses("UPDATE MyType REMOVE items = 'item1'");
    assertParses("UPDATE MyType REMOVE tags = 'obsolete'");
  }

  @Test
  void updateRemoveMixed() {
    assertParses("UPDATE MyType SET x = 1 REMOVE field1");
    assertParses("UPDATE MyType REMOVE field1 SET x = 1");
    assertParses("UPDATE MyType SET x = 1 REMOVE field1, field2 WHERE id = 5");
  }

  // ============================================================================
  // RETURN Statement Without Value
  // ============================================================================

  @Test
  void returnEmpty() {
    final List<Statement> statements = parser.parseScript("RETURN;");
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(ReturnStatement.class);
    final ReturnStatement ret = (ReturnStatement) statements.get(0);
    assertThat(ret.expression).as("RETURN without value should have null expression").isNull();
  }

  @Test
  void returnWithValue() {
    final List<Statement> statements = parser.parseScript("RETURN $result;");
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(ReturnStatement.class);
    final ReturnStatement ret = (ReturnStatement) statements.get(0);
    assertThat(ret.expression).as("RETURN with value should have expression").isNotNull();
  }

  @Test
  void returnInScript() {
    final String script = """
        LET $x = 10;
        IF ($x > 5) {
          RETURN;
        }
        RETURN $x;
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(3);
  }

  // ============================================================================
  // TRUNCATE POLYMORPHIC
  // ============================================================================

  @Test
  void truncateTypePolymorphic() {
    final Statement stmt = assertParses("TRUNCATE TYPE MyType POLYMORPHIC");
    assertThat(stmt).isInstanceOf(TruncateTypeStatement.class);
    final TruncateTypeStatement truncate = (TruncateTypeStatement) stmt;
    assertThat(truncate.polymorphic).as("POLYMORPHIC flag should be set").isTrue();
  }

  @Test
  void truncateTypeUnsafe() {
    final Statement stmt = assertParses("TRUNCATE TYPE MyType UNSAFE");
    assertThat(stmt).isInstanceOf(TruncateTypeStatement.class);
    final TruncateTypeStatement truncate = (TruncateTypeStatement) stmt;
    assertThat(truncate.unsafe).as("UNSAFE flag should be set").isTrue();
  }

  @Test
  void truncateTypePolymorphicUnsafe() {
    final Statement stmt = assertParses("TRUNCATE TYPE MyType POLYMORPHIC UNSAFE");
    assertThat(stmt).isInstanceOf(TruncateTypeStatement.class);
    final TruncateTypeStatement truncate = (TruncateTypeStatement) stmt;
    assertThat(truncate.polymorphic && truncate.unsafe).as("Both flags should be set").isTrue();
  }

  // ============================================================================
  // Method Calls on Expressions
  // ============================================================================

  @Test
  void methodCallsOnLiterals() {
    assertParses("SELECT [{'x':1,'y':2}].keys() AS keys");
    assertParses("SELECT [{'x':1,'y':2}].values() AS values");
    assertParses("SELECT [1,2,3].size() AS count");
  }

  @Test
  void methodCallsOnFields() {
    assertParses("SELECT name.toLowerCase() FROM User");
    assertParses("SELECT name.toUpperCase() FROM User");
    assertParses("SELECT name.trim() FROM User");
  }

  @Test
  void conversionMethods() {
    assertParses("SELECT number.asDecimal() FROM MyType");
    assertParses("SELECT number.asInteger() FROM MyType");
    assertParses("SELECT number.asLong() FROM MyType");
    assertParses("SELECT number.asFloat() FROM MyType");
    assertParses("SELECT string.asString() FROM MyType");
    assertParses("SELECT number.asDate() FROM MyType");
    assertParses("SELECT number.asDateTime() FROM MyType");
  }

  @Test
  void methodCallsWithParameters() {
    assertParses("SELECT name.substring(0, 5) FROM User");
    assertParses("SELECT dateAsString.asDate('yyyy-MM-dd') FROM MyType");
  }

  @Test
  void chainedMethodCalls() {
    assertParses("SELECT name.trim().toLowerCase() FROM User");
    assertParses("SELECT data.keys().size() FROM MyType");
  }

  // ============================================================================
  // INSERT FROM SELECT
  // ============================================================================

  @Test
  void insertFromSelectSimple() {
    // Simple INSERT FROM SELECT works (basic SELECT without WHERE/LIMIT/etc)
    assertParses("INSERT INTO dst FROM SELECT * FROM src");
    assertParses("INSERT INTO dst FROM SELECT a, b FROM src");
  }

  @Test
  void insertFromSelectWithParentheses() {
    // Parenthesized SELECT works for simple queries
    assertParses("INSERT INTO dst FROM (SELECT * FROM src)");
    assertParses("INSERT INTO dst (SELECT * FROM src)");

    // Note: Complex SELECT statements (with WHERE, ORDER BY, etc.) currently have
    // grammar limitations. This is a known issue being addressed.
    // For now, complex queries should be executed as separate steps.
  }

  @Test
  void insertFromSelectWithReturn() {
    assertParses("INSERT INTO dst RETURN @rid FROM SELECT * FROM src");
  }

  // ============================================================================
  // BREAK Statement (Script-only)
  // ============================================================================

  @Test
  void breakInForeach() {
    final String script = """
        FOREACH ($i IN [1, 2, 3]) {
          IF ($i = 2) {
            BREAK;
          }
        }
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(ForEachBlock.class);
  }

  @Test
  void breakInWhile() {
    final String script = """
        WHILE ($x < 10) {
          IF ($x = 5) {
            BREAK;
          }
          LET $x = $x + 1;
        }
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(WhileBlock.class);
  }

  @Test
  void nestedBreak() {
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
    assertThat(statements.size()).isEqualTo(1);
  }

  // ============================================================================
  // BEGIN ISOLATION and COMMIT RETRY
  // ============================================================================

  @Test
  void beginIsolation() {
    final Statement stmt = assertParses("BEGIN ISOLATION REPEATABLE_READ");
    assertThat(stmt).isInstanceOf(BeginStatement.class);
    final BeginStatement begin = (BeginStatement) stmt;
    assertThat(begin.isolation).as("Isolation level should be set").isNotNull();
    assertThat(begin.isolation.getStringValue()).isEqualTo("REPEATABLE_READ");
  }

  @Test
  void beginWithoutIsolation() {
    final Statement stmt = assertParses("BEGIN");
    assertThat(stmt).isInstanceOf(BeginStatement.class);
    final BeginStatement begin = (BeginStatement) stmt;
    assertThat(begin.isolation).as("Isolation level should be null").isNull();
  }

  @Test
  void commitRetry() {
    final Statement stmt = assertParses("COMMIT RETRY 10");
    assertThat(stmt).isInstanceOf(CommitStatement.class);
    final CommitStatement commit = (CommitStatement) stmt;
    assertThat(commit.retry).as("Retry count should be set").isNotNull();
    assertThat(commit.retry.getValue().intValue()).isEqualTo(10);
  }

  @Test
  void commitWithoutRetry() {
    final Statement stmt = assertParses("COMMIT");
    assertThat(stmt).isInstanceOf(CommitStatement.class);
    final CommitStatement commit = (CommitStatement) stmt;
    assertThat(commit.retry).as("Retry count should be null").isNull();
  }

  @Test
  void transactionScript() {
    final String script = """
        BEGIN ISOLATION REPEATABLE_READ;
        INSERT INTO MyType SET x = 1;
        COMMIT RETRY 10;
        """;
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(3);
    assertThat(statements.get(0)).isInstanceOf(BeginStatement.class);
    assertThat(statements.get(1)).isInstanceOf(InsertStatement.class);
    assertThat(statements.get(2)).isInstanceOf(CommitStatement.class);
  }

  // ============================================================================
  // LET with Statements (not just expressions)
  // ============================================================================

  @Test
  void letWithInsert() {
    final String script = "LET $a = INSERT INTO MyType SET x = 1";
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(LetStatement.class);
  }

  @Test
  void letWithSelect() {
    final String script = "LET $result = SELECT * FROM MyType";
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(LetStatement.class);
  }

  @Test
  void letWithParenthesizedStatement() {
    final String script = "LET $result = (SELECT * FROM MyType)";
    final List<Statement> statements = parser.parseScript(script);
    assertThat(statements.size()).isEqualTo(1);
    assertThat(statements.get(0)).isInstanceOf(LetStatement.class);
  }

  // ============================================================================
  // LOCK Statement
  // ============================================================================

  @Test
  void lockType() {
    final Statement stmt = assertParses("LOCK TYPE Node");
    assertThat(stmt).isInstanceOf(LockStatement.class);
    final LockStatement lock = (LockStatement) stmt;
    assertThat(lock.mode).isEqualTo("TYPE");
    assertThat(lock.identifiers).isNotNull();
    assertThat(lock.identifiers.size()).isEqualTo(1);
  }

  @Test
  void lockMultipleTypes() {
    final Statement stmt = assertParses("LOCK TYPE Node, Node2, Node3");
    assertThat(stmt).isInstanceOf(LockStatement.class);
    final LockStatement lock = (LockStatement) stmt;
    assertThat(lock.mode).isEqualTo("TYPE");
    assertThat(lock.identifiers.size()).isEqualTo(3);
  }

  @Test
  void lockBucket() {
    final Statement stmt = assertParses("LOCK BUCKET myBucket");
    assertThat(stmt).isInstanceOf(LockStatement.class);
    final LockStatement lock = (LockStatement) stmt;
    assertThat(lock.mode).isEqualTo("BUCKET");
    assertThat(lock.identifiers.size()).isEqualTo(1);
  }

  @Test
  void lockMultipleBuckets() {
    final Statement stmt = assertParses("LOCK BUCKET bucket1, bucket2");
    assertThat(stmt).isInstanceOf(LockStatement.class);
    final LockStatement lock = (LockStatement) stmt;
    assertThat(lock.mode).isEqualTo("BUCKET");
    assertThat(lock.identifiers.size()).isEqualTo(2);
  }

  // ============================================================================
  // Record Attributes (@rid, @type, @in, @out)
  // ============================================================================

  @Test
  void selectWithRidAttribute() {
    assertParses("SELECT @rid FROM MyType");
    assertParses("SELECT @rid, name FROM MyType");
    assertParses("SELECT * FROM MyType WHERE @rid = #1:0");
  }

  @Test
  void selectWithTypeAttribute() {
    assertParses("SELECT @type FROM MyType");
    assertParses("SELECT @type, @rid FROM MyType");
  }

  @Test
  void selectWithInOutAttributes() {
    assertParses("SELECT @in FROM Edge");
    assertParses("SELECT @out FROM Edge");
    assertParses("SELECT @in, @out FROM Edge");
  }

  @Test
  void updateWithRidFilter() {
    assertParses("UPDATE MyType SET name = 'test' WHERE @rid = #1:0");
    assertParses("UPDATE MyType SET id = id + 1 WHERE @rid = #10:5");
  }

  @Test
  void deleteWithRidFilter() {
    assertParses("DELETE FROM MyType WHERE @rid = #1:0");
  }

  @Test
  void whereRidComparison() {
    assertParses("SELECT * FROM MyType WHERE @rid > #1:0");
    assertParses("SELECT * FROM MyType WHERE @rid < #1:100");
    assertParses("SELECT * FROM MyType WHERE @rid IN [#1:0, #1:1, #1:2]");
  }

  // ============================================================================
  // Unicode Escape Sequences (Issue #3311)
  // ============================================================================

  @Test
  void unicodeEscapeInString() {
    // Issue #3311: Unicode escape sequences should work in SQL strings
    assertParses("SELECT '\\u0026'");
    assertParses("SELECT '\\u0026' FROM V");
    assertParses("SELECT \"\\u0026\"");
    assertParses("SELECT \"\\u0026\" FROM V");
  }

  @Test
  void unicodeEscapeMultiple() {
    // Multiple unicode escapes in a string
    assertParses("SELECT '\\u0048\\u0065\\u006C\\u006C\\u006F'");  // "Hello"
    assertParses("SELECT '\\u005C\\u005C'");  // "\\\\"
  }

  @Test
  void unicodeEscapeInWhere() {
    assertParses("SELECT FROM bucket:internal WHERE \"\\u005C\\u005C\" = \"\\u005C\\u005C\"");
  }

  @Test
  void unicodeEscapeMixedWithText() {
    // Unicode escape mixed with regular text
    assertParses("SELECT 'Hello\\u0020World'");
    assertParses("SELECT 'test\\u003Dvalue'");
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
      assertThat(stmt).as("Parser should return a statement for: " + sql).isNotNull();
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
    assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(Exception.class);
  }
}
