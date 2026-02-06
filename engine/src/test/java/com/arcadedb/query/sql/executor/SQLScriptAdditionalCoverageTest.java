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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional coverage tests for SQLScriptQueryEngine and ANTLR parser paths.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLScriptAdditionalCoverageTest extends TestHelper {

  // --- Multi-statement scripts with LET variables ---
  @Test
  void scriptWithLetVariables() {
    database.getSchema().createDocumentType("ScriptLet");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $name = 'testName';
          INSERT INTO ScriptLet SET name = $name, val = 1;
          LET $result = SELECT count(*) as cnt FROM ScriptLet;
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM ScriptLet");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(1L);
    rs.close();
  }

  // --- Scripts with RETRY block ---
  @Test
  void scriptWithRetry() {
    database.getSchema().createDocumentType("ScriptRetry");
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      database.command("sqlscript", """
          LET $retries = 0;
          BEGIN;
          INSERT INTO ScriptRetry SET attempt = $retries;
          LET $retries = $retries + 1;
          IF($retries < 3) {
              SELECT throwCME(#-1:-1, 1, 1, 1);
          }
          COMMIT RETRY 10;
          """);
    });

    final ResultSet rs = database.query("sql", "SELECT FROM ScriptRetry");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat((int) item.getProperty("attempt")).isEqualTo(2);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  // --- Scripts with nested BEGIN/COMMIT ---
  @Test
  void scriptWithNestedBeginCommit() {
    database.getSchema().createDocumentType("ScriptNested");
    database.command("sqlscript", """
        BEGIN;
        INSERT INTO ScriptNested SET name = 'first', marker = 'nested_test';
        COMMIT;
        BEGIN;
        INSERT INTO ScriptNested SET name = 'second', marker = 'nested_test';
        COMMIT;
        """);
    final ResultSet rs = database.query("sql", "SELECT FROM ScriptNested WHERE marker = 'nested_test'");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThanOrEqualTo(2);
    rs.close();
  }

  // --- Scripts with IF/ELSE blocks ---
  @Test
  void scriptIfElse() {
    database.getSchema().createDocumentType("ScriptIfElse");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $val = 10;
          IF ($val > 5) {
            INSERT INTO ScriptIfElse SET result = 'greater';
          } ELSE {
            INSERT INTO ScriptIfElse SET result = 'lesser';
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT FROM ScriptIfElse");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("greater");
    rs.close();
  }

  @Test
  void scriptIfElseFalseCondition() {
    database.getSchema().createDocumentType("ScriptIfElse2");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $val = 3;
          IF ($val > 5) {
            INSERT INTO ScriptIfElse2 SET result = 'greater';
          } ELSE {
            INSERT INTO ScriptIfElse2 SET result = 'lesser';
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT FROM ScriptIfElse2");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("lesser");
    rs.close();
  }

  // --- Scripts with WHILE loops ---
  @Test
  void scriptWhileLoop() {
    database.getSchema().createDocumentType("ScriptWhile");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $i = 0;
          WHILE ($i < 4) {
            INSERT INTO ScriptWhile SET idx = $i;
            LET $i = $i + 1;
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM ScriptWhile");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(4L);
    rs.close();
  }

  // --- Scripts with FOREACH ---
  @Test
  void scriptForeach() {
    database.getSchema().createDocumentType("ScriptForEach");
    database.transaction(() -> {
      database.command("sqlscript", """
          FOREACH ($item IN [10, 20, 30]) {
            INSERT INTO ScriptForEach SET val = $item;
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM ScriptForEach");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }

  // --- Scripts with RETURN ---
  @Test
  void scriptReturn() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sqlscript", """
          LET $x = 42;
          RETURN $x;
          """);
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<Integer>getProperty("value")).isEqualTo(42);
      rs.close();
    });
  }

  @Test
  void scriptReturnString() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sqlscript", """
          RETURN 'hello';
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("hello");
      rs.close();
    });
  }

  // --- Error cases: syntax errors ---
  @Test
  void scriptSyntaxError() {
    assertThatThrownBy(() -> database.command("sql", "SELECTTT FROM nothing"))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  // --- Subquery expressions ---
  @Test
  void subqueryExpression() {
    database.getSchema().createDocumentType("SubqOuter");
    database.getSchema().createDocumentType("SubqInner");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO SubqInner SET val = 1");
      database.command("sql", "INSERT INTO SubqInner SET val = 2");
      database.command("sql", "INSERT INTO SubqInner SET val = 3");
      database.command("sql", "INSERT INTO SubqOuter SET name = 'a', val = 1");
      database.command("sql", "INSERT INTO SubqOuter SET name = 'b', val = 4");
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM SubqOuter WHERE val IN (SELECT val FROM SubqInner)");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("a");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  // --- Complex WHERE conditions ---
  @Test
  void complexWhereWithAndOr() {
    database.getSchema().createDocumentType("ComplexWhere");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO ComplexWhere SET a = 1, b = 2, c = 3");
      database.command("sql", "INSERT INTO ComplexWhere SET a = 4, b = 5, c = 6");
      database.command("sql", "INSERT INTO ComplexWhere SET a = 7, b = 8, c = 9");
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM ComplexWhere WHERE (a = 1 AND b = 2) OR (a = 7 AND c = 9)");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
    rs.close();
  }

  @Test
  void complexWhereNot() {
    database.getSchema().createDocumentType("NotWhere");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO NotWhere SET val = 1");
      database.command("sql", "INSERT INTO NotWhere SET val = 2");
      database.command("sql", "INSERT INTO NotWhere SET val = 3");
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM NotWhere WHERE NOT (val = 2) ORDER BY val");
    int count = 0;
    while (rs.hasNext()) {
      final int val = rs.next().getProperty("val");
      assertThat(val).isNotEqualTo(2);
      count++;
    }
    assertThat(count).isEqualTo(2);
    rs.close();
  }

  // --- SLEEP statement ---
  @Test
  void sleepStatement() {
    database.transaction(() -> {
      final long start = System.currentTimeMillis();
      database.command("sqlscript", "SLEEP 100");
      final long elapsed = System.currentTimeMillis() - start;
      assertThat(elapsed).isGreaterThanOrEqualTo(80); // Allow small margin
    });
  }

  // --- CONSOLE LOG statement ---
  @Test
  void consoleLogStatement() {
    database.transaction(() -> {
      // Just verify it doesn't throw
      database.command("sqlscript", "CONSOLE.OUTPUT 'test log message';");
    });
  }

  // --- Multi-statement script returning last result ---
  @Test
  void multiStatementScript() {
    database.getSchema().createDocumentType("MultiStmt");
    database.transaction(() -> {
      database.command("sqlscript", """
          INSERT INTO MultiStmt SET name = 'a';
          INSERT INTO MultiStmt SET name = 'b';
          INSERT INTO MultiStmt SET name = 'c';
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM MultiStmt");
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }

  // --- Script with LET referencing previous LET ---
  @Test
  void scriptLetChaining() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sqlscript", """
          LET $a = 10;
          LET $b = $a + 5;
          LET $c = $b * 2;
          RETURN $c;
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(30);
      rs.close();
    });
  }

  // --- Script with LET query ---
  @Test
  void scriptLetWithQuery() {
    database.getSchema().createDocumentType("LetQuery");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO LetQuery SET val = 100");
    });
    database.transaction(() -> {
      final ResultSet rs = database.command("sqlscript", """
          LET $items = SELECT val FROM LetQuery;
          RETURN $items;
          """);
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });
  }

  // --- Script with IF and LET ---
  @Test
  void scriptIfWithLet() {
    database.getSchema().createDocumentType("IfLet");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $count = SELECT count(*) as cnt FROM IfLet;
          IF ($count.size() > 0 AND $count[0].cnt = 0) {
            INSERT INTO IfLet SET state = 'initialized';
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT FROM IfLet");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("state")).isEqualTo("initialized");
    rs.close();
  }

  // --- ANTLR parser: complex nested expressions ---
  @Test
  void antlrNestedExpressions() {
    final ResultSet rs = database.query("sql",
        "SELECT (1 + 2) * (3 + 4) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(21);
    rs.close();
  }

  @Test
  void antlrConcatInWhere() {
    database.getSchema().createDocumentType("AntlrTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO AntlrTest SET first = 'John', last = 'Doe'");
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM AntlrTest WHERE first + ' ' + last = 'John Doe'");
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  // --- ANTLR parser: various statement types ---
  @Test
  void antlrCreateAndQuery() {
    database.command("sql", "CREATE DOCUMENT TYPE AntlrDoc");
    database.command("sql", "CREATE PROPERTY AntlrDoc.name STRING");
    database.command("sql", "CREATE PROPERTY AntlrDoc.age INTEGER");
    database.command("sql", "CREATE INDEX ON AntlrDoc (name) NOTUNIQUE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO AntlrDoc SET name = 'Alice', age = 30");
      database.command("sql", "INSERT INTO AntlrDoc SET name = 'Bob', age = 25");
    });

    final ResultSet rs = database.query("sql",
        "SELECT name, age FROM AntlrDoc WHERE name = 'Alice' ORDER BY age DESC");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    rs.close();
  }

  // --- ANTLR parser: negative numbers ---
  @Test
  void antlrNegativeNumbers() {
    final ResultSet rs = database.query("sql", "SELECT -5 as neg, -3.14 as negFloat");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Integer>getProperty("neg")).isEqualTo(-5);
    rs.close();
  }

  // --- ANTLR parser: boolean literals ---
  @Test
  void antlrBooleanLiterals() {
    final ResultSet rs = database.query("sql",
        "SELECT true as t, false as f");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Boolean>getProperty("t")).isTrue();
    assertThat(item.<Boolean>getProperty("f")).isFalse();
    rs.close();
  }

  // --- ANTLR parser: string escaping ---
  @Test
  void antlrStringEscaping() {
    final ResultSet rs = database.query("sql", "SELECT 'it\\'s a test' as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("it's a test");
    rs.close();
  }

  // --- ANTLR parser: array access ---
  @Test
  void antlrArrayAccess() {
    final ResultSet rs = database.query("sql", "SELECT [10, 20, 30][1] as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(20);
    rs.close();
  }

  // --- ANTLR parser: function calls in WHERE ---
  @Test
  void antlrFunctionInWhere() {
    database.getSchema().createDocumentType("AntlrFuncWhere");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO AntlrFuncWhere SET name = 'HELLO'");
      database.command("sql", "INSERT INTO AntlrFuncWhere SET name = 'world'");
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM AntlrFuncWhere WHERE name.toLowerCase() = 'hello'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("HELLO");
    rs.close();
  }

  // --- Batch script with multiple transaction blocks ---
  @Test
  void batchScriptMultipleTransactions() {
    database.getSchema().createDocumentType("BatchMultiTx");
    database.command("sqlscript", """
        BEGIN;
        INSERT INTO BatchMultiTx SET batchNum = 1, val = 'a', marker = 'batch_test';
        INSERT INTO BatchMultiTx SET batchNum = 1, val = 'b', marker = 'batch_test';
        COMMIT;
        BEGIN;
        INSERT INTO BatchMultiTx SET batchNum = 2, val = 'c', marker = 'batch_test';
        COMMIT;
        """);
    final ResultSet rs = database.query("sql", "SELECT FROM BatchMultiTx WHERE marker = 'batch_test'");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThanOrEqualTo(3);
    rs.close();
  }

  // --- Script with combined features ---
  @Test
  void complexScript() {
    database.getSchema().createDocumentType("ComplexScript");
    database.transaction(() -> {
      database.command("sqlscript", """
          LET $count = 0;
          FOREACH ($i IN [1, 2, 3, 4, 5]) {
            INSERT INTO ComplexScript SET idx = $i;
            LET $count = $count + 1;
          }
          LET $total = SELECT count(*) as cnt FROM ComplexScript;
          IF ($total[0].cnt = 5) {
            INSERT INTO ComplexScript SET idx = 99, marker = 'complete';
          }
          """);
    });
    final ResultSet rs = database.query("sql", "SELECT FROM ComplexScript WHERE marker = 'complete'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("idx")).isEqualTo(99);
    rs.close();

    final ResultSet cnt = database.query("sql", "SELECT count(*) as cnt FROM ComplexScript");
    assertThat(cnt.next().<Long>getProperty("cnt")).isEqualTo(6L);
    cnt.close();
  }

  // --- Helper to define throwCME function ---
  private SQLFunction defineThrowCME() {
    return new SQLFunctionAbstract("throwCME") {
      @Override
      public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
          final Object[] iParams, final CommandContext iContext) {
        throw new ConcurrentModificationException("test");
      }

      @Override
      public String getSyntax() {
        return "throwCME()";
      }

      @Override
      public Object getResult() {
        return null;
      }
    };
  }
}
