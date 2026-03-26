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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLScriptTest extends TestHelper {
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE DOCUMENT TYPE foo");
      database.command("sql", "insert into foo (name, bar) values ('a', 1)");
      database.command("sql", "insert into foo (name, bar) values ('b', 2)");
      database.command("sql", "insert into foo (name, bar) values ('c', 3)");
    });
  }

  @Test
  void queryOnDeprecated() {
    String script = """
        begin;
        let $a = select from foo;
        commit;
        return $a;
        """;
    ResultSet qResult = database.command("sqlscript", script);

    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  void query() {
    String script = """
        begin;
        let $a = select from foo;
        commit;
        return $a;
        """;
    ResultSet qResult = database.command("SQLScript", script);
    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  void tx() {
    String script = """
        begin isolation REPEATABLE_READ;
        let $a = insert into V set test = 'sql script test';
        commit retry 10;
        return $a;
        """;
    Document qResult = database.command("SQLScript", script).next().toElement();

    assertThat(qResult).isNotNull();
  }

  @Test
  void returnExpanded() {
    database.transaction(() -> {
      String script = """
          let $a = insert into V set test = 'sql script test';
          return $a.asJSON();
          """;
      JSONObject qResult = database.command("SQLScript", script).next().toJSON();
      assertThat(qResult).isNotNull();

      script = """
          let $a = select from V limit 2;
          return $a.asJSON();
          """;
      JSONObject result = database.command("SQLScript", script).next().toJSON();
      assertThat(result).isNotNull();
    });

  }

  @Test
  void sleep() {
    long begin = System.currentTimeMillis();

    database.command("SQLScript", "sleep 500");

    assertThat(System.currentTimeMillis() - begin >= 500).isTrue();
  }

  //@Test
  public void testConsoleLog() {
    String script = """
        LET $a = 'log'
        console.log 'This is a test of log for ${a}'
        """;
    database.command("SQLScript", script);
  }

  //@Test
  public void testConsoleOutput() {
    String script = """
        LET $a = 'output'
        console.output 'This is a test of log for ${a}'
        """;
    database.command("SQLScript", script);
  }

  //@Test
  public void testConsoleError() {
    String script = """
        LET $a = 'error';
        CONSOLE.ERROR 'This is a test of log for ${a}';
        """;
    database.command("SQLScript", script);
  }

  @Test
  void returnObject() {
    ResultSet result = database.command("SQLScript", "return [{ a: 'b' }]");

    assertThat(Optional.ofNullable(result)).isNotNull();

    assertThat(result.next().<String>getProperty("a")).isEqualTo("b");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void incrementAndLet() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestCounter");

      String script = """
          INSERT INTO TestCounter set weight = 3;
          LET counter = SELECT count(*) as count FROM TestCounter;
          UPDATE TestCounter SET weight += $counter[0].count RETURN AFTER @this;
          """;
      ResultSet qResult = database.command("SQLScript", script.toString());

      assertThat(qResult.hasNext()).isTrue();
      Result result = qResult.next();
      assertThat(result.<Long>getProperty("weight")).isEqualTo(4L);
    });
  }

  @Test
  void if1() {
    String script = """
        let $a = select 1 as one;
        if($a[0].one = 1){
         return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  void if2() {
    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 1) {
          return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  void if3() {

    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 1) {
          return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);
    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  void nestedIf2() {
    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 1) {
            if ($a[0].one = 'zz') {
              return 'FAIL';
            }
          return 'OK';
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  void nestedIf3() {
    String script = """
        let $a = select 1 as one;
        if ($a[0].one = 'zz') {
            if ($a[0].one = 1) {
              return 'FAIL';
            }
          return 'FAIL';
        }
        return 'OK';
        """;
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  void ifRealQuery() {
    String script = """
        let $a = select from foo;
        if ($a is not null and $a.size() = 3 ){
          return $a;
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  void ifMultipleStatements() {
    String script = """
        let $a = select 1 as one;
        -- this is a comment
        if ($a[0].one = 1) {
          let $b = select 'OK' as ok;
          return $b[0].ok;
        }
        return 'FAIL';
        """;
    ResultSet qResult = database.command("SQLScript", script);

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  void semicolonInString() {
    // testing parsing problem
    ResultSet qResult = database.command("SQLScript", "let $a = select 'foo ; bar' as one\n");
  }

  @Test
  void quotedRegex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE QuotedRegex2");
      String batch = "INSERT INTO QuotedRegex2 SET regexp=\"'';\"";

      database.command("SQLScript", batch.toString());

      ResultSet result = database.query("sql", "SELECT FROM QuotedRegex2");
      Document doc = result.next().toElement();

      assertThat(result.hasNext()).isFalse();
      assertThat(doc.get("regexp")).isEqualTo("'';");

    });
  }

  @Test
  void parameters1() {
    String className = "testParameters1";
    database.getSchema().createVertexType(className);
    database.getSchema().createEdgeType("E");
    String script = """
        BEGIN;
        LET $a = CREATE VERTEX %s SET name = :name;
        LET $b = CREATE VERTEX %s SET name = :name;
        LET $edge = CREATE EDGE E from $a to $b;
        COMMIT;
        RETURN $edge;
        """.formatted(className, className);

    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "bozo");
    map.put("_name2", "bozi");

    ResultSet rs = database.command("sqlscript", script, map);
    rs.close();

    rs = database.query("sql", "SELECT FROM " + className + " WHERE name = ?", "bozo");

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();
  }

  @Test
  void positionalParameters() {
    String className = "testPositionalParameters";
    database.getSchema().createVertexType(className);
    database.getSchema().createEdgeType("E");
    String script = """
        BEGIN;
        LET $a = CREATE VERTEX %s SET name = ?;
        LET $b = CREATE VERTEX %s SET name = ?;
        LET $edge = CREATE EDGE E from $a to $b;
        COMMIT;
        RETURN $edge;
        """.formatted(className, className);

    ResultSet rs = database.command("SQLScript", script, "bozo", "bozi");
    rs.close();

    rs = database.query("sql", "SELECT FROM " + className + " WHERE name = ?", "bozo");

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();
  }

  @Test
  void insertJsonNewLines() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("doc");

      final ResultSet result = database.command("sqlscript", """
          INSERT INTO doc CONTENT {
          "head" : {
            "vars" : [ "item", "itemLabel" ]
          },
          "results" : {
            "bindings" : [ {
              "item" : {
                    "type" : "uri",
                        "value" : "http://www.wikidata.org/entity/Q113997665"
                  },
                  "itemLabel" : {
                    "xml:lang" : "en",
                        "type" : "literal",
                        "value" : "ArcadeDB"
                  }
                }, {
                  "item" : {
                    "type" : "uri",
                        "value" : "http://www.wikidata.org/entity/Q808716"
                  },
                  "itemLabel" : {
                    "xml:lang" : "en",
                        "type" : "literal",
                        "value" : "OrientDB"
                  }
                } ]
              }
          }""");

      assertThat(result.hasNext()).isTrue();
      final Result res = result.next();
      assertThat(res.hasProperty("head")).isTrue();
      assertThat(res.hasProperty("results")).isTrue();
    });
  }

  /**
   * Test for issue #2496: ALTER TYPE should return a result in SQLScript
   * https://github.com/ArcadeData/arcadedb/issues/2496
   * <p>
   * SQLScript always returns the result of the last executed command (unless an explicit RETURN statement is present).
   * DDL statements like ALTER TYPE were returning empty results because DDLExecutionPlan.fetchNext() returned an empty ResultSet.
   */
  @Test
  void alterTypeReturnsResultInSqlScript() {
    database.transaction(() -> {
      database.getSchema().createVertexType("TestAlterType");

      // Test that ALTER TYPE returns a result in SQLScript without explicit RETURN
      String script = """
          ALTER TYPE TestAlterType ALIASES x, y;
          """;
      ResultSet result = database.command("SQLScript", script);

      assertThat(result.hasNext()).as("ALTER TYPE should return a result in SQLScript").isTrue();
      Result res = result.next();
      assertThat(res.<String>getProperty("operation")).isEqualTo("ALTER TYPE");
      assertThat(res.<String>getProperty("typeName")).isEqualTo("TestAlterType");
      assertThat(res.<String>getProperty("result")).isEqualTo("OK");
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Test for issue #2496: SQLScript returns last command result
   * <p>
   * Verify that SQLScript returns the result of the last executed command when no explicit RETURN is present.
   */
  @Test
  void sqlScriptReturnsLastCommandResult() {
    database.transaction(() -> {
      database.getSchema().createVertexType("TestLastCmd");

      // Multiple statements - should return the result of the last one (ALTER TYPE)
      String script = """
          INSERT INTO TestLastCmd SET name = 'test';
          ALTER TYPE TestLastCmd ALIASES z;
          """;
      ResultSet result = database.command("SQLScript", script);

      assertThat(result.hasNext()).as("SQLScript should return last command result").isTrue();
      Result res = result.next();
      // Should be the ALTER TYPE result, not the INSERT result
      assertThat(res.<String>getProperty("operation")).isEqualTo("ALTER TYPE");
      assertThat(res.<String>getProperty("typeName")).isEqualTo("TestLastCmd");
    });
  }

  /**
   * Test for issue #2496: CREATE TYPE also returns a result in SQLScript
   */
  @Test
  void createTypeReturnsResultInSqlScript() {
    database.transaction(() -> {
      // Test that CREATE TYPE returns a result in SQLScript without explicit RETURN
      String script = """
          CREATE VERTEX TYPE TestCreateType;
          """;
      ResultSet result = database.command("SQLScript", script);

      assertThat(result.hasNext()).as("CREATE TYPE should return a result in SQLScript").isTrue();
      Result res = result.next();
      assertThat(res.<String>getProperty("operation")).isEqualTo("create vertex type");
      assertThat(res.<String>getProperty("typeName")).isEqualTo("TestCreateType");
    });
  }

  @Test
  void uninitializedVariables() {
    // Test case for issue #1939: https://github.com/ArcadeData/arcadedb/issues/1939
    // Uninitialized variables should evaluate to null, not to their symbol name as a string
    //
    // Before the fix:
    // - CONSOLE.log $abc would output "$abc" (the string)
    // - LET $test = $abc would set $test to "$abc" (the string)
    // - CONSOLE.log $test would output "$test" (the string)
    //
    // After the fix:
    // - CONSOLE.log $abc should output "null"
    // - LET $test = $abc should set $test to null
    // - CONSOLE.log $test should output "null"

    // Test 1: Direct console.log of uninitialized variable should output "null", not "$abc"
    ResultSet result = database.command("SQLScript", "CONSOLE.log $abc");
    assertThat(result.hasNext()).isTrue();
    Result consoleResult = result.next();
    Object message = consoleResult.getProperty("message");
    assertThat(message).isEqualTo("null");

    // Test 2: Assignment of uninitialized variable to another variable should assign null
    String script = """
        LET $test = $abc;
        RETURN $test;
        """;
    result = database.command("SQLScript", script);
    assertThat(result.hasNext()).isTrue();
    Result returnResult = result.next();
    Object value = returnResult.getProperty("value");
    assertThat(value).isNull();

    // Test 3: LET with uninitialized variable should set to null, then log "null"
    script = """
        LET $test = $abc;
        CONSOLE.log $test;
        """;
    result = database.command("SQLScript", script);
    assertThat(result.hasNext()).isTrue();
    consoleResult = result.next();
    message = consoleResult.getProperty("message");
    assertThat(message).isEqualTo("null");
  }

  /**
   * Test for issue #3558: max()+1 arithmetic not working when field has an index.
   * The index-optimized max/min path was returning the raw max/min value without applying
   * arithmetic operations in the projection (e.g., +1, +2, *2).
   */
  @Test
  void maxPlusArithmeticWithIndex() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestMaxArith");
      database.command("sql", "CREATE PROPERTY TestMaxArith.id INTEGER");
      database.command("sql", "CREATE INDEX ON TestMaxArith (id) UNIQUE");
      database.command("sql", "INSERT INTO TestMaxArith SET id = 100");
      database.command("sql", "INSERT INTO TestMaxArith SET id = 200");
      database.command("sql", "INSERT INTO TestMaxArith SET id = 300");

      // Verify plain max() still uses index optimization correctly
      ResultSet rs = database.query("sql", "SELECT max(id) AS maxid FROM TestMaxArith");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("maxid")).intValue()).isEqualTo(300);
      rs.close();

      // Verify min() still uses index optimization correctly
      rs = database.query("sql", "SELECT min(id) AS minid FROM TestMaxArith");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("minid")).intValue()).isEqualTo(100);
      rs.close();

      // This is the bug: max(id)+1 returned 300 instead of 301 when index existed
      rs = database.query("sql", "SELECT max(id)+1 AS id FROM TestMaxArith");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("id")).intValue()).isEqualTo(301);
      rs.close();

      // Also test max(id)+2
      rs = database.query("sql", "SELECT max(id)+2 AS id FROM TestMaxArith");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("id")).intValue()).isEqualTo(302);
      rs.close();

      // Test min(id)-1
      rs = database.query("sql", "SELECT min(id)-1 AS id FROM TestMaxArith");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("id")).intValue()).isEqualTo(99);
      rs.close();

      // Test in SQLSCRIPT context (the original bug report)
      String script = """
          LET $id = SELECT max(id)+1 AS id FROM TestMaxArith;
          RETURN $id;
          """;
      ResultSet result = database.command("SQLScript", script);
      assertThat(result.hasNext()).isTrue();
      assertThat(((Number) result.next().getProperty("id")).intValue()).isEqualTo(301);
      result.close();
    });
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3664.
   * A SQLScript composed only of LET (with SELECT sub-queries) and RETURN statements
   * must be treated as idempotent so it can be submitted via the /query endpoint.
   */
  @Test
  void queryScriptWithLetAndReturnIsIdempotent() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Chunk IF NOT EXISTS");
      database.command("sql", "INSERT INTO Chunk SET text = 'hello world'");
    });

    // Script is read-only: only LET+SELECT and RETURN — must NOT throw QueryNotIdempotentException
    final String script = """
        LET $chunks = (SELECT @rid, text FROM Chunk);
        LET $count = 15;
        RETURN $chunks;
        """;
    final ResultSet rs = database.query("sqlscript", script);
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  @Test
  void letParenthesisShouldNotAffectContent() {
    // https://github.com/ArcadeData/arcadedb/issues/3735
    // LET $x = SELECT ... and LET $y = (SELECT ...) must produce identical variable types
    final String script = """
        LET $x = SELECT "hi" AS hi;
        LET $y = (SELECT "hi" AS hi);
        SELECT $x, $x.type(), $x.hi, $y, $y.type(), $y.hi
        """;
    final ResultSet rs = database.query("sqlscript", script);
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();

    // Both must be LIST type
    assertThat((String) row.getProperty("$x.type()")).isEqualTo("LIST");
    assertThat((String) row.getProperty("$y.type()")).isEqualTo("LIST");

    // Both must allow field access through the list
    assertThat(row.getProperty("$x.hi").toString()).isEqualTo(row.getProperty("$y.hi").toString());

    rs.close();
  }

  @Test
  void queryScriptWithWriteStatementIsNotIdempotent() {
    // A SQLScript that inserts at the top level must still be rejected by the query endpoint
    final String script = """
        INSERT INTO foo SET name = 'x';
        """;
    assertThatThrownBy(() -> database.query("sqlscript", script))
        .isInstanceOf(QueryNotIdempotentException.class);
  }
}
