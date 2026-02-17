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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for custom function support in OpenCypher queries.
 * Tests SQL, JavaScript, and Cypher-defined functions callable from Cypher expressions.
 */
class OpenCypherCustomFunctionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/testcustomfunc").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // Phase 1: SQL Function Tests

  @Test
  void sqlFunctionInExpression() {
    database.command("sql", "DEFINE FUNCTION math.sum \"SELECT :a + :b\" PARAMETERS [a,b] LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN math.sum(3, 5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(8L);
  }

  @Test
  void sqlFunctionInWhereClause() {
    database.command("sql", "DEFINE FUNCTION math.threshold \"SELECT 5\" LANGUAGE sql");
    database.command("opencypher", "CREATE (:Number {value: 10})");
    database.command("opencypher", "CREATE (:Number {value: 3})");

    final ResultSet rs = database.query("opencypher", "MATCH (n:Number) WHERE n.value > math.threshold() RETURN n.value ORDER BY n.value");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("n.value").longValue()).isEqualTo(10L);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void sqlFunctionWithMultipleArgs() {
    database.command("sql", "DEFINE FUNCTION math.add3 \"SELECT :a + :b + :c\" PARAMETERS [a,b,c] LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN math.add3(1, 2, 3) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(6L);
  }

  @Test
  void sqlFunctionReturningNull() {
    database.command("sql", "DEFINE FUNCTION test.returnNull \"SELECT null\" LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN test.returnNull() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void undefinedFunctionThrowsError() {
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("opencypher", "RETURN nonexistent.func() as result");
      rs.hasNext(); // Force query evaluation (queries are lazy)
    })
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Unknown function");
  }

  @Test
  void sqlFunctionInListComprehension() {
    database.command("sql", "DEFINE FUNCTION math.double \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN [x IN range(1,3) | math.double(x)] as result");
    assertThat(rs.hasNext()).isTrue();
    final Object result = rs.next().getProperty("result");
    assertThat(result).asList().containsExactly(2L, 4L, 6L);
  }

  @Test
  void callClauseStillWorks() {
    database.command("sql", "DEFINE FUNCTION math.multiply \"SELECT :a * :b\" PARAMETERS [a,b] LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "CALL math.multiply(4, 2) YIELD result RETURN result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(8L);
  }

  @Test
  void functionRedefinition() {
    // Define initial function
    database.command("sql", "DEFINE FUNCTION test.value \"SELECT 10\" LANGUAGE sql");

    ResultSet rs = database.query("opencypher", "RETURN test.value() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(10L);

    // Redefine function
    database.getSchema().getFunctionLibrary("test").unregisterFunction("value");
    database.command("sql", "DEFINE FUNCTION test.value \"SELECT 20\" LANGUAGE sql");

    rs = database.query("opencypher", "RETURN test.value() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(20L);
  }

  // Phase 2: JavaScript Function Tests

  @Test
  void javaScriptFunctionInExpression() {
    database.command("sql", "DEFINE FUNCTION js.multiply \"return x * y\" PARAMETERS [x,y] LANGUAGE js");

    final ResultSet rs = database.query("opencypher", "RETURN js.multiply(4, 2) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(8);
  }

  @Test
  void javaScriptFunctionWithString() {
    database.command("sql", "DEFINE FUNCTION js.greet \"return 'Hello ' + name\" PARAMETERS [name] LANGUAGE js");

    final ResultSet rs = database.query("opencypher", "RETURN js.greet('World') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("Hello World");
  }

  // Phase 3: Cypher Function Tests

  @Test
  void defineCypherFunction() {
    database.command("sql", "DEFINE FUNCTION cypher.double \"RETURN $x * 2\" PARAMETERS [x] LANGUAGE cypher");

    final ResultSet rs = database.query("opencypher", "RETURN cypher.double(5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("result")).isEqualTo(10L);
  }

  @Test
  void defineCypherFunctionWithOpenCypherAlias() {
    database.command("sql", "DEFINE FUNCTION cypher.triple \"RETURN $x * 3\" PARAMETERS [x] LANGUAGE opencypher");

    final ResultSet rs = database.query("opencypher", "RETURN cypher.triple(5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("result")).isEqualTo(15L);
  }

  @Test
  void cypherFunctionWithGraphQuery() {
    database.command("sql",
        "DEFINE FUNCTION graph.countNeighbors " +
        "\"MATCH (n) WHERE id(n) = $nodeId MATCH (n)-[]->() RETURN count(*) as cnt\" " +
        "PARAMETERS [nodeId] LANGUAGE cypher");

    // Create test graph
    final ResultSet createResult = database.command("opencypher", "CREATE (a:Person)-[:KNOWS]->(b:Person) RETURN id(a) as aid");
    assertThat(createResult.hasNext()).isTrue();
    final String nodeId = createResult.next().getProperty("aid");

    // Call function that queries graph data
    final ResultSet rs = database.query("opencypher", "RETURN graph.countNeighbors($nodeId) as neighbors", "nodeId", nodeId);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("neighbors")).isEqualTo(1L);
  }

  // Phase 4: Cross-Language Tests

  @Test
  void cypherFunctionCallableFromSQL() {
    database.command("sql", "DEFINE FUNCTION cypher.greet \"RETURN 'Hello ' + $name\" PARAMETERS [name] LANGUAGE cypher");

    // Call Cypher function from SQL
    final ResultSet rs = database.command("sql", "SELECT `cypher.greet`('World') as greeting");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("greeting")).isEqualTo("Hello World");
  }

  @Test
  void sqlFunctionCallableFromCypher() {
    // Use a custom library name to avoid conflicts with built-in SQL functions
    // Test that SQL function can be called from Cypher
    database.command("sql", "DEFINE FUNCTION custom.addTen \"SELECT :value + 10\" PARAMETERS [value] LANGUAGE sql");

    // Call SQL function from Cypher
    final ResultSet rs = database.query("opencypher", "RETURN custom.addTen(5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(15L);
  }

  @Test
  void javaScriptFunctionCallableFromCypher() {
    database.command("sql", "DEFINE FUNCTION js.power \"return Math.pow(x, y)\" PARAMETERS [x,y] LANGUAGE js");

    // Call JS function from Cypher
    final ResultSet rs = database.query("opencypher", "RETURN js.power(2, 3) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(8);
  }

  // Phase 5: Edge Cases

  @Test
  void functionWithNoParameters() {
    database.command("sql", "DEFINE FUNCTION test.pi \"SELECT 3.14159\" LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN test.pi() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Float>getProperty("result")).isEqualTo(3.14159f);
  }

  @Test
  void functionInComplexExpression() {
    database.command("sql", "DEFINE FUNCTION math.square \"SELECT :x * :x\" PARAMETERS [x] LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN math.square(3) + math.square(4) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("result")).isEqualTo(25L);
  }

  @Test
  void functionWithGraphDataInCypher() {
    database.command("sql", "DEFINE FUNCTION cypher.getLabel \"MATCH (n) WHERE id(n) = $id RETURN labels(n)[0] as lbl\" PARAMETERS [id] LANGUAGE cypher");

    // Create node
    final ResultSet createResult = database.command("opencypher", "CREATE (n:TestNode) RETURN id(n) as nid");
    assertThat(createResult.hasNext()).isTrue();
    final String nodeId = createResult.next().getProperty("nid");

    final ResultSet rs = database.query("opencypher", "RETURN cypher.getLabel($nodeId) as label", "nodeId", nodeId);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("label")).isEqualTo("TestNode");
  }

  @Test
  void multipleFunctionCalls() {
    database.command("sql", "DEFINE FUNCTION math.add \"SELECT :a + :b\" PARAMETERS [a,b] LANGUAGE sql");
    database.command("sql", "DEFINE FUNCTION math.sub \"SELECT :a - :b\" PARAMETERS [a,b] LANGUAGE sql");

    final ResultSet rs = database.query("opencypher", "RETURN math.add(10, 5) as sum, math.sub(10, 5) as diff");
    assertThat(rs.hasNext()).isTrue();
    final var result = rs.next();
    assertThat(result.<Long>getProperty("sum")).isEqualTo(15);
    assertThat(result.<Long>getProperty("diff")).isEqualTo(5);
  }
}
