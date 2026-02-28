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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Cypher expression features:
 * - Arithmetic expressions (n.age * 2)
 * - Map literals ({name: 'Alice'})
 * - List comprehensions ([x IN list | x.name])
 * - Map projections (n{.name, .age})
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherExpressionTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-expression").create();

    // Create schema
    database.getSchema().createVertexType("Person");

    // Create test data
    database.transaction(() -> {
      database.newVertex("Person")
          .set("name", "Alice")
          .set("age", 30)
          .set("salary", 50000)
          .save();

      database.newVertex("Person")
          .set("name", "Bob")
          .set("age", 25)
          .set("salary", 45000)
          .save();

      database.newVertex("Person")
          .set("name", "Charlie")
          .set("age", 35)
          .set("salary", 60000)
          .save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ============================================================================
  // Arithmetic Expression Tests
  // ============================================================================

  @Test
  void arithmeticMultiplication() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age * 2 AS doubleAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("doubleAge")).isEqualTo(Long.valueOf(60L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void arithmeticAddition() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age + 10 AS agePlus10");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("agePlus10")).isEqualTo(Long.valueOf(40L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void arithmeticSubtraction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age - 5 AS ageMinus5");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("ageMinus5")).isEqualTo(Long.valueOf(25L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void arithmeticDivision() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age / 2 AS halfAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("halfAge")).isEqualTo(15L);
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void arithmeticModulo() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age % 7 AS modulo");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("modulo")).isEqualTo(Long.valueOf(2L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void arithmeticComplexExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age * 2 + 10 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // 30 * 2 + 10 = 70
    assertThat(result.<Long>getProperty("result")).isEqualTo(70L);
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void arithmeticWithLiterals() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN 10 * 5 + 3 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("result")).isEqualTo(53L);
    assertThat(resultSet.hasNext()).isFalse();
  }

  // ============================================================================
  // Map Literal Tests
  // ============================================================================

  @Test
  void mapLiteralSimple() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN {name: 'Alice', age: 30} AS person");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertThat(personObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertThat(person.get("name")).isEqualTo("Alice");
    assertThat(person.get("age")).isEqualTo(Long.valueOf(30L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void mapLiteralWithExpressions() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN {personName: n.name, doubled: n.age * 2} AS info");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object infoObj = result.getProperty("info");
    assertThat(infoObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> info = (Map<String, Object>) infoObj;
    assertThat(info.get("personName")).isEqualTo("Alice");
    assertThat(info.get("doubled")).isEqualTo(Long.valueOf(60L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void mapLiteralEmpty() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN {} AS emptyMap");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object mapObj = result.getProperty("emptyMap");
    assertThat(mapObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) mapObj;
    assertThat(map.isEmpty()).isTrue();
    assertThat(resultSet.hasNext()).isFalse();
  }

  // ============================================================================
  // List Comprehension Tests
  // ============================================================================

  @Test
  void listComprehensionSimple() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN [1, 2, 3] | x * 2] AS doubled");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("doubled");
    assertThat(listObj).as("Expected List but got: " + (listObj == null ? "null" : listObj.getClass().getName()))
        .isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertThat(list.size()).isEqualTo(3);
    assertThat(list.get(0)).isEqualTo(Long.valueOf(2L));
    assertThat(list.get(1)).isEqualTo(Long.valueOf(4L));
    assertThat(list.get(2)).isEqualTo(Long.valueOf(6L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void listComprehensionWithFilter() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 10] AS filtered");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("filtered");
    assertThat(listObj).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertThat(list.size()).isEqualTo(3);
    assertThat(list.get(0)).isEqualTo(Long.valueOf(30L));
    assertThat(list.get(1)).isEqualTo(Long.valueOf(40L));
    assertThat(list.get(2)).isEqualTo(Long.valueOf(50L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void listComprehensionWithRange() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN range(1, 5) | x * x] AS squares");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("squares");
    assertThat(listObj).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertThat(list.size()).isEqualTo(5);
    assertThat(list.get(0)).isEqualTo(Long.valueOf(1L));
    assertThat(list.get(1)).isEqualTo(Long.valueOf(4L));
    assertThat(list.get(2)).isEqualTo(Long.valueOf(9L));
    assertThat(list.get(3)).isEqualTo(Long.valueOf(16L));
    assertThat(list.get(4)).isEqualTo(Long.valueOf(25L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void listComprehensionFilterOnly() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 3] AS filtered");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("filtered");
    assertThat(listObj).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertThat(list.size()).isEqualTo(2);
    assertThat(list.get(0)).isEqualTo(Long.valueOf(4L));
    assertThat(list.get(1)).isEqualTo(Long.valueOf(5L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void listComprehensionWithModuloFilter() {
    // GitHub issue #3330: list comprehension with modulo filter returns empty list
    final ResultSet resultSet = database.query("opencypher",
        "WITH [0, 1, 2, 3, 4, 5] AS numbers RETURN [n IN numbers WHERE n % 2 = 0 | n * 10] AS evens_multiplied");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("evens_multiplied");
    assertThat(listObj).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertThat(list).containsExactly(0L, 20L, 40L);
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void listComprehensionInWhereWithLabelsAndToLower() {
    // TCK List12 Scenario [6]: list comprehension in WHERE with labels() and toLower()
    // The filter references variables bound in later MATCH steps (b),
    // so it must not be pushed down to the node scan of (n).
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:A {name: 'c'}) CREATE (a)-[:T]->(:B), (a)-[:T]->(:C)");
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n)-->(b) WHERE n.name IN [x IN labels(b) | toLower(x)] RETURN b");

    assertThat(resultSet.hasNext()).as("Should match b=(:C) since toLower('C')='c'=n.name").isTrue();
    resultSet.next();
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void comparisonWithArithmeticOperands() {
    // Verify that comparisons work when operands contain arithmetic expressions
    final ResultSet resultSet = database.query("opencypher", "RETURN 4 % 2 = 0 AS result");
    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Boolean) result.getProperty("result")).isTrue();
    assertThat(resultSet.hasNext()).isFalse();
  }

  // ============================================================================
  // Map Projection Tests
  // ============================================================================

  @Test
  void mapProjectionSimple() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n{.name, .age} AS person");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertThat(personObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertThat(person.get("name")).isEqualTo("Alice");
    assertThat(person.get("age")).isEqualTo(30);
    // Should NOT contain salary since it wasn't projected
    assertThat(person.containsKey("salary")).isFalse();
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void mapProjectionWithComputedValue() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n{.name, doubleAge: n.age * 2} AS person");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertThat(personObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertThat(person.get("name")).isEqualTo("Alice");
    assertThat(person.get("doubleAge")).isEqualTo(Long.valueOf(60L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void mapProjectionAllProperties() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n{.*} AS person");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertThat(personObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertThat(person.get("name")).isEqualTo("Alice");
    assertThat(person.get("age")).isEqualTo(Integer.valueOf(30));
    assertThat(person.get("salary")).isEqualTo(Integer.valueOf(50000));
    assertThat(resultSet.hasNext()).isFalse();
  }

  // ============================================================================
  // Combined Expression Tests
  // ============================================================================

  @Test
  void combinedMapWithArithmetic() {
    final ResultSet resultSet = database.query("opencypher",
        """
        MATCH (n:Person) WHERE n.name = 'Alice' \
        RETURN {name: n.name, nextYearAge: n.age + 1, monthlySalary: n.salary / 12} AS info""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object infoObj = result.getProperty("info");
    assertThat(infoObj).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    final Map<String, Object> info = (Map<String, Object>) infoObj;
    assertThat(info.get("name")).isEqualTo("Alice");
    assertThat(info.get("nextYearAge")).isEqualTo(Long.valueOf(31L));
    // 50000 / 12 = 4166 (Cypher integer division truncates)
    assertThat(info.get("monthlySalary")).isEqualTo(4166L);
    assertThat(resultSet.hasNext()).isFalse();
  }
}
