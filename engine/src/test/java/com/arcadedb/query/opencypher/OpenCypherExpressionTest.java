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

import static org.junit.jupiter.api.Assertions.*;

;

/**
 * Tests for Cypher expression features:
 * - Arithmetic expressions (n.age * 2)
 * - Map literals ({name: 'Alice'})
 * - List comprehensions ([x IN list | x.name])
 * - Map projections (n{.name, .age})
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OpenCypherExpressionTest {
  private Database database;

  @BeforeEach
  public void setup() {
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
  public void teardown() {
    if (database != null)
      database.drop();
  }

  // ============================================================================
  // Arithmetic Expression Tests
  // ============================================================================

  @Test
  public void testArithmeticMultiplication() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age * 2 AS doubleAge");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(60L), result.getProperty("doubleAge"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testArithmeticAddition() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age + 10 AS agePlus10");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(40L), result.getProperty("agePlus10"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testArithmeticSubtraction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age - 5 AS ageMinus5");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(25L), result.getProperty("ageMinus5"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testArithmeticDivision() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age / 2 AS halfAge");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(15.0, result.getProperty("halfAge"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testArithmeticModulo() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age % 7 AS modulo");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(2L), result.getProperty("modulo"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testArithmeticComplexExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age * 2 + 10 AS result");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    // 30 * 2 + 10 = 70
    assertEquals(Long.valueOf(70L), result.getProperty("result"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testArithmeticWithLiterals() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN 10 * 5 + 3 AS result");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(53L), result.getProperty("result"));
    assertFalse(resultSet.hasNext());
  }

  // ============================================================================
  // Map Literal Tests
  // ============================================================================

  @Test
  public void testMapLiteralSimple() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN {name: 'Alice', age: 30} AS person");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertTrue(personObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertEquals("Alice", person.get("name"));
    assertEquals(Long.valueOf(30L), person.get("age"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testMapLiteralWithExpressions() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN {personName: n.name, doubled: n.age * 2} AS info");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object infoObj = result.getProperty("info");
    assertTrue(infoObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> info = (Map<String, Object>) infoObj;
    assertEquals("Alice", info.get("personName"));
    assertEquals(Long.valueOf(60L), info.get("doubled"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testMapLiteralEmpty() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN {} AS emptyMap");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object mapObj = result.getProperty("emptyMap");
    assertTrue(mapObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) mapObj;
    assertTrue(map.isEmpty());
    assertFalse(resultSet.hasNext());
  }

  // ============================================================================
  // List Comprehension Tests
  // ============================================================================

  @Test
  public void testListComprehensionSimple() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN [1, 2, 3] | x * 2] AS doubled");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("doubled");
    assertTrue(listObj instanceof List, "Expected List but got: " + (listObj == null ? "null" : listObj.getClass().getName()));
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertEquals(3, list.size());
    assertEquals(Long.valueOf(2L), list.get(0));
    assertEquals(Long.valueOf(4L), list.get(1));
    assertEquals(Long.valueOf(6L), list.get(2));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testListComprehensionWithFilter() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 10] AS filtered");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("filtered");
    assertTrue(listObj instanceof List);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertEquals(3, list.size());
    assertEquals(Long.valueOf(30L), list.get(0));
    assertEquals(Long.valueOf(40L), list.get(1));
    assertEquals(Long.valueOf(50L), list.get(2));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testListComprehensionWithRange() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN range(1, 5) | x * x] AS squares");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("squares");
    assertTrue(listObj instanceof List);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertEquals(5, list.size());
    assertEquals(Long.valueOf(1L), list.get(0));
    assertEquals(Long.valueOf(4L), list.get(1));
    assertEquals(Long.valueOf(9L), list.get(2));
    assertEquals(Long.valueOf(16L), list.get(3));
    assertEquals(Long.valueOf(25L), list.get(4));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testListComprehensionFilterOnly() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 3] AS filtered");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object listObj = result.getProperty("filtered");
    assertTrue(listObj instanceof List);
    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) listObj;
    assertEquals(2, list.size());
    assertEquals(Long.valueOf(4L), list.get(0));
    assertEquals(Long.valueOf(5L), list.get(1));
    assertFalse(resultSet.hasNext());
  }

  // ============================================================================
  // Map Projection Tests
  // ============================================================================

  @Test
  public void testMapProjectionSimple() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n{.name, .age} AS person");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertTrue(personObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertEquals("Alice", person.get("name"));
    assertEquals(30, person.get("age"));
    // Should NOT contain salary since it wasn't projected
    assertFalse(person.containsKey("salary"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testMapProjectionWithComputedValue() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n{.name, doubleAge: n.age * 2} AS person");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertTrue(personObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertEquals("Alice", person.get("name"));
    assertEquals(Long.valueOf(60L), person.get("doubleAge"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testMapProjectionAllProperties() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n{.*} AS person");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object personObj = result.getProperty("person");
    assertTrue(personObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> person = (Map<String, Object>) personObj;
    assertEquals("Alice", person.get("name"));
    assertEquals(Integer.valueOf(30), person.get("age"));
    assertEquals(Integer.valueOf(50000), person.get("salary"));
    assertFalse(resultSet.hasNext());
  }

  // ============================================================================
  // Combined Expression Tests
  // ============================================================================

  @Test
  public void testCombinedMapWithArithmetic() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' " +
            "RETURN {name: n.name, nextYearAge: n.age + 1, monthlySalary: n.salary / 12} AS info");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object infoObj = result.getProperty("info");
    assertTrue(infoObj instanceof Map);
    @SuppressWarnings("unchecked")
    final Map<String, Object> info = (Map<String, Object>) infoObj;
    assertEquals("Alice", info.get("name"));
    assertEquals(Long.valueOf(31L), info.get("nextYearAge"));
    // 50000 / 12 ≈ 4166.67
    assertTrue(info.get("monthlySalary") instanceof Double);
    assertFalse(resultSet.hasNext());
  }
}
