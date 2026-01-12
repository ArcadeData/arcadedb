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
package com.arcadedb.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Cypher function support.
 */
public class OpenCypherFunctionTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./databases/test-function").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("KNOWS");

    // Create test data
    database.transaction(() -> {
      final Vertex alice = database.newVertex("Person")
          .set("name", "Alice")
          .set("age", 30)
          .save();

      final Vertex bob = database.newVertex("Person")
          .set("name", "Bob")
          .set("age", 25)
          .save();

      final Vertex charlie = database.newVertex("Person")
          .set("name", "Charlie")
          .set("age", 35)
          .save();

      final Vertex arcadedb = database.newVertex("Company")
          .set("name", "ArcadeDB")
          .set("founded", 2021)
          .save();

      // Create relationships
      alice.newEdge("KNOWS", bob, "since", 2020).save();
      alice.newEdge("WORKS_AT", arcadedb, "since", 2021).save();
      bob.newEdge("WORKS_AT", arcadedb, "since", 2022).save();
      charlie.newEdge("KNOWS", alice, "since", 2019).save();
    });
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  public void testIdFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN id(n) AS personId");

    int count = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      assertNotNull(result.getProperty("personId"));
      assertTrue(result.getProperty("personId").toString().contains("#"));
      count++;
    }

    assertEquals(3, count, "Expected 3 persons");
  }

  @Test
  public void testLabelsFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person {name: 'Alice'}) RETURN labels(n) AS personLabels");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object labels = result.getProperty("personLabels");

    assertNotNull(labels);
    assertTrue(labels instanceof List);
    final List<?> labelsList = (List<?>) labels;
    assertTrue(labelsList.contains("Person"));
  }

  @Test
  public void testTypeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r]->(b) RETURN type(r) AS relType");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final String relType = (String) result.getProperty("relType");

    assertNotNull(relType);
    assertTrue(relType.equals("KNOWS") || relType.equals("WORKS_AT"));
  }

  @Test
  public void testKeysFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person {name: 'Alice'}) RETURN keys(n) AS personKeys");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object keys = result.getProperty("personKeys");

    assertNotNull(keys);
    assertTrue(keys instanceof List);
    final List<?> keysList = (List<?>) keys;
    assertTrue(keysList.contains("name"));
    assertTrue(keysList.contains("age"));
  }

  @Test
  public void testCountFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN count(n) AS totalPersons");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object count = result.getProperty("totalPersons");

    assertNotNull(count);
    assertEquals(3L, ((Number) count).longValue());
  }

  @Test
  public void testCountStar() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN count(*) AS total");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object count = result.getProperty("total");

    assertNotNull(count);
    assertEquals(3L, ((Number) count).longValue());
  }

  @Test
  public void testSumFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN sum(n.age) AS totalAge");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object sum = result.getProperty("totalAge");

    assertNotNull(sum);
    assertEquals(90, ((Number) sum).intValue()); // 30 + 25 + 35
  }

  @Test
  public void testAvgFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN avg(n.age) AS averageAge");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object avg = result.getProperty("averageAge");

    assertNotNull(avg);
    assertEquals(30.0, ((Number) avg).doubleValue(), 0.01); // (30 + 25 + 35) / 3
  }

  @Test
  public void testMinFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN min(n.age) AS minAge");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object min = result.getProperty("minAge");

    assertNotNull(min);
    assertEquals(25, ((Number) min).intValue());
  }

  @Test
  public void testMaxFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN max(n.age) AS maxAge");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object max = result.getProperty("maxAge");

    assertNotNull(max);
    assertEquals(35, ((Number) max).intValue());
  }

  @Test
  public void testAbsFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN abs(-42) AS absValue");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object abs = result.getProperty("absValue");

    assertNotNull(abs);
    assertEquals(42, ((Number) abs).intValue());
  }

  @Test
  public void testSqrtFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN sqrt(16) AS sqrtValue");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object sqrt = result.getProperty("sqrtValue");

    assertNotNull(sqrt);
    assertEquals(4.0, ((Number) sqrt).doubleValue(), 0.01);
  }

  @Test
  public void testStartNodeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN startNode(r) AS source");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object source = result.getProperty("source");

    assertNotNull(source);
    assertTrue(source instanceof Vertex);
  }

  @Test
  public void testEndNodeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN endNode(r) AS target");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object target = result.getProperty("target");

    assertNotNull(target);
    assertTrue(target instanceof Vertex);
  }
}
