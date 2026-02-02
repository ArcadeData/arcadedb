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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for Cypher function support.
 */
class OpenCypherFunctionTest {
  private Database database;

  @BeforeEach
  void setup() {
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
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void idFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN id(n) AS personId");

    int count = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      assertThat(result.<String>getProperty("personId")).isNotNull();
      assertThat(result.<String>getProperty("personId").contains("#")).isTrue();
      count++;
    }

    assertThat(count).as("Expected 3 persons").isEqualTo(3);
  }

  @Test
  void labelsFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person {name: 'Alice'}) RETURN labels(n) AS personLabels");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object labels = result.getProperty("personLabels");

    assertThat(labels).isNotNull();
    assertThat(labels).isInstanceOf(List.class);
    final List<?> labelsList = (List<?>) labels;
    assertThat(labelsList.contains("Person")).isTrue();
  }

  @Test
  void typeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r]->(b) RETURN type(r) AS relType");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final String relType = result.getProperty("relType");

    assertThat(relType).isNotNull();
    assertThat(relType.equals("KNOWS") || relType.equals("WORKS_AT")).isTrue();
  }

  @Test
  void keysFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person {name: 'Alice'}) RETURN keys(n) AS personKeys");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object keys = result.getProperty("personKeys");

    assertThat(keys).isNotNull();
    assertThat(keys).isInstanceOf(List.class);
    final List<?> keysList = (List<?>) keys;
    assertThat(keysList.contains("name")).isTrue();
    assertThat(keysList.contains("age")).isTrue();
  }

  @Test
  void countFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN count(n) AS totalPersons");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object count = result.getProperty("totalPersons");

    assertThat(count).isNotNull();
    assertThat(((Number) count).longValue()).isEqualTo(3L);
  }

  @Test
  void countStar() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN count(*) AS total");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object count = result.getProperty("total");

    assertThat(count).isNotNull();
    assertThat(((Number) count).longValue()).isEqualTo(3L);
  }

  @Test
  void sumFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN sum(n.age) AS totalAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object sum = result.getProperty("totalAge");

    assertThat(sum).isNotNull();
    assertThat(((Number) sum).intValue()).isEqualTo(90); // 30 + 25 + 35
  }

  @Test
  void avgFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN avg(n.age) AS averageAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object avg = result.getProperty("averageAge");

    assertThat(avg).isNotNull();
    assertThat(((Number) avg).doubleValue()).isCloseTo(30.0, within(0.01)); // (30 + 25 + 35) / 3
  }

  @Test
  void minFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN min(n.age) AS minAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object min = result.getProperty("minAge");

    assertThat(min).isNotNull();
    assertThat(((Number) min).intValue()).isEqualTo(25);
  }

  @Test
  void maxFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN max(n.age) AS maxAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object max = result.getProperty("maxAge");

    assertThat(max).isNotNull();
    assertThat(((Number) max).intValue()).isEqualTo(35);
  }

  @Test
  void absFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN abs(-42) AS absValue");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object abs = result.getProperty("absValue");

    assertThat(abs).isNotNull();
    assertThat(((Number) abs).intValue()).isEqualTo(42);
  }

  @Test
  void sqrtFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN sqrt(16) AS sqrtValue");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object sqrt = result.getProperty("sqrtValue");

    assertThat(sqrt).isNotNull();
    assertThat(((Number) sqrt).doubleValue()).isCloseTo(4.0, within(0.01));
  }

  @Test
  void startNodeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN startNode(r) AS source");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Single-variable RETURN with Document result is unwrapped to element
    assertThat(result.isElement()).isTrue();
    final Object source = result.toElement();

    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(Vertex.class);
  }

  @Test
  void endNodeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN endNode(r) AS target");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Single-variable RETURN with Document result is unwrapped to element
    assertThat(result.isElement()).isTrue();
    final Object target = result.toElement();

    assertThat(target).isNotNull();
    assertThat(target).isInstanceOf(Vertex.class);
  }
}
