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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for CypherFunctionFactory.
 * Tests Cypher function name mappings and function execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFunctionFactoryTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-cypher-functions").create();
    database.getSchema().createVertexType("TestNode");

    database.transaction(() -> {
      database.newVertex("TestNode")
          .set("name", "Alice")
          .set("value", 42)
          .set("price", 19.99)
          .set("text", "Hello World")
          .save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void shouldMapAbsFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN abs(-5) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.longValue()).isEqualTo(5L);
  }


  @Test
  void shouldMapCoalesceFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN coalesce(null, null, 'value', 'default') AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("value");
  }

  @Test
  void shouldMapRangeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN range(1, 5) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldMapHeadFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN head([1, 2, 3, 4]) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.longValue()).isEqualTo(1L);
  }

  @Test
  void shouldMapLastFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN last([1, 2, 3, 4]) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.longValue()).isEqualTo(4L);
  }

  @Test
  void shouldMapTailFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN tail([1, 2, 3, 4]) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldMapKeysFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN keys(n) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldMapTypeFunction() {
    database.transaction(() -> {
      final var alice = database.newVertex("TestNode").set("name", "Alice").save();
      final var bob = database.newVertex("TestNode").set("name", "Bob").save();
      database.getSchema().createEdgeType("KNOWS");
      alice.newEdge("KNOWS", bob, true, (Object[]) null).save();
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH ()-[r:KNOWS]->() RETURN type(r) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("KNOWS");
  }

  @Test
  void shouldMapStartNodeFunction() {
    database.transaction(() -> {
      final var alice = database.newVertex("TestNode").set("name", "Alice").save();
      final var bob = database.newVertex("TestNode").set("name", "Bob").save();
      database.getSchema().createEdgeType("KNOWS");
      alice.newEdge("KNOWS", bob, true, (Object[]) null).save();
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a)-[r:KNOWS]->(b) RETURN startNode(r).name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }

  @Test
  void shouldMapEndNodeFunction() {
    database.transaction(() -> {
      final var alice = database.newVertex("TestNode").set("name", "Alice").save();
      final var bob = database.newVertex("TestNode").set("name", "Bob").save();
      database.getSchema().createEdgeType("KNOWS");
      alice.newEdge("KNOWS", bob, true, (Object[]) null).save();
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a)-[r:KNOWS]->(b) RETURN endNode(r).name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Bob");
  }

  @Test
  void shouldHandleFunctionChaining() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN coalesce(null, abs(-5)) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.longValue()).isEqualTo(5L);
  }

  @Test
  void shouldHandleNestedFunctions() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN abs(abs(-4)) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.longValue()).isEqualTo(4L);
  }

  @Test
  void shouldHandleFunctionsInReturnExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN n.value * 2 AS doubled, abs(n.value - 100) AS diff");

    final boolean hasNext = resultSet.hasNext();
    assertThat(hasNext).isTrue();
    final Result result = resultSet.next();
    final Number doubled = result.getProperty("doubled");
    final Number diff = result.getProperty("diff");
    assertThat(doubled.longValue()).isEqualTo(84L);
    assertThat(diff.longValue()).isEqualTo(58L);
  }

  @Test
  void shouldHandleFunctionsWithNullValues() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN coalesce(null, null, 123) AS result");

    final boolean hasNext = resultSet.hasNext();
    assertThat(hasNext).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.longValue()).isEqualTo(123L);
  }

  @Test
  void shouldMapIdFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN id(n) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isNotNull();
  }

  @Test
  void shouldMapLabelsFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN labels(n) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldMapPropertiesFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN properties(n) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldMapCountFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN count(n) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number count = result.getProperty("result");
    assertThat(count.longValue()).isEqualTo(1L);
  }

  @Test
  void shouldMapSumFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN sum(n.value) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number sum = result.getProperty("result");
    assertThat(sum.longValue()).isEqualTo(42L);
  }

  @Test
  void shouldMapAvgFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN avg(n.value) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number avg = result.getProperty("result");
    assertThat(avg.doubleValue()).isCloseTo(42.0, within(0.01));
  }

  @Test
  void shouldMapMinFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN min(n.value) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number min = result.getProperty("result");
    assertThat(min.longValue()).isEqualTo(42L);
  }

  @Test
  void shouldMapMaxFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN max(n.value) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number max = result.getProperty("result");
    assertThat(max.longValue()).isEqualTo(42L);
  }

  @Test
  void shouldMapCollectFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:TestNode) RETURN collect(n.name) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }
}
