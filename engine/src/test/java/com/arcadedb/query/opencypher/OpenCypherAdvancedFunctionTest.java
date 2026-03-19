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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for advanced Cypher functions: Path, List, String, and Type Conversion functions.
 */
class OpenCypherAdvancedFunctionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-advanced-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Create test data:
    //   Alice KNOWS Bob KNOWS Charlie
    database.command("opencypher",
        """
        CREATE (alice:Person {name: 'Alice', age: 30}), \
        (bob:Person {name: 'Bob', age: 25}), \
        (charlie:Person {name: 'Charlie', age: 35}), \
        (alice)-[:KNOWS {since: 2020}]->(bob), \
        (bob)-[:KNOWS {since: 2021}]->(charlie)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  // ==================== String Functions ====================

  @Test
  void leftFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN left('Hello World', 5) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("Hello");
  }

  @Test
  void rightFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN right('Hello World', 5) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("World");
  }

  @Test
  void reverseStringFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN reverse('Hello') AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("olleH");
  }

  @Test
  void splitFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN split('one,two,three', ',') AS result");

    assertThat(result.hasNext()).isTrue();
    final List<?> parts = (List<?>) result.next().getProperty("result");
    assertThat(parts).hasSize(3);
    assertThat(parts.get(0)).isEqualTo("one");
    assertThat(parts.get(1)).isEqualTo("two");
    assertThat(parts.get(2)).isEqualTo("three");
  }

  // ==================== List Functions ====================

  @Test
  void sizeFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN size([1, 2, 3, 4, 5]) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).longValue()).isEqualTo(5L);
  }

  @Test
  void sizeFunctionOnString() {
    final ResultSet result = database.command("opencypher",
        "RETURN size('Hello') AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).longValue()).isEqualTo(5L);
  }

  @Test
  void headFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN head([1, 2, 3, 4, 5]) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  @Test
  void tailFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN tail([1, 2, 3, 4, 5]) AS result");

    assertThat(result.hasNext()).isTrue();
    final List<?> tail = (List<?>) result.next().getProperty("result");
    assertThat(tail).hasSize(4);
    assertThat(((Number) tail.get(0)).intValue()).isEqualTo(2);
    assertThat(((Number) tail.get(3)).intValue()).isEqualTo(5);
  }

  @Test
  void lastFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN last([1, 2, 3, 4, 5]) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void rangeFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN range(1, 5) AS result");

    assertThat(result.hasNext()).isTrue();
    final List<?> range = (List<?>) result.next().getProperty("result");
    assertThat(range).hasSize(5);
    assertThat(((Number) range.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) range.get(4)).longValue()).isEqualTo(5L);
  }

  @Test
  void rangeFunctionWithStep() {
    final ResultSet result = database.command("opencypher",
        "RETURN range(0, 10, 2) AS result");

    assertThat(result.hasNext()).isTrue();
    final List<?> range = (List<?>) result.next().getProperty("result");
    assertThat(range).hasSize(6); // 0, 2, 4, 6, 8, 10
    assertThat(((Number) range.get(0)).longValue()).isEqualTo(0L);
    assertThat(((Number) range.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) range.get(5)).longValue()).isEqualTo(10L);
  }

  @Test
  void reverseListFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN reverse([1, 2, 3, 4, 5]) AS result");

    assertThat(result.hasNext()).isTrue();
    final List<?> reversed = (List<?>) result.next().getProperty("result");
    assertThat(reversed).hasSize(5);
    assertThat(((Number) reversed.get(0)).intValue()).isEqualTo(5);
    assertThat(((Number) reversed.get(4)).intValue()).isEqualTo(1);
  }

  // ==================== Type Conversion Functions ====================

  @Test
  void toStringFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN toString(123) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("123");
  }

  @Test
  void toIntegerFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN toInteger('42') AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).longValue()).isEqualTo(42L);
  }

  @Test
  void toFloatFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN toFloat('3.14') AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isEqualTo(3.14);
  }

  @Test
  void toBooleanFunction() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBoolean('true') AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void toBooleanFunctionWithNumber() {
    // toBoolean() supports integers: 0 → false, non-zero → true (issue #3418)
    final ResultSet result = database.command("opencypher",
        "RETURN toBoolean(1) AS result");

    assertThat(result.hasNext()).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  // ==================== Path Functions ====================

  @Test
  @Disabled("Requires path matching with variable length relationships to be fully implemented")
  void nodesFunction() {
    // Get a path and extract nodes from it
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*2]->(c:Person) \
        RETURN nodes(p) AS nodeList""");

    assertThat(result.hasNext()).isTrue();
    final List<?> nodes = (List<?>) result.next().getProperty("nodeList");
    assertThat(nodes).hasSize(3); // Alice, Bob, Charlie
    assertThat(nodes.get(0)).isInstanceOf(Vertex.class);
    assertThat(nodes.get(1)).isInstanceOf(Vertex.class);
    assertThat(nodes.get(2)).isInstanceOf(Vertex.class);
  }

  @Test
  @Disabled("Requires path matching with variable length relationships to be fully implemented")
  void relationshipsFunction() {
    // Get a path and extract relationships from it
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*2]->(c:Person) \
        RETURN relationships(p) AS relList""");

    assertThat(result.hasNext()).isTrue();
    final List<?> rels = (List<?>) result.next().getProperty("relList");
    assertThat(rels).hasSize(2); // Two KNOWS relationships
    assertThat(rels.get(0)).isInstanceOf(Edge.class);
    assertThat(rels.get(1)).isInstanceOf(Edge.class);
  }

  @Test
  @Disabled("Requires path matching with variable length relationships to be fully implemented")
  void lengthFunctionOnPath() {
    // Get a path and return its length (number of relationships)
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*2]->(c:Person) \
        RETURN length(p) AS pathLength""");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("pathLength")).longValue()).isEqualTo(2L);
  }

  @Test
  void lengthFunctionNull() {
    // Length also works on strings
    final ResultSet result = database.command("opencypher",
        "RETURN length(null) as result");

    assertThat(result.hasNext()).isTrue();
    assertThat((Object) result.next().getProperty("RETURN length(null) as result")).isNull();
  }

  @Test
  void lengthFunctionOnString() {
    // Length also works on strings
    final ResultSet result = database.command("opencypher",
        "RETURN length('Hello World') AS stringLength");

    assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("stringLength")).longValue()).isEqualTo(11L);
  }

  // ==================== Combined Function Tests ====================

  @Test
  void combinedStringFunctions() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person {name: 'Alice'}) \
        RETURN left(n.name, 2) AS leftPart, \
               right(n.name, 2) AS rightPart, \
               reverse(n.name) AS reversed""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((String) row.getProperty("leftPart")).isEqualTo("Al");
    assertThat((String) row.getProperty("rightPart")).isEqualTo("ce");
    assertThat((String) row.getProperty("reversed")).isEqualTo("ecilA");
  }

  @Test
  @Disabled("Requires WITH clause to be fully implemented")
  void combinedListFunctions() {
    final ResultSet result = database.command("opencypher",
        """
        WITH [1, 2, 3, 4, 5] AS list \
        RETURN size(list) AS listSize, \
               head(list) AS first, \
               last(list) AS lastElem""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("listSize")).longValue()).isEqualTo(5L);
    assertThat(((Number) row.getProperty("first")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("lastElem")).intValue()).isEqualTo(5);
  }

  @Test
  void typeConversionChain() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person {name: 'Bob'}) \
        RETURN toString(n.age) AS ageString, \
               toInteger(toString(n.age)) AS ageInt""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((String) row.getProperty("ageString")).isEqualTo("25");
    assertThat(((Number) row.getProperty("ageInt")).longValue()).isEqualTo(25L);
  }

  // ==================== "cypher" Language Alias ====================

  @Test
  void cypherLanguageAlias() {
    // Verify that "cypher" language maps to the OpenCypher engine when Gremlin module is not present
    final ResultSet result = database.command("cypher", "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Alice");
  }
}
