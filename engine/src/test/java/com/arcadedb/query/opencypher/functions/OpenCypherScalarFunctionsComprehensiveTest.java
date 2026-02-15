/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for OpenCypher Scalar functions based on Neo4j Cypher documentation.
 * Tests cover: char_length(), character_length(), coalesce(), elementId(), endNode(),
 * head(), id(), last(), length(), nullIf(), properties(), randomUUID(), size(),
 * startNode(), timestamp(), toBoolean(), toBooleanOrNull(), toFloat(), toFloatOrNull(),
 * toInteger(), toIntegerOrNull(), type(), valueType()
 */
class OpenCypherScalarFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-scalar-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create test graph matching Neo4j documentation
    database.getSchema().createVertexType("Developer");
    database.getSchema().createVertexType("Administrator");
    database.getSchema().createVertexType("Designer");
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("MARRIED");

    database.command("opencypher",
        "CREATE " +
            "(alice:Developer {name:'Alice', age: 38, eyes: 'Brown'}), " +
            "(bob:Administrator {name: 'Bob', age: 25, eyes: 'Blue'}), " +
            "(charlie:Administrator {name: 'Charlie', age: 53, eyes: 'Green'}), " +
            "(daniel:Administrator {name: 'Daniel', age: 54, eyes: 'Brown'}), " +
            "(eskil:Designer {name: 'Eskil', age: 41, eyes: 'blue', likedColors: ['Pink', 'Yellow', 'Black']}), " +
            "(alice)-[:KNOWS]->(bob), " +
            "(alice)-[:KNOWS]->(charlie), " +
            "(bob)-[:KNOWS]->(daniel), " +
            "(charlie)-[:KNOWS]->(daniel), " +
            "(bob)-[:MARRIED]->(eskil)");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== char_length() Tests ====================

  @Test
  void charLengthBasic() {
    final ResultSet result = database.command("opencypher", "RETURN char_length('Alice') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void charLengthEmptyString() {
    final ResultSet result = database.command("opencypher", "RETURN char_length('') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(0);
  }

  @Test
  void charLengthUnicode() {
    final ResultSet result = database.command("opencypher", "RETURN char_length('Hello 世界') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(8);
  }

  @Test
  void charLengthNull() {
    final ResultSet result = database.command("opencypher", "RETURN char_length(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== character_length() Tests ====================

  @Test
  void characterLengthBasic() {
    final ResultSet result = database.command("opencypher", "RETURN character_length('Alice') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void characterLengthNull() {
    final ResultSet result = database.command("opencypher", "RETURN character_length(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coalesce() Tests ====================

  @Test
  void coalesceBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Alice' RETURN coalesce(a.hairColor, a.eyes) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("Brown");
  }

  @Test
  void coalesceMultipleArgs() {
    final ResultSet result = database.command("opencypher",
        "RETURN coalesce(null, null, 'third', 'fourth') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("third");
  }

  @Test
  void coalesceAllNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN coalesce(null, null, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void coalesceFirstNonNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN coalesce('first', null, 'third') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("first");
  }

  // ==================== elementId() Tests ====================

  @Test
  void elementIdNode() {
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Developer) RETURN elementId(n) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String elementId = (String) result.next().getProperty("result");
    assertThat(elementId).isNotNull();
    assertThat(elementId).isNotEmpty();
  }

  @Test
  void elementIdRelationship() {
    final ResultSet result = database.command("opencypher",
        "MATCH (:Developer)-[r]-() RETURN elementId(r) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String elementId = (String) result.next().getProperty("result");
    assertThat(elementId).isNotNull();
    assertThat(elementId).isNotEmpty();
  }

  @Test
  void elementIdNull() {
    final ResultSet result = database.command("opencypher", "RETURN elementId(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== endNode() Tests ====================

  @Test
  void endNodeBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (x:Developer)-[r]-() RETURN endNode(r).name AS result ORDER BY result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row1 = result.next();
    assertThat((String) row1.getProperty("result")).isIn("Bob", "Charlie");
  }

  @Test
  void endNodeProperties() {
    final ResultSet result = database.command("opencypher",
        "MATCH (x:Developer)-[r]->() RETURN endNode(r).age AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Integer age = ((Number) row.getProperty("result")).intValue();
    assertThat(age).isIn(25, 53);
  }

  @Test
  void endNodeNull() {
    final ResultSet result = database.command("opencypher", "RETURN endNode(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== head() Tests ====================

  @Test
  void headBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Eskil' RETURN head(a.likedColors) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("Pink");
  }

  @Test
  void headEmptyList() {
    final ResultSet result = database.command("opencypher", "RETURN head([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void headNull() {
    final ResultSet result = database.command("opencypher", "RETURN head(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void headNullElement() {
    final ResultSet result = database.command("opencypher", "RETURN head([null, 'second', 'third']) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== id() Tests ====================

  @Test
  void idNode() {
    final ResultSet result = database.command("opencypher", "MATCH (a) RETURN id(a) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    while (result.hasNext()) {
      final var row = result.next();
      Assertions.assertThat(row.getProperty("result") != null).isTrue();
    }
  }

  @Test
  void idRelationship() {
    final ResultSet result = database.command("opencypher", "MATCH ()-[r]->() RETURN id(r) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    while (result.hasNext()) {
      final var row = result.next();
      Assertions.assertThat(row.getProperty("result") != null).isTrue();
    }
  }

  @Test
  void idNull() {
    final ResultSet result = database.command("opencypher", "RETURN id(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== last() Tests ====================

  @Test
  void lastBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Eskil' RETURN last(a.likedColors) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("Black");
  }

  @Test
  void lastEmptyList() {
    final ResultSet result = database.command("opencypher", "RETURN last([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void lastNull() {
    final ResultSet result = database.command("opencypher", "RETURN last(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void lastNullElement() {
    final ResultSet result = database.command("opencypher", "RETURN last(['first', 'second', null]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== length() Tests ====================

  @Test
  void lengthPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (a)-->(b)-->(c) WHERE a.name = 'Alice' RETURN length(p) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(((Number) row.getProperty("result")).intValue()).isEqualTo(2);
    }
  }

  @Test
  void lengthSingleHop() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (a)-->(b) WHERE a.name = 'Alice' RETURN length(p) AS result LIMIT 1");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  @Test
  void lengthNull() {
    final ResultSet result = database.command("opencypher", "RETURN length(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== nullIf() Tests ====================

  @Test
  void nullIfEqual() {
    final ResultSet result = database.command("opencypher", "RETURN nullIf(4, 4) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void nullIfNotEqual() {
    final ResultSet result = database.command("opencypher", "RETURN nullIf('abc', 'def') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("abc");
  }

  @Test
  void nullIfStrings() {
    final ResultSet result = database.command("opencypher", "RETURN nullIf('same', 'same') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void nullIfWithCoalesce() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Alice' " +
            "RETURN a.name AS name, coalesce(nullIf(a.eyes, 'Brown'), 'Hazel') AS eyeColor");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Alice");
    assertThat((String) row.getProperty("eyeColor")).isEqualTo("Hazel");
  }

  // ==================== properties() Tests ====================

  @Test
  void propertiesNode() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Developer) WHERE p.name = 'Alice' RETURN properties(p) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final Map<String, Object> props = (Map<String, Object>) result.next().getProperty("result");
    assertThat(props).containsEntry("name", "Alice");
    assertThat(props).containsKey("age");
    assertThat(props).containsKey("eyes");
  }

  @Test
  void propertiesRelationship() {
    final ResultSet result = database.command("opencypher",
        "CREATE (a)-[r:TEST {prop1: 'value1', prop2: 42}]->(b) RETURN properties(r) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final Map<String, Object> props = (Map<String, Object>) result.next().getProperty("result");
    assertThat(props).containsEntry("prop1", "value1");
    assertThat(props).containsEntry("prop2", 42);
  }

  @Test
  void propertiesMap() {
    final ResultSet result = database.command("opencypher",
        "WITH {a: 1, b: 'test'} AS map RETURN properties(map) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final Map<String, Object> props = (Map<String, Object>) result.next().getProperty("result");
    assertThat(props).containsEntry("a", 1L);
    assertThat(props).containsEntry("b", "test");
  }

  @Test
  void propertiesNull() {
    final ResultSet result = database.command("opencypher", "RETURN properties(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== randomUUID() Tests ====================

  @Test
  void randomUUIDBasic() {
    final ResultSet result = database.command("opencypher", "RETURN randomUUID() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String uuid = (String) result.next().getProperty("result");
    assertThat(uuid).isNotNull();
    assertThat(uuid).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  }

  @Test
  void randomUUIDUnique() {
    final ResultSet result = database.command("opencypher",
        "RETURN randomUUID() AS uuid1, randomUUID() AS uuid2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final String uuid1 = (String) row.getProperty("uuid1");
    final String uuid2 = (String) row.getProperty("uuid2");
    assertThat(uuid1).isNotEqualTo(uuid2);
  }

  // ==================== size() Tests ====================

  @Test
  void sizeList() {
    final ResultSet result = database.command("opencypher", "RETURN size(['Alice', 'Bob']) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(2);
  }

  @Test
  void sizeString() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE size(a.name) > 6 RETURN size(a.name) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(7);
  }

  @Test
  void sizeEmptyList() {
    final ResultSet result = database.command("opencypher", "RETURN size([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(0);
  }

  @Test
  void sizeNull() {
    final ResultSet result = database.command("opencypher", "RETURN size(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== startNode() Tests ====================

  @Test
  void startNodeBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (x:Developer)-[r]-() RETURN startNode(r).name AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    while (result.hasNext()) {
      final var row = result.next();
      assertThat((String) row.getProperty("result")).isEqualTo("Alice");
    }
  }

  @Test
  void startNodeProperties() {
    final ResultSet result = database.command("opencypher",
        "MATCH (x:Developer)-[r]->() RETURN startNode(r).age AS result LIMIT 1");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(38);
  }

  @Test
  void startNodeNull() {
    final ResultSet result = database.command("opencypher", "RETURN startNode(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== timestamp() Tests ====================

  @Test
  void timestampBasic() {
    final ResultSet result = database.command("opencypher", "RETURN timestamp() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Long ts = ((Number) result.next().getProperty("result")).longValue();
    assertThat(ts).isGreaterThan(0L);
    assertThat(ts).isLessThan(System.currentTimeMillis() + 1000);
  }

  @Test
  void timestampConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN timestamp() AS ts1, timestamp() AS ts2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Long ts1 = ((Number) row.getProperty("ts1")).longValue();
    final Long ts2 = ((Number) row.getProperty("ts2")).longValue();
    assertThat(ts1).isEqualTo(ts2);
  }

  // ==================== toBoolean() Tests ====================

  @Test
  void toBooleanString() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBoolean('true') AS t, toBoolean('false') AS f, toBoolean('not a boolean') AS invalid");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("t")).isTrue();
    assertThat((Boolean) row.getProperty("f")).isFalse();
    Assertions.assertThat(row.getProperty("invalid") == null).isTrue();
  }

  @Test
  void toBooleanInteger() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBoolean(0) AS zero, toBoolean(1) AS one, toBoolean(42) AS other");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("zero")).isFalse();
    assertThat((Boolean) row.getProperty("one")).isTrue();
    assertThat((Boolean) row.getProperty("other")).isTrue();
  }

  @Test
  void toBooleanBoolean() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBoolean(true) AS t, toBoolean(false) AS f");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("t")).isTrue();
    assertThat((Boolean) row.getProperty("f")).isFalse();
  }

  @Test
  void toBooleanNull() {
    final ResultSet result = database.command("opencypher", "RETURN toBoolean(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toBooleanOrNull() Tests ====================

  @Test
  void toBooleanOrNullValid() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBooleanOrNull('true') AS t, toBooleanOrNull(0) AS zero");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("t")).isTrue();
    assertThat((Boolean) row.getProperty("zero")).isFalse();
  }

  @Test
  void toBooleanOrNullInvalid() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBooleanOrNull('not a boolean') AS str, toBooleanOrNull(1.5) AS float");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("str") == null).isTrue();
    Assertions.assertThat(row.getProperty("float") == null).isTrue();
  }

  @Test
  void toBooleanOrNullNull() {
    final ResultSet result = database.command("opencypher", "RETURN toBooleanOrNull(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toFloat() Tests ====================

  @Test
  void toFloatString() {
    final ResultSet result = database.command("opencypher",
        "RETURN toFloat('11.5') AS valid, toFloat('not a number') AS invalid");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("valid")).doubleValue()).isEqualTo(11.5);
    Assertions.assertThat(row.getProperty("invalid") == null).isTrue();
  }

  @Test
  void toFloatInteger() {
    final ResultSet result = database.command("opencypher", "RETURN toFloat(42) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isEqualTo(42.0);
  }

  @Test
  void toFloatFloat() {
    final ResultSet result = database.command("opencypher", "RETURN toFloat(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isEqualTo(3.14);
  }

  @Test
  void toFloatNull() {
    final ResultSet result = database.command("opencypher", "RETURN toFloat(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toFloatOrNull() Tests ====================

  @Test
  void toFloatOrNullValid() {
    final ResultSet result = database.command("opencypher",
        "RETURN toFloatOrNull('11.5') AS str, toFloatOrNull(42) AS int");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("str")).doubleValue()).isEqualTo(11.5);
    assertThat(((Number) row.getProperty("int")).doubleValue()).isEqualTo(42.0);
  }

  @Test
  void toFloatOrNullInvalid() {
    final ResultSet result = database.command("opencypher",
        "RETURN toFloatOrNull('not a number') AS str, toFloatOrNull(true) AS bool");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("str") == null).isTrue();
    Assertions.assertThat(row.getProperty("bool") == null).isTrue();
  }

  @Test
  void toFloatOrNullNull() {
    final ResultSet result = database.command("opencypher", "RETURN toFloatOrNull(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toInteger() Tests ====================

  @Test
  void toIntegerString() {
    final ResultSet result = database.command("opencypher",
        "RETURN toInteger('42') AS valid, toInteger('not a number') AS invalid");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("valid")).intValue()).isEqualTo(42);
    Assertions.assertThat(row.getProperty("invalid") == null).isTrue();
  }

  @Test
  void toIntegerBoolean() {
    final ResultSet result = database.command("opencypher",
        "RETURN toInteger(true) AS t, toInteger(false) AS f");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("t")).intValue()).isEqualTo(1);
    assertThat(((Number) row.getProperty("f")).intValue()).isEqualTo(0);
  }

  @Test
  void toIntegerFloat() {
    final ResultSet result = database.command("opencypher", "RETURN toInteger(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(3);
  }

  @Test
  void toIntegerInteger() {
    final ResultSet result = database.command("opencypher", "RETURN toInteger(42) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(42);
  }

  @Test
  void toIntegerNull() {
    final ResultSet result = database.command("opencypher", "RETURN toInteger(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toIntegerOrNull() Tests ====================

  @Test
  void toIntegerOrNullValid() {
    final ResultSet result = database.command("opencypher",
        "RETURN toIntegerOrNull('42') AS str, toIntegerOrNull(true) AS bool");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("str")).intValue()).isEqualTo(42);
    assertThat(((Number) row.getProperty("bool")).intValue()).isEqualTo(1);
  }

  @Test
  void toIntegerOrNullInvalid() {
    final ResultSet result = database.command("opencypher",
        "RETURN toIntegerOrNull('not a number') AS str, toIntegerOrNull(['A', 'B', 'C']) AS list");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("str") == null).isTrue();
    Assertions.assertThat(row.getProperty("list") == null).isTrue();
  }

  @Test
  void toIntegerOrNullNull() {
    final ResultSet result = database.command("opencypher", "RETURN toIntegerOrNull(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== type() Tests ====================

  @Test
  void typeBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (n)-[r]->() WHERE n.name = 'Alice' RETURN type(r) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    while (result.hasNext()) {
      final var row = result.next();
      assertThat((String) row.getProperty("result")).isEqualTo("KNOWS");
    }
  }

  @Test
  void typeMultipleTypes() {
    final ResultSet result = database.command("opencypher",
        "MATCH (n)-[r]->() WHERE n.name = 'Bob' RETURN DISTINCT type(r) AS result ORDER BY result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var types = new java.util.ArrayList<String>();
    while (result.hasNext()) {
      types.add((String) result.next().getProperty("result"));
    }
    assertThat(types).contains("KNOWS");
  }

  @Test
  void typeNull() {
    final ResultSet result = database.command("opencypher", "RETURN type(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== valueType() Tests ====================

  @Test
  void valueTypeBasic() {
    final ResultSet result = database.command("opencypher",
        "UNWIND ['abc', 1, 2.0, true] AS value RETURN valueType(value) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var types = new java.util.ArrayList<String>();
    while (result.hasNext()) {
      types.add((String) result.next().getProperty("result"));
    }
    assertThat(types).hasSize(4);
    assertThat(types).anyMatch(t -> t.contains("STRING"));
    assertThat(types).anyMatch(t -> t.contains("INTEGER"));
    assertThat(types).anyMatch(t -> t.contains("FLOAT"));
    assertThat(types).anyMatch(t -> t.contains("BOOLEAN"));
  }

  @Test
  void valueTypeList() {
    final ResultSet result = database.command("opencypher",
        "RETURN valueType([1, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String type = (String) result.next().getProperty("result");
    assertThat(type).contains("LIST");
  }

  @Test
  void valueTypeNull() {
    final ResultSet result = database.command("opencypher", "RETURN valueType(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String type = (String) result.next().getProperty("result");
    assertThat(type).containsIgnoringCase("NULL");
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void sizeAndCharLengthEquivalent() {
    final ResultSet result = database.command("opencypher",
        "WITH 'Hello World' AS str " +
            "RETURN size(str) AS sizeResult, char_length(str) AS charLenResult, character_length(str) AS charLenResult2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Integer sizeResult = ((Number) row.getProperty("sizeResult")).intValue();
    final Integer charLenResult = ((Number) row.getProperty("charLenResult")).intValue();
    final Integer charLenResult2 = ((Number) row.getProperty("charLenResult2")).intValue();
    assertThat(sizeResult).isEqualTo(charLenResult);
    assertThat(sizeResult).isEqualTo(charLenResult2);
  }

  @Test
  void headAndLastOnSameList() {
    final ResultSet result = database.command("opencypher",
        "WITH ['first', 'middle', 'last'] AS list " +
            "RETURN head(list) AS first, last(list) AS last, size(list) AS count");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((String) row.getProperty("first")).isEqualTo("first");
    assertThat((String) row.getProperty("last")).isEqualTo("last");
    assertThat(((Number) row.getProperty("count")).intValue()).isEqualTo(3);
  }

  @Test
  void startAndEndNodeSameRelationship() {
    final ResultSet result = database.command("opencypher",
        "MATCH (x:Developer)-[r]->(y) " +
            "RETURN startNode(r).name AS start, endNode(r).name AS end, type(r) AS relType LIMIT 1");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((String) row.getProperty("start")).isEqualTo("Alice");
    assertThat((String) row.getProperty("end")).isIn("Bob", "Charlie");
    assertThat((String) row.getProperty("relType")).isEqualTo("KNOWS");
  }

  @Test
  void typeConversionChain() {
    final ResultSet result = database.command("opencypher",
        "WITH '42' AS str " +
            "RETURN str AS original, " +
            "       toInteger(str) AS asInt, " +
            "       toFloat(toInteger(str)) AS asFloat, " +
            "       toBoolean(toInteger(str)) AS asBool");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((String) row.getProperty("original")).isEqualTo("42");
    assertThat(((Number) row.getProperty("asInt")).intValue()).isEqualTo(42);
    assertThat(((Number) row.getProperty("asFloat")).doubleValue()).isEqualTo(42.0);
    assertThat((Boolean) row.getProperty("asBool")).isTrue();
  }

  @Test
  void coalesceWithNullIf() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) " +
            "RETURN a.name AS name, " +
            "       coalesce(nullIf(a.eyes, 'Brown'), 'Hazel') AS eyeColor " +
            "ORDER BY name");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    int brownToHazelCount = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = (String) row.getProperty("name");
      final String eyeColor = (String) row.getProperty("eyeColor");
      if ("Alice".equals(name) || "Daniel".equals(name)) {
        assertThat(eyeColor).isEqualTo("Hazel");
        brownToHazelCount++;
      } else {
        assertThat(eyeColor).isNotNull();
      }
    }
    assertThat(brownToHazelCount).isEqualTo(2);
  }
}
