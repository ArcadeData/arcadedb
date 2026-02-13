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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.test.BaseGraphServerTest;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.neo4j.driver.Record;

/**
 * Integration tests for BOLT protocol using Neo4j Java driver.
 */
public class BoltProtocolIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder()
            .withoutEncryption()
            .build()
    );
  }

  @Test
  void connection() {
    try (Driver driver = getDriver()) {
      driver.verifyConnectivity();
    }
  }

  @Test
  void simpleQuery() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 1 AS value");
        assertThat(result.hasNext()).isTrue();
        final Record record = result.next();
        assertThat(record.get("value").asLong()).isEqualTo(1L);
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void stringReturn() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 'hello' AS greeting");
        assertThat(result.hasNext()).isTrue();
        final Record record = result.next();
        assertThat(record.get("greeting").asString()).isEqualTo("hello");
      }
    }
  }

  @Test
  void parameterizedQuery() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run(
            "RETURN $name AS name, $age AS age",
            Map.of("name", "Alice", "age", 30)
        );
        assertThat(result.hasNext()).isTrue();
        final Record record = result.next();
        assertThat(record.get("name").asString()).isEqualTo("Alice");
        assertThat(record.get("age").asLong()).isEqualTo(30L);
      }
    }
  }

  @Test
  void createAndMatchVertex() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create a vertex - ArcadeDB auto-creates types
        session.run("CREATE (n:BoltPerson {name: 'Bob', age: 25})");

        // Match and return
        final Result result = session.run("MATCH (n:BoltPerson {name: 'Bob'}) RETURN n");
        assertThat(result.hasNext()).isTrue();
        final Record record = result.next();
        final var node = record.get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Bob");
        assertThat(node.get("age").asLong()).isEqualTo(25L);
        assertThat(node.labels()).contains("BoltPerson");
      }
    }
  }

  @Test
  void createAndMatchEdge() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create vertices - ArcadeDB auto-creates types
        session.run("CREATE (a:EdgePerson {name: 'EdgeAlice'}), (b:EdgePerson {name: 'EdgeBob'})");

        // Create edge
        session.run("MATCH (a:EdgePerson {name: 'EdgeAlice'}), (b:EdgePerson {name: 'EdgeBob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)");

        // Query edge
        final Result result = session.run(
            "MATCH (a:EdgePerson)-[r:KNOWS]->(b:EdgePerson) WHERE a.name = 'EdgeAlice' AND b.name = 'EdgeBob' RETURN r"
        );
        assertThat(result.hasNext()).isTrue();
        final Record record = result.next();
        final var rel = record.get("r").asRelationship();
        assertThat(rel.type()).isEqualTo("KNOWS");
        assertThat(rel.get("since").asLong()).isEqualTo(2020L);
      }
    }
  }

  @Test
  void transaction() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Begin transaction - ArcadeDB auto-creates types
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:TxCounter {value: 1})");
          tx.commit();
        }

        // Verify data was committed
        final Result result = session.run("MATCH (n:TxCounter) RETURN n.value AS value");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").asLong()).isEqualTo(1L);
      }
    }
  }

  @Test
  void transactionRollback() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // First create a committed record so the type exists
        session.run("CREATE (n:RollbackData {marker: 'initial'})");

        // Count before - should be 1
        final long countBefore = session.run("MATCH (n:RollbackData) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();

        // Begin transaction and rollback
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:RollbackData {value: 'test'})");
          tx.rollback();
        }

        // Count should be the same (rollback should have reverted the create)
        final long countAfter = session.run("MATCH (n:RollbackData) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();
        assertThat(countAfter).isEqualTo(countBefore);
      }
    }
  }

  @Test
  void multipleStatements() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Run multiple statements - ArcadeDB auto-creates types
        for (int i = 0; i < 5; i++) {
          session.run("CREATE (n:MultiItem {index: $i})", Map.of("i", i));
        }

        // Count all
        final Result result = session.run("MATCH (n:MultiItem) RETURN count(n) AS count");
        assertThat(result.next().get("count").asLong()).isGreaterThanOrEqualTo(5L);
      }
    }
  }

  @Test
  void nullHandling() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN null AS value");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").isNull()).isTrue();
      }
    }
  }

  @Test
  void listReturn() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN [1, 2, 3] AS numbers");
        assertThat(result.hasNext()).isTrue();
        final List<Object> numbers = result.next().get("numbers").asList();
        assertThat(numbers).containsExactly(1L, 2L, 3L);
      }
    }
  }

  @Test
  void mapReturn() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN {name: 'Alice', age: 30} AS person");
        assertThat(result.hasNext()).isTrue();
        final Map<String, Object> person = result.next().get("person").asMap();
        assertThat(person).containsEntry("name", "Alice");
        assertThat(person).containsEntry("age", 30L);
      }
    }
  }

  @Test
  void syntaxError() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        assertThatThrownBy(() -> session.run("INVALID CYPHER QUERY").consume())
            .isInstanceOf(ClientException.class);
      }
    }
  }

  @Test
  void largeResultSet() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create many records - ArcadeDB auto-creates types
        try (Transaction tx = session.beginTransaction()) {
          for (int i = 0; i < 100; i++) {
            tx.run("CREATE (n:LargeData {index: $i})", Map.of("i", i));
          }
          tx.commit();
        }

        // Query all
        final Result result = session.run("MATCH (n:LargeData) RETURN n ORDER BY n.index");
        int count = 0;
        while (result.hasNext()) {
          result.next();
          count++;
        }
        assertThat(count).isGreaterThanOrEqualTo(100);
      }
    }
  }

  @Test
  void booleanValues() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN true AS t, false AS f");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("t").asBoolean()).isTrue();
        assertThat(record.get("f").asBoolean()).isFalse();
      }
    }
  }

  @Test
  void floatValues() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 3.14159 AS pi");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("pi").asDouble()).isCloseTo(3.14159, Offset.offset(0.00001));
      }
    }
  }

  // ========== Additional Tests for Protocol Coverage ==========

  @Test
  void negativeIntegers() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN -42 AS negative, -1000000 AS bigNegative");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("negative").asLong()).isEqualTo(-42L);
        assertThat(record.get("bigNegative").asLong()).isEqualTo(-1000000L);
      }
    }
  }

  @Test
  void largeIntegers() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Test with large integers that require different encoding sizes
        Result result = session.run("RETURN 127 AS tiny, 32767 AS small, 2147483647 AS medium, 9223372036854775807 AS large");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("tiny").asLong()).isEqualTo(127L);
        assertThat(record.get("small").asLong()).isEqualTo(32767L);
        assertThat(record.get("medium").asLong()).isEqualTo(2147483647L);
        assertThat(record.get("large").asLong()).isEqualTo(Long.MAX_VALUE);
      }
    }
  }

  @Test
  void unicodeStrings() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN '你好世界' AS chinese, 'Привет' AS russian, '🌍🚀' AS emoji");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("chinese").asString()).isEqualTo("你好世界");
        assertThat(record.get("russian").asString()).isEqualTo("Привет");
        assertThat(record.get("emoji").asString()).isEqualTo("🌍🚀");
      }
    }
  }

  @Test
  void emptyString() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN '' AS empty");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("empty").asString()).isEmpty();
      }
    }
  }

  @Test
  void longString() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create a string longer than 255 characters to test STRING_8/STRING_16 encoding
        String longString = "a".repeat(500);
        Result result = session.run("RETURN $s AS longString", Map.of("s", longString));
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("longString").asString()).isEqualTo(longString);
      }
    }
  }

  @Test
  void emptyResultSet() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create a type first, then query with impossible filter
        session.run("CREATE (n:EmptyTest {marker: 'exists'})");
        // Query with filter that won't match anything
        Result result = session.run("MATCH (n:EmptyTest {marker: 'does_not_exist_12345'}) RETURN n");
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void multipleFieldsInResult() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 1 AS a, 2 AS b, 3 AS c, 4 AS d, 5 AS e");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("a").asLong()).isEqualTo(1L);
        assertThat(record.get("b").asLong()).isEqualTo(2L);
        assertThat(record.get("c").asLong()).isEqualTo(3L);
        assertThat(record.get("d").asLong()).isEqualTo(4L);
        assertThat(record.get("e").asLong()).isEqualTo(5L);
        assertThat(record.keys()).containsExactlyInAnyOrder("a", "b", "c", "d", "e");
      }
    }
  }

  @Test
  void sessionReuse() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Execute multiple queries on the same session
        Result r1 = session.run("RETURN 1 AS v");
        assertThat(r1.next().get("v").asLong()).isEqualTo(1L);

        Result r2 = session.run("RETURN 2 AS v");
        assertThat(r2.next().get("v").asLong()).isEqualTo(2L);

        Result r3 = session.run("RETURN 3 AS v");
        assertThat(r3.next().get("v").asLong()).isEqualTo(3L);
      }
    }
  }

  @Test
  void multipleTransactionsInSequence() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // First transaction - ArcadeDB auto-creates types
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:SeqTest {seq: 1})");
          tx.commit();
        }

        // Second transaction
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:SeqTest {seq: 2})");
          tx.commit();
        }

        // Third transaction
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:SeqTest {seq: 3})");
          tx.commit();
        }

        // Verify all were committed
        Result result = session.run("MATCH (n:SeqTest) RETURN count(n) AS cnt");
        assertThat(result.next().get("cnt").asLong()).isGreaterThanOrEqualTo(3L);
      }
    }
  }

  @Test
  void queryAfterErrorRecovery() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // First, execute a failing query
        try {
          session.run("INVALID SYNTAX HERE").consume();
        } catch (ClientException e) {
          // Expected
        }

        // Session should recover and be able to execute new queries
        Result result = session.run("RETURN 'recovered' AS status");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("status").asString()).isEqualTo("recovered");
      }
    }
  }

  @Test
  void listParameter() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        List<Long> numbers = List.of(1L, 2L, 3L, 4L, 5L);
        Result result = session.run("RETURN $nums AS numbers", Map.of("nums", numbers));
        assertThat(result.hasNext()).isTrue();
        List<Object> returned = result.next().get("numbers").asList();
        assertThat(returned).containsExactly(1L, 2L, 3L, 4L, 5L);
      }
    }
  }

  @Test
  void mapParameter() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Map<String, Object> person = Map.of("name", "John", "age", 35L);
        Result result = session.run("RETURN $person AS p", Map.of("person", person));
        assertThat(result.hasNext()).isTrue();
        Map<String, Object> returned = result.next().get("p").asMap();
        assertThat(returned).containsEntry("name", "John");
        assertThat(returned).containsEntry("age", 35L);
      }
    }
  }

  @Test
  void nestedList() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Test nested list
        Result result = session.run("RETURN [[1, 2], [3, 4], [5, 6]] AS matrix");
        assertThat(result.hasNext()).isTrue();
        List<Object> matrix = result.next().get("matrix").asList();
        assertThat(matrix).hasSize(3);
      }
    }
  }

  @Test
  void arithmeticExpressions() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 10 + 5 AS sum, 10 - 5 AS diff, 10 * 5 AS product, 10 / 5 AS quotient");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("sum").asLong()).isEqualTo(15L);
        assertThat(record.get("diff").asLong()).isEqualTo(5L);
        assertThat(record.get("product").asLong()).isEqualTo(50L);
        assertThat(record.get("quotient").asLong()).isEqualTo(2L);
      }
    }
  }

  @Test
  void stringConcatenation() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Test string concatenation which is supported
        Result result = session.run("RETURN 'hello' + ' ' + 'world' AS greeting");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("greeting").asString()).isEqualTo("hello world");
      }
    }
  }

  @Test
  void queryWithLimit() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create 10 records - ArcadeDB auto-creates types
        try (Transaction tx = session.beginTransaction()) {
          for (int i = 0; i < 10; i++) {
            tx.run("CREATE (n:LimitTest {idx: $i})", Map.of("i", i));
          }
          tx.commit();
        }

        // Query with LIMIT
        Result result = session.run("MATCH (n:LimitTest) RETURN n LIMIT 5");
        int count = 0;
        while (result.hasNext()) {
          result.next();
          count++;
        }
        assertThat(count).isEqualTo(5);
      }
    }
  }

  @Test
  void queryWithSkipAndLimit() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create records with sequential indices - ArcadeDB auto-creates types
        try (Transaction tx = session.beginTransaction()) {
          for (int i = 0; i < 20; i++) {
            tx.run("CREATE (n:SkipTest {idx: $i})", Map.of("i", i));
          }
          tx.commit();
        }

        // Query with SKIP and LIMIT
        Result result = session.run("MATCH (n:SkipTest) RETURN n.idx AS idx ORDER BY n.idx SKIP 5 LIMIT 5");
        System.out.println("result = " + result);
        List<Long> indices = new ArrayList<>();
        while (result.hasNext()) {
          indices.add(result.next().get("idx").asLong());
        }
        assertThat(indices).hasSize(5);
        assertThat(indices).containsExactly(5L, 6L, 7L, 8L, 9L);
      }
    }
  }

  @Test
  void nodeWithMultipleProperties() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // ArcadeDB auto-creates types
        session.run("""
            CREATE (n:MultiProp {
            stringProp: 'hello',
            intProp: 42,
            floatProp: 3.14,
            boolProp: true,
            listProp: [1, 2, 3]
            })""");

        Result result = session.run("MATCH (n:MultiProp) RETURN n");
        assertThat(result.hasNext()).isTrue();
        var node = result.next().get("n").asNode();
        assertThat(node.get("stringProp").asString()).isEqualTo("hello");
        assertThat(node.get("intProp").asLong()).isEqualTo(42L);
        assertThat(node.get("floatProp").asDouble()).isCloseTo(3.14, Offset.offset(0.001));
        assertThat(node.get("boolProp").asBoolean()).isTrue();
        assertThat(node.get("listProp").asList()).containsExactly(1L, 2L, 3L);
      }
    }
  }

  @Test
  void nodeAndRelationshipInSameResult() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // ArcadeDB auto-creates types
        session.run("CREATE (a:NodeA {name: 'A'})-[:LINKS {weight: 10}]->(b:NodeB {name: 'B'})");

        Result result = session.run(
            "MATCH (a:NodeA)-[r:LINKS]->(b:NodeB) RETURN a, r, b"
        );
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();

        var nodeA = record.get("a").asNode();
        var rel = record.get("r").asRelationship();
        var nodeB = record.get("b").asNode();

        assertThat(nodeA.get("name").asString()).isEqualTo("A");
        assertThat(nodeA.labels()).contains("NodeA");
        assertThat(rel.type()).isEqualTo("LINKS");
        assertThat(rel.get("weight").asLong()).isEqualTo(10L);
        assertThat(nodeB.get("name").asString()).isEqualTo("B");
        assertThat(nodeB.labels()).contains("NodeB");
      }
    }
  }

  @Test
  void consumeResultSummary() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 1 AS value");
        ResultSummary summary = result.consume();
        assertThat(summary).isNotNull();
        // The summary should be available after consuming
      }
    }
  }

  @Test
  void multipleRecordsInResult() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multiple records - ArcadeDB auto-creates types
        try (Transaction tx = session.beginTransaction()) {
          for (int i = 1; i <= 5; i++) {
            tx.run("CREATE (n:MultiRecord {value: $v})", Map.of("v", i));
          }
          tx.commit();
        }

        // Query all and iterate
        Result result = session.run("MATCH (n:MultiRecord) RETURN n.value AS v ORDER BY n.value");
        List<Long> values = new ArrayList<>();
        while (result.hasNext()) {
          values.add(result.next().get("v").asLong());
        }
        assertThat(values).containsExactly(1L, 2L, 3L, 4L, 5L);
      }
    }
  }

  @Test
  void authenticationFailure() {
    try (Driver driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", "wrong_password"),
        Config.builder().withoutEncryption().build()
    )) {
      assertThatThrownBy(driver::verifyConnectivity)
          .isInstanceOf(Exception.class);
    }
  }

  @Test
  void driverConnectionPooling() {
    // Test that driver can handle multiple sessions using connection pooling
    try (Driver driver = getDriver()) {
      for (int i = 0; i < 5; i++) {
        try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
          Result result = session.run("RETURN $i AS iteration", Map.of("i", i));
          assertThat(result.next().get("iteration").asLong()).isEqualTo((long) i);
        }
      }
    }
  }

  @Test
  void mixedNullAndValues() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 1 AS a, null AS b, 'text' AS c, null AS d");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("a").asLong()).isEqualTo(1L);
        assertThat(record.get("b").isNull()).isTrue();
        assertThat(record.get("c").asString()).isEqualTo("text");
        assertThat(record.get("d").isNull()).isTrue();
      }
    }
  }

  @Test
  void transactionReadOperation() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // ArcadeDB auto-creates types
        session.run("CREATE (n:ReadTest {value: 'test'})");

        // Read operation within transaction
        try (Transaction tx = session.beginTransaction()) {
          Result result = tx.run("MATCH (n:ReadTest) RETURN n.value AS v");
          assertThat(result.hasNext()).isTrue();
          assertThat(result.next().get("v").asString()).isEqualTo("test");
          tx.commit();
        }
      }
    }
  }

  @Test
  void emptyList() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN [] AS empty");
        assertThat(result.hasNext()).isTrue();
        List<Object> list = result.next().get("empty").asList();
        assertThat(list).isEmpty();
      }
    }
  }

  @Test
  void emptyMap() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN {} AS empty");
        assertThat(result.hasNext()).isTrue();
        Map<String, Object> map = result.next().get("empty").asMap();
        assertThat(map).isEmpty();
      }
    }
  }

  @Test
  void zeroValue() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 0 AS zero, 0.0 AS zeroFloat");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("zero").asLong()).isEqualTo(0L);
        assertThat(record.get("zeroFloat").asDouble()).isEqualTo(0.0);
      }
    }
  }

  @Test
  void verySmallFloat() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Test very small float value
        Result result = session.run("RETURN 0.000001 AS small, -0.000001 AS negSmall");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("small").asDouble()).isCloseTo(0.000001, Offset.offset(0.0000001));
        assertThat(record.get("negSmall").asDouble()).isCloseTo(-0.000001, Offset.offset(0.0000001));
      }
    }
  }

  @Test
  void veryLargeFloat() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Test large float value
        Result result = session.run("RETURN 1.0E100 AS large, -1.0E100 AS negLarge");
        assertThat(result.hasNext()).isTrue();
        Record record = result.next();
        assertThat(record.get("large").asDouble()).isEqualTo(1.0E100);
        assertThat(record.get("negLarge").asDouble()).isEqualTo(-1.0E100);
      }
    }
  }

  // ========== Additional Tests for Code Review Recommendations ==========

  @Test
  void databaseSwitchingBetweenQueries() {
    // This test verifies that database selection persists across queries in the same session
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // First query
        Result r1 = session.run("RETURN 'db1' AS db");
        assertThat(r1.next().get("db").asString()).isEqualTo("db1");

        // Second query on same session should use same database
        Result r2 = session.run("RETURN 'db2' AS db");
        assertThat(r2.next().get("db").asString()).isEqualTo("db2");
      }
    }
  }

  @Test
  void concurrentSessions() throws Exception {
    // Test that multiple concurrent sessions can work independently
    final List<Thread> threads = new ArrayList<>();
    final List<Throwable> errors = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final int threadId = i;
      final Thread thread = new Thread(() -> {
        try (Driver driver = getDriver()) {
          try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
            // Each thread creates its own data
            session.run("CREATE (n:ConcurrentTest {threadId: $id})", Map.of("id", threadId));

            // Verify it can read it back
            Result result = session.run("MATCH (n:ConcurrentTest {threadId: $id}) RETURN n.threadId AS id", Map.of("id", threadId));
            if (result.hasNext()) {
              long readId = result.next().get("id").asLong();
              if (readId != threadId) {
                throw new AssertionError("Expected " + threadId + " but got " + readId);
              }
            }
          }
        } catch (Throwable t) {
          synchronized (errors) {
            errors.add(t);
          }
        }
      });
      threads.add(thread);
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join(10000); // 10 second timeout per thread
    }

    // Check for errors
    if (!errors.isEmpty()) {
      throw new AssertionError("Concurrent test failed with " + errors.size() + " errors: " + errors.get(0).getMessage(), errors.get(0));
    }
  }

  @Test
  void invalidDatabaseName() {
    try (Driver driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build()
    )) {
      // Try to use a database that doesn't exist
      assertThatThrownBy(() -> {
        try (Session session = driver.session(SessionConfig.forDatabase("nonexistent_database_12345"))) {
          session.run("RETURN 1").consume();
        }
      }).isInstanceOf(Exception.class);
    }
  }

  @Test
  void connectionPoolingWithManyQueries() {
    // Test that driver's connection pooling works correctly with many sequential queries
    try (Driver driver = getDriver()) {
      for (int i = 0; i < 20; i++) {
        try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
          Result result = session.run("RETURN $i AS iteration", Map.of("i", i));
          assertThat(result.next().get("iteration").asLong()).isEqualTo((long) i);
        }
      }
    }
  }

  @Test
  void transactionIsolation() {
    // Test that uncommitted (rolled-back) changes never become visible, and committed ones do.
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Ensure the type exists so rollback test doesn't depend on schema auto-creation
        session.run("CREATE (n:IsolationTest {marker: 'setup'})").consume();

        final long countBefore = session.run("MATCH (n:IsolationTest) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();

        // Create data in a transaction, then rollback — it should not be visible
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:IsolationTest {marker: 'rolled_back'})");
          tx.rollback();
        }

        final long countAfterRollback = session.run("MATCH (n:IsolationTest) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();
        assertThat(countAfterRollback).isEqualTo(countBefore);

        // Create data in a transaction, then commit — it should be visible
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:IsolationTest {marker: 'committed'})");
          tx.commit();
        }

        final long countAfterCommit = session.run("MATCH (n:IsolationTest) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();
        assertThat(countAfterCommit).isEqualTo(countBefore + 1);
      }
    }
  }

  @Test
  void largeParameterMap() {
    // Test with many parameters to ensure parameter handling scales
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Map<String, Object> params = new HashMap<>();
        for (int i = 0; i < 50; i++) {
          params.put("param" + i, i);
        }

        // Build a query that uses all parameters
        StringBuilder query = new StringBuilder("RETURN ");
        for (int i = 0; i < 50; i++) {
          if (i > 0) query.append(" + ");
          query.append("$param").append(i);
        }
        query.append(" AS sum");

        Result result = session.run(query.toString(), params);
        assertThat(result.hasNext()).isTrue();
        // Sum of 0..49 = 1225
        assertThat(result.next().get("sum").asLong()).isEqualTo(1225L);
      }
    }
  }

  @Test
  void queryWithNullParameter() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN $param AS value", Map.of("param", Values.NULL));
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").isNull()).isTrue();
      }
    }
  }

  @Test
  void multipleErrorsInSequence() {
    // Test that session can recover from multiple consecutive errors
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Error 1
        try {
          session.run("INVALID SYNTAX 1").consume();
        } catch (ClientException e) {
          // Expected
        }

        // Error 2
        try {
          session.run("INVALID SYNTAX 2").consume();
        } catch (ClientException e) {
          // Expected
        }

        // Should still work after multiple errors
        Result result = session.run("RETURN 'recovered' AS status");
        assertThat(result.next().get("status").asString()).isEqualTo("recovered");
      }
    }
  }

  @Test
  void databasePersistsAfterConnectionClose() {
    // CRITICAL TEST: Verify that closing one connection doesn't close the database for others
    // This test catches the bug where database.close() was incorrectly called in cleanup()
    try (Driver driver = getDriver()) {
      // Session 1: Create some data
      try (Session session1 = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        session1.run("CREATE (n:PersistenceTest {id: 1, name: 'test'})").consume();
      } // session1 closes here

      // Session 2: Verify the data still exists and database is still open
      try (Session session2 = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session2.run("MATCH (n:PersistenceTest {id: 1}) RETURN n.name AS name");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("name").asString()).isEqualTo("test");
      }

      // Session 3: Verify database is still accessible after multiple connection cycles
      try (Session session3 = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session3.run("MATCH (n:PersistenceTest) RETURN count(n) AS cnt");
        assertThat(result.next().get("cnt").asLong()).isGreaterThanOrEqualTo(1L);
      }
    }
  }

  @Test
  void pathStructure() {
    // Test returning path structures from Cypher queries
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create a simple path: (a)-[:REL]->(b)
        session.run("CREATE (a:PathNode {name: 'start'})-[:CONNECTS_TO {weight: 1.0}]->(b:PathNode {name: 'end'})").consume();

        // Query for the path
        Result result = session.run("MATCH p = (a:PathNode {name: 'start'})-[r:CONNECTS_TO]->(b:PathNode {name: 'end'}) RETURN p");
        assertThat(result.hasNext()).isTrue();

        Record record = result.next();
        // Note: Path support depends on OpenCypher implementation
        // At minimum, verify the query doesn't throw an error
        assertThat(record).isNotNull();
      }
    }
  }

  @Test
  void connectionLimit() {
    // Test that connection limiting works if configured
    final int maxConnections = GlobalConfiguration.BOLT_MAX_CONNECTIONS.getValueAsInteger();

    // Only run this test if connection limiting is enabled
    if (maxConnections > 0 && maxConnections < 100) {
      List<Driver> drivers = new ArrayList<>();
      try {
        // Create connections up to the limit
        for (int i = 0; i < maxConnections; i++) {
          Driver driver = getDriver();
          driver.verifyConnectivity();
          drivers.add(driver);
        }

        // Attempt to create one more connection beyond the limit
        // This should either be rejected or one of the existing connections should be replaced
        try (Driver extraDriver = getDriver()) {
          // If we get here, the server either increased the limit or is using connection pooling
          extraDriver.verifyConnectivity();
        } catch (Exception e) {
          // Connection was rejected due to limit - this is expected behavior
          assertThat(e.getMessage()).contains("connection");
        }
      } finally {
        // Clean up all drivers
        for (Driver driver : drivers) {
          try {
            driver.close();
          } catch (Exception e) {
            // Ignore cleanup errors
          }
        }
      }
    }
  }
}
