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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.ClientException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  void testConnection() {
    try (Driver driver = getDriver()) {
      driver.verifyConnectivity();
    }
  }

  @Test
  void testSimpleQuery() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 1 AS value");
        assertThat(result.hasNext()).isTrue();
        final org.neo4j.driver.Record record = result.next();
        assertThat(record.get("value").asLong()).isEqualTo(1L);
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void testStringReturn() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 'hello' AS greeting");
        assertThat(result.hasNext()).isTrue();
        final org.neo4j.driver.Record record = result.next();
        assertThat(record.get("greeting").asString()).isEqualTo("hello");
      }
    }
  }

  @Test
  void testParameterizedQuery() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run(
            "RETURN $name AS name, $age AS age",
            Map.of("name", "Alice", "age", 30)
        );
        assertThat(result.hasNext()).isTrue();
        final org.neo4j.driver.Record record = result.next();
        assertThat(record.get("name").asString()).isEqualTo("Alice");
        assertThat(record.get("age").asLong()).isEqualTo(30L);
      }
    }
  }

  @Test
  void testCreateAndMatchVertex() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create a vertex
        session.run("CREATE VERTEX TYPE Person IF NOT EXISTS");
        session.run("CREATE (n:Person {name: 'Bob', age: 25})");

        // Match and return
        final Result result = session.run("MATCH (n:Person {name: 'Bob'}) RETURN n");
        assertThat(result.hasNext()).isTrue();
        final org.neo4j.driver.Record record = result.next();
        final var node = record.get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Bob");
        assertThat(node.get("age").asLong()).isEqualTo(25L);
        assertThat(node.labels()).contains("Person");
      }
    }
  }

  @Test
  void testCreateAndMatchEdge() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create types and vertices
        session.run("CREATE VERTEX TYPE Person IF NOT EXISTS");
        session.run("CREATE EDGE TYPE KNOWS IF NOT EXISTS");
        session.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})");

        // Create edge
        session.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)");

        // Query edge
        final Result result = session.run(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN r"
        );
        assertThat(result.hasNext()).isTrue();
        final org.neo4j.driver.Record record = result.next();
        final var rel = record.get("r").asRelationship();
        assertThat(rel.type()).isEqualTo("KNOWS");
        assertThat(rel.get("since").asLong()).isEqualTo(2020L);
      }
    }
  }

  @Test
  void testTransaction() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create type first
        session.run("CREATE VERTEX TYPE Counter IF NOT EXISTS");

        // Begin transaction
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:Counter {value: 1})");
          tx.commit();
        }

        // Verify data was committed
        final Result result = session.run("MATCH (n:Counter) RETURN n.value AS value");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").asLong()).isEqualTo(1L);
      }
    }
  }

  @Test
  void testTransactionRollback() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create type first
        session.run("CREATE VERTEX TYPE TempData IF NOT EXISTS");

        // Count before
        final long countBefore = session.run("MATCH (n:TempData) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();

        // Begin transaction and rollback
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:TempData {value: 'test'})");
          tx.rollback();
        }

        // Count should be the same
        final long countAfter = session.run("MATCH (n:TempData) RETURN count(n) AS cnt")
            .next().get("cnt").asLong();
        assertThat(countAfter).isEqualTo(countBefore);
      }
    }
  }

  @Test
  void testMultipleStatements() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        session.run("CREATE VERTEX TYPE Item IF NOT EXISTS");

        // Run multiple statements
        for (int i = 0; i < 5; i++) {
          session.run("CREATE (n:Item {index: $i})", Map.of("i", i));
        }

        // Count all
        final Result result = session.run("MATCH (n:Item) RETURN count(n) AS count");
        assertThat(result.next().get("count").asLong()).isGreaterThanOrEqualTo(5L);
      }
    }
  }

  @Test
  void testNullHandling() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN null AS value");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").isNull()).isTrue();
      }
    }
  }

  @Test
  void testListReturn() {
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
  void testMapReturn() {
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
  void testSyntaxError() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        assertThatThrownBy(() -> session.run("INVALID CYPHER QUERY").consume())
            .isInstanceOf(ClientException.class);
      }
    }
  }

  @Test
  void testLargeResultSet() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        session.run("CREATE VERTEX TYPE LargeData IF NOT EXISTS");

        // Create many records
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
  void testBooleanValues() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN true AS t, false AS f");
        assertThat(result.hasNext()).isTrue();
        org.neo4j.driver.Record record = result.next();
        assertThat(record.get("t").asBoolean()).isTrue();
        assertThat(record.get("f").asBoolean()).isFalse();
      }
    }
  }

  @Test
  void testFloatValues() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        Result result = session.run("RETURN 3.14159 AS pi");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("pi").asDouble()).isCloseTo(3.14159, org.assertj.core.data.Offset.offset(0.00001));
      }
    }
  }
}
