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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4355: verify that records written via SQL are visible to subsequent
 * Cypher queries (and vice versa) within the same connection and transaction. This is the cross
 * language consistency contract: both engines share the same database/transaction manager, so
 * writes by one engine must be immediately visible to the other engine.
 * <p>
 * The original report came from a user migrating from Neo4j and concluded that "Engines don't
 * always share consistent views". These tests pin the contract so the multi-model promise is
 * verified by CI.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/4355">Issue #4355</a>
 */
class CrossLanguageVisibilityTest {

  private static final String DB_PATH = "./target/databases/test-cross-language-visibility";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void sqlInsertedVertexVisibleToCypher_separateTransactions() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");
    });

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name AS name");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void sqlInsertedVertexVisibleToCypher_sameTransaction() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");

      final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name AS name");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("name")).isEqualTo("Alice");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void cypherCreatedVertexVisibleToSql_separateTransactions() {
    database.transaction(() ->
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})"));

    final ResultSet result = database.query("sql", "SELECT name FROM Person");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void cypherCreatedVertexVisibleToSql_sameTransaction() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");

      final ResultSet result = database.query("sql", "SELECT name FROM Person");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("name")).isEqualTo("Bob");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void sqlInsertedEdgeVisibleToCypher() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE EDGE TYPE KNOWS");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      database.command("sql", "INSERT INTO Person SET name = 'Bob'");
      database.command("sql",
          "CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE name = 'Alice') TO (SELECT FROM Person WHERE name = 'Bob')");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("src")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("dst")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void cypherCreatedEdgeVisibleToSql() {
    database.transaction(() ->
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"));

    final ResultSet result = database.query("sql",
        "SELECT FROM KNOWS");
    assertThat(result.hasNext()).isTrue();
    final Edge edge = (Edge) result.next().toElement();
    assertThat(edge.getIn().asVertex().<String>get("name")).isEqualTo("Bob");
    assertThat(edge.getOut().asVertex().<String>get("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void sqlInsertedEdgeVisibleToCypher_sameTransaction() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE EDGE TYPE KNOWS");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      database.command("sql", "INSERT INTO Person SET name = 'Bob'");
      database.command("sql",
          "CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE name = 'Alice') TO (SELECT FROM Person WHERE name = 'Bob')");

      final ResultSet result = database.query("opencypher",
          "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst");
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<String>getProperty("src")).isEqualTo("Alice");
      assertThat(row.<String>getProperty("dst")).isEqualTo("Bob");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void sqlInsertedEdgeOnHierarchyVisibleToCypher() {
    // Issue test matrix step 3: edge subtype extends parent type. Cypher should be able to
    // resolve the parent type when matching by parent label.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE EDGE TYPE E_PARENT");
      database.command("sql", "CREATE EDGE TYPE E IF NOT EXISTS EXTENDS E_PARENT");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      database.command("sql", "INSERT INTO Person SET name = 'Bob'");
      database.command("sql",
          "CREATE EDGE E FROM (SELECT FROM Person WHERE name = 'Alice') TO (SELECT FROM Person WHERE name = 'Bob')");
    });

    // Match by the concrete subtype label
    final ResultSet matchByE = database.query("opencypher",
        "MATCH (a:Person)-[r:E]->(b:Person) RETURN a.name AS src, b.name AS dst");
    assertThat(matchByE.hasNext()).isTrue();
    final Result rowE = matchByE.next();
    assertThat(rowE.<String>getProperty("src")).isEqualTo("Alice");
    assertThat(rowE.<String>getProperty("dst")).isEqualTo("Bob");
    assertThat(matchByE.hasNext()).isFalse();
  }

  @Test
  void cypherUpdateVisibleToSql_sameTransaction() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30");
    });

    database.transaction(() -> {
      database.command("opencypher", "MATCH (p:Person {name: 'Alice'}) SET p.age = 31");

      final ResultSet result = database.query("sql", "SELECT age FROM Person WHERE name = 'Alice'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Number>getProperty("age").intValue()).isEqualTo(31);
    });
  }

  @Test
  void sqlUpdateVisibleToCypher_sameTransaction() {
    database.transaction(() ->
      database.command("opencypher", "CREATE (n:Person {name: 'Bob', age: 25})"));

    database.transaction(() -> {
      database.command("sql", "UPDATE Person SET age = 26 WHERE name = 'Bob'");

      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person {name: 'Bob'}) RETURN p.age AS age");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Number>getProperty("age").intValue()).isEqualTo(26);
    });
  }

  @Test
  void cypherDeleteVisibleToSql_sameTransaction() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      database.command("sql", "INSERT INTO Person SET name = 'Bob'");
    });

    database.transaction(() -> {
      database.command("opencypher", "MATCH (p:Person {name: 'Alice'}) DELETE p");

      final ResultSet result = database.query("sql", "SELECT name FROM Person ORDER BY name");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("name")).isEqualTo("Bob");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void sqlDeleteVisibleToCypher_sameTransaction() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob'})");
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Person WHERE name = 'Alice'");

      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("name")).isEqualTo("Bob");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void cypherCreatedTypeReportedBySchemaToSql() {
    // Cypher CREATE auto-creates the vertex type. Verify SQL can immediately reference it.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Movie {title: 'Matrix'})");
      assertThat(database.getSchema().existsType("Movie")).isTrue();

      final ResultSet result = database.query("sql", "SELECT count(*) AS c FROM Movie");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    });
  }

  @Test
  void boundParameterReadFromIndexCreatedByOtherEngine() {
    // Issue test matrix step 5: bound parameters and projections that touch indexes created with
    // the other language. Build the index with SQL, then look up via Cypher using a bound parameter.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.name STRING");
      database.command("sql", "CREATE INDEX ON Person (name) UNIQUE");
      database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      database.command("sql", "INSERT INTO Person SET name = 'Bob'");
    });

    final Map<String, Object> params = new HashMap<>();
    params.put("name", "Alice");

    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person {name: $name}) RETURN p.name AS name", params);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void boundParameterReadFromIndexCreatedByCypher() {
    // Reverse: build the index implicitly via Cypher MERGE, then query via SQL using a parameter.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.name STRING");
      database.command("sql", "CREATE INDEX ON Person (name) UNIQUE");
    });

    database.transaction(() -> {
      database.command("opencypher", "MERGE (p:Person {name: 'Alice'})");
      database.command("opencypher", "MERGE (p:Person {name: 'Bob'})");
    });

    final Map<String, Object> params = new HashMap<>();
    params.put("name", "Bob");

    final ResultSet result = database.query("sql",
        "SELECT name FROM Person WHERE name = :name", params);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void rollbackHidesWritesFromBothEngines() {
    database.transaction(() ->
      database.command("sql", "CREATE VERTEX TYPE Person"));

    // Start a transaction, write via SQL, then read via Cypher, then roll back.
    database.begin();
    try {
      database.command("sql", "INSERT INTO Person SET name = 'Ephemeral'");

      final ResultSet during = database.query("opencypher",
          "MATCH (p:Person {name: 'Ephemeral'}) RETURN p.name AS name");
      assertThat(during.hasNext()).isTrue();
      assertThat(during.next().<String>getProperty("name")).isEqualTo("Ephemeral");
    } finally {
      database.rollback();
    }

    // After rollback both engines must agree the row never existed.
    final ResultSet afterSql = database.query("sql", "SELECT name FROM Person WHERE name = 'Ephemeral'");
    assertThat(afterSql.hasNext()).isFalse();

    final ResultSet afterCypher = database.query("opencypher",
        "MATCH (p:Person {name: 'Ephemeral'}) RETURN p.name AS name");
    assertThat(afterCypher.hasNext()).isFalse();
  }

  @Test
  void interleavedWritesVisibleAcrossEngines_sameTransaction() {
    // Stress the cross-engine view by interleaving SQL and Cypher writes inside one transaction.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE EDGE TYPE KNOWS");

      database.command("sql", "INSERT INTO Person SET name = 'A'");
      database.command("opencypher", "CREATE (n:Person {name: 'B'})");
      database.command("sql", "INSERT INTO Person SET name = 'C'");
      database.command("opencypher", "CREATE (n:Person {name: 'D'})");

      // Connect them with mixed engines.
      database.command("sql",
          "CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE name = 'A') TO (SELECT FROM Person WHERE name = 'B')");
      database.command("opencypher",
          "MATCH (b:Person {name: 'B'}), (c:Person {name: 'C'}) CREATE (b)-[:KNOWS]->(c)");
      database.command("sql",
          "CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE name = 'C') TO (SELECT FROM Person WHERE name = 'D')");

      // Count via SQL.
      final ResultSet sqlCount = database.query("sql", "SELECT count(*) AS c FROM Person");
      assertThat(sqlCount.next().<Number>getProperty("c").longValue()).isEqualTo(4L);

      // Count via Cypher.
      final ResultSet cypherCount = database.query("opencypher",
          "MATCH (n:Person) RETURN count(n) AS c");
      assertThat(cypherCount.next().<Number>getProperty("c").longValue()).isEqualTo(4L);

      // Walk the chain A -> B -> C -> D via Cypher and verify it's complete.
      final ResultSet chain = database.query("opencypher",
          "MATCH p = (a:Person {name: 'A'})-[:KNOWS*3]->(d:Person {name: 'D'}) RETURN count(p) AS c");
      assertThat(chain.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    });
  }

  @Test
  void schemaCreatedBySqlVisibleAsLabelToCypherInSameTransaction() {
    // Smoke test: SQL CREATE VERTEX TYPE creates the type. Cypher must see it immediately as a
    // label so MATCH returns the rows.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Product");
      database.command("sql", "INSERT INTO Product SET sku = 'X1'");

      assertThat(database.getSchema().existsType("Product")).isTrue();

      final ResultSet result = database.query("opencypher", "MATCH (p:Product) RETURN p.sku AS sku");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("sku")).isEqualTo("X1");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void schemaCreatedByCypherVisibleAsTypeToSqlInSameTransaction() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Product {sku: 'X1'})");

      assertThat(database.getSchema().existsType("Product")).isTrue();

      final ResultSet result = database.query("sql", "SELECT sku FROM Product");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("sku")).isEqualTo("X1");
      assertThat(result.hasNext()).isFalse();
    });
  }
}
