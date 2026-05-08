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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the GQL INSERT clause (issue #3365 section 1.1).
 * INSERT is a synonym of CREATE per ISO/IEC 39075:2024 with stricter syntax (no variable-length).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3365InsertClauseTest {
  private Database database;
  private String   databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-insert-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_AT");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void insertSingleVertex() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "INSERT (n:Person {name: 'Alice', age: 30}) RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();
      final Vertex v = (Vertex) r.toElement();
      assertThat(v.getTypeName()).isEqualTo("Person");
      assertThat((String) v.get("name")).isEqualTo("Alice");
      assertThat((Integer) v.get("age")).isEqualTo(30);
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void insertMultiplePatternsInOneStatement() {
    database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void insertRelationship() {
    database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person {name: 'Alice'})");
      database.command("opencypher", "INSERT (b:Person {name: 'Bob'})");
    });

    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) INSERT (a)-[r:KNOWS]->(b) RETURN r");
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void insertProducesIdenticalGraphToCreate() {
    database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})");
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'C'})-[:KNOWS]->(b:Person {name: 'D'})");
    });

    final ResultSet verifyVertices = database.query("opencypher", "MATCH (n:Person) RETURN count(n) AS c");
    assertThat(((Number) verifyVertices.next().getProperty("c")).longValue()).isEqualTo(4L);

    final ResultSet verifyEdges = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c");
    assertThat(((Number) verifyEdges.next().getProperty("c")).longValue()).isEqualTo(2L);
  }

  @Test
  void insertRejectsVariableLengthRelationship() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person)-[:KNOWS*1..3]->(b:Person)");
    }))
        .isInstanceOfAny(CommandParsingException.class, RuntimeException.class)
        .hasMessageContaining("");
  }

  @Test
  void insertWithLabelIsSyntax() {
    database.transaction(() -> {
      database.command("opencypher", "INSERT (n IS Person {name: 'Eve'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Eve'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }
}
