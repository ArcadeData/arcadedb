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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4413: cannot insert edges between existing nodes when the edge type
 * has mandatory properties.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherEdgeMandatoryPropertyTest {
  private Database database;
  private String databasePath;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-edge-mandatory-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE Node IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY Node.id IF NOT EXISTS STRING (MANDATORY TRUE, READONLY TRUE)");
    database.command("sql", "CREATE EDGE TYPE Edge IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY Edge.id IF NOT EXISTS STRING (MANDATORY TRUE, READONLY TRUE)");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createEdgeWithMandatoryPropertyBetweenExistingNodes() {
    database.transaction(() -> {
      database.command("opencypher", "MERGE (a:Node {id: 'a'})");
      database.command("opencypher", "MERGE (b:Node {id: 'b'})");
    });

    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Node {id: 'a'}), (b:Node {id: 'b'}) CREATE (a)-[e:Edge {id: 'e'}]->(b) RETURN e");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();
      final Object edge = r.getProperty("e");
      assertThat(edge).isInstanceOf(Edge.class);
      assertThat((String) ((Edge) edge).get("id")).isEqualTo("e");
    });

    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Node {id: 'a'})-[e:Edge]->(b:Node {id: 'b'}) RETURN e");
    assertThat(verify.hasNext()).isTrue();
    assertThat((String) verify.next().<Edge>getProperty("e").get("id")).isEqualTo("e");
  }

  @Test
  void mergeEdgeWithMandatoryPropertyBetweenExistingNodes() {
    database.transaction(() -> {
      database.command("opencypher", "MERGE (a:Node {id: 'a'})");
      database.command("opencypher", "MERGE (b:Node {id: 'b'})");
    });

    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Node {id: 'a'}), (b:Node {id: 'b'}) MERGE (a)-[e:Edge {id: 'e'}]->(b) RETURN e");

      assertThat(result.hasNext()).isTrue();
      final Object edge = result.next().getProperty("e");
      assertThat(edge).isInstanceOf(Edge.class);
      assertThat((String) ((Edge) edge).get("id")).isEqualTo("e");
    });

    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Node {id: 'a'})-[e:Edge]->(b:Node {id: 'b'}) RETURN e");
    assertThat(verify.hasNext()).isTrue();
    assertThat((String) verify.next().<Edge>getProperty("e").get("id")).isEqualTo("e");
  }
}
