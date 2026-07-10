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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #5194: using an existing schema type in a Cypher context requiring the
 * other graph-element kind (vertex type as relationship type or edge type as node label) leaked raw
 * Java {@link ClassCastException} details instead of a clean, user-facing behavior.
 * <p>
 * Reference semantics (Neo4j keeps node labels and relationship types in separate namespaces):
 * <ul>
 *   <li>MATCH with a kind-mismatched name matches nothing (0 rows), same as a non-existent type.</li>
 *   <li>CREATE with a kind-mismatched name cannot succeed in ArcadeDB's shared type namespace, so it
 *       must fail with a clean schema validation error, never an internal cast failure.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherTypeKindMismatchIssue5194Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherkind5194").create();

    database.transaction(() -> {
      // 'SharedCollision' exists only as a vertex type; 'EdgeOnlyType' only as an edge type
      database.command("opencypher", "CREATE (:SharedCollision {v:1})");
      database.command("opencypher", """
          CREATE
            (a:VertexA {v:1}),
            (b:VertexB {v:2}),
            (a)-[:EdgeOnlyType {w:1}]->(b)
          """);
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void createRelationshipWithVertexTypeNameFailsWithCleanError() {
    final Throwable thrown = catchThrowable(() -> database.transaction(() -> database.command("opencypher", """
        MATCH (a:SharedCollision)
        CREATE (a)-[:SharedCollision]->(:TargetCollision {v:2})
        RETURN 1 AS ok
        """)));

    assertThat(thrown).isNotNull();
    assertMessageChainIsClean(thrown, "SharedCollision");
  }

  @Test
  void mergeRelationshipWithVertexTypeNameFailsWithCleanError() {
    final Throwable thrown = catchThrowable(() -> database.transaction(() -> database.command("opencypher", """
        MATCH (a:SharedCollision)
        MERGE (a)-[:SharedCollision]->(:TargetCollision {v:2})
        RETURN 1 AS ok
        """)));

    assertThat(thrown).isNotNull();
    assertMessageChainIsClean(thrown, "SharedCollision");
  }

  @Test
  void matchNodeWithEdgeTypeNameReturnsZero() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "MATCH (a:EdgeOnlyType) RETURN count(a) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat((Long) rs.next().getProperty("cnt")).isEqualTo(0L);
    });
  }

  @Test
  void matchNodeWithEdgeTypeNameAndPropertyFilterReturnsZero() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "MATCH (a:EdgeOnlyType {w:1}) RETURN count(a) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat((Long) rs.next().getProperty("cnt")).isEqualTo(0L);
    });
  }

  @Test
  void matchRelationshipWithVertexTypeNameReturnsZero() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "MATCH ()-[r:SharedCollision]->() RETURN count(r) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat((Long) rs.next().getProperty("cnt")).isEqualTo(0L);
    });
  }

  @Test
  void createNodeWithEdgeTypeNameFailsWithCleanError() {
    final Throwable thrown = catchThrowable(
        () -> database.transaction(() -> database.command("opencypher", "CREATE (:EdgeOnlyType {v:9})")));

    assertThat(thrown).isNotNull();
    assertMessageChainIsClean(thrown, "EdgeOnlyType");
  }

  @Test
  void sqlCreateEdgeWithVertexTypeNameFailsWithCleanError() {
    final Throwable thrown = catchThrowable(() -> database.transaction(() -> database.command("sql",
        "CREATE EDGE SharedCollision FROM (SELECT FROM VertexA) TO (SELECT FROM VertexB)")));

    assertThat(thrown).isNotNull();
    assertMessageChainIsClean(thrown, "SharedCollision");
  }

  @Test
  void controlQueriesStillWork() {
    database.transaction(() -> {
      ResultSet rs = database.query("opencypher", "MATCH (a:VertexA) RETURN count(a) AS cnt");
      assertThat((Long) rs.next().getProperty("cnt")).isEqualTo(1L);

      rs = database.query("opencypher", "MATCH (a:NoSuchType) RETURN count(a) AS cnt");
      assertThat((Long) rs.next().getProperty("cnt")).isEqualTo(0L);

      rs = database.command("opencypher", "CREATE (a:ControlNode {v:1})-[:ControlEdge]->(b:ControlNode {v:2}) RETURN 1 AS ok");
      assertThat(((Number) rs.next().getProperty("ok")).intValue()).isEqualTo(1);
    });
  }

  /**
   * The error must mention the offending type name and must not leak internal Java class names or
   * raw ClassCastException wording anywhere in the exception chain.
   */
  private static void assertMessageChainIsClean(final Throwable thrown, final String typeName) {
    boolean mentionsType = false;
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      assertThat(t).isNotInstanceOf(ClassCastException.class);
      final String msg = t.getMessage();
      if (msg == null)
        continue;
      assertThat(msg).doesNotContain("cannot be cast", "ClassCastException", "LocalVertexType", "LocalEdgeType");
      if (msg.contains(typeName))
        mentionsType = true;
    }
    assertThat(mentionsType).isTrue();
  }
}
