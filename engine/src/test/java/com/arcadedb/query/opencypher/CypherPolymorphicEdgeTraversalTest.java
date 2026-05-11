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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for polymorphic edge traversal in Cypher MATCH.
 * <p>
 * ArcadeDB supports edge type inheritance (unlike Neo4j). When edge type {@code SUB_REL}
 * extends edge type {@code BASE_REL}, a Cypher pattern {@code [:BASE_REL]} should match
 * edges of both {@code BASE_REL} and {@code SUB_REL}, mirroring vertex label polymorphism
 * and matching the behavior of the native Java traversal API
 * ({@code vertex.getEdges(direction, "BASE_REL")} returns subtype edges too).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherPolymorphicEdgeTraversalTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher-polymorphic-edge").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("BASE_REL");
      database.getSchema().createEdgeType("SUB_REL").addSuperType("BASE_REL");

      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
      final MutableVertex dave = database.newVertex("Person").set("name", "Dave").save();

      // alice -[BASE_REL]-> bob
      // alice -[SUB_REL]-> charlie  (SUB_REL extends BASE_REL)
      // bob   -[SUB_REL]-> dave
      alice.newEdge("BASE_REL", bob);
      alice.newEdge("SUB_REL", charlie);
      bob.newEdge("SUB_REL", dave);
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Sanity baseline: native Java API is polymorphic (uses {@code getBucketIds(true)}).
   * Establishes the expected behavior Cypher should match.
   */
  @Test
  void nativeJavaApiIsPolymorphic() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = 'Alice'");
      final Vertex alice = rs.next().getVertex().get();

      int count = 0;
      for (final var ignored : alice.getEdges(Vertex.DIRECTION.OUT, "BASE_REL"))
        count++;
      assertThat(count).isEqualTo(2);
    });
  }

  /**
   * Reproduces the discord bug: {@code MATCH (a)-[r:BASE_REL]->(b)} on the standard path
   * (relationship variable forces standard path) should include subtype edges.
   */
  @Test
  void cypherMatchWithRelVarIsPolymorphic() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'Alice'})-[r:BASE_REL]->(b) RETURN count(r) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue())
        .as("MATCH (a)-[r:BASE_REL]->(b) must include subtype SUB_REL edges (polymorphic)")
        .isEqualTo(2L);
  }

  /**
   * Same as above but on the fast path (no relationship variable). Fast path uses
   * {@code vertex.getVertices(direction, types)} which already honors polymorphism.
   */
  @Test
  void cypherMatchWithoutRelVarIsPolymorphic() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'Alice'})-[:BASE_REL]->(b) RETURN count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue())
        .as("MATCH (a)-[:BASE_REL]->(b) must include subtype SUB_REL edges (polymorphic)")
        .isEqualTo(2L);
  }

  /**
   * The subtype is not polymorphic: {@code [:SUB_REL]} must NOT match parent {@code BASE_REL} edges.
   */
  @Test
  void cypherMatchOnSubtypeDoesNotMatchParent() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'Alice'})-[r:SUB_REL]->(b) RETURN count(r) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }

  /**
   * Multi-hop traversal across the base label: alice -[BASE_REL]-> bob -[SUB_REL]-> dave
   * should be reachable via {@code [:BASE_REL]*2}.
   */
  @Test
  void cypherMultiHopOverBaseTypeIsPolymorphic() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'Alice'})-[:BASE_REL*2]->(b) RETURN b.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Dave");
    assertThat(rs.hasNext()).isFalse();
  }
}
