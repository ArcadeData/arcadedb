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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12247: an {@code @in} / {@code @out} among the properties of a new edge
 * ({@code CREATE EDGE ... SET @in = c}, {@code CONTENT {"@in": ...}}, openCypher {@code CREATE ()-[:F {`@in`: ...}]->()},
 * {@code newEdge(..., "@in", c)}) set the endpoint of the edge record, while the edge was linked into the adjacency lists of
 * the FROM and TO of the creation. SQL traversals and openCypher then disagreed, and CHECK DATABASE FIX deleted the edge.
 * The edge is now wired to the vertices its record names, as UPDATE re-points an existing edge.
 */
class CreateEdgeEndpointPropertyIssue12247Test extends TestHelper {
  private RID a, b, c;

  private void createVertices() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
    database.getSchema().createDocumentType("D");
    database.transaction(() -> {
      a = database.newVertex("V").set("name", "a").save().getIdentity();
      b = database.newVertex("V").set("name", "b").save().getIdentity();
      c = database.newVertex("V").set("name", "c").save().getIdentity();
    });
  }

  private static List<RID> rids(final Iterable<Edge> edges) {
    final List<RID> list = new ArrayList<>();
    for (final Edge e : edges)
      list.add(e.getIdentity());
    return list;
  }

  private List<RID> out(final RID vertex, final String type) {
    return rids(vertex.asVertex().getEdges(Vertex.DIRECTION.OUT, type));
  }

  private List<RID> in(final RID vertex, final String type) {
    return rids(vertex.asVertex().getEdges(Vertex.DIRECTION.IN, type));
  }

  private void assertWired(final RID edgeRid, final RID expectedOut, final RID expectedIn, final boolean bidirectional) {
    final Edge edge = edgeRid.asEdge();
    assertThat(edge.getOut()).isEqualTo(expectedOut);
    assertThat(edge.getIn()).isEqualTo(expectedIn);
    final String type = edge.getTypeName();
    for (final RID v : new RID[] { a, b, c }) {
      assertThat(out(v, type).contains(edgeRid)).as("OUT list of " + v).isEqualTo(v.equals(expectedOut));
      assertThat(in(v, type).contains(edgeRid)).as("IN list of " + v).isEqualTo(bidirectional && v.equals(expectedIn));
    }
    assertCheckDatabaseClean();
  }

  private void assertCheckDatabaseClean() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result check = rs.next();
      assertThat(check.<Long>getProperty("totalWarnings")).isZero();
      assertThat(check.<Long>getProperty("totalErrors")).isZero();
    }
  }

  private RID createdBy(final String language, final String command) {
    final RID[] created = new RID[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command(language, command)) {
        if (rs.hasNext())
          created[0] = rs.next().getIdentity().orElse(null);
      }
    });
    if (created[0] == null)
      try (final ResultSet rs = database.query("sql", "SELECT FROM F ORDER BY @rid DESC LIMIT 1")) {
        created[0] = rs.next().getIdentity().orElseThrow();
      }
    return created[0];
  }

  private long countF() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM F")) {
      return rs.next().<Long>getProperty("n");
    }
  }

  @Test
  void sqlSetIn() {
    createVertices();
    assertWired(createdBy("sql", "CREATE EDGE F FROM " + a + " TO " + b + " SET `@in` = " + c), a, c, true);
  }

  @Test
  void sqlSetInWithoutBackticksAndAProperty() {
    createVertices();
    final RID edge = createdBy("sql", "CREATE EDGE F FROM " + a + " TO " + b + " SET @in = " + c + ", w = 1");
    assertWired(edge, a, c, true);
    assertThat(edge.asEdge().<Integer>get("w")).isEqualTo(1);
  }

  @Test
  void sqlContentIn() {
    createVertices();
    assertWired(createdBy("sql", "CREATE EDGE F FROM " + a + " TO " + b + " CONTENT {\"@in\": \"" + c + "\"}"), a, c, true);
  }

  @Test
  void sqlSetOut() {
    createVertices();
    assertWired(createdBy("sql", "CREATE EDGE F FROM " + a + " TO " + b + " SET `@out` = " + c), c, b, true);
  }

  @Test
  void sqlSetInOnUnidirectionalType() {
    createVertices();
    final RID[] created = new RID[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "CREATE EDGE U FROM " + a + " TO " + b + " SET `@in` = " + c)) {
        created[0] = rs.next().getIdentity().orElseThrow();
      }
    });
    assertWired(created[0], a, c, false);
  }

  @Test
  void cypherCreateWithInProperty() {
    createVertices();
    database.transaction(() -> database.command("opencypher",
        "MATCH (x {name: 'a'}), (y {name: 'b'}) CREATE (x)-[:F {`@in`: '" + c + "'}]->(y)"));
    try (final ResultSet rs = database.query("sql", "SELECT FROM F")) {
      assertWired(rs.next().getIdentity().orElseThrow(), a, c, true);
    }
  }

  @Test
  void javaNewEdgeWithInProperty() {
    createVertices();
    final RID[] created = new RID[1];
    database.transaction(() -> created[0] = a.asVertex().modify().newEdge("F", b, "@in", c).getIdentity());
    assertWired(created[0], a, c, true);
  }

  @Test
  void endpointThatIsNotAVertexIsRefusedAndNothingIsCreated() {
    createVertices();
    final RID[] document = new RID[1];
    database.transaction(() -> document[0] = database.newDocument("D").save().getIdentity());
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "CREATE EDGE F FROM " + a + " TO " + b + " SET `@in` = " + document[0])))
        .hasMessageContaining("not a vertex");
    assertThatThrownBy(() -> database.transaction(() -> a.asVertex().modify().newEdge("F", b, "@in", document[0])))
        .hasMessageContaining("not a vertex");
    assertThat(countF()).isZero();
    assertThat(out(a, "F")).isEmpty();
    assertThat(in(b, "F")).isEmpty();
    assertCheckDatabaseClean();
  }

  @Test
  void endpointThatDoesNotExistIsRefusedAndNothingIsCreated() {
    createVertices();
    final RID missing = new RID(a.getBucketId(), 999_999L);
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "CREATE EDGE F FROM " + a + " TO " + b + " SET `@in` = " + missing)))
        .hasMessageContaining(missing.toString());
    assertThatThrownBy(() -> database.transaction(() -> a.asVertex().modify().newEdge("F", b, "@in", missing)))
        .hasMessageContaining(missing.toString());
    assertThat(countF()).isZero();
    assertCheckDatabaseClean();
  }

  @Test
  void endpointEqualToTheGivenOneKeepsTheEdge() {
    createVertices();
    assertWired(createdBy("sql", "CREATE EDGE F FROM " + a + " TO " + b + " SET `@in` = " + b + ", `@out` = " + a), a, b, true);
  }
}
