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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Gremlin traversals use CSR acceleration when a GAV (GraphAnalyticalView) is available.
 */
class GremlinGAVTest {
  private ArcadeGraph graph;
  private GraphAnalyticalView gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/testgremlin-gav");

    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");
    graph.getDatabase().getSchema().createEdgeType("LIKES");

    // Build graph: Alice -KNOWS-> Bob -KNOWS-> Charlie, Alice -LIKES-> Charlie
    graph.getDatabase().transaction(() -> {
      final MutableVertex alice = graph.getDatabase().newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = graph.getDatabase().newVertex("Person").set("name", "Bob").save();
      final MutableVertex charlie = graph.getDatabase().newVertex("Person").set("name", "Charlie").save();

      alice.newEdge("KNOWS", bob);
      bob.newEdge("KNOWS", charlie);
      alice.newEdge("LIKES", charlie);
    });

    // Build GAV covering all edge types
    gav = new GraphAnalyticalView((Database) graph.getDatabase());
    gav.build(new String[] { "Person" }, new String[] { "KNOWS", "LIKES" });
  }

  @AfterEach
  void teardown() {
    if (gav != null)
      gav.drop();
    if (graph != null)
      graph.drop();
  }

  @Test
  void outWithGAV() {
    // g.V().has('name','Alice').out('KNOWS') should return Bob via CSR
    final ResultSet rs = graph.gremlin("g.V().has('name','Alice').out('KNOWS').values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactly("Bob");
  }

  @Test
  void inWithGAV() {
    // g.V().has('name','Bob').in('KNOWS') should return Alice via CSR
    final ResultSet rs = graph.gremlin("g.V().has('name','Bob').in('KNOWS').values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactly("Alice");
  }

  @Test
  void bothWithGAV() {
    // g.V().has('name','Bob').both('KNOWS') should return Alice and Charlie via CSR
    final ResultSet rs = graph.gremlin("g.V().has('name','Bob').both('KNOWS').values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactlyInAnyOrder("Alice", "Charlie");
  }

  @Test
  void multiHopWithGAV() {
    // g.V().has('name','Alice').out('KNOWS').out('KNOWS') should return Charlie
    final ResultSet rs = graph.gremlin("g.V().has('name','Alice').out('KNOWS').out('KNOWS').values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactly("Charlie");
  }

  @Test
  void multipleEdgeTypesWithGAV() {
    // Alice -KNOWS-> Bob AND Alice -LIKES-> Charlie
    final ResultSet rs = graph.gremlin("g.V().has('name','Alice').out('LIKES').values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactly("Charlie");
  }

  @Test
  void outWithNoEdgeLabelWithGAV() {
    // g.V().has('name','Alice').out() — no label filter, should return Bob (KNOWS) and Charlie (LIKES)
    final ResultSet rs = graph.gremlin("g.V().has('name','Alice').out().values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactlyInAnyOrder("Bob", "Charlie");
  }

  @Test
  void countWithGAV() {
    // Count outgoing edges from Alice
    final ResultSet rs = graph.gremlin("g.V().has('name','Alice').out().count()").execute();
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("result")).longValue()).isEqualTo(2L);
  }

  @Test
  void resultConsistencyWithoutGAV() {
    // Drop GAV and verify same results via OLTP
    gav.drop();
    gav = null;

    final ResultSet rs = graph.gremlin("g.V().has('name','Alice').out('KNOWS').values('name')").execute();
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("result"));
    assertThat(names).containsExactly("Bob");
  }
}
