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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.wcc Cypher procedure (Weakly Connected Components).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoWCCTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-wcc");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");
    database.getSchema().createEdgeType("OTHER");

    // Two disconnected components:
    // Component 1: A -> B -> C
    // Component 2: D -> E
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      d.newEdge("EDGE", e, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void wccReturnsComponentIdForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.wcc() YIELD node, componentId RETURN node, componentId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(5);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object componentId = result.getProperty("componentId");
      assertThat(componentId).isNotNull();
    }
  }

  @Test
  void wccDetectsTwoComponents() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.wcc() YIELD node, componentId RETURN DISTINCT componentId");

    final Set<Object> componentIds = new HashSet<>();
    while (rs.hasNext())
      componentIds.add(rs.next().getProperty("componentId"));

    assertThat(componentIds).hasSize(2);
  }

  @Test
  void wccNodesInSameComponentHaveSameId() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.wcc() YIELD node, componentId RETURN node.name AS name, componentId ORDER BY name");

    long compA = -1, compB = -1, compC = -1, compD = -1, compE = -1;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final long comp = ((Number) result.getProperty("componentId")).longValue();
      switch (name) {
        case "A" -> compA = comp;
        case "B" -> compB = comp;
        case "C" -> compC = comp;
        case "D" -> compD = comp;
        case "E" -> compE = comp;
      }
    }

    // A, B, C are in same component
    assertThat(compA).isEqualTo(compB);
    assertThat(compB).isEqualTo(compC);
    // D, E are in same component
    assertThat(compD).isEqualTo(compE);
    // The two components should be different
    assertThat(compA).isNotEqualTo(compD);
  }

  @Test
  void wccSingleComponent() {
    final DatabaseFactory wccFactory = new DatabaseFactory("./target/databases/test-algo-wcc-single");
    if (wccFactory.exists())
      wccFactory.open().drop();
    final Database db = wccFactory.create();
    try {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("EDGE");
      db.transaction(() -> {
        final MutableVertex x = db.newVertex("Node").set("name", "X").save();
        final MutableVertex y = db.newVertex("Node").set("name", "Y").save();
        final MutableVertex z = db.newVertex("Node").set("name", "Z").save();
        x.newEdge("EDGE", y, true, (Object[]) null).save();
        y.newEdge("EDGE", z, true, (Object[]) null).save();
      });

      final ResultSet rs = db.query("opencypher",
          "CALL algo.wcc() YIELD node, componentId RETURN DISTINCT componentId");

      final Set<Object> componentIds = new HashSet<>();
      while (rs.hasNext())
        componentIds.add(rs.next().getProperty("componentId"));

      assertThat(componentIds).hasSize(1);
    } finally {
      db.drop();
    }
  }

  @Test
  void wccWithRelTypesRestrictsToThoseEdges() {
    bridgeComponentsWithOtherEdge();

    final Map<String, Long> comp = componentsByName("CALL algo.wcc('EDGE')");
    assertThat(comp.get("B")).isEqualTo(comp.get("A"));
    assertThat(comp.get("C")).isEqualTo(comp.get("A"));
    assertThat(comp.get("E")).isEqualTo(comp.get("D"));
    assertThat(comp.get("D")).isNotEqualTo(comp.get("A"));
  }

  @Test
  void wccWithRelTypesIgnoresOtherEdgeTypes() {
    bridgeComponentsWithOtherEdge();

    // OTHER holds only C -> D, so A, B and E stay on their own
    final Map<String, Long> comp = componentsByName("CALL algo.wcc('OTHER')");
    assertThat(comp.get("D")).isEqualTo(comp.get("C"));
    assertThat(comp.get("A")).isNotEqualTo(comp.get("C"));
    assertThat(comp.get("B")).isNotEqualTo(comp.get("C"));
    assertThat(comp.get("E")).isNotEqualTo(comp.get("C"));
    assertThat(Set.copyOf(List.of(comp.get("A"), comp.get("B"), comp.get("E")))).hasSize(3);
  }

  @Test
  void wccWithUnknownRelTypeIsolatesEveryVertex() {
    final Map<String, Long> comp = componentsByName("CALL algo.wcc('NOSUCH')");
    assertThat(Set.copyOf(comp.values())).hasSize(5);
  }

  @Test
  void wccWithCommaSeparatedRelTypesCoversBoth() {
    bridgeComponentsWithOtherEdge();

    final Map<String, Long> comp = componentsByName("CALL algo.wcc('EDGE,OTHER')");
    assertThat(comp.values()).containsOnly(comp.get("A"));
  }

  @Test
  void wccWithRelTypesIsNotWidenedByAViewCoveringMoreTypes() {
    bridgeComponentsWithOtherEdge();

    // a view covering both types matches a request for either one, so the filter has to be
    // applied to the traversal rather than inherited from the view's topology
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("EDGE", "OTHER")
        .build();
    assertThat(gav.isBuilt()).isTrue();

    final Map<String, Long> comp = componentsByName("CALL algo.wcc('EDGE')");
    assertThat(comp.get("D")).isNotEqualTo(comp.get("A"));
  }

  private void bridgeComponentsWithOtherEdge() {
    database.transaction(() -> {
      final Vertex c = (Vertex) database.query("sql",
          "SELECT FROM Node WHERE name = 'C'").next().getElement().get();
      final Vertex d = (Vertex) database.query("sql",
          "SELECT FROM Node WHERE name = 'D'").next().getElement().get();
      c.newEdge("OTHER", d, true, (Object[]) null).save();
    });
  }

  private Map<String, Long> componentsByName(final String call) {
    final ResultSet rs = database.query("opencypher",
        call + " YIELD node, componentId RETURN node.name AS name, componentId");
    final Map<String, Long> byName = new HashMap<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      byName.put(result.getProperty("name"), ((Number) result.getProperty("componentId")).longValue());
    }
    return byName;
  }
}
