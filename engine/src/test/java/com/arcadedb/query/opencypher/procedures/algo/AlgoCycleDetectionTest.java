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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.cycleDetection Cypher procedure.
 */
class AlgoCycleDetectionTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-cycle-detection");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void buildCyclicGraph() {
    // A->B->C->A forms a cycle
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  private void buildAcyclicGraph() {
    // Linear chain A->B->C->D (DAG, no cycle)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @Test
  void cycleDetectionReturnsThreeNodes() {
    buildCyclicGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.cycleDetection() YIELD node, inCycle, hasCycle RETURN node, inCycle, hasCycle");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void cycleDetectionFieldsNotNull() {
    buildCyclicGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.cycleDetection() YIELD node, inCycle, hasCycle RETURN node, inCycle, hasCycle");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object inCycle = r.getProperty("inCycle");
      final Object hasCycle = r.getProperty("hasCycle");
      assertThat(node).isNotNull();
      assertThat(inCycle).isNotNull();
      assertThat(hasCycle).isNotNull();
    }
  }

  @Test
  void cycleDetectedInCyclicGraph() {
    buildCyclicGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.cycleDetection() YIELD node, inCycle, hasCycle RETURN node, inCycle, hasCycle");

    boolean hasCycleGlobal = false;
    boolean anyNodeInCycle = false;

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object hasCycleObj = r.getProperty("hasCycle");
      final Object inCycleObj = r.getProperty("inCycle");
      if (Boolean.TRUE.equals(hasCycleObj))
        hasCycleGlobal = true;
      if (Boolean.TRUE.equals(inCycleObj))
        anyNodeInCycle = true;
    }

    assertThat(hasCycleGlobal).isTrue();
    assertThat(anyNodeInCycle).isTrue();
  }

  @Test
  void nodesInCyclicComponentAreMarked() {
    buildCyclicGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.cycleDetection() YIELD node, inCycle RETURN node, inCycle");

    boolean inCycleA = false, inCycleB = false, inCycleC = false;

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final Object inCycleObj = r.getProperty("inCycle");
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        final boolean inCycle = Boolean.TRUE.equals(inCycleObj);
        switch (name) {
          case "A" -> inCycleA = inCycle;
          case "B" -> inCycleB = inCycle;
          case "C" -> inCycleC = inCycle;
        }
      }
    }

    // A, B, C form a cycle so all should be marked inCycle=true
    assertThat(inCycleA).isTrue();
    assertThat(inCycleB).isTrue();
    assertThat(inCycleC).isTrue();
  }

  @Test
  void noCycleDetectedInAcyclicGraph() {
    buildAcyclicGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.cycleDetection() YIELD node, inCycle, hasCycle RETURN node, inCycle, hasCycle");

    boolean hasCycleGlobal = false;

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object hasCycleObj = r.getProperty("hasCycle");
      if (Boolean.TRUE.equals(hasCycleObj))
        hasCycleGlobal = true;
    }

    assertThat(hasCycleGlobal).isFalse();
  }
}
