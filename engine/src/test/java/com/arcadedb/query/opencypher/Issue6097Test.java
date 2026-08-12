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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for <a href="https://github.com/ArcadeData/arcadedb/issues/6097">#6097</a>.
 * <p>
 * A hub-and-spoke graph with a modest branching factor (200 nodes per level, 3 levels) has only
 * 601 nodes and 80,200 edges, but {@code FANOUT^3 = 8,000,000} distinct length-3 paths between the
 * hub and the outermost level. Before the fix, {@code ExpandPathStep} always drove variable-length
 * patterns through a BFS traverser whose iterator materialized every one of those paths - across
 * every intermediate level - into a single in-memory list before the first row was even produced,
 * regardless of what the consuming clause actually needed. A plain {@code count(DISTINCT b)} (600
 * distinct destinations) or a {@code LIMIT 5} therefore paid for the full 8,000,000-path enumeration
 * anyway.
 */
@Tag("slow")
class Issue6097Test extends TestHelper {
  private static final int FANOUT = 200;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub6097");
      database.getSchema().createVertexType("L1_6097");
      database.getSchema().createVertexType("L2_6097");
      database.getSchema().createVertexType("L3_6097");
      database.getSchema().createEdgeType("R1_6097");
      database.getSchema().createEdgeType("R2_6097");
      database.getSchema().createEdgeType("R3_6097");

      final MutableVertex hub = database.newVertex("Hub6097").set("id", 0).save();

      final List<MutableVertex> l1 = new ArrayList<>(FANOUT);
      for (int i = 0; i < FANOUT; i++) {
        final MutableVertex v = database.newVertex("L1_6097").set("id", i).save();
        hub.newEdge("R1_6097", v).save();
        l1.add(v);
      }

      final List<MutableVertex> l2 = new ArrayList<>(FANOUT);
      for (int i = 0; i < FANOUT; i++)
        l2.add(database.newVertex("L2_6097").set("id", i).save());

      for (final MutableVertex a : l1)
        for (final MutableVertex b : l2)
          a.newEdge("R2_6097", b).save();

      final List<MutableVertex> l3 = new ArrayList<>(FANOUT);
      for (int i = 0; i < FANOUT; i++)
        l3.add(database.newVertex("L3_6097").set("id", i).save());

      for (final MutableVertex a : l2)
        for (final MutableVertex b : l3)
          a.newEdge("R3_6097", b).save();
    });
  }

  @Test
  void countDistinctOverVariableLengthPathDoesNotMaterializeEveryPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH (:Hub6097)-[*1..3]->(b) RETURN count(DISTINCT b) AS c");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    // Only the 3*FANOUT distinct destination nodes matter - not the FANOUT^3 distinct paths.
    assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(3L * FANOUT);
  }

  @Test
  void limitShortCircuitsInsteadOfExploringEveryPath() {
    final List<Object> targets = new ArrayList<>();

    // With a genuinely lazy (streaming) traversal, LIMIT stops expansion almost immediately - it
    // never needs to enumerate anywhere near the 8,000,000 length-3 paths that exist. Before the
    // fix this never returned in any reasonable time: the traverser built its full path list
    // before ExpandPathStep - let alone LIMIT - ever pulled the first row.
    assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (:Hub6097)-[*1..3]->(b) RETURN b LIMIT 5");
      while (result.hasNext())
        targets.add(result.next().getProperty("b"));
    });

    assertThat(targets).hasSize(5);
  }
}
