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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ORDER BY on a graph-function expression (e.g. {@code ORDER BY both().size() DESC}) must actually sort.
 * The AST builder used to collapse such an expression into a property ALIAS holding its literal text
 * ("both().size()"), so every sort key resolved to null and the rows silently came back in scan order -
 * only queries whose hub happened to be the first record in the bucket looked correct.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrderByGraphFunctionTest extends TestHelper {
  private static final int SOURCES = 50;

  /**
   * Creates {@code SOURCES} low-degree vertices and one high-degree hub. The hub's position in the bucket is
   * chosen AGAINST the expected result order ({@code hubFirst} for ascending tests, hub last for descending
   * ones) so a broken (no-op) sort returning scan order can never look correct by accident.
   */
  private RID setupGraph(final boolean hubFirst) {
    database.transaction(() -> {
      database.getSchema().createVertexType("Node", 1);
      database.getSchema().createEdgeType("Link", 1);
    });

    final RID[] hub = new RID[1];
    if (hubFirst)
      database.transaction(() -> hub[0] = database.newVertex("Node").set("name", "hub").save().getIdentity());

    final RID[] sources = new RID[SOURCES];
    database.transaction(() -> {
      for (int i = 0; i < SOURCES; i++)
        sources[i] = database.newVertex("Node").set("i", i).save().getIdentity();
    });

    database.transaction(() -> {
      if (!hubFirst)
        hub[0] = database.newVertex("Node").set("name", "hub").save().getIdentity();
      for (int i = 0; i < SOURCES; i++)
        sources[i].asVertex(true).newEdge("Link", hub[0]);
    });
    return hub[0];
  }

  private RID setupGraph() {
    return setupGraph(false);
  }

  @Test
  void orderByGraphFunctionExpressionWithMatchingProjection() {
    setupGraph();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT name, both().size() AS degree FROM Node ORDER BY both().size() DESC LIMIT 3")) {
        final Result top = rs.next();
        assertThat((String) top.getProperty("name")).isEqualTo("hub");
        assertThat(((Number) top.getProperty("degree")).intValue()).isEqualTo(SOURCES);
        // The remaining rows are the degree-1 sources.
        assertThat(((Number) rs.next().getProperty("degree")).intValue()).isEqualTo(1);
      }
    });
  }

  @Test
  void orderByGraphFunctionExpressionWithoutMatchingProjection() {
    setupGraph();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT name FROM Node ORDER BY in().size() DESC LIMIT 1")) {
        assertThat((String) rs.next().getProperty("name")).isEqualTo("hub");
      }
    });
  }

  @Test
  void orderByGraphFunctionExpressionWithoutProjection() {
    final RID hub = setupGraph();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Node ORDER BY both().size() DESC LIMIT 1")) {
        assertThat(rs.next().getIdentity().orElse(null)).isEqualTo(hub);
      }
    });
  }

  @Test
  void orderByProjectionAliasStillSorts() {
    setupGraph();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT name, both().size() AS degree FROM Node ORDER BY degree DESC LIMIT 1")) {
        final Result top = rs.next();
        assertThat((String) top.getProperty("name")).isEqualTo("hub");
        assertThat(((Number) top.getProperty("degree")).intValue()).isEqualTo(SOURCES);
      }
    });
  }

  /** Ascending order must put the hub LAST, proving the direction handling on expression sort keys. */
  @Test
  void orderByGraphFunctionExpressionAscending() {
    setupGraph(true);
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT name, both().size() AS degree FROM Node ORDER BY both().size() ASC")) {
        Result last = null;
        while (rs.hasNext())
          last = rs.next();
        assertThat(last).isNotNull();
        assertThat((String) last.getProperty("name")).isEqualTo("hub");
      }
    });
  }

  /** The hub still traverses as a vertex after the projection machinery (sanity on the hidden projection). */
  @Test
  void hubDegreeIsCorrect() {
    final RID hub = setupGraph();
    database.transaction(() ->
        assertThat(hub.asVertex(true).countEdges(Vertex.DIRECTION.BOTH, "Link")).isEqualTo(SOURCES));
  }
}
