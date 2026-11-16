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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.clique Cypher procedure.
 */
class AlgoCliqueTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-clique");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Create a triangle A-B-C (clique of size 3) plus isolated D
    // A-B, B-C, A-C (triangle), D is isolated
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      // Bidirectional to form undirected triangle
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", a, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void cliqueFindsTriangle() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.clique('EDGE', 3) YIELD clique, size RETURN clique, size");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
    // There should be at least one clique of size 3
    boolean foundSize3 = false;
    for (final Result r : results) {
      final Object val = r.getProperty("size");
      if (((Number) val).intValue() == 3) {
        foundSize3 = true;
        break;
      }
    }
    assertThat(foundSize3).isTrue();
  }

  @Test
  void cliqueSizesAtLeastMinSize() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.clique('EDGE', 3) YIELD clique, size RETURN clique, size");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object val = result.getProperty("size");
      assertThat(((Number) val).intValue()).isGreaterThanOrEqualTo(3);
    }
  }

  @Test
  void cliqueListContainsRIDs() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.clique('EDGE', 3) YIELD clique, size RETURN clique, size");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object clique = result.getProperty("clique");
      assertThat(clique).isNotNull();
      assertThat(clique).isInstanceOf(List.class);
      final List<?> cliqueList = (List<?>) clique;
      assertThat(cliqueList).isNotEmpty();
    }
  }

  @Test
  void cliqueWithMinSizeTwoReturnsResults() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.clique('EDGE', 2) YIELD clique, size RETURN clique, size");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
  }

  /**
   * Issue #12173: the search held an n x n neighbour bit matrix (2 MB for these 4,000 nodes) and n-bit sets in every
   * frame. A ring whose every node also links to the node two ahead has the 4,000 triangles {i, i+1, i+2} as its
   * maximal cliques, and they are now found with memory linear in the graph.
   */
  @Test
  void sparseGraphRunsWithinABudgetSmallerThanTheBitMatrix() {
    final int n = 4000;
    database.getSchema().createEdgeType("RING");
    database.transaction(() -> {
      final List<MutableVertex> ring = new ArrayList<>(n);
      for (int i = 0; i < n; i++)
        ring.add(database.newVertex("Node").set("name", "r" + i).save());
      for (int i = 0; i < n; i++) {
        ring.get(i).newEdge("RING", ring.get((i + 1) % n), (Object[]) null).save();
        ring.get(i).newEdge("RING", ring.get((i + 2) % n), (Object[]) null).save();
      }
    });
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1_500_000L);

    final ResultSet rs = database.query("opencypher",
        "CALL algo.clique('RING', 3) YIELD clique, size RETURN clique, size");
    final Set<Integer> firsts = new HashSet<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.<Integer>getProperty("size")).isEqualTo(3);
      final List<Integer> members = new ArrayList<>();
      for (final RID rid : row.<List<RID>>getProperty("clique"))
        members.add(Integer.parseInt(rid.asVertex().getString("name").substring(1)));
      // {i, i+1, i+2} modulo n: the member followed by the other two
      final Integer first = members.stream().filter(m -> members.contains((m + 1) % n) && members.contains((m + 2) % n))
          .findFirst().orElseThrow();
      firsts.add(first);
    }
    assertThat(firsts).hasSize(n);
  }
}
