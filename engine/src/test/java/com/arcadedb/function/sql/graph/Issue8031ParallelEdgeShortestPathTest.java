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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8031.
 * <p>
 * ArcadeDB is a multigraph: several edges of the same type may join the same ordered pair of vertices, each with
 * its own weight. {@code SQLFunctionAstar.getNeighborWeightsCSR()} assembled the neighbourhood as a
 * {@code Map<Vertex, Double>} with one entry per NEIGHBOUR and filled it with {@code put()}, so parallel edges
 * overwrote each other and the neighbour ended up priced at whichever edge adjacency happened to list last.
 * {@code dijkstra()} delegates straight to it, so both functions answered a genuinely more expensive path and
 * called it the shortest one - and which path they answered depended on the order the edges had been created in.
 * <p>
 * The graph below is the smallest one that shows it: A-to-B costs 1 over one parallel edge and 100 over the other,
 * while the detour A-to-C-to-B costs 4. The two fixtures differ only in the order the two parallel edges are
 * created, which is exactly what used to decide the answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8031ParallelEdgeShortestPathTest extends TestHelper {

  private RID a;
  private RID b;
  private RID c;

  @Test
  void dijkstraTakesTheCheapestParallelEdgeWhenItWasCreatedFirst() {
    buildGraph(true);
    assertThat(path("dijkstra")).containsExactly(a, b);
  }

  @Test
  void dijkstraTakesTheCheapestParallelEdgeWhenItWasCreatedSecond() {
    buildGraph(false);
    assertThat(path("dijkstra")).containsExactly(a, b);
  }

  @Test
  void astarTakesTheCheapestParallelEdgeWhenItWasCreatedFirst() {
    buildGraph(true);
    assertThat(path("astar")).containsExactly(a, b);
  }

  @Test
  void astarTakesTheCheapestParallelEdgeWhenItWasCreatedSecond() {
    buildGraph(false);
    assertThat(path("astar")).containsExactly(a, b);
  }

  /**
   * The mirror-image half of the same defect: {@code getDistance(node, parent, target)}, which prices the step the
   * heuristic takes, used to {@code break} on the FIRST edge it found to the target rather than on the cheapest.
   * With the expensive parallel edge created first, an A* run that consults it must still not be talked out of the
   * cheap edge.
   */
  @Test
  void theHeuristicStepCostAlsoTakesTheCheapestParallelEdge() {
    buildGraph(false);
    assertThat(path("astar", ", {direction:'OUT', heuristicFormula:'MANHATTAN'}")).containsExactly(a, b);
  }

  /**
   * The edge-type filter has to reach the heuristic step cost too (PR #8093 review).
   * <p>
   * With an {@code edgeTypeNames} option naming only E8031, a second edge type joining the same pair must not be
   * priced into the answer at all. The decoy below is a free (weight 0) F8031 edge straight from A to B: if the
   * heuristic weighed it, it would report the A-to-B step as costing nothing.
   */
  @Test
  void anEdgeOfAnotherTypeIsNotWeighedWhenEdgeTypeNamesExcludesIt() {
    buildGraph(true);
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE F8031");
      database.command("sql", "CREATE PROPERTY F8031.weight DOUBLE");
      // A decoy of a type the search is told to ignore, cheaper than anything it is allowed to walk.
      database.command("sql", "CREATE EDGE F8031 FROM " + a + " TO " + c + " SET weight = 0.0");
    });

    // Restricted to E8031, the cheapest A-to-B route is still the direct parallel edge at cost 1.
    assertThat(path("astar", ", {direction:'OUT', edgeTypeNames:['E8031']}")).containsExactly(a, b);
    assertThat(path("dijkstra", ", {direction:'OUT', edgeTypeNames:['E8031']}")).containsExactly(a, b);
  }

  /**
   * @param cheapFirst whether the weight-1 edge is created before the weight-100 one. The only difference between
   *                   the two fixtures, and the whole of the non-determinism the issue reports.
   */
  private void buildGraph(final boolean cheapFirst) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V8031");
      database.command("sql", "CREATE EDGE TYPE E8031");
      database.command("sql", "CREATE PROPERTY E8031.weight DOUBLE");

      final MutableVertex va = database.newVertex("V8031").set("name", "A").save();
      final MutableVertex vb = database.newVertex("V8031").set("name", "B").save();
      final MutableVertex vc = database.newVertex("V8031").set("name", "C").save();
      a = va.getIdentity();
      b = vb.getIdentity();
      c = vc.getIdentity();

      if (cheapFirst) {
        va.newEdge("E8031", vb).set("weight", 1.0).save();
        va.newEdge("E8031", vb).set("weight", 100.0).save();
      } else {
        va.newEdge("E8031", vb).set("weight", 100.0).save();
        va.newEdge("E8031", vb).set("weight", 1.0).save();
      }

      // The detour: cost 4 in total, so it wins only when the cheap parallel edge is lost.
      va.newEdge("E8031", vc).set("weight", 2.0).save();
      vc.newEdge("E8031", vb).set("weight", 2.0).save();
    });
  }

  private List<RID> path(final String function) {
    return path(function, "");
  }

  private List<RID> path(final String function, final String extraArgs) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT " + function + "(" + a + ", " + b + ", 'weight'" + extraArgs + ") AS p")) {
      final Result row = rs.next();
      for (final Object each : row.<List<Object>>getProperty("p"))
        rids.add(each instanceof RID rid ? rid : ((Identifiable) each).getIdentity());
    }
    return rids;
  }
}
