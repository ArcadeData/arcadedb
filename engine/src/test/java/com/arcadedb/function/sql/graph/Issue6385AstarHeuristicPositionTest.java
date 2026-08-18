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
import com.arcadedb.database.Database;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The N-axis branch of the A* heuristic stored the SOURCE vertex's coordinate in the map that stands for the CURRENT
 * node's position, so every {@code h(n)} was computed as if {@code n} were the start: constant over the whole search,
 * and no longer an estimate of the remaining distance (issue #6385). The two-axis branch was already correct, which is
 * why the tests below use three axes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6385AstarHeuristicPositionTest {

  @Test
  void heuristicMeasuresFromTheCurrentNodeNotFromTheSource() throws Exception {
    TestHelper.executeInNewDatabase("Issue6385AstarHeuristic", db -> {
      final Vertex[] points = new Vertex[4];
      db.transaction(() -> {
        db.getSchema().createVertexType("Point6385");
        points[0] = newPoint(db, 0, 0, 0);   // source
        points[1] = newPoint(db, 5, 0, 0);   // halfway
        points[2] = newPoint(db, 9, 0, 0);   // near the goal
        points[3] = newPoint(db, 10, 0, 0);  // goal
      });

      final Vertex source = points[0];
      final Vertex goal = points[3];

      for (final SQLHeuristicFormula formula : axisFormulas()) {
        final SQLFunctionAstar astar = axisHeuristic(db, source, formula);

        final double atSource = astar.getHeuristicCost(source, null, goal, astar.context);
        final double atHalfway = astar.getHeuristicCost(points[1], null, goal, astar.context);
        final double atNearGoal = astar.getHeuristicCost(points[2], null, goal, astar.context);
        final double atGoal = astar.getHeuristicCost(goal, null, goal, astar.context);

        // A node closer to the target must have a strictly smaller estimated remaining cost. With the source
        // coordinate standing in for the node's own, all four were the same number.
        assertThat(atSource).as("%s: h(source) > h(halfway)", formula).isGreaterThan(atHalfway);
        assertThat(atHalfway).as("%s: h(halfway) > h(nearGoal)", formula).isGreaterThan(atNearGoal);
        assertThat(atGoal).as("%s: h(goal) is zero", formula).isZero();
      }
    });
  }

  /**
   * The heuristic has to agree with the two-axis branch, which never had the defect: the same points, measured over
   * the first two of three axes (the third held at zero everywhere), must produce the same estimate whichever branch
   * computes it.
   */
  @Test
  void nAxisBranchAgreesWithTheTwoAxisBranch() throws Exception {
    TestHelper.executeInNewDatabase("Issue6385AstarAxisAgreement", db -> {
      final Vertex[] points = new Vertex[3];
      db.transaction(() -> {
        db.getSchema().createVertexType("Point6385");
        points[0] = newPoint(db, 1, 2, 0);  // source
        points[1] = newPoint(db, 7, 3, 0);  // node
        points[2] = newPoint(db, 10, 8, 0); // goal
      });

      // A non-default dFactor as well as the default: it scales every heuristic, and running only at 1.0 cannot
      // see a branch that drops it - which is how the N-axis EUCLIDEAN came to be the one of five that did.
      for (final double dFactor : new double[] { 1.0, 3.0 })
        for (final SQLHeuristicFormula formula : axisFormulas()) {
          final SQLFunctionAstar threeAxis = axisHeuristic(db, points[0], formula);
          final SQLFunctionAstar twoAxis = axisHeuristic(db, points[0], formula);
          twoAxis.paramVertexAxisNames = new String[] { "x", "y" };
          threeAxis.paramDFactor = dFactor;
          twoAxis.paramDFactor = dFactor;

          assertThat(threeAxis.getHeuristicCost(points[1], null, points[2], threeAxis.context))
              .as("%s at dFactor %s", formula, dFactor)
              .isEqualTo(twoAxis.getHeuristicCost(points[1], null, points[2], twoAxis.context));
        }
    });
  }

  /**
   * Every formula that is computed FROM THE AXES. CUSTOM is not one of them: it delegates h(n) wholesale to a
   * user function and reads no coordinate, so the distance assertions below say nothing about it (issue #6414).
   */
  private static SQLHeuristicFormula[] axisFormulas() {
    return new SQLHeuristicFormula[] { SQLHeuristicFormula.MANHATTAN, SQLHeuristicFormula.MAXAXIS,
        SQLHeuristicFormula.DIAGONAL, SQLHeuristicFormula.EUCLIDEAN, SQLHeuristicFormula.EUCLIDEANNOSQR };
  }

  private SQLFunctionAstar axisHeuristic(final Database db, final Vertex source, final SQLHeuristicFormula formula) {
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setDatabase(db);
    astar.context = ctx;
    astar.paramSourceVertex = source;
    astar.paramVertexAxisNames = new String[] { "x", "y", "z" };
    astar.paramHeuristicFormula = formula;
    // The tie breaker perturbs the estimate with the parent/source cross product: off, so the assertions are about
    // the heuristic itself.
    astar.paramTieBreaker = false;
    return astar;
  }

  private Vertex newPoint(final Database db, final double x, final double y, final double z) {
    return db.newVertex("Point6385").set("x", x).set("y", y).set("z", z).save();
  }
}
