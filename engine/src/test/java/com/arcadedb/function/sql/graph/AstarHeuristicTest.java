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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the graph path functions, grouped by feature.
 */
class AstarHeuristicTest {

  /**
   * Regression for #6385: heuristic measured from the current node.
   *
   * The N-axis branch of the A* heuristic stored the SOURCE vertex's coordinate in the map that stands for the CURRENT
   * node's position, so every {@code h(n)} was computed as if {@code n} were the start: constant over the whole search,
   * and no longer an estimate of the remaining distance (issue #6385). The two-axis branch was already correct, which is
   * why the tests below use three axes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AstarHeuristicPosition {

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
                .isCloseTo(twoAxis.getHeuristicCost(points[1], null, points[2], twoAxis.context), Offset.offset(1e-9));
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

  /**
   * Regression for #6414: custom heuristic function.
   *
   * {@code astar}'s {@code customHeuristicFormula} option was documented, listed among the accepted options and read into
   * a field that nothing ever consulted: a query supplying one silently got MANHATTAN, with no error and no warning
   * (issue #6414, item 1). It now names a SQL function that computes h(n), and every way of getting that wrong is an
   * error the caller sees.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AstarCustomHeuristic extends TestHelper {
    private static final String            CUSTOM_FUNCTION = "distanceOnX6414";
    private static final String            NOT_A_NUMBER    = "notANumber6414";
    private static final AtomicInteger     CALLS           = new AtomicInteger();
    private static final List<List<Object>> ARGUMENTS      = new ArrayList<>();

    @BeforeEach
    void registerFunctionsAndGraph() {
      CALLS.set(0);
      ARGUMENTS.clear();

      final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
      engine.getFunctionFactory().register(new SQLFunctionAbstract(CUSTOM_FUNCTION) {
        @Override
        public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
            final Object[] params, final CommandContext context) {
          CALLS.incrementAndGet();
          ARGUMENTS.add(Arrays.asList(params));
          final Vertex current = (Vertex) params[0];
          final Vertex target = (Vertex) params[2];
          return Math.abs(target.getDouble("x") - current.getDouble("x"));
        }

        @Override
        public String getSyntax() {
          return CUSTOM_FUNCTION + "(current, parent, target, source, depth, dFactor)";
        }
      });
      engine.getFunctionFactory().register(new SQLFunctionAbstract(NOT_A_NUMBER) {
        @Override
        public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
            final Object[] params, final CommandContext context) {
          return "not a number";
        }

        @Override
        public String getSyntax() {
          return NOT_A_NUMBER + "()";
        }
      });

      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Node6414");
        database.command("sql", "CREATE PROPERTY Node6414.name STRING");
        database.command("sql", "CREATE PROPERTY Node6414.x DOUBLE");
        database.command("sql", "CREATE EDGE TYPE Road6414");
        database.command("sql", "CREATE PROPERTY Road6414.weight DOUBLE");

        for (int i = 0; i < 4; i++)
          database.command("sql", "INSERT INTO Node6414 SET name = ?, x = ?", "n" + i, (double) i);

        // A cheap three-hop chain and one expensive shortcut, so the answer is only right if the search really runs.
        link("n0", "n1", 1);
        link("n1", "n2", 1);
        link("n2", "n3", 1);
        link("n0", "n3", 10);
      });
    }

    @AfterEach
    void unregisterFunctions() {
      final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
      engine.getFunctionFactory().unregister(CUSTOM_FUNCTION);
      engine.getFunctionFactory().unregister(NOT_A_NUMBER);
    }

    @Test
    void customHeuristicFormulaIsInvokedAndDrivesTheSearch() {
      final List<String> names = pathNames(
          "{'direction':'OUT','customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}");

      assertThat(names).containsExactly("n0", "n1", "n2", "n3");
      // The point of the whole item: the option is applied, not read and dropped.
      assertThat(CALLS.get()).isGreaterThan(0);
    }

    /**
     * A custom formula owns h(n) outright, so it applies with no {@code vertexAxisNames} declared at all - the branch
     * every built-in formula lives in is never reached. The arguments it receives are the contract the syntax states.
     */
    @Test
    void customHeuristicReceivesTheDocumentedArguments() {
      pathNames("{'direction':'OUT','dFactor':2.5,'customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}");

      assertThat(ARGUMENTS).isNotEmpty();
      for (final List<Object> args : ARGUMENTS) {
        assertThat(args).hasSize(6);
        assertThat(args.get(0)).isInstanceOf(Vertex.class);   // current
        assertThat(args.get(2)).isInstanceOf(Vertex.class);   // target
        assertThat(args.get(3)).isInstanceOf(Vertex.class);   // source
        assertThat(args.get(4)).isInstanceOf(Long.class);     // depth
        assertThat(args.get(5)).isEqualTo(2.5);               // dFactor
      }
      // The first call is the source's own estimate, taken before any node has a parent.
      assertThat(ARGUMENTS.getFirst().get(1)).isNull();
    }

    @Test
    void heuristicFormulaCustomWithoutAFunctionNameIsRejected() {
      assertThatThrownBy(() -> pathNames("{'direction':'OUT','heuristicFormula':'CUSTOM'}"))
          .hasMessageContaining("customHeuristicFormula");
    }

    @Test
    void anUnknownFunctionNameIsRejected() {
      assertThatThrownBy(() -> pathNames("{'direction':'OUT','customHeuristicFormula':'noSuchFunction6414'}"))
          .hasMessageContaining("noSuchFunction6414");
    }

    @Test
    void aBuiltInFormulaAndACustomOneTogetherAreAContradiction() {
      assertThatThrownBy(() -> pathNames(
          "{'direction':'OUT','heuristicFormula':'EUCLIDEAN','customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}"))
          .hasMessageContaining("conflict");
    }

    @Test
    void aCustomFormulaThatDoesNotReturnANumberIsRejected() {
      assertThatThrownBy(() -> pathNames("{'direction':'OUT','customHeuristicFormula':'" + NOT_A_NUMBER + "'}"))
          .hasMessageContaining("must return a number");
    }

    /**
     * {@code dijkstra} does not accept the option at all - it never had a heuristic - and says so rather than ignoring it.
     */
    @Test
    void dijkstraStillRejectsTheOptionOutright() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT dijkstra((SELECT FROM Node6414 WHERE name = 'n0'), (SELECT FROM Node6414 WHERE name = 'n3'), 'weight', "
                + "{'direction':'OUT','customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}) AS path")) {
          while (rs.hasNext())
            rs.next();
        }
      }).hasMessageContaining("customHeuristicFormula");
    }

    private List<String> pathNames(final String options) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(astar((SELECT FROM Node6414 WHERE name = 'n0'), (SELECT FROM Node6414 WHERE name = 'n3'), 'weight', "
              + options + ")) AS path")) {
        while (rs.hasNext())
          names.add(rs.next().toElement().getString("name"));
      }
      return names;
    }

    private void link(final String from, final String to, final double weight) {
      database.command("sql",
          "CREATE EDGE Road6414 FROM (SELECT FROM Node6414 WHERE name = ?) TO (SELECT FROM Node6414 WHERE name = ?) SET weight = ?",
          from, to, weight);
    }
  }

  /**
   * Regression for #8767: astar options and heuristics.
   *
   * Issues #8704 (list-valued {@code edgeTypeNames}/{@code vertexAxisNames} silently dropped), #8705 (DIAGONAL was
   * MANHATTAN, N-axis diagonal component constant zero) and #8706 (two-axis tie-breaker scored the parent, not the node).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AstarOptionsAndHeuristics {

    @Test
    void stringArrayAcceptsCollectionsArraysAndTrimsStrings() {
      final SQLFunctionAstar astar = new SQLFunctionAstar();
      assertThat(astar.stringArray(List.of("Road", "Rail"))).containsExactly("Road", "Rail");
      assertThat(astar.stringArray(new Object[] { "x", "y" })).containsExactly("x", "y");
      assertThat(astar.stringArray("Road, Rail")).containsExactly("Road", "Rail");
      assertThat(astar.stringArray(List.of("Road", " Rail "))).containsExactly("Road", "Rail");
      assertThat(astar.stringArray("a,,b")).containsExactly("a", "b");
      assertThat(astar.stringArray("")).isEmpty();
      assertThat(astar.stringArray(null)).isEmpty();
      assertThatThrownBy(() -> astar.stringArray(42)).isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void edgeTypeNamesAsListRestrictsTheWalk() throws Exception {
      TestHelper.executeInNewDatabase("Issue8767EdgeTypes", db -> {
        final RID[] v = new RID[3];
        db.transaction(() -> {
          db.getSchema().createVertexType("V8767");
          db.getSchema().createEdgeType("Road8767");
          db.getSchema().createEdgeType("Rail8767");
          final MutableVertex a = db.newVertex("V8767").set("name", "A").save();
          final MutableVertex b = db.newVertex("V8767").set("name", "B").save();
          final MutableVertex c = db.newVertex("V8767").set("name", "C").save();
          v[0] = a.getIdentity();
          v[1] = b.getIdentity();
          v[2] = c.getIdentity();
          a.newEdge("Road8767", c).set("weight", 1.0).save();
          c.newEdge("Road8767", b).set("weight", 1.0).save();
          a.newEdge("Rail8767", b).set("weight", 0.0).save();
        });

        final List<RID> viaRoad = List.of(v[0], v[2], v[1]);
        assertThat(path(db, "astar", v, "{direction:'OUT', edgeTypeNames:['Road8767']}")).isEqualTo(viaRoad);
        assertThat(path(db, "astar", v, "{direction:'OUT', edgeTypeNames:'Road8767'}")).isEqualTo(viaRoad);
        assertThat(path(db, "dijkstra", v, "{direction:'OUT', edgeTypeNames:['Road8767']}")).isEqualTo(viaRoad);
        // a comma list with a space must allow both types
        assertThat(path(db, "astar", v, "{direction:'OUT', edgeTypeNames:'Road8767, Rail8767'}")).isEqualTo(List.of(v[0], v[1]));
      });
    }

    @Test
    void vertexAxisNamesAsListAppliesTheHeuristic() throws Exception {
      TestHelper.executeInNewDatabase("Issue8767Axes", db -> {
        final RID[] v = new RID[3];
        db.transaction(() -> {
          db.getSchema().createVertexType("P8767");
          db.getSchema().createEdgeType("E8767");
          final MutableVertex a = db.newVertex("P8767").set("x", 0).set("y", 0).save();
          final MutableVertex b = db.newVertex("P8767").set("x", 100).set("y", 0).save();
          final MutableVertex c = db.newVertex("P8767").set("x", 0).set("y", 100).save();
          v[0] = a.getIdentity();
          v[1] = b.getIdentity();
          v[2] = c.getIdentity();
          a.newEdge("E8767", c).set("weight", 1.0).save();
          c.newEdge("E8767", b).set("weight", 1.0).save();
          a.newEdge("E8767", b).set("weight", 5.0).save();
        });

        final String common = "direction:'OUT', heuristicFormula:'MANHATTAN', tieBreaker:false, ";
        // With the heuristic applied, MANHATTAN over coordinates up to 100 against weights of 1 is inadmissible and A*
        // takes the direct A->B edge; with the axes silently dropped (h = 0) it would walk A, C, B.
        final List<RID> direct = List.of(v[0], v[1]);
        assertThat(path(db, "astar", v, "{" + common + "vertexAxisNames:['x','y']}")).isEqualTo(direct);
        assertThat(path(db, "astar", v, "{" + common + "vertexAxisNames:'x,y'}")).isEqualTo(direct);
      });
    }

    @Test
    void diagonalDiffersFromManhattan() throws Exception {
      TestHelper.executeInNewDatabase("Issue8767Diagonal", db -> {
        final Vertex[] p = new Vertex[2];
        db.transaction(() -> {
          db.getSchema().createVertexType("P8767");
          p[0] = db.newVertex("P8767").set("x", 0).set("y", 0).set("z", 0).save();
          p[1] = db.newVertex("P8767").set("x", 3).set("y", 1).set("z", 2).save();
        });

        for (final String[] axes : new String[][] { { "x", "y" }, { "x", "y", "z" } }) {
          final double manhattan = heuristic(db, p, axes, SQLHeuristicFormula.MANHATTAN, false);
          final double diagonal = heuristic(db, p, axes, SQLHeuristicFormula.DIAGONAL, false);
          final double maxAxis = heuristic(db, p, axes, SQLHeuristicFormula.MAXAXIS, false);
          assertThat(diagonal).as("%d axes", axes.length).isLessThan(manhattan - 0.1).isGreaterThan(maxAxis);
        }
        // N-axis octile on deltas (3,1,2): sorted 3>=2>=1 gives (3-2)*1 + (2-1)*sqrt(2) + 1*sqrt(3), scaled by dFactor
        final String[] xyz = { "x", "y", "z" };
        final double expected3 = 1 + Math.sqrt(2) + Math.sqrt(3);
        assertThat(heuristic(db, p, xyz, SQLHeuristicFormula.DIAGONAL, false)).isCloseTo(expected3, Offset.offset(1e-9));
        final SQLFunctionAstar scaled = newAstar(db, p[0], xyz, SQLHeuristicFormula.DIAGONAL, false);
        scaled.paramDFactor = 2.5;
        assertThat(scaled.getHeuristicCost(p[0], null, p[1], scaled.context)).isCloseTo(2.5 * expected3, Offset.offset(1e-9));
        // octile distance on deltas (3,1): 2 straight + 1 diagonal of cost sqrt(2)
        assertThat(heuristic(db, p, new String[] { "x", "y" }, SQLHeuristicFormula.DIAGONAL, false)).isCloseTo(2 + Math.sqrt(2),
            Offset.offset(1e-9));
      });
    }

    @Test
    void diagonalNAxisAgreesWithTwoAxisWhenTheExtraAxisIsFlat() throws Exception {
      TestHelper.executeInNewDatabase("Issue8767DiagonalAgree", db -> {
        final Vertex[] p = new Vertex[2];
        db.transaction(() -> {
          db.getSchema().createVertexType("P8767");
          p[0] = db.newVertex("P8767").set("x", 0).set("y", 0).set("z", 0).save();
          p[1] = db.newVertex("P8767").set("x", 7).set("y", 3).set("z", 0).save();
        });
        assertThat(heuristic(db, p, new String[] { "x", "y", "z" }, SQLHeuristicFormula.DIAGONAL, false))
            .isCloseTo(heuristic(db, p, new String[] { "x", "y" }, SQLHeuristicFormula.DIAGONAL, false),
                Offset.offset(1e-9));
      });
    }

    @Test
    void tieBreakerScoresTheNodeNotItsParent() throws Exception {
      TestHelper.executeInNewDatabase("Issue8767TieBreaker", db -> {
        final Vertex[] p = new Vertex[4];
        db.transaction(() -> {
          db.getSchema().createVertexType("P8767");
          p[0] = db.newVertex("P8767").set("x", 0).set("y", 0).save();   // source
          p[1] = db.newVertex("P8767").set("x", 10).set("y", 0).save();  // goal
          p[2] = db.newVertex("P8767").set("x", 5).set("y", 7).save();   // node, off the start-goal line
          p[3] = db.newVertex("P8767").set("x", 2).set("y", 1).save();   // a parent
        });

        final SQLFunctionAstar astar = newAstar(db, p[0], new String[] { "x", "y" }, SQLHeuristicFormula.MANHATTAN, true);
        final double fromSource = astar.getHeuristicCost(p[2], p[0], p[1], astar.context);
        final double fromOther = astar.getHeuristicCost(p[2], p[3], p[1], astar.context);
        assertThat(fromSource).isEqualTo(fromOther);
        // a direct successor of the source must still be charged for being off the line
        assertThat(fromSource).isGreaterThan(astar.getManhattanHeuristicCost(5, 7, 10, 0, 1.0));
      });
    }

    private static double heuristic(final Database db, final Vertex[] p, final String[] axes, final SQLHeuristicFormula formula,
        final boolean tieBreaker) {
      final SQLFunctionAstar astar = newAstar(db, p[0], axes, formula, tieBreaker);
      return astar.getHeuristicCost(p[0], null, p[1], astar.context);
    }

    private static SQLFunctionAstar newAstar(final Database db, final Vertex source, final String[] axes,
        final SQLHeuristicFormula formula, final boolean tieBreaker) {
      final SQLFunctionAstar astar = new SQLFunctionAstar();
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(db);
      astar.context = ctx;
      astar.paramSourceVertex = source;
      astar.paramVertexAxisNames = axes;
      astar.paramHeuristicFormula = formula;
      astar.paramTieBreaker = tieBreaker;
      return astar;
    }

    private static List<RID> path(final Database db, final String function, final RID[] v, final String options) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = db.query("sql",
          "SELECT " + function + "(" + v[0] + ", " + v[1] + ", 'weight', " + options + ") AS p")) {
        final Result row = rs.next();
        for (final Object each : row.<List<Object>>getProperty("p"))
          rids.add(each instanceof RID rid ? rid : ((Identifiable) each).getIdentity());
      }
      return rids;
    }
  }
}
