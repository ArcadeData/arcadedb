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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #8704 (list-valued {@code edgeTypeNames}/{@code vertexAxisNames} silently dropped), #8705 (DIAGONAL was
 * MANHATTAN, N-axis diagonal component constant zero) and #8706 (two-axis tie-breaker scored the parent, not the node).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8767AstarOptionsAndHeuristicsTest {

  @Test
  void stringArrayAcceptsCollectionsArraysAndTrimsStrings() {
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    assertThat(astar.stringArray(List.of("Road", "Rail"))).containsExactly("Road", "Rail");
    assertThat(astar.stringArray(new Object[] { "x", "y" })).containsExactly("x", "y");
    assertThat(astar.stringArray("Road, Rail")).containsExactly("Road", "Rail");
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
      assertThat(path(db, "astar", v, "{" + common + "vertexAxisNames:['x','y']}"))
          .isEqualTo(path(db, "astar", v, "{" + common + "vertexAxisNames:'x,y'}"));
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
      // octile distance on deltas (3,1): 2 straight + 1 diagonal of cost sqrt(2)
      assertThat(heuristic(db, p, new String[] { "x", "y" }, SQLHeuristicFormula.DIAGONAL, false)).isCloseTo(2 + Math.sqrt(2),
          org.assertj.core.data.Offset.offset(1e-9));
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
              org.assertj.core.data.Offset.offset(1e-9));
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
