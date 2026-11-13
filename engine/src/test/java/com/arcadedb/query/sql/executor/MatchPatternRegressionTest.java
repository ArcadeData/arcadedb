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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the planning and execution of MATCH patterns: {@code $matched} reads, NOT patterns, ORDER BY on
 * record attributes, RID-bound nodes with a type, edge types on reverse hops and lazily built WHILE paths.
 * <p>
 * Kept apart from {@link MatchStatementExecutionTest} because that class runs every test inside an open transaction
 * ({@code autoStartTx}) over a shared graph that a MATCH without a type would also see. Each section below builds its
 * own graph through a private fixture method called at the start of each of its tests.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MatchPatternRegressionTest extends TestHelper {

  // ---- #7434: a property of a matched alias in WHERE and RETURN ----
  /*
   * Issue #7434: in a SQL MATCH, reading a property through {@code $matched.<alias>.<property>} answered {@code null}
   * everywhere. In a pattern node's {@code where:} the predicate was false for every candidate, so the statement returned
   * zero rows with no error; in {@code RETURN} the projection was {@code null}. Only the identity comparison
   * {@code $matched.<alias> != $currentMatch} worked.
   * <p>
   * The schema and the six statements mirror the issue's repro table line for line.
   */
  private void matchedAliasGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE F");
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE EDGE TYPE CONTAINS");
      database.command("sql", "INSERT INTO F SET path = 'a.ts'");
      database.command("sql", "INSERT INTO F SET path = 'b.ts'");
      database.command("sql", "INSERT INTO S SET id = 's1', filePath = 'a.ts'");
      database.command("sql",
          "CREATE EDGE CONTAINS FROM (SELECT FROM F WHERE path = 'a.ts') TO (SELECT FROM S WHERE id = 's1')");
    });
  }

  /** Regression test for issue #7434. */
  @Test
  void identityComparisonControlStillWorks() {
    matchedAliasGraph();
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}<-CONTAINS-{type: F, as: f, where: ($matched.s != $currentMatch)}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void matchedPropertyInConnectedPatternWhere() {
    matchedAliasGraph();
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}<-CONTAINS-{type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void cartesianProductControlStillWorks() {
    matchedAliasGraph();
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f}
        RETURN f.path AS p""")).containsExactlyInAnyOrder("a.ts", "b.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void matchedPropertyInCartesianPatternWhere() {
    matchedAliasGraph();
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void tautologicalMatchedPropertyPredicateKeepsEveryRow() {
    matchedAliasGraph();
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: ($matched.s.id = 's1')}
        RETURN f.path AS p""")).containsExactlyInAnyOrder("a.ts", "b.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void matchedPropertyInReturnProjection() {
    matchedAliasGraph();
    final List<Object> sfp = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f}
        RETURN f.path AS p, $matched.s.filePath AS sfp""")) {
      while (rs.hasNext())
        sfp.add(rs.next().getProperty("sfp"));
    }
    assertThat(sfp).containsExactly("a.ts", "a.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void matchedPropertyThroughAnAliasBoundLaterInTheSchedule() {
    matchedAliasGraph();
    // THE PREDICATE READS THE ALIAS THAT IS NOT THE ROOT OF THE SCHEDULE: THE PLANNER MUST ORDER s BEFORE f EVEN
    // THOUGH f IS LISTED FIRST AND HAS THE SMALLER ESTIMATE ONCE ITS FILTER IS IGNORED
    assertThat(paths("""
        MATCH {type: F, as: f, where: (path = $matched.s.filePath)}, {type: S, as: s, where: (id = 's1')}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void correlatedSubPatternWithAHopKeepsTheOuterBindingForEveryCandidate() {
    matchedAliasGraph();
    // THE CORRELATED SUB-PATTERN HAS ITS OWN HOP: THE ROWS ITS MatchStep EMITS MUST NOT SHADOW THE OUTER s WHILE THE
    // ROOT'S FILTER IS STILL BEING EVALUATED FOR THE REMAINING F CANDIDATES
    database.transaction(() -> database.command("sql",
        "CREATE EDGE CONTAINS FROM (SELECT FROM F WHERE path = 'b.ts') TO (SELECT FROM S WHERE id = 's1')"));
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: ($matched.s.id = 's1')}-CONTAINS->{type: S, as: t}
        RETURN f.path AS p""")).containsExactlyInAnyOrder("a.ts", "b.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void matchedPropertyInReturnSurvivesANotPatternAndOrderBy() {
    matchedAliasGraph();
    final List<Object> sfp = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f},
        NOT {as: f}-CONTAINS->{type: S, where: (id = 'none')}
        RETURN f.path AS p, $matched.s.filePath AS sfp ORDER BY p DESC""")) {
      while (rs.hasNext())
        sfp.add(rs.next().getProperty("sfp"));
    }
    assertThat(sfp).containsExactly("a.ts", "a.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void circularMatchedDependencyAcrossSubPatternsIsRejected() {
    matchedAliasGraph();
    assertThatThrownBy(() -> paths("""
        MATCH {type: S, as: s, where: (id = $matched.f.path)}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p"""))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("circular dependency");
  }

  /** Regression test for issue #7434. */
  @Test
  void correlatedLegIsRunAgainForEveryRowOfAnIndependentLeg() {
    matchedAliasGraph();
    // THREE LEGS, THE CORRELATED ONE LAST: IT MUST BE PLANNED AND RUN AGAIN FOR EVERY ROW OF THE INDEPENDENT LEG g. A
    // RESET OF THE SAME SUB-PLAN IS NOT ENOUGH, SINCE THE FETCH AND FILTER STEPS OF ITS ROOT SELECT DO NOT RESTART: THE
    // SECOND ROW OF g CAME BACK WITH NO f AND THE PRODUCT STOPPED AT ONE ROW
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: (path = $matched.s.filePath)}, {type: F, as: g}
        RETURN f.path AS p, g.path AS q, $matched.s.filePath AS sfp""")) {
      assertThat(rows(rs)).containsExactlyInAnyOrder("a.ts|a.ts|a.ts", "a.ts|b.ts|a.ts");
    }
  }

  /** Regression test for issue #7434. */
  @Test
  void correlatedLegAnsweringNoRowForSomeOuterTuplesSkipsThemAndGoesOn() {
    matchedAliasGraph();
    // THE OUTER LEG BINDS s1 (a.ts), s2 (NO SUCH FILE) AND s3 (b.ts) IN THIS ORDER: THE CORRELATED LEG ANSWERS ONE ROW,
    // THEN NONE, THEN ONE AGAIN, WHICH IS WHAT THE BACKTRACK-AND-RETRY LOOP OF THE PRODUCT EXISTS FOR
    database.transaction(() -> {
      database.command("sql", "INSERT INTO S SET id = 's2', filePath = 'none.ts'");
      database.command("sql", "INSERT INTO S SET id = 's3', filePath = 'b.ts'");
    });
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN s.id AS id, f.path AS p""")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add(row.getProperty("id") + "|" + row.getProperty("p"));
      }
    }
    assertThat(out).containsExactlyInAnyOrder("s1|a.ts", "s3|b.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void correlatedLegWithSeveralMatchesForALaterOuterTupleKeepsThemAll() {
    matchedAliasGraph();
    // TWO OUTER TUPLES, TWO MATCHES EACH. THE PRODUCT PREPARES THE NEXT ROW BEFORE HANDING OUT THE CURRENT ONE, AND THE
    // BIND STEP DOWNSTREAM REBINDS $matched TO EVERY ROW IT HANDS OUT: THE SECOND MATCH FOR s2 IS PULLED FROM THE STILL
    // OPEN LEG AFTER THAT REBINDING, SO ITS FILTER MUST NOT SEE THE PREVIOUS ROW'S s
    database.transaction(() -> {
      database.command("sql", "INSERT INTO F SET path = 'a.ts'");
      database.command("sql", "INSERT INTO S SET id = 's2', filePath = 'b.ts'");
      database.command("sql", "INSERT INTO F SET path = 'b.ts'");
    });
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN s.id AS id, f.path AS p""")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add(row.getProperty("id") + "|" + row.getProperty("p"));
      }
    }
    assertThat(out).containsExactlyInAnyOrder("s1|a.ts", "s1|a.ts", "s2|b.ts", "s2|b.ts");
  }

  /** Regression test for issue #7434. */
  @Test
  void anIndependentLegWithNoRowEmptiesTheProduct() {
    matchedAliasGraph();
    // WHETHER THE EMPTY LEG COMES FIRST OR AFTER OTHER LEGS, THE ANSWER IS EMPTY, WITH OR WITHOUT A CORRELATED LEG BEHIND IT
    assertThat(paths("""
        MATCH {type: F, as: g, where: (path = 'none')}, {type: S, as: s, where: (id = 's1')}
        RETURN g.path AS p""")).isEmpty();
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: g, where: (path = 'none')}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p""")).isEmpty();
  }

  private static List<String> rows(final ResultSet rs) {
    final List<String> out = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      out.add(row.getProperty("p") + "|" + row.getProperty("q") + "|" + row.getProperty("sfp"));
    }
    return out;
  }

  private List<Object> paths(final String query) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("p"));
    }
    return out;
  }

  // ---- #9201: a NOT pattern is evaluated again for every row ----
  /*
   * Issue #9201: a SQL MATCH with a NOT pattern kept excluding every row after the first one the NOT pattern matched,
   * because the sub-steps of the NOT filter were reused for each row without being reset.
   */
  private void notPatternGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id LONG");
      database.command("sql", "CREATE EDGE TYPE K");
      for (int i = 0; i < 4; i++)
        database.command("sql", "CREATE VERTEX P SET id = ?", (long) i);
      final int[][] edges = { { 0, 1 }, { 1, 0 }, { 1, 2 }, { 2, 3 } };
      for (final int[] e : edges)
        database.command("sql", "CREATE EDGE K FROM (SELECT FROM P WHERE id = ?) TO (SELECT FROM P WHERE id = ?)", (long) e[0],
            (long) e[1]);
    });
  }

  /** Regression test for issue #9201. */
  @Test
  void excludedRowsDoNotHideLaterRows() {
    notPatternGraph();
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(1,2)", "(2,3)");
  }

  /** Regression test for issue #9201. */
  @Test
  void twoHopNotPatternAndFilteredNotBranchAlsoResetPerRow() {
    notPatternGraph();
    // P0->P1->P2->P3 chain plus 0->1 reciprocal; NOT of a two-hop path back to x keeps only rows without a 2-hop return
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: z}.out('K'){as: x} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(0,1)", "(1,0)", "(1,2)", "(2,3)");
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x, where: (id < 2)} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(1,2)", "(2,3)");
  }

  /** Regression test for issue #9201. */
  @Test
  void bothDirectionAndWhileTraversalsInsideNotPatternResetPerRow() {
    notPatternGraph();
    // 'both' sees every neighbour, so every x with at least one edge has some y: NOT of the same pattern keeps none
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.both('K'){as: x} RETURN x.id AS x, y.id AS y")).isEmpty();
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x, while: (true), maxDepth: 2} RETURN x.id AS x, y.id AS y"))
        .containsExactly("(1,2)", "(2,3)");
  }

  /** Regression test for issue #9201. */
  @Test
  void randomGraphMatchesReciprocalEdgeCount() {
    notPatternGraph();
    database.transaction(() -> database.command("sql", "DELETE FROM P"));
    final int n = 60;
    final Random r = new Random(7);
    final Set<Long> seen = new HashSet<>();
    final List<int[]> edges = new ArrayList<>();
    while (edges.size() < 240) {
      final int a = r.nextInt(n), b = r.nextInt(n);
      if (a != b && seen.add(a * 1000L + b))
        edges.add(new int[] { a, b });
    }
    database.transaction(() -> {
      final RID[] v = new RID[n];
      for (int i = 0; i < n; i++)
        v[i] = database.newVertex("P").set("id", (long) i).save().getIdentity();
      for (final int[] e : edges)
        v[e[0]].asVertex().newEdge("K", v[e[1]]);
    });
    int expected = 0;
    for (final int[] e : edges)
      if (!seen.contains(e[1] * 1000L + e[0]))
        expected++;
    assertThat(pairs("MATCH {type: P, as: x}.out('K'){as: y}, NOT {as: y}.out('K'){as: x} RETURN x.id AS x, y.id AS y"))
        .hasSize(expected);
  }

  private List<String> pairs(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add("(" + row.getProperty("x") + "," + row.getProperty("y") + ")");
      }
    }
    Collections.sort(out);
    return out;
  }

  // ---- #9249: $matched read inside an IN subquery ----
  /*
   * Issue #9249: a MATCH node filter that reads {@code $matched} only inside a nested statement was planned as independent of
   * the aliases it reads, and an IN subquery on a node after a hop ran without the enclosing context.
   */
  private void matchedInSubqueryGraph() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[6];
      for (int i = 0; i < 6; i++)
        v[i] = database.newVertex("P").set("name", "p" + i).set("n", i % 3).set("g", i % 2).save();
      for (int i = 0; i < 6; i++)
        v[i].newEdge("K", v[(i + 1) % 6]);
    });
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /** Regression test for issue #9249. */
  @Test
  void disjointInSubquery() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: (n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(12);
  }

  /** Regression test for issue #9249. */
  @Test
  void disjointWithMatchedReadOutside() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: ($matched.a.g >= 0 AND n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(12);
  }

  /** Regression test for issue #9249. */
  @Test
  void disjointSubquerySizeTested() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: ((SELECT FROM P WHERE g = $matched.a.g).size() > 0)} RETURN count(*) AS c")).isEqualTo(12);
  }

  /** Regression test for issue #9249. */
  @Test
  void oneHopInSubquery() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: (n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(2);
  }

  /** Regression test for issue #9249. */
  @Test
  void oneHopWithMatchedReadOutside() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: ($matched.a.g >= 0 AND n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(2);
  }

  /** Regression test for issue #9249. */
  @Test
  void oneHopSubquerySizeTested() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: ((SELECT FROM P WHERE g = $matched.a.g).size() > 0)} RETURN count(*) AS c")).isEqualTo(2);
  }

  /** Regression test for issue #9249. */
  @Test
  void literalControl() {
    matchedInSubqueryGraph();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: (n IN (SELECT n FROM P WHERE g = 0))} RETURN count(*) AS c")).isEqualTo(12);
  }

  /** Regression test for issue #9249. */
  @Test
  void functionArgumentReadsMatched() {
    matchedInSubqueryGraph();
    // p2 (g 0) and p5 (g 1) are the a nodes; b matches when its g equals the g of a: 3 of the 6 vertices for each
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: (coalesce($matched.a.g, -1) = g)} RETURN count(*) AS c")).isEqualTo(6);
  }

  /** Regression test for issue #9249. */
  @Test
  void oneHopReturnsTheRightRows() {
    matchedInSubqueryGraph();
    // 12 and 2 above: a = p2 (n 2) and p5 (n 2) reach p3 and p0 (n 0), both in the subquery set of their g
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: (n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN b.name AS name ORDER BY name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p0");
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p3");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ---- #9911: ORDER BY a record attribute ----
  /*
   * Issue #9911: in a MATCH whose RETURN builds new rows (projections, {@code $paths}, {@code $patterns}), a bare
   * {@code ORDER BY @rid} sorted on a key that is null for every row, so the stable sort silently kept the pattern order. It is
   * now refused at planning time, while {@code ORDER BY <alias>.@rid}, {@code ORDER BY <projected column>} and
   * {@code ORDER BY @rid} over rows that are records ({@code $elements}) keep working.
   */
  private static final int COUNT = 15;

  private void orderByRecordAttributeGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE R");
      for (int i = 0; i < COUNT; i++) {
        database.command("sql", "CREATE VERTEX S SET n = " + i);
        database.command("sql", "CREATE VERTEX P SET n = " + i);
        database.command("sql", "CREATE EDGE R FROM (SELECT FROM S WHERE n = " + i + ") TO (SELECT FROM P WHERE n = " + i + ")");
      }
    });
  }

  /** Regression test for issue #9911. */
  @Test
  void bareRidOnAProjectionIsRefused() {
    orderByRecordAttributeGraph();
    for (final String direction : new String[] { "", " ASC", " DESC" })
      assertThatThrownBy(() -> database.query("sql",
          "MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY @rid" + direction).close())//
          .isInstanceOf(CommandExecutionException.class)//
          .hasMessageContaining("ORDER BY @rid has no meaning on the rows of this MATCH")//
          .hasMessageContaining("<alias>.@rid");
  }

  /** Regression test for issue #9911. */
  @Test
  void bareTypeOnAProjectionIsRefused() {
    orderByRecordAttributeGraph();
    assertThatThrownBy(() -> database.query("sql", "MATCH {type:S, as:s} RETURN s.n AS n ORDER BY @type").close())//
        .isInstanceOf(CommandExecutionException.class)//
        .hasMessageContaining("ORDER BY @type has no meaning");
  }

  /** Regression test for issue #9911. */
  @Test
  void bareRidOnPathsAndPatternsIsRefused() {
    orderByRecordAttributeGraph();
    assertThatThrownBy(() -> database.query("sql", "MATCH {type:S, as:s}.out('R'){as:p} RETURN $paths ORDER BY @rid").close())//
        .isInstanceOf(CommandExecutionException.class);
    assertThatThrownBy(() -> database.query("sql", "MATCH {type:S, as:s}.out('R'){as:p} RETURN $patterns ORDER BY @rid").close())//
        .isInstanceOf(CommandExecutionException.class);
  }

  /** Regression test for issue #9911. */
  @Test
  void aliasQualifiedRidSortsInBothDirections() {
    orderByRecordAttributeGraph();
    final List<RID> expected = allSRids();
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY s.@rid ASC", "srid"))
        .containsExactlyElementsOf(expected);
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY s.@rid DESC", "srid"))
        .containsExactlyElementsOf(expected.reversed());
  }

  /** Regression test for issue #9911. */
  @Test
  void projectedColumnSortsInBothDirections() {
    orderByRecordAttributeGraph();
    final List<RID> expected = allSRids();
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY srid ASC", "srid"))
        .containsExactlyElementsOf(expected);
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY srid DESC", "srid"))
        .containsExactlyElementsOf(expected.reversed());
  }

  /** Regression test for issue #9911. */
  @Test
  void bareRidOnElementsStillSorts() {
    orderByRecordAttributeGraph();
    final List<RID> expected = allSRids();
    final List<RID> actual = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "MATCH {type:S, as:s} RETURN $elements ORDER BY @rid DESC")) {
      while (rs.hasNext())
        actual.add(rs.next().getIdentity().get());
    }
    assertThat(actual).containsExactlyElementsOf(expected.reversed());
  }

  private List<RID> allSRids() {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM S")) {
      while (rs.hasNext())
        rids.add(rs.next().getProperty("r"));
    }
    rids.sort(Comparator.naturalOrder());
    assertThat(rids).hasSize(COUNT);
    return rids;
  }

  private List<RID> ridColumn(final String query, final String column) {
    final List<RID> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  // ---- #9912: a node bound to a RID keeps its type ----
  /*
   * Issue #9912: a MATCH node with both {@code type:} and {@code rid:} must answer the record at that RID only when it is of
   * that type (polymorphically). The prefetch and the single-node root fetched by RID and dropped the type; the edge-pattern
   * root scanned the type and dropped the RID, so every record of the type became a root once the type was too large to be
   * prefetched.
   * <p>
   * {@code Small} has fewer records than the MATCH prefetch threshold, {@code Big} more, so both root paths are covered.
   */
  private static final int BIG_COUNT = 250;

  private RID small2;
  private RID smallSub;
  private RID big7;
  private RID other0;

  private void ridWithTypeGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Small");
      database.command("sql", "CREATE VERTEX TYPE SmallSub EXTENDS Small");
      database.command("sql", "CREATE VERTEX TYPE Big");
      database.command("sql", "CREATE VERTEX TYPE Other");
      database.command("sql", "CREATE EDGE TYPE R");

      for (int i = 0; i < 5; i++)
        database.command("sql", "CREATE VERTEX Small SET n = " + i);
      database.command("sql", "CREATE VERTEX SmallSub SET n = 100");
      for (int i = 0; i < BIG_COUNT; i++)
        database.command("sql", "CREATE VERTEX Big SET n = " + i);
      for (int i = 0; i < 3; i++)
        database.command("sql", "CREATE VERTEX Other SET n = " + i);

      // EVERY Other POINTS TO EVERY Small, SmallSub AND Big: A ROOT THAT DROPS ITS rid: SEES ALL OF THEM AS NEIGHBOURS
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM Other) TO (SELECT FROM Small)");
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM Other) TO (SELECT FROM Big)");
    });

    small2 = ridOf("Small", 2);
    smallSub = ridOf("SmallSub", 100);
    big7 = ridOf("Big", 7);
    other0 = ridOf("Other", 0);
  }

  /** Regression test for issue #9912. */
  @Test
  void singleNodeRidOfAnotherTypeAnswersNoRows() {
    ridWithTypeGraph();
    assertThat(column("MATCH {type:Other, as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).isEmpty();
    assertThat(column("MATCH {type:Other, as:x, rid:" + big7 + "} RETURN x.n AS n", "n")).isEmpty();
  }

  /** Regression test for issue #9912. */
  @Test
  void singleNodeRidOfTheDeclaredTypeMatches() {
    ridWithTypeGraph();
    assertThat(column("MATCH {type:Small, as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).containsExactly(2);
    assertThat(column("MATCH {type:Big, as:x, rid:" + big7 + "} RETURN x.n AS n", "n")).containsExactly(7);
  }

  /** Regression test for issue #9912. */
  @Test
  void singleNodeRidOfASubtypeMatches() {
    ridWithTypeGraph();
    assertThat(column("MATCH {type:Small, as:x, rid:" + smallSub + "} RETURN x.n AS n", "n")).containsExactly(100);
    // AND NOT THE OTHER WAY AROUND: A Small IS NOT A SmallSub
    assertThat(column("MATCH {type:SmallSub, as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).isEmpty();
  }

  /** Regression test for issue #9912. */
  @Test
  void edgeRootOnASmallTypeHonoursTheType() {
    ridWithTypeGraph();
    // Small IS PREFETCHED: THE PREFETCH USED TO FETCH THE RID AND DROP THE TYPE
    assertThat(column("MATCH {type:Small, as:s, rid:" + small2 + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    assertThat(column("MATCH {type:Other, as:s, rid:" + small2 + "}.in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
  }

  /** Regression test for issue #9912. */
  @Test
  void edgeRootOnABigTypeHonoursTheRid() {
    ridWithTypeGraph();
    // Big IS NOT PREFETCHED: THE ROOT USED TO SCAN THE TYPE AND DROP THE RID, ANSWERING 3 NEIGHBOURS FOR EVERY Big
    assertThat(column("MATCH {type:Big, as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    assertThat(column("MATCH {type:Small, as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
  }

  /** Regression test for issue #9912. */
  @Test
  void edgeRootRidOfASubtypeMatches() {
    ridWithTypeGraph();
    assertThat(column("MATCH {type:Small, as:s, rid:" + smallSub + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
  }

  /** Regression test for issue #9912. */
  @Test
  void parameterizedRidKeepsTheTypeCheck() {
    ridWithTypeGraph();
    final String query = "MATCH {type:Big, as:b, rid: :rid}.in('R'){as:o} RETURN o.n AS n ORDER BY n";
    assertThat(column(query, "n", Map.of("rid", big7))).containsExactly(0, 1, 2);
    // SAME TEXT, SO THE CACHED PLAN: THE TYPE IS CHECKED ON THE RECORD, NOT DECIDED FOR THE FIRST BINDING
    assertThat(column(query, "n", Map.of("rid", small2))).isEmpty();
    assertThat(column(query, "n", Map.of("rid", big7))).containsExactly(0, 1, 2);
  }

  /** Regression test for issue #9912. */
  @Test
  void ridWithABucketOfAnotherTypeAnswersNoRows() {
    ridWithTypeGraph();
    final String smallBucket = database.getSchema().getType("Small").getBuckets(false).getFirst().getName();
    final String bigBucket = database.getSchema().getType("Big").getBuckets(false).getFirst().getName();
    assertThat(column("MATCH {bucket:" + smallBucket + ", as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).containsExactly(2);
    assertThat(column("MATCH {bucket:" + bigBucket + ", as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).isEmpty();
    assertThat(column("MATCH {bucket:" + bigBucket + ", as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    assertThat(column("MATCH {bucket:" + smallBucket + ", as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
  }

  /** Regression test for issue #9912. */
  @Test
  void ridTypeAndWhereAreAllApplied() {
    ridWithTypeGraph();
    // THE TYPE CHECK IS CHAINED AFTER THE where: OF THE SELECT BY RID: BOTH MUST STILL SEE (AND PASS ON) THE RECORD ITSELF
    for (final String root : new String[] { "{type:Big, as:b, rid:%s, where:(n = %d)}", "{type:Small, as:b, rid:%s, where:(n = %d)}" }) {
      final boolean big = root.contains("Big");
      assertThat(column("MATCH " + root.formatted(big7, 7) + " RETURN b.n AS n", "n")).isEqualTo(big ? List.of(7) : List.of());
      assertThat(column("MATCH " + root.formatted(big7, 8) + " RETURN b.n AS n", "n")).isEmpty();
      assertThat(column("MATCH " + root.formatted(big7, 7) + ".in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
          .isEqualTo(big ? List.of(0, 1, 2) : List.of());
      assertThat(column("MATCH " + root.formatted(big7, 8) + ".in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
    }
  }

  /** Regression test for issue #9912. */
  @Test
  void typeAndBucketWithoutRidAreBothApplied() {
    ridWithTypeGraph();
    // THE SAME RULE WITHOUT A rid: THE ROOT SCANNED THE TYPE AND DROPPED THE bucket:, WHILE A TRAVERSAL CHECKED BOTH
    final String smallBucket = database.getSchema().getType("Small").getBuckets(false).getFirst().getName();
    final String subBucket = database.getSchema().getType("SmallSub").getBuckets(false).getFirst().getName();
    assertThat(column("MATCH {type:Small, bucket:" + subBucket + ", as:x} RETURN x.n AS n", "n")).containsExactly(100);
    assertThat(column("MATCH {type:Small, bucket:" + smallBucket + ", as:x} RETURN x.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2, 3, 4);
    assertThat(column("MATCH {type:Small, bucket:" + subBucket + ", as:x}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    final String bigBucket = database.getSchema().getType("Big").getBuckets(false).getFirst().getName();
    assertThat(column("MATCH {type:Big, bucket:" + bigBucket + ", as:x}.in('R'){as:o} RETURN count(*) AS c", "c"))
        .containsExactly(3L * BIG_COUNT);
  }

  /** Regression test for issue #9912. */
  @Test
  void aRidReadingAnotherAliasIsNeverTheRoot() {
    ridWithTypeGraph();
    // THE RID IS ONLY KNOWN ONCE o IS BOUND: AS A ROOT OR A PREFETCH IT WOULD BE RESOLVED AGAINST NOTHING
    database.transaction(() -> database.command("sql", "UPDATE Other SET target = " + small2));
    for (final String type : new String[] { "", "type:Small, " })
      assertThat(column("MATCH {type:Other, as:o}.out('R'){" + type + "as:s, rid:{\"@rid\": $matched.o.target}} "
          + "RETURN o.n AS n ORDER BY n", "n")).containsExactly(0, 1, 2);
  }

  /** Regression test for issue #9912. */
  @Test
  void unknownTypeOrBucketNextToARidIsStillReported() {
    ridWithTypeGraph();
    assertThatThrownBy(() -> column("MATCH {type:NoSuchType, as:x, rid:" + small2 + "} RETURN x.n AS n", "n"))//
        .isInstanceOf(SchemaException.class).hasMessageContaining("Type 'NoSuchType' not defined");
    assertThatThrownBy(() -> column("MATCH {bucket:NoSuchBucket, as:x, rid:" + small2 + "} RETURN x.n AS n", "n"))//
        .isInstanceOf(SchemaException.class).hasMessageContaining("Bucket 'NoSuchBucket' not defined");
  }

  /** Regression test for issue #9912. */
  @Test
  void aRidBoundNodeIsTheRootEvenWhenAnotherNodeIsDeclaredFirst() {
    ridWithTypeGraph();
    // THE RID USED TO BE ESTIMATED AS THE WHOLE TYPE, SO THE PLANNER STARTED FROM Other AND CHECKED THE RID ON EVERY NEIGHBOUR
    final String query = "MATCH {type:Other, as:o}.out('R'){type:Big, as:b, rid:" + big7 + "} RETURN o.n AS n ORDER BY n";
    assertThat(column(query, "n")).containsExactly(0, 1, 2);
    final String plan = database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(plan).contains("+ MATCH      <----\n  {o}.out('R'){b}");

    // A RID-ONLY NODE (NO type:) IS A ROOT CANDIDATE TOO
    final String ridOnly = "MATCH {type:Other, as:o}.out('R'){as:b, rid:" + big7 + "} RETURN o.n AS n ORDER BY n";
    assertThat(column(ridOnly, "n")).containsExactly(0, 1, 2);
    assertThat(database.query("sql", "EXPLAIN " + ridOnly).getExecutionPlan().get().prettyPrint(0, 2))
        .contains("+ MATCH      <----\n  {o}.out('R'){b}");
  }

  /** Regression test for issue #9912. */
  @Test
  void ridRootStillWorksWithoutType() {
    ridWithTypeGraph();
    assertThat(column("MATCH {as:x, rid:" + other0 + "}.out('R'){type:Small, as:s} RETURN s.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2, 3, 4, 100);
  }

  private RID ridOf(final String type, final int n) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE n = ?", n)) {
      return rs.next().getIdentity().get();
    }
  }

  private List<Object> column(final String query, final String column) {
    return column(query, column, Map.of());
  }

  private List<Object> column(final String query, final String column, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  // ---- #10215: the edge type is checked on a reverse hop ----
  /*
   * Issue #10215: a SQL MATCH pattern {@code {type: V, as: a}.outE('R2'){as: e}.inV(){type: A, as: b}} also returned edges that
   * are not {@code R2} when the planner started from the typed end node {@code b} and walked both steps backwards. Reversed,
   * {@code {e}.inV(){b}} became {@code inE()} (any edge type) and {@code {a}.outE('R2'){e}} became {@code outV('R2')}, where the
   * edge type means nothing: no step checked that the edge bound to {@code e} is an {@code R2}.
   * <p>
   * Graph: {@code C1 -R2-> A2} and {@code C1 -R-> A3}, {@code A} and {@code C} sub types of {@code V}.
   */
  private void reverseEdgeTypeGraph() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE VERTEX TYPE A EXTENDS V");
    database.command("sql", "CREATE VERTEX TYPE C EXTENDS V");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE EDGE TYPE R2");
    database.command("sql", "CREATE EDGE TYPE R2Sub EXTENDS R2");
    database.transaction(() -> {
      final MutableVertex c1 = database.newVertex("C").set("id", 1).save();
      final MutableVertex a2 = database.newVertex("A").set("id", 2).save();
      final MutableVertex a3 = database.newVertex("A").set("id", 3).save();
      c1.newEdge("R2", a2).save();
      c1.newEdge("R", a3).save();
    });
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> rows.add(
          r.<Object>getProperty("a") + "-" + r.<Object>getProperty("t") + "->" + r.<Object>getProperty("b")));
    }
    rows.sort(null);
    return rows;
  }

  /** Regression test for issue #10215. */
  @Test
  void aTypedEndNodeDoesNotLetEdgesOfAnotherTypeThrough() {
    reverseEdgeTypeGraph();
    assertThat(rows("MATCH {type: V, as: a}.outE('R2'){as: e}.inV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2");
    assertThat(rows("MATCH {type: V, as: a}.outE('R2'){as: e}.inV(){as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2");
  }

  /**
   * Regression test for issue #10215.
   * <p>
   * The same three shapes with the edge read from its target: {@code inE()} reversed is {@code inV()} with the types dropped.
   */
  @Test
  void theIncomingAndTheBothDirectionsToo() {
    reverseEdgeTypeGraph();
    assertThat(rows("MATCH {type: A, as: b}.inE('R2'){as: e}.outV(){type: C, as: a} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2");
    // bothV() answers BOTH ends of the edge, the one the walk came from included: A2 reaches itself through its own R2 edge
    assertThat(rows("MATCH {type: V, as: a}.bothE('R2'){as: e}.bothV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2", "2-R2->2");
    assertThat(rows("MATCH {type: V, as: a}.bothE('R2'){as: e}.bothV(){as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->1", "1-R2->2", "2-R2->1", "2-R2->2");
  }

  /**
   * Regression test for issue #10215.
   * <p>
   * The edge type is matched polymorphically, as the forward walk does.
   */
  @Test
  void aSubTypeOfTheEdgeTypeStillMatches() {
    reverseEdgeTypeGraph();
    database.transaction(() -> database.command("sql",
        "CREATE EDGE R2Sub FROM (SELECT FROM C WHERE id = 1) TO (SELECT FROM A WHERE id = 3)"));

    assertThat(rows("MATCH {type: V, as: a}.outE('R2'){as: e}.inV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2", "1-R2Sub->3");
  }

  /**
   * Regression test for issue #10215.
   * <p>
   * Several edge types, as the method accepts them.
   */
  @Test
  void severalEdgeTypes() {
    reverseEdgeTypeGraph();
    assertThat(rows("MATCH {type: V, as: a}.outE('R2', 'R'){as: e}.inV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R->3", "1-R2->2");
  }

  // ---- #11501: WHILE paths are built lazily ----
  /*
   * Issue #11501: a SQL MATCH path item with {@code while:} or {@code maxDepth:} built EVERY path into one list before the
   * step returned its first row, so {@code LIMIT} bounded nothing, the command deadline was never checked inside the walk and
   * no heap limit was charged: a 42-vertex "ladder" (2^21 paths from one start) exhausted a 1 GB heap with {@code LIMIT 1}.
   * The walk now streams its paths depth first, holding only the vertices of the current path and their pending
   * neighbours, checks the deadline per vertex reached and charges what it holds to the operation's heap limit.
   * <p>
   * The {@code @Timeout}s are hang detectors, not latency bounds: before the fix these queries ran until the heap was gone.
   */
  /** 2^(LAYERS+1)-1 paths from one start: far more than any test heap can hold as path objects. */
  private static final int LAYERS = 40;
  private static final int HUB_DEGREE = 5_000;
  private static final String TRACKER_PROBE = "issue11501TrackerProbe";

  private void whileLazyPathsGraph() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE EDGE TYPE CE");
    database.command("sql", "CREATE VERTEX TYPE H");
    database.command("sql", "CREATE EDGE TYPE HE");
    database.transaction(() -> {
      // A LADDER: LAYERS + 1 LAYERS OF 2 VERTICES, EVERY VERTEX -E-> BOTH VERTICES OF THE NEXT LAYER
      MutableVertex[] previous = null;
      for (int layer = 0; layer <= LAYERS; layer++) {
        final MutableVertex[] current = new MutableVertex[] {
            database.newVertex("N").set("layer", layer, "k", 0).save(),
            database.newVertex("N").set("layer", layer, "k", 1).save() };
        if (previous != null)
          for (final MutableVertex from : previous)
            for (final MutableVertex to : current)
              from.newEdge("E", to).save();
        previous = current;
      }

      // A 2-VERTEX CYCLE: c0 -CE-> c1 -CE-> c0
      final MutableVertex c0 = database.newVertex("C").set("id", 0).save();
      final MutableVertex c1 = database.newVertex("C").set("id", 1).save();
      c0.newEdge("CE", c1).save();
      c1.newEdge("CE", c0).save();

      // A HUB WITH MANY NEIGHBOURS: ITS FRAME HOLDS THEM ALL, ENOUGH TO RESERVE A SHARE OF THE HEAP BUDGET
      final MutableVertex hub = database.newVertex("H").set("id", 0).save();
      for (int i = 0; i < HUB_DEGREE; i++)
        hub.newEdge("HE", database.newVertex("H").set("id", i + 1).save()).save();
    });
  }

  /** Regression test for issue #11501. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void limitStopsAWhileTraversalBeforeEveryPathIsBuilt() {
    whileLazyPathsGraph();
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){while: (true), as: b} RETURN b.layer AS layer LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      // THE START ITSELF IS THE FIRST PATH (DEPTH 0) OF A WHILE ITEM
      assertThat(rs.next().<Integer>getProperty("layer")).isEqualTo(0);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Regression test for issue #11501. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void limitStopsAMaxDepthTraversalBeforeEveryPathIsBuilt() {
    whileLazyPathsGraph();
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){maxDepth: " + LAYERS
            + ", where: (layer = " + LAYERS + "), as: b} RETURN b.layer AS layer LIMIT 3")) {
      final List<Integer> layers = new ArrayList<>();
      rs.stream().forEach(r -> layers.add(r.getProperty("layer")));
      // THE FIRST DESCENT REACHES THE DEEPEST LAYER AT ONCE: ITS LEAVES ARE SIBLINGS IN WALK ORDER
      assertThat(layers).containsExactly(LAYERS, LAYERS, LAYERS);
    }
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * The filter rejects every vertex, so the walk spends the whole query inside one {@code hasNext()} without emitting a row:
   * only a deadline checked inside the walk can stop it.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void theCommandDeadlineIsCheckedInsideTheWalk() {
    whileLazyPathsGraph();
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 300L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){while: (true), where: (layer < 0), as: b} RETURN b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(TimeoutException.class);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    }
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * A {@code while: (true)} around a cycle has infinitely many paths: {@code LIMIT} takes the first ones in walk order.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aWhileAroundACycleStreamsItsFirstPaths() {
    whileLazyPathsGraph();
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (true), as: b, depthAlias: d} RETURN b.id AS id, d LIMIT 5")) {
      final List<String> rows = new ArrayList<>();
      rs.stream().forEach(r -> rows.add(r.<Integer>getProperty("id") + "@" + r.<Integer>getProperty("d")));
      assertThat(rows).containsExactly("0@0", "1@1", "0@2", "1@3", "0@4");
    }
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * Without a bound, the walk around a cycle only gets deeper: what it holds is charged to the operation's element cap, so the
   * query fails with the usual over-budget error instead of a {@code StackOverflowError} or an {@code OutOfMemoryError}.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anUnboundedWalkAroundACycleFailsOnTheHeapLimit() {
    whileLazyPathsGraph();
    final Object previousCap = database.getConfiguration().getValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (true), where: (id < 0), as: b} RETURN b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previousCap);
    }
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * A walk stopped by {@code LIMIT} still holds its current path and the pending neighbours of the hub: closing the result set
   * gives that heap back to the query's share of the budget, instead of leaving it reserved until the garbage collector finds
   * the abandoned query. Read on the query's own tracker, not on the JVM-wide budget other tests share.
   */
  @Test
  void closingAWalkStoppedByLimitGivesItsHeapBack() {
    whileLazyPathsGraph();
    // THE BUDGET IS ON: THE STACK OF THE WALK IS CHARGED TO IT. TestHelper PUTS THE SETTING BACK AFTER THE TEST
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L << 30);
    final Set<QueryHeapTracker> trackers = ConcurrentHashMap.newKeySet();
    final AtomicLong peakUsed = new AtomicLong();
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(TRACKER_PROBE) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        // EVALUATED AT EVERY VERTEX THE WALK EXPANDS FROM: WHAT THE QUERY HOLDS WHILE THE WALK IS IN FLIGHT
        final QueryHeapTracker tracker = context.getQueryHeapTracker();
        trackers.add(tracker);
        peakUsed.accumulateAndGet(tracker.getUsedBytes(), Math::max);
        return true;
      }

      @Override
      public String getSyntax() {
        return TRACKER_PROBE + "()";
      }
    });
    try {
      try (final ResultSet rs = database.query("sql",
          "MATCH {type: H, where: (id = 0), as: a}.out('HE'){while: (" + TRACKER_PROBE + "()), as: b} RETURN b.id AS id LIMIT 1")) {
        // THE START ITSELF IS THE FIRST PATH: THE WHILE CONDITION WAS TAKEN, NOT DROPPED (A SINGLE HOP WOULD START AT A NEIGHBOUR)
        assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(0);
      }
      assertThat(trackers).hasSize(1);
      // THE CONTROL: THE WALK DID CHARGE THE HUB'S PENDING NEIGHBOURS, SO THE ASSERTIONS BELOW CANNOT PASS ON A WALK THAT NEVER
      // CHARGED ANYTHING
      assertThat(peakUsed.get()).as("the walk held the hub's pending neighbours").isGreaterThanOrEqualTo((long) HUB_DEGREE * 64);
      final QueryHeapTracker tracker = trackers.iterator().next();
      assertThat(tracker.getUsedBytes()).as("closing the ResultSet gave the walk's heap back").isZero();
      assertThat(tracker.getReservedBytes()).as("and its share of the shared budget with it").isZero();
    } finally {
      sql.getFunctionFactory().unregister(TRACKER_PROBE);
    }
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * A {@code where:} or {@code while:} whose parenthesized value is not a comparison - a function call, a bare property - was
   * dropped by the parser: the filter filtered nothing and the while item ran as a single hop. Both are now tested as
   * {@code <value> = true}, as a SQL {@code WHERE fn()} is.
   */
  @Test
  void aWhereOrWhileThatIsAPlainExpressionIsNotDropped() {
    whileLazyPathsGraph();
    database.transaction(() -> database.command("sql", "UPDATE C SET flag = (id = 1)"));

    // where: (property): ONLY c1 HAS flag = true
    assertThat(ids("MATCH {type: C, where: (flag), as: b} RETURN b.id AS id")).containsExactly(1);
    // where: (function call)
    assertThat(ids("MATCH {type: C, where: (ifnull(flag, false)), as: b} RETURN b.id AS id")).containsExactly(1);
    // while: (function call): c0 HAS flag = false, SO ONLY THE START IS RETURNED. A DROPPED WHILE WOULD ANSWER THE SINGLE HOP [1]
    assertThat(ids("MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (ifnull(flag, true)), as: b} RETURN b.id AS id"))
        .containsExactly(0);
    // while: (property): c0 HAS flag = false, SO ONLY THE START IS RETURNED, NOT EXPANDED
    assertThat(ids("MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (flag), as: b} RETURN b.id AS id")).containsExactly(0);
  }

  private List<Integer> ids(final String query) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> ids.add(r.getProperty("id")));
    }
    return ids;
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * A multi-item path {@code .(out(){while: ...})} has to collect the paths of its while item, because the next sub-item
   * starts from each of them: that list is held under the element cap too.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aMultiItemWhileCollectsUnderTheHeapLimit() {
    whileLazyPathsGraph();
    final Object previousCap = database.getConfiguration().getValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "MATCH {type: C, where: (id = 0), as: a}.(out('CE'){while: (true)}){as: b} RETURN b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previousCap);
    }

    // BOUNDED, THE SAME SHAPE ANSWERS: c0 (DEPTH 0), c1 (1), c0 (2)
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: C, where: (id = 0), as: a}.(out('CE'){while: ($depth < 2)}){as: b} RETURN b.id AS id")) {
      final List<Integer> ids = new ArrayList<>();
      rs.stream().forEach(r -> ids.add(r.getProperty("id")));
      assertThat(ids).containsExactly(0, 1, 0);
    }
  }

  /**
   * Regression test for issue #11501.
   * <p>
   * The streamed walk answers exactly what the materialized one did, in the same depth-first pre-order, with the same
   * {@code depthAlias} and {@code pathAlias} values.
   */
  @Test
  void theStreamedWalkKeepsOrderDepthAndPath() {
    whileLazyPathsGraph();
    final String query = "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){while: ($depth < 2), as: b, "
        + "depthAlias: d, pathAlias: p} RETURN b.layer AS layer, b.k AS k, d, p";
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> {
        final List<String> path = new ArrayList<>();
        for (final Object step : r.<List<?>>getProperty("p")) {
          final RID rid = ((Identifiable) step).getIdentity();
          final Result v = database.query("sql", "SELECT layer, k FROM " + rid).next();
          path.add(v.<Integer>getProperty("layer") + "." + v.<Integer>getProperty("k"));
        }
        rows.add(r.<Integer>getProperty("layer") + "." + r.<Integer>getProperty("k") + "@" + r.<Integer>getProperty("d") + path);
      });
    }
    assertThat(rows).containsExactly(//
        "0.0@0[]", //
        "1.1@1[1.1]", "2.1@2[1.1, 2.1]", "2.0@2[1.1, 2.0]", //
        "1.0@1[1.0]", "2.1@2[1.0, 2.1]", "2.0@2[1.0, 2.0]");
  }
}
