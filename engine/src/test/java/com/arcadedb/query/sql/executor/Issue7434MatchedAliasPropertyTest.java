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
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7434: in a SQL MATCH, reading a property through {@code $matched.<alias>.<property>} answered {@code null}
 * everywhere. In a pattern node's {@code where:} the predicate was false for every candidate, so the statement returned
 * zero rows with no error; in {@code RETURN} the projection was {@code null}. Only the identity comparison
 * {@code $matched.<alias> != $currentMatch} worked.
 * <p>
 * The schema and the six statements mirror the issue's repro table line for line.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7434MatchedAliasPropertyTest extends TestHelper {

  @Override
  public void beginTest() {
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

  @Test
  void identityComparisonControlStillWorks() {
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}<-CONTAINS-{type: F, as: f, where: ($matched.s != $currentMatch)}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  @Test
  void matchedPropertyInConnectedPatternWhere() {
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}<-CONTAINS-{type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  @Test
  void cartesianProductControlStillWorks() {
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f}
        RETURN f.path AS p""")).containsExactlyInAnyOrder("a.ts", "b.ts");
  }

  @Test
  void matchedPropertyInCartesianPatternWhere() {
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  @Test
  void tautologicalMatchedPropertyPredicateKeepsEveryRow() {
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: ($matched.s.id = 's1')}
        RETURN f.path AS p""")).containsExactlyInAnyOrder("a.ts", "b.ts");
  }

  @Test
  void matchedPropertyInReturnProjection() {
    final List<Object> sfp = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f}
        RETURN f.path AS p, $matched.s.filePath AS sfp""")) {
      while (rs.hasNext())
        sfp.add(rs.next().getProperty("sfp"));
    }
    assertThat(sfp).containsExactly("a.ts", "a.ts");
  }

  @Test
  void matchedPropertyThroughAnAliasBoundLaterInTheSchedule() {
    // THE PREDICATE READS THE ALIAS THAT IS NOT THE ROOT OF THE SCHEDULE: THE PLANNER MUST ORDER s BEFORE f EVEN
    // THOUGH f IS LISTED FIRST AND HAS THE SMALLER ESTIMATE ONCE ITS FILTER IS IGNORED
    assertThat(paths("""
        MATCH {type: F, as: f, where: (path = $matched.s.filePath)}, {type: S, as: s, where: (id = 's1')}
        RETURN f.path AS p""")).containsExactly("a.ts");
  }

  @Test
  void correlatedSubPatternWithAHopKeepsTheOuterBindingForEveryCandidate() {
    // THE CORRELATED SUB-PATTERN HAS ITS OWN HOP: THE ROWS ITS MatchStep EMITS MUST NOT SHADOW THE OUTER s WHILE THE
    // ROOT'S FILTER IS STILL BEING EVALUATED FOR THE REMAINING F CANDIDATES
    database.transaction(() -> database.command("sql",
        "CREATE EDGE CONTAINS FROM (SELECT FROM F WHERE path = 'b.ts') TO (SELECT FROM S WHERE id = 's1')"));
    assertThat(paths("""
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: ($matched.s.id = 's1')}-CONTAINS->{type: S, as: t}
        RETURN f.path AS p""")).containsExactlyInAnyOrder("a.ts", "b.ts");
  }

  @Test
  void matchedPropertyInReturnSurvivesANotPatternAndOrderBy() {
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

  @Test
  void circularMatchedDependencyAcrossSubPatternsIsRejected() {
    assertThatThrownBy(() -> paths("""
        MATCH {type: S, as: s, where: (id = $matched.f.path)}, {type: F, as: f, where: (path = $matched.s.filePath)}
        RETURN f.path AS p"""))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("circular dependency");
  }

  @Test
  void correlatedLegIsRunAgainForEveryRowOfAnIndependentLeg() {
    // THREE LEGS, THE CORRELATED ONE LAST: IT MUST BE PLANNED AND RUN AGAIN FOR EVERY ROW OF THE INDEPENDENT LEG g. A
    // RESET OF THE SAME SUB-PLAN IS NOT ENOUGH, SINCE THE FETCH AND FILTER STEPS OF ITS ROOT SELECT DO NOT RESTART: THE
    // SECOND ROW OF g CAME BACK WITH NO f AND THE PRODUCT STOPPED AT ONE ROW
    try (final ResultSet rs = database.query("sql", """
        MATCH {type: S, as: s, where: (id = 's1')}, {type: F, as: f, where: (path = $matched.s.filePath)}, {type: F, as: g}
        RETURN f.path AS p, g.path AS q, $matched.s.filePath AS sfp""")) {
      assertThat(rows(rs)).containsExactlyInAnyOrder("a.ts|a.ts|a.ts", "a.ts|b.ts|a.ts");
    }
  }

  @Test
  void correlatedLegAnsweringNoRowForSomeOuterTuplesSkipsThemAndGoesOn() {
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

  @Test
  void correlatedLegWithSeveralMatchesForALaterOuterTupleKeepsThemAll() {
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

  @Test
  void anIndependentLegWithNoRowEmptiesTheProduct() {
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
}
