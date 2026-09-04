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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7153, two faults that the reporter's query hit together.
 * <p>
 * A FROM target alias was parsed and thrown away, so {@code SELECT FROM Main m WHERE m.code = 'C1'} was planned as a
 * filter on a property literally named {@code m}: no index, a full scan, and no rows at all.
 * <p>
 * And an index search whose leftover condition read a per-record LET variable spliced the LET steps in AHEAD of the
 * index fetch, producing a plan whose first step had nothing to pull from ("Cannot execute a local LET on a query
 * without a target"). Those leftovers now stay in the WHERE clause, which is chained after the LET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7153TargetAliasIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Main");
    database.command("sql", "CREATE PROPERTY Main.code STRING");
    database.command("sql", "CREATE INDEX ON Main (code) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Data");
    database.command("sql", "CREATE PROPERTY Data.code STRING");
    database.command("sql", "CREATE EDGE TYPE relation");

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("sql", "CREATE VERTEX Main SET code = 'C" + i + "'");

      database.command("sql", "CREATE VERTEX Data SET code = 'D1'");
      database.command("sql",
          "CREATE EDGE relation FROM (SELECT FROM Main WHERE code = 'C1') TO (SELECT FROM Data WHERE code = 'D1')");
    });
  }

  @Test
  void aliasQualifiedFilterUsesTheIndex() {
    for (final String query : new String[] { //
        "SELECT FROM Main m WHERE m.code = 'C1'", //
        "SELECT FROM Main AS m WHERE m.code = 'C1'" }) {
      try (final ResultSet rs = database.query("sql", query)) {
        assertThat(plan(rs)).as(query).contains("FETCH FROM INDEX Main[code]");
        assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
        assertThat(rs.hasNext()).as(query).isFalse();
      }
    }
  }

  @Test
  void aliasResolvesInEveryClause() {
    try (final ResultSet rs = database.query("sql",
        "SELECT m.code AS c FROM Main m LET $upper = m.code.toUpperCase() WHERE m.code = 'C1' ORDER BY m.code")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("c")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // A bare alias stands for the record itself, whether it is projected or a record attribute is read off it
    try (final ResultSet rs = database.query("sql", "SELECT m FROM Main m WHERE m.@rid IS NOT NULL AND m.code = 'C2'")) {
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aliasIsNotInheritedByASubQuery() {
    // 'm' inside the sub-query is NOT the outer alias: the sub-query has its own target, and the outer record is
    // reachable only through $parent. Stripping the outer alias there would silently re-point the reference.
    try (final ResultSet rs = database.query("sql",
        "SELECT code FROM Main m WHERE m.code IN (SELECT code FROM Data WHERE code = 'D1') OR m.code = 'C3'")) {
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C3");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aliasWorksOnUpdateAndDelete() {
    database.transaction(() -> {
      database.command("sql", "UPDATE Main m SET touched = true WHERE m.code = 'C4'");
      try (final ResultSet rs = database.query("sql", "SELECT FROM Main WHERE code = 'C4'")) {
        assertThat(rs.next().<Boolean>getProperty("touched")).isTrue();
      }

      database.command("sql", "DELETE FROM Main m WHERE m.code = 'C4'");
      try (final ResultSet rs = database.query("sql", "SELECT FROM Main WHERE code = 'C4'")) {
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * The reported query: the index on {@code Main.code} must survive the second AND term, which can only be decided
   * once the per-record LET has run.
   */
  @Test
  void indexSurvivesAConditionOnAPerRecordLetVariable() {
    final String query = "SELECT FROM Main m LET $relation = out('relation') "//
        + "WHERE m.code = 'C1' AND $relation CONTAINS (code = 'D1')";

    try (final ResultSet rs = database.query("sql", query)) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      assertThat(plan).doesNotContain("FETCH FROM BUCKET");
      // the LET must be chained BEFORE the filter that reads the variable it defines
      assertThat(plan.indexOf("LET (for each record)")).isLessThan(plan.indexOf("FILTER ITEMS WHERE"));

      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // and the deferred filter really filters: C2 has no relation, so the same shape must answer nothing
    try (final ResultSet rs = database.query("sql", "SELECT FROM Main LET $relation = out('relation') "//
        + "WHERE code = 'C2' AND $relation CONTAINS (code = 'D1')")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * OR branches are planned as one indexed sub-plan each, and each one chains the LET ahead of its own residual
   * filter. Pinned here because the deferral above must not disturb it: there is a single WHERE clause to defer
   * into and one residual per branch, so this path keeps filtering inside the sub-plans.
   */
  @Test
  void everyOrBranchKeepsItsIndexAndItsOwnLet() {
    final String query = "SELECT code FROM Main LET $relation = out('relation') "//
        + "WHERE (code = 'C1' AND $relation.size() = 1) OR (code = 'C2' AND $relation.size() = 0)";

    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(plan(rs)).contains("FETCH FROM INDEX Main[code]").doesNotContain("FETCH FROM BUCKET");
      assertThat(rs.stream().map(r -> r.<String>getProperty("code"))).containsExactlyInAnyOrder("C1", "C2");
    }
  }

  /**
   * A type with no records of its own is answered by one indexed sub-plan per sub-type. Each sub-plan would need its
   * own deferred residual and there is a single WHERE clause to hold one, so the statement gives the index up for a
   * plain scan, which evaluates the filter after the LET by construction. Before issue #7153 this shape built a plan
   * whose index fetch pulled from a LET step that had no source, and raised at the first row.
   */
  @Test
  void perRecordLetResidualOnASubTypeFallsBackToTheScan() {
    database.command("sql", "CREATE VERTEX TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.tag STRING");
    database.command("sql", "CREATE VERTEX TYPE Sub1 EXTENDS Base");
    database.command("sql", "CREATE VERTEX TYPE Sub2 EXTENDS Base");
    database.command("sql", "CREATE INDEX ON Sub1 (tag) UNIQUE");
    database.command("sql", "CREATE INDEX ON Sub2 (tag) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.newVertex("Sub1").set("tag", "A" + i).save();
        database.newVertex("Sub2").set("tag", "B" + i).save();
      }
    });

    try (final ResultSet rs = database.query("sql",
        "SELECT tag FROM Base b LET $relation = out('relation') WHERE b.tag = 'A1' AND $relation.size() >= 0")) {
      assertThat(plan(rs)).contains("FETCH FROM TYPE Base");
      assertThat(rs.next().<String>getProperty("tag")).isEqualTo("A1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private static String plan(final ResultSet rs) {
    return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
  }
}
