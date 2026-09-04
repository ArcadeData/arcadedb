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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.SQLQueryEngine;
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

  /**
   * {@code m} inside a sub-query is NOT the outer alias: the sub-query has its own target, and the outer record is
   * reachable only through {@code $parent}. Stripping the outer alias there would silently re-point the reference at
   * the inner record.
   */
  @Test
  void aliasIsNotInheritedByASubQuery() {
    // Data carries a property literally named like the outer alias, so the two readings are distinguishable: inside
    // the sub-query 'm' must stay that property and not become the sub-query's own record.
    database.transaction(() -> database.command("sql", "UPDATE Data SET m = 'a-property-not-an-alias'"));

    assertThat(rendered("SELECT code FROM Main m WHERE m.code IN (SELECT m FROM Data)"))//
        .contains("SELECT m FROM Data").doesNotContain("SELECT @this FROM Data");

    try (final ResultSet rs = database.query("sql",
        "SELECT m AS inner FROM Data WHERE code IN (SELECT code FROM Main m WHERE m.code = 'C1')")) {
      assertThat(rs.hasNext()).isFalse(); // Data has no 'code' equal to a Main code - the point is that it parses and runs
    }

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
      // the LET must be chained BEFORE the filter that reads the variable it defines. Both markers are asserted
      // present first: indexOf() answers -1 for a missing step, and -1 is less than anything.
      assertThat(plan).contains("LET (for each record)").contains("FILTER ITEMS WHERE");
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
   * OR branches never reach the deferral above: {@code handleTypeAsTargetWithIndexedFunction} claims a multi-block
   * WHERE first and plans one indexed sub-plan per branch, each chaining the LET ahead of its own residual filter -
   * correctly, because that sub-plan is not empty when {@code handleLet()} is handed it. Pinned here because the
   * deferral must not disturb it: there is one residual per branch and a single WHERE clause to defer into, so this
   * path has to keep filtering inside the sub-plans.
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

  /**
   * {@code FromItem.toString()} renders the alias back ({@code Main AS m}), and the hardwired {@code count(*)} plan
   * used to hand that rendering to {@code CountFromTypeStep}, which looks its argument up in the schema.
   */
  @Test
  void countStarOnAnAliasedTargetCountsTheType() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Main m")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(50L);
    }
  }

  /**
   * A nested TRAVERSE or MATCH gets its own alias scope: the enclosing SELECT's alias must not rewrite an identifier
   * that means something else inside them - a property of the TRAVERSE target's own rows, or a MATCH pattern alias.
   * <p>
   * Asserted on the statement the parser built, because that is where the rewrite happens and what it did is visible
   * there without depending on what the nested statement then computes: an inherited alias would have turned the
   * nested {@code m} into {@code @this}.
   */
  @Test
  void aNestedStatementDoesNotInheritTheAlias() {
    assertThat(rendered("SELECT FROM Main m LET $t = (TRAVERSE m FROM Data) WHERE m.code = 'C1'"))//
        .contains("TRAVERSE m FROM Data").doesNotContain("TRAVERSE @this");

    assertThat(rendered("SELECT FROM Main m LET $x = (MATCH {type: Data, as: m} RETURN m.code AS mc) WHERE m.code = 'C1'"))//
        .contains("m.code AS mc").doesNotContain("@this");

    // and the enclosing statement's own references are still resolved
    assertThat(rendered("SELECT FROM Main m LET $t = (TRAVERSE m FROM Data) WHERE m.code = 'C1'"))//
        .contains("WHERE code = 'C1'");
  }

  private String rendered(final String query) {
    return ((SQLQueryEngine) database.getQueryEngine("sql")).parse(query, (DatabaseInternal) database).toString();
  }

  /**
   * A dollar sign inside a string literal is not a LET reference. Matching a bare {@code "$"} would defer the
   * residual - or, on the sub-type path, give the index up altogether - for a condition no LET can influence.
   */
  @Test
  void aDollarInAStringLiteralIsNotALetReference() {
    database.transaction(() -> database.command("sql", "UPDATE Main SET currency = 'US$' WHERE code = 'C1'"));

    final String query = "SELECT code FROM Main m LET $relation = out('relation') WHERE m.code = 'C1' AND currency = 'US$'";
    try (final ResultSet rs = database.query("sql", query)) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      // the residual mentions no LET variable, so it is filtered inside the fetch plan, ahead of the LET step
      assertThat(plan.indexOf("FILTER ITEMS WHERE")).isLessThan(plan.indexOf("LET (for each record)"));
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private static String plan(final ResultSet rs) {
    return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
  }
}
