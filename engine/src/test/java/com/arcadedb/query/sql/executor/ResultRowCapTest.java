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
import com.arcadedb.query.ResultRowCapScope;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12016. The row cap of a caller that serializes a window of the answer (the HTTP endpoints) used to be
 * pushed down as a LIMIT appended to the statement text, and the planner could not tell it from a LIMIT the user wrote: a DISTINCT
 * stayed a serial streaming DISTINCT, a GROUP BY lost its columnar path, and the plan was cached under the rewritten text. The cap is
 * now a bound handed to the engine through {@link ResultRowCapScope}: it reaches the steps that hold rows and stops them at it, and
 * changes nothing about the plan.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ResultRowCapTest extends TestHelper {
  private static final int ROWS = 20_000;
  private static final int CAP  = 11;

  private void createFixture() {
    // SMALL UNITS, SO THE FIXTURE'S PAGES ARE CUT IN MANY OF THEM AND A PARALLEL SCAN HAS SOMETHING TO SPLIT
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createVertexType("T", 4);
    final Random rnd = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("T").set("id", i, "grp", rnd.nextInt(100), "x", rnd.nextDouble()).save();
    });
  }

  /** The rows of the query run under a cap of {@code cap} rows, and the plan that ran. */
  private record Run(List<Result> rows, String plan) {
    List<Object> column(final String name) {
      final List<Object> values = new ArrayList<>(rows.size());
      for (final Result row : rows)
        values.add(row.getProperty(name));
      return values;
    }
  }

  private Run run(final String language, final String sql, final int cap) {
    return run(language, sql, cap, Map.of());
  }

  private Run run(final String language, final String sql, final int cap, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(cap);
        final ResultSet rs = "sqlscript".equals(language) ? database.command(language, sql, params) : database.query(language, sql, params)) {
      while (rs.hasNext())
        rows.add(rs.next());
      return new Run(rows, rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse(""));
    }
  }

  private Run run(final String sql, final int cap) {
    return run("sql", sql, cap);
  }

  private Run runUncapped(final String sql) {
    return run(sql, 0);
  }

  @Test
  void theScopeIsHandedToTheFirstStatementOnly() {
    assertThat(ResultRowCapScope.take()).isZero();
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(5)) {
      assertThat(ResultRowCapScope.take()).isEqualTo(5);
      assertThat(ResultRowCapScope.take()).as("a statement that runs meanwhile finds nothing").isZero();
    }
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(5)) {
      // NO ENGINE TOOK IT: THE CLOSE CLEARS IT, SO THE NEXT REQUEST OF THE THREAD CANNOT FIND IT
    }
    assertThat(ResultRowCapScope.take()).isZero();
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(-3)) {
      assertThat(ResultRowCapScope.take()).as("a negative cap is no cap").isZero();
    }
  }

  @Test
  void orderByKeepsTheRowsOfTheCapOnly() {
    createFixture();
    final List<Object> expected = runUncapped("SELECT FROM T ORDER BY x LIMIT " + CAP).column("id");

    final Run capped = run("SELECT FROM T ORDER BY x", CAP);
    assertThat(capped.plan()).contains("buffer size: " + CAP);
    assertThat(capped.column("id")).isEqualTo(expected);

    final Run uncapped = runUncapped("SELECT FROM T ORDER BY x");
    assertThat(uncapped.rows()).hasSize(ROWS);
    assertThat(uncapped.plan()).doesNotContain("buffer size");
  }

  @Test
  void theSkipIsCountedBeforeTheCap() {
    createFixture();
    final List<Object> expected = runUncapped("SELECT FROM T ORDER BY x SKIP 5 LIMIT " + CAP).column("id");

    final Run capped = run("SELECT FROM T ORDER BY x SKIP 5", CAP);
    assertThat(capped.plan()).contains("buffer size: " + (5 + CAP));
    assertThat(capped.column("id")).isEqualTo(expected);
  }

  @Test
  void aSkipGivenAsAParameterIsKnownToTheSort() {
    createFixture();
    final Run capped = run("sql", "SELECT FROM T ORDER BY x SKIP :s", CAP, Map.of("s", 5));
    assertThat(capped.plan()).contains("buffer size: " + (5 + CAP));
    assertThat(capped.column("id")).isEqualTo(runUncapped("SELECT FROM T ORDER BY x SKIP 5 LIMIT " + CAP).column("id"));
  }

  @Test
  void aCachedAggregationIsNotBoundedByTheSkipOfAnotherExecution() {
    createFixture();
    final String sql = "SELECT grp, count(*) AS c FROM T GROUP BY grp SKIP :s";
    // THE PLAN OF AN AGGREGATION IS CACHED UNDER THE TEXT: A BOUND TAKEN FROM THE SKIP OF THE FIRST EXECUTION WOULD CUT THE NEXT ONE
    assertThat(run("sql", sql, CAP, Map.of("s", 0)).rows()).hasSizeGreaterThanOrEqualTo(CAP);
    assertThat(run("sql", sql, CAP, Map.of("s", 95)).rows()).hasSize(5);
  }

  @Test
  void aMatchSortsOnlyTheRowsOfTheCap() {
    createFixture();
    final List<Object> expected = runUncapped("MATCH {type: T, as: t} RETURN t.id AS id, t.x AS x ORDER BY x LIMIT " + CAP)
        .column("id");
    final Run capped = run("MATCH {type: T, as: t} RETURN t.id AS id, t.x AS x ORDER BY x", CAP);
    assertThat(capped.plan()).contains("buffer size: " + CAP);
    assertThat(capped.column("id")).isEqualTo(expected);
  }

  @Test
  void aStatementWithItsOwnLimitIgnoresTheCap() {
    createFixture();
    final Run run = run("SELECT FROM T ORDER BY x LIMIT 50", CAP);
    assertThat(run.rows()).hasSize(50);
    assertThat(run.plan()).contains("buffer size: 50");
  }

  @Test
  void aStatementThatSortsAfterAnExpandDoesNotTakeTheCap() {
    createFixture();
    // THE SORT SEES THE ROWS BEFORE THE DISTINCT COLLAPSES THEM: BOUNDED, IT WOULD LEAVE FEWER ROWS THAN THE CALLER READS
    final Run distinct = run("SELECT DISTINCT grp FROM T ORDER BY grp", CAP);
    assertThat(distinct.plan()).doesNotContain("buffer size");
    assertThat(distinct.rows()).hasSize(100);
  }

  @Test
  void aDistinctStaysTheParallelGroupByUnderTheCap() {
    createFixture();
    final Set<String> seen = new LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT grp AS v FROM T")) {
      while (rs.hasNext())
        seen.add(String.valueOf(rs.next().<Object>getProperty("v")));
    }
    final List<String> firstOccurrences = new ArrayList<>(seen);

    final Run capped = run("SELECT DISTINCT grp FROM T", CAP);
    assertThat(capped.plan()).contains("CALCULATE AGGREGATE PROJECTIONS (parallel").doesNotContain("+ DISTINCT");
    // THE GROUPS COME OUT IN THE ORDER OF THEIR FIRST ROW, AS THE ROWS OF A DISTINCT DO, AND NOT MORE THAN THE CALLER READS
    assertThat(capped.column("grp").stream().map(String::valueOf).toList()).isEqualTo(firstOccurrences.subList(0, CAP));

    final Run uncapped = runUncapped("SELECT DISTINCT grp FROM T");
    assertThat(uncapped.plan()).contains("CALCULATE AGGREGATE PROJECTIONS (parallel").doesNotContain("+ DISTINCT");
    assertThat(uncapped.rows()).hasSize(100);
  }

  @Test
  void aGroupByHoldsNoMoreGroupsThanTheCap() {
    createFixture();
    final Run capped = run("SELECT grp, count(*) AS c FROM T GROUP BY grp", CAP);
    assertThat(capped.rows()).hasSize(CAP);

    assertThat(runUncapped("SELECT grp, count(*) AS c FROM T GROUP BY grp").rows()).hasSize(100);
  }

  @Test
  void aGroupByThatUnwindsItsGroupsIsNotBoundedByTheGroups() {
    createFixture();
    // AN UNWIND AFTER THE AGGREGATION MAKES THE ROWS OF A GROUP ANY NUMBER, NONE FOR AN EMPTY LIST: THE FIRST GROUPS ARE NOT THE ROWS
    // THE CALLER READS
    final String sql = "SELECT grp, list(id) AS ids FROM T GROUP BY grp UNWIND ids";
    assertThat(run(sql, CAP).rows()).hasSize(ROWS);
  }

  @Test
  void aGroupByWithAnOrderByIsNotBoundedByTheGroups() {
    createFixture();
    // THE SORT NEEDS EVERY GROUP TO KNOW WHICH ARE THE FIRST: ONLY ITS OWN TOP-K IS BOUNDED
    final Run capped = run("SELECT grp, count(*) AS c FROM T GROUP BY grp ORDER BY c DESC, grp", CAP);
    final List<Object> expected = runUncapped("SELECT grp, count(*) AS c FROM T GROUP BY grp ORDER BY c DESC, grp LIMIT " + CAP)
        .column("grp");
    assertThat(capped.column("grp")).isEqualTo(expected);
  }

  @Test
  void aSubqueryDoesNotTakeTheCapOfItsStatement() {
    createFixture();
    // THE SUBQUERY FEEDS THE OUTER STATEMENT: BOUNDED, IT WOULD HAND IT FEWER ROWS THAN THE TYPE HOLDS
    final Run run = run("SELECT FROM (SELECT FROM T ORDER BY x)", CAP);
    assertThat(run.rows()).hasSize(ROWS);
  }

  @Test
  void aStatementThatRunsMeanwhileDoesNotTakeTheCap() {
    createFixture();
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(CAP)) {
      try (final ResultSet first = database.query("sql", "SELECT FROM T ORDER BY x")) {
        // A SUBQUERY, A FUNCTION OR A TRIGGER OF THE FIRST STATEMENT RUNS AFTER ITS ENTRY: IT FINDS NOTHING
        try (final ResultSet nested = database.query("sql", "SELECT FROM T ORDER BY x")) {
          int count = 0;
          while (nested.hasNext()) {
            nested.next();
            count++;
          }
          assertThat(count).isEqualTo(ROWS);
        }
      }
    }
  }

  @Test
  void aScriptBoundsItsLastStatementOnly() {
    createFixture();
    final Run run = run("sqlscript",
        "LET a = SELECT FROM T ORDER BY x; SELECT $a.size() AS n, id FROM T ORDER BY x;", CAP);
    assertThat(run.rows()).hasSize(CAP);
    // THE LET HELD EVERY ROW: THE CAP IS THE LAST STATEMENT'S, WHICH IS THE ONE THE CALLER READS
    for (final Result row : run.rows())
      assertThat(row.<Integer>getProperty("n")).isEqualTo(ROWS);
  }

  @Test
  void aScriptThatEndsWithAStatementOfItsOwnIsLeftAlone() {
    createFixture();
    final Run[] run = new Run[1];
    database.transaction(() -> run[0] = run("sqlscript", "SELECT FROM T ORDER BY x; INSERT INTO T SET id = -1, grp = 0, x = 0.5;", CAP));
    assertThat(run[0].rows()).hasSize(1);
    assertThat(database.countType("T", true)).isEqualTo(ROWS + 1);
  }

  @Test
  void theCachedPlanIsTheOneOfTheStatementWithoutTheCap() {
    createFixture();
    final String sql = "SELECT FROM T ORDER BY x";
    assertThat(run(sql, CAP).rows()).hasSize(CAP);
    // THE PLAN THE CAPPED EXECUTION CACHED IS THE SAME ONE THE NEXT CALLER OF THE TEXT GETS: IT MUST NOT CARRY THE CAP
    final Run uncapped = runUncapped(sql);
    assertThat(uncapped.rows()).hasSize(ROWS);
    assertThat(uncapped.plan()).doesNotContain("buffer size");
    assertThat(run(sql, 3).plan()).contains("buffer size: 3");
  }
}
