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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8443: a correlated level of a MATCH cartesian product ({@code {as: c, where: (k = $matched.a.k)}}) was
 * planned and executed again for every tuple of the levels before it, even when many tuples hand it the same values.
 * {@link CartesianProductStep} now remembers the rows of such a level per distinct binding of the aliases it reads,
 * with the {@link CorrelatedSubQueryCache} a correlated LET subquery uses (issue #8400).
 * <p>
 * Every test checks every row, because the failure a result cache can introduce is a row answered with another
 * binding's rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CartesianProductStepResultCacheTest extends TestHelper {

  private static final int KEYS             = 3;
  private static final int B_PER_RUN        = 4;
  private static final int C_PER_KEY        = 2;
  private static final int EXPECTED_TUPLES  = KEYS * B_PER_RUN * C_PER_KEY;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("MA");
    database.getSchema().createVertexType("MB");
    database.getSchema().createVertexType("MC");
    database.transaction(() -> {
      for (int k = 1; k <= KEYS; k++) {
        database.command("sql", "create vertex MA set name = 'a" + k + "', k = " + k).close();
        for (int c = 1; c <= C_PER_KEY; c++)
          database.command("sql", "create vertex MC set name = 'c" + k + "_" + c + "', k = " + k).close();
      }
      for (int b = 1; b <= B_PER_RUN; b++)
        database.command("sql", "create vertex MB set name = 'b" + b + "', w = " + (b % 2)).close();
    });
  }

  private static final String LEVEL_READS_ONE_ALIAS =
      "MATCH {type: MB, as: b}, {type: MA, as: a}, {type: MC, as: c, where: (k = $matched.a.k)} RETURN a.name AS a, b.name AS b, c.name AS c";

  @Test
  void levelRunsOncePerDistinctValueOfTheAliasItReads() {
    final ResultSet rs = database.query("sql", LEVEL_READS_ONE_ALIAS);
    final List<String> rows = drain(rs);

    assertThat(rows).hasSize(EXPECTED_TUPLES);
    for (int k = 1; k <= KEYS; k++)
      for (int b = 1; b <= B_PER_RUN; b++)
        for (int c = 1; c <= C_PER_KEY; c++)
          assertThat(rows).contains("a" + k + "|b" + b + "|c" + k + "_" + c);

    final CorrelatedSubQueryCache cache = levelCache(rs);
    assertThat(cache).as("result cache in use").isNotNull();
    assertThat(cache.isDisabled()).isFalse();
    assertThat(cache.getMisses()).as("one execution per distinct a").isEqualTo(KEYS);
    assertThat(cache.getHits()).isEqualTo(KEYS * B_PER_RUN - KEYS);
    rs.close();
  }

  @Test
  void levelReadingTwoAliasesIsKeyedOnBoth() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: MB, as: b}, {type: MA, as: a}, {type: MC, as: c, where: (k = $matched.a.k AND $matched.b.w >= 0)} "
            + "RETURN a.name AS a, b.name AS b, c.name AS c");
    final List<String> rows = drain(rs);
    assertThat(rows).hasSize(EXPECTED_TUPLES);

    final CorrelatedSubQueryCache cache = levelCache(rs);
    assertThat(cache).isNotNull();
    // b.w takes two values over the four b, and the key holds b itself: one run per distinct (a, b)
    assertThat(cache.getMisses()).isEqualTo(KEYS * B_PER_RUN);
    assertThat(cache.getHits()).isZero();
    rs.close();
  }

  @Test
  void aValueThatChangesTheAnswerIsNotServedFromAnotherBinding() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: MB, as: b}, {type: MA, as: a}, {type: MC, as: c, where: (k = $matched.a.k AND $matched.b.w = 1)} "
            + "RETURN a.name AS a, b.name AS b, c.name AS c");
    final List<String> rows = drain(rs);

    // ONLY b1 AND b3 HAVE w = 1
    assertThat(rows).hasSize(KEYS * 2 * C_PER_KEY);
    assertThat(new TreeSet<>(rows)).allSatisfy(row -> assertThat(row).matches("a\\d\\|b[13]\\|c\\d_\\d"));
    rs.close();
  }

  @Test
  void aBareMatchedReadGivesTheLevelTheWholeOuterTupleAndRemembersNothing() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: MB, as: b}, {type: MA, as: a}, {type: MC, as: c, where: (k = $matched.a.k AND $matched IS NOT NULL)} "
            + "RETURN a.name AS a, b.name AS b, c.name AS c");
    final List<String> rows = drain(rs);
    assertThat(rows).hasSize(EXPECTED_TUPLES);
    assertThat(levelCache(rs)).isNull();
    rs.close();
  }

  @Test
  void aNonRepeatableFunctionRemembersNothing() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: MB, as: b}, {type: MA, as: a}, {type: MC, as: c, where: (k = $matched.a.k AND randomInt(1) = 0)} "
            + "RETURN a.name AS a, b.name AS b, c.name AS c");
    final List<String> rows = drain(rs);
    assertThat(rows).hasSize(EXPECTED_TUPLES);
    assertThat(levelCache(rs)).isNull();
    rs.close();
  }

  @Test
  void disabledByConfiguration() {
    final int previous = GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE.getValueAsInteger();
    GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE.setValue(0);
    try {
      final ResultSet rs = database.query("sql", LEVEL_READS_ONE_ALIAS);
      assertThat(drain(rs)).hasSize(EXPECTED_TUPLES);
      assertThat(levelCache(rs)).isNull();
      rs.close();
    } finally {
      GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE.setValue(previous);
    }
  }

  @Test
  void levelThatAnswersNoRowIsRemembered() {
    database.transaction(() -> database.command("sql", "create vertex MA set name = 'lonely', k = 99").close());
    final ResultSet rs = database.query("sql", LEVEL_READS_ONE_ALIAS);
    final List<String> rows = drain(rs);
    assertThat(rows).hasSize(EXPECTED_TUPLES);
    assertThat(rows).noneMatch(row -> row.startsWith("lonely"));
    // a lonely has no c at all: the first b asks, the other three are answered from the cache
    final CorrelatedSubQueryCache cache = levelCache(rs);
    assertThat(cache.getMisses()).isEqualTo(KEYS + 1);
    assertThat(cache.getHits()).isEqualTo((KEYS + 1) * (B_PER_RUN - 1));
    rs.close();
  }

  private static List<String> drain(final ResultSet rs) {
    final List<String> rows = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      rows.add(row.getProperty("a") + "|" + row.getProperty("b") + "|" + row.getProperty("c"));
    }
    return rows;
  }

  /** The one result cache of the plan: the planner orders the levels, so the correlated one is not at a fixed position. */
  private static CorrelatedSubQueryCache levelCache(final ResultSet rs) {
    final CartesianProductStep step = cartesianStep(rs);
    CorrelatedSubQueryCache found = null;
    for (int level = 0; level < 3; level++) {
      final CorrelatedSubQueryCache cache = step.getResultCache(level);
      if (cache != null) {
        assertThat(found).as("at most one remembered level").isNull();
        found = cache;
      }
    }
    return found;
  }

  private static CartesianProductStep cartesianStep(final ResultSet rs) {
    for (final ExecutionStep step : rs.getExecutionPlan().get().getSteps())
      if (step instanceof CartesianProductStep cartesian)
        return cartesian;
    throw new AssertionError("no CARTESIAN PRODUCT step in " + rs.getExecutionPlan().get().prettyPrint(0, 2));
  }
}
