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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12696: {@code sum()} and {@code avg()} of a DOUBLE answered a different value on almost every run
 * of the same read query over unchanged data, once the type was cut in several parallel scan units. Every worker of the parallel
 * scan aggregates the units it happens to take into one partial, so which values were added together, and in which order,
 * depended on thread timing, and floating-point addition is not associative. The sequential scan answered yet another value.
 * <p>
 * The guarantee: the sum is the exact sum of the values, correctly rounded, so every run, parallel or sequential, SQL or
 * openCypher, answers the same value, and the average is that sum over the count.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12696ParallelDoubleSumTest extends TestHelper {
  private static final int ROWS = 20_000;
  private static final int RUNS = 25;

  private final List<Double> prices = new ArrayList<>();

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO EVEN THE ONE-BUCKET TYPE IS CUT IN SEVERAL OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.command("sql", "CREATE VERTEX TYPE Order8 BUCKETS 8");
    database.command("sql", "CREATE PROPERTY Order8.price DOUBLE");
    database.command("sql", "CREATE VERTEX TYPE Order1 BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Order1.price DOUBLE");
    database.command("sql", "CREATE VERTEX TYPE Cancel4 BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Cancel4.d DOUBLE");

    final Random random = new Random(3);
    for (int i = 0; i < ROWS; i++)
      prices.add(Math.round((0.01 + random.nextDouble() * 99.98) * 100) / 100.0);

    database.transaction(() -> {
      for (final double price : prices) {
        database.newVertex("Order8").set("price", price).save();
        database.newVertex("Order1").set("price", price).save();
      }
      // VALUES THAT CANCEL: A RUNNING double TOTAL LOSES EVERY SMALL VALUE ADDED WHILE A 1e300 IS IN IT
      final double[] cancelling = { 1e300, 0.5, -1e300, 0.25, 1e300, -0.125, -1e300, 2.0 };
      for (int i = 0; i < 4_000; i++)
        database.newVertex("Cancel4").set("d", cancelling[i % cancelling.length]).save();
    });
  }

  @Test
  void sqlSumAndAvgAnswerTheSameValueOnEveryRun() {
    final double exactSum = exactSum(prices);
    for (final String type : new String[] { "Order8", "Order1" }) {
      assertThat(planOf("sql", "SELECT sum(price) AS v FROM " + type)).contains("(parallel");

      assertThat(distinctAnswers("sql", "SELECT sum(price) AS v FROM " + type)).as(type).containsExactly(exactSum);
      assertThat(distinctAnswers("sql", "SELECT avg(price) AS v FROM " + type)).as(type).containsExactly(exactSum / ROWS);

      assertThat(sequentialAnswer("sql", "SELECT sum(price) AS v FROM " + type)).as(type).isEqualTo(exactSum);
      assertThat(sequentialAnswer("sql", "SELECT avg(price) AS v FROM " + type)).as(type).isEqualTo(exactSum / ROWS);
    }
  }

  @Test
  void cypherSumAndAvgAnswerTheSameValueOnEveryRun() {
    final double exactSum = exactSum(prices);
    for (final String type : new String[] { "Order8", "Order1" }) {
      final String sum = "MATCH (o:" + type + ") RETURN sum(o.price) AS v";
      final String avg = "MATCH (o:" + type + ") RETURN avg(o.price) AS v";
      assertThat(planOf("opencypher", sum)).contains("(parallel");

      assertThat(distinctAnswers("opencypher", sum)).as(type).containsExactly(exactSum);
      assertThat(distinctAnswers("opencypher", avg)).as(type).containsExactly(exactSum / ROWS);

      assertThat(sequentialAnswer("opencypher", sum)).as(type).isEqualTo(exactSum);
      assertThat(sequentialAnswer("opencypher", avg)).as(type).isEqualTo(exactSum / ROWS);
    }
  }

  @Test
  void cancellingValuesSumToTheirExactTotal() {
    // 500 TIMES (0.5 + 0.25 - 0.125 + 2.0): THE 1e300 CANCEL OUT
    final double expected = 500 * 2.625;
    assertThat(distinctAnswers("sql", "SELECT sum(d) AS v FROM Cancel4")).containsExactly(expected);
    assertThat(distinctAnswers("opencypher", "MATCH (c:Cancel4) RETURN sum(c.d) AS v")).containsExactly(expected);
    assertThat(distinctAnswers("sql", "SELECT avg(d) AS v FROM Cancel4")).containsExactly(expected / 4_000);
    assertThat(distinctAnswers("opencypher", "MATCH (c:Cancel4) RETURN avg(c.d) AS v")).containsExactly(expected / 4_000);
    assertThat(sequentialAnswer("sql", "SELECT sum(d) AS v FROM Cancel4")).isEqualTo(expected);
  }

  @Test
  void groupedSumsAnswerTheSameValueOnEveryRun() {
    // A GROUP BY MERGES THE WORKERS' GROUPS, NOT ONE TOTAL: THE SAME GUARANTEE PER GROUP
    final String query = "SELECT (price * 100 % 7) AS g, sum(price) AS s, avg(price) AS a FROM Order8 GROUP BY g ORDER BY g";
    final Set<String> answers = new LinkedHashSet<>();
    for (int i = 0; i < RUNS; i++)
      answers.add(rows("sql", query));
    assertThat(answers).hasSize(1);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      assertThat(rows("sql", query)).isEqualTo(answers.iterator().next());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  /** The exact sum of the values, correctly rounded to a double. */
  private static double exactSum(final List<Double> values) {
    BigDecimal sum = BigDecimal.ZERO;
    for (final double value : values)
      sum = sum.add(new BigDecimal(value));
    return sum.doubleValue();
  }

  private Set<Double> distinctAnswers(final String language, final String query) {
    final Set<Double> answers = new LinkedHashSet<>();
    for (int i = 0; i < RUNS; i++)
      try (final ResultSet rs = database.query(language, query)) {
        answers.add(rs.next().<Number>getProperty("v").doubleValue());
      }
    return answers;
  }

  private double sequentialAnswer(final String language, final String query) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try (final ResultSet rs = database.query(language, query)) {
      return rs.next().<Number>getProperty("v").doubleValue();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private String rows(final String language, final String query) {
    final StringBuilder sb = new StringBuilder();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        for (final String p : row.getPropertyNames())
          sb.append(p).append('=').append(row.<Object>getProperty(p)).append(';');
        sb.append('\n');
      }
    }
    return sb.toString();
  }

  private String planOf(final String language, final String query) {
    try (final ResultSet rs = database.query(language, "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
