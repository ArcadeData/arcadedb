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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12993: the openCypher {@code sum()} of integers whose total leaves the 64-bit range answered a
 * BIGDECIMAL, a type Cypher does not have (one operation later an inexact FLOAT), where {@code +}, {@code -}, {@code *},
 * {@code reduce()} and {@code abs()} raise {@code long overflow}. With a FLOAT among the values the type of the answer depended
 * on the order of the rows: FLOAT when the float came before the integer total overflowed, BIGDECIMAL after.
 * <p>
 * The guarantee: an all-integer total out of the 64-bit range is the {@code long overflow} ArithmeticError; a total with a FLOAT
 * in it is the FLOAT of the exact sum; an intermediate overflow that later comes back in range is the exact INTEGER. None of the
 * three depends on the order of the rows nor on how a parallel scan splits them. SQL {@code sum()} keeps widening to DECIMAL.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherSumLongOverflowIssue12993Test extends TestHelper {
  private static final long TWO_62 = 1L << 62;
  private static final long TWO_60 = 1L << 60;
  private static final int  ROWS   = 4_000;
  private static final int  RUNS   = 10;

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO THE TYPES ARE CUT IN SEVERAL PARALLEL SCAN UNITS AND THE PARTIAL SUMS ARE MERGED
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.command("sql", "CREATE VERTEX TYPE Big8 BUCKETS 8");
    database.command("sql", "CREATE VERTEX TYPE Cancel8 BUCKETS 8");
    database.command("sql", "CREATE VERTEX TYPE Mixed8 BUCKETS 8");

    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        database.newVertex("Big8").set("n", TWO_60).save();
        database.newVertex("Mixed8").set("n", TWO_60).save();
        // MAX + MAX + MIN + MIN = -2: EVERY PARTIAL AND EVERY ORDER OVERFLOWS ON THE WAY, THE TOTAL IS -2000
        final long[] cancelling = { Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };
        database.newVertex("Cancel8").set("n", cancelling[i % cancelling.length]).save();
      }
      database.newVertex("Mixed8").set("n", 0.5).save();
    });
  }

  @Test
  void anAllIntegerTotalPastTheLongRangeIsALongOverflow() {
    for (final String list : new String[] { "[9223372036854775807, 1]", "[1, 9223372036854775807]", "[-9223372036854775808, -1]",
        "[-1, -9223372036854775808]", "[4611686018427387904, 4611686018427387904]" })
      assertThatThrownBy(() -> single("UNWIND " + list + " AS x RETURN sum(x) AS v"))//
          .as(list)//
          .isInstanceOf(ArithmeticErrorException.class)//
          .hasMessageContaining("long overflow");
  }

  @Test
  void theOverflowIsTheOneOfTheArithmeticOperators() {
    assertThatThrownBy(() -> single("RETURN 9223372036854775807 + 1 AS v")).isInstanceOf(ArithmeticErrorException.class)
        .hasMessageContaining("long overflow");
    assertThatThrownBy(() -> single("UNWIND [9223372036854775807, 1] AS x RETURN sum(x) + 1 AS v")).isInstanceOf(
        ArithmeticErrorException.class).hasMessageContaining("long overflow");
    assertThatThrownBy(() -> single("UNWIND [9223372036854775807, 1, 1] AS x RETURN sum(DISTINCT x) AS v")).isInstanceOf(
        ArithmeticErrorException.class).hasMessageContaining("long overflow");
  }

  @Test
  void aFloatAmongTheValuesIsAFloatWhateverTheOrder() {
    final Set<Object> answers = new LinkedHashSet<>();
    final Set<Object> types = new LinkedHashSet<>();
    for (final String list : new String[] { "[0.5, 4611686018427387904, 4611686018427387904]",
        "[4611686018427387904, 0.5, 4611686018427387904]", "[4611686018427387904, 4611686018427387904, 0.5]" }) {
      answers.add(single("UNWIND " + list + " AS x RETURN sum(x) AS v"));
      types.add(single("UNWIND " + list + " AS x RETURN valueType(sum(x)) AS v"));
    }
    assertThat(answers).containsExactly(9.223372036854776E18);
    assertThat(types).containsExactly("FLOAT NOT NULL");
  }

  @Test
  void anOverflowOnTheWayThatComesBackInRangeIsTheExactInteger() {
    for (final String list : new String[] { "[9223372036854775807, 1, -1]", "[1, 9223372036854775807, -1]", "[-1, 1, 9223372036854775807]",
        "[-9223372036854775808, -1, 1]" }) {
      final Object expected = list.contains("-9223372036854775808") ? Long.MIN_VALUE : Long.MAX_VALUE;
      assertThat(single("UNWIND " + list + " AS x RETURN sum(x) AS v")).as(list).isEqualTo(expected);
    }
    assertThat(single("UNWIND [9223372036854775807, 1, -1] AS x RETURN valueType(sum(x)) AS v")).isEqualTo("INTEGER NOT NULL");
  }

  @Test
  void integerSumsInRangeAreUnchanged() {
    assertThat(single("UNWIND [1, 2, 3] AS x RETURN sum(x) AS v")).isEqualTo(6L);
    assertThat(single("UNWIND [1, 2.5] AS x RETURN sum(x) AS v")).isEqualTo(3.5);
    assertThat(single("UNWIND [] AS x RETURN sum(x) AS v")).isEqualTo(0);
    assertThat(single("UNWIND [9223372036854775806, 1] AS x RETURN sum(x) AS v")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void theParallelScanAnswersTheSameAsTheSequentialOne() {
    final String big = "MATCH (b:Big8) RETURN sum(b.n) AS v";
    final String cancel = "MATCH (c:Cancel8) RETURN sum(c.n) AS v";
    final String mixed = "MATCH (m:Mixed8) RETURN sum(m.n) AS v";
    // PROFILE RUNS THE QUERY, AND THE ONE OVER Big8 FAILS: ITS PLAN IS THE ONE OF THE OTHER TWO
    assertThat(planOf(cancel)).contains("(parallel");
    assertThat(planOf(mixed)).contains("(parallel");

    for (final boolean parallel : new boolean[] { true, false }) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
      try {
        for (int i = 0; i < RUNS; i++) {
          assertThatThrownBy(() -> single(big)).isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");
          assertThat(single(cancel)).isEqualTo(-2L * (ROWS / 4));
          assertThat(single(mixed)).isEqualTo(ROWS * (double) TWO_60);
        }
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    }
  }

  @Test
  void sqlSumKeepsWideningToDecimal() {
    try (final ResultSet rs = database.query("sql", "SELECT sum(n) AS v FROM Big8")) {
      assertThat(rs.next().<Object>getProperty("v")).isEqualTo(BigDecimal.valueOf(TWO_60).multiply(BigDecimal.valueOf(ROWS)));
    }
    // AN EXPLICIT sql.sum() FROM CYPHER IS THE SQL FUNCTION
    assertThat(single("UNWIND [9223372036854775807, 1] AS x RETURN sql.sum(x) AS v")).isEqualTo(
        new BigDecimal("9223372036854775808"));
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }

  private String planOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
