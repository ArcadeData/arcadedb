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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9956: a window function orders its rows by the key its answer is defined over. {@code ts.rank} is defined over
 * the VALUE (equal values share a rank, wherever they sit in time), while its output list stays aligned with the
 * timestamp order like {@code ts.lag}/{@code ts.lead}. {@code ts.interpolate} is defined over time: its anchors are the
 * neighbours in TIMESTAMP order, not in arrival order.
 */
class Issue9956WindowFunctionOrderKeyTest extends TestHelper {

  private static final double[] NON_MONOTONE = { 30, 10, 20, 10, 50, 20 };

  @Test
  void rankRanksByValueOnASeriesNotMonotoneInTime() {
    database.command("sql", "CREATE TIMESERIES TYPE Rank9956 TIMESTAMP ts FIELDS (v DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < NON_MONOTONE.length; i++)
        database.command("sql", "INSERT INTO Rank9956 SET ts = ?, v = ?", (i + 1) * 1000L, NON_MONOTONE[i]);
    });

    final Result row = database.query("sql", "SELECT ts.rank(v, ts) AS rnk, ts.rowNumber(ts) AS rn FROM Rank9956").next();
    final List<Integer> rank = row.getProperty("rnk");
    final List<Integer> rowNumber = row.getProperty("rn");

    // the reference RANK() OVER (ORDER BY v), listed in timestamp order
    assertThat(rank).containsExactly(5, 1, 3, 1, 6, 3);
    // the claim, not only the cells: a rank is not a row number, and equal values share a rank however far apart in time
    assertThat(rank).isNotEqualTo(rowNumber);
    assertThat(rank.get(1)).isEqualTo(rank.get(3));
    assertThat(rank.get(2)).isEqualTo(rank.get(5));
  }

  @Test
  void rankDoesNotDependOnArrivalOrder() {
    database.command("sql", "CREATE DOCUMENT TYPE RankArrival9956");
    database.transaction(() -> {
      for (int i = NON_MONOTONE.length - 1; i >= 0; i--)
        database.command("sql", "INSERT INTO RankArrival9956 SET ts = ?, v = ?", (i + 1) * 1000L, NON_MONOTONE[i]);
    });

    final List<Integer> rank = database.query("sql", "SELECT ts.rank(v, ts) AS rnk FROM RankArrival9956").next()
        .getProperty("rnk");
    assertThat(rank).containsExactly(5, 1, 3, 1, 6, 3);
  }

  @Test
  void rankRanksByValueWithinEachGroup() {
    database.command("sql", "CREATE DOCUMENT TYPE RankGroup9956");
    database.transaction(() -> {
      for (int i = 0; i < NON_MONOTONE.length; i++) {
        database.command("sql", "INSERT INTO RankGroup9956 SET g = 'a', ts = ?, v = ?", (i + 1) * 1000L, NON_MONOTONE[i]);
        database.command("sql", "INSERT INTO RankGroup9956 SET g = 'b', ts = ?, v = ?", (i + 1) * 1000L, -NON_MONOTONE[i]);
      }
    });

    final List<Result> groups = database.query("sql", "SELECT g, ts.rank(v, ts) AS rnk FROM RankGroup9956 GROUP BY g ORDER BY g")
        .stream().toList();
    assertThat(groups).hasSize(2);
    assertThat(groups.get(0).<List<Integer>>getProperty("rnk")).containsExactly(5, 1, 3, 1, 6, 3);
    // negated values reverse the order: -50 is the smallest, -10 the largest
    assertThat(groups.get(1).<List<Integer>>getProperty("rnk")).containsExactly(2, 5, 3, 5, 1, 3);
  }

  @Test
  void rankComparesMixedNumericTypesByValue() {
    // schemaless: 10 is an Integer, 10.0 and 2.5 are not, and they must still order (and tie) numerically
    database.command("sql", "CREATE DOCUMENT TYPE RankMixed9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO RankMixed9956 SET ts = 1000, v = 10");
      database.command("sql", "INSERT INTO RankMixed9956 SET ts = 2000, v = 2.5");
      database.command("sql", "INSERT INTO RankMixed9956 SET ts = 3000, v = 10.0");
    });

    final List<Integer> rank = database.query("sql", "SELECT ts.rank(v, ts) AS rnk FROM RankMixed9956").next().getProperty("rnk");
    assertThat(rank).containsExactly(2, 1, 2);
  }

  @Test
  void timestampsOfMixedIntegerWidthsStillOrder() {
    // schemaless: 1000 is stored as an Integer, 5000000000 as a Long. The shared sort orders them by value for every caller
    database.command("sql", "CREATE DOCUMENT TYPE MixedTs9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO MixedTs9956 SET ts = 5000000000, v = 1.0");
      database.command("sql", "INSERT INTO MixedTs9956 SET ts = 1000, v = 2.0");
    });

    final Result row = database.query("sql", "SELECT ts.rank(v, ts) AS rnk, ts.lag(v, 1, ts) AS lg, ts.lead(v, 1, ts) AS ld FROM MixedTs9956")
        .next();
    assertThat(row.<List<Integer>>getProperty("rnk")).containsExactly(2, 1);
    assertThat(toDoubles(row.getProperty("lg"))).containsExactly(null, 2.0);
    assertThat(toDoubles(row.getProperty("ld"))).containsExactly(1.0, null);
  }

  @Test
  void rankOrdersNaNAboveEveryNumber() {
    // Double.compare puts NaN above +Infinity; the comparator must stay a total order or the sort can throw
    database.command("sql", "CREATE DOCUMENT TYPE RankNaN9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO RankNaN9956 SET ts = 1000, v = ?", Double.NaN);
      database.command("sql", "INSERT INTO RankNaN9956 SET ts = 2000, v = 3");
      database.command("sql", "INSERT INTO RankNaN9956 SET ts = 3000, v = ?", Double.NaN);
      database.command("sql", "INSERT INTO RankNaN9956 SET ts = 4000, v = 1.5");
    });

    final List<Integer> rank = database.query("sql", "SELECT ts.rank(v, ts) AS rnk FROM RankNaN9956").next().getProperty("rnk");
    assertThat(rank).containsExactly(3, 2, 3, 1);
  }

  @Test
  void lagKeepsArrivalOrderAmongEqualTimestamps() {
    database.command("sql", "CREATE DOCUMENT TYPE LagTie9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO LagTie9956 SET ts = 2000, v = 'c'");
      database.command("sql", "INSERT INTO LagTie9956 SET ts = 1000, v = 'a'");
      database.command("sql", "INSERT INTO LagTie9956 SET ts = 1000, v = 'b'");
    });

    final List<Object> lag = database.query("sql", "SELECT ts.lag(v, 1, ts) AS lg FROM LagTie9956").next().getProperty("lg");
    assertThat(lag).containsExactly(null, "a", "b");
  }

  @Test
  void rankPutsNullValuesLast() {
    database.command("sql", "CREATE DOCUMENT TYPE RankNull9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO RankNull9956 SET ts = 1000");
      database.command("sql", "INSERT INTO RankNull9956 SET ts = 2000, v = 7");
      database.command("sql", "INSERT INTO RankNull9956 SET ts = 3000, v = 3");
      database.command("sql", "INSERT INTO RankNull9956 SET ts = 4000");
    });

    final List<Integer> rank = database.query("sql", "SELECT ts.rank(v, ts) AS rnk FROM RankNull9956").next().getProperty("rnk");
    assertThat(rank).containsExactly(3, 2, 1, 3);
  }

  @Test
  void rankRefusesValuesThatCannotBeOrdered() {
    database.command("sql", "CREATE DOCUMENT TYPE RankBad9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO RankBad9956 SET ts = 1000, v = 1");
      database.command("sql", "INSERT INTO RankBad9956 SET ts = 2000, v = [1, 2]");
    });

    assertThatThrownBy(() -> database.query("sql", "SELECT ts.rank(v, ts) AS rnk FROM RankBad9956").next())
        .hasMessageContaining("ts.rank").hasMessageContaining("<value>");
  }

  @Test
  void linearInterpolationAnchorsOnTimestampNeighbours() {
    // arrival order 3000, 1000, 5000: in time the null sample sits between 0.0 and 20.0
    database.command("sql", "CREATE DOCUMENT TYPE InterpBracket9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpBracket9956 SET ts = 3000");
      database.command("sql", "INSERT INTO InterpBracket9956 SET ts = 1000, v = 0.0");
      database.command("sql", "INSERT INTO InterpBracket9956 SET ts = 5000, v = 20.0");
    });

    final List<Object> filled = database.query("sql", "SELECT ts.interpolate(v, 'linear', ts) AS f FROM InterpBracket9956").next()
        .getProperty("f");
    assertThat(toDoubles(filled)).containsExactly(0.0, 10.0, 20.0);
  }

  @Test
  void linearInterpolationDoesNotExtrapolateAnUnbracketedSample() {
    // the issue's repro: arrival order 1000, 5000, 2000 with the 5000 sample null. In time it is LAST, so it has no bracket
    database.command("sql", "CREATE DOCUMENT TYPE InterpTail9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpTail9956 SET ts = 1000, v = 0.0");
      database.command("sql", "INSERT INTO InterpTail9956 SET ts = 5000");
      database.command("sql", "INSERT INTO InterpTail9956 SET ts = 2000, v = 10.0");
    });

    final List<Object> filled = database.query("sql", "SELECT ts.interpolate(v, 'linear', ts) AS f FROM InterpTail9956").next()
        .getProperty("f");
    assertThat(filled).hasSize(3);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isEqualTo(10.0);
    assertThat(filled.get(2)).isNull();
  }

  @Test
  void carryForwardFollowsTimestampOrder() {
    // arrival order 2000 (null), 3000, 1000: in time the null follows 5.0, so 'prev' carries 5.0 into it
    database.command("sql", "CREATE DOCUMENT TYPE InterpPrev9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpPrev9956 SET ts = 2000");
      database.command("sql", "INSERT INTO InterpPrev9956 SET ts = 3000, v = 7.0");
      database.command("sql", "INSERT INTO InterpPrev9956 SET ts = 1000, v = 5.0");
    });

    final List<Object> filled = database.query("sql", "SELECT ts.interpolate(v, 'prev', ts) AS f FROM InterpPrev9956").next()
        .getProperty("f");
    assertThat(toDoubles(filled)).containsExactly(5.0, 5.0, 7.0);
  }

  @Test
  void interpolationWithoutTimestampKeepsArrivalOrder() {
    database.command("sql", "CREATE DOCUMENT TYPE InterpNoTs9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpNoTs9956 SET v = 4.0");
      database.command("sql", "INSERT INTO InterpNoTs9956 SET n = 1");
      database.command("sql", "INSERT INTO InterpNoTs9956 SET v = 0.0");
    });

    final List<Object> filled = database.query("sql", "SELECT ts.interpolate(v, 'linear') AS f FROM InterpNoTs9956").next()
        .getProperty("f");
    assertThat(toDoubles(filled)).containsExactly(4.0, 2.0, 0.0);
  }

  @Test
  void linearInterpolationWithSomeRowsUntimestamped() {
    // timestamped rows come first in time order; the untimestamped ones follow in arrival order, and a gap that touches
    // one of them is filled by position
    database.command("sql", "CREATE DOCUMENT TYPE InterpPartial9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpPartial9956 SET v = 12.0");
      database.command("sql", "INSERT INTO InterpPartial9956 SET ts = 3000, v = 6.0");
      database.command("sql", "INSERT INTO InterpPartial9956 SET n = 1");
      database.command("sql", "INSERT INTO InterpPartial9956 SET ts = 2000");
      database.command("sql", "INSERT INTO InterpPartial9956 SET ts = 1000, v = 0.0");
    });

    final List<Object> filled = database.query("sql", "SELECT ts.interpolate(v, 'linear', ts) AS f FROM InterpPartial9956").next()
        .getProperty("f");
    // time order: 1000 -> 0.0, 2000 -> null, 3000 -> 6.0, then arrival order: 12.0, null
    assertThat(toDoubles(filled)).containsExactly(0.0, 3.0, 6.0, 12.0, null);
  }

  @Test
  void linearInterpolationPositionalFallbackAcrossAnUntimestampedGap() {
    database.command("sql", "CREATE DOCUMENT TYPE InterpPartialGap9956");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpPartialGap9956 SET n = 1");
      database.command("sql", "INSERT INTO InterpPartialGap9956 SET v = 9.0");
      database.command("sql", "INSERT INTO InterpPartialGap9956 SET ts = 1000, v = 3.0");
    });

    final List<Object> filled = database.query("sql", "SELECT ts.interpolate(v, 'linear', ts) AS f FROM InterpPartialGap9956").next()
        .getProperty("f");
    // time order: 1000 -> 3.0, then arrival order: null, 9.0. The gap's middle row has no timestamp: filled by position
    assertThat(toDoubles(filled)).containsExactly(3.0, 6.0, 9.0);
  }

  private static List<Double> toDoubles(final List<Object> values) {
    return values.stream().map(v -> v == null ? null : ((Number) v).doubleValue()).toList();
  }
}
