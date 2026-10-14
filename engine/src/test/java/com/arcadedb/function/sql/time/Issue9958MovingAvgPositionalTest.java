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
package com.arcadedb.function.sql.time;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression for issue #9958: {@code ts.movingAvg} must answer one element per input row (a row without a value keeps its
 * slot), and each window's average must depend only on the members of that window (a large sample that has left the
 * window must not leave a rounding residue behind).
 */
class Issue9958MovingAvgPositionalTest extends TestHelper {

  private void insertSeries(final String type, final Double... values) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
      for (int i = 0; i < values.length; i++)
        database.command("sql", "INSERT INTO " + type + " SET ts = ?, v = ?", (i + 1) * 1000L, values[i]);
    });
  }

  @SuppressWarnings("unchecked")
  private List<Object> single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Object value = rs.next().getProperty("r");
      return (List<Object>) value;
    }
  }

  @Test
  void nullSampleKeepsItsSlot() {
    insertSeries("Gap9958", 10.0, 20.0, null, 40.0, 50.0, 60.0);

    final List<Object> ma = single("SELECT ts.movingAvg(v, 2) AS r FROM Gap9958");
    // window {20,null} averages the value it has; window {null,40} likewise
    assertThat(ma).containsExactly(10.0, 15.0, 20.0, 40.0, 45.0, 55.0);
  }

  @Test
  void windowWithNoValueAnswersNull() {
    insertSeries("AllNull9958", 1.0, null, null, 4.0);

    final List<Object> ma = single("SELECT ts.movingAvg(v, 2) AS r FROM AllNull9958");
    assertThat(ma).containsExactly(1.0, 1.0, null, 4.0);
  }

  @Test
  void leadingNullsKeepTheirSlots() {
    insertSeries("Lead9958", null, null, 3.0, 5.0);

    final List<Object> ma = single("SELECT ts.movingAvg(v, 3) AS r FROM Lead9958");
    assertThat(ma).containsExactly(null, null, 3.0, 4.0);
  }

  @Test
  void largeSampleLeavingTheWindowLeavesNoResidue() {
    insertSeries("Spike9958", 1e16, 1.0, 1.0, 1.0, 1.0);

    final List<Object> ma = single("SELECT ts.movingAvg(v, 2) AS r FROM Spike9958");
    assertThat(ma).containsExactly(1.0e16, 5.0e15, 1.0, 1.0, 1.0);
  }

  @Test
  void largeSampleInTheMiddleDoesNotCorruptLaterWindows() {
    insertSeries("Mid9958", 1.0, 2.0, 3.0, 1e17, 4.0, 5.0, 6.0, 7.0, 8.0);

    final List<Object> ma = single("SELECT ts.movingAvg(v, 3) AS r FROM Mid9958");
    assertThat(ma).hasSize(9);
    // windows that no longer hold the spike: {4,5,6}, {5,6,7}, {6,7,8}
    assertThat(ma.subList(6, 9)).containsExactly(5.0, 6.0, 7.0);
  }

  @Test
  void windowEqualToAndWiderThanTheSeries() {
    // small integers sum exactly in any order, so exact comparison is safe
    insertSeries("Wide9958", 2.0, null, 4.0, 9.0, 5.0);

    // w == n: the single block spans every row
    assertThat(single("SELECT ts.movingAvg(v, 5) AS r FROM Wide9958")).containsExactly(2.0, 2.0, 3.0, 5.0, 5.0);
    // w > n: every window is a prefix of the series
    assertThat(single("SELECT ts.movingAvg(v, 50) AS r FROM Wide9958")).containsExactly(2.0, 2.0, 3.0, 5.0, 5.0);
  }

  @Test
  void allNullSeriesAnswersOneNullPerRow() {
    insertSeries("OnlyNull9958", null, null, null);

    // before #9958 this answered [], now every row keeps its slot and no window holds a value
    assertThat(single("SELECT ts.movingAvg(v, 2) AS r FROM OnlyNull9958")).containsExactly(null, null, null);
  }

  @Test
  void controlSeriesUnchanged() {
    insertSeries("Ctl9958", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0);

    final List<Object> ma = single("SELECT ts.movingAvg(v, 3) AS r FROM Ctl9958");
    assertThat(ma).containsExactly(1.0, 1.5, 2.0, 3.0, 4.0, 5.0);
  }

  /**
   * The rule the issue names for the whole family: every positional {@code ts.*} function answers exactly one element per
   * input row, over a series that contains a null sample.
   */
  @Test
  void everyPositionalFunctionAnswersOneElementPerRow() {
    insertSeries("Family9958", 10.0, 20.0, null, 40.0, 50.0, 60.0);

    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c, ts.movingAvg(v, 2) AS ma, ts.rowNumber(ts) AS rn, ts.lag(v, 1, ts) AS lag, "
            + "ts.lead(v, 1, ts) AS lead, ts.rank(v, ts) AS rank, ts.interpolate(v, 'none', ts) AS ip FROM Family9958")) {
      final Result row = rs.next();
      final long rows = row.<Number>getProperty("c").longValue();
      assertThat(rows).isEqualTo(6L);
      for (final String fn : List.of("ma", "rn", "lag", "lead", "rank", "ip"))
        assertThat(row.<List<?>>getProperty(fn)).as(fn).hasSize((int) rows);
    }
  }

  @Test
  void groupByKeepsSlotsPerGroup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Grp9958 BUCKETS 1");
      database.command("sql", "INSERT INTO Grp9958 SET g = 'a', v = 1.0");
      database.command("sql", "INSERT INTO Grp9958 SET g = 'b', v = 10.0");
      database.command("sql", "INSERT INTO Grp9958 SET g = 'a', v = null");
      database.command("sql", "INSERT INTO Grp9958 SET g = 'b', v = 30.0");
      database.command("sql", "INSERT INTO Grp9958 SET g = 'a', v = 5.0");
    });

    final Map<String, List<Object>> byGroup = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT g, ts.movingAvg(v, 2) AS r FROM Grp9958 GROUP BY g")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        byGroup.put(row.getProperty("g"), row.getProperty("r"));
      }
    }
    assertThat(byGroup.get("a")).containsExactly(1.0, 1.0, 5.0);
    assertThat(byGroup.get("b")).containsExactly(10.0, 20.0);
  }

  /**
   * Each answer must equal the average of its own window's members, recomputed from scratch, over a long series with
   * nulls and occasional spikes. Small integers sum exactly, so a window free of spikes must match exactly.
   */
  @Test
  void matchesPerWindowReferenceOnLongSeries() {
    final Random random = new Random(9958);
    final int n = 500;
    final int w = 7;
    final Double[] values = new Double[n];
    for (int i = 0; i < n; i++) {
      final int pick = random.nextInt(20);
      values[i] = pick == 0 ? null : pick == 1 ? 1e16 : (double) random.nextInt(100);
    }
    insertSeries("Long9958", values);

    final List<Object> ma = single("SELECT ts.movingAvg(v, " + w + ") AS r FROM Long9958");
    assertThat(ma).hasSize(n);

    final List<Double> reference = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      double sum = 0;
      int count = 0;
      for (int j = Math.max(0, i - w + 1); j <= i; j++)
        if (values[j] != null) {
          sum += values[j];
          count++;
        }
      reference.add(count == 0 ? null : sum / count);
    }

    for (int i = 0; i < n; i++) {
      final Double expected = reference.get(i);
      final Object actual = ma.get(i);
      if (expected == null)
        assertThat(actual).as("slot %d", i).isNull();
      else
        assertThat((Double) actual).as("slot %d", i).isCloseTo(expected, within(Math.abs(expected) * 1e-12));
    }
  }
}
