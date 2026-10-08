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
package com.arcadedb.function.sql.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9496: {@code sum()} and {@code avg()} keep their running total unboxed while the inputs are Double, Long or
 * Integer. The total must stay exactly what folding the inputs with {@link Type#increment(Number, Number)} gives, value
 * and type, including the int to long widening, the long overflow to BigDecimal, a first -0.0, and every other input type
 * falling back to the boxed path.
 */
class Issue9496NumericSumTest {

  /** The total as the boxed accumulator built it before #9496. */
  private static Number reference(final List<Number> values) {
    Number sum = null;
    for (final Number v : values)
      sum = sum == null ? v : Type.increment(sum, v);
    return sum;
  }

  private static Number unboxed(final List<Number> values) {
    final NumericSum sum = new NumericSum();
    for (final Number v : values)
      sum.add(v);
    return sum.get();
  }

  private static void assertSame(final Number... values) {
    final List<Number> list = List.of(values);
    final Number expected = reference(list);
    final Number actual = unboxed(list);
    assertThat(actual).as("sum of %s", list).isEqualTo(expected);
    if (expected != null)
      assertThat(actual.getClass()).as("type of the sum of %s", list).isEqualTo(expected.getClass());
  }

  @Test
  void typesAndValuesMatchTheBoxedFold() {
    assertSame(1.5, 2.25, -3.0);
    assertSame(-0.0);
    assertSame(-0.0, -0.0);
    assertSame(1L, 2L, 3L);
    assertSame(1, 2, 3);
    assertSame(Integer.MAX_VALUE, 1);
    assertSame(Integer.MAX_VALUE, 1, -5);
    assertSame(Integer.MIN_VALUE, -1, 2);
    assertSame(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertSame(1, 2L);
    assertSame(1L, 2);
    assertSame(Long.MAX_VALUE, 1L);
    assertSame(Long.MAX_VALUE, 1);
    assertSame(Long.MIN_VALUE, -1L, 5L);
    assertSame(Integer.MAX_VALUE, Long.MAX_VALUE);
    assertSame(1, 0.5);
    assertSame(1L, 0.5);
    assertSame(0.5, 1L, 2);
    assertSame(Long.MAX_VALUE, 1.0);
    assertSame(1.0, Long.MAX_VALUE, Long.MAX_VALUE);
    assertSame(Double.NaN, 1L);
    assertSame(1L, Double.POSITIVE_INFINITY, 2);
    assertSame(Long.MAX_VALUE, 1L, Double.NaN);
    assertSame(1.5f, 2.5);
    assertSame(1.5, 2.5f, 1L);
    assertSame(1, 2.5f);
    assertSame((short) 1, (short) 2);
    assertSame((short) 1, 2);
    assertSame(1, (short) 2, 3L);
    assertSame((byte) 1, 2.0);
    assertSame(1.0, (byte) 2);
    assertSame(new BigDecimal("1.10"), 2L, 3.5);
    assertSame(1L, new BigDecimal("1.10"), 2);
    assertSame(1.25, new BigDecimal("1.10"));
  }

  @Test
  void randomMixesMatchTheBoxedFold() {
    final Random random = new Random(9496);
    for (int round = 0; round < 2_000; round++) {
      final List<Number> values = new ArrayList<>();
      final int n = 1 + random.nextInt(12);
      for (int i = 0; i < n; i++)
        values.add(switch (random.nextInt(round % 3 == 0 ? 6 : 3)) {
          case 0 -> random.nextDouble() * 1e6 - 5e5;
          case 1 -> random.nextBoolean() ? random.nextLong() : (long) random.nextInt(1000);
          case 2 -> random.nextBoolean() ? random.nextInt() : random.nextInt(1000);
          case 3 -> (short) random.nextInt();
          case 4 -> random.nextFloat();
          default -> new BigDecimal(random.nextInt(1000)).movePointLeft(2);
        });
      final Number expected = reference(values);
      final Number actual = unboxed(values);
      assertThat(actual).as("sum of %s", values).isEqualTo(expected);
      assertThat(actual.getClass()).as("type of the sum of %s", values).isEqualTo(expected.getClass());
    }
  }

  @Test
  void emptyIsNull() {
    assertThat(new NumericSum().get()).isNull();
    assertThat(new SQLFunctionSum().getResult()).isNull();
    assertThat(new SQLFunctionAverage().getResult()).isNull();
  }

  @Test
  void aggregateMatchesExecute() {
    final Object[][] rows = { { 1 }, { null }, { Integer.MAX_VALUE }, { 2L }, { List.of(3, 4L) }, { 0.5 } };
    final SQLFunctionSum viaExecute = new SQLFunctionSum();
    final SQLFunctionSum viaAggregate = new SQLFunctionSum();
    final SQLFunctionAverage avgViaExecute = new SQLFunctionAverage();
    final SQLFunctionAverage avgViaAggregate = new SQLFunctionAverage();
    for (final Object[] row : rows) {
      viaExecute.execute(null, null, null, row, null);
      viaAggregate.aggregate(null, row, null);
      avgViaExecute.execute(null, null, null, row, null);
      avgViaAggregate.aggregate(null, row, null);
    }
    assertThat(viaAggregate.getResult()).isEqualTo(viaExecute.getResult()).isInstanceOf(Double.class);
    assertThat(avgViaAggregate.getResult()).isEqualTo(avgViaExecute.getResult());
  }

  @Test
  void mergedPartialsMatchTheBoxedFold() {
    final SQLFunctionSum left = new SQLFunctionSum();
    final SQLFunctionSum right = new SQLFunctionSum();
    left.aggregate(null, new Object[] { Integer.MAX_VALUE }, null);
    right.aggregate(null, new Object[] { 1 }, null);
    left.mergePartial(right);
    assertThat(left.getResult()).isEqualTo(Type.increment(Integer.MAX_VALUE, 1)).isInstanceOf(Long.class);

    final SQLFunctionAverage avg = new SQLFunctionAverage();
    final SQLFunctionAverage other = new SQLFunctionAverage();
    avg.aggregate(null, new Object[] { Long.MAX_VALUE }, null);
    other.aggregate(null, new Object[] { Long.MAX_VALUE }, null);
    avg.mergePartial(other);
    // A long OVERFLOW WIDENS THE TOTAL TO BigDecimal, BUT THE INPUTS WERE NOT DECIMALS: A DOUBLE MEAN (#8974)
    assertThat(avg.getResult()).isEqualTo((double) Long.MAX_VALUE);
  }

  @Test
  void groupByKeepsTypesThroughSql() throws Exception {
    TestHelper.executeInNewDatabase("issue-9496-sum", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "CREATE PROPERTY T.g STRING");
      db.command("sql", "CREATE PROPERTY T.i INTEGER");
      db.command("sql", "CREATE PROPERTY T.l LONG");
      db.command("sql", "CREATE PROPERTY T.d DOUBLE");
      db.transaction(() -> {
        db.newDocument("T").set("g", "a", "i", 1, "l", 1L, "d", 0.1).save();
        db.newDocument("T").set("g", "a", "i", 2, "l", 2L, "d", 0.2).save();
        db.newDocument("T").set("g", "b", "i", Integer.MAX_VALUE, "l", Long.MAX_VALUE, "d", -0.0).save();
        db.newDocument("T").set("g", "b", "i", 1, "l", 1L).save();
        db.newDocument("T").set("g", "c").save();
      });
      try (final ResultSet rs = db.query("sql",
          "SELECT g, sum(i) AS si, sum(l) AS sl, sum(d) AS sd, avg(i) AS ai, avg(l) AS al, avg(d) AS ad FROM T GROUP BY g ORDER BY g")) {
        Result r = rs.next();
        assertThat(r.<Object>getProperty("si")).isEqualTo(3);
        assertThat(r.<Object>getProperty("sl")).isEqualTo(3L);
        assertThat(r.<Object>getProperty("sd")).isEqualTo(0.1 + 0.2);
        assertThat(r.<Object>getProperty("ai")).isEqualTo(1.5);
        assertThat(r.<Object>getProperty("al")).isEqualTo(1.5);
        assertThat(r.<Object>getProperty("ad")).isEqualTo((0.1 + 0.2) / 2);
        r = rs.next();
        assertThat(r.<Object>getProperty("si")).isEqualTo((long) Integer.MAX_VALUE + 1);
        assertThat(r.<Object>getProperty("sl")).isEqualTo(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE));
        assertThat(r.<Object>getProperty("sd")).isEqualTo(-0.0);
        assertThat(r.<Object>getProperty("ad")).isEqualTo(-0.0);
        r = rs.next();
        assertThat(r.<Object>getProperty("si")).isNull();
        assertThat(r.<Object>getProperty("sd")).isNull();
        assertThat(r.<Object>getProperty("ad")).isNull();
      }
    });
  }
}
