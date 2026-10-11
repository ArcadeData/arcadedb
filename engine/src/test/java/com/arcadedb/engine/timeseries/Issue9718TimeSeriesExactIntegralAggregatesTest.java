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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9718 and #9720: the TimeSeries aggregation push-down accumulated every aggregate as a double, and a sealed block kept the
 * min/max/sum of every numeric column as doubles, so on a LONG column past 2^53 a grouped or bucketed {@code min}/{@code max}
 * and any {@code sum} answered a rounded double, and the exact ungrouped {@code min}/{@code max} of #9710 had to re-read every
 * row. The integral columns now keep their block statistics as longs (a 128-bit total for the sum) and the aggregation folds them
 * as longs, so every shape answers the values and the Java types a document twin answers, from the block headers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9718TimeSeriesExactIntegralAggregatesTest extends TestHelper {
  private static final long T0   = 1_767_225_600_000L;
  private static final long BASE = 9_007_199_254_740_990L; // 2^53 - 2

  @Test
  void groupedAndBucketedMinMaxSumOfALongPastTwoPow53AreExact() {
    for (final int shards : new int[] { 1, 3 }) {
      createTypes("bg LONG, d DOUBLE", shards);
      load(0, 90, i -> BASE + i % 9);
      final String[] queries = {
          "SELECT host, min(bg) AS mn, max(bg) AS mx, sum(bg) AS s, avg(bg) AS a, count(*) AS c FROM X GROUP BY host ORDER BY host",
          "SELECT ts.timeBucket('20s', ts) AS t, min(bg) AS mn, max(bg) AS mx, sum(bg) AS s FROM X GROUP BY t ORDER BY t",
          "SELECT ts.timeBucket('30s', ts) AS t, host, min(bg) AS mn, max(bg) AS mx, sum(bg) AS s FROM X GROUP BY t, host ORDER BY t, host",
          "SELECT host, min(bg) AS mn, max(bg) AS mx FROM X WHERE bg >= 9007199254740993 GROUP BY host ORDER BY host",
          "SELECT min(bg) AS mn, max(bg) AS mx, sum(bg) AS s, avg(bg) AS a FROM X",
          "SELECT sum(bg) AS s FROM X WHERE host = 'h1'" };
      assertEveryQueryMatchesTheTwin(queries);
      dropTypes();
    }
  }

  @Test
  void aSumPastTheLongRangeIsABigDecimalAndAnIntegerSumWidensLikeTheGenericPlan() {
    createTypes("bg LONG, i INTEGER", 2);
    // all positive, so the generic plan's running total overflows a long once and stays a BigDecimal, whatever the order
    load(0, 40, i -> Long.MAX_VALUE - i);
    final String[] queries = { "SELECT sum(bg) AS s, avg(bg) AS a, min(bg) AS mn, max(bg) AS mx FROM X",
        "SELECT host, sum(bg) AS s FROM X GROUP BY host ORDER BY host", "SELECT sum(i) AS s, min(i) AS mn FROM X" };
    assertEveryQueryMatchesTheTwin(queries);

    final BigDecimal expected = new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(40))
        .subtract(BigInteger.valueOf(39 * 40 / 2)));
    assertThat(row("SELECT sum(bg) AS s FROM T")).isEqualTo("[s=" + expected + "(BigDecimal)]");
  }

  @Test
  void theBlockStatisticsOfALongPastTwoPow53AnswerWithoutDecodingABlock() throws Exception {
    createTypes("bg LONG", 1);
    load(0, 3000, i -> BASE + i);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isPositive();

    final AggregationMetrics metrics = new AggregationMetrics();
    final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(new MultiColumnAggregationRequest(2, AggregationType.MIN, "mn"), new MultiColumnAggregationRequest(2, AggregationType.MAX, "mx"),
            new MultiColumnAggregationRequest(2, AggregationType.SUM, "s")), 0L, 0L, null, null, metrics, 0);
    final long bucket = result.getBucketTimestamps().getFirst();
    assertThat(result.getExactValue(bucket, 0)).isEqualTo(BASE);
    assertThat(result.getExactValue(bucket, 1)).isEqualTo(BASE + 2999);
    assertThat(result.getExactValue(bucket, 2)).isEqualTo(
        new BigDecimal(BigInteger.valueOf(BASE).multiply(BigInteger.valueOf(3000)).add(BigInteger.valueOf(2999L * 3000 / 2))));
    // every block answered from its header: none decoded, which is what #9710's exact answer used to cost
    assertThat(metrics.getSlowPathBlocks()).isZero();
    assertThat(metrics.getFastPathBlocks()).isPositive();

    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(row("SELECT min(bg) AS mn, max(bg) AS mx FROM D"));
  }

  @Test
  void theExactStatisticsSurviveAReopenAndTheFileSaysItsFormat() throws Exception {
    createTypes("bg LONG, i INTEGER", 2);
    load(0, 200, i -> i % 2 == 0 ? BASE + i : -BASE - i);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final String[] queries = { "SELECT min(bg) AS mn, max(bg) AS mx, sum(bg) AS s, avg(bg) AS a FROM X",
        "SELECT host, min(bg) AS mn, max(bg) AS mx, sum(i) AS s FROM X GROUP BY host ORDER BY host" };
    assertEveryQueryMatchesTheTwin(queries);

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    final String sealedPath = engine.getShard(0).getSealedStore().getSealedFileName();
    reopenDatabase();
    for (final String sql : queries)
      assertThat(rows(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(rows(sql.replace("FROM X", "FROM D")));

    // a build that cannot read the exact statistics (or a block's null bitmap) is refused at the header, not misled by it
    try (final RandomAccessFile file = new RandomAccessFile(database.getDatabasePath() + "/" + sealedPath, "r")) {
      file.seek(4);
      assertThat(file.readByte()).isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);
    }
    assertThat(TimeSeriesSealedStore.CURRENT_VERSION).isEqualTo(3);
  }

  @Test
  void exactStatisticsAreProvenFromAnOlderBlocksDoublesOnlyWhenTheyCanBe() {
    // within 2^53 and with partial sums that cannot pass it, the doubles are the longs
    assertThat(TimeSeriesSealedStore.exactFromDoubles(-5.0, 1_000.0, 2_000.0, 10)).containsExactly(-5L, 1_000L, 2_000L, 0L, 10L);
    assertThat(TimeSeriesSealedStore.exactFromDoubles(-5.0, -1.0, -20.0, 10)).containsExactly(-5L, -1L, -20L, -1L, 10L);
    // an extreme at 2^53 may be a rounded neighbour, and so may a total whose partial sums could reach it
    assertThat(TimeSeriesSealedStore.exactFromDoubles(0.0, 0x1p53, 0x1p53, 1)).isNull();
    assertThat(TimeSeriesSealedStore.exactFromDoubles(0.0, 0x1p50, 0x1p52, 16)).isNull();
    // a legacy block whose sum went through a NaN declares no count
    assertThat(TimeSeriesSealedStore.exactFromDoubles(0.0, 1.0, Double.NaN, TimeSeriesSealedStore.BlockEntry.COUNT_UNKNOWN)).isNull();
    assertThat(TimeSeriesSealedStore.exactFromDoubles(Double.NaN, Double.NaN, Double.NaN, 0)).containsExactly(0L, 0L, 0L, 0L, 0L);
  }

  @Test
  void anIntegralFieldConditionIsJudgedOnTheExactStatistics() {
    final ColumnDefinition lng = new ColumnDefinition("l", Type.LONG, ColumnDefinition.ColumnRole.FIELD);
    final long twoPow53 = 1L << 53;
    final FieldFilter.Condition above = FieldFilter.range(0, lng, twoPow53 + 1, true, null, false).getConditions().getFirst();
    // a block holding exactly 2^53: its double statistics could not rule out 2^53 + 1, the exact ones can
    assertThat(FieldFilter.blockMatchExact(above, twoPow53, twoPow53, 10, 10)).isEqualTo(FieldFilter.BlockMatch.NONE);
    assertThat(FieldFilter.blockMatchExact(above, twoPow53 + 1, twoPow53 + 5, 10, 10)).isEqualTo(FieldFilter.BlockMatch.ALL);
    assertThat(FieldFilter.blockMatchExact(above, twoPow53, twoPow53 + 5, 10, 10)).isEqualTo(FieldFilter.BlockMatch.SOME);
    // a sample with no value fails every condition, so the block is decoded rather than answered whole
    assertThat(FieldFilter.blockMatchExact(above, twoPow53 + 1, twoPow53 + 5, 9, 10)).isEqualTo(FieldFilter.BlockMatch.SOME);
    assertThat(FieldFilter.blockMatchExact(above, 0, 0, 0, 10)).isEqualTo(FieldFilter.BlockMatch.NONE);
  }

  @Test
  void theExactSumHelperCarriesAcrossBothWords() {
    final long[] acc = new long[2];
    ExactLongSum.add(acc, 0, Long.MAX_VALUE);
    ExactLongSum.add(acc, 0, Long.MAX_VALUE);
    ExactLongSum.add(acc, 0, 2);
    assertThat(ExactLongSum.toBigInteger(acc[0], acc[1])).isEqualTo(BigInteger.ONE.shiftLeft(64));
    ExactLongSum.add(acc, 0, Long.MIN_VALUE);
    ExactLongSum.add(acc, 0, Long.MIN_VALUE);
    ExactLongSum.add(acc, 0, -1);
    assertThat(ExactLongSum.toNumber(acc[0], acc[1])).isEqualTo(-1L);
    final long[] other = { 5L, 0L };
    ExactLongSum.add(acc, 0, other[0], other[1]);
    assertThat(ExactLongSum.toNumber(acc[0], acc[1])).isEqualTo(4L);
    assertThat(ExactLongSum.truncatedAverage(-7L, -1L, 2)).isEqualTo(-3L);
  }

  @Test
  void downsamplingAnIntegralFieldAveragesItExactly() throws Exception {
    createTypes("bg LONG", 1);
    // ten samples per 10s bucket, past 2^53, where a double mean is no longer one of the column's values
    load(0, 100, i -> BASE + (i % 10) * 3L);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    engine.getShard(0).getSealedStore().downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(1), List.of(2));
    // per 10s bucket and host, the exact mean of the samples truncated toward zero: in the first bucket h0 holds BASE + 0, 9, 18,
    // 27, whose mean BASE + 13.5 a double sum makes BASE + 12, a mean of nothing here
    final List<String> expected = new ArrayList<>();
    for (int b = 0; b < 10; b++)
      for (int h = 0; h < 3; h++) {
        long sum = 0;
        int count = 0;
        for (int i = b * 10; i < b * 10 + 10; i++)
          if (i % 3 == h) {
            sum += (i % 10) * 3L;
            count++;
          }
        expected.add("[bg=" + (BASE + sum / count) + "(Long), host=h" + h + "(String)]");
      }
    assertThat(rows("SELECT host, bg FROM T ORDER BY ts, host")).isEqualTo(expected);
    assertThat(expected.getFirst()).isEqualTo("[bg=" + (BASE + 13) + "(Long), host=h0(String)]");
  }

  private interface ValueOf {
    long of(int i);
  }

  private void createTypes(final String fields, final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (" + fields + ") SHARDS " + shards);
    database.command("sql", "CREATE DOCUMENT TYPE D");
  }

  private void dropTypes() {
    database.command("sql", "DROP TYPE T");
    database.command("sql", "DROP TYPE D");
  }

  private void load(final int from, final int to, final ValueOf valueOf) {
    final boolean withInt = ((LocalTimeSeriesType) database.getSchema().getType("T")).isDeclaredColumn("i");
    final boolean withDouble = ((LocalTimeSeriesType) database.getSchema().getType("T")).isDeclaredColumn("d");
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        for (final String type : new String[] { "T", "D" }) {
          final StringBuilder sql = new StringBuilder("INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?");
          final List<Object> args = new ArrayList<>(List.of(T0 + i * 1_000L, "h" + (i % 3), valueOf.of(i)));
          if (withInt) {
            sql.append(", i = ?");
            args.add(Integer.MAX_VALUE - i);
          }
          if (withDouble) {
            sql.append(", d = ?");
            args.add((double) i);
          }
          database.command("sql", sql.toString(), args.toArray());
        }
    });
  }

  private void assertEveryQueryMatchesTheTwin(final String[] queries) {
    for (int pass = 0; pass < 3; pass++) {
      for (final String sql : queries) {
        final String onT = sql.replace("FROM X", "FROM T");
        assertThat(plan(onT)).as(onT).contains("AGGREGATE FROM TIMESERIES");
        assertThat(rows(onT)).as(onT + " pass " + pass).isEqualTo(rows(sql.replace("FROM X", "FROM D")));
      }
      if (pass == 0)
        database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private String row(final String sql) {
    final List<String> rows = rows(sql);
    assertThat(rows).hasSize(1);
    return rows.getFirst();
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(describe(rs.next()));
    }
    return out;
  }

  private static String describe(final Result r) {
    final List<String> out = new ArrayList<>();
    for (final String name : r.getPropertyNames().stream().sorted().toList()) {
      final Object value = r.getProperty(name);
      out.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
    }
    return out.toString();
  }
}
