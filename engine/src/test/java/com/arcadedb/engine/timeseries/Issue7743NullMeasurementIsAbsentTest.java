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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7743: a {@code null} field value - the natural way a client says "no measurement here" - was stored and
 * read back as a real {@code 0.0}, so AVG counted it and MIN returned it.
 * <p>
 * Two samples in one bucket, one of 10.0 and one null, answered {@code avg=5.0} and {@code min=0.0}: the null had
 * become a measurement smaller than every real one, and the sum was right only by the accident that zero is the
 * additive identity. An explicit {@code Double.NaN} behaved correctly all along, because NaN IS the absent marker
 * of the whole subsystem ({@link TimeSeriesNaN}), which is what makes the null the odd one out rather than the
 * NaN policy being wrong.
 * <p>
 * The fix stores a null measurement AS the absent marker wherever the column can carry one - a floating-point
 * column, whose NaN both layers round-trip: {@code TimeSeriesBatch.rawNull} on the mutable page,
 * {@link ColumnDefinition#storedNumericValueOf} in the {@code GORILLA_XOR} encoder and in the block statistics the
 * push-down answers from. An integer column has no value to spend on absence and keeps storing zero, which is what
 * it always did on both layers, so nothing there changes or diverges.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7743">issue #7743</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7743NullMeasurementIsAbsentTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  private TimeSeriesEngine create(final String typeName, final String fieldType) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value " + fieldType + ") SHARDS 1");
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private static void appendRealAndNull(final TimeSeriesEngine engine) throws IOException {
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { 10.0d, null } });
  }

  /** {@code SELECT sum/avg/min/count} over the two samples, as the issue's repro runs it. */
  private void assertAggregates(final String typeName, final double sum, final double avg, final double min,
      final long count) {
    try (final ResultSet rs = database.query("sql",
        "SELECT sum(value) AS s, avg(value) AS a, min(value) AS mn, count(*) AS c FROM " + typeName)) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue()).as("sum").isEqualTo(sum);
      assertThat(((Number) row.getProperty("a")).doubleValue()).as("avg").isEqualTo(avg);
      assertThat(((Number) row.getProperty("mn")).doubleValue()).as("min").isEqualTo(min);
      assertThat(((Number) row.getProperty("c")).longValue()).as("count counts ROWS, absent sample included")
          .isEqualTo(count);
    }
  }

  @Test
  void theMutableLayerTreatsANullAsNoSampleAtAll() throws Exception {
    final TimeSeriesEngine engine = create("Mutable", "DOUBLE");
    appendRealAndNull(engine);

    assertAggregates("Mutable", 10.0d, 10.0d, 10.0d, 2);
  }

  @Test
  void theSealedLayerAnswersTheSameAfterCompaction() throws Exception {
    final TimeSeriesEngine engine = create("Sealed", "DOUBLE");
    appendRealAndNull(engine);
    engine.compactAll();

    assertAggregates("Sealed", 10.0d, 10.0d, 10.0d, 2);
  }

  /**
   * The two layers have to agree sample by sample, not only in aggregate: a read of the raw rows is what the
   * projection endpoints hand back, and a phantom 0.0 there is a measurement the client never took.
   */
  @Test
  void bothLayersReadTheNullBackAsTheAbsentMarker() throws Exception {
    final TimeSeriesEngine engine = create("RawRows", "DOUBLE");
    appendRealAndNull(engine);

    database.begin();
    try {
      assertThat(measurements(engine)).containsExactly(10.0d, TimeSeriesNaN.ABSENT);
    } finally {
      database.commit();
    }

    engine.compactAll();

    database.begin();
    try {
      assertThat(measurements(engine))
          .as("compaction is not a question the caller asked, so it cannot change the answer")
          .containsExactly(10.0d, TimeSeriesNaN.ABSENT);
    } finally {
      database.commit();
    }
  }

  /** The ENGINE boundary, where absence is the NaN marker; the SQL boundary spells the same thing NULL. */
  private static List<Double> measurements(final TimeSeriesEngine engine) throws IOException {
    return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).stream()
        .map(row -> ((Number) row[1]).doubleValue())
        .toList();
  }

  /** And the SQL boundary on the same rows: a null measurement reads back as SQL NULL, not as NaN. */
  @Test
  void aProjectedNullMeasurementReadsBackAsSqlNull() throws Exception {
    final TimeSeriesEngine engine = create("Projected", "DOUBLE");
    appendRealAndNull(engine);

    try (final ResultSet rs = database.query("sql", "SELECT value FROM Projected ORDER BY ts")) {
      assertThat(rs.next().<Object>getProperty("value")).isEqualTo(10.0d);
      assertThat(rs.next().<Object>getProperty("value")).as("no measurement is NULL, not NaN and not 0.0").isNull();
    }
  }

  /** A FLOAT column carries the marker too, and its narrower codec round-trips it. */
  @Test
  void aFloatColumnCarriesTheMarkerAsWell() throws Exception {
    final TimeSeriesEngine engine = create("Floats", "FLOAT");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { 10.0f, null } });

    assertAggregates("Floats", 10.0d, 10.0d, 10.0d, 2);
    engine.compactAll();
    assertAggregates("Floats", 10.0d, 10.0d, 10.0d, 2);
  }

  /**
   * An integer column has no absent marker in its stored form, so a null there is still a real zero - on both
   * layers, which is the property that matters. Pinned so the asymmetry is a decision rather than an oversight.
   */
  @Test
  void anIntegerColumnStillStoresANullAsZeroOnBothLayers() throws Exception {
    final TimeSeriesEngine engine = create("Longs", "LONG");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { 10L, null } });

    assertAggregates("Longs", 10.0d, 5.0d, 0.0d, 2);
    engine.compactAll();
    assertAggregates("Longs", 10.0d, 5.0d, 0.0d, 2);
  }

  /**
   * A DOUBLE column explicitly sealed with {@code DICTIONARY}, which is a supported pairing (issue #7689): the
   * codec stores the text form, so the null used to compact to {@code "0.0"} and now compacts to {@code "NaN"} -
   * which {@code ColumnDefinition.boxString} parses straight back to the marker. Fixed as a side effect rather
   * than by its own code path, so it gets its own test (code review on PR #7747).
   */
  @Test
  void aDictionaryEncodedDoubleColumnCarriesTheMarkerToo() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Dict TIMESTAMP ts FIELDS (value DOUBLE CODEC DICTIONARY) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Dict")).getEngine();
    appendRealAndNull(engine);

    assertAggregates("Dict", 10.0d, 10.0d, 10.0d, 2);
    engine.compactAll();
    assertAggregates("Dict", 10.0d, 10.0d, 10.0d, 2);
  }

  /** An explicit NaN and a null are now the same thing, which is what made the NaN the working case. */
  @Test
  void anExplicitNaNAndANullAreIndistinguishable() throws Exception {
    final TimeSeriesEngine withNaN = create("NanSamples", "DOUBLE");
    withNaN.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 },
        new Object[][] { new Object[] { 10.0d, Double.NaN } });
    final TimeSeriesEngine withNull = create("NullSamples", "DOUBLE");
    appendRealAndNull(withNull);

    assertAggregates("NanSamples", 10.0d, 10.0d, 10.0d, 2);
    assertAggregates("NullSamples", 10.0d, 10.0d, 10.0d, 2);
  }

  /**
   * A whole bucket of nulls is an absent window, not a window that measured zero: MIN and AVG have nothing to
   * answer with and say so.
   * <p>
   * {@code SUM} is left out here on purpose. What SUM answers over a group with no value is the generic SQL
   * function's question, not this one's - it answers 0 for an empty group whatever the source - and #7694 owns
   * it. What this test is about is that no null CONTRIBUTED a zero.
   */
  @Test
  void aWindowOfNothingButNullsIsAbsentRatherThanZero() throws Exception {
    final TimeSeriesEngine engine = create("AllNull", "DOUBLE");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { null, null } });
    engine.compactAll();

    try (final ResultSet rs = database.query("sql",
        "SELECT avg(value) AS a, min(value) AS mn, count(*) AS c FROM AllNull")) {
      final Result row = rs.next();
      assertThat(isAbsent(row.getProperty("a"))).as("avg over no real sample").isTrue();
      assertThat(isAbsent(row.getProperty("mn"))).as("min over no real sample, NOT a phantom 0.0").isTrue();
      assertThat(((Number) row.getProperty("c")).longValue()).as("the rows are still there").isEqualTo(2);
    }
  }

  /**
   * At the SQL boundary "absent" is spelled {@code NULL} and nothing else - not NaN, which is the STORAGE
   * spelling. Asserted strictly on purpose (code review on PR #7747): a helper that accepted either would let
   * the marker leak into a result property without failing anything.
   */
  private static boolean isAbsent(final Object value) {
    return value == null;
  }

  // ---- the aggregation push-down, which is a different plan for the same question ----

  /**
   * The query shape the issue reports, and the one that is PUSHED DOWN: {@code GROUP BY} on the time-bucket alias
   * routes through {@code AggregateFromTimeSeriesStep} instead of the generic aggregators, so it reads the
   * engine's numbers directly rather than the rows. Both layers, because compaction must not change the answer.
   */
  @Test
  void theGroupedPushDownAnswersTheRealSamplesOnBothLayers() throws Exception {
    final TimeSeriesEngine engine = create("Grouped", "DOUBLE");
    appendRealAndNull(engine);

    assertGroupedAggregates("Grouped", 10.0d, 10.0d, 10.0d);
    engine.compactAll();
    assertGroupedAggregates("Grouped", 10.0d, 10.0d, 10.0d);
  }

  /**
   * A pushed-down bucket that measured nothing answers {@code NULL}, the same word the generic aggregation and
   * the raw-row projection answer - not the raw NaN marker (code review on PR #7747). A client that read NaN
   * from one plan and NULL from the other would be reading the PLAN rather than the data, which is the very
   * divergence this PR exists to remove.
   */
  @Test
  void aPushedDownBucketWithNoRealSampleAnswersNullRatherThanTheMarker() throws Exception {
    final TimeSeriesEngine engine = create("GroupedAllNull", "DOUBLE");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { null, null } });
    engine.compactAll();

    try (final ResultSet rs = database.query("sql", groupedQuery("GroupedAllNull"))) {
      final Result row = rs.next();
      assertThat(isAbsent(row.getProperty("s"))).as("SUM over no real sample").isTrue();
      assertThat(isAbsent(row.getProperty("a"))).as("AVG over no real sample").isTrue();
      assertThat(isAbsent(row.getProperty("mn"))).as("MIN over no real sample, NOT a phantom 0.0").isTrue();
    }
  }

  /**
   * The other NaN, and why the translation is keyed on the COUNT of real contributors rather than on the value
   * (CodeRabbit on PR #7747): a SUM over {@code +Infinity} and {@code -Infinity} is NaN with real samples behind
   * it - an undefined TOTAL, which IEEE keeps and so does {@link TimeSeriesNaN#sum} - not an absence. Turning
   * that into NULL would report "nothing was measured" about two measurements.
   */
  @Test
  void anArithmeticNaNOverRealSamplesIsNotTurnedIntoNull() throws Exception {
    final TimeSeriesEngine engine = create("Undefined", "DOUBLE");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 },
        new Object[][] { new Object[] { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY } });

    try (final ResultSet rs = database.query("sql", groupedQuery("Undefined"))) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("s")).as("an undefined total is an answer, not an absence").isNotNull();
      assertThat(((Number) row.getProperty("s")).doubleValue()).isNaN();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(2);
    }

    engine.compactAll();
    try (final ResultSet rs = database.query("sql", groupedQuery("Undefined"))) {
      assertThat(rs.next().<Object>getProperty("s")).as("and the sealed layer agrees").isNotNull();
    }
  }

  private static String groupedQuery(final String typeName) {
    // ts.timeBucket takes the INTERVAL first and the timestamp column second, and the planner pushes the
    // aggregation down only when GROUP BY names the bucket alias.
    return "SELECT ts.timeBucket('1h', ts) AS b, sum(value) AS s, avg(value) AS a, min(value) AS mn, count(*) AS c"
        + " FROM " + typeName + " GROUP BY b";
  }

  private void assertGroupedAggregates(final String typeName, final double sum, final double avg, final double min) {
    try (final ResultSet rs = database.query("sql", groupedQuery(typeName))) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue()).as("sum").isEqualTo(sum);
      assertThat(((Number) row.getProperty("a")).doubleValue()).as("avg").isEqualTo(avg);
      assertThat(((Number) row.getProperty("mn")).doubleValue()).as("min").isEqualTo(min);
      assertThat(((Number) row.getProperty("c")).longValue()).as("count counts ROWS").isEqualTo(2);
    }
  }
}
