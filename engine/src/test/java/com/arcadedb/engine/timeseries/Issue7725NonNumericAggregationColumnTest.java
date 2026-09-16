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
import com.arcadedb.engine.timeseries.ColumnDefinition.ColumnRole;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7725: the MUTABLE aggregation path turned a value that is not a {@link Number} into a REAL sample of
 * {@code 0.0}, and the SEALED path over the same samples did something else entirely.
 * <p>
 * Two disagreements hid behind one {@code else}. A {@code BOOLEAN} field has been an integer column since issue
 * #5475, so the sealed layer packs it through {@code SIMPLE8B} and reads it back as 1 and 0 - while
 * {@link Boolean} is not a {@link Number}, so the mutable layer read every one of those samples as zero. The
 * answer to {@code SUM(flag)} therefore depended on whether compaction had run, which is precisely the shape of
 * issue #7089. And a column the sealed layer cannot read as a number AT ALL - a {@code STRING} field, any TAG,
 * the timestamp - was answered by the mutable layer as a column of zeros and by the sealed layer with an
 * {@code IllegalArgumentException} out of its decoder, so the same request answered 200 before compaction and
 * 500 after it.
 * <p>
 * The fix has two halves and this class pins both: such a column is REFUSED up front, by
 * {@link TimeSeriesGateway#requireAggregatableColumn}, so neither layer is reached; and what the mutable layer
 * does read, it reads exactly the way the sealed layer does, through
 * {@link TimeSeriesNaN#asMeasurement(Object)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7725NonNumericAggregationColumnTest extends TestHelper {

  private static final long HOUR = 3_600_000L;

  // ---- the unboxing itself ----

  /**
   * The reported branch. A boolean is one or zero - what the sealed layer's {@code SIMPLE8B} column holds - a
   * null is zero, because that is what BOTH sealing codecs write for it, and anything that is not a measurement
   * at all is {@link TimeSeriesNaN#ABSENT}, which every aggregate skips, rather than a measurement of zero.
   */
  @Test
  void aMutableSampleIsUnboxedTheWayTheSealedLayerUnboxesIt() {
    assertThat(TimeSeriesNaN.asMeasurement(Boolean.TRUE)).as("what SIMPLE8B stores for true").isEqualTo(1.0);
    assertThat(TimeSeriesNaN.asMeasurement(Boolean.FALSE)).isEqualTo(0.0);
    assertThat(TimeSeriesNaN.asMeasurement(null)).as("both sealing codecs write a null out as zero").isEqualTo(0.0);
    assertThat(TimeSeriesNaN.asMeasurement(42)).isEqualTo(42.0);
    assertThat(TimeSeriesNaN.asMeasurement(2.5f)).isEqualTo(2.5);
    assertThat(TimeSeriesNaN.asMeasurement(Double.NaN)).as("an absent sample stays absent").isNaN();
    assertThat(TimeSeriesNaN.asMeasurement("us-east")).as("not a measurement: a gap, never a zero").isNaN();
    assertThat(TimeSeriesNaN.asMeasurement(new Object())).isNaN();
  }

  /**
   * The predicate the refusal rests on is asked of the CODEC, because the codec is what decides whether the
   * sealed layer has a numeric decoder for the column at all. A {@code LONG} TAG is the case that makes the
   * distinction load-bearing: its declared type is numeric and it is still unreadable as a number, because a
   * tag is always dictionary-encoded.
   */
  @Test
  void onlyAColumnWithANumericCodecCanBeAggregated() {
    assertThat(field("v", Type.DOUBLE).isNumericallyAggregatable()).isTrue();
    assertThat(field("v", Type.FLOAT).isNumericallyAggregatable()).isTrue();
    assertThat(field("v", Type.LONG).isNumericallyAggregatable()).isTrue();
    assertThat(field("v", Type.INTEGER).isNumericallyAggregatable()).isTrue();
    assertThat(field("v", Type.BOOLEAN).isNumericallyAggregatable()).as("SIMPLE8B since issue #5475").isTrue();
    assertThat(field("v", Type.DATETIME).isNumericallyAggregatable()).isTrue();

    assertThat(field("v", Type.STRING).isNumericallyAggregatable()).as("DICTIONARY has no numeric decoder").isFalse();
    assertThat(tag("host", Type.STRING).isNumericallyAggregatable()).isFalse();
    assertThat(tag("shard", Type.LONG).isNumericallyAggregatable())
        .as("a numeric TAG is still dictionary-encoded, so still unreadable as a number").isFalse();
    assertThat(timestamp().isNumericallyAggregatable()).as("DELTA_OF_DELTA is not a numeric codec").isFalse();
  }

  /**
   * The refusal names the column and says what is wrong with it, because that sentence is what reaches the
   * caller as the 400 on all three aggregation surfaces. COUNT is exempt: it counts rows and never reads the
   * column, which is what both layers already do.
   */
  @Test
  void aggregatingAColumnNoLayerCanReadIsRefusedByName() {
    final ColumnDefinition host = tag("host", Type.STRING);

    for (final AggregationType type : List.of(AggregationType.SUM, AggregationType.AVG, AggregationType.MIN,
        AggregationType.MAX))
      assertThatThrownBy(() -> TimeSeriesGateway.requireAggregatableColumn(host, type))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Aggregation " + type.name())
          .hasMessageContaining("'host'")
          .hasMessageContaining("TAG")
          .hasMessageContaining("STRING");

    TimeSeriesGateway.requireAggregatableColumn(host, AggregationType.COUNT);
    TimeSeriesGateway.requireAggregatableColumn(field("v", Type.DOUBLE), AggregationType.SUM);
  }

  // ---- the two layers over the same samples ----

  /**
   * The reported divergence, end to end: the same aggregation over the same six boolean samples, before and
   * after compaction. Before the fix the mutable answer was a SUM of zero and the sealed answer was the count
   * of {@code true}s, so the panel changed by itself when the maintenance scheduler happened to run.
   */
  @Test
  void aBooleanFieldAggregatesTheSameBeforeAndAfterCompaction() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Flags TIMESTAMP ts FIELDS (active BOOLEAN) SHARDS 1"
        + " COMPACTION_INTERVAL 1 HOURS");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Flags")).getEngine();

    // Bucket 0: true, false, true -> sum 2, avg 2/3. Bucket 1: true, true, false -> sum 2, avg 2/3.
    database.transaction(() -> {
      insertFlag(0L, true);
      insertFlag(1_000L, false);
      insertFlag(2_000L, true);
      insertFlag(HOUR, true);
      insertFlag(HOUR + 1_000L, true);
      insertFlag(HOUR + 2_000L, false);
    });

    final double[] mutable = sumAvgCount(engine);

    engine.compactAll();

    final double[] sealed = sumAvgCount(engine);

    assertThat(sealed).as("the same samples must aggregate the same whether or not compaction has run")
        .containsExactly(mutable);
    assertThat(mutable[0]).as("SUM counts the true samples, not zero for every one of them").isEqualTo(2.0);
    assertThat(mutable[1]).as("AVG over three samples, two of them true").isEqualTo(2.0 / 3);
    assertThat(mutable[2]).as("COUNT is rows, on both layers").isEqualTo(3.0);
  }

  /**
   * The single-column {@code aggregate()} entry point carried a third copy of the same {@code else 0.0}, and
   * has to agree with the multi-column one over the same samples.
   */
  @Test
  void theSingleColumnAggregateAgreesWithTheMultiColumnOne() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Flags2 TIMESTAMP ts FIELDS (active BOOLEAN) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Flags2")).getEngine();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Flags2 SET ts = 0, active = true");
      database.command("sql", "INSERT INTO Flags2 SET ts = 1000, active = false");
      database.command("sql", "INSERT INTO Flags2 SET ts = 2000, active = true");
    });

    database.begin();
    try {
      final AggregationResult sum = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.SUM, HOUR, null);
      assertThat(sum.size()).isEqualTo(1);
      assertThat(sum.getValue(0)).as("two true samples, not zero").isEqualTo(2.0);
    } finally {
      database.commit();
    }
  }

  /**
   * The width guard, reached through the one public entry point that can: {@code aggregate()} takes a raw
   * column index and never validates it, so an index past the row's width lands on the guard.
   * <p>
   * This is a deliberate behaviour change beyond what #7725 reported, and the one place the fix is not purely
   * "make the two layers agree": such a request used to contribute a real {@code 0.0} per row, so SUM answered
   * the number zero and AVG answered zero for a column that was never read at all. Absence is what "this row
   * carries no such column" means, and {@link TimeSeriesNaN#ABSENT} is how the rest of the stack says it - so
   * SUM is absent rather than a total of zero, and COUNT still counts the rows, which is what distinguishes
   * "no measurement" from "no rows".
   */
  @Test
  void aColumnIndexPastTheRowWidthIsAbsentRatherThanAZeroSample() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Narrow TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Narrow")).getEngine();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Narrow SET ts = 0, value = 3.0");
      database.command("sql", "INSERT INTO Narrow SET ts = 1000, value = 4.0");
    });

    database.begin();
    try {
      // Column 9 does not exist: the row carries the timestamp and one field, so row.length is 2.
      final AggregationResult sum = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 9, AggregationType.SUM, HOUR, null);
      assertThat(sum.size()).isEqualTo(1);
      assertThat(sum.getValue(0)).as("a column the row does not carry is a gap, not a total of zero").isNaN();

      final AggregationResult count = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 9, AggregationType.COUNT, HOUR,
          null);
      assertThat(count.getValue(0)).as("COUNT counts rows, so it is unaffected").isEqualTo(2.0);

      // The counter-case: the column that IS there still answers a number.
      final AggregationResult real = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.SUM, HOUR,
          null);
      assertThat(real.getValue(0)).isEqualTo(7.0);
    } finally {
      database.commit();
    }
  }

  /**
   * The SQL push-down declines a column it cannot read rather than refusing the query: the generic aggregation
   * path answers it instead. Pinned by the PLAN, because both paths answer the query and only one of them is
   * what this is about.
   */
  @Test
  void theSqlPushDownDeclinesANonNumericColumn() {
    database.command("sql", "CREATE TIMESERIES TYPE Labelled TIMESTAMP ts TAGS (host STRING)"
        + " FIELDS (value DOUBLE, note STRING) SHARDS 1");
    database.transaction(() -> database.command("sql",
        "INSERT INTO Labelled SET ts = 1000, host = 'a', value = 1.0, note = 'x'"));

    assertThat(planOf("SELECT ts.timeBucket('1h', ts) AS b, sum(value) AS s FROM Labelled GROUP BY b"))
        .as("a DOUBLE field is still pushed down").contains("AGGREGATE FROM TIMESERIES Labelled");

    // The generic path's own refusal is what now answers a SUM over text, and its arrival is the proof the
    // push-down declined: before this change the push-down took the query and reported a total of 0.0.
    assertThatThrownBy(
        () -> planOf("SELECT ts.timeBucket('1h', ts) AS b, sum(note) AS s FROM Labelled GROUP BY b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sum() requires numeric input");

    assertThat(planOf("SELECT ts.timeBucket('1h', ts) AS b, max(host) AS s FROM Labelled GROUP BY b"))
        .as("a TAG is not pushed down either; MAX over text is the generic path's business")
        .doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(planOf("SELECT ts.timeBucket('1h', ts) AS b, count(note) AS c FROM Labelled GROUP BY b"))
        .as("COUNT never reads the column, so it stays pushed down")
        .contains("AGGREGATE FROM TIMESERIES Labelled");
  }

  // ---- helpers ----

  private String planOf(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
    }
  }

  private void insertFlag(final long ts, final boolean value) {
    database.command("sql", "INSERT INTO Flags SET ts = :ts, active = :v", Map.of("ts", ts, "v", value));
  }

  /**
   * SUM, AVG and COUNT of {@code active} over the first hour bucket, as one array so the two layers can be
   * compared in one assertion.
   */
  private double[] sumAvgCount(final TimeSeriesEngine engine) throws Exception {
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "a"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "c"));

    database.begin();
    try {
      final MultiColumnAggregationResult result =
          engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null);
      return new double[] { result.getValue(0L, 0), result.getValue(0L, 1), result.getValue(0L, 2) };
    } finally {
      database.commit();
    }
  }

  private static ColumnDefinition field(final String name, final Type type) {
    return new ColumnDefinition(name, type, ColumnRole.FIELD);
  }

  private static ColumnDefinition tag(final String name, final Type type) {
    return new ColumnDefinition(name, type, ColumnRole.TAG);
  }

  private static ColumnDefinition timestamp() {
    return new ColumnDefinition("ts", Type.LONG, ColumnRole.TIMESTAMP);
  }
}
