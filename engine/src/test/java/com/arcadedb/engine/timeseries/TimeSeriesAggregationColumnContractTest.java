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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition.ColumnRole;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Aggregation push-down column contract: non-numeric columns, column index conventions, caller errors, concurrent
 * aliases and exact integral MIN/MAX/SUM results.
 */
class TimeSeriesAggregationColumnContractTest extends TestHelper {

  // ---- #7725 ----
  // Issue #7725: the MUTABLE aggregation path turned a value that is not a {@link Number} into a REAL sample of
  // {@code 0.0}, and the SEALED path over the same samples did something else entirely.
  // <p>
  // Two disagreements hid behind one {@code else}. A {@code BOOLEAN} field has been an integer column since issue
  // #5475, so the sealed layer packs it through {@code SIMPLE8B} and reads it back as 1 and 0 - while
  // {@link Boolean} is not a {@link Number}, so the mutable layer read every one of those samples as zero. The
  // answer to {@code SUM(flag)} therefore depended on whether compaction had run, which is precisely the shape of
  // issue #7089. And a column the sealed layer cannot read as a number AT ALL - a {@code STRING} field, any TAG,
  // the timestamp - was answered by the mutable layer as a column of zeros and by the sealed layer with an
  // {@code IllegalArgumentException} out of its decoder, so the same request answered 200 before compaction and
  // 500 after it.
  // <p>
  // The fix has two halves and this class pins both: such a column is REFUSED up front, by
  // {@link TimeSeriesGateway#requireAggregatableColumn}, so neither layer is reached; and what the mutable layer
  // does read, it reads exactly the way the sealed layer does, through
  // {@link TimeSeriesNaN#asMeasurement(Object)}.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long HOUR = 3_600_000L;

  // #7725
  // ---- the unboxing itself ----

  /**
   * The reported branch. A boolean is one or zero - what the sealed layer's {@code SIMPLE8B} column holds - a
   * null is absent, because both sealing codecs mark it so (issue #9721), and anything that is not a measurement
   * at all is {@link TimeSeriesNaN#ABSENT}, which every aggregate skips, rather than a measurement of zero.
   */
  @Test
  void aMutableSampleIsUnboxedTheWayTheSealedLayerUnboxesIt() {
    assertThat(TimeSeriesNaN.asMeasurement(Boolean.TRUE)).as("what SIMPLE8B stores for true").isEqualTo(1.0);
    assertThat(TimeSeriesNaN.asMeasurement(Boolean.FALSE)).isEqualTo(0.0);
    // a row hands a null back for a sample with no value, which both layers mark absent (issues #7743, #9721)
    assertThat(TimeSeriesNaN.asMeasurement(null)).as("a sample with no value is absent").isNaN();
    assertThat(TimeSeriesNaN.asMeasurement(42)).isEqualTo(42.0);
    assertThat(TimeSeriesNaN.asMeasurement(2.5f)).isEqualTo(2.5);
    assertThat(TimeSeriesNaN.asMeasurement(Double.NaN)).as("an absent sample stays absent").isNaN();
    assertThat(TimeSeriesNaN.asMeasurement("us-east")).as("not a measurement: a gap, never a zero").isNaN();
    assertThat(TimeSeriesNaN.asMeasurement(new Object())).isNaN();
  }

  // #7725
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

  // #7725
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

  // #7725
  /**
   * The upgrade-visible half of asking the CODEC rather than the type: on a TimeSeries type created before
   * issue #5475, {@code BOOLEAN} resolves through {@link ColumnDefinition#legacyCodecFor} to
   * {@code DICTIONARY}, not {@code SIMPLE8B}. Such a column is refused where a post-#5475 one is accepted.
   * <p>
   * That is deliberate and it is the only honest answer available. The sealed layer's decoder handles the two
   * numeric codecs and nothing else, so a legacy boolean genuinely is not stored as a number: the choices were
   * a real {@code 0.0} per sample (what the mutable path did, and the defect this issue reports), an
   * {@code IllegalArgumentException} out of the decoder once compaction had run, or a named refusal before
   * either layer is reached. But it does mean an aggregation that used to ANSWER on an old database now
   * throws, which reads as a regression to an operator upgrading in place unless it is written down - so it is
   * written down here and in the PR, and pinned so it cannot change by accident.
   */
  @Test
  void aLegacyBooleanColumnIsRefusedBecauseItReallyIsNotStoredAsANumber() {
    // Tied to the real legacy table rather than to a hand-picked codec, or the test would pin the wrong thing.
    assertThat(ColumnDefinition.legacyCodecFor(Type.BOOLEAN, ColumnRole.FIELD))
        .as("pre-#5475, a BOOLEAN field fell to the default arm").isEqualTo(TimeSeriesCodec.DICTIONARY);
    assertThat(ColumnDefinition.defaultCodecFor(Type.BOOLEAN, ColumnRole.FIELD))
        .as("since #5475 it is an integer column").isEqualTo(TimeSeriesCodec.SIMPLE8B);

    final ColumnDefinition legacy = new ColumnDefinition("active", Type.BOOLEAN, ColumnRole.FIELD,
        ColumnDefinition.legacyCodecFor(Type.BOOLEAN, ColumnRole.FIELD));
    assertThat(legacy.isNumericallyAggregatable())
        .as("the sealed decoder has no arm for DICTIONARY, so this column cannot be read as a number").isFalse();

    assertThatThrownBy(() -> TimeSeriesGateway.requireAggregatableColumn(legacy, AggregationType.SUM))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'active'")
        .hasMessageContaining("is not stored as a number");

    // COUNT never reads the column, so it keeps working on an old schema exactly as it did.
    TimeSeriesGateway.requireAggregatableColumn(legacy, AggregationType.COUNT);

    // And the post-#5475 column of the same declared type is still accepted, which is what makes the refusal
    // above a statement about STORAGE rather than about BOOLEAN.
    TimeSeriesGateway.requireAggregatableColumn(field("active", Type.BOOLEAN), AggregationType.SUM);
  }

  // #7725
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

  // #7725
  /**
   * A BOOLEAN column asked for on its own rather than beside a numeric sibling. The single-column
   * {@code aggregate()} entry point used to carry a third copy of the same {@code else 0.0}; it has since been
   * deleted (issue #8189), so the same question is asked of the path production actually uses, with one request
   * in the list instead of several.
   */
  @Test
  void aBooleanColumnAskedForOnItsOwnStillSumsItsTrueSamples() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Flags2 TIMESTAMP ts FIELDS (active BOOLEAN) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Flags2")).getEngine();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Flags2 SET ts = 0, active = true");
      database.command("sql", "INSERT INTO Flags2 SET ts = 1000, active = false");
      database.command("sql", "INSERT INTO Flags2 SET ts = 2000, active = true");
    });

    database.begin();
    try {
      final MultiColumnAggregationResult sum = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum")), HOUR, null);
      assertThat(sum.size()).isEqualTo(1);
      assertThat(sum.getValue(0L, 0)).as("two true samples, not zero").isEqualTo(2.0);
    } finally {
      database.commit();
    }
  }

  // #7725
  /**
   * The width guard, as the only public entry point now expresses it.
   * <p>
   * #7725's own change was that a position the row does not carry contributes {@link TimeSeriesNaN#ABSENT}
   * instead of a real {@code 0.0} - so SUM is absent rather than a total of zero for a column that was never
   * read. That rule still governs the MUTABLE half, in {@code mutableSample}. What a CALLER can observe changed
   * with issue #8140, which gave the sealed half a by-name refusal for a {@code columnIndex} naming no value
   * column, and with #8189, which deleted the single-column {@code aggregate()} that reached the mutable half
   * without passing it: {@code aggregateMulti} resolves every non-COUNT request's column up front, so an index
   * past the last one is refused rather than quietly answered as a gap. That is the better of the two - a gap
   * and a typo are indistinguishable to the caller, an exception is not.
   * <p>
   * COUNT is the contrast, and it is why the refusal is not simply "validate the index": COUNT names no column
   * at all, so its {@code columnIndex} is never resolved and counting the rows still works.
   */
  @Test
  void aColumnIndexPastTheRowWidthIsRefusedRatherThanAnsweredAsAGap() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Narrow TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Narrow")).getEngine();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Narrow SET ts = 0, value = 3.0");
      database.command("sql", "INSERT INTO Narrow SET ts = 1000, value = 4.0");
    });

    database.begin();
    try {
      // Row position 10 does not exist: the row carries the timestamp and one field, so row.length is 2.
      assertThatThrownBy(() -> engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(10, AggregationType.SUM, "sum")), HOUR, null))
          .as("a position no column occupies is a caller error, not a gap in the data")
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("out of range");

      final MultiColumnAggregationResult count = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(10, AggregationType.COUNT, "count")), HOUR, null);
      assertThat(count.getValue(0L, 0)).as("COUNT names no column, so it is unaffected").isEqualTo(2.0);

      // The counter-case: the column that IS there still answers a number.
      final MultiColumnAggregationResult real = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum")), HOUR, null);
      assertThat(real.getValue(0L, 0)).isEqualTo(7.0);
    } finally {
      database.commit();
    }
  }

  // #7725
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
        .as("COUNT(field) skips nulls, which the row counter cannot tell: the generic path has it (issue #8915)")
        .doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(planOf("SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM Labelled GROUP BY b"))
        .as("COUNT(*) counts rows, so it stays pushed down")
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

  // ---- #8140 ----
  // Issue #8140: {@link MultiColumnAggregationRequest#columnIndex()} names a position in the ENGINE ROW -
  // {@code 0} is the timestamp and {@code 1 + <ordinal among non-TIMESTAMP columns>} is a value column - and both
  // halves of the push-down resolve it that way.
  // <p>
  // The record's javadoc has always said so; the mutable half ({@code TimeSeriesEngine.mutableSample}, which does
  // {@code row[columnIndex]}) read it that way and the sealed half
  // ({@code TimeSeriesSealedStore.aggregateMultiBlocks}, which fed it to the column decoder) read it as a SCHEMA
  // index. The two numbers are equal only while the TIMESTAMP column is declared FIRST, which issue #7702 stopped
  // being a property of every declaration {@code CREATE TIMESERIES TYPE} can spell, so the same samples answered
  // one number before compaction and another after it.
  // <p>
  // Every type below declares a column BEFORE the timestamp, and every assertion here is made twice - once while
  // the samples are still in the mutable bucket and once after {@code compactAll()} - because either half alone
  // passes against a fix that corrects only the other.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/8140">issue #8140</a>
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7899">issue #7899</a>, the same confusion on the read surfaces

  private static final long   TS     = 1_700_000_000_000L;

  private static final long   HOUR8140   = 3_600_000L;

  private static final String TYPE   = "Aggr8140";

  /**
   * Schema order is {@code [v, host, ts, w]}; the engine row order is {@code [ts, v, host, w]}.
   * <ul>
   *   <li>{@code v} - schema index 0, row index 1: the two conventions disagree, and the SCHEMA reading points
   *       the mutable half at the timestamp, which is how five samples of 1..5 summed to 8.5e12;</li>
   *   <li>{@code w} - schema index 3, row index 3: the two conventions agree, so it is the control that says a
   *       fix has not merely shifted every index by one.</li>
   * </ul>
   */
  private void createSeries() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " FIELDS (v DOUBLE) TAGS (host STRING) TIMESTAMP ts FIELDS (w DOUBLE)");
    for (int i = 0; i < 5; i++)
      database.command("sql", "INSERT INTO " + TYPE + " SET ts = " + (TS + i * 1000L)
          + ", host = 'h', v = " + (i + 1) + ".0, w = " + (i + 1) * 10 + ".0");
  }

  private LocalTimeSeriesType tsType() {
    return (LocalTimeSeriesType) database.getSchema().getType(TYPE);
  }

  private List<ColumnDefinition> columns() {
    return tsType().getTsColumns();
  }

  // #8140
  // -------------------------------------------------------------------------------------------------
  // The SQL push-down - SelectExecutionPlanner.handleTimeSeriesAggregationPushDown
  // -------------------------------------------------------------------------------------------------

  /** The reporter's repro, asserted on both sides of the compaction it is triggered by. */
  @Test
  void theSqlPushDownAnswersTheSameNumbersBeforeAndAfterCompaction() throws Exception {
    createSeries();

    assertPushDown("mutable", 15.0, 5.0);
    tsType().getEngine().compactAll();
    assertPushDown("sealed", 15.0, 5.0);
  }

  // #8140
  /** The control column, whose row and schema indices coincide: it was right before and must stay right. */
  @Test
  void theSqlPushDownStillAnswersAColumnWhoseRowAndSchemaIndicesCoincide() throws Exception {
    createSeries();

    assertPushDownOnW("mutable", 150.0, 50.0);
    tsType().getEngine().compactAll();
    assertPushDownOnW("sealed", 150.0, 50.0);
  }

  // #8140
  /**
   * The shape that has no right answer under two conventions: half the samples sealed, half still mutable, one
   * bucket. {@code aggregateMulti} reads both layers in the same pass, so a disagreement shows up INSIDE a
   * single number rather than as two queries answering differently.
   */
  @Test
  void aPartlyCompactedSeriesAnswersOneNumberForOneBucket() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Split8140"
        + " FIELDS (v DOUBLE) TAGS (host STRING) TIMESTAMP ts");
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Split8140 SET ts = " + (TS + i * 1000L) + ", host = 'h', v = " + (i + 1) + ".0");

    ((LocalTimeSeriesType) database.getSchema().getType("Split8140")).getEngine().compactAll();

    for (int i = 3; i < 5; i++)
      database.command("sql", "INSERT INTO Split8140 SET ts = " + (TS + i * 1000L) + ", host = 'h', v = " + (i + 1) + ".0");

    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, sum(v) AS s, max(v) AS m FROM Split8140 GROUP BY b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue())
          .as("3 sealed samples and 2 mutable ones are one sum, not the sum of two readings").isEqualTo(15.0);
      assertThat(((Number) row.getProperty("m")).doubleValue()).isEqualTo(5.0);
    }
  }

  // #8140
  // -------------------------------------------------------------------------------------------------
  // The engine contract itself - both halves of aggregateMulti
  // -------------------------------------------------------------------------------------------------

  /**
   * The record read straight, with no producer in the way: {@code columnIndex} 1 is the first non-timestamp
   * column whatever the declaration says, on the mutable half and on the sealed half alike.
   */
  @Test
  void aggregateMultiResolvesColumnIndexAgainstTheRowOnBothHalves() throws Exception {
    createSeries();

    final TimeSeriesEngine engine = tsType().getEngine();
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_v"),
        new MultiColumnAggregationRequest(3, AggregationType.SUM, "sum_w"));

    final long bucket = Math.floorDiv(TS, HOUR8140) * HOUR8140;

    MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR8140, null);
    assertThat(result.getValue(bucket, 0)).as("mutable: row index 1 is 'v', not the timestamp").isEqualTo(15.0);
    assertThat(result.getValue(bucket, 1)).as("mutable: row index 3 is 'w'").isEqualTo(150.0);

    engine.compactAll();

    result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR8140, null);
    assertThat(result.getValue(bucket, 0)).as("sealed: the same row index must name the same column").isEqualTo(15.0);
    assertThat(result.getValue(bucket, 1)).isEqualTo(150.0);
  }

  // #8140
  /**
   * Row position 0 is the timestamp, so no request that reads a column may carry it - and the sealed half now
   * refuses one by name rather than decoding the timestamp as if it were a measurement. That refusal is only
   * safe because the push-down never builds such a request: a TIMESTAMP column is DELTA_OF_DELTA encoded, so
   * {@code ColumnDefinition.isNumericallyAggregatable()} is false for it and the planner declines the whole
   * push-down. Pinned here because the refusal and the guard have to stay on the same side of each other.
   */
  @Test
  void theSqlPushDownDeclinesAnAggregationOverTheTimestampColumn() {
    createSeries();

    assertThat(columns().get(2).isNumericallyAggregatable())
        .as("the TIMESTAMP column is not a measurement on either storage layer").isFalse();
    assertThat(planOf8140("SELECT ts.timeBucket('1h', ts) AS b, max(v) AS m FROM " + TYPE + " GROUP BY b"))
        .as("a DOUBLE field is still pushed down").contains("AGGREGATE FROM TIMESERIES " + TYPE);
    assertThat(planOf8140("SELECT ts.timeBucket('1h', ts) AS b, max(ts) AS m FROM " + TYPE + " GROUP BY b"))
        .as("the timestamp column is not, so row position 0 never reaches the engine")
        .doesNotContain("AGGREGATE FROM TIMESERIES");
  }

  // #8140
  /** COUNT names no column, so the plan spells it as the query did rather than as a placeholder index. */
  @Test
  void theExplainedPlanSpellsACountWithoutAColumn() {
    createSeries();

    final String plan = planOf8140(
        "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c, sum(v) AS s FROM " + TYPE + " GROUP BY b");
    assertThat(plan).contains("AGGREGATE FROM TIMESERIES " + TYPE).contains("count(*)").contains("sum(col1)");
  }

  // #8140
  // -------------------------------------------------------------------------------------------------
  // The helper the four producers share
  // -------------------------------------------------------------------------------------------------

  /** The conversion every producer applies between the name it resolved and the request it builds. */
  @Test
  void theGatewayTurnsASchemaIndexIntoTheRowIndexTheRequestCarries() {
    createSeries();
    final List<ColumnDefinition> columns = columns();

    assertThat(TimeSeriesGateway.findColumnIndex("v", columns)).as("'v' is declared first").isEqualTo(0);
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 0)).as("and is returned in row position 1").isEqualTo(1);
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 1)).as("'host' follows it in the row").isEqualTo(2);
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 2)).as("the TIMESTAMP column is row position 0").isZero();
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 3)).as("'w' is declared last and read last").isEqualTo(3);

    // The order the helper describes is the order the engine really returns, so this is not the test's opinion.
    assertThat(TimeSeriesGateway.columnNames(columns, null)).containsExactly("ts", "v", "host", "w");
  }

  // #8140
  /**
   * The factory every producer builds through, which is what makes the two rules hold at all four of them
   * rather than at whichever call sites remembered them (code review on PR #8192). Before this, three of the
   * four producers converted unconditionally, so an HTTP- or gRPC-built COUNT carried a real row index while
   * the record's javadoc said it named no column - harmless only for as long as every consumer kept checking
   * the type before the index.
   */
  @Test
  void theFactoryGivesACountNoColumnAndEveryOtherRequestTheRowIndex() {
    createSeries();
    final List<ColumnDefinition> columns = columns();

    assertThat(MultiColumnAggregationRequest.of(columns, 0, AggregationType.SUM, "s").columnIndex())
        .as("'v' has schema index 0 and row index 1").isEqualTo(1);
    assertThat(MultiColumnAggregationRequest.of(columns, 3, AggregationType.MAX, "m").columnIndex())
        .as("'w' is the control: its two indices coincide").isEqualTo(3);

    assertThat(MultiColumnAggregationRequest.of(columns, 0, AggregationType.COUNT, "c").columnIndex())
        .as("a COUNT names no column however it was asked for")
        .isEqualTo(MultiColumnAggregationRequest.NO_COLUMN);
    assertThat(MultiColumnAggregationRequest.of(columns, 2, AggregationType.COUNT, "c").columnIndex())
        .as("not even when the field it names is the TIMESTAMP column, which COUNT is exempt from refusing")
        .isEqualTo(MultiColumnAggregationRequest.NO_COLUMN);
    assertThat(MultiColumnAggregationRequest.count("c").columnIndex())
        .isEqualTo(MultiColumnAggregationRequest.NO_COLUMN);

    assertThat(MultiColumnAggregationRequest.NO_COLUMN)
        .as("position 0 of an engine row is the timestamp, so it cannot double as 'no column'").isNegative();
  }

  // -------------------------------------------------------------------------------------------------
  // Plumbing
  // -------------------------------------------------------------------------------------------------

  private String planOf8140(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
    }
  }

  private void assertPushDown(final String half, final double expectedSum, final double expectedMax) {
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, sum(v) AS s, max(v) AS m FROM " + TYPE + " GROUP BY b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue())
          .as("%s: sum(v) must sum 'v', not the timestamps beside it", half).isEqualTo(expectedSum);
      assertThat(((Number) row.getProperty("m")).doubleValue())
          .as("%s: max(v) must read 'v'", half).isEqualTo(expectedMax);
    }
  }

  private void assertPushDownOnW(final String half, final double expectedSum, final double expectedMax) {
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, sum(w) AS s, max(w) AS m FROM " + TYPE + " GROUP BY b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue()).as("%s: sum(w)", half).isEqualTo(expectedSum);
      assertThat(((Number) row.getProperty("m")).doubleValue()).as("%s: max(w)", half).isEqualTo(expectedMax);
    }
  }

  // ---- #8189 ----
  // Issue #8189: ONE column-index convention in the TimeSeries aggregation subsystem, not two.
  // <p>
  // After #8140 both halves of the multi-column push-down read
  // {@link MultiColumnAggregationRequest#columnIndex()} as a position in the ENGINE ROW - {@code 0} is the
  // timestamp, {@code 1 +} the ordinal among the non-TIMESTAMP columns is a value column. The single-column
  // {@code TimeSeriesEngine.aggregate} / {@code TimeSeriesSealedStore.aggregate} pair beside it counted
  // non-TIMESTAMP columns instead, with no {@code +1}. Both were correct and both were documented, which is
  // exactly the shape of a foot-gun: the two numbers coincide for any type whose TIMESTAMP column is declared
  // FIRST, so a caller that picked the wrong one saw nothing wrong until issue #7702 made a later TIMESTAMP
  // spellable - the way #8140 itself was found.
  // <p>
  // The pair is deleted rather than converted. Nothing in {@code src/main} called it - the three wire protocols
  // and the SQL push-down all go through {@code aggregateMulti}, which subsumes it - so its only callers were its
  // own tests, and a method that exists to be tested is the cheapest kind of convention to be rid of. Those tests
  // now ask {@code aggregateMulti} the same questions, which is a coverage improvement on top: they exercise the
  // path production actually takes.
  // <p>
  // This test is a REFLECTION GUARD because the property it pins is an absence, and an absence has no call site
  // to assert against. Re-adding either method, under any signature taking a column index, brings the second
  // convention back with it.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/8189">issue #8189</a>
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/8140">issue #8140</a>

  private static final long TS8189   = 1_700_000_000_000L;

  // #8189
  @Test
  void neitherAggregationClassCarriesASecondColumnIndexConventionAnyMore() {
    assertThat(singleColumnAggregationMethodsOf(TimeSeriesEngine.class))
        .as("TimeSeriesEngine.aggregate counted non-TIMESTAMP columns; aggregateMulti subsumes it").isEmpty();
    assertThat(singleColumnAggregationMethodsOf(TimeSeriesSealedStore.class))
        .as("and so did its sealed-half counterpart").isEmpty();
  }

  // #8189
  /**
   * The convention that remains, asserted where it can actually be wrong: a type declaring a column BEFORE the
   * timestamp, so the row index and the schema index of the same column are different numbers.
   */
  @Test
  void theOneConventionLeftIsTheRowIndexOnBothStorageHalves() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Conv8189"
        + " FIELDS (v DOUBLE) TAGS (host STRING) TIMESTAMP ts SHARDS 1");
    for (int i = 0; i < 5; i++)
      database.command("sql", "INSERT INTO Conv8189 SET ts = " + (TS8189 + i * 1000L)
          + ", host = 'h', v = " + (i + 1) + ".0");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Conv8189");
    final TimeSeriesEngine engine = tsType.getEngine();
    final long bucket = Math.floorDiv(TS8189, HOUR) * HOUR;

    // 'v' is SCHEMA index 0 and ROW index 1. The request carries the row index, and the gateway is the one
    // place that converts - which is why it is public.
    assertThat(TimeSeriesGateway.findColumnIndex("v", tsType.getTsColumns())).isZero();
    assertThat(TimeSeriesGateway.aggregationRowIndex(tsType.getTsColumns(), 0)).isEqualTo(1);

    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_v"));

    assertThat(engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null).getValue(bucket, 0))
        .as("mutable").isEqualTo(15.0);

    engine.compactAll();

    assertThat(engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null).getValue(bucket, 0))
        .as("sealed: the same number names the same column").isEqualTo(15.0);
  }

  /**
   * Two nets, and deliberately the broader pair. The load-bearing one is the SHAPE - anything returning an
   * {@link AggregationResult} is by construction the single-column path, since the multi-column one returns
   * {@link MultiColumnAggregationResult} - so reintroducing the convention under a different name does not slip
   * past. The NAME is matched as well, which is wider than it needs to be: a future method called
   * {@code aggregate} for some unrelated purpose would trip this too. That is the intended trade on a class
   * whose whole subject is one deleted pair of methods - a false positive costs whoever adds it one look at this
   * javadoc, and a false negative costs the second convention coming back unnoticed.
   */
  private static List<String> singleColumnAggregationMethodsOf(final Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(m -> AggregationResult.class.equals(m.getReturnType()) || "aggregate".equals(m.getName()))
        .map(Method::toGenericString)
        .toList();
  }

  // ---- #8190 ----
  // Issue #8190: a caller mistake must not change its exception type because of how many shards the type has.
  // <p>
  // {@code aggregateMulti} fans the sealed reads out over {@code shardExecutor} only when {@code shardCount > 1};
  // with one shard the identical work runs inline. So an {@code IllegalArgumentException} - a {@code columnIndex}
  // that names no value column, or one out of range - reached the caller as itself on a 1-shard type and, on a
  // multi-shard one, as {@code IOException("Parallel shard aggregation failed", <the real cause>)}. The two map to
  // a 400 / {@code INVALID_ARGUMENT} and a 500 / {@code INTERNAL} respectively, so the same mistake was a client
  // error on one declaration and a server fault on another, with the message one {@code getCause()} away.
  // <p>
  // Not reachable from a wire protocol or from the SQL push-down - {@code requireAggregatableColumn} and the
  // planner both refuse a TIMESTAMP column before the engine is asked, which
  // {@code TimeSeriesAggregationColumnContractTest} (#8140) pins. It is reachable for an EMBEDDED caller building a
  // request by hand, which is exactly the caller with no 400 to read.
  // <p>
  // The wrapping is older than #8140: {@code findNonTsColumnSchemaIndex} has always refused an out-of-range index
  // the same way, and took the same route. Both shapes are asserted below, on 1 shard and on 4, so the fix is
  // pinned as "the answer does not depend on the shard count" rather than as one exception type.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/8190">issue #8190</a>

  private static final long TS8190   = 1_700_000_000_000L;

  // #8190
  /** Row position 0 is the timestamp: refused by name, whatever the shard count. */
  @Test
  void aTimestampRowIndexIsTheSameCallerErrorOnOneShardAndOnFour() throws Exception {
    final TimeSeriesEngine single = seriesWith("Single8190", 1);
    final TimeSeriesEngine sharded = seriesWith("Sharded8190", 4);

    final List<MultiColumnAggregationRequest> overTheTimestamp =
        List.of(new MultiColumnAggregationRequest(0, AggregationType.SUM, "sum_ts"));

    assertThatThrownBy(() -> single.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, overTheTimestamp, HOUR, null))
        .as("1 shard runs the sealed read inline, so the refusal always arrived as itself")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not name a value column");

    assertThatThrownBy(() -> sharded.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, overTheTimestamp, HOUR, null))
        .as("4 shards fan out, and used to answer IOException for the identical mistake")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not name a value column")
        .isNotInstanceOf(IOException.class);
  }

  // #8190
  /** The pre-existing shape of the same wrapping: a row index past the last value column. */
  @Test
  void anOutOfRangeRowIndexIsTheSameCallerErrorOnOneShardAndOnFour() throws Exception {
    final TimeSeriesEngine single = seriesWith("SingleOor8190", 1);
    final TimeSeriesEngine sharded = seriesWith("ShardedOor8190", 4);

    final List<MultiColumnAggregationRequest> pastTheEnd =
        List.of(new MultiColumnAggregationRequest(99, AggregationType.SUM, "sum_nothing"));

    assertThatThrownBy(() -> single.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, pastTheEnd, HOUR, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range");

    assertThatThrownBy(() -> sharded.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, pastTheEnd, HOUR, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range")
        .isNotInstanceOf(IOException.class);
  }

  // #8190
  /** A valid request over four shards still answers, so the unwrapping did not swallow the success path. */
  @Test
  void aValidMultiShardAggregationStillAnswers() throws Exception {
    final TimeSeriesEngine sharded = seriesWith("Ok8190", 4);

    // The engine row is [ts, host, v] - timestamp first, then the non-TIMESTAMP columns in SCHEMA order - so
    // 'v' is row position 2 and the TAG declared before it is position 1 (issue #8140).
    final MultiColumnAggregationResult result = sharded.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_v")), HOUR, null);

    assertThat(result.getValue(Math.floorDiv(TS8190, HOUR) * HOUR, 0)).isEqualTo(55.0);
  }

  // ---- Helpers ----

  /**
   * A series whose samples are compacted into the sealed stores, because the refusal lives in
   * {@code aggregateMultiBlocks} and only the sealed half of the pass is what the shard executor runs.
   */
  private TimeSeriesEngine seriesWith(final String typeName, final int shards) throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + shards);
    for (int i = 0; i < 10; i++)
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + (TS8190 + i * 1000L)
          + ", host = 'h', v = " + (i + 1) + ".0");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    engine.compactAll();
    return engine;
  }

  // ---- #8260 ----
  // Regression for a gap found reviewing #8260's {@code ProjectionItem.cachedDefaultAlias} field:
  // {@code SelectExecutionPlanner.tryTimeSeriesAggregationPushDown()} reads {@code statement.getProjection()} - the
  // UNCOPIED projection {@code StatementCache.get()} hands out, the same instance for every caller of identical SQL
  // text - and calls {@code ProjectionItem.getProjectionAliasAsString()} on it directly, unlike every other planner
  // path, which works off a {@code copy()}. So an unaliased item in a push-down-eligible query is a
  // {@code ProjectionItem} genuinely shared and written by concurrent threads the very first time that SQL text is
  // parsed, which is exactly the scenario {@code cachedDefaultAlias} has to survive; it is now {@code volatile} for
  // this reason, matching {@code Projection.excludes}/{@code sourceColumns}.
  // <p>
  // A benign data race - the recomputed value is always the same immutable {@link com.arcadedb.query.sql.parser.Identifier}
  // either way - cannot be forced to fail deterministically by a unit test; what this pins down instead is that many
  // threads racing the push-down of the same, already-cached statement all get the correct default-aliased column
  // names and the correct aggregate values, with nothing thrown. The statement cache is warmed on the test's own
  // thread before the workers start (see below): {@code StatementCache.get()} only holds its lock long enough to
  // check and update the LRU map, and parses a cold entry OUTSIDE it, so racing threads on a genuinely cold cache
  // would each parse and execute their OWN private {@code Statement}/{@code ProjectionItem} tree instead of sharing
  // one - which would defeat the point of this test (found in review of #8267). The PLAN cache is deliberately left
  // cold, so every worker still plans - and reaches {@code tryTimeSeriesAggregationPushDown()} - concurrently,
  // against that one shared, cached {@code Statement}.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final int THREADS = 16;

  // #8260
  @Test
  void concurrentFirstExecutionsOfTheSameUnaliasedPushDownQueryAllSeeTheCorrectAliases() throws Exception {
    final String type = "PushDownAliasRace";
    database.command("sql", "CREATE TIMESERIES TYPE " + type + " TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(type);
    database.transaction(() -> {
      try {
        tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L }, new Object[] { 4.0, 1.0, 5.0 });
      } catch (final Exception e) {
        throw new IllegalStateException("cannot append the test samples", e);
      }
    });

    // No AS on the aggregate items: this is the shape whose default alias is computed through the UNCOPIED
    // projection in tryTimeSeriesAggregationPushDown(). The bucket alias stays explicit only because GROUP BY has
    // to name it; that item is unaliased too until then, exercising the same call.
    final String query = "SELECT ts.timeBucket('1h', ts) AS b, sum(v), count(*) FROM " + type + " GROUP BY b";

    // WARM THE STATEMENT CACHE FIRST, ON THIS THREAD: otherwise every worker below races StatementCache.get() on a
    // cold entry, and each parses (outside the cache's lock) its own private Statement/ProjectionItem tree instead
    // of sharing the one this test means to race on (see the class Javadoc).
    ((DatabaseInternal) database).getStatementCache().get(query);

    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final CyclicBarrier barrier = new CyclicBarrier(THREADS);
      final List<Callable<List<String>>> tasks = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        tasks.add(() -> {
          barrier.await(30, TimeUnit.SECONDS);
          final List<String> columns = new ArrayList<>();
          try (ResultSet rs = database.query("sql", query)) {
            assertThat(rs.hasNext()).isTrue();
            final Result row = rs.next();
            columns.addAll(row.getPropertyNames());
            assertThat(row.<Number>getProperty("sum(v)").doubleValue()).isEqualTo(10.0);
            assertThat(row.<Number>getProperty("count(*)").longValue()).isEqualTo(3L);
          }
          return columns;
        });
      }

      final List<Future<List<String>>> futures = pool.invokeAll(tasks, 30, TimeUnit.SECONDS);
      for (final Future<List<String>> future : futures)
        assertThat(future.get()).containsExactlyInAnyOrder("b", "sum(v)", "count(*)");
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }

  // ---- #9710 ----
  // Issue #9710: since the ungrouped TimeSeries aggregate push-down (#9612) the engine answers {@code min}/{@code max} through a
  // double, so a LONG sample needing more than 53 bits came back rounded - as a {@code Long} for some shapes and a {@code Double}
  // for others - and under a field filter even as a value the WHERE clause excludes. They must be the stored longs, which is what
  // a document twin holding the same rows answers.

  private static final long T0   = 1_767_225_600_000L;

  private static final long BASE = 9_007_199_254_740_990L;

 // 2^53 - 2

  private static final String[] WHERE = { "", //
      " WHERE bg BETWEEN 9007199254740991 AND 9007199254740993", //
      " WHERE bg >= 9007199254740993", //
      " WHERE bg > 9007199254740992 AND bg < 9007199254740997", //
      " WHERE host = 'h1'", //
      " WHERE host = 'h2' AND bg >= 9007199254740993", //
      " WHERE ts >= " + (T0 + 10_000L) + " AND ts < " + (T0 + 50_000L), //
      " WHERE ts > " + (T0 + 10_000L) + " AND ts <= " + (T0 + 50_000L) + " AND bg <= 9007199254740997" };

  private void createAndLoad(final int shards, final long base, final int from, final int to, final boolean createTypes) {
    if (createTypes) {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, d DOUBLE) SHARDS " + shards);
      database.command("sql", "CREATE DOCUMENT TYPE D");
    }
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?, d = ?", T0 + i * 1_000L, "h" + (i % 3),
              base + i % 9, (double) i);
    });
  }

  /** The only row of an ungrouped answer, each value spelled with its Java class. */
  private String row(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Result r = rs.next();
      assertThat(rs.hasNext()).isFalse();
      final List<String> out = new ArrayList<>();
      for (final String name : r.getPropertyNames().stream().sorted().toList()) {
        final Object value = r.getProperty(name);
        out.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
      }
      return out.toString();
    }
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private void assertEveryShapeAnswersTheStoredLongs() {
    for (final String where : WHERE) {
      final String sql = "SELECT min(bg) AS mn, max(bg) AS mx, count(*) AS c FROM X" + where;
      // the case this issue is about is the push-down, so a plan that stops using it would not prove anything
      assertThat(plan(sql.replace("FROM X", "FROM T"))).as(sql).contains("AGGREGATE FROM TIMESERIES").contains("ungrouped");
      assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
    }
  }

  // #9710
  @Test
  void ungroupedMinMaxOfALongPastTwoPow53AreTheStoredLongs() {
    for (final int shards : new int[] { 1, 3 }) {
      createAndLoad(shards, BASE, 0, 90, true);
      assertEveryShapeAnswersTheStoredLongs(); // mutable buffer
      database.command("sql", "COMPACT TIMESERIES TYPE T");
      assertEveryShapeAnswersTheStoredLongs(); // sealed blocks
      createAndLoad(shards, BASE, 90, 120, false);
      assertEveryShapeAnswersTheStoredLongs(); // sealed blocks plus a mutable tail
      database.command("sql", "DROP TYPE T");
      database.command("sql", "DROP TYPE D");
    }
  }

  // #9710
  @Test
  void theReportedAnswersAreExact() {
    createAndLoad(1, BASE, 0, 90, true);
    for (int pass = 0; pass < 2; pass++) {
      try (final ResultSet rs = database.query("sql", "SELECT min(bg) AS mn, max(bg) AS mx FROM T WHERE bg >= 9007199254740993")) {
        final Result r = rs.next();
        // 9007199254740993 rounds to 2^53 as a double, a value the WHERE clause excludes
        assertThat(r.<Object>getProperty("mn")).isEqualTo(9_007_199_254_740_993L);
        assertThat(r.<Object>getProperty("mx")).isEqualTo(9_007_199_254_740_998L);
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT min(bg) AS mn, max(bg) AS mx FROM T WHERE bg BETWEEN 9007199254740991 AND 9007199254740993")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("mn")).isEqualTo(9_007_199_254_740_991L);
        assertThat(r.<Object>getProperty("mx")).isEqualTo(9_007_199_254_740_993L);
      }
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  // #9710
  @Test
  void negativeLongsPastMinusTwoPow53AreExactToo() {
    createAndLoad(2, -BASE - 8, 0, 90, true); // -9007199254740998 .. -9007199254740990
    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(row("SELECT min(bg) AS mn, max(bg) AS mx FROM D"));
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(row("SELECT min(bg) AS mn, max(bg) AS mx FROM D"));
    assertThat(row("SELECT min(bg) AS mn FROM T")).isEqualTo("[mn=-9007199254740998(Long)]");
  }

  // #9710
  @Test
  void otherAggregatesOfTheSameRowAreUntouchedByTheExactRecount() {
    createAndLoad(2, BASE, 0, 90, true);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final String sql = "SELECT min(bg) AS mn, max(bg) AS mx, count(*) AS c, min(d) AS md, max(d) AS xd, avg(d) AS ad FROM X WHERE bg >= 9007199254740993";
    assertThat(row(sql.replace("FROM X", "FROM T"))).isEqualTo(row(sql.replace("FROM X", "FROM D")));
  }

  // #9710
  @Test
  void extremesAtTheThresholdAndAtTheLimitsOfALong() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, sm LONG) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    // bg: exactly 2^53 and its neighbours, plus both limits of a long; sm: small values, which no recount touches
    final long[] bg = { (1L << 53) - 1, 1L << 53, (1L << 53) + 1, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1 };
    database.transaction(() -> {
      for (int i = 0; i < 70; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?, sm = ?", T0 + i * 1_000L, "h" + (i % 2),
              bg[i % bg.length], (long) i);
    });
    final String[] queries = { "SELECT min(bg) AS mn, max(bg) AS mx, min(sm) AS ms, max(sm) AS xs FROM X",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE bg BETWEEN 9007199254740991 AND 9007199254740993",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE bg >= 9007199254740992 AND bg < 9223372036854775807",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE bg > -9223372036854775808 AND bg <= 9007199254740992",
        // min under 2^53, max over it: only the max is recounted
        "SELECT min(bg) AS mn, max(bg) AS mx, max(sm) AS xs FROM X WHERE bg >= 9007199254740991 AND bg <= 9007199254740993",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE host = 'h1'" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries)
        assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(
        "[mn=" + Long.MIN_VALUE + "(Long), mx=" + Long.MAX_VALUE + "(Long)]");
    assertThat(row("SELECT max(bg) AS mx FROM T WHERE bg <= 9007199254740992")).isEqualTo("[mx=9007199254740992(Long)]");
  }

  // #9710
  @Test
  void twoLongColumnsRecountedTogetherUnderAFilterOnAThird() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (a LONG, b LONG, f LONG) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      for (int i = 0; i < 80; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, a = ?, b = ?, f = ?", T0 + i * 1_000L, "h" + (i % 2),
              BASE + i % 9, -BASE - i % 7, (long) i);
    });
    final String[] queries = { "SELECT min(a) AS na, max(a) AS xa, min(b) AS nb, max(b) AS xb FROM X WHERE f >= 20 AND f < 60",
        "SELECT max(a) AS xa, min(a) AS na, max(a) AS xa2, min(b) AS nb, count(*) AS c FROM X WHERE f BETWEEN 3 AND 41 AND host = 'h0'",
        "SELECT min(a) AS na, max(a) AS xa, min(b) AS nb, max(b) AS xb FROM X" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries) {
        assertThat(plan(sql.replace("FROM X", "FROM T"))).as(sql).contains("ungrouped");
        assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
      }
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  // #9710
  @Test
  void samplesWithoutAMeasurementDoNotDisturbTheRecount() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, f LONG) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?, f = ?", T0 + i * 1_000L, "h" + (i % 2),
              i % 3 == 0 ? null : BASE + i % 9, (long) i);
    });
    // a sample stored without bg is a sample with no value since issue #9721, which min skips like the twin does
    final String[] queries = { "SELECT min(bg) AS mn, max(bg) AS mx, count(bg) AS c FROM X",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE f >= 10", "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE host = 'h1'" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries)
        assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  // #9710
  @Test
  void anEmptyRangeStillAnswersNull() {
    createAndLoad(1, BASE, 0, 90, true);
    final String sql = "SELECT min(bg) AS mn, max(bg) AS mx, count(*) AS c FROM X WHERE bg > 9007199254741000";
    assertThat(row(sql.replace("FROM X", "FROM T"))).isEqualTo(row(sql.replace("FROM X", "FROM D")));
  }

  // ---- #9718 ----
  // Issues #9718 and #9720: the TimeSeries aggregation push-down accumulated every aggregate as a double, and a sealed block kept the
  // min/max/sum of every numeric column as doubles, so on a LONG column past 2^53 a grouped or bucketed {@code min}/{@code max}
  // and any {@code sum} answered a rounded double, and the exact ungrouped {@code min}/{@code max} of #9710 had to re-read every
  // row. The integral columns now keep their block statistics as longs (a 128-bit total for the sum) and the aggregation folds them
  // as longs, so every shape answers the values and the Java types a document twin answers, from the block headers.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  // #9718
 // 2^53 - 2

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

  // #9718
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
    assertThat(row9718("SELECT sum(bg) AS s FROM T")).isEqualTo("[s=" + expected + "(BigDecimal)]");
  }

  // #9718
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

    assertThat(row9718("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(row9718("SELECT min(bg) AS mn, max(bg) AS mx FROM D"));
  }

  // #9718
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
    // 4 since issue #11274, which put the owner into the header after the layout version 3 introduced
    assertThat(TimeSeriesSealedStore.CURRENT_VERSION).isEqualTo(4);
  }

  // #9718
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

  // #9718
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

  // #9718
  @Test
  void anExactRequestRefusesAValueOnlyADoubleCarries() {
    final List<MultiColumnAggregationRequest> requests = List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"),
        MultiColumnAggregationRequest.count("c"));
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests, new boolean[] { true, false });
    // a double past 2^53 has already lost what the exact lane keeps, so the double-only overloads refuse an exact request
    assertThatThrownBy(() -> result.accumulate(0L, 0, 0x1p60)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> result.accumulateRow(0L, new double[] { 0x1p60, 1 })).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> result.accumulateSingleStat(0L, 0, 0x1p60, 1)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> result.accumulateBlockStats(0L, new double[] { 0x1p60, 1 }, new long[] { 1, 1 }))
        .isInstanceOf(IllegalStateException.class);
    // a COUNT is never exact, so it still takes them
    result.accumulateSingleStat(0L, 1, 1, 1);
    result.accumulateRow(0L, new double[] { 0, 1 }, new long[] { (1L << 60) + 1, 0 });
    assertThat(result.getExactValue(0L, 0)).isEqualTo((1L << 60) + 1);
    assertThat(result.getValue(0L, 1)).isEqualTo(2.0);
  }

  // #9718
  @Test
  void theExactSumAgreesWithBigIntegerOverRandomLongsAndTheLimits() {
    final SplittableRandom random = new SplittableRandom(9718L);
    final long[] limits = { Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE };
    for (int round = 0; round < 200; round++) {
      final long[] acc = new long[2];
      final long[] other = new long[2];
      BigInteger expected = BigInteger.ZERO;
      final int samples = 1 + random.nextInt(300);
      for (int i = 0; i < samples; i++) {
        final long v = random.nextInt(4) == 0 ? limits[random.nextInt(limits.length)] : random.nextLong();
        // half folded one by one, half into a second accumulator merged at the end, as shards are
        ExactLongSum.add(i % 2 == 0 ? acc : other, 0, v);
        expected = expected.add(BigInteger.valueOf(v));
      }
      ExactLongSum.add(acc, 0, other[0], other[1]);
      assertThat(ExactLongSum.toBigInteger(acc[0], acc[1])).as("round " + round).isEqualTo(expected);
      assertThat(ExactLongSum.fitsLong(acc[0], acc[1])).isEqualTo(expected.bitLength() < 64);
      assertThat(ExactLongSum.truncatedAverage(acc[0], acc[1], samples)).isEqualTo(expected.divide(BigInteger.valueOf(samples)).longValue());
    }
  }

  // #9718
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
    // the boundary of the long range: two Long.MAX_VALUE add up to 2^64 - 2, past it, and average back to Long.MAX_VALUE
    final long[] max = new long[2];
    ExactLongSum.add(max, 0, Long.MAX_VALUE);
    ExactLongSum.add(max, 0, Long.MAX_VALUE);
    assertThat(ExactLongSum.fitsLong(max[0], max[1])).isFalse();
    assertThat(ExactLongSum.truncatedAverage(max[0], max[1], 2)).isEqualTo(Long.MAX_VALUE);
    assertThat(ExactLongSum.average(max[0], max[1], 2)).isEqualTo((double) Long.MAX_VALUE);
    final long[] min = new long[2];
    ExactLongSum.add(min, 0, Long.MIN_VALUE);
    ExactLongSum.add(min, 0, Long.MIN_VALUE + 1);
    // -2^63 + 0.5, truncated toward zero
    assertThat(ExactLongSum.truncatedAverage(min[0], min[1], 2)).isEqualTo(Long.MIN_VALUE + 1);
    assertThat(ExactLongSum.toNumber(min[0], min[1])).isEqualTo(new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.TWO).add(BigInteger.ONE)));
  }

  // #9718
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

  private String row9718(final String sql) {
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
