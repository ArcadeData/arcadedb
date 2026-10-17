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
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.WriteReport;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9809: a quoted datetime written to a TIMESERIES DATETIME column through the line protocol failed
 * with a raw {@code ClassCastException} ("Failed to append timeseries samples"), because only the SQL INSERT converted a
 * temporal value before it reached the storage layer, whose row source casts every value to {@code Number}. The write now goes
 * through the one conversion {@code INSERT} uses, and a value no column can store is dropped from the write and named, like
 * every other refusal of that path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9809LineProtocolTemporalValueTest extends TestHelper {

  private static final long T0 = 1_700_000_000_000L;

  private WriteReport write(final Sample... samples) throws Exception {
    return TimeSeriesGateway.write((DatabaseInternal) database, new ArrayList<>(List.of(samples)));
  }

  private static Sample field(final String type, final long ts, final String name, final Object value) {
    return new Sample(type, Map.of(), Map.of(name, value), ts);
  }

  private Object valueAt(final String type, final String column, final long ts) {
    try (final ResultSet rs = database.query("sql", "SELECT " + column + " AS c FROM " + type + " WHERE ts = " + ts)) {
      assertThat(rs.hasNext()).as("a row at %d", ts).isTrue();
      return rs.next().getProperty("c");
    }
  }

  private long rows(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void aQuotedDatetimeIsStoredAsTheSameInstantSqlStores() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");

    final WriteReport report = write(field("T", T0, "v", "2023-11-14 22:15:23"));

    assertThat(report.isComplete()).isTrue();
    assertThat(report.written()).isEqualTo(1);
    assertThat(report.unreadableValues()).isEmpty();

    database.command("sql", "INSERT INTO T SET ts = " + (T0 + 1000) + ", v = '2023-11-14 22:15:23'");
    assertThat(valueAt("T", "v", T0)).isNotNull().isEqualTo(valueAt("T", "v", T0 + 1000));
  }

  @Test
  void aNumberIsStillACountInTheColumnUnit() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");

    final WriteReport report = write(field("T", T0, "v", 1_700_000_002_000L));
    assertThat(report.isComplete()).isTrue();

    database.command("sql", "INSERT INTO T SET ts = " + (T0 + 1000) + ", v = 1700000002000");
    assertThat(valueAt("T", "v", T0)).isEqualTo(valueAt("T", "v", T0 + 1000));
  }

  @Test
  void aQuotedCountIsACountInTheColumnUnit() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");

    final WriteReport report = write(field("T", T0, "v", "1700000002000"), field("T", T0 + 1000, "v", 1_700_000_002_000L));

    assertThat(report.isComplete()).isTrue();
    assertThat(valueAt("T", "v", T0)).isEqualTo(valueAt("T", "v", T0 + 1000));
  }

  @Test
  void everyTemporalPrecisionReadsTheTextTheWaySqlDoes() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE P TIMESTAMP ts FIELDS (d DATE, s DATETIME_SECOND, m DATETIME_MICROS,"
        + " n DATETIME_NANOS) SHARDS 1");

    final String text = "2023-11-14 22:15:23";
    final WriteReport report = write(new Sample("P", Map.of(), Map.of("d", "2023-11-14", "s", text, "m", text, "n", text), T0));
    assertThat(report.isComplete()).as("%s", report).isTrue();

    database.command("sql", "INSERT INTO P SET ts = " + (T0 + 1000)
        + ", d = '2023-11-14', s = '" + text + "', m = '" + text + "', n = '" + text + "'");

    for (final String column : List.of("d", "s", "m", "n"))
      assertThat(valueAt("P", column, T0)).as(column).isNotNull().isEqualTo(valueAt("P", column, T0 + 1000));
  }

  @Test
  void anUnreadableDatetimeDropsOnlyItsSampleAndNamesTheColumn() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");

    final WriteReport report = write(field("T", T0, "v", "2023-11-14 22:15:23"), field("T", T0 + 1, "v", "not a date"),
        field("T", T0 + 2, "v", 1_700_000_002_000L));

    assertThat(report.isComplete()).isFalse();
    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(1);
    assertThat(report.unreadableValues()).hasSize(1);
    assertThat(report.unreadableValues().iterator().next()).startsWith("T.v (field)").contains("'v'");
    assertThat(rows("T")).isEqualTo(2);
  }

  @Test
  void aValueOfTheWrongKindIsDroppedAndNamedInsteadOfFailingTheWholeWrite() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DATETIME, n LONG) SHARDS 1");

    final WriteReport report = write(field("T", T0, "v", true), field("T", T0 + 1, "n", "abc"), field("T", T0 + 2, "n", "42"),
        field("T", T0 + 3, "n", 43L));

    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(2);
    assertThat(report.unreadableValues()).hasSize(2);
    assertThat(report.unreadableValues()).anyMatch(entry -> entry.startsWith("T.v (field)"));
    assertThat(report.unreadableValues()).anyMatch(entry -> entry.startsWith("T.n (field)"));
    assertThat(valueAt("T", "n", T0 + 2)).isEqualTo(42L);
    assertThat(valueAt("T", "n", T0 + 3)).isEqualTo(43L);
  }

  @Test
  void aTypedTagArrivesAsTextAndIsStoredInItsType() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Tagged TIMESTAMP ts TAGS (ok BOOLEAN, day DATE) FIELDS (v DOUBLE) SHARDS 1");

    final WriteReport report = write(new Sample("Tagged", Map.of("ok", "true", "day", "2023-11-14"), Map.of("v", 1.5), T0));
    assertThat(report.isComplete()).as("%s", report).isTrue();

    database.command("sql", "INSERT INTO Tagged SET ts = " + (T0 + 1000) + ", ok = true, day = '2023-11-14', v = 1.5");
    assertThat(valueAt("Tagged", "ok", T0)).isEqualTo(true);
    assertThat(valueAt("Tagged", "day", T0)).isNotNull().isEqualTo(valueAt("Tagged", "day", T0 + 1000));
  }

  @Test
  void aFiniteNumberTooLargeForAFloatColumnIsRefusedWhileNaNAndInfinityAreValues() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Floats TIMESTAMP ts FIELDS (f FLOAT, d DOUBLE) SHARDS 1");

    // THE EDGE OF THE RANGE: Float.MAX_VALUE IS A FLOAT, THE NEXT DOUBLE ABOVE IT IS NOT
    final WriteReport edge = write(field("Floats", T0 + 20, "f", (double) Float.MAX_VALUE),
        field("Floats", T0 + 21, "f", Math.nextUp((double) Float.MAX_VALUE)));
    assertThat(edge.written()).isEqualTo(1);
    assertThat(edge.dropped()).isEqualTo(1);
    assertThat(((Number) valueAt("Floats", "f", T0 + 20)).floatValue()).isEqualTo(Float.MAX_VALUE);

    final WriteReport report = write(field("Floats", T0, "f", 1.0e300), field("Floats", T0 + 1, "f", 1.5e30),
        field("Floats", T0 + 2, "f", Double.NaN), field("Floats", T0 + 3, "f", Double.POSITIVE_INFINITY),
        field("Floats", T0 + 4, "d", 1.0e300), field("Floats", T0 + 5, "d", new BigDecimal("1e500")),
        field("Floats", T0 + 6, "d", new BigDecimal("1e300")));

    assertThat(report.written()).isEqualTo(5);
    assertThat(report.dropped()).isEqualTo(2);
    assertThat(report.unreadableValues()).hasSize(2);
    assertThat(report.unreadableValues()).anyMatch(entry -> entry.startsWith("Floats.f (field)") && entry.contains("out of range"));
    assertThat(report.unreadableValues()).anyMatch(entry -> entry.startsWith("Floats.d (field)") && entry.contains("out of range"));
    assertThat(((Number) valueAt("Floats", "f", T0 + 1)).floatValue()).isEqualTo(1.5e30f);
    // NaN IS THE ENGINE'S ABSENT MARKER FOR A FLOATING POINT COLUMN, SO IT IS WRITTEN (COUNTED ABOVE) AND READS BACK AS NO VALUE
    assertThat(valueAt("Floats", "f", T0 + 2)).isNull();
    assertThat(((Number) valueAt("Floats", "f", T0 + 3)).doubleValue()).isInfinite();
  }

  @Test
  void digitsPastTheLongRangeAreDroppedNotFailed() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Stamps TIMESTAMP ts FIELDS (at DATETIME, n LONG) SHARDS 1");

    final WriteReport report = write(field("Stamps", T0, "at", "99999999999999999999"), field("Stamps", T0 + 1, "n", "99999999999999999999"),
        field("Stamps", T0 + 2, "at", "1700000002000"));

    assertThat(report.written()).isEqualTo(1);
    assertThat(report.dropped()).isEqualTo(2);
    assertThat(report.unreadableValues()).hasSize(2);
  }

  @Test
  void aRefusedSampleLeavesNoPartialRowWhateverItsPlaceInTheBatch() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Pair TIMESTAMP ts FIELDS (a LONG, b LONG) SHARDS 1");

    // THE COLUMN BEFORE THE REFUSED ONE IS ALREADY WRITTEN WHEN THE REFUSAL COMES: FIRST, MIDDLE AND LAST PLACE
    final Sample first = new Sample("Pair", Map.of(), Map.of("a", 11L, "b", "bad"), T0);
    final Sample good = new Sample("Pair", Map.of(), Map.of("a", 1L, "b", 2L), T0 + 1);
    final Sample middle = new Sample("Pair", Map.of(), Map.of("a", 22L, "b", "bad"), T0 + 2);
    final Sample alsoGood = new Sample("Pair", Map.of(), Map.of("a", 3L, "b", 4L), T0 + 3);
    final Sample last = new Sample("Pair", Map.of(), Map.of("a", 33L, "b", "bad"), T0 + 4);

    final WriteReport report = write(first, good, middle, alsoGood, last);

    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(3);
    assertThat(rows("Pair")).isEqualTo(2);
    try (final ResultSet rs = database.query("sql", "SELECT a, b FROM Pair ORDER BY ts")) {
      final Result one = rs.next();
      assertThat(one.<Long>getProperty("a")).isEqualTo(1L);
      assertThat(one.<Long>getProperty("b")).isEqualTo(2L);
      final Result two = rs.next();
      assertThat(two.<Long>getProperty("a")).isEqualTo(3L);
      assertThat(two.<Long>getProperty("b")).isEqualTo(4L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aRefusedSampleLeavesNothingBehindForTheNextOneWithAMissingValue() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Gap TIMESTAMP ts FIELDS (a LONG, b LONG) SHARDS 1");

    // THE REFUSED SAMPLE HAS WRITTEN a = 22 INTO THE ROW THE NEXT ONE TAKES; THAT ONE HAS NO a AT ALL
    final Sample good = new Sample("Gap", Map.of(), Map.of("a", 1L, "b", 2L), T0);
    final Sample refused = new Sample("Gap", Map.of(), Map.of("a", 22L, "b", "bad"), T0 + 1);
    final Sample withoutA = new Sample("Gap", Map.of(), Map.of("b", 4L), T0 + 2);

    final WriteReport report = write(good, refused, withoutA);

    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(1);
    assertThat(valueAt("Gap", "b", T0 + 2)).isEqualTo(4L);
    assertThat(valueAt("Gap", "a", T0 + 2)).as("no value leaks from the refused sample").isNull();
  }

  @Test
  void anArbitraryPrecisionNumberIsCheckedExactlyAgainstTheBounds() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Big TIMESTAMP ts FIELDS (l LONG) SHARDS 1");

    final WriteReport report = write(field("Big", T0, "l", new BigDecimal("9223372036854775807")),
        field("Big", T0 + 1, "l", new BigDecimal("9223372036854775808")), field("Big", T0 + 2, "l", BigInteger.TWO.pow(63)),
        field("Big", T0 + 3, "l", BigInteger.TWO.pow(63).negate()), field("Big", T0 + 4, "l", new BigDecimal("1e30")));

    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(3);
    assertThat(valueAt("Big", "l", T0)).isEqualTo(Long.MAX_VALUE);
    assertThat(valueAt("Big", "l", T0 + 3)).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void aBooleanColumnRefusesTextThatIsNotABooleanInsteadOfStoringFalse() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Flags TIMESTAMP ts FIELDS (flag BOOLEAN) SHARDS 1");

    final WriteReport report = write(field("Flags", T0, "flag", "TRUE"), field("Flags", T0 + 1, "flag", "false"),
        field("Flags", T0 + 2, "flag", "yes"), field("Flags", T0 + 3, "flag", "tru"), field("Flags", T0 + 4, "flag", ""),
        field("Flags", T0 + 5, "flag", 1L), field("Flags", T0 + 6, "flag", Boolean.TRUE));

    assertThat(report.written()).isEqualTo(3);
    assertThat(report.dropped()).isEqualTo(4);
    // ONE ENTRY PER REASON: THREE TEXTS AND A NUMBER ARE REFUSED FOR TWO OF THEM
    assertThat(report.unreadableValues()).hasSize(2);
    assertThat(report.unreadableValues()).allMatch(entry -> entry.startsWith("Flags.flag (field)") && entry.contains("BOOLEAN"));
    assertThat(valueAt("Flags", "flag", T0)).isEqualTo(true);
    assertThat(valueAt("Flags", "flag", T0 + 1)).isEqualTo(false);
    assertThat(valueAt("Flags", "flag", T0 + 6)).isEqualTo(true);
  }

  @Test
  void aNumberTheColumnCannotHoldIsRefusedInsteadOfTruncated() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Narrow TIMESTAMP ts FIELDS (b BYTE, s SHORT, i INTEGER, l LONG, d DOUBLE) SHARDS 1");

    final WriteReport report = write(field("Narrow", T0, "b", 300L), field("Narrow", T0 + 1, "b", 100L),
        field("Narrow", T0 + 2, "s", 40_000L), field("Narrow", T0 + 3, "i", 3_000_000_000L), field("Narrow", T0 + 4, "i", 7.5e12),
        field("Narrow", T0 + 5, "l", 1.0e30), field("Narrow", T0 + 6, "l", Double.NaN), field("Narrow", T0 + 7, "d", 1.0e300),
        field("Narrow", T0 + 8, "l", Long.MAX_VALUE), field("Narrow", T0 + 9, "i", Integer.MIN_VALUE));

    assertThat(report.written()).isEqualTo(4);
    assertThat(report.dropped()).isEqualTo(6);
    assertThat(report.unreadableValues()).isNotEmpty().allMatch(entry -> entry.contains("out of range"));
    assertThat(valueAt("Narrow", "b", T0 + 1)).isEqualTo((byte) 100);
    assertThat(valueAt("Narrow", "l", T0 + 8)).isEqualTo(Long.MAX_VALUE);
    assertThat(valueAt("Narrow", "i", T0 + 9)).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  void aStringLongerThanTheRowCanHoldIsDroppedAloneInsteadOfFailingTheBatch() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Texts TIMESTAMP ts FIELDS (note STRING) SHARDS 1");

    final String fits = "x".repeat(TimeSeriesBucket.MAX_STRING_BYTES);
    // 86 THREE-BYTE CHARS ARE 258 BYTES: SHORT IN CHARS, TOO LONG IN BYTES
    final String tooLongInBytes = "€".repeat(86);
    final WriteReport report = write(field("Texts", T0, "note", fits), field("Texts", T0 + 1, "note", fits + "x"),
        field("Texts", T0 + 2, "note", tooLongInBytes), field("Texts", T0 + 3, "note", "short"));

    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(2);
    // THE MESSAGE STATES THE LENGTH (NEVER THE TEXT), SO THE 257 AND THE 258 BYTE TEXTS ARE TWO REASONS
    assertThat(report.unreadableValues()).hasSize(2);
    assertThat(report.unreadableValues()).allMatch(entry -> entry.contains("'note'") && entry.contains("the limit is 256"));
    assertThat(report.unreadableValues()).anyMatch(entry -> entry.contains("257 UTF-8 bytes"));
    assertThat(report.unreadableValues()).anyMatch(entry -> entry.contains("258 UTF-8 bytes"));
    assertThat(valueAt("Texts", "note", T0)).isEqualTo(fits);
    assertThat(valueAt("Texts", "note", T0 + 3)).isEqualTo("short");
  }

  @Test
  void theReportIsCappedButDroppedCountsEverySample() throws Exception {
    final int columns = TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES + 20;
    final StringBuilder ddl = new StringBuilder("CREATE TIMESERIES TYPE Wide TIMESTAMP ts FIELDS (");
    for (int c = 0; c < columns; c++)
      ddl.append(c == 0 ? "" : ", ").append('f').append(c).append(" LONG");
    database.command("sql", ddl.append(") SHARDS 1").toString());

    // ONE UNREADABLE SAMPLE PER COLUMN, SO EVERY ENTRY OF THE REPORT IS A DIFFERENT ONE
    final Sample[] samples = new Sample[columns];
    for (int c = 0; c < columns; c++)
      samples[c] = field("Wide", T0 + c, "f" + c, "not a number");

    final WriteReport report = write(samples);

    assertThat(report.written()).isZero();
    assertThat(report.dropped()).isEqualTo(columns);
    assertThat(report.unreadableValues()).hasSize(TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES);
  }

  @Test
  void aDatetimeColumnRefusesANumberOutsideTheLongRangeInsteadOfTruncatingIt() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Moments TIMESTAMP ts FIELDS (at DATETIME, day DATE) SHARDS 1");

    final WriteReport report = write(field("Moments", T0, "at", Double.NaN), field("Moments", T0 + 1, "at", 1.0e30),
        field("Moments", T0 + 2, "at", new BigDecimal("9223372036854775808")), field("Moments", T0 + 3, "day", Double.POSITIVE_INFINITY),
        field("Moments", T0 + 4, "at", 1_700_000_002_000L));

    assertThat(report.written()).isEqualTo(1);
    assertThat(report.dropped()).isEqualTo(4);
    assertThat(report.unreadableValues()).isNotEmpty().allMatch(entry -> entry.contains("out of range"));
  }

  @Test
  void aBooleanColumnRefusesANumberOnEverySurfaceAndTheStorageLayerNamesTheColumn() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Switches TIMESTAMP ts FIELDS (on_ BOOLEAN) SHARDS 1");

    // SQL INSERT: storableValue converts the value the statement wrote, so a number is refused as every other surface refuses it
    // (issue #10137, where the document layer used to turn it into a Boolean first)
    assertThatCode(() -> database.command("sql", "INSERT INTO Switches SET ts = " + T0 + ", on_ = true")).doesNotThrowAnyException();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Switches SET ts = " + (T0 + 1) + ", on_ = 1"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'on_'");
    assertThatCode(() -> database.command("sql", "INSERT INTO Switches SET ts = " + (T0 + 2) + ", on_ = 'true'")).doesNotThrowAnyException();
    assertThat(valueAt("Switches", "on_", T0)).isEqualTo(true);

    // The engine API: no writer in the codebase hands it a number or text for a boolean, so the storage layer names the column
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Switches")).getEngine();
    assertThatThrownBy(() -> engine.appendSamples(new long[] { T0 + 10 }, new Object[] { 1L }))
        .isInstanceOf(IOException.class).hasRootCauseInstanceOf(IllegalArgumentException.class)
        .rootCause().hasMessageContaining("'on_'").hasMessageContaining("BOOLEAN");
    assertThat(rows("Switches")).as("nothing was stored for the refused calls").isEqualTo(2);
  }

  @Test
  void theEngineApiRefusesAWrongValueByColumnNameInsteadOfACastFailure() {
    database.command("sql", "CREATE TIMESERIES TYPE E TIMESTAMP ts FIELDS (v DATETIME, s STRING) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("E")).getEngine();

    // The shard wraps whatever the append throws in an IOException, so the refusal is the root cause
    assertThatThrownBy(() -> engine.appendSamples(new long[] { T0 }, new Object[] { "2023-11-14 22:15:23" }, new Object[] { "x" }))
        .isInstanceOf(IOException.class).hasRootCauseInstanceOf(IllegalArgumentException.class)
        .rootCause().hasMessageContaining("'v'").hasMessageContaining("DATETIME");
    assertThatThrownBy(() -> engine.appendSamples(new long[] { T0 }, new Object[] { 1L }, new Object[] { 42L }))
        .isInstanceOf(IOException.class).hasRootCauseInstanceOf(IllegalArgumentException.class)
        .rootCause().hasMessageContaining("'s'");
  }

  @Test
  void aColumnReportsNoUnreadableValueWhenEverySampleIsReadable() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");

    final WriteReport report = write(field("T", T0, "v", 1.5), field("T", T0 + 1, "v", 2L));

    assertThat(report.isComplete()).isTrue();
    assertThat(report.unreadableValues()).isEmpty();
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T ORDER BY ts")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    assertThat(rows).hasSize(2);
  }
}
