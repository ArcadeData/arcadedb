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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
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
    assertThat(report.unreadableValues()).hasSize(1);
    assertThat(report.unreadableValues().iterator().next()).contains("'note'").contains("longer than");
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
