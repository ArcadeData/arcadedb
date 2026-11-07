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
import com.arcadedb.database.Document;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.WriteReport;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Temporal (DATE/DATETIME) columns and temporal bounds of time series types across SQL, gateway and engine surfaces.
 */
class TimeSeriesTemporalColumnsTest extends TestHelper {
  private static final long MILLIS = 1_700_000_000_000L;

  // Issue #10233: SQL and the line protocol / gRPC / Prometheus read a number given to a DATE/DATETIME column of a TIMESERIES type
  // differently. That split is the intended contract of each surface and stays: SQL reads the column like a document property of the
  // same type (a number is epoch MILLIS whatever the precision, epoch DAYS for a DATE, issues #9643 and #9724), the wire protocols
  // read a number, and text of digits, as a count in the column's own unit. What was not intended is how SQL refused a value: by a
  // bare {@code IllegalArgumentException} out of the document layer, where every other refusal of an INSERT into a TIMESERIES type is
  // a {@link CommandExecutionException} naming the column. The temporal conversion now runs in the time series step, once.
  // #10233
  @Test
  void textOfDigitsIsRefusedByCommandExecutionExceptionNamingTheColumn() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE D TIMESTAMP ts TAGS (k DATETIME) FIELDS (d DATETIME, dd DATE) SHARDS 1");

    // not on the DATE column: digits there read as the number they spell, and one the size of epoch millis is that day (issue
    // #11025, pinned in the TemporalSurfaceMatrix nested class)
    for (final String column : new String[] { "k", "d" })
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO D SET ts = 1, " + column + " = '" + MILLIS + "'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column '" + column + "'");
    // a document property of the same type refuses it too: SQL treats the column as its document twin
    database.command("sql", "CREATE DOCUMENT TYPE Twin");
    database.command("sql", "CREATE PROPERTY Twin.d DATETIME");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Twin SET d = '" + MILLIS + "'")))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(rows("D")).isEmpty();
  }

  // #10233
  @Test
  void aValueNoDateCanStandForIsRefusedNamingTheColumn() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE B TIMESTAMP ts FIELDS (d DATETIME, dd DATE) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO B SET ts = 1, d = 'not a date'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'd'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO B SET ts = 1, d = true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'd'");
    // a number no date can hold, as on a DATE document property: past any day count it is epoch millis (issue #11025), and these
    // fall before the first day a java.util.Date can hold
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO B SET ts = 1, dd = " + (Long.MIN_VALUE + 1)))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'dd'");
    assertThat(rows("B")).isEmpty();
  }

  // #10233
  /**
   * The documented split, pinned on both sides: the same number is epoch millis through SQL and a count in the column's unit through
   * the gateway, and SQL stores what the document twin stores.
   */
  @Test
  void sqlReadsANumberLikeTheDocumentTwinAndTheGatewayAsACountInTheColumnUnit() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE S TIMESTAMP ts FIELDS (d DATETIME, ds DATETIME_SECOND, dm DATETIME_MICROS, dn DATETIME_NANOS, dd DATE) SHARDS 1");
    database.command("sql", "INSERT INTO S SET ts = 1, d = " + MILLIS + ", ds = " + MILLIS + ", dm = " + MILLIS + ", dn = " + MILLIS
        + ", dd = 19675");

    final Object[] sql = rowAt("S", 1);
    assertThat(((Number) sql[1]).longValue()).isEqualTo(MILLIS);
    assertThat(((Number) sql[2]).longValue()).isEqualTo(MILLIS / 1_000L);
    assertThat(((Number) sql[3]).longValue()).isEqualTo(MILLIS * 1_000L);
    assertThat(((Number) sql[4]).longValue()).isEqualTo(MILLIS * 1_000_000L);
    assertThat(((Number) sql[5]).longValue()).isEqualTo(19675L);

    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database, new ArrayList<>(List.of(
        new Sample("S", Map.of(), Map.of("d", MILLIS, "ds", MILLIS, "dm", MILLIS, "dn", "" + MILLIS, "dd", 19675L), 2))));
    assertThat(report.written()).isEqualTo(1);
    final Object[] gateway = rowAt("S", 2);
    for (int c = 1; c <= 4; c++)
      assertThat(((Number) gateway[c]).longValue()).as("column %d", c).isEqualTo(MILLIS);
    assertThat(((Number) gateway[5]).longValue()).isEqualTo(19675L);

    // the document twin of each column stores what SQL stored
    database.command("sql", "CREATE DOCUMENT TYPE Twin");
    database.command("sql", "CREATE PROPERTY Twin.dn DATETIME_NANOS");
    database.command("sql", "CREATE PROPERTY Twin.dd DATE");
    database.transaction(() -> database.command("sql", "INSERT INTO Twin SET dn = " + MILLIS + ", dd = 19675"));
    final var twin = database.query("sql", "SELECT FROM Twin").next();
    final var series = database.query("sql", "SELECT FROM S WHERE ts = 1").next();
    assertThat(series.<Object>getProperty("dn")).isEqualTo(twin.getProperty("dn"));
    assertThat(series.<Object>getProperty("dd")).isEqualTo(twin.getProperty("dd"));
  }

  // #10233
  /** A date given as text names the same instant on every surface. */
  @Test
  void aDateGivenAsTextIsTheSameInstantOnEverySurface() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE X TIMESTAMP ts FIELDS (dm DATETIME_MICROS) SHARDS 1");
    database.command("sql", "INSERT INTO X SET ts = 1, dm = '2023-11-14 22:13:20'");
    TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("X", Map.of(), Map.of("dm", "2023-11-14 22:13:20"), 2))));
    assertThat(((Number) rowAt("X", 1)[1]).longValue()).isEqualTo(MILLIS * 1_000L).isEqualTo(((Number) rowAt("X", 2)[1]).longValue());
  }

  private Object[] rowAt(final String typeName, final long ts) throws Exception {
    for (final Object[] row : rows(typeName))
      if (((Number) row[0]).longValue() == ts)
        return row;
    throw new AssertionError("no sample at " + ts + " in " + typeName);
  }

  private List<Object[]> rows(final String typeName) throws Exception {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }

  private static final long BASE_TS = 1_700_000_000_000L;

  private int documentTypes;

  /**
   * Declared type, a numeric literal and the epoch count the column must hold for it. A number assigned to a
   * datetime property is read by the document layer as epoch MILLIS whatever the declared precision, and a number
   * assigned to a DATE as epoch DAYS (issue #9324), so {@code DATETIME} and {@code DATE} hold the literal itself and
   * the other precisions hold the same instant in their own unit - what a document property of that type stores.
   */
  private static final Object[][] NUMERIC_LITERALS = {
      { "DATETIME", 1_700_000_123_456L, 1_700_000_123_456L },
      { "DATE", 19_675L, 19_675L },
      { "DATETIME_SECOND", 1_700_000_123_456L, 1_700_000_123L },
      { "DATETIME_MICROS", 1_700_000_123_456L, 1_700_000_123_456_000L },
      { "DATETIME_NANOS", 1_700_000_123_456L, 1_700_000_123_456_000_000L },
  };

  // Issue #9643: {@code INSERT INTO} a TIMESERIES type threw a raw {@code ClassCastException} for every
  // {@code DATETIME}, {@code DATE}, {@code DATETIME_SECOND}, {@code DATETIME_MICROS} and {@code DATETIME_NANOS}
  // column, as a FIELD or as a TAG, whatever the literal form. The document layer had already turned the property
  // into a {@code LocalDateTime} / {@code LocalDate}, and the SQL write path handed that object to the engine, whose
  // row source stores these columns as an epoch count and casts the value to {@code Number}.
  // <p>
  // The invariant pinned here: an INSERT stores the same epoch count, in the column's own unit, that the binary
  // serializer stores for the same value on a document property of the same type. For {@code DATETIME} (epoch millis)
  // and {@code DATE} (epoch days) that makes a numeric literal round-trip unchanged, exactly as
  // {@link TimeSeriesEngine#appendSamples(long[], Object[][])} stores it.
  // #9643
  @Test
  void aNumericLiteralIntoADateTimeFieldIsStoredOnBothLayers() throws Exception {
    for (final Object[] spec : NUMERIC_LITERALS) {
      final String declared = (String) spec[0];
      final long literal = (Long) spec[1];
      final long expected = (Long) spec[2];
      final String typeName = "Field_" + declared;

      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared
          + ", tail DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS + ", v = " + literal + ", tail = 2.5");

      assertStored(typeName, declared, expected, 2.5d);
      assertThat(expected).as("%s stores what a document property stores", declared)
          .isEqualTo(documentEncoding(declared, String.valueOf(literal)));
    }
  }

  // #9643
  @Test
  void aNumericLiteralIntoADateTimeTagIsStoredAndFiltersBack() throws Exception {
    for (final Object[] spec : NUMERIC_LITERALS) {
      final String declared = (String) spec[0];
      final long literal = (Long) spec[1];
      final long expected = (Long) spec[2];
      // one day later in every unit the literal can be read in, so the two rows never share a tag value
      final long otherLiteral = "DATE".equals(declared) ? literal + 1 : literal + 86_400_000L;
      final String typeName = "Tag_" + declared;

      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (k " + declared
          + ") FIELDS (tail DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS + ", k = " + literal + ", tail = 2.5");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + (BASE_TS + 1) + ", k = " + otherLiteral
          + ", tail = 3.5");

      final TimeSeriesEngine engine = engineOf(typeName);
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).as("%s tag rows", declared).hasSize(2);
      assertThat(rows.get(0)[1]).as("%s tag value", declared).isEqualTo(expected);

      // the tag push-down converts a literal as INSERT did (issue #9724), so the literal that wrote the row finds it - unless the
      // column's precision dropped part of it, as a DATETIME_SECOND drops the milliseconds, which a document property does too
      if (!"DATETIME_SECOND".equals(declared))
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE k = " + literal)) {
          final List<Result> found = rs.stream().toList();
          assertThat(found).as("%s tag filter", declared).hasSize(1);
          assertThat(found.getFirst().<Object>getProperty("tail")).isEqualTo(2.5d);
        }
      // and the value a row reads back as finds that row whatever the precision (issue #9725)
      final Object readBack;
      try (final ResultSet rs = database.query("sql", "SELECT k FROM " + typeName + " WHERE tail = 2.5")) {
        readBack = rs.next().getProperty("k");
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE k = ?", readBack)) {
        final List<Result> found = rs.stream().toList();
        assertThat(found).as("%s tag filter by the value read back", declared).hasSize(1);
        assertThat(found.getFirst().<Object>getProperty("tail")).isEqualTo(2.5d);
      }

      engine.compactAll();
      final List<Object[]> sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(sealed.get(0)[1]).as("%s tag value after compaction", declared).isEqualTo(expected);
    }
  }

  // #9643
  /**
   * A string literal is parsed by the document layer exactly as it is for a document property, so the stored
   * epoch must be what the binary serializer would store for that same property.
   */
  @Test
  void aStringLiteralIsStoredAsTheEpochADocumentPropertyWouldHold() throws Exception {
    final Object[][] stringLiterals = {
        { "DATETIME", "2023-11-14 22:15:23" },
        { "DATE", "2023-11-14" },
        { "DATETIME_SECOND", "2023-11-14 22:15:23" },
        { "DATETIME_MICROS", "2023-11-14 22:15:23.456789" },
        { "DATETIME_NANOS", "2023-11-14 22:15:23.456789123" },
    };

    for (final Object[] spec : stringLiterals) {
      final String declared = (String) spec[0];
      final String literal = (String) spec[1];

      final long expected = documentEncoding(declared, "'" + literal + "'");

      final String typeName = "Str_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared
          + ", tail DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS + ", v = '" + literal + "', tail = 2.5");

      assertStored(typeName, declared, expected, 2.5d);
    }
  }

  // #9643
  /**
   * The SQL path and the engine API must agree: the issue's control case, an engine append of the same epoch.
   */
  @Test
  void theInsertStoresWhatTheEngineAppendStores() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE ViaSql TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE ViaEngine TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");

    database.command("sql", "INSERT INTO ViaSql SET ts = " + BASE_TS + ", v = 1700000123456");
    engineOf("ViaEngine").appendSamples(new long[] { BASE_TS }, new Object[] { 1_700_000_123_456L });

    assertThat(engineOf("ViaSql").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst())
        .containsExactly(engineOf("ViaEngine").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst());
  }

  // #9643
  @Test
  void aMissingDateTimeFieldStillInserts() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Sparse TIMESTAMP ts FIELDS (v DATETIME, tail DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Sparse SET ts = " + BASE_TS + ", tail = 2.5");

    final List<Object[]> rows = engineOf("Sparse").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[2]).isEqualTo(2.5d);
  }

  // #9643
  @Test
  void anExplicitNullIntoADateTimeFieldStillInserts() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Nulls TIMESTAMP ts FIELDS (v DATETIME_MICROS, d DATE, tail DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Nulls SET ts = " + BASE_TS + ", v = null, d = null, tail = 2.5");

    final List<Object[]> rows = engineOf("Nulls").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[3]).isEqualTo(2.5d);
  }

  // #9643
  /**
   * A value no datetime encoding can read is refused naming the column, not cast to {@code Number} by the engine and
   * not stored as the absent marker. Through SQL the document layer refuses it first, when the property is set; the
   * refusal in {@code SaveElementStep.convertValue} is the second line for a value that reaches it unconverted.
   */
  @Test
  void aValueNoDateTimeEncodingCanReadIsRefusedNamingTheColumn() throws Exception {
    for (final String declared : new String[] { "DATETIME", "DATETIME_NANOS", "DATE" }) {
      final String typeName = "Bad_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (observed " + declared
          + ") SHARDS 1");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS
          + ", observed = [1, 2]"))
          .as("%s refuses a list", declared)
          .isInstanceOfAny(IllegalArgumentException.class, CommandExecutionException.class)
          .hasMessageContaining("'observed'")
          .hasMessageContaining(declared);
      assertThat(engineOf(typeName).query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).as("%s stored nothing", declared)
          .isEmpty();
    }
  }

  private void assertStored(final String typeName, final String declared, final long expected, final double tail)
      throws Exception {
    final TimeSeriesEngine engine = engineOf(typeName);

    final List<Object[]> mutable = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(mutable).as("%s row count", declared).hasSize(1);
    assertThat(mutable.getFirst()[1]).as("%s value from the mutable layer", declared).isEqualTo(expected);
    assertThat(mutable.getFirst()[2]).as("%s neighbour from the mutable layer", declared).isEqualTo(tail);

    engine.compactAll();

    final List<Object[]> sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(sealed.getFirst()[1]).as("%s value from the sealed layer", declared).isEqualTo(expected);
    assertThat(sealed.getFirst()[2]).as("%s neighbour from the sealed layer", declared).isEqualTo(tail);
  }

  /**
   * The epoch count the binary serializer stores for {@code sqlLiteral} assigned to a document property declared
   * {@code declared}: the reference every INSERT into a TimeSeries column of that type must agree with.
   */
  private long documentEncoding(final String declared, final String sqlLiteral) {
    final String docType = "Doc_" + declared + "_" + (++documentTypes);
    database.command("sql", "CREATE DOCUMENT TYPE " + docType);
    database.command("sql", "CREATE PROPERTY " + docType + ".v " + declared);
    final Object[] holder = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "INSERT INTO " + docType + " SET v = " + sqlLiteral)) {
        final Document doc = rs.next().getElement().orElseThrow();
        holder[0] = doc.get("v");
      }
    });
    return "DATE".equals(declared) ?
        DateUtils.dateToEpochDays(holder[0]) :
        DateUtils.dateTimeToTimestamp(holder[0], DateUtils.getPrecisionFromType(Type.valueOf(declared)));
  }

  private TimeSeriesEngine engineOf(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private static final String[] OPERATORS = { "<", "<=", ">", ">=", "=", "<>" };

  /** Literals written in the statement: whole, fractional, out of the range of a long on either side, and on the edge of it. */
  private static final String[] LITERALS = {
      "2000", "2000.0", "2000.5", "1999.5", "2999.5", "999.5", "3000.5", "0.5", "-0.5", "1e19", "-1e19", "1.0E19", "9.3e18", "-9.3e18",
      "9223372036854775807", "-9223372036854775807", "9223372036854775807.5", "9223372036854775808.0", "-9223372036854775809.0" };

  /** Values bound as a parameter, the ones a literal cannot spell. */
  private static final Object[] PARAMETERS = {
      2000L, 2000.5d, 1999.5d, 2000.5f, Double.MAX_VALUE, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN,
      Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN, new BigDecimal("2000.5"), new BigDecimal("2000"),
      new BigDecimal("9223372036854775808"), new BigDecimal("-9223372036854775809"), new BigDecimal("9223372036854775807.5"),
      new BigInteger("9223372036854775808"), new BigInteger("-9223372036854775809"), new BigInteger("2000") };

  private int cases;

  // Issue #10560: the range a time predicate is pushed down as is EXACTLY that predicate (the rule of #10528), and it was not for a
  // bound that is not a whole {@code long}. A fractional bound was truncated to a whole millisecond ({@code ts >= 2000.5} selected the
  // sample at 2000 and {@code DELETE ... WHERE ts = 2000.5} removed it), a numeric bound outside the range of a {@code long} selected
  // nothing whichever the predicate meant, and {@code 1000 <> ts} was ignored.
  // <p>
  // The oracle is the same three samples in a plain document type: every operator, written either way round, against every kind of
  // bound, answers the same rows for the SELECT and leaves the same rows after the DELETE.
  // #10560
  @Test
  void aLiteralBoundSelectsTheRowsThePredicateSelectsInADocumentType() {
    final List<String> differing = new ArrayList<>();
    final String[] types = createSamples();

    for (final String operator : OPERATORS)
      for (final String literal : LITERALS) {
        compare(types, "ts " + operator + " " + literal, Map.of(), differing);
        compare(types, literal + " " + operator + " ts", Map.of(), differing);
      }

    assertThat(differing).as("predicates answered differently by the time series type").isEmpty();
  }

  // #10560
  @Test
  void aParameterBoundSelectsTheRowsThePredicateSelectsInADocumentType() {
    final List<String> differing = new ArrayList<>();
    final String[] types = createSamples();

    for (final String operator : OPERATORS)
      for (final Object parameter : PARAMETERS) {
        final String description = parameter.getClass().getSimpleName() + " " + parameter;
        compare(types, "ts " + operator + " :p", Map.of("p", parameter), differing, description);
        compare(types, ":p " + operator + " ts", Map.of("p", parameter), differing, description);
      }

    assertThat(differing).as("predicates answered differently by the time series type").isEmpty();
  }

  // #10560
  @Test
  void aBoundCombinedWithAnotherPredicateSelectsTheSameRows() {
    final List<String> differing = new ArrayList<>();
    final String[] types = createSamples();

    final String[] predicates = {
        "ts >= 1500.5 AND ts <= 2500.5", "ts > 1999.5 AND ts < 3000.5", "ts >= 2000.5 OR ts < 1000.5", "ts <= 1e19 AND ts >= 1500",
        "ts > -1e19 AND ts <> 2000", "ts BETWEEN 1500.5 AND 2500.5", "ts BETWEEN -1e19 AND 1e19", "ts BETWEEN 2000.5 AND 1e19",
        "ts <= 1e19 AND t = 'h'", "ts >= 2000.5 AND t = 'h'", "1000 <> ts AND t = 'h'", "1000 <> ts AND 3000 <> ts",
        "ts <= 1e19 OR ts > 5000", "NOT (ts >= 2000.5)" };
    for (final String predicate : predicates)
      compare(types, predicate, Map.of(), differing);

    assertThat(differing).as("predicates answered differently by the time series type").isEmpty();
  }

  // #10560
  @Test
  void aDeleteRemovesTheRowsThePredicateSelectsInADocumentType() {
    final List<String> differing = new ArrayList<>();

    for (final String operator : OPERATORS)
      for (final String literal : LITERALS) {
        compareDelete("ts " + operator + " " + literal, Map.of(), differing, "");
        compareDelete(literal + " " + operator + " ts", Map.of(), differing, "");
      }
    for (final String operator : OPERATORS)
      for (final Object parameter : PARAMETERS) {
        final String description = parameter.getClass().getSimpleName() + " " + parameter;
        compareDelete("ts " + operator + " :p", Map.of("p", parameter), differing, description);
        compareDelete(":p " + operator + " ts", Map.of("p", parameter), differing, description);
      }
    for (final String predicate : new String[] { "ts >= 1500.5 AND ts <= 2500.5", "ts BETWEEN 1500.5 AND 2500.5", "ts BETWEEN -1e19 AND 1e19",
        "1000 <> ts AND t = 'h'" })
      compareDelete(predicate, Map.of(), differing, "");

    assertThat(differing).as("DELETE statements that left other rows than the document type does").isEmpty();
  }

  // #10560
  @Test
  void theFractionalBoundsOfTheReport() {
    final String[] types = createSamples();

    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts >= 2000.5")).containsExactly(3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts = 2000.5")).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts < 2000.5")).containsExactly(1000L, 2000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts <= 1e19")).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts > -1e19")).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE 1000 <> ts")).containsExactly(2000L, 3000L);

    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts = 2000.5");
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(1000L, 2000L, 3000L);
    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts >= 2000.5");
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(1000L, 2000L);
    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts < 1000.5");
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(2000L);
  }

  // #10560
  /**
   * A bound is the client's: {@code 1E+999999999} must not make the server build a number of a billion digits to find that it is above
   * every sample, nor {@code 1E-999999999} the power of ten that divides by. The timeout is a hang detector, not a latency bound.
   */
  @Test
  @Timeout(120)
  void aBoundWithAHugeExponentIsDecidedWithoutBuildingTheNumber() {
    final String[] types = createSamples();
    final BigDecimal huge = new BigDecimal("1E+999999999");
    final BigDecimal tiny = new BigDecimal("1E-999999999");

    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts <= :p", Map.of("p", huge))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts < :p", Map.of("p", huge))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts >= :p", Map.of("p", huge))).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts = :p", Map.of("p", huge))).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts <> :p", Map.of("p", huge))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts > :p", Map.of("p", huge.negate()))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts < :p", Map.of("p", huge.negate()))).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts BETWEEN :a AND :b", Map.of("a", huge.negate(), "b", huge)))
        .containsExactly(1000L, 2000L, 3000L);

    // a positive that is nearly zero is above 0 and below 1: every sample is after it
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts > :p", Map.of("p", tiny))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts >= :p", Map.of("p", tiny))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts < :p", Map.of("p", tiny))).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts = :p", Map.of("p", tiny))).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts > :p", Map.of("p", tiny.negate()))).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts <= :p", Map.of("p", tiny.negate()))).isEmpty();

    // and the DELETE it makes a scope of
    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts >= :p", Map.of("p", huge));
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(1000L, 2000L, 3000L);
    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts <= :p", Map.of("p", huge));
    assertThat(tsOf("SELECT FROM " + types[0])).isEmpty();
  }

  /**
   * The three samples of the report, in a time series type and in a document type; returns the two type names. The types are created
   * once, and every later call empties them and writes the samples again.
   */
  private String[] createSamples() {
    final String ts = "D1";
    final String doc = "D2";
    if (cases++ == 0) {
      database.command("sql", "CREATE TIMESERIES TYPE " + ts + " TIMESTAMP ts TAGS (t STRING) FIELDS (i INTEGER)");
      database.command("sql", "CREATE DOCUMENT TYPE " + doc);
    } else {
      database.command("sql", "DELETE FROM " + ts);
      database.transaction(() -> database.command("sql", "DELETE FROM " + doc));
    }
    fill(ts, doc);
    return new String[] { ts, doc };
  }

  private void fill(final String tsType, final String docType) {
    for (final String type : new String[] { tsType, docType })
      // a time series insert opens no transaction of its own, a document insert needs one
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + type + " SET ts = 1000, t = 'h', i = 1");
        database.command("sql", "INSERT INTO " + type + " SET ts = 2000, t = 'h', i = 2");
        database.command("sql", "INSERT INTO " + type + " SET ts = 3000, t = 'h', i = 3");
      });
  }

  private void compare(final String[] types, final String where, final Map<String, Object> parameters, final List<String> differing) {
    compare(types, where, parameters, differing, "");
  }

  private void compare(final String[] types, final String where, final Map<String, Object> parameters, final List<String> differing,
      final String description) {
    final List<Long> expected = tsOf("SELECT FROM " + types[1] + " WHERE " + where, parameters);
    final List<Long> actual;
    try {
      actual = tsOf("SELECT FROM " + types[0] + " WHERE " + where, parameters);
    } catch (final RuntimeException e) {
      differing.add(where + " " + description + ": refused (" + e.getMessage() + ") where the document type answers " + expected);
      return;
    }
    if (!expected.equals(actual))
      differing.add(where + " " + description + ": " + actual + " where the document type answers " + expected);
  }

  /** Runs the same DELETE on a fresh pair of types and compares what each one keeps. */
  private void compareDelete(final String where, final Map<String, Object> parameters, final List<String> differing, final String description) {
    final String[] types = createSamples();
    final List<Long> expected;
    try {
      database.transaction(() -> database.command("sql", "DELETE FROM " + types[1] + " WHERE " + where, parameters));
      expected = tsOf("SELECT FROM " + types[1]);
    } catch (final RuntimeException e) {
      // the document type refuses the statement (a NaN comparison, for instance): there is no answer to hold the series to
      return;
    }
    try {
      database.command("sql", "DELETE FROM " + types[0] + " WHERE " + where, parameters);
    } catch (final RuntimeException e) {
      // `<>` against a whole millisecond is two ranges, which a DELETE of a series does not take: refused, and nothing removed
      if (where.contains("<>") && tsOf("SELECT FROM " + types[0]).size() == 3)
        return;
      differing.add(where + " " + description + ": refused (" + e.getMessage() + ") where the document type keeps " + expected);
      return;
    }
    final List<Long> actual = tsOf("SELECT FROM " + types[0]);
    if (!expected.equals(actual))
      differing.add(where + " " + description + ": keeps " + actual + " where the document type keeps " + expected);
  }

  private List<Long> tsOf(final String sql) {
    return tsOf(sql, Map.of());
  }

  private List<Long> tsOf(final String sql, final Map<String, Object> parameters) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", sql, parameters)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final Object ts = row.getProperty("ts");
        values.add(epochMillisOf(ts));
      }
    }
    values.sort(Long::compare);
    return values;
  }

  /** The timestamp column answers a datetime, the plain document type the number it was given. */
  private static long epochMillisOf(final Object ts) {
    if (ts instanceof Number number)
      return number.longValue();
    if (ts instanceof LocalDateTime dateTime)
      return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    return ((Date) ts).getTime();
  }

  /**
   * Issue #10398: a DATE/DATETIME column of a TIMESERIES type answers the same literal differently over SQL and over the wire (line
   * protocol, gRPC, Prometheus). That is the intended contract of each surface (issue #10233), not an accident of each side being
   * settled alone, and this is the table that makes it visible - one row per (column type, literal shape), the two surfaces side by
   * side - so that moving either side is a decision somebody makes by editing a cell:
   * <ul>
   *   <li><b>SQL</b> reads the column like the document property of the same type: a number is epoch MILLISECONDS whatever the
   *   precision (epoch DAYS for a DATE), and text is a date in the database's format, so text of digits is no date and is refused
   *   (issues #9643, #9724) - except on a DATE column, where digits that fit a day count are read as one;</li>
   *   <li><b>the wire</b> reads a number, and text of digits, as a count in the column's OWN unit (seconds for DATETIME_SECOND, days
   *   for a DATE). The text half is not a convenience: a line protocol TAG has no type, so a DATETIME tag can only arrive as text, and
   *   its digits are the count a writer of that protocol means (timestamps in that protocol are counts too).</li>
   * </ul>
   * Where the two agree (a date as text, a value no date can stand for) the table says so, and a cell where they store different
   * instants is the one an operator writing the same measurement from both places has to know about: the refusal message of SQL
   * names the rule.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TemporalSurfaceMatrix {
    /** 2023-11-14 22:13:20 UTC. */
    private static final long MILLIS = 1_700_000_000_000L;
    private static final long SECONDS = MILLIS / 1_000L;
    private static final long DAYS    = 19_675L;

    private static final String[] COLUMN_TYPES = { "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS", "DATE" };

    /** The cell value of a surface that refuses the literal. */
    private static final Long REFUSED = null;

    @BeforeEach
    void createTypes() {
      for (final String columnType : COLUMN_TYPES)
        database.command("sql", "CREATE TIMESERIES TYPE T_" + columnType + " TIMESTAMP ts FIELDS (f " + columnType + ") SHARDS 1");
    }

    // #10398
    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("cells")
    void eachSurfaceAnswersAsTheTableSays(final String columnType, final String shape, final Object literal, final Long sql,
        final Long wire) throws Exception {
      final String typeName = "T_" + columnType;

      final String sqlLiteral = literal instanceof String text ? "'" + text + "'" : String.valueOf(literal);
      if (sql == REFUSED)
        assertThatThrownBy(() -> database.command("sql", "INSERT INTO " + typeName + " SET ts = 1, f = " + sqlLiteral)).as("SQL")
            .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'f'");
      else {
        database.command("sql", "INSERT INTO " + typeName + " SET ts = 1, f = " + sqlLiteral);
        assertThat(count(typeName, 1)).as("SQL").isEqualTo(sql);
      }

      final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
          new ArrayList<>(List.of(new Sample(typeName, Map.of(), Map.of("f", literal), 2))));
      if (wire == REFUSED) {
        assertThat(report.written()).as("wire written").isZero();
        assertThat(report.dropped()).as("wire dropped").isEqualTo(1);
      } else {
        assertThat(report.dropped()).as("wire dropped").isZero();
        assertThat(count(typeName, 2)).as("wire").isEqualTo(wire);
      }
    }

    // #10398
    /** Same column, same literal, two instants three orders of magnitude apart, and no refusal on either side (issue #10398). */
    @Test
    void aLongIntoADatetimeSecondColumnIsMillisOnSqlAndSecondsOnTheWire() throws Exception {
      database.command("sql", "INSERT INTO T_DATETIME_SECOND SET ts = 1, f = " + SECONDS);
      TimeSeriesGateway.write((DatabaseInternal) database,
          new ArrayList<>(List.of(new Sample("T_DATETIME_SECOND", Map.of(), Map.of("f", SECONDS), 2))));

      assertThat(count("T_DATETIME_SECOND", 1)).isEqualTo(SECONDS / 1_000L);
      assertThat(count("T_DATETIME_SECOND", 2)).isEqualTo(SECONDS);
    }

    // #10398
    /** A line protocol tag has no type: a DATETIME tag arrives as text of digits, and is the count a writer of that protocol means. */
    @Test
    void aDatetimeTagGivenAsTextOfDigitsIsACountOnTheWireAndRefusedBySql() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE TagT TIMESTAMP ts TAGS (k DATETIME) FIELDS (v LONG) SHARDS 1");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO TagT SET ts = 1, k = '" + MILLIS + "', v = 1"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'k'");
      final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
          new ArrayList<>(List.of(new Sample("TagT", Map.of("k", "" + MILLIS), Map.of("v", 1L), 2))));
      assertThat(report.written()).isEqualTo(1);
      assertThat(((Number) rows("TagT").getFirst()[1]).longValue()).isEqualTo(MILLIS);
    }

    // #10398
    /**
     * The refusal of a literal SQL reads differently from the wire says what SQL reads, so that an operator who wrote the number or
     * the digits the other surface took does not read the refusal as a defect of this one.
     */
    @Test
    void theSqlRefusalOfTextOfDigitsNamesTheRule() {
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO T_DATETIME SET ts = 1, f = '" + MILLIS + "'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'f'")
          .hasMessageContaining("number is epoch milliseconds").hasMessageContaining("date in the database's format");
    }

    static Stream<Arguments> cells() {
      final List<Arguments> cells = new ArrayList<>();
      for (final String columnType : COLUMN_TYPES) {
        final boolean date = columnType.equals("DATE");

        // a number: epoch millis on SQL, the column's own unit on the wire. A DATE reads a number as days on SQL, but one no day
        // count can be is the epoch millis it is, stored as its day (issue #11025)
        cells.add(cell(columnType, "number the size of epoch millis", MILLIS, date ? DAYS : sqlCountOf(columnType, MILLIS), MILLIS));
        // a DATE reads the same number as days on both, so the two agree on it
        cells.add(cell(columnType, "number the size of epoch seconds", SECONDS, date ? SECONDS : sqlCountOf(columnType, SECONDS), SECONDS));
        // text of digits: no date for SQL, a count for the wire. Except for a DATE, whose SQL reading of text of digits is the
        // reading of the number, so digits the size of epoch millis are that day, as above
        cells.add(cell(columnType, "text of digits the size of epoch millis", "" + MILLIS, date ? Long.valueOf(DAYS) : REFUSED, MILLIS));
        // ... and '0' and '19675' agree with the wire
        cells.add(cell(columnType, "text of zero", "0", date ? Long.valueOf(0L) : REFUSED, 0L));
        if (date)
          cells.add(cell(columnType, "text of epoch days", "" + DAYS, DAYS, DAYS));
        // a date as text names the same instant on both
        final long instant = date ? DAYS : sqlCountOf(columnType, MILLIS);
        cells.add(cell(columnType, "date as text", "2023-11-14 22:13:20", instant, instant));
        // a value no date can stand for is refused by both
        cells.add(cell(columnType, "text that is no date", "not a date", REFUSED, REFUSED));
        cells.add(cell(columnType, "boolean", true, REFUSED, REFUSED));
      }
      return cells.stream();
    }

    /** What SQL stores for {@code millis}: the instant, counted in the unit of the column. */
    private static Long sqlCountOf(final String columnType, final long millis) {
      return switch (columnType) {
        case "DATETIME_SECOND" -> millis / 1_000L;
        case "DATETIME_MICROS" -> millis * 1_000L;
        case "DATETIME_NANOS" -> millis * 1_000_000L;
        default -> millis;
      };
    }

    private static Arguments cell(final String columnType, final String shape, final Object literal, final Long sql, final Long wire) {
      return Arguments.of(columnType, shape, literal, sql, wire);
    }

    private long count(final String typeName, final long ts) throws Exception {
      for (final Object[] row : rows(typeName))
        if (((Number) row[0]).longValue() == ts)
          return ((Number) row[1]).longValue();
      throw new AssertionError("no sample at " + ts + " in " + typeName);
    }

    private List<Object[]> rows(final String typeName) throws Exception {
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    }
  }

  /**
   * Issues #9724 and #9725: a TimeSeries DATE/DATETIME column (not the TIMESTAMP one) answered SQL with the raw epoch count in its
   * own unit, while INSERT reads a number as epoch MILLIS like a document property does. So a value read back from a row could not
   * be written back as it was (a DATETIME_NANOS count read as millis saturated to Long.MAX_VALUE nanos), and filtering by the very
   * literal INSERT accepted matched nothing, on a tag (push-down) and on a field. Such a column now reads back as the date/time
   * object a document property of the same type holds, a filter literal is converted exactly as INSERT converts it, and an instant
   * the precision cannot represent is refused instead of saturated - on a document property too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeSeriesDateTimeColumns {
    private static final String[] TYPES = { "DATE", "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS" };

    // #9724
    @Test
    void theReportedScenario() {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME) FIELDS (v DATETIME) SHARDS 1");
      database.command("sql", "INSERT INTO T SET ts = 1700000000000, k = '2023-11-14 22:15:23', v = '2023-11-14 22:15:23'");
      for (int pass = 0; pass < 2; pass++) {
        assertThat(count("SELECT FROM T")).isEqualTo(1);
        final String byTag = "SELECT FROM T WHERE k = '2023-11-14 22:15:23'";
        assertThat(plan(byTag)).contains("TAGS");
        assertThat(count(byTag)).as("tag push-down, pass " + pass).isEqualTo(1);
        assertThat(count("SELECT FROM T WHERE v = '2023-11-14 22:15:23'")).as("field filter, pass " + pass).isEqualTo(1);
        assertThat(count("SELECT FROM T WHERE k = '2023-11-14 22:15:24'")).isZero();
        assertThat(count("SELECT FROM T WHERE v = ?", LocalDateTime.of(2023, 11, 14, 22, 15, 23))).isEqualTo(1);
        database.command("sql", "COMPACT TIMESERIES TYPE T");
      }
    }

    // #9724
    @Test
    void everyPrecisionReadsBackLikeADocumentTwinAndFiltersByItsOwnLiteral() {
      final StringBuilder tags = new StringBuilder();
      final StringBuilder fields = new StringBuilder();
      database.command("sql", "CREATE DOCUMENT TYPE D");
      for (int t = 0; t < TYPES.length; t++) {
        tags.append(t > 0 ? ", " : "").append("k").append(t).append(' ').append(TYPES[t]);
        fields.append(t > 0 ? ", " : "").append("v").append(t).append(' ').append(TYPES[t]);
        database.command("sql", "CREATE PROPERTY D.k" + t + " " + TYPES[t]);
        database.command("sql", "CREATE PROPERTY D.v" + t + " " + TYPES[t]);
      }
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (" + tags + ") FIELDS (" + fields + ") SHARDS 2");

      final Object[] literals = { "2023-11-14 22:15:23.123456789", LocalDateTime.of(2023, 11, 14, 22, 15, 23, 987_654_321), 1_700_000_123_456L };
      database.transaction(() -> {
        for (int r = 0; r < literals.length; r++)
          for (final String type : new String[] { "T", "D" }) {
            final StringBuilder sql = new StringBuilder("INSERT INTO " + type + " SET ts = " + (1_700_000_000_000L + r * 1_000L));
            final List<Object> args = new ArrayList<>();
            for (int t = 0; t < TYPES.length; t++) {
              sql.append(", k").append(t).append(" = ?, v").append(t).append(" = ?");
              args.add(literalFor(literals[r], t));
              args.add(literalFor(literals[r], t));
            }
            database.command("sql", sql.toString(), args.toArray());
          }
      });

      for (int pass = 0; pass < 2; pass++) {
        final List<String> onT = new ArrayList<>();
        final List<String> onD = new ArrayList<>();
        for (int t = 0; t < TYPES.length; t++) {
          onT.addAll(rows("SELECT k" + t + ", v" + t + " FROM T ORDER BY ts"));
          onD.addAll(rows("SELECT k" + t + ", v" + t + " FROM D ORDER BY ts"));
          for (final Object any : literals) {
            final Object literal = literalFor(any, t);
            // a literal finer than the column's precision does not match on the twin either: the stored value lost its fraction
            final long expected = count("SELECT FROM D WHERE k" + t + " = ?", literal);
            if (TYPES[t].equals("DATETIME_NANOS"))
              assertThat(expected).as(TYPES[t] + " " + literal).isPositive();
            assertThat(count("SELECT FROM T WHERE k" + t + " = ?", literal)).as("tag " + TYPES[t] + " = " + literal).isEqualTo(expected);
            assertThat(count("SELECT FROM T WHERE v" + t + " = ?", literal)).as("field " + TYPES[t] + " = " + literal).isEqualTo(expected);
          }
        }
        assertThat(onT).as("pass " + pass).isEqualTo(onD);
        database.command("sql", "COMPACT TIMESERIES TYPE T");
      }
    }

    // #9724
    @Test
    void aValueReadFromARowWritesBackAsItIs() {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (n DATETIME_NANOS, s DATETIME_SECOND, d DATE)");
      database.command("sql", "INSERT INTO T SET ts = 1, host = 'a', n = '2023-11-14 22:15:23.123456789', s = '2023-11-14 22:15:23', d = '2023-11-14'");
      final Result first = database.query("sql", "SELECT FROM T").next();
      final Object[] readBack = { first.getProperty("n"), first.getProperty("s"), first.getProperty("d") };
      database.command("sql", "INSERT INTO T SET ts = 2, host = 'b', n = ?, s = ?, d = ?", readBack);
      final List<String> rows = rows("SELECT n, s, d FROM T ORDER BY ts");
      assertThat(rows).hasSize(2);
      assertThat(rows.get(1)).isEqualTo(rows.get(0));
      assertThat(first.<Object>getProperty("n")).isEqualTo(LocalDateTime.of(2023, 11, 14, 22, 15, 23, 123_456_789));
    }

    // #9724
    @Test
    void aPreEpochInstantRoundTripsInEveryPrecision() {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME_NANOS) FIELDS (n DATETIME_NANOS, u DATETIME_MICROS, "
          + "s DATETIME_SECOND, d DATE)");
      // before 1970, where a floored division and a truncated one land on different units
      final LocalDateTime instant = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999);
      database.command("sql", "INSERT INTO T SET ts = 1, k = ?, n = ?, u = ?, s = ?, d = ?", instant, instant, instant, instant, instant);
      for (int pass = 0; pass < 2; pass++) {
        assertThat(rows("SELECT k, n, u, s, d FROM T")).containsExactly(
            "[d=1969-12-31(LocalDate), k=1969-12-31T23:59:59.999999999(LocalDateTime), n=1969-12-31T23:59:59.999999999(LocalDateTime), "
                + "s=1969-12-31T23:59:59(LocalDateTime), u=1969-12-31T23:59:59.999999(LocalDateTime)]");
        assertThat(count("SELECT FROM T WHERE k = ?", instant)).isEqualTo(1);
        assertThat(count("SELECT FROM T WHERE n = ?", instant)).isEqualTo(1);
        database.command("sql", "COMPACT TIMESERIES TYPE T");
      }
    }

    // #9724
    @Test
    void minAndMaxOfADateTimeFieldAndAGroupingDateTimeTagReadAsDateTimes() {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME_MICROS) FIELDS (v DATETIME_NANOS)");
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.k DATETIME_MICROS");
      database.command("sql", "CREATE PROPERTY D.v DATETIME_NANOS");
      database.transaction(() -> {
        for (int i = 0; i < 30; i++)
          for (final String type : new String[] { "T", "D" })
            database.command("sql", "INSERT INTO " + type + " SET ts = ?, k = ?, v = ?", 1_700_000_000_000L + i * 1_000L,
                LocalDateTime.of(2024, 1, 1 + i % 3, 0, 0, 0, 1_000), LocalDateTime.of(2262, 1, 1, 0, 0, 0, i * 7 + 1));
      });
      final String[] queries = { "SELECT min(v) AS mn, max(v) AS mx FROM X", "SELECT k, min(v) AS mn, max(v) AS mx FROM X GROUP BY k ORDER BY k" };
      for (int pass = 0; pass < 2; pass++) {
        for (final String sql : queries) {
          assertThat(plan(sql.replace("FROM X", "FROM T"))).as(sql).contains("AGGREGATE FROM TIMESERIES");
          assertThat(rows(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(rows(sql.replace("FROM X", "FROM D")));
        }
        database.command("sql", "COMPACT TIMESERIES TYPE T");
      }
      assertThat(rows("SELECT max(v) AS mx FROM T")).containsExactly("[mx=2262-01-01T00:00:00.000000204(LocalDateTime)]");
    }

    // #9724
    @Test
    void anInstantThePrecisionCannotHoldIsRefusedNotSaturated() {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (n DATETIME_NANOS)");
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.n DATETIME_NANOS");
      // the reporter's row: a nanosecond count, read as millis like every number into a DATETIME, is ~53 million years away
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO T SET ts = 1, host = 'a', n = 1700000123456789000"))
          .isInstanceOf(CommandExecutionException.class).hasStackTraceContaining("outside the range");
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO D SET n = 1700000123456789000")))
          .hasStackTraceContaining("outside the range");
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO D SET n = '2500-01-01 00:00:00'")))
          .hasStackTraceContaining("outside the range");
      assertThat(count("SELECT FROM T")).isZero();
      assertThat(count("SELECT FROM D")).isZero();

      // a FILTER with such an instant is not an error: no row can hold it, so it matches nothing, on a tag and on a field
      database.command("sql", "CREATE TIMESERIES TYPE F TIMESTAMP ts TAGS (k DATETIME_NANOS) FIELDS (n DATETIME_NANOS)");
      database.command("sql", "INSERT INTO F SET ts = 1, k = '2262-04-11 23:47:16', n = '2262-04-11 23:47:16'");
      assertThat(count("SELECT FROM F WHERE k = '2500-01-01 00:00:00'")).isZero();
      assertThat(count("SELECT FROM F WHERE n = '2500-01-01 00:00:00'")).isZero();
      assertThat(count("SELECT FROM F WHERE k = '2262-04-11 23:47:16'")).isEqualTo(1);

      // the limits themselves are representable and stored as they are
      database.command("sql", "INSERT INTO T SET ts = 1, host = 'a', n = '2262-04-11 23:47:16.854775807'");
      assertThat(rows("SELECT n FROM T")).containsExactly("[n=2262-04-11T23:47:16.854775807(LocalDateTime)]");
    }

    // #9724
    @Test
    void theEngineSurfacesKeepTheirEpochCountsAndReadATemporalTagFilter() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME_SECOND) FIELDS (x LONG)");
      database.command("sql", "INSERT INTO T SET ts = 1, k = '2023-11-14 22:15:23', x = 1");
      final ColumnDefinition tag = new ColumnDefinition("k", Type.DATETIME_SECOND, ColumnDefinition.ColumnRole.TAG);
      // a number is a count in the column's unit at the engine API, as the line protocol and the HTTP surfaces speak it
      assertThat(tag.coerceValue(1_700_000_123L)).isEqualTo(1_700_000_123L);
      assertThat(tag.coerceValue("1700000123")).isEqualTo(1_700_000_123L);
      // a date or a datetime in any form is the count it stands for
      assertThat(tag.coerceValue("2023-11-14 22:15:23")).isEqualTo(1_700_000_123L);
      assertThat(tag.coerceValue(LocalDateTime.of(2023, 11, 14, 22, 15, 23))).isEqualTo(1_700_000_123L);
      // only ASCII digits make a count; anything else that is no date is handed back for the filter to match nothing
      assertThat(tag.coerceValue("-1")).isEqualTo(-1L);
      assertThat(tag.coerceValue("\u0661\u0662")).isEqualTo("\u0661\u0662");
      assertThat(tag.coerceValue("-")).isEqualTo("-");
    }

    /** A number is epoch DAYS into a DATE and epoch MILLIS into every DATETIME, as on a document property (issue #9324). */
    private static Object literalFor(final Object literal, final int type) {
      return literal instanceof Long && TYPES[type].equals("DATE") ? (Object) 19_675L : literal;
    }

    // #9724
    @Test
    void theStrictConversionAnswersWhatTheSaturatingOneDoesWhereverItFits() {
      final Object[] values = { new Date(-1_500L), new Date(1_700_000_123_456L), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999),
          LocalDateTime.of(2023, 11, 14, 22, 15, 23, 123_456_789), LocalDate.of(1900, 1, 1),
          Instant.ofEpochSecond(-5, 1), ZonedDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_807, ZoneOffset.UTC),
          1_700_000_123_456L, "1700000123456", "2023-11-14 22:15:23.123456789" };
      for (final ChronoUnit unit : new ChronoUnit[] { ChronoUnit.SECONDS,
          ChronoUnit.MILLIS, ChronoUnit.MICROS, ChronoUnit.NANOS })
        for (final Object value : values)
          assertThat(DateUtils.dateTimeToTimestampExact(database, value, unit)).as(value + " in " + unit)
              .isEqualTo(DateUtils.dateTimeToTimestamp(database, value, unit));
      // past the range: refused rather than the saturated value
      assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(database, LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_808),
          ChronoUnit.NANOS)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(database, LocalDate.of(1600, 1, 1),
          ChronoUnit.NANOS)).isInstanceOf(IllegalArgumentException.class);
    }

    private long count(final String sql, final Object... args) {
      long count = 0;
      try (final ResultSet rs = database.query("sql", sql, args)) {
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
      }
      return count;
    }

    private String plan(final String sql) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private List<String> rows(final String sql) {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          final List<String> values = new ArrayList<>();
          for (final String name : r.getPropertyNames().stream().sorted().toList()) {
            final Object value = r.getProperty(name);
            values.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
          }
          out.add(values.toString());
        }
      }
      return out;
    }
  }

  /**
   * Issue #9879: {@link TimeSeriesGateway#andTag} read a DATE/DATETIME tag value given as text with no database, in the JVM's
   * default time zone, while the {@code INSERT} that wrote the tag read the same text in the database's. On a JVM whose zone is
   * not the database's, the filter named another epoch than the stored one and selected nothing. Every protocol's tag selection
   * converges on {@code andTag}: the gRPC {@code TimeSeries*} RPCs through {@link TimeSeriesGateway#buildTagFilter}, the HTTP
   * {@code tags} object and {@code tag=name:value} parameter directly.
   * <p>
   * The database captures its zone when it opens. The JVM default is moved afterwards, to a zone with an offset on the stored
   * date, so the two differ wherever the test runs - CI runs in UTC, where they would otherwise coincide.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class GatewayTagFilterDatabaseTimeZone {
    private TimeZone jvmZone;

    @BeforeEach
    void moveTheJvmZoneAwayFromTheDatabases() {
      jvmZone = TimeZone.getDefault();
      final TimeZone databaseZone = database.getSchema().getTimeZone();
      TimeZone.setDefault(TimeZone.getTimeZone(databaseZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));
    }

    @AfterEach
    void restoreTheJvmZone() {
      TimeZone.setDefault(jvmZone);
    }

    // #9879
    @Test
    void aDateTimeTagGivenAsTextSelectsTheRowTheInsertWroteWhateverTheJvmZone() throws Exception {
      final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
      assertSelectedOnBothLayers(type, "2024-01-01 00:00:00");
    }

    // #9879
    @Test
    void aDateTagGivenAsTextSelectsTheRowTheInsertWroteWhateverTheJvmZone() throws Exception {
      final String type = createType("DATE", "'2024-01-01'");
      assertSelectedOnBothLayers(type, "2024-01-01");
    }

    // #9879
    @Test
    void theDateTimeTextReadWithoutTheDatabaseNamesAnotherEpoch() throws Exception {
      // The precondition the regression rests on: read with the JVM defaults, the same text names another instant here, which
      // is exactly what the gateway used to do. Without it the tests above could not tell the fix from the bug.
      final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
      final ColumnDefinition code = columnsOf(type).get(1);
      assertThat(code.coerceValue(null, "2024-01-01 00:00:00")).isNotEqualTo(code.coerceValue(database, "2024-01-01 00:00:00"));
      assertThat(rows(type, TimeSeriesGateway.andTag(null, null, "code", "2024-01-01 00:00:00", columnsOf(type)))).isZero();
    }

    // #9879
    @Test
    void aDifferentDateTimeStillSelectsNothing() throws Exception {
      final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
      assertThat(rows(type, TimeSeriesGateway.andTag(database, null, "code", "2025-06-01 00:00:00", columnsOf(type)))).isZero();
    }

    private void assertSelectedOnBothLayers(final String type, final String value) throws Exception {
      for (final boolean sealed : new boolean[] { false, true }) {
        if (sealed)
          engineOf(type).compactAll();
        final String layer = sealed ? "sealed store" : "mutable bucket";
        final List<ColumnDefinition> columns = columnsOf(type);

        assertThat(rows(type, TimeSeriesGateway.andTag(database, null, "code", value, columns)))
            .as("%s: andTag, the HTTP 'tags' object and 'tag=name:value' parameter", layer).isEqualTo(1);
        assertThat(rows(type, TimeSeriesGateway.buildTagFilter(database, Map.of("code", value), columns)))
            .as("%s: buildTagFilter, the gRPC TimeSeries* RPCs", layer).isEqualTo(1);
        assertThat(TimeSeriesGateway.latest(engineOf(type), TimeSeriesGateway.andTag(database, null, "code", value, columns)))
            .as("%s: latest", layer).isNotNull();
      }
    }

    private String createType(final String declaredType, final String stored) {
      final String type = "ts9879_" + declaredType.toLowerCase();
      database.command("sql", "CREATE TIMESERIES TYPE " + type + " TIMESTAMP ts TAGS (code " + declaredType + ") FIELDS (value DOUBLE)");
      database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET ts = 1000, code = " + stored + ", value = 1.0"));
      return type;
    }

    private int rows(final String type, final TagFilter filter) throws Exception {
      return engineOf(type).query(Long.MIN_VALUE, Long.MAX_VALUE, null, filter).size();
    }

    private List<ColumnDefinition> columnsOf(final String type) {
      return ((LocalTimeSeriesType) database.getSchema().getType(type)).getTsColumns();
    }

    private TimeSeriesEngine engineOf(final String type) {
      return ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
    }
  }

  /**
   * Issue #10528: {@code ts > <Long.MAX_VALUE>} was turned into the range {@code [value + 1, Long.MAX_VALUE]} with no overflow check, so
   * the bound wrapped to {@code Long.MIN_VALUE}, which the range union reads as "no lower bound": a predicate no sample satisfies
   * answered every sample, and since #10342 {@code DELETE} destroyed every sample of the type, outside any transaction.
   * <p>
   * The rule: the range a time predicate is pushed down as is EXACTLY that predicate, at the extremes of {@code long} as anywhere else.
   * Every comparison operator is checked against the extremes, written either way round and as a bound parameter.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeSeriesBoundOverflow {
    private static final long MAX = Long.MAX_VALUE;
    private static final long MIN = Long.MIN_VALUE;

    @BeforeEach
    void createSamples() {
      database.command("sql", "CREATE TIMESERIES TYPE D1 TIMESTAMP ts TAGS (t STRING) FIELDS (i INTEGER)");
      database.command("sql", "INSERT INTO D1 SET ts = 1000, t = 'h', i = 1");
      database.command("sql", "INSERT INTO D1 SET ts = 2000, t = 'h', i = 2");
      database.command("sql", "INSERT INTO D1 SET ts = 3000, t = 'h', i = 3");
    }

    // #10528
    @ParameterizedTest
    @ValueSource(strings = {
        "ts > 9223372036854775807",
        "9223372036854775807 < ts",
        "ts > 9223372036854775807 AND t = 'h'",
        "ts > 9223372036854775807 OR ts > 9223372036854775807",
        // a union of blocks: the empty one adds nothing
        "ts > 9223372036854775807 OR ts < 0",
        // both ends of the union form: the lower one is declined by the push-down, so the WHERE stays and judges the rows
        "ts > 9223372036854775807 OR ts < -9223372036854775808",
        "-9223372036854775808 > ts OR 9223372036854775807 < ts",
        "ts >= 9223372036854775807",
        "ts = 9223372036854775807",
        "ts < -9223372036854775808",
        "-9223372036854775808 > ts"
    })
    void aPredicateNoSampleSatisfiesSelectsNothing(final String where) {
      assertThat(count("SELECT FROM D1 WHERE " + where)).isZero();
    }

    // #10528
    @ParameterizedTest
    @ValueSource(strings = {
        "ts > 9223372036854775807",
        "9223372036854775807 < ts",
        "ts >= 9223372036854775807",
        "ts = 9223372036854775807"
    })
    void aPredicateNoSampleSatisfiesDeletesNothing(final String where) {
      database.command("sql", "DELETE FROM D1 WHERE " + where);

      assertThat(count("SELECT FROM D1")).isEqualTo(3L);
    }

    // #10528
    @ParameterizedTest
    @ValueSource(strings = {
        "ts < -9223372036854775808",
        "-9223372036854775808 > ts"
    })
    void aLowerBoundBelowEveryTimestampIsNeverAScopeAndNeverDeletesAnything(final String where) {
      // Long.MIN_VALUE is the sentinel for "not a timestamp", so the push-down declines it and the DELETE is refused as unsupported
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM D1 WHERE " + where)).isInstanceOf(CommandExecutionException.class);

      assertThat(count("SELECT FROM D1")).isEqualTo(3L);
    }

    // #10528
    @Test
    void aBoundParameterAtTheExtremeIsTheSamePredicate() {
      assertThat(count("SELECT FROM D1 WHERE ts > :max", Map.of("max", MAX))).isZero();
      assertThat(count("SELECT FROM D1 WHERE :max < ts", Map.of("max", MAX))).isZero();

      database.command("sql", "DELETE FROM D1 WHERE ts > :max", Map.of("max", MAX));

      assertThat(count("SELECT FROM D1")).isEqualTo(3L);
    }

    // #10528
    @Test
    void theNeighboursOfTheExtremeKeepTheirMeaning() {
      // the predicates the issue lists as behaving, and the ones the fix must not turn into an empty range
      assertThat(count("SELECT FROM D1 WHERE ts > 9223372036854775806")).isZero();
      assertThat(count("SELECT FROM D1 WHERE ts < 9223372036854775807")).isEqualTo(3L);
      assertThat(count("SELECT FROM D1 WHERE ts > -9223372036854775807")).isEqualTo(3L);
      assertThat(count("SELECT FROM D1 WHERE ts <= 9223372036854775807")).isEqualTo(3L);
      assertThat(count("SELECT FROM D1 WHERE ts >= 2000")).isEqualTo(2L);
      assertThat(count("SELECT FROM D1 WHERE ts > 2000")).isEqualTo(1L);
      assertThat(count("SELECT FROM D1 WHERE 2000 < ts")).isEqualTo(1L);
      assertThat(count("SELECT FROM D1 WHERE 2000 >= ts")).isEqualTo(2L);
      // the empty block of a union adds no sample and takes none away
      assertThat(count("SELECT FROM D1 WHERE ts > 9223372036854775807 OR ts >= 2000")).isEqualTo(2L);
      assertThat(count("SELECT FROM D1 WHERE ts >= 2000 OR 9223372036854775807 < ts")).isEqualTo(2L);
    }

    // #10528
    @Test
    void deleteAboveTheLastSampleStillRemovesTheSamplesAbove() {
      database.command("sql", "DELETE FROM D1 WHERE ts > 1000");

      assertThat(count("SELECT FROM D1")).isEqualTo(1L);
    }

    private long count(final String sql) {
      return count(sql, Map.of());
    }

    private long count(final String sql, final Map<String, Object> parameters) {
      long rows = 0;
      try (final ResultSet resultSet = database.query("sql", sql, parameters)) {
        while (resultSet.hasNext()) {
          final Result ignored = resultSet.next();
          rows++;
        }
      }
      return rows;
    }
  }
}
