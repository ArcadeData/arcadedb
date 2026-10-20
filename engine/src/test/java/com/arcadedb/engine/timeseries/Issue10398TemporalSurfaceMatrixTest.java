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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue10398TemporalSurfaceMatrixTest extends TestHelper {
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

  /** Same column, same literal, two instants three orders of magnitude apart, and no refusal on either side (issue #10398). */
  @Test
  void aLongIntoADatetimeSecondColumnIsMillisOnSqlAndSecondsOnTheWire() throws Exception {
    database.command("sql", "INSERT INTO T_DATETIME_SECOND SET ts = 1, f = " + SECONDS);
    TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("T_DATETIME_SECOND", Map.of(), Map.of("f", SECONDS), 2))));

    assertThat(count("T_DATETIME_SECOND", 1)).isEqualTo(SECONDS / 1_000L);
    assertThat(count("T_DATETIME_SECOND", 2)).isEqualTo(SECONDS);
  }

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

      // a number: epoch millis on SQL (days for a DATE), the column's own unit on the wire
      cells.add(cell(columnType, "number the size of epoch millis", MILLIS, date ? REFUSED : sqlCountOf(columnType, MILLIS), MILLIS));
      // a DATE reads the same number as days on both, so the two agree on it
      cells.add(cell(columnType, "number the size of epoch seconds", SECONDS, date ? SECONDS : sqlCountOf(columnType, SECONDS), SECONDS));
      // text of digits: no date for SQL, a count for the wire
      cells.add(cell(columnType, "text of digits the size of epoch millis", "" + MILLIS, REFUSED, MILLIS));
      // ... except for a DATE, whose SQL reading of text of digits is a number of days, so '0' and '19675' agree with the wire
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
