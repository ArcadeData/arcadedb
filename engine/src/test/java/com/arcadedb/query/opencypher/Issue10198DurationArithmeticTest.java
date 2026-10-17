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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.function.agg.CypherAvgFunction;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10198: Cypher duration arithmetic differed from Neo4j. Every expected value below was checked against Neo4j
 * 2026.09, except where a comment says the result is exact where Neo4j's double precision cascade is not.
 * <ul>
 *   <li>dividing or multiplying a duration rounded each carry at 40 digits, so {@code duration('P1D') / 24} was one
 *   nanosecond short of an hour, and an INTEGER operand went through a double;</li>
 *   <li>{@code duration.between}, {@code inDays} and {@code inMonths} converted zoned operands to UTC and so counted
 *   calendar units in elapsed time across a daylight saving change;</li>
 *   <li>{@code sum()} and {@code avg()} refused durations.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10198DurationArithmeticTest extends TestHelper {

  @Test
  void divisionIsExactUntilTheNanosecond() {
    assertThat(text("duration('P1D') / 24")).isEqualTo("PT1H");
    assertThat(text("duration('P1D') / 24.0")).isEqualTo("PT1H");
    assertThat(text("duration('-P1D') / 24")).isEqualTo("PT-1H");
    assertThat(text("duration('P1D') / -24")).isEqualTo("PT-1H");
    assertThat(text("duration('P1D') / 3")).isEqualTo("PT8H");
    assertThat(text("duration('P1D') / 2")).isEqualTo("PT12H");
    assertThat(text("duration('P1M') / 3")).isEqualTo("P10DT3H29M42S");
    assertThat(text("duration('P2M') / 3")).isEqualTo("P20DT6H59M24S");
    assertThat(text("duration('P1M') / 1.5")).isEqualTo("P20DT6H59M24S");
    assertThat(text("duration('P5M') / 2")).isEqualTo("P2M15DT5H14M33S");
    assertThat(text("duration('P1M1DT1H') / 2")).isEqualTo("P15DT17H44M33S");
    assertThat(text("duration('P1M2DT3H') / 7")).isEqualTo("P4DT15H38M26.571428571S");
    assertThat(text("duration('PT10S') / 4")).isEqualTo("PT2.5S");
    assertThat(text("duration('P1D') / 0.5")).isEqualTo("P2D");
    assertThat(text("duration('P1D') / 86400000000000")).isEqualTo("PT0.000000001S");
    assertThat(text("duration('P1D') / 172800000000000")).isEqualTo("PT0S");
  }

  @Test
  void anInexactQuotientIsTruncatedTowardZero() {
    assertThat(text("duration('PT1S') / 3")).isEqualTo("PT0.333333333S");
    assertThat(text("duration('PT2S') / 3")).isEqualTo("PT0.666666666S");
    assertThat(text("duration('PT1H') / 7")).isEqualTo("PT8M34.285714285S");
    assertThat(text("duration('PT0.000000003S') / 2")).isEqualTo("PT0.000000001S");
    assertThat(text("duration('-PT0.000000003S') / 2")).isEqualTo("PT-0.000000001S");
    assertThat(text("duration('PT0.000000005S') * 0.3")).isEqualTo("PT0.000000001S");
    assertThat(text("duration('PT0.000000001S') * 1.5")).isEqualTo("PT0.000000001S");
  }

  @Test
  void multiplicationIsExact() {
    assertThat(text("duration('PT1S') * 3")).isEqualTo("PT3S");
    assertThat(text("2 * duration('PT10S')")).isEqualTo("PT20S");
    assertThat(text("0.5 * duration('PT10S')")).isEqualTo("PT5S");
    assertThat(text("duration('PT10S') * 0")).isEqualTo("PT0S");
    assertThat(text("duration('P1D') * 1.5")).isEqualTo("P1DT12H");
    assertThat(text("duration('P1M') * 1.5")).isEqualTo("P1M15DT5H14M33S");
    assertThat(text("duration('P1M') * 2.5")).isEqualTo("P2M15DT5H14M33S");
    assertThat(text("duration('P1Y') * 0.1")).isEqualTo("P1M6DT2H5M49.2S");
  }

  @Test
  void anIntegerOperandIsNeverRoundedThroughADouble() {
    // 9223372036854775807 as a double is 2^63, one nanosecond more than the operand
    assertThat(text("duration('PT0.000000001S') * 9223372036854775807")).isEqualTo("PT2562047H47M16.854775807S");
    assertThat(text("9223372036854775807 * duration('PT0.000000001S')")).isEqualTo("PT2562047H47M16.854775807S");
    assertThat(text("duration('PT2562047H47M16.854775807S') / 9223372036854775807")).isEqualTo("PT0.000000001S");
  }

  /**
   * Where Neo4j's answer carries the rounding error of its double precision cascade, the exact one is kept: a floating
   * point operand is the decimal it is written as, so {@code * 0.1} is one tenth.
   */
  @Test
  void aFloatingPointOperandIsTheDecimalItIsWrittenAs() {
    // Neo4j: PT7H18M17.459999999S, P3DT1H2M54.599999999S, P1M21DT17H46M29.999999999S
    assertThat(text("duration('P1M') / 100")).isEqualTo("PT7H18M17.46S");
    assertThat(text("duration('P1M') * 0.1")).isEqualTo("P3DT1H2M54.6S");
    assertThat(text("duration('P1Y') / 7")).isEqualTo("P1M21DT17H46M30S");
    // 0.3333333333333333 is not one third: an hour times it is a fraction of a nanosecond short of 20 minutes (Neo4j: PT20M)
    assertThat(text("duration('PT1H') * 0.3333333333333333")).isEqualTo("PT19M59.999999999S");
  }

  @Test
  void divisionByZeroOrANonFiniteNumberIsRefused() {
    assertThatThrownBy(() -> text("duration('P1D') / 0")).hasMessageContaining("divide duration by zero");
    assertThatThrownBy(() -> text("duration('P1D') / 0.0")).hasMessageContaining("divide duration by zero");
  }

  @Test
  void betweenCountsCalendarDaysAcrossADaylightSavingChange() {
    assertThat(between("datetime('2024-03-30T12:00[Europe/Paris]')", "datetime('2024-03-31T12:00[Europe/Paris]')")).isEqualTo("P1D");
    assertThat(between("datetime('2024-10-26T12:00[Europe/Paris]')", "datetime('2024-10-27T12:00[Europe/Paris]')")).isEqualTo("P1D");
    assertThat(between("datetime('2024-01-01T00:00[Europe/Paris]')", "datetime('2024-07-01T00:00[Europe/Paris]')")).isEqualTo("P6M");
    assertThat(between("datetime('2024-07-01T00:00[Europe/Paris]')", "datetime('2024-01-01T00:00[Europe/Paris]')")).isEqualTo("P-6M");
    // The elapsed hours of the day the clocks move are what is left after the calendar day
    assertThat(between("datetime('2024-03-31T01:30[Europe/Paris]')", "datetime('2024-03-31T03:30[Europe/Paris]')")).isEqualTo("PT1H");
    assertThat(between("datetime('2024-03-30T02:30[Europe/Paris]')", "datetime('2024-03-31T03:30[Europe/Paris]')")).isEqualTo("P1D");
    assertThat(between("datetime('2024-10-27T02:30+02:00[Europe/Paris]')", "datetime('2024-10-27T02:30+01:00[Europe/Paris]')"))
        .isEqualTo("PT1H");
    assertThat(between("datetime('2024-10-26T02:30[Europe/Paris]')", "datetime('2024-10-27T02:30+01:00[Europe/Paris]')"))
        .isEqualTo("P1DT1H");
  }

  @Test
  void betweenReadsTheSecondOperandInTheZoneOfTheFirst() {
    assertThat(between("datetime('2024-01-15T12:00[Australia/Lord_Howe]')", "datetime('2024-06-15T12:00[UTC]')")).isEqualTo("P5MT10H30M");
    assertThat(between("datetime('2024-03-30T12:00[Europe/Paris]')", "datetime('2024-03-31T12:00[America/New_York]')")).isEqualTo("P1DT6H");
    // Fixed offsets have no calendar to follow: the controls that already agreed
    assertThat(between("datetime('2024-01-01T00:00+01:00')", "datetime('2024-07-01T00:00+02:00')")).isEqualTo("P5M29DT23H");
    assertThat(between("datetime('2024-01-01T00:00Z')", "datetime('2024-07-01T00:00[Europe/Paris]')")).isEqualTo("P5M29DT22H");
    assertThat(between("datetime('2024-01-01T00:00Z')", "datetime('2024-07-01T00:00Z')")).isEqualTo("P6M");
  }

  @Test
  void betweenReadsAZonelessOperandInTheZoneOfTheOther() {
    assertThat(between("date('2024-01-31')", "datetime('2024-10-27T01:59[Europe/Paris]')")).isEqualTo("P8M27DT1H59M");
    assertThat(between("localdatetime('2024-03-30T12:00')", "datetime('2024-03-31T12:00[Europe/Paris]')")).isEqualTo("P1D");
    assertThat(between("datetime('2024-03-30T12:00[Europe/Paris]')", "localdatetime('2024-03-31T12:00')")).isEqualTo("P1D");
    assertThat(between("date('2024-03-30')", "datetime('2024-03-31T12:00[Europe/Paris]')")).isEqualTo("P1DT11H");
    assertThat(between("datetime('2024-03-30T12:00[Europe/Paris]')", "date('2024-04-01')")).isEqualTo("P1DT12H");
    // Without a zone, unchanged
    assertThat(between("date('2024-01-01')", "date('2024-03-01')")).isEqualTo("P2M");
    assertThat(between("localdatetime('2024-01-31T12:00')", "localdatetime('2024-03-01T11:00')")).isEqualTo("P29DT23H");
  }

  @Test
  void inDaysAndInMonthsCountTheLocalCalendar() {
    assertThat(text("duration.inDays(datetime('2024-03-30T12:00[Europe/Paris]'), datetime('2024-03-31T12:00[Europe/Paris]'))"))
        .isEqualTo("P1D");
    assertThat(text("duration.inDays(datetime('2024-10-27T01:59[Europe/Paris]'), datetime('2024-02-29T12:30-12:00'))"))
        .isEqualTo("P-240D");
    assertThat(text("duration.inDays(datetime('1945-06-01T12:00[Europe/Berlin]'), datetime('9999-12-31T23:59:59.999999999+14:00'))"))
        .isEqualTo("P2941875D");
    assertThat(text("duration.inDays(date('2024-03-30'), datetime('2024-03-31T12:00[Europe/Paris]'))")).isEqualTo("P1D");
    assertThat(text("duration.inDays(localdatetime('2024-03-30T13:00'), datetime('2024-03-31T12:00[Europe/Paris]'))"))
        .isEqualTo("PT0S");
    assertThat(text("duration.inMonths(datetime('2024-01-01T00:00[Europe/Paris]'), datetime('2024-07-01T00:00[Europe/Paris]'))"))
        .isEqualTo("P6M");
    assertThat(text("duration.inMonths(datetime('2024-01-01T00:00Z'), datetime('2024-07-01T00:00[Europe/Paris]'))"))
        .isEqualTo("P5M");
    assertThat(text("duration.inMonths(datetime('2024-01-31T12:00[Europe/Paris]'), datetime('2024-03-31T11:00[Europe/Paris]'))"))
        .isEqualTo("P1M");
    // inSeconds is elapsed time, and already was
    assertThat(text("duration.inSeconds(datetime('2024-03-30T12:00[Europe/Paris]'), datetime('2024-03-31T12:00[Europe/Paris]'))"))
        .isEqualTo("PT23H");
    assertThat(text("duration.inSeconds(date('2024-03-30'), datetime('2024-03-31T12:00[Europe/Paris]'))")).isEqualTo("PT35H");
  }

  @Test
  void betweenStillLandsOnTheSecondOperand() {
    // from + between(from, to) is the instant of to: the property the split into months, days and clock exists for (issue
    // #8386). In the zone of from, so the instants are compared: = tells two zones apart (issue #8300)
    final String[] values = { "datetime('2024-03-30T02:30[Europe/Paris]')", "datetime('2024-10-27T02:30+01:00[Europe/Paris]')",
        "datetime('2024-01-31T12:00[Europe/Paris]')", "datetime('2024-06-15T12:00[America/New_York]')",
        "datetime('2024-02-29T23:59:59.999999999Z')" };
    for (final String from : values)
      for (final String to : values)
        try (final ResultSet rs = database.query("opencypher", "WITH " + from + " AS a, " + to + " AS b WITH a + duration.between(a, b) AS c, b "
            + "RETURN c.epochSeconds = b.epochSeconds AND c.nanosecond = b.nanosecond AS v")) {
          assertThat(rs.next().<Boolean>getProperty("v")).as("%s + between(%s, %s)", from, from, to).isTrue();
        }
  }

  @Test
  void sumAddsDurationsComponentByComponent() {
    assertThat(aggregate("sum", "duration('P1M'), duration('P1D')")).isEqualTo("P1M1D");
    assertThat(aggregate("sum", "duration('P1D'), null")).isEqualTo("P1D");
    assertThat(text("sum(x)", "UNWIND [duration('P1D'), duration('PT3H'), duration('P1D')] AS x WITH DISTINCT x RETURN ")).isEqualTo("P1DT3H");
    assertThat(aggregate("sum", "duration('P1D'), duration('PT24H')")).isEqualTo("P1DT24H");
  }

  @Test
  void avgDividesTheSumOfDurationsByTheirCount() {
    assertThat(aggregate("avg", "duration('P1M'), duration('P1D')")).isEqualTo("P15DT17H14M33S");
    assertThat(aggregate("avg", "duration('PT1S'), duration('PT2S')")).isEqualTo("PT1.5S");
    assertThat(aggregate("avg", "duration('PT1S'), duration('PT2S'), duration('PT2S')")).isEqualTo("PT1.666666666S");
    assertThat(aggregate("avg", "duration('-PT1S'), duration('-PT1S'), duration('-PT2S')")).isEqualTo("PT-1.333333333S");
    assertThat(aggregate("avg", "duration('P1Y'), duration('P1Y'), duration('P1Y')")).isEqualTo("P1Y");
    assertThat(aggregate("avg", "duration('P1M'), duration('P2M')")).isEqualTo("P1M15DT5H14M33S");
    assertThat(aggregate("avg", "duration('P1D'), duration('P2D'), duration('P2D')")).isEqualTo("P1DT16H");
    assertThat(aggregate("avg", "duration('P1D'), null, duration('PT1H')")).isEqualTo("PT12H30M");
    // Exact, where Neo4j answers P1M10DT3H29M41.999999999S
    assertThat(aggregate("avg", "duration('P1M'), duration('P1M'), duration('P2M')")).isEqualTo("P1M10DT3H29M42S");
  }

  @Test
  void anAggregateOverNoDurationIsUnchanged() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [] AS x RETURN sum(x) AS s, avg(x) AS a")) {
      final var row = rs.next();
      assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(0L);
      assertThat(row.<Object>getProperty("a")).isNull();
    }
    // Numbers are still averaged as numbers
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 2] AS x RETURN avg(x) AS a, sum(x) AS s")) {
      final var row = rs.next();
      assertThat(row.<Double>getProperty("a")).isEqualTo(1.5);
      assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(3L);
    }
  }

  @Test
  void numbersAndDurationsDoNotMix() {
    assertThatThrownBy(() -> aggregate("sum", "duration('P1D'), 1")).hasMessageContaining("cannot mix numbers and durations");
    assertThatThrownBy(() -> aggregate("sum", "1, duration('P1D')")).hasMessageContaining("cannot mix numbers and durations");
    assertThatThrownBy(() -> aggregate("avg", "duration('P1D'), 1")).hasMessageContaining("cannot mix numbers and durations");
    assertThatThrownBy(() -> aggregate("avg", "1, duration('P1D')")).hasMessageContaining("cannot mix numbers and durations");
    assertThatThrownBy(() -> aggregate("avg", "'a'")).hasMessageContaining("DURATION");
  }

  @Test
  void aSqlSumOfStoredDurationsIsTheirDuration() {
    database.getSchema().createVertexType("Shift").createProperty("length", Type.DURATION);
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Shift {length: duration('PT8H')})");
      database.command("opencypher", "CREATE (:Shift {length: duration('PT30M')})");
    });
    try (final ResultSet rs = database.query("sql", "SELECT sum(length) AS total FROM Shift")) {
      assertThat(String.valueOf(rs.next().<Object>getProperty("total"))).isEqualTo("PT8H30M");
    }
  }

  @Test
  void nullsBeforeDurationsAreSkipped() {
    assertThat(aggregate("sum", "null, null, duration('P1D'), duration('PT1H')")).isEqualTo("P1DT1H");
    assertThat(aggregate("avg", "null, duration('P1D'), null, duration('PT1H')")).isEqualTo("PT12H30M");
    try (final ResultSet rs = database.query("opencypher", "UNWIND [null, null] AS x RETURN avg(x) AS a, sum(x) AS s")) {
      final var row = rs.next();
      assertThat(row.<Object>getProperty("a")).isNull();
      assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(0L);
    }
  }

  /** Parallel aggregation merges partial states: a duration partial merges as the rows would have added up. */
  @Test
  void partialAggregatesMergeDurations() {
    final SQLFunctionSum sumA = new SQLFunctionSum();
    final SQLFunctionSum sumB = new SQLFunctionSum();
    sumA.execute(null, null, null, new Object[] { CypherDuration.parse("P1D") }, null);
    sumB.execute(null, null, null, new Object[] { null }, null);
    sumB.execute(null, null, null, new Object[] { CypherDuration.parse("PT2H") }, null);
    sumA.mergePartial(sumB);
    assertThat(sumA.getResult()).hasToString("P1DT2H");

    final SQLFunctionSum numbers = new SQLFunctionSum();
    numbers.execute(null, null, null, new Object[] { 5 }, null);
    final SQLFunctionSum durations = new SQLFunctionSum();
    durations.execute(null, null, null, new Object[] { CypherDuration.parse("P1D") }, null);
    assertThatThrownBy(() -> numbers.mergePartial(durations)).hasMessageContaining("cannot mix numbers and durations");
    // The unboxed per-row path refuses a number after a duration, and a duration after a number, the same way
    final SQLFunctionSum durationFirst = new SQLFunctionSum();
    durationFirst.aggregate(null, new Object[] { CypherDuration.parse("P1D") }, null);
    assertThatThrownBy(() -> durationFirst.aggregate(null, new Object[] { 5 }, null)).hasMessageContaining("cannot mix numbers and durations");
    final SQLFunctionSum numberFirst = new SQLFunctionSum();
    numberFirst.aggregate(null, new Object[] { 5L }, null);
    assertThatThrownBy(() -> numberFirst.aggregate(null, new Object[] { CypherDuration.parse("P1D") }, null))
        .hasMessageContaining("cannot mix numbers and durations");

    final CypherAvgFunction avgA = new CypherAvgFunction();
    final CypherAvgFunction avgB = new CypherAvgFunction();
    final CypherAvgFunction empty = new CypherAvgFunction();
    avgA.execute(new Object[] { CypherDuration.parse("PT1S") }, null);
    avgB.execute(new Object[] { CypherDuration.parse("PT2S") }, null);
    avgB.execute(new Object[] { CypherDuration.parse("PT2S") }, null);
    avgA.mergePartial(avgB);
    avgA.mergePartial(empty);
    assertThat(avgA.getAggregatedResult()).hasToString("PT1.666666666S");

    final CypherAvgFunction avgNumbers = new CypherAvgFunction();
    avgNumbers.execute(new Object[] { 1 }, null);
    final CypherAvgFunction avgDurations = new CypherAvgFunction();
    avgDurations.execute(new Object[] { CypherDuration.parse("P1D") }, null);
    assertThatThrownBy(() -> avgNumbers.mergePartial(avgDurations)).hasMessageContaining("cannot mix numbers and durations");
  }

  @Test
  void aNonFiniteOperandOrAnOverflowIsAnErrorNotAWrap() {
    for (final double operand : new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
      assertThatThrownBy(() -> parameterized("duration('P1D') * $f", operand)).as("* %s", operand).isInstanceOf(ArithmeticErrorException.class);
      assertThatThrownBy(() -> parameterized("duration('P1D') / $f", operand)).as("/ %s", operand).isInstanceOf(ArithmeticErrorException.class);
    }
    assertThatThrownBy(() -> text("duration({months: 9223372036854775807}) * 2")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> text("duration({seconds: 9223372036854775807}) * 1.5")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> text("duration({days: 9223372036854775807}) / 0.5")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> text("duration({months: -9223372036854775807 - 1}) / -1")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> text("duration({months: -9223372036854775807 - 1}) * -1")).isInstanceOf(ArithmeticErrorException.class);
    // A product only the nanosecond part of which overflows a long is still exact
    assertThat(text("duration('PT0.999999999S') * 9223372036854775807")).isEqualTo(
        CypherDuration.parse("PT0.999999999S").multiply(9223372036854775807L).toString());
  }

  private String parameterized(final String expression, final Object parameter) {
    try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v", Map.of("f", parameter))) {
      return rs.next().getProperty("v");
    }
  }

  private String between(final String from, final String to) {
    return text("duration.between(" + from + ", " + to + ")");
  }

  private String aggregate(final String function, final String values) {
    return text(function + "(x)", "UNWIND [" + values + "] AS x RETURN ");
  }

  private String text(final String expression) {
    return text(expression, "RETURN ");
  }

  private String text(final String expression, final String prefix) {
    try (final ResultSet rs = database.query("opencypher", prefix + "toString(" + expression + ") AS v")) {
      final List<String> values = new ArrayList<>();
      while (rs.hasNext())
        values.add(rs.next().getProperty("v"));
      assertThat(values).hasSize(1);
      return values.getFirst();
    }
  }
}
