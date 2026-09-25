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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for four openCypher temporal issues:
 * <ul>
 *   <li>#8387: a duration's sub-second fraction was formatted through the default locale, so on an Arabic, Persian,
 *   Bengali, ... JVM the stored duration no longer parsed back;</li>
 *   <li>#8386: {@code duration.between()} borrowed a fixed 30-day month, so {@code from + duration.between(from, to)}
 *   missed {@code to} by a day or two;</li>
 *   <li>#8385: {@code duration()} cast its sub-second total to a 32-bit int, so any total beyond 2.147483648 seconds
 *   wrapped, often to a negative duration;</li>
 *   <li>#8384: a schema-declared STRING property holding an opening-hours range ({@code "09:00-17:00"}) read back
 *   through openCypher as a zoned Time.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8387Issue8386Issue8385Issue8384TemporalTest extends TestHelper {

  // ---------------------------------------------------------------- #8387

  @ParameterizedTest
  @ValueSource(strings = { "ar-SA", "ar-EG", "fa-IR", "bn-BD", "my-MM", "ne-NP", "hi-IN-u-nu-deva", "en-US" })
  void durationFractionIsAsciiWhateverTheDefaultLocale(final String languageTag) {
    final Locale previous = Locale.getDefault();
    final Locale previousFormat = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.forLanguageTag(languageTag));

      final CypherDuration duration = new CypherDuration(0, 0, 90, 123_456_789);
      assertThat(duration.toString()).isEqualTo("PT1M30.123456789S");
      assertThat(CypherDuration.parse(duration.toString())).isEqualTo(duration);

      // Trailing zeros are stripped, which the ASCII-only strip silently stopped doing on localized digits.
      final CypherDuration trailingZeros = new CypherDuration(0, 0, 1, 123_000_000);
      final Object stored = TemporalUtil.toCoreJavaType(trailingZeros);
      assertThat(stored).isEqualTo("PT1.123S");
      assertThat(TemporalUtil.convertFromStorage(stored)).isEqualTo(trailingZeros);

      assertThat(new CypherDuration(0, 0, 0, 1).toString()).isEqualTo("PT0.000000001S");
      assertThat(new CypherDuration(0, 0, -1, 500_000_000).toString()).isEqualTo("PT-0.5S");
    } finally {
      Locale.setDefault(previous);
      Locale.setDefault(Locale.Category.FORMAT, previousFormat);
    }
  }

  // ---------------------------------------------------------------- #8386

  /**
   * {@code from + duration.between(from, to)} lands on {@code to}, for every month length the borrow can meet (28, 29,
   * 30 and 31 days) and both directions.
   */
  @Test
  void durationBetweenAddedBackToTheStartLandsOnTheEnd() {
    final LocalDateTime[] starts = { LocalDateTime.parse("2023-01-15T12:00"), LocalDateTime.parse("2024-01-10T10:00"),
        LocalDateTime.parse("2024-01-31T12:00"), LocalDateTime.parse("2024-03-15T10:00"), LocalDateTime.parse("2024-04-15T10:00"),
        LocalDateTime.parse("2023-02-28T23:59:59.5"), LocalDateTime.parse("2024-12-31T00:00:00.1") };
    final List<String> mismatches = new ArrayList<>();
    for (final LocalDateTime from : starts)
      for (int monthsAhead = -14; monthsAhead <= 14; monthsAhead++)
        for (final int hourShift : new int[] { -23, -2, 0, 3 }) {
          final LocalDateTime to = from.plusMonths(monthsAhead).plusHours(hourShift);
          final CypherDuration between = TemporalUtil.durationBetween(new CypherLocalDateTime(from), new CypherLocalDateTime(to));
          final LocalDateTime back = from.plusMonths(between.getMonths()).plusDays(between.getDays())
              .plusSeconds(between.getSeconds()).plusNanos(between.getNanosAdjustment());
          if (!back.equals(to))
            mismatches.add(from + " -> " + to + " = " + between + " lands on " + back);
        }
    assertThat(mismatches).isEmpty();
  }

  @Test
  void durationBetweenMatchesNeo4jForTheReportedPairs() {
    assertThat(between("2024-03-15T10:00", "2024-04-15T08:00")).isEqualTo("P30DT22H");
    assertThat(between("2023-01-15T12:00", "2023-02-15T06:00")).isEqualTo("P30DT18H");
    assertThat(between("2024-01-15T12:00", "2024-02-15T06:00")).isEqualTo("P30DT18H");
    assertThat(between("2024-01-10T10:00", "2024-03-10T08:00")).isEqualTo("P1M28DT22H");
    assertThat(between("2024-01-31T12:00", "2024-02-29T06:00")).isEqualTo("P28DT18H");
    assertThat(between("2024-04-15T08:00", "2024-03-15T10:00")).isEqualTo("P-30DT-22H");
  }

  @Test
  void durationBetweenRoundTripsThroughCypher() {
    try (final ResultSet rs = database.query("opencypher",
        "WITH localdatetime('2024-01-15T12:00') AS f, localdatetime('2024-02-15T06:00') AS t "
            + "RETURN f + duration.between(f, t) = t AS same, toString(duration.between(f, t)) AS d")) {
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("same")).isTrue();
      assertThat(row.<String>getProperty("d")).isEqualTo("P30DT18H");
    }
  }

  private static String between(final String from, final String to) {
    return TemporalUtil.durationBetween(new CypherLocalDateTime(LocalDateTime.parse(from)),
        new CypherLocalDateTime(LocalDateTime.parse(to))).toString();
  }

  // ---------------------------------------------------------------- #8385

  @Test
  void subSecondFieldsBeyondTheIntRangeDoNotWrap() {
    assertThat(duration("{nanoseconds: 3000000000}")).isEqualTo("PT3S");
    assertThat(duration("{nanoseconds: 10000000000}")).isEqualTo("PT10S");
    assertThat(duration("{microseconds: 5000000}")).isEqualTo("PT5S");
    assertThat(duration("{milliseconds: 5000}")).isEqualTo("PT5S");
    assertThat(duration("{days: 1, milliseconds: 5000}")).isEqualTo("P1DT5S");
    assertThat(duration("{seconds: 1, milliseconds: 3000}")).isEqualTo("PT4S");
    assertThat(duration("{hours: 1, microseconds: 2500000}")).isEqualTo("PT1H2.5S");
    assertThat(duration("{days: 1, nanoseconds: 3000000001}")).isEqualTo("P1DT3.000000001S");
    assertThat(duration("{seconds: 1, nanoseconds: -3000000000}")).isEqualTo("PT-2S");
    assertThat(duration("{milliseconds: 2500.5}")).isEqualTo("PT2.5005S");
    // A nanosecond count past 2^53 stays exact: a double sum would round it
    assertThat(duration("{minutes: 1, nanoseconds: 9007199254740993}")).isEqualTo("PT2502H59.254740993S");
  }

  @Test
  void subSecondFieldsFromAMapAreExactDirectly() {
    final CypherDuration duration = CypherDuration.fromMap(Map.of("nanoseconds", 3_000_000_000L));
    assertThat(duration.getSeconds()).isEqualTo(3);
    assertThat(duration.getNanosAdjustment()).isZero();
  }

  private String duration(final String map) {
    try (final ResultSet rs = database.query("opencypher", "RETURN toString(duration(" + map + ")) AS d")) {
      return rs.next().getProperty("d");
    }
  }

  // ---------------------------------------------------------------- #8384

  @Test
  void declaredStringPropertyIsNotSniffedAsATemporal() {
    database.getSchema().createVertexType("Shop8384").createProperty("hours", Type.STRING);
    database.getSchema().getType("Shop8384").createProperty("code", Type.STRING);
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Shop8384 {name: 'a', hours: '09:00-17:00', code: 'P100D'})");
      database.command("opencypher", "CREATE (:Shop8384 {name: 'b', hours: '09:00 to 17:00', code: 'X'})");
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (s:Shop8384 {name: 'a'}) RETURN s.hours AS h, s.code AS c")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("h")).isInstanceOf(String.class).isEqualTo("09:00-17:00");
      assertThat(row.<Object>getProperty("c")).isInstanceOf(String.class).isEqualTo("P100D");
    }

    assertThat(names("MATCH (s:Shop8384) WHERE s.hours = '09:00-17:00' RETURN s.name AS n")).containsExactly("a");
    assertThat(names("MATCH (s:Shop8384) WHERE s.hours STARTS WITH '09:00' RETURN s.name AS n ORDER BY n")).containsExactly("a", "b");
    assertThat(names("MATCH (s:Shop8384) WHERE s.code = 'P100D' RETURN s.name AS n")).containsExactly("a");
    assertThat(names("MATCH (s:Shop8384) RETURN toUpper(s.hours) AS n ORDER BY n")).containsExactly("09:00 TO 17:00", "09:00-17:00");
    assertThat(names("MATCH (s:Shop8384) RETURN s.name AS n ORDER BY s.hours DESC")).containsExactly("a", "b");
  }

  /** A declared property inherited from a supertype is honoured too. */
  @Test
  void inheritedDeclaredStringPropertyIsNotSniffed() {
    database.getSchema().createVertexType("Place8384").createProperty("hours", Type.STRING);
    database.getSchema().createVertexType("Bar8384").addSuperType("Place8384");
    database.transaction(() -> database.command("opencypher", "CREATE (:Bar8384 {hours: '12:00-13:00'})"));

    try (final ResultSet rs = database.query("opencypher", "MATCH (b:Bar8384) RETURN b.hours AS h")) {
      assertThat(rs.next().<Object>getProperty("h")).isEqualTo("12:00-13:00");
    }
  }

  /** An undeclared property still round-trips a Cypher time, which is what the sniff exists for. */
  @Test
  void undeclaredPropertyStillRestoresATemporal() {
    database.getSchema().createVertexType("Event8384");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Event8384 {at: time('10:35-08:00'), took: duration('PT1M30.5S')})"));

    try (final ResultSet rs = database.query("opencypher", "MATCH (e:Event8384) RETURN e.at AS at, e.took.seconds AS s")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("at")).isInstanceOf(CypherTime.class);
      assertThat(row.<Long>getProperty("s")).isEqualTo(90L);
    }
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("n"));
    }
    return names;
  }
}
