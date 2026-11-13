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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Expected values taken from Neo4j's value unit tests (community/values, 2026.10: DurationValueTest and TernaryEqualityTest),
 * replayed as Cypher. Only the cases the openCypher TCK does not already cover: signed and fractional duration components,
 * the text form of a duration whose seconds and nanoseconds differ in sign, a datetime plus the duration between it and another
 * landing on that other one across a daylight saving change, and equality of maps and lists holding a null.
 */
class CypherNeo4jValueSemanticsTest extends TestHelper {

  private static String d(final long months, final long days, final long seconds, final long nanos) {
    return "duration({months: " + months + ", days: " + days + ", seconds: " + seconds + ", nanoseconds: " + nanos + "})";
  }

  /** DurationValueTest.shouldParseDuration, shouldParseNegativeDuration(WithFractionalComponent) and the conversion of fractions. */
  @Test
  void signedAndFractionalDurationComponents() {
    assertEqual(new String[][] { //
        { "duration('PT-0.000000009S')", d(0, 0, 0, -9) }, { "duration('PT-1,1S')", d(0, 0, -1, -100_000_000) },
        { "duration('P1Y-2M')", d(10, 0, 0, 0) }, { "duration('P3W-1D')", d(0, 20, 0, 0) },
        { "duration('PT1H-10M')", d(0, 0, 3000, 0) }, { "duration('PT1H-600S')", d(0, 0, 3000, 0) },
        { "duration('PT1M-10S')", d(0, 0, 50, 0) }, { "duration('-P-1Y')", d(12, 0, 0, 0) },
        { "duration('-P1Y-2M3DT-4H5M-6S')", d(-10, -3, 14106, 0) }, { "duration('+P-1D')", d(0, -1, 0, 0) },
        { "duration('-P-1D')", d(0, 1, 0, 0) }, { "duration('-P0.5Y')", d(-6, 0, 0, 0) },
        { "duration('-P1.5W')", d(0, -10, -43200, 0) }, { "duration('-P-1Y2M1.5W')", d(10, -10, -43200, 0) },
        { "duration('-P3.1D')", d(0, -3, -8640, 0) }, { "duration('-P5M-3.1D')", d(-5, 3, 8640, 0) },
        { "duration('-PT-0.2H')", d(0, 0, 720, 0) }, { "duration('-P1DT-1.1M')", d(0, -1, 66, 0) },
        { "duration('-PT-6.55S')", d(0, 0, 6, 550_000_000) }, { "duration('P2W2.5D')", d(0, 16, 43200, 0) },
        { "duration('PT1H72.5M')", d(0, 0, 7950, 0) }, { "duration('PT1H72M72.5S')", d(0, 0, 7992, 500_000_000) },
        { "duration('P14.2M')", "duration('P1Y2.2M')" }, { "duration('P1Y-0.5M')", "duration('-P-1Y0.5M')" } });
  }

  /** DurationValueTest.shouldFormatDurationToString: seconds and nanoseconds of different signs. */
  @Test
  void durationTextWithSecondsAndNanosecondsOfDifferentSigns() {
    final long[][] secondsNanos = { { 59, -500_000_000 }, { 60, -500_000_000 }, { 60, 500_000_000 }, { -59, 500_000_000 },
        { -60, -500_000_000 }, { -61, 500_000_000 }, { -61, -500_000_000 }, { 0, -5 }, { 1, -5 }, { -1, 5 }, { 1, -999_999_999 },
        { -1, 999_999_999 }, { -78036, -143_000_000 } };
    final String[] text = { "PT58.5S", "PT59.5S", "PT1M0.5S", "PT-58.5S", "PT-1M-0.5S", "PT-1M-0.5S", "PT-1M-1.5S",
        "PT-0.000000005S", "PT0.999999995S", "PT-0.999999995S", "PT0.000000001S", "PT-0.000000001S", "PT-21H-40M-36.143S" };
    for (int i = 0; i < text.length; i++)
      assertThat(value("RETURN toString(" + d(0, 0, secondsNanos[i][0], secondsNanos[i][1]) + ") AS v")).as(text[i])
          .isEqualTo(text[i]);
  }

  /**
   * DurationValueTest.shouldGetSameInstantWhenAddingDurationBetweenToInstant: across the CET/CEST changes of 2017, a plus the
   * duration between a and b is b, whether the time of day of b is before or after that of a.
   */
  @Test
  void addingTheDurationBetweenTwoDateTimesLandsOnTheSecondAcrossDaylightSaving() {
    final String[][] pairs = { { "2017-03-20T13:37", "2017-03-26T19:40" }, { "2017-03-20T13:37", "2017-03-26T11:40" },
        { "2017-10-20T13:37", "2017-10-29T19:40" }, { "2017-10-20T13:37", "2017-10-29T11:40" } };
    final List<String[]> cases = new ArrayList<>();
    for (final String[] pair : pairs) {
      final String a = "datetime('" + pair[0] + "[Europe/Stockholm]')";
      final String b = "datetime('" + pair[1] + "[Europe/Stockholm]')";
      cases.add(new String[] { a + " + duration.between(" + a + ", " + b + ")", b });
      cases.add(new String[] { b + " + duration.between(" + b + ", " + a + ")", a });
      cases.add(new String[] { a + " + duration.inSeconds(" + a + ", " + b + ")", b });
      cases.add(new String[] { b + " + duration.inSeconds(" + b + ", " + a + ")", a });
    }
    assertEqual(cases.toArray(new String[0][]));
  }

  /** DurationValueTest.shouldComputeDurationBetweenTimes / DateAndTime / DateTimeAndTime, and shouldNotEqualOther. */
  @Test
  void durationBetweenTimesOfDifferentKinds() {
    assertEqual(new String[][] { //
        { "duration.between(time('11:30:52+18:00'), time('14:27:55-18:00'))", d(0, 0, 140223, 0) },
        { "duration.between(time('11:30:52+17:00'), localtime('14:27:55'))", d(0, 0, 10623, 0) },
        { "duration.between(localtime('14:27:55'), time('11:30:52+17:00'))", d(0, 0, -10623, 0) },
        { "duration.between(date('2017-12-21'), time('14:32:11-12:00'))", "duration('PT14H32M11S')" },
        { "duration.between(time('14:32:11-12:00'), date('2017-12-21'))", "duration('-PT14H32M11S')" },
        { "duration.inDays(localtime('14:32:11'), date('2017-12-21'))", "duration('P0D')" },
        { "duration.between(datetime('2017-12-21T06:52:11Z'), localtime('14:32:11'))", "duration('PT8H-20M')" },
        { "duration.between(localtime('14:32:11'), datetime('2017-12-21T06:52:11Z'))", "duration('PT-8H+20M')" } });
    // the average length of a month, a day of seconds and the days of 400 years make no equal durations
    for (final String unequal : new String[] { d(1, 0, 0, 0) + " = " + d(0, 0, 2_629_800, 0), d(0, 1, 0, 0) + " = " + d(0, 0, 86400, 0),
        d(4800, 0, 0, 0) + " = " + d(0, 146_097, 0, 0) })
      assertThat(value("RETURN " + unequal + " AS v")).as(unequal).isEqualTo(false);
  }

  /** TernaryEqualityTest.shouldHandleListsWithNoValue and shouldHandleMaps: a null decides only when nothing else does. */
  @Test
  void equalityOfListsAndMapsHoldingANull() {
    final String[][] cases = { //
        { "[1, null, 2]", "[1, null]", "false" }, { "[1, null, 2]", "[2, null, 2]", "false" },
        { "[1, null, 2]", "[1, null, 3]", "false" }, { "[1, null, 2]", "[1, null, 2]", null },
        { "[null, null, null]", "[null, null]", "false" }, { "[[1, 2, 3]]", "[[1.0, 2.0, 3.0]]", "true" },
        { "{key: [42]}", "{key: [42.0]}", "true" }, { "{key: [42]}", "{key: null}", null },
        { "{key1: [42]}", "{key2: null}", "false" }, { "{k1: 42, k2: null}", "{k1: 43, k2: 45}", "false" },
        { "{k1: 42, k2: null}", "{k1: 42, k2: 45}", null },
        // the same with the null met first: a later definite false still decides
        { "{k1: null, k2: 42}", "{k1: 45, k2: 43}", "false" }, { "[null, 1]", "[2, 3]", "false" } };
    for (final String[] c : cases)
      for (final String query : new String[] { "RETURN " + c[0] + " = " + c[1] + " AS v", "RETURN " + c[1] + " = " + c[0] + " AS v" })
        assertThat(value(query)).as(query).isEqualTo(c[2] == null ? null : Boolean.valueOf(c[2]));
  }

  private void assertEqual(final String[][] cases) {
    for (final String[] c : cases)
      try (final ResultSet rs = database.query("opencypher",
          "RETURN " + c[0] + " = " + c[1] + " AS ok, toString(" + c[0] + ") AS got, toString(" + c[1] + ") AS expected")) {
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("ok")).as("%s: got %s, expected %s", c[0], row.getProperty("got"),
            row.getProperty("expected")).isTrue();
      }
  }

  private Object value(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }
}
