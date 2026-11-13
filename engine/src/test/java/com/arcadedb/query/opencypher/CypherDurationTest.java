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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.function.agg.CypherAvgFunction;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cypher duration values: parsing, map construction, arithmetic and the duration function family.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherDurationTest {
  /**
   * Regression test for issue #9338: the Cypher DURATION type did not survive storage (short ISO texts were never
   * restored, nanoseconds drifted past ~97 days), its arithmetic wrapped or saturated silently, and temporal-by-duration
   * arithmetic accepted operators other than + and -.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9338DurationFamily {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9338");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void teardown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private Object scalar(final String query) {
      try (final ResultSet rs = database.query("cypher", query)) {
        return rs.next().getProperty("r");
      }
    }

    private void expectError(final String query) {
      expectError(query, RuntimeException.class);
    }

    private void expectError(final String query, final Class<? extends RuntimeException> expectedType) {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("cypher", query)) {
          rs.next();
        }
      }).as(query).isInstanceOf(expectedType);
    }

    @Test
    void shortIsoDurationIsRestoredFromStorage() {
      database.transaction(() -> database.command("cypher", "CREATE (:T {d: duration({days: 1})})"));
      assertThat(scalar("MATCH (n:T) RETURN n.d = duration({days: 1}) AS r")).isEqualTo(true);
      assertThat(((Number) scalar("MATCH (n:T) RETURN n.d.days AS r")).longValue()).isEqualTo(1L);
    }

    @Test
    void storedDurationsOrderAndFilterAsDurations() {
      database.transaction(() -> {
        database.command("cypher", "CREATE (:S {n: '2d', d: duration({days: 2})})");
        database.command("cypher", "CREATE (:S {n: '10d', d: duration({days: 10})})");
        database.command("cypher", "CREATE (:S {n: '3d', d: duration({days: 3})})");
      });
      final List<Object> order = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", "MATCH (s:S) RETURN s.n AS r ORDER BY s.d")) {
        while (rs.hasNext())
          order.add(rs.next().getProperty("r"));
      }
      assertThat(order).containsExactly("2d", "3d", "10d");
      // Durations are equal or not, and not comparable: > answers null, so a range filter keeps no row, as in Neo4j
      // (issue #10200). Equality still filters
      assertThat(((Number) scalar("MATCH (s:S) WHERE s.d > duration({days: 2}) RETURN count(s) AS r")).longValue()).isEqualTo(0L);
      assertThat(((Number) scalar("MATCH (s:S) WHERE s.d = duration({days: 3}) RETURN count(s) AS r")).longValue()).isEqualTo(1L);
    }

    @Test
    void storageRoundTripKeepsEveryNanosecond() {
      for (final String[] c : new String[][] { { "9007199", "999999999" }, { "100000000", "123456789" },
          { "1000000000", "123456789" } }) {
        final String d = "duration({seconds: " + c[0] + ", nanoseconds: " + c[1] + "})";
        assertThat(scalar("RETURN duration(toString(" + d + ")) = " + d + " AS r")).as(d).isEqualTo(true);
      }
      assertThat(scalar("RETURN duration(toString(duration({seconds: 1234567890123456789}))) = duration({seconds: 1234567890123456789}) AS r"))
          .isEqualTo(true);
      assertThat(scalar("RETURN toString(duration('PT1000000000.123456789S')) AS r")).isEqualTo("PT277777H46M40.123456789S");
    }

    @Test
    void multiplyAndDivideByOneAreTheIdentity() {
      assertThat(scalar("RETURN (duration({seconds: 9007200, nanoseconds: 1}) * 1) = duration({seconds: 9007200, nanoseconds: 1}) AS r"))
          .isEqualTo(true);
      assertThat(scalar("RETURN toString(duration({seconds: 10000000, nanoseconds: 123456789}) * 1) AS r"))
          .isEqualTo("PT2777H46M40.123456789S");
      assertThat(scalar("RETURN toString(duration({seconds: 10000000, nanoseconds: 123456789}) / 1) AS r"))
          .isEqualTo("PT2777H46M40.123456789S");
      assertThat(scalar("RETURN toString(duration({days: 3}) / 3) AS r")).isEqualTo("P1D");
    }

    @Test
    void durationArithmeticRaisesOnOverflow() {
      expectError("RETURN duration({months: 9223372036854775807}) + duration({months: 1})", ArithmeticErrorException.class);
      expectError("RETURN duration({seconds: 5000000000000000000}) + duration({seconds: 5000000000000000000})", ArithmeticErrorException.class);
      expectError("RETURN duration({days: -9223372036854775808}) - duration({days: 1})", ArithmeticErrorException.class);
      expectError("RETURN duration({hours: 1}) * 10000000000000000", ArithmeticErrorException.class);
      // 3.6e15 seconds is representable, so the old nanosecond saturation is gone and the exact answer is returned
      assertThat(scalar("RETURN toString(duration({hours: 1}) * 1000000000000) AS r")).isEqualTo("PT1000000000000H");
      expectError("RETURN duration({months: 9223372036854775807}) * 2", ArithmeticErrorException.class);
      expectError("RETURN duration({seconds: 9223372037}).nanoseconds", ArithmeticErrorException.class);
      expectError("RETURN duration({seconds: 9223372036854776}).milliseconds", ArithmeticErrorException.class);
      assertThatThrownBy(() -> new CypherDuration(Long.MAX_VALUE, 0, 0, 0).multiply(2)).isInstanceOf(ArithmeticErrorException.class);
    }

    @Test
    void localTimePlusLargeDurationWrapsTheClockCorrectly() {
      assertThat(scalar("RETURN toString(localtime('12:00') + duration({seconds: 9223372036})) AS r")).isEqualTo("11:47:16");
      // one second more than the case above: the clock must move forward by one second, never backwards
      assertThat(scalar("RETURN toString(localtime('12:00') + duration({seconds: 9223372037})) AS r")).isEqualTo("11:47:17");
    }

    @Test
    void wholeUnitFieldsStayExactWithManyKeys() {
      assertThat(((Number) scalar("RETURN duration({days: 1, seconds: 9007199254740993}).seconds AS r")).longValue())
          .isEqualTo(9007199254740993L);
      assertThat(((Number) scalar("RETURN duration({months: 1, days: 9007199254740993}).days AS r")).longValue())
          .isEqualTo(9007199254740993L);
    }

    @Test
    void temporalWithUnsupportedOperatorIsATypeError() {
      for (final String op : new String[] { "*", "/", "%" })
        for (final String t : new String[] { "date('2024-01-01')", "datetime('2024-01-01T00:00Z')",
            "localdatetime('2024-01-01T00:00')", "localtime('12:00')", "time('12:00Z')" })
          expectError("RETURN " + t + " " + op + " duration({days: 1})");
    }

    @Test
    void durationOnTheLeftOfPlusIsAccepted() {
      assertThat(scalar("RETURN toString(duration({days: 1}) + date('2024-01-01')) AS r")).isEqualTo("2024-01-02");
      assertThat(scalar("RETURN toString(duration({days: 1}) + datetime('2024-01-01T00:00Z')) AS r")).isEqualTo("2024-01-02T00:00Z");
      assertThat(scalar("RETURN toString(duration({days: 1}) + localdatetime('2024-01-01T00:00')) AS r")).isEqualTo("2024-01-02T00:00");
      assertThat(scalar("RETURN toString(duration({hours: 1}) + localtime('12:00')) AS r")).isEqualTo("13:00");
      expectError("RETURN duration({days: 1}) - date('2024-01-01')");
    }

    @Test
    void divisionByAnIntegerKeepsExactnessAndTruncatesTheRemainder() {
      assertThat(scalar("RETURN toString(duration({days: 4, hours: 2}) / 2) AS r")).isEqualTo("P2DT1H");
      // the carry of a day is exact, so 1 day / 3 is 8h (issue #10198: it was one nanosecond short), and only an inexact
      // nanosecond quotient truncates
      assertThat(scalar("RETURN toString(duration({days: 1}) / 3) AS r")).isEqualTo("PT8H");
      assertThat(scalar("RETURN toString(duration({seconds: 1}) / 3) AS r")).isEqualTo("PT0.333333333S");
    }

    @Test
    void parsingRoundsTheDigitsBeyondTheNanosecondHalfUp() {
      assertThat(scalar("RETURN toString(duration('PT0.0000000015S')) AS r")).isEqualTo("PT0.000000002S");
      assertThat(scalar("RETURN toString(duration('PT0.0000000014S')) AS r")).isEqualTo("PT0.000000001S");
    }

    @Test
    void dividingTheMostNegativeDurationByMinusOneRaisesInsteadOfWrapping() {
      expectError("RETURN duration({days: -9223372036854775808}) / -1", ArithmeticErrorException.class);
      assertThat(scalar("RETURN toString(duration({days: 4}) / -1) AS r")).isEqualTo("P-4D");
    }

    @Test
    void parseBoundaryCases() {
      assertThat(scalar("RETURN toString(duration('PT9223372036854775807S')) AS r")).isEqualTo("PT2562047788015215H30M7S");
      expectError("RETURN duration('PT9223372036854775808S')");
      assertThat(scalar("RETURN toString(duration('P-1DT-2H-3M-4S')) AS r")).isEqualTo("P-1DT-2H-3M-4S");
      assertThat(scalar("RETURN toString(duration('PT1.100000000S')) AS r")).isEqualTo("PT1.1S");
      assertThat(scalar("RETURN toString(duration('PT0.000000001S')) AS r")).isEqualTo("PT0.000000001S");
    }

    @Test
    void durationTimesATemporalIsATypeError() {
      for (final String t : new String[] { "date('2024-01-01')", "datetime('2024-01-01T00:00Z')", "localtime('12:00')" })
        for (final String op : new String[] { "*", "/", "%", "-" })
          expectError("RETURN duration({days: 1}) " + op + " " + t);
    }

    @Test
    void clockArithmeticWithAHugeDurationWrapsBackwardsCorrectly() {
      // 9223372037 s mod 86400 = 85637 s = 23:47:17 ; 00:00:01 - 23:47:17 = 00:12:44 (the day before)
      assertThat(scalar("RETURN toString(localtime('00:00:01') - duration({seconds: 9223372037})) AS r")).isEqualTo("00:12:44");
    }

    @Test
    void fractionalFactorsUseTheDecimalPathAndNonFiniteFactorsAreRefused() {
      assertThat(scalar("RETURN toString(duration({days: 3}) * 0.5) AS r")).isEqualTo("P1DT12H");
      expectError("RETURN duration({days: 1}) * (0.0 / 0.0)");
      expectError("RETURN duration({days: 1}) * (1 / 0.0)");
      expectError("RETURN duration({days: 1}) / (1 / 0.0)");
    }

    @Test
    void plainTextStartingWithPStaysAString() {
      database.transaction(() -> {
        database.command("cypher", "CREATE (:W {w: 'Paris'})");
        database.command("cypher", "CREATE (:W {w: 'Peter'})");
        database.command("cypher", "CREATE (:W {w: 'P'})");
        database.command("cypher", "CREATE (:W {w: 'P1D'})");
        database.command("cypher", "CREATE (:W {w: 'Paris1'})");
        database.command("cypher", "CREATE (:W {w: 'P-'})");
      });
      for (final String text : new String[] { "Paris", "Peter", "P", "Paris1", "P-" })
        assertThat(scalar("MATCH (n:W) WHERE n.w = '" + text + "' RETURN count(n) AS r")).as(text + " stays a string").isEqualTo(1L);
      // 'P1D' was written as a String, so it stays one: a duration is stored as a duration (issue #8572)
      assertThat(scalar("MATCH (n:W) WHERE n.w = 'P1D' RETURN count(n) AS r")).as("a valid short duration text stays a string")
          .isEqualTo(1L);
    }

    @Test
    void parsingNegativeFractionsAndMoreThanNineDigits() {
      assertThat(scalar("RETURN toString(duration('PT-0.5S')) AS r")).isEqualTo("PT-0.5S");
      assertThat(scalar("RETURN toString(duration('PT-1.5S')) AS r")).isEqualTo("PT-1.5S");
      // digits past the nanosecond round half away from zero, in both directions
      assertThat(scalar("RETURN toString(duration('PT0.0000000015S')) AS r")).isEqualTo("PT0.000000002S");
      assertThat(scalar("RETURN toString(duration('PT-0.0000000015S')) AS r")).isEqualTo("PT-0.000000002S");
      assertThat(scalar("RETURN toString(duration('PT0.1234567891234S')) AS r")).isEqualTo("PT0.123456789S");
    }

    @Test
    void dividingADurationByZeroIsAnArithmeticError() {
      expectError("RETURN duration({days: 1}) / 0", ArithmeticErrorException.class);
      expectError("RETURN duration({days: 1}) / 0.0", ArithmeticErrorException.class);
    }

    @Test
    void aFractionThatRoundsUpToAFullSecondIsCarried() {
      assertThat(scalar("RETURN toString(duration('PT0.9999999999S')) AS r")).isEqualTo("PT1S");
      assertThat(scalar("RETURN toString(duration('PT1.9999999999S')) AS r")).isEqualTo("PT2S");
    }

    @Test
    void aFloatingPointFieldBeyondTheLongRangeOverflowsInsteadOfSaturating() {
      expectError("RETURN duration({days: 1e20, hours: 1})", ArithmeticErrorException.class);
      expectError("RETURN duration({days: 1e20})", ArithmeticErrorException.class);
      expectError("RETURN duration({seconds: -1e30})", ArithmeticErrorException.class);
      expectError("RETURN duration({days: 1, nanoseconds: 1e30})", ArithmeticErrorException.class);
      assertThat(scalar("RETURN toString(duration({days: 1e3, hours: 1})) AS r")).isEqualTo("P1000DT1H");
    }

    @Test
    void hugeComponentsAndFactorsRaiseTheArithmeticError() {
      expectError("RETURN duration('P99999999999999999999D')", ArithmeticErrorException.class);
      expectError("RETURN duration('PT99999999999999999999S')", ArithmeticErrorException.class);
      expectError("RETURN duration({days: 1}) * 1e300", ArithmeticErrorException.class);
      expectError("RETURN duration({days: 1}) * (0.0 / 0.0)", ArithmeticErrorException.class);
    }

    @Test
    void negativeSubSecondFieldsNormalise() {
      assertThat(scalar("RETURN toString(duration({milliseconds: -1500})) AS r")).isEqualTo("PT-1.5S");
      assertThat(scalar("RETURN toString(duration({microseconds: -1})) AS r")).isEqualTo("PT-0.000001S");
    }

    @Test
    void fractionalComponentsBeyondTheLongRangeOverflowToo() {
      expectError("RETURN duration('P99999999999999999999.5D')", ArithmeticErrorException.class);
      expectError("RETURN duration({days: 1e20, hours: 1.5})", ArithmeticErrorException.class);
      expectError("RETURN duration({months: 1e19, days: 0.5})", ArithmeticErrorException.class);
      assertThat(scalar("RETURN toString(duration('PT1.5H')) AS r")).isEqualTo("PT1H30M");
    }
  }

  /**
   * Regression test for issue #10055: {@link CypherDuration#parse} built its ISO pattern with every designator optional and
   * every number as any run of digits, dots and minus signs, so a text with no component at all ({@code P}, {@code PT}), a
   * dangling {@code T} ({@code P1YT}) or a malformed number parsed. The accepted and refused shapes below are the ones Neo4j
   * 2026.09 answers {@code RETURN duration(...)} with.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10055DurationParse {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue10055");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void teardown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void textWithoutAnyComponentIsRefused() {
      for (final String text : new String[] { "P", "PT", "P1YT", "P1DT", "-P", "+P", "-PT" })
        assertThatThrownBy(() -> CypherDuration.parse(text)).as(text).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void malformedNumbersAreRefusedAsMalformedNotAsOverflow() {
      // "P-Y" used to reach Long.parseLong("-") and be reported as an overflow, "P.Y" escaped as a NumberFormatException
      for (final String text : new String[] { "P-Y", "P.Y", "P.5Y", "PT1.S", "PT-.5S", "PT--1S", "PT1.2.3S", "P1-2D", "P1Y2Y" })
        assertThatThrownBy(() -> CypherDuration.parse(text)).as(text).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void onlyTheLastComponentMayCarryAFraction() {
      assertThatThrownBy(() -> CypherDuration.parse("P1.5Y2M")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> CypherDuration.parse("PT1.5H30M")).isInstanceOf(IllegalArgumentException.class);
      assertThat(CypherDuration.parse("P1.5Y").toString()).isEqualTo("P1Y6M");
      assertThat(CypherDuration.parse("P-0.5D").toString()).isEqualTo("PT-12H");
      assertThat(CypherDuration.parse("P1Y2M1.5W").toString()).isEqualTo("P1Y2M10DT12H");
    }

    @Test
    void aFractionDoesNotRoundTheWholeCountsThroughADouble() {
      // 2^53 + 1 months: a double holds 2^53, so reading the whole text through doubles lost a month
      final CypherDuration duration = CypherDuration.parse("P9007199254740993M0.5D");
      assertThat(duration.getMonths()).isEqualTo(9_007_199_254_740_993L);
      assertThat(duration.getDays()).isZero();
      assertThat(duration.getSeconds()).isEqualTo(12 * 3600L);

      // The whole part of the fractional component itself stays exact too
      assertThat(CypherDuration.parse("P9007199254740993.5D").getDays()).isEqualTo(9_007_199_254_740_993L);
      assertThat(CypherDuration.parse("P9007199254740993.5D").getSeconds()).isEqualTo(12 * 3600L);
      assertThat(CypherDuration.parse("-P9007199254740993.5D").getDays()).isEqualTo(-9_007_199_254_740_993L);
    }

    @Test
    void validTextsStillParse() {
      assertThat(CypherDuration.parse("PT0S").toString()).isEqualTo("PT0S");
      assertThat(CypherDuration.parse("P1Y").toString()).isEqualTo("P1Y");
      assertThat(CypherDuration.parse("P-1Y").toString()).isEqualTo("P-1Y");
      assertThat(CypherDuration.parse("P1Y2M3W4DT5H6M7.5S").toString()).isEqualTo("P1Y2M25DT5H6M7.5S");
      assertThat(CypherDuration.parse("P0001-00-00T00:00:00").toString()).isEqualTo("P1Y");
    }

    @Test
    void signsAndDecimalCommaAreReadAsNeo4jReadsThem() {
      assertThat(CypherDuration.parse("+P1Y").toString()).isEqualTo("P1Y");
      assertThat(CypherDuration.parse("PT+1S").toString()).isEqualTo("PT1S");
      assertThat(CypherDuration.parse("PT1,5S").toString()).isEqualTo("PT1.5S");
      // A leading minus negates the whole duration, components included
      assertThat(CypherDuration.parse("-P1Y2DT3S")).isEqualTo(CypherDuration.parse("P-1Y-2DT-3S"));
      assertThat(CypherDuration.parse("-PT0.5S").toString()).isEqualTo("PT-0.5S");
      assertThat(CypherDuration.parse("-P-1D").toString()).isEqualTo("P1D");
      // A signed seconds component with a fraction takes the exact decimal path
      assertThat(CypherDuration.parse("PT+1.5S").toString()).isEqualTo("PT1.5S");
      assertThat(CypherDuration.parse("PT-0.5S").toString()).isEqualTo("PT-0.5S");
      assertThat(CypherDuration.parse("-P1Y2DT3.5S")).isEqualTo(CypherDuration.parse("P-1Y-2DT-3.5S"));
      assertThat(CypherDuration.parse("-P1Y2DT3.5S").toString()).isEqualTo("P-1Y-2DT-3.5S");
      // The negated nanoseconds fold back into the seconds
      assertThat(CypherDuration.parse("-P1DT0.000000001S")).isEqualTo(CypherDuration.parse("P-1DT-0.000000001S"));
      assertThat(CypherDuration.parse("-P1DT0.000000001S").getNanosAdjustment()).isEqualTo(999_999_999);
      assertThat(CypherDuration.parse("-P1DT0.000000001S").getSeconds()).isEqualTo(-1L);
    }

    @Test
    void cypherDurationFunctionRefusesTheEmptyShapes() {
      for (final String text : new String[] { "P", "PT", "P1YT" })
        assertThatThrownBy(() -> {
          try (final ResultSet rs = database.query("opencypher", "RETURN duration('" + text + "') AS r")) {
            rs.next();
          }
        }).as(text).isInstanceOf(RuntimeException.class);

      try (final ResultSet rs = database.query("opencypher", "RETURN duration('+P1Y') AS r")) {
        assertThat(rs.next().<Object>getProperty("r").toString()).isEqualTo("P1Y");
      }
    }

    @Test
    void aLegacyTextThatIsNotADurationStaysText() {
      // A legacy stored text is read back as a duration only when it is one: "P" or "PT" is a string
      assertThat(TemporalUtil.parseLegacyText("PT")).isEqualTo("PT");
      assertThat(TemporalUtil.parseLegacyText("P1YT")).isEqualTo("P1YT");
      assertThat(TemporalUtil.parseLegacyText("P1Y")).isInstanceOf(CypherDuration.class);
    }
  }

  /**
   * Issue #10139: {@code duration({milliseconds: X})} past ~292 years was accepted, and the same duration written with any
   * second field ({@code duration({years: 0, milliseconds: X})}) was refused as an overflow, because the multi-field path summed
   * every sub-second field into one nanosecond count before carrying the whole seconds out of it. A field's reach must not depend
   * on how many other fields the map has.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10139DurationMapSubSecondReach extends TestHelper {
    private static final String[] SUB_SECOND_FIELDS = { "milliseconds", "microseconds", "nanoseconds" };

    private static final long[] MAGNITUDES = { 0L, 1L, -1L, 999L, 1_000L, 1_001L, 999_999L, 1_000_000L, 1_000_001L, 999_999_999L,
        1_000_000_000L, Integer.MAX_VALUE, 1L << 31, 1L << 53, (1L << 53) + 1, 31_536_000_000_000L,
        Long.MAX_VALUE / 1_000_000L - 1, Long.MAX_VALUE / 1_000_000L, Long.MAX_VALUE / 1_000_000L + 1,
        Long.MAX_VALUE / 1_000L - 1, Long.MAX_VALUE / 1_000L, Long.MAX_VALUE / 1_000L + 1, Long.MAX_VALUE - 1, Long.MAX_VALUE,
        Long.MIN_VALUE + 1, Long.MIN_VALUE, -(Long.MAX_VALUE / 1_000_000L + 1), -(Long.MAX_VALUE / 1_000L + 1), -31_536_000_000_000L };

    @Test
    void theIssueReproduction() {
      final String oneField = durationText("duration({milliseconds: 31536000000000})");
      assertThat(oneField).isEqualTo("PT8760000H");
      assertThat(durationText("duration({years: 0, milliseconds: 31536000000000})")).isEqualTo(oneField);
      assertThat(durationText("duration('PT31536000000S')")).isEqualTo(oneField);
    }

    @Test
    void theBoundaryOfTheNanosecondTotalDoesNotMatter() {
      assertThat(durationText("duration({milliseconds: 9223372036854, days: 0})")).isEqualTo("PT2562047H47M16.854S");
      assertThat(durationText("duration({milliseconds: 9223372036855, days: 0})")).isEqualTo("PT2562047H47M16.855S");
      assertThat(durationText("duration({microseconds: 9223372036854776, days: 0})")).isEqualTo("PT2562047H47M16.854776S");
      assertThat(durationText("duration({microseconds: 9223372036854777, days: 0})")).isEqualTo("PT2562047H47M16.854777S");
    }

    /** The guarantee: one field alone, or the same field with another one that adds nothing, is the same duration or the same refusal. */
    @Test
    void aSecondFieldThatAddsNothingNeverChangesTheAnswer() {
      for (final String field : SUB_SECOND_FIELDS)
        for (final long magnitude : MAGNITUDES) {
          final CypherDuration alone = fromMap(field, magnitude, null);
          for (final String other : new String[] { "days", "years", "seconds", "hours" }) {
            final CypherDuration withOther = fromMap(field, magnitude, other);
            assertThat(withOther).as("%s: %d alone vs with %s: 0", field, magnitude, other).isEqualTo(alone);
          }
        }
    }

    /** The general path with a fractional field must reach as far as the single-field one: the half day is the only difference. */
    @Test
    void aFractionalSecondFieldAddsItsOwnValueAndNothingElse() {
      final long halfDay = 12 * 3600;
      for (final String field : SUB_SECOND_FIELDS)
        for (final long magnitude : MAGNITUDES) {
          final CypherDuration alone = fromMap(field, magnitude, null);
          if (alone == null) {
            assertThat(fromMap(field, magnitude, "fractionalDays")).as("%s: %d", field, magnitude).isNull();
            continue;
          }
          final CypherDuration withHalfDay = fromMap(field, magnitude, "fractionalDays");
          assertThat(withHalfDay).as("%s: %d with days: 0.5", field, magnitude).isNotNull();
          assertThat(withHalfDay.getSeconds()).isEqualTo(alone.getSeconds() + halfDay);
          assertThat(withHalfDay.getNanosAdjustment()).isEqualTo(alone.getNanosAdjustment());
          assertThat(withHalfDay.getDays()).isEqualTo(alone.getDays());
        }
    }

    @Test
    void theSubSecondFieldsAreSummedExactly() {
      // 1 s + 1 ms + 1 us + 1 ns, in every combination of fields that includes a second one
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put("days", 0L);
      map.put("milliseconds", 1_001L);
      map.put("microseconds", 1_000_001L);
      map.put("nanoseconds", 1_000_000_001L);
      final CypherDuration sum = CypherDuration.fromMap(map);
      // 1.001 s + 1.000001 s + 1.000000001 s
      assertThat(sum.getSeconds()).isEqualTo(3L);
      assertThat(sum.getNanosAdjustment()).isEqualTo(1_000_000 + 1_000 + 1);

      // the largest of them together: each reaches the whole long range, and the carried seconds are added exactly
      map.put("milliseconds", Long.MAX_VALUE);
      map.put("microseconds", Long.MAX_VALUE);
      map.put("nanoseconds", Long.MAX_VALUE);
      final CypherDuration largest = CypherDuration.fromMap(map);
      final long seconds = Long.MAX_VALUE / 1_000L + Long.MAX_VALUE / 1_000_000L + Long.MAX_VALUE / 1_000_000_000L;
      final long nanos = Long.MAX_VALUE % 1_000L * 1_000_000L + Long.MAX_VALUE % 1_000_000L * 1_000L + Long.MAX_VALUE % 1_000_000_000L;
      assertThat(largest).isEqualTo(new CypherDuration(0, 0, seconds, nanos));
    }

    @Test
    void aFractionalSubSecondValueKeepsItsFractionAtAnyMagnitude() {
      // 4e15 is below 2^52, so the half is representable: the value is 4e12 s and 500000 ns. It was rounded through 4e21 ns,
      // which saturates at Long.MAX_VALUE without a word
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put("days", 0L);
      map.put("milliseconds", 4_000_000_000_000_000.5d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 4_000_000_000_000L, 500_000L));

      map.put("milliseconds", 1.5d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 0, 1_500_000L));
      map.put("milliseconds", -1.5d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 0, -1_500_000L));
      map.put("milliseconds", 1_000.25d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 1, 250_000L));

      // the sign is carried by both halves, the whole part and the rounded fraction
      map.put("milliseconds", -4_000_000_000_000_000.5d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, -4_000_000_000_000L, -500_000L));
      map.put("milliseconds", -1_000.25d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, -1, -250_000L));

      map.remove("milliseconds");
      map.put("microseconds", 2_000_000.5d);
      assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 2, 500L));
    }

    @Test
    void aDurationTheFieldsCannotHoldIsStillRefused() {
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put("seconds", Long.MAX_VALUE);
      map.put("milliseconds", 1_000L);
      assertThatThrownBy(() -> CypherDuration.fromMap(map)).isInstanceOf(ArithmeticErrorException.class);

      final Map<String, Object> hours = new LinkedHashMap<>();
      hours.put("hours", Long.MAX_VALUE);
      hours.put("milliseconds", 1L);
      assertThatThrownBy(() -> CypherDuration.fromMap(hours)).isInstanceOf(ArithmeticErrorException.class);

      // a double past the long range is no value either way
      final Map<String, Object> huge = new LinkedHashMap<>();
      huge.put("days", 0L);
      huge.put("milliseconds", 1e300d);
      assertThatThrownBy(() -> CypherDuration.fromMap(huge)).isInstanceOf(ArithmeticErrorException.class);
    }

    /**
     * The duration {@code field: magnitude} alone, or with one more field: {@code null}, a whole {@code 0} for that field, or
     * {@code fractionalDays} for a half day. Null when the map is refused.
     */
    private static CypherDuration fromMap(final String field, final long magnitude, final String other) {
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put(field, magnitude);
      if ("fractionalDays".equals(other))
        map.put("days", 0.5d);
      else if (other != null)
        map.put(other, 0L);
      try {
        return CypherDuration.fromMap(map);
      } catch (final ArithmeticErrorException e) {
        return null;
      }
    }

    private String durationText(final String expression) {
      try (final ResultSet resultSet = database.query("opencypher", "RETURN " + expression + " AS d")) {
        return String.valueOf(resultSet.next().<Object>getProperty("d"));
      }
    }
  }

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
  @Nested
  class Issue10198DurationArithmetic extends TestHelper {

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
}
