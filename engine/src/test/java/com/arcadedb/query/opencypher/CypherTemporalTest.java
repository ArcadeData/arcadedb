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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.procedures.db.DbMigrateLegacyTemporals;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Cypher temporal values: constructors, comparison, sub-second precision, time zones, legacy text and time buckets.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherTemporalTest {
  /**
   * Issue #8300: {@code CypherDateTime.compareTo} compared instants only, while {@code equals}/{@code hashCode} compare
   * the zone too. {@code =} (built on compareTo) called two datetimes at one instant in different zones equal, while
   * DISTINCT and GROUP BY (built on equals/hashCode) kept them apart. compareTo now breaks the instant tie by offset and
   * then zone name, the openCypher order, so all of them agree the two are distinct values.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8300DateTimeIdentity extends TestHelper {
    private static final String X = "datetime('2026-01-01T12:00:00+01:00')";
    private static final String Y = "datetime('2026-01-01T11:00:00Z')";

    @Test
    void compareToIsConsistentWithEquals() {
      final CypherDateTime x = CypherDateTime.parse("2026-01-01T12:00:00+01:00");
      final CypherDateTime y = CypherDateTime.parse("2026-01-01T11:00:00Z");
      assertThat(x.equals(y)).isFalse();
      assertThat(x.compareTo(y)).isNotZero();
      assertThat(Integer.signum(x.compareTo(y))).isEqualTo(-Integer.signum(y.compareTo(x)));
      // West to east: at one instant, Z (offset 0) sorts before +01:00
      assertThat(y.compareTo(x)).isNegative();
      assertThat(x.compareTo(CypherDateTime.parse("2026-01-01T12:00:00+01:00"))).isZero();

      // Time already broke its offset tie: pinned so the two stay consistent
      final CypherTime t1 = CypherTime.parse("12:00:00+01:00");
      final CypherTime t2 = CypherTime.parse("11:00:00Z");
      assertThat(t1.compareTo(t2) == 0).isEqualTo(t1.equals(t2));
    }

    @Test
    void equalityAgreesWithDistinctAndGrouping() {
      assertThat(single("RETURN " + X + " = " + Y + " AS v")).isEqualTo(false);
      assertThat(single("RETURN " + X + " <> " + Y + " AS v")).isEqualTo(true);
      assertThat(single("RETURN " + X + " IN [" + Y + "] AS v")).isEqualTo(false);
      assertThat(((Number) single("UNWIND [" + X + ", " + Y + "] AS d RETURN count(DISTINCT d) AS v")).longValue()).isEqualTo(2L);

      try (final ResultSet rs = database.query("opencypher", "UNWIND [" + X + ", " + Y + "] AS d RETURN d AS k, count(*) AS c")) {
        assertThat(rs.stream().map(r -> ((Number) r.getProperty("c")).longValue()).toList()).containsExactly(1L, 1L);
      }

      // Same instant AND same zone: one value everywhere
      assertThat(single("RETURN " + X + " = " + X + " AS v")).isEqualTo(true);
      assertThat(((Number) single("UNWIND [" + X + ", " + X + "] AS d RETURN count(DISTINCT d) AS v")).longValue()).isEqualTo(1L);
    }

    @Test
    void orderingAndRangesStillFollowTheInstant() {
      assertThat(single("RETURN datetime('2026-01-01T12:30:00+01:00') > " + Y + " AS v")).isEqualTo(true);
      assertThat(single("RETURN datetime('2026-01-01T11:30:00+01:00') < " + Y + " AS v")).isEqualTo(true);
      assertThat(single("RETURN " + X + " >= " + Y + " AND " + X + " <= datetime('2026-01-01T11:00:01Z') AS v")).isEqualTo(true);
    }

    /**
     * compareTo is a total order across DateTime and LocalDateTime too (a LocalDateTime is read as UTC and sorts first at
     * the same instant). In Cypher the two are two types (issue #10200, as in Neo4j): = is false and ORDER BY puts every
     * zoned datetime before every local one, each ordered by value.
     */
    @Test
    void localAndZonedDateTimesAtOneInstant() {
      final CypherDateTime x = CypherDateTime.parse("2026-01-01T12:00:00+01:00");
      final CypherDateTime y = CypherDateTime.parse("2026-01-01T11:00:00Z");
      final CypherLocalDateTime local = new CypherLocalDateTime(LocalDateTime.of(2026, 1, 1, 11, 0));
      assertThat(local.compareTo(x)).isNegative();
      assertThat(local.compareTo(y)).isNegative();
      assertThat(x.compareTo(local)).isPositive();
      assertThat(y.compareTo(local)).isPositive();

      assertThat(single("RETURN localdatetime('2026-01-01T11:00:00') = " + Y + " AS v")).isEqualTo(false);
      assertThat(single("RETURN localdatetime('2026-01-01T11:00:00') = " + X + " AS v")).isEqualTo(false);

      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [" + X + ", localdatetime('2026-01-01T11:00:00'), " + Y + ", localdatetime('2026-01-01T10:59:00')] AS d "
              + "RETURN toString(d) AS s ORDER BY d")) {
        assertThat(rs.stream().map(r -> (String) r.getProperty("s")).toList())
            .containsExactly("2026-01-01T11:00Z", "2026-01-01T12:00+01:00", "2026-01-01T10:59", "2026-01-01T11:00");
      }
    }

    /**
     * A java.util.Date or Instant parameter has no zone: it still equals a stored datetime at its instant, whatever that
     * datetime's zone is.
     */
    @Test
    void zonelessParametersMatchAnyZoneAtTheirInstant() {
      database.getSchema().createVertexType("Event8300");
      database.transaction(() -> database.command("opencypher", "CREATE (:Event8300 {ts: " + X + "})"));

      final Instant instant = ZonedDateTime.parse("2026-01-01T11:00:00Z").toInstant();
      for (final Object param : List.of(Date.from(instant), instant))
        try (final ResultSet rs = database.query("opencypher", "MATCH (e:Event8300) WHERE e.ts = $p RETURN count(e) AS c",
            Map.of("p", param))) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).as(param.getClass().getSimpleName()).isEqualTo(1L);
        }
    }

    /**
     * The comparison memoizes a coerced parameter by identity: a java.util.Date moved with setTime() between two
     * evaluations must not be answered with the value it had before.
     */
    @Test
    void mutatedDateIsNotAnsweredFromTheMemo() {
      final ComparisonExpression equals = ComparisonExpression.valueComparator(ComparisonExpression.Operator.EQUALS);
      final CypherDateTime stored = CypherDateTime.parse("2026-01-01T12:00:00+01:00");
      final Date param = Date.from(ZonedDateTime.parse("2026-01-01T11:00:00Z").toInstant());

      assertThat(equals.evaluateWithValues(param, stored)).isEqualTo(true);
      param.setTime(param.getTime() + 60_000L);
      assertThat(equals.evaluateWithValues(param, stored)).isEqualTo(false);
      param.setTime(param.getTime() - 60_000L);
      assertThat(equals.evaluateWithValues(stored, param)).isEqualTo(true);
    }

    private Object single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Result r = rs.next();
        return r.getProperty("v");
      }
    }
  }

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
  @Nested
  class Issue8387Issue8386Issue8385Issue8384Temporal extends TestHelper {

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
        final String stored = TemporalUtil.toStorageText(trailingZeros);
        assertThat(stored).isEqualTo("PT1.123S");
        assertThat(CypherDuration.parse(stored)).isEqualTo(trailingZeros);

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

    /** An overflow is reported as the engine's ArithmeticErrorException, never as a raw ArithmeticException. */
    @Test
    void overflowingDurationRaisesArithmeticError() {
      assertThatThrownBy(() -> new CypherDuration(0, 0, Long.MAX_VALUE, 2_000_000_000L)).isInstanceOf(ArithmeticErrorException.class);
      // The sub-second fields reach as far as the long each is (issue #10139), so it takes whole seconds that do not fit to overflow
      assertThatThrownBy(() -> CypherDuration.fromMap(Map.of("days", 1, "seconds", Long.MAX_VALUE, "milliseconds", Long.MAX_VALUE)))
          .isInstanceOf(ArithmeticErrorException.class);
      assertThatThrownBy(() -> CypherDuration.fromMap(Map.of("days", 1, "seconds", Long.MAX_VALUE, "nanoseconds", Long.MAX_VALUE)))
          .isInstanceOf(ArithmeticErrorException.class);
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

    /** A projected alias holds what its expression evaluated to: ORDER BY on it must not re-sniff the String. */
    @Test
    void orderByProjectedAliasKeepsDeclaredStrings() {
      database.getSchema().createVertexType("Part8384").createProperty("code", Type.STRING);
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:Part8384 {code: 'P10D'})");
        database.command("opencypher", "CREATE (:Part8384 {code: 'P2D'})");
      });

      // Lexical order: "P10D" < "P2D". As durations it would be the reverse.
      assertThat(names("MATCH (p:Part8384) RETURN p.code AS n ORDER BY n")).containsExactly("P10D", "P2D");
      assertThat(names("MATCH (p:Part8384) RETURN p.code AS n ORDER BY p.code")).containsExactly("P10D", "P2D");
      assertThat(names("UNWIND ['P2D', 'P10D'] AS n RETURN n ORDER BY n")).containsExactly("P10D", "P2D");
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

  /**
   * Regression test for issue #8571: the temporal constructors and the {@code *.truncate()} functions combined their
   * {@code millisecond}/{@code microsecond}/{@code nanosecond} fields in 32-bit int arithmetic, so a total at or beyond
   * 2.147483648s wrapped silently into a different, plausible fraction ({@code time({hour: 12, millisecond: 5000})}
   * answered {@code 12:00:00.705032704Z}) while the smaller {@code millisecond: 2000} was refused. Each component is now
   * range-checked the way Neo4j checks it, and the other components ({@code hour}, {@code year}, ...) no longer wrap
   * through {@code intValue()} either.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8571TemporalSubSecondOverflow extends TestHelper {

    @ParameterizedTest
    @ValueSource(strings = {
        "time({hour: 12, millisecond: 5000})",
        "time({hour: 12, millisecond: 4295})",
        "time({hour: 12, millisecond: 2000})",
        "time({hour: 12, microsecond: 5000000})",
        "localtime({hour: 12, microsecond: 5000000})",
        "localtime({hour: 12, nanosecond: 3000000000})",
        "localdatetime({year: 2024, month: 1, day: 1, hour: 0, millisecond: 5000})",
        "datetime({year: 2024, month: 1, day: 1, nanosecond: 3000000000})",
        "datetime.truncate('second', datetime({year: 2024, month: 1, day: 1}), {millisecond: 5000})",
        "localdatetime.truncate('second', localdatetime({year: 2024, month: 1, day: 1}), {microsecond: 5000000})",
        "time.truncate('second', time({hour: 12}), {millisecond: 5000})",
        "localtime.truncate('second', localtime({hour: 12}), {nanosecond: 3000000000})",
        // Neo4j's per-component ranges: a finer component cannot carry into a coarser one that is also given
        "time({hour: 12, millisecond: 1, microsecond: 1000})",
        "time({hour: 12, microsecond: 1, nanosecond: 1000})",
        "time({hour: 12, millisecond: 1, nanosecond: 1000000})",
        "time({hour: 12, millisecond: -1})",
        // The other components wrapped through intValue() the same way: 4294967308 = 2^32 + 12
        "time({hour: 4294967308})",
        "localtime({hour: 4294967308})",
        "date({year: 4294969320, month: 1, day: 1})",
        "localdatetime({year: 2024, month: 1, day: 1, hour: 4294967308})" })
    void outOfRangeSubSecondFieldIsRefusedInsteadOfWrapping(final String expression) {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
          rs.next().getProperty("v");
        }
      }).as(expression).hasStackTraceContaining("Invalid value for");
    }

    @Test
    void inRangeSubSecondFieldsStillCombine() {
      assertThat(cypher("time({hour: 12, millisecond: 999})")).isEqualTo("12:00:00.999Z");
      assertThat(cypher("time({hour: 12, millisecond: 645, microsecond: 876, nanosecond: 123})")).isEqualTo("12:00:00.645876123Z");
      assertThat(cypher("localtime({hour: 12, microsecond: 999999})")).isEqualTo("12:00:00.999999");
      assertThat(cypher("localtime({hour: 12, nanosecond: 999999999})")).isEqualTo("12:00:00.999999999");
      assertThat(cypher("localtime({hour: 12, millisecond: 1, nanosecond: 999999})")).isEqualTo("12:00:00.001999999");
      assertThat(cypher("datetime({year: 2024, month: 1, day: 1, microsecond: 5})")).isEqualTo("2024-01-01T00:00:00.000005Z");
      // truncate keeps the portion the adjustment map does not name
      assertThat(cypher("localtime.truncate('millisecond', localtime('12:00:00.123456789'), {nanosecond: 2})"))
          .isEqualTo("12:00:00.123000002");
    }

    @Test
    void fractionalArbitraryPrecisionValueIsNotRoundedIntoAnInteger() {
      // doubleValue() would round this to exactly 1.0 and accept it as the integer 1
      assertThatThrownBy(() -> TemporalUtil.computeNanos(Map.of("millisecond", new BigDecimal("1.0000000000000001")), 0))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> TemporalUtil.toIntField("hour", new BigInteger("4294967308"))).isInstanceOf(IllegalArgumentException.class);
      assertThat(TemporalUtil.computeNanos(Map.of("millisecond", new BigDecimal("5.000")), 0)).isEqualTo(5_000_000);
      assertThat(TemporalUtil.toIntField("hour", new BigInteger("12"))).isEqualTo(12);
    }

    private String cypher(final String expression) {
      try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
        return rs.next().getProperty("v");
      }
    }
  }

  /**
   * Regression test for issue #8921. A temporal value that went through a {@code WITH}, an {@code UNWIND} or a
   * {@code datetime()} / {@code date()} call is a {@code CypherTemporalValue} wrapper, which the two paths a MATCH uses
   * to find candidates (the index seek and the inline property filter) did not understand: the seek threw an NPE on an
   * indexed property and the inline filter answered no rows at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8921TemporalWrapperLookup {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue8921");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createVertexType("I");
      database.command("sql", "CREATE PROPERTY N.d DATETIME");
      database.command("sql", "CREATE PROPERTY N.dt DATE");
      database.command("sql", "CREATE PROPERTY I.d DATETIME");
      database.command("sql", "CREATE PROPERTY I.dt DATE");
      database.command("sql", "CREATE INDEX ON I (d) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (dt) NOTUNIQUE");
      database.transaction(() -> {
        for (final String type : new String[] { "N", "I" })
          for (int i = 0; i < 3; i++)
            database.command("cypher", "CREATE (:" + type + " {d: $d, dt: $dt})",
                Map.of("d", LocalDateTime.of(2021, 6, 15, 12, 30, 0), "dt", LocalDate.of(2021, 6, 15)));
      });
    }

    @AfterEach
    void teardown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("cypher", query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private void both(final String template, final long expected) {
      assertThat(count(template.replace("%s", "N"))).as("unindexed: " + template).isEqualTo(expected);
      assertThat(count(template.replace("%s", "I"))).as("indexed: " + template).isEqualTo(expected);
    }

    @Test
    void withBoundDatetimeInlineForm() {
      both("MATCH (a:N) WITH a.d AS m MATCH (p:%s {d: m}) RETURN count(p) AS c", 9);
    }

    @Test
    void withBoundDatetimeWhereForm() {
      both("MATCH (a:N) WITH a.d AS m MATCH (p:%s) WHERE p.d = m RETURN count(p) AS c", 9);
    }

    @Test
    void withBoundDate() {
      both("MATCH (a:N) WITH a.dt AS m MATCH (p:%s {dt: m}) RETURN count(p) AS c", 9);
      both("MATCH (a:N) WITH a.dt AS m MATCH (p:%s) WHERE p.dt = m RETURN count(p) AS c", 9);
    }

    @Test
    void unwindOfLiterals() {
      both("UNWIND [datetime('2021-06-15T12:30:00')] AS m MATCH (p:%s) WHERE p.d = m RETURN count(p) AS c", 3);
      both("UNWIND [date('2021-06-15')] AS m MATCH (p:%s) WHERE p.dt = m RETURN count(p) AS c", 3);
      both("UNWIND [date('2021-06-15')] AS m MATCH (p:%s {dt: m}) RETURN count(p) AS c", 3);
    }

    @Test
    void literalConstructors() {
      both("MATCH (p:%s {d: datetime('2021-06-15T12:30:00')}) RETURN count(p) AS c", 3);
      both("MATCH (p:%s) WHERE p.d = datetime('2021-06-15T12:30:00') RETURN count(p) AS c", 3);
    }

    @Test
    void optionalMatchAfterWith() {
      both("MATCH (a:N) WITH a.d AS m OPTIONAL MATCH (p:%s {d: m}) RETURN count(p) AS c", 9);
    }

    /** Pins the semantic: a naive stored datetime and a zoned one compare by instant, the same in the inline and WHERE forms. */
    @Test
    void zonedLiteralAgreesBetweenInlineAndWhereForms() {
      final long where = count("MATCH (p:N) WHERE p.d = datetime('2021-06-15T12:30:00Z') RETURN count(p) AS c");
      final long inline = count("MATCH (p:N {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c");
      // all three stored rows hold 2021-06-15T12:30 read as UTC, the instant of the zoned literal
      assertThat(where).isEqualTo(3);
      assertThat(inline).isEqualTo(where);
      final long indexed = count("MATCH (p:I {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c");
      assertThat(indexed).isEqualTo(where);
    }
  }

  /**
   * Issue #9325: a stored DATETIME has no zone on disk, so the engine materializes one. With
   * {@code arcadedb.dateTimeImplementation=java.time.ZonedDateTime} that zone was {@code UTC} while a {@code Z} literal's is
   * {@code Z}, and a stored value compared strictly below the literal for its own instant. With {@code java.util.Date} or
   * {@code java.time.Instant} it matched the {@code Z} spelling but not the same instant in another zone, because the
   * zone-less adoption rule never fired for a value already wrapped by the property read.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9325StoredDateTimeZone extends TestHelper {
    private static final String Z     = "datetime('2026-01-01T11:00:00Z')";
    private static final String PLUS1 = "datetime('2026-01-01T12:00:00+01:00')";

    @Test
    void zonedDateTimeImplementationStoredValueEqualsItsOwnInstant() {
      run(ZonedDateTime.class, () -> {
        assertThat(count("e.ts = " + Z)).isEqualTo(1L);
        assertThat(count("e.ts >= " + Z)).isEqualTo(1L);
        assertThat(count("e.ts <= " + Z)).isEqualTo(1L);
        assertThat(count("e.ts < datetime('2026-01-01T11:00:01Z')")).isEqualTo(1L);
        assertThat(count("e.ts = datetime('2026-01-01T11:00:00[UTC]')")).isEqualTo(1L);
        assertThat(count("e.ts = " + PLUS1)).isEqualTo(1L);
        assertThat(count("e.ts = datetime('2026-01-01T11:00:01Z')")).isEqualTo(0L);
      });
    }

    @Test
    void dateImplementationStoredValueEqualsItsInstantInAnyZone() {
      run(Date.class, () -> {
        assertThat(count("e.ts = " + Z)).isEqualTo(1L);
        assertThat(count("e.ts = " + PLUS1)).isEqualTo(1L);
        assertThat(count("e.ts >= " + PLUS1)).isEqualTo(1L);
        assertThat(count("e.ts < " + PLUS1)).isEqualTo(0L);
      });
    }

    @Test
    void instantImplementationStoredValueEqualsItsInstantInAnyZone() {
      run(Instant.class, () -> {
        assertThat(count("e.ts = " + Z)).isEqualTo(1L);
        assertThat(count("e.ts = " + PLUS1)).isEqualTo(1L);
      });
    }

    @Test
    void flagDoesNotLeakIntoTwoStoredValues() {
      run(ZonedDateTime.class, () -> {
        database.transaction(() -> database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save());
        try (final ResultSet rs = database.query("opencypher", "MATCH (a:E), (b:E) WHERE a.ts = b.ts RETURN count(*) AS c")) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(4L);
        }
      });
    }

    @Test
    void storedValuesStillGroupAndDistinctAsOne() {
      run(ZonedDateTime.class, () -> {
        database.transaction(() -> database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save());
        try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) RETURN count(DISTINCT e.ts) AS c")) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        }
      });
    }

    @Test
    void derivedValueIsAnOrdinaryZonedDateTime() {
      run(ZonedDateTime.class, () -> {
        // e.ts + PT0S is a new value: it keeps the materialized UTC zone and is NOT zone-less, so it matches only its own zone spelling
        assertThat(count("e.ts + duration('PT0S') = " + Z)).isEqualTo(1L);
        assertThat(count("e.ts + duration('PT0S') = " + PLUS1)).isEqualTo(0L);
      });
    }

    private void run(final Class<?> implementation, final Runnable body) {
      final var serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateTimeImplementation();
      try {
        serializer.setDateTimeImplementation(implementation);
        database.transaction(() -> {
          database.getSchema().createVertexType("E").createProperty("ts", Type.DATETIME);
          database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save();
        });
        body.run();
      } finally {
        serializer.setDateTimeImplementation(previous);
      }
    }

    private long count(final String predicate) {
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) WHERE " + predicate + " RETURN count(e) AS c")) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  /**
   * Regression test for issue #9636: a release before the native temporal types (#8572) stored an openCypher
   * {@code duration()}, {@code time()}, {@code localtime()} or zoned {@code datetime()} on an undeclared property as its ISO
   * text and restored it on read by its shape. After the upgrade those values read back as plain Strings.
   * <p>
   * A legacy database is reproduced the way a release before #8572 left it: the temporal is a String in the record (what
   * {@code toCoreJavaType} answered then), and the schema file carries no {@code temporalStorage} setting.
   */
  @Nested
  class Issue9636LegacyTemporalText extends TestHelper {

    @Test
    void newDatabaseIsNativeAndStaysSoAfterReopen() {
      database.getSchema().createVertexType("T");
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
      database.transaction(() -> database.newVertex("T").set("d", "P1D").save());

      reopenDatabase();

      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
      // #8572 unchanged: on a native database a String that looks temporal is a String
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN n.d AS d")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
      }
    }

    @Test
    void legacyDatabaseReadsUndeclaredTemporalTextAsTemporal() throws IOException {
      writeLegacyRecords();
      makeLegacy();

      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isTrue();

      // Component access, the repro of the issue
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days, n.t.hour AS th, n.lt.minute AS ltm, n.dt.timezone AS tz")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("days")).intValue()).isEqualTo(1);
        assertThat(((Number) r.getProperty("th")).intValue()).isEqualTo(9);
        assertThat(((Number) r.getProperty("ltm")).intValue()).isEqualTo(15);
        assertThat(r.<Object>getProperty("tz")).isEqualTo("Europe/Rome");
      }

      // Equality against a temporal
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) WHERE n.d = duration({days: 1}) RETURN n.name AS name")) {
        assertThat(rs.next().<Object>getProperty("name")).isEqualTo("a");
        assertThat(rs.hasNext()).isFalse();
      }

      // ORDER BY sorts durations as durations: as text "P10D" sorts before "P2D"
      final List<Object> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN n.name AS name ORDER BY n.d")) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      assertThat(names).containsExactly("a", "b", "c");
    }

    @Test
    void legacyDatabaseNeverRestoresADeclaredStringOrPlainText() throws IOException {
      database.getSchema().createVertexType("S").createProperty("code", Type.STRING);
      database.transaction(() -> database.newVertex("S").set("code", "P100D").set("free", "Paris").save());
      makeLegacy();

      try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN n.code AS code, n.free AS free")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("code")).isEqualTo("P100D");
        assertThat(r.<Object>getProperty("free")).isEqualTo("Paris");
      }

      // MERGE finds the declared STRING by the text the temporal would be stored as (issue #9695), not by a legacy restore:
      // the text is still text, so no duplicate is created and the property reads back as the String it holds
      database.command("opencypher", "MERGE (n:S {code: duration('P100D')})");
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN count(n) AS c, n.code AS code")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("c")).intValue()).isEqualTo(1);
        assertThat(r.<Object>getProperty("code")).isEqualTo("P100D");
      }
    }

    @Test
    void aMigrationThatFailsLeavesTheDatabaseLegacyAndARerunCompletesIt() throws IOException {
      writeLegacyRecords();
      // A record the migration cannot save: it predates a mandatory property
      database.getSchema().getType("T").createProperty("required", Type.STRING).setMandatory(true);
      makeLegacy();

      try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
        rs.next();
        fail("the migration must fail on a record it cannot save");
      } catch (final RuntimeException expected) {
        // the record validation refused the save
      }

      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isTrue();
      try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'a'")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
      }
      // Still read as a temporal by openCypher, as before the attempt
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days")) {
        assertThat(((Number) rs.next().getProperty("days")).intValue()).isEqualTo(1);
      }

      database.getSchema().getType("T").getProperty("required").setMandatory(false);
      try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
        assertThat(((Number) rs.next().getProperty("records")).longValue()).isEqualTo(3L);
      }
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
      try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'a'")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 1, 0, 0));
      }
    }

    @Test
    void mergeOnLegacyDatabaseMatchesTheTemporalText() throws IOException {
      writeLegacyRecords();
      makeLegacy();

      database.command("opencypher", "MERGE (n:T {d: duration({days: 1})})");

      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN count(n) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(3);
      }
    }

    @Test
    void migrationRewritesTheTextNativelyAndEndsTheLegacyReading() throws IOException {
      writeLegacyRecords();
      database.getSchema().createVertexType("S").createProperty("code", Type.STRING);
      database.transaction(() -> database.newVertex("S").set("code", "P100D").set("free", "Paris").save());
      makeLegacy();

      try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals() YIELD records, properties RETURN records, properties")) {
        final Result r = rs.next();
        // a: d, t, lt, dt. b and c: d
        assertThat(((Number) r.getProperty("records")).longValue()).isEqualTo(3L);
        assertThat(((Number) r.getProperty("properties")).longValue()).isEqualTo(6L);
      }
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();

      // SQL sees the native values, not the text
      try (final ResultSet rs = database.query("sql", "SELECT d, t, lt, dt FROM T WHERE name = 'a'")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 1, 0, 0));
        assertThat(r.<Object>getProperty("t")).isInstanceOf(OffsetTime.class);
        assertThat(r.<Object>getProperty("lt")).isInstanceOf(LocalTime.class);
        assertThat(r.<Object>getProperty("dt")).isInstanceOf(ZonedDateTime.class);
      }
      // A declared STRING and plain text are left alone
      try (final ResultSet rs = database.query("sql", "SELECT code, free FROM S")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("code")).isEqualTo("P100D");
        assertThat(r.<Object>getProperty("free")).isEqualTo("Paris");
      }

      reopenDatabase();
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days, n.dt.timezone AS tz")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("days")).intValue()).isEqualTo(1);
        assertThat(r.<Object>getProperty("tz")).isEqualTo("Europe/Rome");
      }
      // A String written from now on is a String
      database.command("opencypher", "CREATE (:T {name: 'z', d: 'P3D'})");
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'z'}) RETURN n.d AS d")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P3D");
      }
    }

    @Test
    void migrationConvertsEdgesLeavesListsAndRunsSeveralBatches() throws IOException {
      final int count = DbMigrateLegacyTemporals.BATCH_SIZE * 2 + 7;
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        final MutableVertex first = database.newVertex("V").set("d", "P1D").set("list", List.of("P2D", "P3D")).save();
        for (int i = 1; i < count; i++)
          database.newVertex("V").set("d", "P" + i + "D").save();
        first.newEdge("E", first).set("since", "2024-01-01T10:00+01:00[Europe/Rome]").save();
      });
      makeLegacy();

      try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("records")).longValue()).isEqualTo(count + 1L);
        assertThat(((Number) r.getProperty("properties")).longValue()).isEqualTo(count + 1L);
      }

      final int[] durations = new int[1];
      database.iterateType("V", false).forEachRemaining(record -> {
        assertThat(record.asDocument().get("d")).isInstanceOf(CypherDuration.class);
        ++durations[0];
      });
      assertThat(durations[0]).isEqualTo(count);
      try (final ResultSet rs = database.query("sql", "SELECT since FROM E")) {
        assertThat(rs.next().<Object>getProperty("since")).isInstanceOf(ZonedDateTime.class);
      }
      // The elements of a list were text on a property read before the native types too: left as they are
      try (final ResultSet rs = database.query("sql", "SELECT list FROM V WHERE list IS NOT NULL")) {
        assertThat(rs.next().<List<Object>>getProperty("list")).containsExactly("P2D", "P3D");
      }
    }

    @Test
    void migrationSurvivesARollbackOfTheCallersTransaction() throws IOException {
      writeLegacyRecords();
      makeLegacy();

      // The HTTP handlers wrap every command in a transaction: the batches commit on their own, nested inside it
      database.begin();
      try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
        assertThat(((Number) rs.next().getProperty("records")).longValue()).isEqualTo(3L);
      }
      database.rollback();

      try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'b'")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 2, 0, 0));
      }
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
      reopenDatabase();
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
      try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'b'")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 2, 0, 0));
      }
    }

    @Test
    void migrationOnANativeDatabaseConvertsNothing() {
      database.getSchema().createVertexType("T");
      database.transaction(() -> database.newVertex("T").set("d", "P1D").save());

      try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("records")).longValue()).isZero();
        assertThat(((Number) r.getProperty("properties")).longValue()).isZero();
      }
      try (final ResultSet rs = database.query("sql", "SELECT d FROM T")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
      }
    }

    /**
     * The records a release before #8572 wrote for {@code CREATE (:T {d: duration(...), ...})}: the temporals as text.
     */
    private void writeLegacyRecords() {
      database.getSchema().createVertexType("T");
      database.transaction(() -> {
        database.newVertex("T").set("name", "a").set("d", "P1D").set("t", "09:00-17:00").set("lt", "10:15:30")
            .set("dt", "2024-01-01T10:00+01:00[Europe/Rome]").save();
        database.newVertex("T").set("name", "b").set("d", "P2D").save();
        database.newVertex("T").set("name", "c").set("d", "P10D").save();
      });
    }

    /**
     * Closes the database and removes the temporal storage setting from its schema file, which is what a schema written by
     * a release before #8572 looks like, then opens it again.
     */
    private void makeLegacy() throws IOException {
      database.close();
      final File schemaFile = new File(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      assertThat(root.getJSONObject("settings").has("temporalStorage")).isTrue();
      root.getJSONObject("settings").remove("temporalStorage");
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);
      database = factory.open();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);
    }
  }

  /**
   * Issue #9647: a stored DATE or DATETIME must answer the same Cypher temporal predicates whatever Java class
   * {@code arcadedb.dateImplementation} / {@code arcadedb.dateTimeImplementation} deserializes it into. A DATE read back as a
   * {@code java.util.Date}, a {@code LocalDateTime} or a {@code Calendar} matched no {@code date()} literal, and a DATETIME read
   * back as a {@code Calendar} matched nothing at all.
   */
  @Nested
  class Issue9647TemporalImplementationComparison extends TestHelper {
    private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

    @ParameterizedTest
    @ValueSource(classes = { LocalDate.class, Date.class, LocalDateTime.class, Calendar.class })
    void storedDateMatchesDateLiteralUnderEveryDateImplementation(final Class<?> implementation) {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateImplementation();
      try {
        serializer.setDateImplementation(implementation);
        database.transaction(() -> {
          database.getSchema().createVertexType("D").createProperty("d", Type.DATE);
          final VertexType indexed = database.getSchema().createVertexType("DI");
          indexed.createProperty("d", Type.DATE);
          indexed.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "d");
          for (final String type : new String[] { "D", "DI" }) {
            database.newVertex(type).set("n", 1).set("d", LocalDate.of(2022, 6, 1)).save();
            database.newVertex(type).set("n", 2).set("d", DAY).save();
            database.newVertex(type).set("n", 3).set("d", LocalDate.of(2025, 3, 1)).save();
          }
        });

        // the setting took effect: otherwise the run silently degrades to the default LocalDate path
        assertThat(database.iterateType("D", false).next().asDocument().get("d")).isInstanceOf(implementation);

        for (final String type : new String[] { "D", "DI" }) {
          assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d = date('2024-02-29') RETURN count(n) AS c")).isEqualTo(1L);
          assertThat(cypherCount("MATCH (n:" + type + " {d: date('2024-02-29')}) RETURN count(n) AS c")).isEqualTo(1L);
          assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d > date('2023-01-01') RETURN count(n) AS c")).isEqualTo(2L);
          assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d <= date('2024-02-29') RETURN count(n) AS c")).isEqualTo(2L);
          assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d <> date('2024-02-29') RETURN count(n) AS c")).isEqualTo(2L);
          assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d.year = 2024 RETURN count(n) AS c")).isEqualTo(1L);
          // chained access on a non-variable base reads the property the way the variable-bound access does
          assertThat(cypherCount("MATCH (n:" + type + ") WITH {node: n} AS m WHERE m.node.d = date('2024-02-29') RETURN count(*) AS c"))
              .isEqualTo(1L);

          try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + " {n: 2}) RETURN toString(n.d) AS s")) {
            assertThat((String) rs.next().getProperty("s")).isEqualTo("2024-02-29");
          }

          // SQL is the control: a scan and an index agree on the same predicate
          try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE d = date('2024-02-29','yyyy-MM-dd')")) {
            assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
          }
        }

        // MERGE is the MATCH of its pattern: it finds the stored row instead of creating a duplicate
        database.transaction(() -> database.command("opencypher", "MERGE (n:D {d: date('2024-02-29')})").close());
        assertThat(cypherCount("MATCH (n:D) RETURN count(n) AS c")).isEqualTo(3L);
      } finally {
        serializer.setDateImplementation(previous);
      }
    }

    @ParameterizedTest
    @ValueSource(classes = { LocalDateTime.class, Date.class, ZonedDateTime.class, Instant.class, Calendar.class })
    void storedDateTimeMatchesDateTimeLiteralUnderEveryDateTimeImplementation(final Class<?> implementation) {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateTimeImplementation();
      try {
        serializer.setDateTimeImplementation(implementation);
        database.transaction(() -> {
          database.getSchema().createVertexType("E").createProperty("ts", Type.DATETIME);
          final VertexType indexed = database.getSchema().createVertexType("EI");
          indexed.createProperty("ts", Type.DATETIME);
          indexed.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "ts");
          for (final String type : new String[] { "E", "EI" }) {
            database.newVertex(type).set("n", 1).set("ts", ZonedDateTime.parse("2022-06-01T08:00:00Z")).save();
            database.newVertex(type).set("n", 2).set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save();
            database.newVertex(type).set("n", 3).set("ts", ZonedDateTime.parse("2027-03-01T09:30:00Z")).save();
          }
        });

        assertThat(database.iterateType("E", false).next().asDocument().get("ts")).isInstanceOf(implementation);

        for (final String type : new String[] { "E", "EI" }) {
          assertThat(where(type, "e.ts = datetime('2026-01-01T11:00:00Z')")).isEqualTo(1L);
          assertThat(where(type, "e.ts >= datetime('2026-01-01T11:00:00Z')")).isEqualTo(2L);
          assertThat(where(type, "e.ts < datetime('2026-01-01T11:00:01Z')")).isEqualTo(2L);
          assertThat(where(type, "e.ts = datetime('2026-01-01T12:00:00+01:00')")).isEqualTo(1L);
          assertThat(where(type, "e.ts = datetime('2026-01-01T11:00:00[UTC]')")).isEqualTo(1L);
          assertThat(where(type, "e.ts > datetime('2023-01-01T00:00:00Z')")).isEqualTo(2L);
          assertThat(where(type, "e.ts.year = 2026")).isEqualTo(1L);
          assertThat(cypherCount("MATCH (e:" + type + " {ts: datetime('2026-01-01T11:00:00Z')}) RETURN count(e) AS c")).isEqualTo(1L);

          try (final ResultSet rs = database.query("opencypher", "MATCH (e:" + type + " {n: 2}) RETURN toString(e.ts) AS s")) {
            final String rendered = rs.next().getProperty("s");
            assertThat(rendered).doesNotContain("Calendar").startsWith("2026-01-01T11:00");
          }
        }
      } finally {
        serializer.setDateTimeImplementation(previous);
      }
    }

    @ParameterizedTest
    @ValueSource(classes = { Date.class, LocalDateTime.class, Calendar.class })
    void storedDateOnAnEdgeMatchesInlineDateLiteral(final Class<?> implementation) {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateImplementation();
      try {
        serializer.setDateImplementation(implementation);
        database.transaction(() -> {
          database.getSchema().createVertexType("V");
          database.getSchema().createEdgeType("R").createProperty("d", Type.DATE);
          database.command("opencypher", "CREATE (:V {n: 1})-[:R {d: date('2024-02-29')}]->(:V {n: 2})").close();
        });

        assertThat(cypherCount("MATCH (a:V)-[r:R {d: date('2024-02-29')}]->(b:V) RETURN count(r) AS c")).isEqualTo(1L);
        assertThat(cypherCount("MATCH (a:V {n: 1})-[r:R {d: date('2024-02-29')}]->(b) RETURN count(r) AS c")).isEqualTo(1L);
        assertThat(cypherCount("MATCH (a:V)-[r:R]->(b:V) WHERE r.d = date('2024-02-29') RETURN count(r) AS c")).isEqualTo(1L);
        // MERGE of the relationship finds the stored one instead of creating a second
        database.transaction(() -> database.command("opencypher",
            "MATCH (a:V {n: 1}), (b:V {n: 2}) MERGE (a)-[:R {d: date('2024-02-29')}]->(b)").close());
        assertThat(cypherCount("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
      } finally {
        serializer.setDateImplementation(previous);
      }
    }

    @ParameterizedTest
    @ValueSource(classes = { Date.class, Calendar.class })
    void preEpochDateKeepsItsDay(final Class<?> implementation) {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateImplementation();
      try {
        serializer.setDateImplementation(implementation);
        database.transaction(() -> {
          database.getSchema().createVertexType("D").createProperty("d", Type.DATE);
          database.newVertex("D").set("d", LocalDate.of(1960, 5, 1)).save();
        });
        // a negative epoch is floored to its own day, not rounded to the next one
        assertThat(cypherCount("MATCH (n:D) WHERE n.d = date('1960-05-01') RETURN count(n) AS c")).isEqualTo(1L);
        assertThat(cypherCount("MATCH (n:D {d: date('1960-05-01')}) RETURN count(n) AS c")).isEqualTo(1L);
      } finally {
        serializer.setDateImplementation(previous);
      }
    }

    @ParameterizedTest
    @ValueSource(classes = { ZonedDateTime.class, Calendar.class })
    void calendarParameterComparesByItsInstant(final Class<?> implementation) {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateTimeImplementation();
      try {
        serializer.setDateTimeImplementation(implementation);
        database.transaction(() -> {
          database.getSchema().createVertexType("E").createProperty("ts", Type.DATETIME);
          database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save();
        });

        // a java.util.Calendar parameter (an embedded or Java client) carries an instant, as a java.util.Date one does
        // in a zone other than UTC, where the local day differs from the UTC one: it still compares by its instant
        final Calendar parameter = Calendar.getInstance(TimeZone.getTimeZone("Pacific/Kiritimati"));
        parameter.setTimeInMillis(Instant.parse("2026-01-01T11:00:00Z").toEpochMilli());
        try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) WHERE e.ts = $p RETURN count(e) AS c", "p", parameter)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        }
        try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) WHERE datetime('2026-01-01T12:00:00+01:00') = $p RETURN count(e) AS c",
            "p", parameter)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        }
        try (final ResultSet rs = database.query("opencypher", "RETURN $p.year AS c", "p", parameter)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2026L);
        }
        // SQL reads a Calendar as its instant on either side of '='
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E WHERE ts = ?", parameter)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        }
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E WHERE ? = ts", parameter)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        }
      } finally {
        serializer.setDateTimeImplementation(previous);
      }
    }

    private long where(final String type, final String predicate) {
      return cypherCount("MATCH (e:" + type + ") WHERE " + predicate + " RETURN count(e) AS c");
    }

    private long cypherCount(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  /**
   * Issue #9806: on a database written before the native temporal types, a property READ restores a stored ISO text to its
   * temporal, but the comparison of an inline pattern property did not: a relationship pattern {@code -[r:R {d: duration(...)}]->}
   * matched nothing while {@code WHERE r.d = duration(...)} found the row. MERGE had the restore as a local copy, so a node
   * pattern and MERGE were the two shapes that passed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9806LegacyTemporalPattern extends TestHelper {

    @Test
    void everyPatternShapeFindsALegacyTemporalTextByItsTemporal() throws IOException {
      database.getSchema().createVertexType("T");
      database.getSchema().createEdgeType("R");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("T").set("name", "a").save();
        final MutableVertex b = database.newVertex("T").set("name", "b").save();
        // what a release before the native temporal types stored for duration({days: 1})
        a.newEdge("R", b).set("d", "P1D").save();
      });
      makeLegacy();
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isTrue();

      // the control: a WHERE reads the property, which restores it
      assertThat(names("MATCH (a:T)-[r:R]->(b:T) WHERE r.d = duration({days: 1}) RETURN b.name AS name")).containsExactly("b");

      assertThat(names("MATCH (a:T)-[r:R {d: duration({days: 1})}]->(b:T) RETURN b.name AS name")).containsExactly("b");
      assertThat(names("MATCH (a:T)-[:R*1..2 {d: duration({days: 1})}]->(b:T) RETURN b.name AS name")).containsExactly("b");
      assertThat(names("MATCH (a:T {name: 'a'}) WHERE (a)-[:R {d: duration({days: 1})}]->() RETURN a.name AS name")).containsExactly(
          "a");
      assertThat(names("MATCH (a:T {name: 'a'}) RETURN [(a)-[:R {d: duration({days: 1})}]->(b) | b.name][0] AS name"))
          .containsExactly("b");

      // a temporal among the identifying properties of merge.relationship finds the existing edge instead of adding one beside it
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:T {name: 'a'}), (b:T {name: 'b'}) CALL merge.relationship(a, 'R', {d: duration({days: 1})}, {}, b) YIELD rel RETURN rel")
          .close());
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:T {name: 'a'}), (b:T {name: 'b'}) MERGE (a)-[:R {d: duration({days: 1})}]->(b)").close());
      assertThat(names("MATCH ()-[r:R]->() RETURN r.d AS name")).hasSize(1);

      // the restore never applies to text the pattern does not compare against a temporal
      assertThat(names("MATCH (a:T)-[r:R {d: 'P1D'}]->(b:T) RETURN b.name AS name")).containsExactly("b");
    }

    @Test
    void aNativeDatabaseKeepsTheTextAsText() {
      database.getSchema().createVertexType("T");
      database.getSchema().createEdgeType("R");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("T").set("name", "a").save();
        final MutableVertex b = database.newVertex("T").set("name", "b").save();
        a.newEdge("R", b).set("d", "P1D").save();
        a.newEdge("R", b).set("d", new CypherDuration(0, 1, 0, 0)).save();
      });

      // only the native duration is the duration: on a database created with the native types a text that looks like one is text
      assertThat(names("MATCH (a:T)-[r:R {d: duration({days: 1})}]->(b:T) RETURN b.name AS name")).hasSize(1);
    }

    private List<Object> names(final String query) {
      final List<Object> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      return names;
    }

    /**
     * Closes the database and removes the temporal storage setting from its schema file, which is what a schema written by a
     * release before the native temporal types looks like, then opens it again.
     */
    private void makeLegacy() throws IOException {
      database.close();
      final File schemaFile = new File(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      root.getJSONObject("settings").remove("temporalStorage");
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);
      database = factory.open();
    }
  }

  /**
   * Issue #10199: Cypher temporal constructors ignored or rejected parts of their input. Every expected value below was
   * checked against Neo4j 2026.09 (the text is ArcadeDB's, which drops zero seconds).
   * <ul>
   *   <li>a millisecond, microsecond or nanosecond given with a base {@code time:} replaced only its own digits of the
   *   fraction, and a {@code datetime:} base ignored them;</li>
   *   <li>{@code datetime({epochSeconds|epochMillis, timezone})} ignored the timezone;</li>
   *   <li>{@code date({date, dayOfWeek})} ignored the dayOfWeek, and {@code dayOfQuarter} the same way;</li>
   *   <li>{@code datetime()} refused years outside 0000-9999, because the compact offset normalizer read a signed year as
   *   an offset;</li>
   *   <li>{@code time()} refused a named zone.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10199TemporalConstructor extends TestHelper {

    @Test
    void aSubSecondFieldReplacesTheWholeFractionOfTheBase() {
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), nanosecond: 5})")).isEqualTo("12:30:45.000000005");
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), millisecond: 5})")).isEqualTo("12:30:45.005");
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), microsecond: 5})")).isEqualTo("12:30:45.000005");
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), millisecond: 5, nanosecond: 7})")).isEqualTo("12:30:45.005000007");
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), millisecond: 5, microsecond: 6})")).isEqualTo("12:30:45.005006");
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), nanosecond: 123456})")).isEqualTo("12:30:45.000123456");
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), microsecond: 123456})")).isEqualTo("12:30:45.123456");
      assertThat(text("time({time: time('12:30:45.123456789+01:00'), nanosecond: 5})")).isEqualTo("12:30:45.000000005+01:00");
      assertThat(text("localdatetime({datetime: localdatetime('2024-01-01T12:30:45.123456789'), nanosecond: 5})"))
          .isEqualTo("2024-01-01T12:30:45.000000005");
      assertThat(text("localdatetime({date: date('2024-01-01'), time: localtime('12:30:45.123456789'), microsecond: 5})"))
          .isEqualTo("2024-01-01T12:30:45.000005");
      assertThat(text("datetime({datetime: datetime('2024-01-01T12:30:45.123456789Z'), millisecond: 5})")).isEqualTo("2024-01-01T12:30:45.005Z");
      // Without a sub-second field the fraction of the base is kept
      assertThat(text("localtime({time: localtime('12:30:45.123456789'), second: 1})")).isEqualTo("12:30:01.123456789");
      assertThat(text("datetime({datetime: datetime('2024-01-01T12:30:45.123456789Z'), second: 1})")).isEqualTo("2024-01-01T12:30:01.123456789Z");
    }

    /** The date fields are resolved together: applied one at a time, February 29 clamped to the 28th before the month moved. */
    @Test
    void dateFieldOverridesAreResolvedTogether() {
      assertThat(text("datetime({datetime: datetime('2024-02-29T12:00Z'), year: 2023, month: 3})")).isEqualTo("2023-03-29T12:00Z");
      assertThat(text("localdatetime({datetime: localdatetime('2024-02-29T12:00'), year: 2023, month: 3})")).isEqualTo("2023-03-29T12:00");
      assertThat(text("datetime({epochSeconds: 1709208000, year: 2023, month: 3})")).isEqualTo("2023-03-29T12:00Z");
      assertThatThrownBy(() -> text("datetime({datetime: datetime('2024-02-29T12:00Z'), year: 2023})")).isNotNull();
    }

    @Test
    void truncateStillKeepsTheUnitsItDidNotTruncate() {
      assertThat(text("localtime.truncate('second', localtime('12:30:45.123456789'), {nanosecond: 5})")).isEqualTo("12:30:45.000000005");
      assertThat(text("localtime.truncate('millisecond', localtime('12:30:45.123456789'), {nanosecond: 5})")).isEqualTo("12:30:45.123000005");
      assertThat(text("localtime.truncate('millisecond', localtime('12:30:45.123456789'), {microsecond: 5})")).isEqualTo("12:30:45.123005");
      assertThat(text("localtime.truncate('microsecond', localtime('12:30:45.123456789'), {nanosecond: 5})")).isEqualTo("12:30:45.123456005");
      assertThat(text("localtime.truncate('minute', localtime('12:30:45.123456789'), {millisecond: 5})")).isEqualTo("12:30:00.005");
    }

    @Test
    void anEpochDatetimeIsReadInItsTimezone() {
      assertThat(text("datetime({epochSeconds: 1700000000, timezone: 'Europe/Paris'})")).isEqualTo("2023-11-14T23:13:20+01:00[Europe/Paris]");
      assertThat(text("datetime({epochSeconds: 0, timezone: '+05:30'})")).isEqualTo("1970-01-01T05:30+05:30");
      assertThat(text("datetime({epochMillis: 1700000000123, timezone: 'Asia/Kolkata'})"))
          .isEqualTo("2023-11-15T03:43:20.123+05:30[Asia/Kolkata]");
      assertThat(text("datetime({epochSeconds: 1700000000, nanosecond: 5, timezone: 'Europe/Paris'})"))
          .isEqualTo("2023-11-14T23:13:20.000000005+01:00[Europe/Paris]");
      assertThat(text("datetime({epochSeconds: 1700000000, millisecond: 5, timezone: 'Europe/Paris'})"))
          .isEqualTo("2023-11-14T23:13:20.005+01:00[Europe/Paris]");
      assertThat(text("datetime({epochSeconds: 1700000000, timezone: 'Europe/Paris', hour: 3})")).isEqualTo("2023-11-14T03:13:20+01:00[Europe/Paris]");
      assertThat(text("datetime({epochMillis: 1700000000123, nanosecond: 5})")).isEqualTo("2023-11-14T22:13:20.000000005Z");
      // Without a timezone, UTC as before
      assertThat(text("datetime({epochSeconds: 1700000000})")).isEqualTo("2023-11-14T22:13:20Z");
      assertThat(text("datetime({epochSeconds: 1000000000, nanosecond: 123456789})")).isEqualTo("2001-09-09T01:46:40.123456789Z");
      assertThatThrownBy(() -> text("datetime({epochSeconds: 1700000000, epochMillis: 5})"))
          .hasMessageContaining("epochMillis cannot be selected together with epochSeconds");
    }

    @Test
    void dayOfWeekAndDayOfQuarterMoveTheBaseDate() {
      assertThat(text("date({date: date('2024-02-29'), dayOfWeek: 1})")).isEqualTo("2024-02-26");
      assertThat(text("date({date: date('2024-02-29'), dayOfWeek: 7})")).isEqualTo("2024-03-03");
      // 2024-12-30 is in the first ISO week of 2025
      assertThat(text("date({date: date('2024-12-30'), dayOfWeek: 3})")).isEqualTo("2025-01-01");
      assertThat(text("date({date: date('2024-02-29'), dayOfQuarter: 3})")).isEqualTo("2024-01-03");
      assertThat(text("localdatetime({date: date('2024-02-29'), dayOfWeek: 1, hour: 3})")).isEqualTo("2024-02-26T03:00");
      assertThat(text("datetime({date: date('2024-02-29'), dayOfWeek: 1, timezone: 'Z'})")).isEqualTo("2024-02-26T00:00Z");
      // The controls that already agreed
      assertThat(text("date({date: date('2024-02-29'), week: 1})")).isEqualTo("2024-01-04");
      assertThat(text("date({date: date('2024-02-29'), week: 1, dayOfWeek: 1})")).isEqualTo("2024-01-01");
      assertThat(text("date({date: date('2024-02-29'), quarter: 3})")).isEqualTo("2024-08-29");
      assertThat(text("date({date: date('2024-02-29'), ordinalDay: 3})")).isEqualTo("2024-01-03");
      assertThat(text("date({year: 2024, week: 3, dayOfWeek: 3})")).isEqualTo("2024-01-17");
    }

    @Test
    void datetimeReadsAnyYearTheOtherTypesRead() {
      assertThat(text("datetime('-0001-12-31T23:59:59Z')")).isEqualTo("-0001-12-31T23:59:59Z");
      assertThat(text("datetime('+10000-01-01T00:00Z')")).isEqualTo("+10000-01-01T00:00Z");
      assertThat(text("datetime('-0001-12-31T23:59:59+01:00')")).isEqualTo("-0001-12-31T23:59:59+01:00");
      assertThat(text("datetime('+10000-01-01T00:00[Europe/Paris]')")).isEqualTo("+10000-01-01T00:00+01:00[Europe/Paris]");
      assertThat(text("datetime('-999999999-01-01T00:00Z')")).isEqualTo("-999999999-01-01T00:00Z");
      assertThat(text("datetime('+999999999-12-31T23:59:59.999999999Z')")).isEqualTo("+999999999-12-31T23:59:59.999999999Z");
      assertThat(text("datetime('-0001-12-31')")).isEqualTo("-0001-12-31T00:00Z");
      assertThat(text("localdatetime('+10000-01-01T00:00')")).isEqualTo("+10000-01-01T00:00");
      assertThat(text("date('+10000-01-01')")).isEqualTo("+10000-01-01");
      assertThat(text("datetime({year: -1, month: 12, day: 31, timezone: 'Z'})")).isEqualTo("-0001-12-31T00:00Z");
      // A compact offset still reads as one
      assertThat(text("datetime('2015-07-21T21:40:32.142+0100')")).isEqualTo("2015-07-21T21:40:32.142+01:00");
      assertThat(text("time('21:40:32-0130')")).isEqualTo("21:40:32-01:30");
    }

    @Test
    void aTimeTakesTheOffsetANamedZoneHasNow() {
      assertThat(text("time({hour: 12, timezone: 'UTC'})")).isEqualTo("12:00Z");
      assertThat(text("time({hour: 12, timezone: 'Asia/Tokyo'})")).isEqualTo("12:00+09:00");
      assertThat(text("time({hour: 12, minute: 30, timezone: 'Europe/Paris'})")).isEqualTo("12:30" + offsetNow("Europe/Paris"));
      assertThat(text("time({hour: 12, minute: 30, timezone: 'America/New_York'})")).isEqualTo("12:30" + offsetNow("America/New_York"));
      assertThat(text("time.truncate('minute', time('12:30+05:45'), {timezone: 'Asia/Tokyo'})")).isEqualTo("12:30+09:00");
      assertThat(text("time({time: time('12:30+05:45'), timezone: 'Asia/Tokyo'})")).isEqualTo("15:45+09:00");
      assertThat(text("time({time: localtime('12:30'), timezone: 'Asia/Tokyo'})")).isEqualTo("12:30+09:00");
      assertThat(text("time({time: datetime('2024-07-01T12:00[Europe/Paris]'), timezone: 'Asia/Tokyo'})")).isEqualTo("19:00+09:00");
      assertThat(text("time.truncate('second', datetime('2024-07-01T12:30:45.123[Europe/Paris]'), {timezone: 'Asia/Tokyo'})"))
          .isEqualTo("12:30:45+09:00");
      assertThatThrownBy(() -> text("time({hour: 12, timezone: 'Not/AZone'})")).isNotNull();
    }

    private static String offsetNow(final String zone) {
      final String offset = ZoneId.of(zone).getRules().getOffset(Instant.now()).getId();
      return "Z".equals(offset) ? "Z" : offset;
    }

    private String text(final String expression) {
      try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
        return rs.next().getProperty("v");
      }
    }
  }

  /**
   * Issue #10200: Cypher comparison and ordering of temporal values differed from Neo4j. Every expected value below was
   * checked against Neo4j 2026.09 (the text is ArcadeDB's, which drops zero seconds).
   * <ul>
   *   <li>two durations compared with {@code <}/{@code >}: they are not comparable, only equal or not;</li>
   *   <li>{@code ORDER BY}, {@code min()} and {@code max()} ordered durations component by component instead of by length;</li>
   *   <li>a {@code localdatetime()} equalled the {@code datetime()} at the same wall clock time;</li>
   *   <li>{@code ORDER BY} over two temporal types threw instead of ordering by type.</li>
   * </ul>
   * A DATETIME read from storage, which ArcadeDB keeps as an instant and reads back as a local datetime, still compares with
   * a zoned datetime by instant, so filters on stored DATETIME properties keep working.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10200TemporalComparison extends TestHelper {

    @Test
    void durationsAreEqualOrNotButNotComparable() {
      assertThat(value("duration('P1D') < duration('P2D')")).isNull();
      assertThat(value("duration('P1D') > duration('PT24H')")).isNull();
      assertThat(value("duration('P1D') <= duration('P2D')")).isNull();
      assertThat(value("duration('P1D') < duration('P1D')")).isNull();
      assertThat(value("duration('P1D') <= duration('PT24H')")).isNull();
      assertThat(value("duration('PT1H') < duration('PT2H')")).isNull();
      assertThat(value("[duration('P1D')] < [duration('P2D')]")).isNull();
      // Equality settles what it can
      assertThat(value("duration('P1D') <= duration('P1D')")).isEqualTo(true);
      assertThat(value("duration('P1D') >= duration('P1D')")).isEqualTo(true);
      assertThat(value("duration('P1D') = duration('P1D')")).isEqualTo(true);
      assertThat(value("duration('P1D') = duration('PT24H')")).isEqualTo(false);
      assertThat(value("duration('P1D') <> duration('PT24H')")).isEqualTo(true);
      assertThat(value("duration('P1D') IN [duration('PT24H')]")).isEqualTo(false);
      assertThat(value("duration('P1D') IN [duration('PT24H'), duration('P1D')]")).isEqualTo(true);
      assertThat(value("[duration('P1D'), 1] = [duration('P1D'), 1]")).isEqualTo(true);
      assertThat(value("duration('P1D') = date('2024-01-01')")).isEqualTo(false);
      assertThat(value("duration('P1D') < date('2024-01-01')")).isNull();
    }

    @Test
    void durationsOrderByAverageLengthThenMonthsThenDays() {
      assertThat(sorted("duration('P1D'), duration('PT25H')")).containsExactly("P1D", "PT25H");
      assertThat(sorted("duration('P1M'), duration('P31D')")).containsExactly("P1M", "P31D");
      assertThat(sorted("duration('P1D'), duration('PT23H')")).containsExactly("PT23H", "P1D");
      // The same average length: fewer months first, then fewer days
      assertThat(sorted("duration('P1D'), duration('PT24H')")).containsExactly("PT24H", "P1D");
      assertThat(sorted("duration('P30D'), duration('P1M'), duration('PT720H')")).containsExactly("PT720H", "P30D", "P1M");
      assertThat(sorted("duration('P1M'), duration('P30DT10H29M6S'), duration('PT730H29M6S')"))
          .containsExactly("PT730H29M6S", "P30DT10H29M6S", "P1M");
      assertThat(sorted("duration('P1DT-1S'), duration('PT86399S')")).containsExactly("PT23H59M59S", "P1DT-1S");
      assertThat(sorted("duration('PT0.5S'), duration('PT0S'), duration('-PT1S')")).containsExactly("PT-1S", "PT0S", "PT0.5S");

      final List<String> descending = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [duration('P1D'), duration('PT24H')] AS x RETURN toString(x) AS v ORDER BY x DESC")) {
        while (rs.hasNext())
          descending.add(rs.next().getProperty("v"));
      }
      assertThat(descending).containsExactly("P1D", "PT24H");
    }

    @Test
    void minAndMaxFollowTheSameOrder() {
      assertThat(text("max(x)", "duration('P1D'), duration('PT25H')")).isEqualTo("PT25H");
      assertThat(text("min(x)", "duration('P1M'), duration('P1D')")).isEqualTo("P1D");
      assertThat(text("max(x)", "duration('P1D'), duration('PT24H')")).isEqualTo("P1D");
      assertThat(text("min(x)", "duration('PT24H'), duration('P1D')")).isEqualTo("PT24H");
      try (final ResultSet rs = database.query("opencypher",
          "RETURN toString(coll.max([duration('P1M'), duration('P31D')])) AS hi, toString(coll.min([duration('P1M'), duration('P31D')])) AS lo")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("hi")).isEqualTo("P31D");
        assertThat(row.<String>getProperty("lo")).isEqualTo("P1M");
      }
    }

    @Test
    void aLocalAndAZonedDatetimeAreTwoTypes() {
      assertThat(value("localdatetime('2024-01-01T12:00') = datetime('2024-01-01T12:00Z')")).isEqualTo(false);
      assertThat(value("localdatetime('2024-01-01T12:00') <> datetime('2024-01-01T12:00Z')")).isEqualTo(true);
      assertThat(value("localdatetime('2024-01-01T12:00') < datetime('2024-01-01T13:00Z')")).isNull();
      assertThat(value("localdatetime('2024-01-01T12:00') <= datetime('2024-01-01T12:00Z')")).isNull();
      assertThat(value("datetime('2024-01-01T13:00Z') > localdatetime('2024-01-01T12:00')")).isNull();
      assertThat(value("localdatetime('2024-01-01T12:00') IN [datetime('2024-01-01T12:00Z')]")).isEqualTo(false);
      // The controls that already agreed
      assertThat(value("date('2024-01-01') < datetime('2024-01-01T13:00Z')")).isNull();
      assertThat(value("date('2024-01-01') = datetime('2024-01-01T00:00Z')")).isEqualTo(false);
      assertThat(value("localtime('12:00') < time('13:00Z')")).isNull();
      assertThat(value("time('12:00Z') = localtime('12:00')")).isEqualTo(false);
      assertThat(value("localdatetime('2024-01-01T12:00') >= localdatetime('2024-01-01T12:00')")).isEqualTo(true);
      assertThat(value("time('12:00+01:00') = time('11:00Z')")).isEqualTo(false);
      assertThat(value("time('12:00+01:00') < time('11:30Z')")).isEqualTo(true);
    }

    /** The design point of the fix: a DATETIME column is an instant, so it still meets a zoned datetime as one. */
    @Test
    void aStoredDatetimeStillComparesByInstant() {
      database.getSchema().createVertexType("Event").createProperty("at", Type.DATETIME);
      database.transaction(() -> {
        database.newVertex("Event").set("name", "a", "at", LocalDateTime.of(2024, 1, 1, 12, 0)).save();
        database.newVertex("Event").set("name", "b", "at", LocalDateTime.of(2024, 1, 1, 14, 0)).save();
      });

      assertThat(names("MATCH (e:Event) WHERE e.at > datetime('2024-01-01T13:00Z') RETURN e.name AS name")).containsExactly("b");
      assertThat(names("MATCH (e:Event) WHERE e.at = datetime('2024-01-01T13:00+01:00') RETURN e.name AS name")).containsExactly("a");
      assertThat(names("MATCH (e:Event) WITH e, e.at AS at WHERE at < datetime('2024-01-01T13:00Z') RETURN e.name AS name"))
          .containsExactly("a");
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:Event) WHERE e.at >= $from RETURN e.name AS name",
          Map.of("from", ZonedDateTime.of(2024, 1, 1, 13, 0, 0, 0, ZoneOffset.UTC)))) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("b");
        assertThat(rs.hasNext()).isFalse();
      }
      // A java.time.LocalDateTime parameter is the same raw value a DATETIME read gives: it compares with a zoned datetime by
      // instant too (a deliberate deviation from Neo4j, which answers null), where a localdatetime() of the query does not
      try (final ResultSet rs = database.query("opencypher", "RETURN $p = datetime('2024-01-01T12:00Z') AS v, $p < datetime('2024-01-01T13:00Z') AS w",
          Map.of("p", LocalDateTime.of(2024, 1, 1, 12, 0)))) {
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("v")).isTrue();
        assertThat(row.<Boolean>getProperty("w")).isTrue();
      }
      // Against a local datetime of the query, a stored DATETIME is a local datetime like any other
      assertThat(names("MATCH (e:Event) WHERE e.at = localdatetime('2024-01-01T14:00') RETURN e.name AS name")).containsExactly("b");
      // A value derived from it is an ordinary local datetime
      assertThat(names("MATCH (e:Event) WHERE e.at + duration('PT1H') = datetime('2024-01-01T13:00Z') RETURN e.name AS name")).isEmpty();
    }

    @Test
    void valuesOfDifferentTemporalTypesOrderByType() {
      assertThat(sorted("date('2024-01-01'), datetime('2024-01-01T00:00Z')")).containsExactly("2024-01-01T00:00Z", "2024-01-01");
      assertThat(sorted("time('12:00Z'), localtime('12:00')")).containsExactly("12:00Z", "12:00");
      assertThat(sorted("localdatetime('2024-01-01T12:00'), datetime('2024-01-01T12:00Z')"))
          .containsExactly("2024-01-01T12:00Z", "2024-01-01T12:00");
      assertThat(sorted("duration('P1M'), localtime('12:00'), time('12:00Z'), date('2024-01-01'), localdatetime('2024-01-01T00:00'), "
          + "datetime('2024-01-01T00:00Z')"))
          .containsExactly("2024-01-01T00:00Z", "2024-01-01T00:00", "2024-01-01", "12:00Z", "12:00", "P1M");
      // Between PATH and STRING, as Neo4j orders them
      final List<String> mixed = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [duration('P1M'), 'a', 1, true, date('2024-01-01'), [1]] AS x RETURN x AS v ORDER BY x")) {
        while (rs.hasNext())
          mixed.add(String.valueOf(rs.next().<Object>getProperty("v")));
      }
      assertThat(mixed).containsExactly("[1]", "2024-01-01", "P1M", "a", "true", "1");
      // One temporal type still orders by value
      assertThat(sorted("datetime('2024-01-01T12:00+01:00'), datetime('2024-01-01T11:00Z')"))
          .containsExactly("2024-01-01T11:00Z", "2024-01-01T12:00+01:00");
    }

    /** A native java.time / java.util.Date parameter orders by value among Cypher temporals of its type. */
    @Test
    void aNativeTemporalParameterOrdersByValue() {
      assertThat(sortedNames(Map.of("p", Duration.ofHours(25)), "[$p, 'param'], [duration('P1D'), 'literal']")).containsExactly("literal", "param");
      assertThat(sortedNames(Map.of("p", Duration.ofHours(23)), "[$p, 'param'], [duration('P1D'), 'literal']")).containsExactly("param", "literal");
      assertThat(sortedNames(Map.of("p", Period.ofMonths(1)), "[$p, 'param'], [duration('P31D'), 'literal']")).containsExactly("param", "literal");
      final Date noon = Date.from(ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC).toInstant());
      assertThat(sortedNames(Map.of("p", noon), "[$p, 'param'], [datetime('2024-01-01T11:00Z'), 'literal']")).containsExactly("literal", "param");
      assertThat(sortedNames(Map.of("p", noon), "[$p, 'param'], [datetime('2024-01-01T13:00Z'), 'literal']")).containsExactly("param", "literal");
    }

    private List<String> sortedNames(final Map<String, Object> parameters, final String pairs) {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [" + pairs + "] AS pair WITH pair[0] AS x, pair[1] AS name RETURN name ORDER BY x", parameters)) {
        while (rs.hasNext())
          out.add(rs.next().getProperty("name"));
      }
      return out;
    }

    @Test
    void minAndMaxOverMixedTemporalTypesDoNotThrow() {
      assertThat(text("max(x)", "date('2024-01-01'), datetime('2024-01-01T00:00Z'), duration('P1D')")).isEqualTo("P1D");
      assertThat(text("min(x)", "date('2024-01-01'), datetime('2024-01-01T00:00Z'), duration('P1D')")).isEqualTo("2024-01-01T00:00Z");
    }

    /** The whole Cypher order of types, so a type added to the rank table cannot silently reorder the others. */
    @Test
    void theOrderOfTypesIsPinned() {
      final Object[] ascending = { Map.of("a", 1), List.of(1), ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
          LocalDateTime.of(2024, 1, 1, 0, 0), LocalDate.of(2024, 1, 1), OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC),
          LocalTime.of(12, 0), CypherDuration.parse("P1D"), "a", true, 1, Double.NaN, null };
      for (int i = 1; i < ascending.length; i++)
        assertThat(CypherFunctionHelper.cypherTypeRank(ascending[i - 1])).as("%s before %s", ascending[i - 1], ascending[i])
            .isLessThan(CypherFunctionHelper.cypherTypeRank(ascending[i]));
    }

    private Object value(final String expression) {
      try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v")) {
        return rs.next().getProperty("v");
      }
    }

    private String text(final String aggregate, final String values) {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [" + values + "] AS x RETURN toString(" + aggregate + ") AS v")) {
        return rs.next().getProperty("v");
      }
    }

    private List<String> sorted(final String values) {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "UNWIND [" + values + "] AS x RETURN toString(x) AS v ORDER BY x")) {
        while (rs.hasNext())
          out.add(rs.next().getProperty("v"));
      }
      return out;
    }

    private List<String> names(final String query) {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          out.add(rs.next().getProperty("name"));
      }
      return out;
    }
  }

  /**
   * Issue #10441: {@code ts.timeBucket()} called from Cypher refused Cypher's own temporal values, because the one
   * date/time converter ({@code DateUtils.toEpochMillis}) did not know the {@code com.arcadedb.query.opencypher.temporal}
   * wrappers. The same call through the explicit {@code sql.} prefix failed on the backticks kept in the function name.
   */
  @Nested
  class Issue10441TimeBucketCypherTemporal extends TestHelper {

    private static final LocalDateTime BUCKET = LocalDateTime.of(2026, 10, 19, 8, 0);

    @Test
    void zonedDatetimeIsBucketed() {
      assertThat(single("RETURN ts.timeBucket('1m', datetime('2026-10-19T08:00:33Z')) AS b")).isEqualTo(BUCKET);
    }

    @Test
    void zonedDatetimeWithOffsetIsBucketedAtItsInstant() {
      // 10:00:33+02:00 is 08:00:33Z: the bucket is the instant's, read at UTC as every other zoned value is
      assertThat(single("RETURN ts.timeBucket('1m', datetime('2026-10-19T10:00:33+02:00')) AS b")).isEqualTo(BUCKET);
    }

    @Test
    void localDatetimeIsBucketed() {
      assertThat(single("RETURN ts.timeBucket('1m', localdatetime('2026-10-19T08:00:33')) AS b")).isEqualTo(BUCKET);
    }

    @Test
    void dateIsBucketed() {
      assertThat(single("RETURN ts.timeBucket('1d', date('2026-10-19')) AS b")).isEqualTo(LocalDateTime.of(2026, 10, 19, 0, 0));
    }

    @Test
    void epochMillisStillWorks() {
      assertThat(single("RETURN ts.timeBucket('1m', 1792396833000) AS b")).isEqualTo(BUCKET);
    }

    @Test
    void datetimeOriginIsAccepted() {
      assertThat(single(
          "RETURN ts.timeBucket('1h', datetime('2026-10-19T08:10:00Z'), {origin: datetime('2026-10-19T00:30:00Z')}) AS b"))
          .isEqualTo(LocalDateTime.of(2026, 10, 19, 7, 30));
    }

    @Test
    void datetimeEdgePropertyCreatedInCypherIsBucketed() {
      database.transaction(() -> database.command("opencypher",
          "CREATE (:Account {id: 1})-[:TRANSFERRED {ts: datetime('2026-10-19T08:00:33Z')}]->(:Account {id: 2}), "
              + "(:Account {id: 3})-[:TRANSFERRED {ts: datetime('2026-10-19T08:00:59Z')}]->(:Account {id: 4}), "
              + "(:Account {id: 5})-[:TRANSFERRED {ts: datetime('2026-10-19T08:01:02Z')}]->(:Account {id: 6})").close());

      try (final ResultSet rs = database.query("opencypher",
          "MATCH ()-[t:TRANSFERRED]->() RETURN ts.timeBucket('1m', t.ts) AS b, count(*) AS c ORDER BY b")) {
        final Result first = rs.next();
        assertThat(asLocalDateTime(first.getProperty("b"))).isEqualTo(BUCKET);
        assertThat(((Number) first.getProperty("c")).longValue()).isEqualTo(2L);
        final Result second = rs.next();
        assertThat(asLocalDateTime(second.getProperty("b"))).isEqualTo(LocalDateTime.of(2026, 10, 19, 8, 1));
        assertThat(((Number) second.getProperty("c")).longValue()).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }

      // the same properties read from SQL bucket the same way
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('1m', ts) AS b, count(*) AS c FROM TRANSFERRED GROUP BY b ORDER BY b")) {
        final Result first = rs.next();
        assertThat(asLocalDateTime(first.getProperty("b"))).isEqualTo(BUCKET);
        assertThat(((Number) first.getProperty("c")).longValue()).isEqualTo(2L);
        assertThat(asLocalDateTime(rs.next().getProperty("b"))).isEqualTo(LocalDateTime.of(2026, 10, 19, 8, 1));
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void otherTimeSeriesFunctionsAcceptCypherDatetimeTimestamps() {
      // ts.first / ts.last read their timestamp through the same DateUtils conversion
      assertThat(single("UNWIND [{v: 1, t: datetime('2026-10-19T08:00:02Z')}, {v: 2, t: datetime('2026-10-19T08:00:01Z')}] AS r "
          + "RETURN ts.first(r.v, r.t) AS b")).isEqualTo(2L);
      assertThat(single("UNWIND [{v: 1, t: localdatetime('2026-10-19T08:00:02')}, {v: 2, t: localdatetime('2026-10-19T08:00:01')}] AS r "
          + "RETURN ts.last(r.v, r.t) AS b")).isEqualTo(1L);
    }

    @Test
    void aTimeOfDayIsStillRefused() {
      // a time without a date is not a moment: refused, never read as some number that means something else
      assertThatThrownBy(() -> single("RETURN ts.timeBucket('1m', localtime('08:00:33')) AS b"))
          .hasStackTraceContaining("Unsupported timestamp for time_bucket");
    }

    @Test
    void sqlPrefixedBacktickedNameReachesTheFunction() {
      assertThat(single("RETURN sql.`ts.timeBucket`('1m', 1792396833000) AS b")).isEqualTo(BUCKET);
      assertThat(single("RETURN sql.`ts.timeBucket`('1m', datetime('2026-10-19T08:00:33Z')) AS b")).isEqualTo(BUCKET);
      assertThat(single("RETURN `ts`.`timeBucket`('1m', 1792396833000) AS b")).isEqualTo(BUCKET);
    }

    @Test
    void backtickedExistsStillTestsThePattern() {
      // the function name is unescaped before the exists(pattern) special case reads it, so the quoted spelling keeps
      // testing the pattern instead of becoming a non-null check that is always true
      database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})-[:R]->(:P {id: 2})").close());
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:P) RETURN n.id AS id, `exists`((n)-[:R]->()) AS e ORDER BY id")) {
        final Result first = rs.next();
        assertThat((Boolean) first.getProperty("e")).isTrue();
        final Result second = rs.next();
        assertThat((Boolean) second.getProperty("e")).isFalse();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    private Object single(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        final Object value = rs.next().getProperty("b");
        assertThat(rs.hasNext()).isFalse();
        return value instanceof Number n ? n.longValue() : asLocalDateTime(value);
      }
    }

    private static Object asLocalDateTime(final Object value) {
      if (value instanceof LocalDateTime)
        return value;
      if (value instanceof TemporalAccessor accessor)
        return LocalDateTime.from(accessor);
      return value;
    }
  }
}
