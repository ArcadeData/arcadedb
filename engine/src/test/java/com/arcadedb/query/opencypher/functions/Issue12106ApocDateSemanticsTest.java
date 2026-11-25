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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.date.ApocDateAdd;
import com.arcadedb.function.date.ApocDateConvert;
import com.arcadedb.function.date.ApocDateField;
import com.arcadedb.function.date.ApocDateFields;
import com.arcadedb.function.date.ApocDateFormat;
import com.arcadedb.function.date.ApocDateParse;
import com.arcadedb.function.date.ApocDateToISO8601;
import com.arcadedb.function.date.DateAdd;
import com.arcadedb.function.date.DateConvert;
import com.arcadedb.function.date.DateField;
import com.arcadedb.function.date.DateFields;
import com.arcadedb.function.date.DateFormat;
import com.arcadedb.function.date.DateParse;
import com.arcadedb.function.date.DateToISO8601;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12106: under their {@code apoc.} names the date functions answer as APOC 5.26's apoc.date.* do (every expected value is
 * what Neo4j 5.26 with APOC core 5.26.33 answers for the same call), while the {@code date.} names keep ArcadeDB's own
 * semantics, on openCypher and on SQL.
 */
class Issue12106ApocDateSemanticsTest extends TestHelper {

  // ============ the split between the two names ============

  @Test
  void theApocNamesResolveToTheApocFunctionsAndTheDateNamesToArcadeDbs() {
    final Map<String, Class<?>[]> expected = Map.of(//
        "format", new Class<?>[] { ApocDateFormat.class, DateFormat.class },//
        "parse", new Class<?>[] { ApocDateParse.class, DateParse.class },//
        "add", new Class<?>[] { ApocDateAdd.class, DateAdd.class },//
        "convert", new Class<?>[] { ApocDateConvert.class, DateConvert.class },//
        "field", new Class<?>[] { ApocDateField.class, DateField.class },//
        "fields", new Class<?>[] { ApocDateFields.class, DateFields.class },//
        "toISO8601", new Class<?>[] { ApocDateToISO8601.class, DateToISO8601.class });

    for (final Map.Entry<String, Class<?>[]> entry : expected.entrySet()) {
      final String apocName = "apoc.date." + entry.getKey();
      final String dateName = "date." + entry.getKey();
      assertThat(CypherFunctionRegistry.get(apocName)).as(apocName).isExactlyInstanceOf(entry.getValue()[0]);
      assertThat(CypherFunctionRegistry.get(apocName.toUpperCase(Locale.ROOT))).as(apocName).isExactlyInstanceOf(entry.getValue()[0]);
      assertThat(FunctionRegistry.get(apocName)).as(apocName).isExactlyInstanceOf(entry.getValue()[0]);
      assertThat(CypherFunctionRegistry.get(apocName).getName()).isEqualTo(apocName);
      assertThat(CypherFunctionRegistry.isBuiltInOrUnregistered(apocName)).as(apocName).isTrue();

      assertThat(CypherFunctionRegistry.get(dateName)).as(dateName).isExactlyInstanceOf(entry.getValue()[1]);
      assertThat(FunctionRegistry.get(dateName)).as(dateName).isExactlyInstanceOf(entry.getValue()[1]);
    }

    // the functions APOC and ArcadeDB answer alike have one implementation, which the apoc. name still reaches
    for (final String name : List.of("currentTimestamp", "fromISO8601", "systemTimezone")) {
      final StatelessFunction function = CypherFunctionRegistry.get("date." + name);
      assertThat(function).as(name).isNotNull();
      assertThat(CypherFunctionRegistry.get("apoc.date." + name)).as(name).isSameAs(function);
      assertThat(FunctionRegistry.get("apoc.date." + name)).as(name).isSameAs(function);
    }
    // and every other apoc. name still resolves by stripping the prefix
    assertThat(CypherFunctionRegistry.get("apoc.text.indexOf")).isSameAs(CypherFunctionRegistry.get("text.indexOf"));
  }

  /** The date. names answer exactly as before issue #12106, from openCypher and from SQL. */
  @Test
  @SuppressWarnings("unchecked")
  void theDateNamesKeepArcadeDbSemanticsOnBothLanguages() {
    for (final String language : List.of("opencypher", "sql")) {
      // the default pattern is ISO, a text without an offset read in the JVM's zone
      assertThat(eval(language, "date.parse('2024-01-15T10:30:00')")).as(language)
          .isEqualTo(LocalDateTime.parse("2024-01-15T10:30:00").atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
      // strict resolution (issue #10206): February 30th is refused, where apoc.date.parse rolls it over
      assertThatThrownBy(() -> eval(language, "date.parse('2021-02-30 00:00:00', 'ms', 'yyyy-MM-dd HH:mm:ss')")).as(language)
          .hasStackTraceContaining("2021-02-30");

      // the ISO week keys of issue #7044
      final Map<String, Object> fields = (Map<String, Object>) eval(language, "date.fields('2023-01-01T00:00:00')");
      assertThat(fields).as(language).containsEntry("weekOfYear", 52L).containsEntry("weekBasedYear", 2022L).containsEntry("year", 2023L)
          .containsKey("timezone");

      assertThat(eval(language, "date.toISO8601(0)")).as(language)
          .isEqualTo(Instant.EPOCH.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
      assertThatThrownBy(() -> eval(language, "date.convert(1000000000000, 'd', 'ms')")).as(language)
          .hasStackTraceContaining("Date conversion overflow");
      assertThat(eval(language, "date.add(1000, 500, 'ms')")).as(language).isEqualTo(1500L);
    }
    assertThat(eval("opencypher", "apoc.date.parse('2021-02-30 00:00:00')")).isEqualTo(Instant.parse("2021-03-02T00:00:00Z").toEpochMilli());
  }

  // ============ apoc.date.* answers as APOC ============

  @Test
  void formatDefaultsToApocPatternInUtc() {
    assertThat(cypher("apoc.date.format(1600000000000)")).isEqualTo("2020-09-13 12:26:40");
    assertThat(cypher("apoc.date.format(1600000000000, 'ms')")).isEqualTo("2020-09-13 12:26:40");
    assertThat(cypher("apoc.date.format(-1)")).isEqualTo("1969-12-31 23:59:59");
    assertThat(cypher("apoc.date.format(253402300800000)")).isEqualTo("10000-01-01 00:00:00");
    assertThat(cypher("apoc.date.format(1600000000000, 'ms', '')")).isEqualTo("2020-09-13 12:26:40");
    assertThat(cypher("apoc.date.format(1600000000000, 'ms', 'EEE, d MMM yyyy')")).isEqualTo("Sun, 13 Sep 2020");
  }

  @Test
  void formatTakesTimezoneAndSaturatesUnit() {
    assertThat(cypher("apoc.date.format(1600000000000, 'ms', 'yyyy-MM-dd HH:mm:ss', 'Europe/Berlin')")).isEqualTo("2020-09-13 14:26:40");
    assertThat(cypher("apoc.date.format(-1, 'milliseconds', 'yyyy-MM-dd HH:mm:ss zzz', 'GMT+5')")).isEqualTo("1970-01-01 04:59:59 GMT+05:00");
    // 1600000000000 days is past Long.MAX_VALUE milliseconds: TimeUnit saturates
    assertThat(cypher("apoc.date.format(1600000000000, 'd')")).isEqualTo("292278994-08-17 07:12:55");
    assertThatThrownBy(() -> cypher("apoc.date.format(1600000000000, 'xyz')")).isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("The unit: xyz is not correct");
  }

  @Test
  void parseDefaultsToApocPattern() {
    assertThat(cypher("apoc.date.parse('2020-01-02 03:04:05')")).isEqualTo(1577934245000L);
    assertThat(cypher("apoc.date.parse('2020-01-02 03:04:05', 's')")).isEqualTo(1577934245L);
    assertThat(cypher("apoc.date.parse('2020-01-02 03:04:05', 'd')")).isEqualTo(18263L);
    assertThat(cypher("apoc.date.parse('1969-12-31 23:59:59', 's')")).isEqualTo(-1L);
    assertThat(cypher("apoc.date.parse('')")).isNull();
    assertThat(cypher("apoc.date.parse('   ', 'ms', 'yyyy-MM-dd')")).isNull();
    // a prefix of the text is enough, as for SimpleDateFormat
    assertThat(cypher("apoc.date.parse('2020-01-02 03:04:05', 'ms', 'yyyy-MM-dd')")).isEqualTo(1577923200000L);
    assertThat(cypher("apoc.date.parse('2020-01-02 03:04:05', 'ms', 'yyyy-MM-dd HH:mm:ss', 'Europe/Berlin')")).isEqualTo(1577930645000L);
    assertThatThrownBy(() -> cypher("apoc.date.parse('2020-01-02T03:04:05')")).isInstanceOf(InvalidArgumentException.class);
  }

  @Test
  void fieldsAnswersApocKeys() {
    assertThat(cypher("apoc.date.fields('2020-01-02 03:04:05')")).isEqualTo(
        Map.of("years", 2020L, "months", 1L, "weekdays", 4L, "days", 2L, "hours", 3L, "minutes", 4L, "seconds", 5L, "zoneid", "UTC"));
    assertThat(cypher("apoc.date.fields('2020-01-02', 'yyyy-MM-dd')")).isEqualTo(
        Map.of("years", 2020L, "months", 1L, "weekdays", 4L, "days", 2L, "zoneid", "UTC"));
    assertThat(cypher("apoc.date.fields('03:04', 'HH:mm')")).isEqualTo(Map.of("hours", 3L, "minutes", 4L, "seconds", 0L, "zoneid", "UTC"));
    // the short name of the zone in Locale.ROOT, as APOC renders it, depends on the JDK's locale data (CET where Neo4j 5.26 was
    // checked, GMT+01:00 on JDK 26), so the expectation is read from the running JDK
    assertThat(cypher("apoc.date.fields('2020-01-02 03:04:05 Europe/Rome', 'yyyy-MM-dd HH:mm:ss VV')")).isEqualTo(
        Map.of("years", 2020L, "months", 1L, "weekdays", 4L, "days", 2L, "hours", 3L, "minutes", 4L, "seconds", 5L, "zoneid",
            ZoneId.of("Europe/Rome").getDisplayName(TextStyle.SHORT, Locale.ROOT)));
    assertThat(cypher("apoc.date.fields(null)")).isEqualTo(Map.of());
  }

  @Test
  void fieldTakesApocUnitsAndTimezone() {
    assertThat(cypher("apoc.date.field(1600000000000)")).isEqualTo(13L);
    assertThat(cypher("apoc.date.field(1600000000000, 'h')")).isEqualTo(12L);
    assertThat(cypher("apoc.date.field(1600000000000, 'm')")).isEqualTo(26L);
    assertThat(cypher("apoc.date.field(1600000000000, 's')")).isEqualTo(40L);
    assertThat(cypher("apoc.date.field(-1, 'ms')")).isEqualTo(999L);
    assertThat(cypher("apoc.date.field(1600000000000, 'w')")).isEqualTo(7L);
    assertThat(cypher("apoc.date.field(1600000000000, 'weekdays')")).isEqualTo(7L);
    assertThat(cypher("apoc.date.field(1600000000000, 'years')")).isEqualTo(2020L);
    assertThat(cypher("apoc.date.field(1600000000000, 'h', 'Europe/Berlin')")).isEqualTo(14L);
    // ArcadeDB's ISO week (issue #7044) is there too
    assertThat(cypher("apoc.date.field(1672531200000, 'weekOfYear')")).isEqualTo(52L);
    assertThat(cypher("apoc.date.field(1672531200000, 'weekYear')")).isEqualTo(2022L);
  }

  @Test
  void toISO8601PrintsMillisecondsAndTakesUnit() {
    assertThat(cypher("apoc.date.toISO8601(1600000000000)")).isEqualTo("2020-09-13T12:26:40.000Z");
    assertThat(cypher("apoc.date.toISO8601(1600000000123)")).isEqualTo("2020-09-13T12:26:40.123Z");
    assertThat(cypher("apoc.date.toISO8601(1600000000, 's')")).isEqualTo("2020-09-13T12:26:40.000Z");
    assertThat(cypher("apoc.date.toISO8601(-1, 'days')")).isEqualTo("1969-12-31T00:00:00.000Z");
  }

  @Test
  void convertSaturates() {
    assertThat(cypher("apoc.date.convert(1600000000000, 'd', 'ms')")).isEqualTo(Long.MAX_VALUE);
    assertThat(cypher("apoc.date.convert(-1600000000000, 'd', 'ms')")).isEqualTo(Long.MIN_VALUE);
    assertThat(cypher("apoc.date.convert(253402300800000, 'hours', 'days')")).isEqualTo(10558429200000L);
    assertThat(cypher("apoc.date.convert(-1500, 'ms', 's')")).isEqualTo(-1L);
  }

  // ============ date.add: one signature per name ============

  /**
   * apoc.date.add is APOC's (time, unit, addValue, addUnit) only, date.add ArcadeDB's (timestamp, value, unit) only: no call is
   * told apart by its argument count, so an APOC call missing its last argument is an arity error, not the other signature.
   */
  @Test
  void addHasOneSignaturePerName() {
    assertThat(cypher("apoc.date.add(1600000000000, 'ms', 1, 'd')")).isEqualTo(1600086400000L);
    assertThat(cypher("apoc.date.add(1600000000, 's', -2, 'h')")).isEqualTo(1599992800L);
    // the amount is converted to the unit of the time, truncating toward zero
    assertThat(cypher("apoc.date.add(1600000000000, 'minutes', 100000000, 's')")).isEqualTo(1600001666666L);
    assertThat(cypher("apoc.date.add(0, 'ms', 9223372036854775807, 'd')")).isEqualTo(Long.MAX_VALUE);
    assertThatThrownBy(() -> cypher("apoc.date.add(9223372036854775807, 'ms', 1, 'ms')")).isInstanceOf(InvalidArgumentException.class);
    assertThatThrownBy(() -> cypher("apoc.date.add(1000, 500, 'ms')")).hasStackTraceContaining("apoc.date.add");
    // the three-argument call of before the split is pointed at the name that still takes it
    assertThatThrownBy(() -> cypher("apoc.date.add(1000, 500, 'ms')")).hasStackTraceContaining("date.add(timestamp, value, unit)");

    assertThat(cypher("date.add(1000, 500, 'ms')")).isEqualTo(1500L);
    assertThat(cypher("date.add(1000, 2, 's')")).isEqualTo(3000L);
    assertThatThrownBy(() -> cypher("date.add(1000, 'ms', 1, 'd')")).hasStackTraceContaining("date.add");
  }

  // ============ invalid arguments are client errors ============

  /**
   * TimeZone.getTimeZone answers GMT for an id it does not know, which APOC's apoc.date.format/parse pass on silently. A zone
   * neither TimeZone nor ZoneId knows is an error in every apoc.date function, as it already was in apoc.date.field/fields.
   */
  @Test
  void anUnknownTimezoneIsRefusedByFormatAndParse() {
    for (final String call : List.of("apoc.date.format(0, 'ms', 'yyyy-MM-dd', 'Mars/Olympus')",
        "apoc.date.parse('2020-01-02 03:04:05', 'ms', 'yyyy-MM-dd HH:mm:ss', 'Mars/Olympus')", "apoc.date.field(0, 'h', 'Mars/Olympus')",
        "apoc.date.fields('2020-01-02 03:04:05', 'yyyy-MM-dd HH:mm:ss', 'Mars/Olympus')"))
      assertThatThrownBy(() -> cypher(call)).as(call).isInstanceOf(InvalidArgumentException.class)
          .hasMessageContaining("The timezone: Mars/Olympus is not correct")
          .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION));

    // every id TimeZone knows, and the offsets and short ids only ZoneId reads, are zones
    assertThat(cypher("apoc.date.format(0, 'ms', 'yyyy-MM-dd HH:mm', 'GMT')")).isEqualTo("1970-01-01 00:00");
    assertThat(cypher("apoc.date.format(0, 'ms', 'yyyy-MM-dd HH:mm', 'GMT+2:00')")).isEqualTo("1970-01-01 02:00");
    assertThat(cypher("apoc.date.format(0, 'ms', 'yyyy-MM-dd HH:mm', '+02:00')")).isEqualTo("1970-01-01 02:00");
    assertThat(cypher("apoc.date.format(0, 'ms', 'yyyy-MM-dd HH:mm', 'Z')")).isEqualTo("1970-01-01 00:00");
    assertThat(cypher("apoc.date.format(0, 'ms', 'yyyy-MM-dd HH:mm', 'PST')")).isEqualTo("1969-12-31 16:00");
    assertThat(cypher("apoc.date.parse('1970-01-01 02:00', 'ms', 'yyyy-MM-dd HH:mm', '+02:00')")).isEqualTo(0L);
  }

  @Test
  void aBadUnitIsAClientErrorOnBothNames() {
    for (final String call : List.of("apoc.date.field(0, 'fortnight')", "apoc.date.convert(1, 'fortnight', 'ms')",
        "apoc.date.add(0, 'ms', 1, 'fortnight')", "apoc.date.format(0, 'fortnight')", "apoc.date.parse('x', 'fortnight')",
        "apoc.date.field(0, null)", "apoc.date.format(0, 'ms', 'yyyy-MM-dd bb')"))
      assertThatThrownBy(() -> cypher(call)).as(call).isInstanceOf(InvalidArgumentException.class)
          .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION));

    // the date. names keep their IllegalArgumentException, which is a client error too
    for (final String call : List.of("date.field(0, 'fortnight')", "date.convert(1, 'fortnight', 'ms')", "date.add(0, 1, 'fortnight')"))
      assertThatThrownBy(() -> cypher(call)).as(call).hasStackTraceContaining("Unknown")
          .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION));
  }

  // ============ the per-thread formatter cache ============

  /** Parsing a text that names a zone sets it on the SimpleDateFormat: the cached formatter must not keep it for the next call. */
  @Test
  void aCachedFormatterDoesNotKeepTheZoneATextNamed() {
    assertThat(cypher("apoc.date.parse('2020-01-02 03:04:05 PST', 'ms', 'yyyy-MM-dd HH:mm:ss z')")).isEqualTo(
        Instant.parse("2020-01-02T11:04:05Z").toEpochMilli());
    assertThat(cypher("apoc.date.format(0, 'ms', 'yyyy-MM-dd HH:mm:ss z')")).isEqualTo("1970-01-01 00:00:00 UTC");
  }

  /** SimpleDateFormat is not thread-safe: the cache is per thread, so concurrent calls answer as sequential ones. */
  @Test
  void concurrentCallsAnswerAsSequentialOnes() throws Exception {
    final ApocDateFormat format = new ApocDateFormat();
    final ApocDateParse parse = new ApocDateParse();
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      final List<Future<List<String>>> futures = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        final int thread = t;
        futures.add(executor.submit(() -> {
          final List<String> mismatches = new ArrayList<>();
          for (long i = 0; i < 2_000; i++) {
            final long millis = (thread * 2_000L + i) * 86_399_001L;
            final String text = (String) format.execute(new Object[] { millis, "ms", "yyyy-MM-dd HH:mm:ss.SSS" }, null);
            final Object back = parse.execute(new Object[] { text, "ms", "yyyy-MM-dd HH:mm:ss.SSS" }, null);
            if (!Long.valueOf(millis).equals(back))
              mismatches.add(millis + " -> " + text + " -> " + back);
          }
          return mismatches;
        }));
      }
      for (final Future<List<String>> future : futures)
        assertThat(future.get()).isEmpty();
    } finally {
      executor.shutdownNow();
    }
  }

  // ============ a point in time is not a count in the unit ============

  /**
   * A Date, Instant, ZonedDateTime or LocalDateTime is already a point in time: the unit says what a number counts, so it must not
   * scale one. apoc.date.format(instant, 's') used to read the instant's milliseconds as seconds and render a date 1000 times too far.
   */
  @Test
  void theUnitDoesNotScaleAPointInTime() {
    final Instant instant = Instant.parse("2024-01-15T10:30:00.250Z");
    final List<Object> points = List.of(instant, Date.from(instant), instant.atZone(ZoneId.of("UTC")),
        LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));

    for (final Object point : points) {
      assertThat(new ApocDateFormat().execute(new Object[] { point, "s", "yyyy-MM-dd HH:mm:ss.SSS" }, null)).as(point.getClass().getName())
          .isEqualTo("2024-01-15 10:30:00.250");
      assertThat(new ApocDateToISO8601().execute(new Object[] { point, "s" }, null)).as(point.getClass().getName())
          .isEqualTo("2024-01-15T10:30:00.250Z");
      // the time is in the unit the call names, so a point in time is expressed in it before the amount is added
      assertThat(new ApocDateAdd().execute(new Object[] { point, "s", 1L, "m" }, null)).as(point.getClass().getName())
          .isEqualTo(instant.getEpochSecond() + 60L);
    }

    // a number, or its text, is still a count in the unit
    assertThat(new ApocDateFormat().execute(new Object[] { 1L, "s", "yyyy-MM-dd HH:mm:ss" }, null)).isEqualTo("1970-01-01 00:00:01");
    assertThat(new ApocDateFormat().execute(new Object[] { "1", "s", "yyyy-MM-dd HH:mm:ss" }, null)).isEqualTo("1970-01-01 00:00:01");
    assertThat(new ApocDateAdd().execute(new Object[] { 10L, "s", 1L, "m" }, null)).isEqualTo(70L);
    // and an unknown unit is refused for a point in time too
    assertThatThrownBy(() -> new ApocDateFormat().execute(new Object[] { instant, "fortnight" }, null))
        .isInstanceOf(InvalidArgumentException.class);
  }

  // ============ unregister by an apoc. name ============

  /** A function registered under its apoc. name is the one unregister removes by that name; the plain name's then answers it. */
  @Test
  void unregisterByAnApocNameRemovesTheFunctionRegisteredUnderIt() {
    final StatelessFunction plain = new NamedFunction("zz12106.f");
    final StatelessFunction apoc = new NamedFunction("apoc.zz12106.f");
    CypherFunctionRegistry.register(plain);
    CypherFunctionRegistry.register(apoc);
    try {
      assertThat(CypherFunctionRegistry.get("apoc.zz12106.f")).isSameAs(apoc);
      assertThat(FunctionRegistry.get("apoc.zz12106.f")).isSameAs(apoc);
      assertThat(FunctionRegistry.normalizeApocName("APOC.ZZ12106.F")).isEqualTo("apoc.zz12106.f");

      assertThat(CypherFunctionRegistry.unregister("apoc.zz12106.f")).isSameAs(apoc);
      assertThat(FunctionRegistry.unregister("apoc.zz12106.f")).isSameAs(apoc);

      // with the apoc. one gone, the apoc. name strips the prefix again
      assertThat(CypherFunctionRegistry.get("apoc.zz12106.f")).isSameAs(plain);
      assertThat(FunctionRegistry.get("apoc.zz12106.f")).isSameAs(plain);
      assertThat(FunctionRegistry.normalizeApocName("apoc.zz12106.f")).isEqualTo("zz12106.f");
    } finally {
      CypherFunctionRegistry.unregister("apoc.zz12106.f");
      CypherFunctionRegistry.unregister("zz12106.f");
      FunctionRegistry.unregister("apoc.zz12106.f");
      FunctionRegistry.unregister("zz12106.f");
    }
    assertThat(CypherFunctionRegistry.get("zz12106.f")).isNull();
    assertThat(FunctionRegistry.get("zz12106.f")).isNull();
  }

  private record NamedFunction(String name) implements StatelessFunction {
    @Override
    public String getName() {
      return name;
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      return name;
    }
  }

  private Object cypher(final String expression) {
    return eval("opencypher", expression);
  }

  private Object eval(final String language, final String expression) {
    try (final ResultSet rs = database.query(language, ("sql".equals(language) ? "SELECT " : "RETURN ") + expression + " AS r")) {
      return rs.next().getProperty("r");
    }
  }
}
