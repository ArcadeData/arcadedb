/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.function.date.*;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for OpenCypher date functions.
 */
class OpenCypherDateFunctionsTest {

  // ============ DateCurrentTimestamp tests ============

  @Test
  void dateCurrentTimestampBasic() {
    final DateCurrentTimestamp fn = new DateCurrentTimestamp();
    assertThat(fn.getName()).isEqualTo("date.currentTimestamp");

    final long before = System.currentTimeMillis();
    final long result = (Long) fn.execute(new Object[]{}, null);
    final long after = System.currentTimeMillis();

    assertThat(result).isBetween(before, after);
  }

  @Test
  void dateCurrentTimestampMetadata() {
    final DateCurrentTimestamp fn = new DateCurrentTimestamp();

    assertThat(fn.getMinArgs()).isEqualTo(0);
    assertThat(fn.getMaxArgs()).isEqualTo(0);
    assertThat(fn.getDescription()).contains("timestamp");
  }

  // ============ DateFormat tests ============

  @Test
  void dateFormatBasic() {
    final DateFormat fn = new DateFormat();
    assertThat(fn.getName()).isEqualTo("date.format");

    // Format a known timestamp
    final long timestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC
    final String result = (String) fn.execute(new Object[]{timestamp}, null);

    assertThat(result).isNotEmpty();
  }

  @Test
  void dateFormatWithPattern() {
    final DateFormat fn = new DateFormat();

    // Format with specific pattern, rendered in UTC
    final long timestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC
    final String result = (String) fn.execute(new Object[]{timestamp, "ms", "yyyy-MM-dd"}, null);

    assertThat(result).isEqualTo("2024-01-01");
  }

  @Test
  void dateFormatNullHandling() {
    final DateFormat fn = new DateFormat();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ DateSystemTimezone tests ============

  @Test
  void dateSystemTimezoneBasic() {
    final DateSystemTimezone fn = new DateSystemTimezone();
    assertThat(fn.getName()).isEqualTo("date.systemTimezone");

    final String result = (String) fn.execute(new Object[]{}, null);

    assertThat(result).isEqualTo(ZoneId.systemDefault().getId());
  }

  // ============ DateField tests ============

  @Test
  void dateFieldBasic() {
    final DateField fn = new DateField();
    assertThat(fn.getName()).isEqualTo("date.field");

    // DateField takes a timestamp, not a date string
    // Use a timestamp for 2024-01-15 (UTC)
    final long timestamp = 1705276200000L; // 2024-01-15 at some hour UTC
    final Object result = fn.execute(new Object[]{timestamp, "year"}, null);
    assertThat(result).isEqualTo(2024L);
  }

  @Test
  void dateFieldMonth() {
    final DateField fn = new DateField();

    // Use a timestamp for June 2024
    final long timestamp = 1718438400000L; // 2024-06-15 00:00:00 UTC
    final Object result = fn.execute(new Object[]{timestamp, "month"}, null);
    assertThat(result).isEqualTo(6L);
  }

  @Test
  void dateFieldNullHandling() {
    final DateField fn = new DateField();
    assertThat(fn.execute(new Object[]{null, "year"}, null)).isNull();
  }

  @Test
  void dateFieldIsoWeekAtYearBoundary() {
    final DateField fn = new DateField();

    // 2023-01-01 (Sunday) belongs to ISO-8601 week 52 of the week-based-year 2022, not week 0. date.field reads UTC.
    final long jan1_2023 = 1672531200000L; // 2023-01-01 00:00:00 UTC
    assertThat(fn.execute(new Object[]{jan1_2023, "weekofyear"}, null)).isEqualTo(52L);
    assertThat(fn.execute(new Object[]{jan1_2023, "week"}, null)).isEqualTo(52L);

    // 2023-01-02 (Monday) is the start of ISO week 1
    final long jan2_2023 = 1672617600000L; // 2023-01-02 00:00:00 UTC
    assertThat(fn.execute(new Object[]{jan2_2023, "weekofyear"}, null)).isEqualTo(1L);
  }

  /** The new weekyear/weekbasedyear names must not weaken the rejection of unrecognized field names. */
  @Test
  void dateFieldRejectsUnknownFieldName() {
    final DateField fn = new DateField();

    assertThatThrownBy(() -> fn.execute(new Object[]{1672531200000L, "weekofthemonth"}, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown date field: weekofthemonth");
    assertThatThrownBy(() -> fn.execute(new Object[]{1672531200000L, ""}, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown date field");
  }

  // ============ DateFields tests ============

  @Test
  void dateFieldsBasic() {
    final DateFields fn = new DateFields();
    assertThat(fn.getName()).isEqualTo("date.fields");

    // APOC's keys, the default pattern being 'yyyy-MM-dd HH:mm:ss' (issue #12106)
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(new Object[]{"2024-01-15 10:30:45"}, null);

    assertThat(result).containsExactly(Map.entry("years", 2024L), Map.entry("months", 1L), Map.entry("weekdays", 1L),
        Map.entry("days", 15L), Map.entry("hours", 10L), Map.entry("minutes", 30L), Map.entry("seconds", 45L),
        Map.entry("zoneid", "UTC"));
  }

  @Test
  void dateFieldsNullHandling() {
    final DateFields fn = new DateFields();
    assertThat(fn.execute(new Object[]{null}, null)).isEqualTo(Map.of());
  }

  // ============ DateAdd tests ============

  @Test
  void dateAddBasic() {
    final DateAdd fn = new DateAdd();
    assertThat(fn.getName()).isEqualTo("date.add");

    final long timestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC

    // DateAdd takes (timestamp, value, unit)
    // Add 1 day (d unit, 1 value)
    final long result = (Long) fn.execute(new Object[]{timestamp, 1, "d"}, null);
    assertThat(result).isEqualTo(timestamp + 86400000L);
  }

  @Test
  void dateAddHours() {
    final DateAdd fn = new DateAdd();

    final long timestamp = 1704067200000L;

    // Add 2 hours
    final long result = (Long) fn.execute(new Object[]{timestamp, 2, "h"}, null);
    assertThat(result).isEqualTo(timestamp + 2 * 3600000L);
  }

  @Test
  void dateAddNullHandling() {
    final DateAdd fn = new DateAdd();
    assertThat(fn.execute(new Object[]{null, 1, "d"}, null)).isNull();
  }

  // ============ DateConvert tests ============

  @Test
  void dateConvertBasic() {
    final DateConvert fn = new DateConvert();
    assertThat(fn.getName()).isEqualTo("date.convert");

    // Convert from ms to s
    final long msTimestamp = 1704067200000L;
    final long result = (Long) fn.execute(new Object[]{msTimestamp, "ms", "s"}, null);
    assertThat(result).isEqualTo(1704067200L);
  }

  @Test
  void dateConvertNullHandling() {
    final DateConvert fn = new DateConvert();
    assertThat(fn.execute(new Object[]{null, "ms", "s"}, null)).isNull();
  }

  @Test
  void dateConvertDaysToMillis() {
    final DateConvert fn = new DateConvert();

    // 100 days to milliseconds: well within long range, must be exact and positive
    final long result = (Long) fn.execute(new Object[]{100L, "d", "ms"}, null);
    assertThat(result).isEqualTo(100L * 86_400_000L);
  }

  @Test
  void dateConvertOverflowSaturates() {
    final DateConvert fn = new DateConvert();

    // 1_000_000_000_000 days * 86_400_000 ms overflows long: saturates as TimeUnit.convert (and APOC) does
    assertThat(fn.execute(new Object[]{1_000_000_000_000L, "d", "ms"}, null)).isEqualTo(Long.MAX_VALUE);
    assertThat(fn.execute(new Object[]{-1_000_000_000_000L, "d", "ms"}, null)).isEqualTo(Long.MIN_VALUE);
  }

  // ============ DateToISO8601 tests ============

  @Test
  void dateToISO8601Basic() {
    final DateToISO8601 fn = new DateToISO8601();
    assertThat(fn.getName()).isEqualTo("date.toISO8601");

    final long timestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC
    final String result = (String) fn.execute(new Object[]{timestamp}, null);

    // Should be a valid ISO8601 string with date and time parts
    assertThat(result).contains("T");

    // Round-trip: converting back should return the same timestamp
    final DateFromISO8601 fromFn = new DateFromISO8601();
    final long roundTrip = (Long) fromFn.execute(new Object[]{result}, null);
    assertThat(roundTrip).isEqualTo(timestamp);
  }

  @Test
  void dateToISO8601NullHandling() {
    final DateToISO8601 fn = new DateToISO8601();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ DateFromISO8601 tests ============

  @Test
  void dateFromISO8601Basic() {
    final DateFromISO8601 fn = new DateFromISO8601();
    assertThat(fn.getName()).isEqualTo("date.fromISO8601");

    final long result = (Long) fn.execute(new Object[]{"2024-01-01T00:00:00Z"}, null);

    // Should return timestamp for 2024-01-01
    assertThat(result).isEqualTo(1704067200000L);
  }

  @Test
  void dateFromISO8601NullHandling() {
    final DateFromISO8601 fn = new DateFromISO8601();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ DateParse tests ============

  @Test
  void dateParseBasic() {
    final DateParse fn = new DateParse();
    assertThat(fn.getName()).isEqualTo("date.parse");

    // The default pattern is 'yyyy-MM-dd HH:mm:ss', a text with no zone read in UTC (issue #12106)
    final Object result = fn.execute(new Object[]{"2024-01-15 10:30:00", "ms"}, null);

    assertThat(result).isEqualTo(1705314600000L);
  }

  @Test
  void dateParseWithCustomFormat() {
    final DateParse fn = new DateParse();

    // With custom format - need to match the format pattern
    final Object result = fn.execute(new Object[]{"2024-01-15 10:30:00", "ms", "yyyy-MM-dd HH:mm:ss"}, null);

    assertThat(result).isNotNull();
    assertThat((Long) result).isGreaterThan(0L);
  }

  @Test
  void dateParseNullHandling() {
    final DateParse fn = new DateParse();
    assertThat(fn.execute(new Object[]{null, "ms"}, null)).isNull();
  }

  // ============ Metadata tests ============

  @Test
  void dateFunctionsMetadata() {
    final DateFormat formatFn = new DateFormat();
    assertThat(formatFn.getMinArgs()).isEqualTo(1);
    assertThat(formatFn.getMaxArgs()).isEqualTo(4);
    assertThat(formatFn.getDescription()).isNotEmpty();

    final DateAdd addFn = new DateAdd();
    assertThat(addFn.getMinArgs()).isEqualTo(3);
    assertThat(addFn.getMaxArgs()).isEqualTo(4);
    assertThat(addFn.getDescription()).isNotEmpty();
  }

  /**
   * Issue #12106: the date.* functions answer as APOC 5.26's apoc.date.* do under the apoc. names. Every expected value is
   * what Neo4j 5.26 with APOC core 5.26.33 answers for the same call.
   */
  @Nested
  class Issue12106ApocDateSemantics extends TestHelper {

    private Object eval(final String expression) {
      final Result row = database.query("opencypher", "RETURN " + expression + " AS r").next();
      return row.getProperty("r");
    }

    @Test
    void formatDefaultsToApocPatternInUtc() {
      assertThat(eval("apoc.date.format(1600000000000)")).isEqualTo("2020-09-13 12:26:40");
      assertThat(eval("apoc.date.format(1600000000000, 'ms')")).isEqualTo("2020-09-13 12:26:40");
      assertThat(eval("apoc.date.format(-1)")).isEqualTo("1969-12-31 23:59:59");
      assertThat(eval("apoc.date.format(253402300800000)")).isEqualTo("10000-01-01 00:00:00");
      assertThat(eval("apoc.date.format(1600000000000, 'ms', '')")).isEqualTo("2020-09-13 12:26:40");
      assertThat(eval("apoc.date.format(1600000000000, 'ms', 'EEE, d MMM yyyy')")).isEqualTo("Sun, 13 Sep 2020");
    }

    @Test
    void formatTakesTimezoneAndSaturatesUnit() {
      assertThat(eval("apoc.date.format(1600000000000, 'ms', 'yyyy-MM-dd HH:mm:ss', 'Europe/Berlin')")).isEqualTo("2020-09-13 14:26:40");
      assertThat(eval("apoc.date.format(-1, 'milliseconds', 'yyyy-MM-dd HH:mm:ss zzz', 'GMT+5')"))
          .isEqualTo("1970-01-01 04:59:59 GMT+05:00");
      // 1600000000000 days is past Long.MAX_VALUE milliseconds: TimeUnit saturates
      assertThat(eval("apoc.date.format(1600000000000, 'd')")).isEqualTo("292278994-08-17 07:12:55");
      assertThatThrownBy(() -> eval("apoc.date.format(1600000000000, 'xyz')")).hasStackTraceContaining("The unit: xyz is not correct");
    }

    @Test
    void parseDefaultsToApocPattern() {
      assertThat(eval("apoc.date.parse('2020-01-02 03:04:05')")).isEqualTo(1577934245000L);
      assertThat(eval("apoc.date.parse('2020-01-02 03:04:05', 's')")).isEqualTo(1577934245L);
      assertThat(eval("apoc.date.parse('2020-01-02 03:04:05', 'd')")).isEqualTo(18263L);
      assertThat(eval("apoc.date.parse('1969-12-31 23:59:59', 's')")).isEqualTo(-1L);
      assertThat(eval("apoc.date.parse('')")).isNull();
      assertThat(eval("apoc.date.parse('   ', 'ms', 'yyyy-MM-dd')")).isNull();
      // a prefix of the text is enough, as for SimpleDateFormat
      assertThat(eval("apoc.date.parse('2020-01-02 03:04:05', 'ms', 'yyyy-MM-dd')")).isEqualTo(1577923200000L);
      assertThat(eval("apoc.date.parse('2020-01-02 03:04:05', 'ms', 'yyyy-MM-dd HH:mm:ss', 'Europe/Berlin')")).isEqualTo(1577930645000L);
      assertThatThrownBy(() -> eval("apoc.date.parse('2020-01-02T03:04:05')")).isInstanceOf(InvalidArgumentException.class);
    }

    @Test
    void fieldsAnswersApocKeys() {
      assertThat(eval("apoc.date.fields('2020-01-02 03:04:05')")).isEqualTo(
          Map.of("years", 2020L, "months", 1L, "weekdays", 4L, "days", 2L, "hours", 3L, "minutes", 4L, "seconds", 5L, "zoneid", "UTC"));
      assertThat(eval("apoc.date.fields('2020-01-02', 'yyyy-MM-dd')")).isEqualTo(
          Map.of("years", 2020L, "months", 1L, "weekdays", 4L, "days", 2L, "zoneid", "UTC"));
      assertThat(eval("apoc.date.fields('03:04', 'HH:mm')")).isEqualTo(Map.of("hours", 3L, "minutes", 4L, "seconds", 0L, "zoneid", "UTC"));
      assertThat(eval("apoc.date.fields('2020-01-02 03:04:05 Europe/Rome', 'yyyy-MM-dd HH:mm:ss VV')")).isEqualTo(
          Map.of("years", 2020L, "months", 1L, "weekdays", 4L, "days", 2L, "hours", 3L, "minutes", 4L, "seconds", 5L, "zoneid", "CET"));
      assertThat(eval("apoc.date.fields(null)")).isEqualTo(Map.of());
    }

    @Test
    void fieldTakesApocUnitsAndTimezone() {
      assertThat(eval("apoc.date.field(1600000000000)")).isEqualTo(13L);
      assertThat(eval("apoc.date.field(1600000000000, 'h')")).isEqualTo(12L);
      assertThat(eval("apoc.date.field(1600000000000, 'm')")).isEqualTo(26L);
      assertThat(eval("apoc.date.field(1600000000000, 's')")).isEqualTo(40L);
      assertThat(eval("apoc.date.field(-1, 'ms')")).isEqualTo(999L);
      assertThat(eval("apoc.date.field(1600000000000, 'w')")).isEqualTo(7L);
      assertThat(eval("apoc.date.field(1600000000000, 'weekdays')")).isEqualTo(7L);
      assertThat(eval("apoc.date.field(1600000000000, 'years')")).isEqualTo(2020L);
      assertThat(eval("apoc.date.field(1600000000000, 'h', 'Europe/Berlin')")).isEqualTo(14L);
    }

    @Test
    void addTakesApocSignature() {
      assertThat(eval("apoc.date.add(1600000000000, 'ms', 1, 'd')")).isEqualTo(1600086400000L);
      assertThat(eval("apoc.date.add(1600000000, 's', -2, 'h')")).isEqualTo(1599992800L);
      // the amount is converted to the unit of the time, truncating toward zero
      assertThat(eval("apoc.date.add(1600000000000, 'minutes', 100000000, 's')")).isEqualTo(1600001666666L);
      assertThat(eval("apoc.date.add(0, 'ms', 9223372036854775807, 'd')")).isEqualTo(Long.MAX_VALUE);
      // ArcadeDB's three-argument form on a timestamp in milliseconds stays
      assertThat(eval("date.add(1000, 500, 'ms')")).isEqualTo(1500L);
    }

    @Test
    void toISO8601PrintsMillisecondsAndTakesUnit() {
      assertThat(eval("apoc.date.toISO8601(1600000000000)")).isEqualTo("2020-09-13T12:26:40.000Z");
      assertThat(eval("apoc.date.toISO8601(1600000000123)")).isEqualTo("2020-09-13T12:26:40.123Z");
      assertThat(eval("apoc.date.toISO8601(1600000000, 's')")).isEqualTo("2020-09-13T12:26:40.000Z");
      assertThat(eval("apoc.date.toISO8601(-1, 'days')")).isEqualTo("1969-12-31T00:00:00.000Z");
    }

    @Test
    void convertSaturates() {
      assertThat(eval("apoc.date.convert(1600000000000, 'd', 'ms')")).isEqualTo(Long.MAX_VALUE);
      assertThat(eval("apoc.date.convert(-1600000000000, 'd', 'ms')")).isEqualTo(Long.MIN_VALUE);
      assertThat(eval("apoc.date.convert(253402300800000, 'hours', 'days')")).isEqualTo(10558429200000L);
      assertThat(eval("apoc.date.convert(-1500, 'ms', 's')")).isEqualTo(-1L);
    }
  }
}
