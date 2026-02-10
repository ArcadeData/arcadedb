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

import com.arcadedb.query.opencypher.functions.date.*;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

    // Format with specific pattern
    final long timestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC
    final String result = (String) fn.execute(new Object[]{timestamp, "ms", "yyyy-MM-dd"}, null);

    // Note: Result depends on system timezone, but should contain date parts
    assertThat(result).matches("\\d{4}-\\d{2}-\\d{2}");
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

  // ============ DateFields tests ============

  @Test
  void dateFieldsBasic() {
    final DateFields fn = new DateFields();
    assertThat(fn.getName()).isEqualTo("date.fields");

    // DateFields expects a date string, not a timestamp
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(new Object[]{"2024-01-15T10:30:45"}, null);

    assertThat(result).containsEntry("year", 2024L);
    assertThat(result).containsEntry("month", 1L);
    assertThat(result).containsEntry("day", 15L);
    assertThat(result).containsEntry("hour", 10L);
    assertThat(result).containsEntry("minute", 30L);
    assertThat(result).containsEntry("second", 45L);
  }

  @Test
  void dateFieldsNullHandling() {
    final DateFields fn = new DateFields();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
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

    // DateParse uses ISO datetime format by default
    // Parse with unit (2nd arg) and optional format (3rd arg)
    final Object result = fn.execute(new Object[]{"2024-01-15T10:30:00", "ms"}, null);

    assertThat(result).isNotNull();
    assertThat((Long) result).isGreaterThan(0L);
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
    assertThat(formatFn.getMaxArgs()).isEqualTo(3);
    assertThat(formatFn.getDescription()).isNotEmpty();

    final DateAdd addFn = new DateAdd();
    assertThat(addFn.getMinArgs()).isEqualTo(3);
    assertThat(addFn.getMaxArgs()).isEqualTo(3);
    assertThat(addFn.getDescription()).isNotEmpty();
  }
}
