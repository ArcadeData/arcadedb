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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue10199TemporalConstructorTest extends TestHelper {

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
