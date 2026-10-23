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
package com.arcadedb.utility;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.method.string.SQLMethodFormat;
import com.arcadedb.serializer.BinarySerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10676: {@code DateUtils.format(value, pattern, timezone)}, which backs the SQL {@code .format(pattern, timezone)} method,
 * converted only some value classes into the requested zone. A {@code LocalDateTime} (the UTC wall clock a DATETIME is read back
 * as by default) had the zone attached to the same wall clock, and a {@code ZonedDateTime} / {@code OffsetDateTime} ignored the
 * zone entirely, so a zoned format, and a GROUP BY on it, used the UTC wall clock.
 * <p>
 * Every value class that denotes an instant must render the same instant identically in the requested zone. A {@code LocalDate}
 * is a day, not an instant, and keeps its fields.
 */
class Issue10676FormatZoneConversionTest extends TestHelper {
  private static final String  PATTERN = "yyyy-MM-dd HH:mm";
  private static final String  ROME    = "Europe/Rome";
  // 2020-01-31T23:30Z is already February in Rome (UTC+1 in winter)
  private static final Instant LATE_JANUARY      = Instant.parse("2020-01-31T23:30:00Z");
  // the spring DST change in Rome is at 2020-03-29T01:00Z: 02:00 CET becomes 03:00 CEST
  private static final Instant BEFORE_SPRING_DST = Instant.parse("2020-03-29T00:30:00Z");
  private static final Instant AFTER_SPRING_DST  = Instant.parse("2020-03-29T01:30:00Z");
  // the autumn DST change in Rome is at 2020-10-25T01:00Z: 03:00 CEST becomes 02:00 CET
  private static final Instant BEFORE_AUTUMN_DST = Instant.parse("2020-10-25T00:30:00Z");
  private static final Instant AFTER_AUTUMN_DST  = Instant.parse("2020-10-25T01:30:00Z");

  private static final Map<Instant, String> EXPECTED_IN_ROME = Map.of(//
      LATE_JANUARY, "2020-02-01 00:30",//
      BEFORE_SPRING_DST, "2020-03-29 01:30",//
      AFTER_SPRING_DST, "2020-03-29 03:30",//
      BEFORE_AUTUMN_DST, "2020-10-25 02:30",//
      AFTER_AUTUMN_DST, "2020-10-25 02:30");

  @Test
  void everyInstantClassRendersTheSameInstantInTheRequestedZone() {
    for (final Map.Entry<Instant, String> entry : EXPECTED_IN_ROME.entrySet()) {
      final Instant instant = entry.getKey();
      final String expected = entry.getValue();

      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTimeInMillis(instant.toEpochMilli());

      final Map<String, Object> values = new TreeMap<>();
      values.put("LocalDateTime (UTC wall clock)", LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
      values.put("ZonedDateTime (UTC)", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC));
      values.put("ZonedDateTime (Asia/Tokyo)", ZonedDateTime.ofInstant(instant, ZoneId.of("Asia/Tokyo")));
      values.put("OffsetDateTime (+05:00)", OffsetDateTime.ofInstant(instant, ZoneOffset.ofHours(5)));
      values.put("Instant", instant);
      values.put("Date", Date.from(instant));
      values.put("Calendar", calendar);
      values.put("epoch millis", instant.toEpochMilli());

      for (final Map.Entry<String, Object> value : values.entrySet())
        assertThat(DateUtils.format(value.getValue(), PATTERN, ROME)).as("%s of %s", value.getKey(), instant).isEqualTo(expected);
    }
  }

  @Test
  void zonePatternLettersStillRenderTheRequestedZone() {
    final LocalDateTime utcWallClock = LocalDateTime.ofInstant(LATE_JANUARY, ZoneOffset.UTC);
    assertThat(DateUtils.format(utcWallClock, "yyyy-MM-dd HH:mm VV", ROME)).isEqualTo("2020-02-01 00:30 Europe/Rome");
    assertThat(DateUtils.format(ZonedDateTime.ofInstant(LATE_JANUARY, ZoneOffset.UTC), "yyyy-MM-dd HH:mm VV", ROME)).isEqualTo(
        "2020-02-01 00:30 Europe/Rome");
  }

  @Test
  void withoutAZoneEveryClassKeepsItsOwnFields() {
    assertThat(DateUtils.format(LocalDateTime.of(2020, 1, 31, 23, 30), PATTERN)).isEqualTo("2020-01-31 23:30");
    assertThat(DateUtils.format(ZonedDateTime.of(2020, 1, 31, 23, 30, 0, 0, ZoneId.of(ROME)), PATTERN)).isEqualTo(
        "2020-01-31 23:30");
    assertThat(DateUtils.format(OffsetDateTime.of(2020, 1, 31, 23, 30, 0, 0, ZoneOffset.ofHours(5)), PATTERN)).isEqualTo(
        "2020-01-31 23:30");
  }

  /** A DATE is a day with no instant: a zone west of UTC must not move it to the previous day. */
  @Test
  void aLocalDateKeepsItsDayInEveryZone() {
    final LocalDate day = LocalDate.of(2020, 2, 1);
    for (final String zone : new String[] { "UTC", ROME, "America/New_York", "Pacific/Kiritimati", "Pacific/Pago_Pago" })
      assertThat(DateUtils.format(day, PATTERN, zone)).as(zone).isEqualTo("2020-02-01 00:00");
  }

  @Test
  void theSqlMethodConvertsUnderEveryDateTimeImplementation() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.publishDate DATETIME");
    database.transaction(() -> {
      for (final Instant instant : EXPECTED_IN_ROME.keySet())
        database.command("sql", "INSERT INTO Asset SET publishDate = ?", instant.toEpochMilli()).close();
    });

    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Class<?>[] implementations = { LocalDateTime.class, ZonedDateTime.class, Instant.class, Date.class, Calendar.class };
    try {
      for (final Class<?> implementation : implementations) {
        serializer.setDateTimeImplementation(implementation);

        final Map<String, String> actual = new TreeMap<>();
        try (final ResultSet rs = database.query("sql",
            "SELECT publishDate.asLong() AS millis, publishDate.format('" + PATTERN + "', '" + ROME + "') AS rome FROM Asset")) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            actual.put(Instant.ofEpochMilli(row.<Long>getProperty("millis")).toString(), row.getProperty("rome"));
          }
        }

        final Map<String, String> expected = new TreeMap<>();
        EXPECTED_IN_ROME.forEach((instant, text) -> expected.put(instant.toString(), text));
        assertThat(actual).as(implementation.getSimpleName()).isEqualTo(expected);
      }
    } finally {
      serializer.setDateTimeImplementation(LocalDateTime.class);
    }
  }

  @Test
  void groupByOnAZonedMonthGroupsByTheMonthInThatZone() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.publishDate DATETIME");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Asset SET publishDate = ?", LATE_JANUARY.toEpochMilli()).close();
      database.command("sql", "INSERT INTO Asset SET publishDate = ?", Instant.parse("2020-02-10T12:00:00Z").toEpochMilli()).close();
      database.command("sql", "INSERT INTO Asset SET publishDate = ?", Instant.parse("2020-01-15T12:00:00Z").toEpochMilli()).close();
    });

    final Map<String, Long> months = new TreeMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT publishDate.format('yyyy-MM', '" + ROME + "') AS month, count(*) AS n FROM Asset GROUP BY month")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        months.put(row.getProperty("month"), row.<Number>getProperty("n").longValue());
      }
    }
    assertThat(months).isEqualTo(Map.of("2020-01", 1L, "2020-02", 2L));
  }

  /** The collection branch of the SQL method used to drop the zone argument altogether. */
  @Test
  void aCollectionOfDatesIsConvertedToo() {
    final List<Object> values = new ArrayList<>();
    values.add(LocalDateTime.ofInstant(LATE_JANUARY, ZoneOffset.UTC));
    values.add(Date.from(AFTER_SPRING_DST));

    final Object result = new SQLMethodFormat().execute(values, null, new BasicCommandContext(),
        new Object[] { "'" + PATTERN + "'", ROME });
    assertThat(result).isEqualTo(List.of("2020-02-01 00:30", "2020-03-29 03:30"));
  }
}
