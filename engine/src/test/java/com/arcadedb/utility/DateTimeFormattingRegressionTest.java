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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.function.date.DateFormat;
import com.arcadedb.function.text.FormatFunction;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.method.string.SQLMethodFormat;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for date, time, locale and zone handling in formatting and parsing.
 */
class DateTimeFormattingRegressionTest {

  /**
   * Regression for #10676.
   * <p>
   * Issue #10676: {@code DateUtils.format(value, pattern, timezone)}, which backs the SQL {@code .format(pattern, timezone)} method,
   * converted only some value classes into the requested zone. A {@code LocalDateTime} (the UTC wall clock a DATETIME is read back
   * as by default) had the zone attached to the same wall clock, and a {@code ZonedDateTime} / {@code OffsetDateTime} ignored the
   * zone entirely, so a zoned format, and a GROUP BY on it, used the UTC wall clock.
   * <p>
   * Every value class that denotes an instant must render the same instant identically in the requested zone. A {@code LocalDate}
   * is a day, not an instant, and keeps its fields.
   */
  @Nested
  class FormatZoneConversionTest extends TestHelper {
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

  /**
   * Regression for #7144, the follow-up to #7112: pinning {@link DateUtils#getFormatter(String)} to {@code Locale.ENGLISH} only
   * covered the values that reach {@code DateUtils.format/parse}. The same schema patterns were still fed straight to
   * {@code DateTimeFormatter.ofPattern()} - which binds {@code Locale.getDefault(Locale.Category.FORMAT)} - from
   * {@code DateUtils.parseDateTime}, from {@link Type}'s String conversions, from {@link JSONObject}'s date formats and from
   * the SQL/Cypher date functions. Two nodes with different JVM locales then disagreed on the textual form of a date,
   * and a value written by one failed to parse on the other.
   * <p>
   * Every pattern used here carries a textual field ({@code MMMM}, {@code EEEE}) and is unique to this class, so it is
   * built - and cached - for the first time under the Italian locale installed below. An unpinned formatter would
   * render {@code marzo}/{@code mercoledì} and fail to parse the English form.
   */
  @Nested
  @Isolated
  class SchemaDatePatternLocaleTest extends TestHelper {
    private static final String  DATE_PATTERN     = "dd MMMM yyyy";
    private static final String  DATETIME_PATTERN = "dd MMMM yyyy HH:mm:ss";
    private static final String  ENGLISH_DATE     = "04 March 2026";
    private static final String  ENGLISH_DATETIME = "04 March 2026 10:20:30";
    private              Locale  originalLocale;

    @BeforeEach
    void installHostileLocale() {
      originalLocale = Locale.getDefault(Locale.Category.FORMAT);
      Locale.setDefault(Locale.Category.FORMAT, Locale.ITALIAN);
    }

    @AfterEach
    void restoreLocale() {
      Locale.setDefault(Locale.Category.FORMAT, originalLocale);
    }

    @Override
    protected void beginTest() {
      database.getSchema().setDateFormat(DATE_PATTERN);
      database.getSchema().setDateTimeFormat(DATETIME_PATTERN);
    }

    /** The premise: an unpinned formatter really would answer in Italian under this locale. */
    @Test
    void localeIsActuallyHostile() {
      assertThat(DateTimeFormatter.ofPattern(DATE_PATTERN).format(LocalDate.of(2026, 3, 4)))
          .as("otherwise every assertion below would pass with or without the fix")
          .isNotEqualTo(ENGLISH_DATE);
    }

    @Test
    void typeConvertsStringToTemporalWithSchemaPatternInEnglish() {
      assertThat(Type.convert(database, ENGLISH_DATE, LocalDate.class)).isEqualTo(LocalDate.of(2026, 3, 4));
      assertThat(Type.convert(database, ENGLISH_DATETIME, LocalDateTime.class))
          .isEqualTo(LocalDateTime.of(2026, 3, 4, 10, 20, 30));
      // java.util.Date goes through the SimpleDateFormat twin of the same chain
      assertThat(Type.convert(database, ENGLISH_DATETIME, Date.class)).isNotNull();
    }

    @Test
    void jsonSerializesTheSchemaPatternInEnglish() {
      final JSONObject json = new JSONObject().setDateFormat(DATE_PATTERN).setDateTimeFormat(DATETIME_PATTERN);
      json.put("when", LocalDateTime.of(2026, 3, 4, 10, 20, 30));
      assertThat(json.getString("when")).isEqualTo(ENGLISH_DATETIME);
    }

    /**
     * {@code setDateFormat}'s own javadoc says "Null means using the timestamp", but the setter fed the value straight
     * to a formatter factory, and both {@code DateTimeFormatter.ofPattern(null)} and {@code DateUtils.getFormatter(null)}
     * throw {@link NullPointerException}, which the {@link IllegalArgumentException} catch never covered. The mode the
     * API documented could not actually be entered.
     */
    @Test
    void resettingTheDateFormatsToNullRestoresTimestampMode() {
      final JSONObject json = new JSONObject().setDateFormat(DATE_PATTERN).setDateTimeFormat(DATETIME_PATTERN);
      json.setDateFormat(null).setDateTimeFormat(null);
      json.put("when", LocalDateTime.of(2026, 3, 4, 10, 20, 30));
      assertThat(json.getLong("when"))
          .as("with no format the value is serialized as a timestamp")
          .isEqualTo(LocalDateTime.of(2026, 3, 4, 10, 20, 30).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    void sqlFormatFunctionsRenderInEnglish() {
      assertThat(new FormatFunction().execute(new Object[] { LocalDateTime.of(2026, 3, 4, 10, 20, 30), DATETIME_PATTERN }, null))
          .isEqualTo(ENGLISH_DATETIME);
      // date.format() renders in the machine's own time zone, so only the textual field is asserted here
      assertThat(new DateFormat().execute(new Object[] { 1772622030000L, "ms", DATE_PATTERN }, null).toString())
          .contains("March");
    }

    /** End to end: a record written with a textual schema pattern reads back the same under a foreign locale. */
    @Test
    void recordRoundTripsThroughTheSchemaPattern() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7144").createProperty("occurredAt", Type.DATETIME);
        // The String goes through Type.convert with the schema's own textual pattern
        database.newDocument("Issue7144").set("occurredAt", ENGLISH_DATETIME).save();
      });

      database.transaction(() -> {
        final Object readBack = database.query("sql", "select occurredAt from Issue7144").nextIfAvailable()
            .getProperty("occurredAt");
        assertThat(readBack).isEqualTo(LocalDateTime.of(2026, 3, 4, 10, 20, 30));
      });
    }
  }

  /**
   * Regression for #7788.
   * <p>
   * Regression test for issue #7788: {@code StringUtils.format()} (backing the SQL {@code .format()} method) called
   * {@code String.format(format, args)} with no {@link Locale}, so the decimal separator of every {@code %f}/{@code %e}
   * conversion came from the server JVM's default locale instead of being locale-independent like the sibling
   * {@code number.format()} function. A comma-decimal default locale turned {@code %e} output such as
   * {@code "1.234568e+06"} into {@code "1,234568e+06"}, which is no longer parseable as a number downstream.
   * <p>
   * Mutates the JVM default locale for the duration of the test and restores it afterward - tests in this module run
   * sequentially (no {@code junit.jupiter.execution.parallel.enabled}), so this is safe, but it is still process-wide
   * state and must never leak into another test.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LocaleIndependentFormatTest {

    private Locale originalDefault;

    @BeforeEach
    void saveDefaultLocale() {
      originalDefault = Locale.getDefault();
    }

    @AfterEach
    void restoreDefaultLocale() {
      Locale.setDefault(originalDefault);
    }

    @Test
    void stringUtilsFormatIgnoresTheJvmDefaultLocale() {
      Locale.setDefault(Locale.GERMANY);
      try {
        assertThat(StringUtils.format("format", "%,.2f", 1234567.891)).isEqualTo("1,234,567.89");
        assertThat(StringUtils.format("format", "%e", 1234567.891)).isEqualTo("1.234568e+06");
      } finally {
        Locale.setDefault(originalDefault);
      }
    }

    @Test
    void stringUtilsFormatIsConsistentAcrossLocales() {
      final String rootResult;
      Locale.setDefault(Locale.US);
      try {
        rootResult = StringUtils.format("format", "%,.2f", 1234567.891);
      } finally {
        Locale.setDefault(originalDefault);
      }

      Locale.setDefault(Locale.GERMANY);
      final String germanResult;
      try {
        germanResult = StringUtils.format("format", "%,.2f", 1234567.891);
      } finally {
        Locale.setDefault(originalDefault);
      }

      assertThat(germanResult).isEqualTo(rootResult);
    }
  }

  /**
   * Regression for #8216.
   * <p>
   * Issue #8216: every date/time conversion read a {@link Number} with {@link Number#longValue()}, which truncates a
   * fractional value toward zero (the LATER unit for a pre-epoch instant), maps {@code NaN} to the epoch and wraps a
   * {@link BigInteger} outside the {@code long} range. A fractional value is now floored (the unit containing the
   * instant) and a value that denotes no instant is refused.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FractionalTimestampTest extends TestHelper {

    @Test
    void fractionalNumberIsFlooredNotTruncated() {
      assertThat(DateUtils.dateTimeToTimestamp(1770000000000.9d, ChronoUnit.MILLIS)).isEqualTo(1770000000000L);
      assertThat(DateUtils.dateTimeToTimestamp(1770000000000.9f, ChronoUnit.MILLIS)).isEqualTo((long) Math.floor(1770000000000.9f));
      // PRE-EPOCH: -1.5 ms is an instant inside millisecond -2, truncation toward zero answered -1
      assertThat(DateUtils.dateTimeToTimestamp(-1.5d, ChronoUnit.MILLIS)).isEqualTo(-2L);
      assertThat(DateUtils.dateTimeToTimestamp(new BigDecimal("-1.5"), ChronoUnit.MILLIS)).isEqualTo(-2L);
      assertThat(DateUtils.dateTimeToTimestamp(new BigDecimal("1770000000000.9"), ChronoUnit.MILLIS)).isEqualTo(1770000000000L);
      // AN INTEGRAL VALUE IS UNCHANGED
      assertThat(DateUtils.dateTimeToTimestamp(-5.0d, ChronoUnit.MILLIS)).isEqualTo(-5L);
      assertThat(DateUtils.dateTimeToTimestamp(Long.MIN_VALUE, ChronoUnit.MILLIS)).isEqualTo(Long.MIN_VALUE);
      assertThat(DateUtils.dateTimeToTimestamp(BigInteger.valueOf(Long.MAX_VALUE), ChronoUnit.MILLIS)).isEqualTo(Long.MAX_VALUE);
      // A DATE IS EPOCH DAYS: -0.5 is the day before the epoch
      assertThat(DateUtils.dateToEpochDays(-0.5d)).isEqualTo(-1L);
    }

    @Test
    void valueDenotingNoInstantIsRefused() {
      for (final Number n : new Number[] { Double.NaN, Float.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1e19d,
          -1e19d, BigInteger.ONE.shiftLeft(64), new BigDecimal("1e30") })
        assertThatThrownBy(() -> DateUtils.dateTimeToTimestamp(n, ChronoUnit.MILLIS))
            .as("value %s", n).isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(() -> DateUtils.dateToEpochDays(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> DateUtils.format(Double.NaN, "yyyy-MM-dd")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void propertyCoercionFloorsAndRefuses() {
      final DocumentType type = database.getSchema().createDocumentType("Ev8216");
      type.createProperty("ts", Type.DATETIME);
      type.createProperty("d", Type.DATE);

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Ev8216");
        doc.set("ts", -1.5d);
        doc.set("d", 1.5d);
        doc.save();

        assertThat(doc.getLocalDateTime("ts")).isEqualTo(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).minusNanos(2_000_000));
        assertThat(doc.getLocalDate("d")).isEqualTo(LocalDate.ofEpochDay(1));

        assertThatThrownBy(() -> database.newDocument("Ev8216").set("ts", Double.NaN)).isInstanceOf(IllegalArgumentException.class);
      });
    }
  }
}
