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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.NonFiniteNumbers;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class DateConversionTest {

  /**
   * Regression for #10104: #9976 guarded the one route the report named, {@code ALTER DATABASE}, at its call site, and the failure it
   * reported reproduced verbatim by the other routes that change the class a DATE or DATETIME value is held as. The held class is
   * decided by {@code arcadedb.dateImplementation} and {@code arcadedb.dateTimeImplementation}, which the context configuration
   * an open reads sets as well, and by the public setters of the serializer.
   * <p>
   * The guarantee is that every route either refuses or warns, naming the property: the statement refuses (a statement can be
   * refused), while the open and the setter, which cannot refuse (an open must not fail over a constraint), warn.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DateImplementationRoutes extends TestHelper {
    private static final String ISO_DATE = "[0-9]{4}-[0-9]{2}-[0-9]{2}";

    @Test
    void theStatementRouteRefuses() {
      declareIsoPattern();

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("R1.d");
      assertThat(serializer().getDateImplementation()).isEqualTo(LocalDate.class);
    }

    @Test
    void theContextConfigurationAtOpenWarnsNamingTheProperty() {
      declareIsoPattern();
      database.close();

      // what a -Darcadedb.dateImplementation=java.util.Date on the command line does: open() resets the context configuration
      // and reads the saved one, so the value that reaches an open is the one the setting itself holds
      GlobalConfiguration.DATE_IMPLEMENTATION.setValue(Date.class);
      try {
        final List<String> warnings = WarningCapture.captureWarnings(() -> database = factory.open());

        assertThat(database.isOpen()).as("an open must not fail over a constraint").isTrue();
        assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
        assertThat(warnings).as("the warnings the open logged").anyMatch(
            w -> w.contains("R1.d") && w.contains(ISO_DATE) && w.contains("REGEXP null"));

        // the symptom the warning announces: the property can no longer be written
        assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')")
            .close())).isInstanceOf(ValidationException.class).hasMessageContaining("R1.d");

        // and its remedy works
        database.command("sql", "ALTER PROPERTY R1.d REGEXP null");
        database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')").close());
      } finally {
        GlobalConfiguration.DATE_IMPLEMENTATION.reset();
      }
    }

    @Test
    void theDatetimeImplementationAtOpenWarnsToo() {
      database.command("sql", "CREATE VERTEX TYPE R2");
      database.command("sql", "CREATE PROPERTY R2.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
      database.close();

      GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(Date.class);
      try {
        final List<String> warnings = WarningCapture.captureWarnings(() -> database = factory.open());
        assertThat(warnings).anyMatch(w -> w.contains("R2.t"));
      } finally {
        GlobalConfiguration.DATE_TIME_IMPLEMENTATION.reset();
      }
    }

    @Test
    void thePublicSetterWarnsNamingTheProperty() {
      declareIsoPattern();

      final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateImplementation(Date.class));
      try {
        assertThat(warnings).anyMatch(w -> w.contains("R1.d") && w.contains(ISO_DATE));
      } finally {
        serializer().setDateImplementation(LocalDate.class);
      }

      // given by name, as the configuration holds it
      final List<String> byName = WarningCapture.captureWarnings(() -> serializer().setDateImplementation("java.util.Date"));
      try {
        assertThat(byName).anyMatch(w -> w.contains("R1.d"));
      } finally {
        serializer().setDateImplementation(LocalDate.class);
      }
    }

    @Test
    void theDatetimeSetterWarnsForTheTypesItDecidesOnly() {
      database.command("sql", "CREATE VERTEX TYPE R3");
      database.command("sql", "CREATE PROPERTY R3.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
      // a microsecond datetime stays a LocalDateTime when DATETIME becomes a java.util.Date, so its pattern is not in question
      database.command("sql", "CREATE PROPERTY R3.micros DATETIME_MICROS (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");

      final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateTimeImplementation(Date.class));
      try {
        assertThat(warnings).anyMatch(w -> w.contains("R3.t")).noneMatch(w -> w.contains("R3.micros"));
      } finally {
        serializer().setDateTimeImplementation(LocalDateTime.class);
      }
    }

    @Test
    void aPatternTheNewClassSatisfiesWarnsNothing() {
      database.command("sql", "CREATE VERTEX TYPE R4");
      database.command("sql", "CREATE PROPERTY R4.d DATE (regexp '.*20[0-9]{2}.*')");

      final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateImplementation(Date.class));
      try {
        assertThat(warnings).noneMatch(w -> w.contains("R4.d"));
      } finally {
        serializer().setDateImplementation(LocalDate.class);
      }
    }

    @Test
    void aSchemaWithoutADatePatternWarnsNothingWhateverTheClass() {
      database.command("sql", "CREATE VERTEX TYPE R5");
      database.command("sql", "CREATE PROPERTY R5.d DATE");
      database.command("sql", "CREATE PROPERTY R5.s STRING (regexp '[a-z]+')");

      final List<String> warnings = WarningCapture.captureWarnings(() -> {
        serializer().setDateImplementation(Date.class);
        serializer().setDateTimeImplementation(Date.class);
      });
      try {
        assertThat(warnings).noneMatch(w -> w.contains("R5."));
      } finally {
        serializer().setDateImplementation(LocalDate.class);
        serializer().setDateTimeImplementation(LocalDateTime.class);
      }
    }

    @Test
    void settingTheClassAlreadyInForceChangesNothingAndWarnsNothing() {
      declareIsoPattern();
      final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateImplementation(LocalDate.class));
      assertThat(warnings).noneMatch(w -> w.contains("R1.d"));
    }

    /** The class has already changed when the listener runs, so a listener that fails must not reach the caller of the setter. */
    @Test
    void aListenerThatFailsDoesNotBreakTheSetter() {
      final List<String> warnings = WarningCapture.captureWarnings(() -> {
        serializer().setImplementationChangeListener(() -> {
          throw new IllegalStateException("a listener that fails");
        });
        try {
          serializer().setDateImplementation(Date.class);
          assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
        } finally {
          serializer().setImplementationChangeListener(null);
          serializer().setDateImplementation(LocalDate.class);
        }
      });

      assertThat(warnings).as("the failure is reported, not swallowed silently").anyMatch(w -> w.contains("date implementation"));
    }

    private void declareIsoPattern() {
      database.command("sql", "CREATE VERTEX TYPE R1");
      database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '" + ISO_DATE + "')");
      database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-01-15','yyyy-MM-dd')").close());
    }

    private BinarySerializer serializer() {
      return ((DatabaseInternal) database).getSerializer();
    }
  }

  /**
   * Regression for #10489: the same instant, {@code 2020-01-02T03:04:05+02:00}, was stored as {@code 03:04:05} from a
   * String (an HTTP JSON value, a SQL literal or parameter) and as {@code 01:04:05} from an {@link OffsetDateTime} or a
   * {@link ZonedDateTime} (a Bolt DateTime with an offset, a Java value), so HTTP and Bolt wrote different DATETIME values for
   * one instant. A string now names the instant it carries and stores it as the UTC wall clock the typed values do.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DateTimeOffsetString extends TestHelper {

    private static final String           INSTANT = "2020-01-02T03:04:05+02:00";
    private static final LocalDateTime    UTC     = LocalDateTime.of(2020, 1, 2, 1, 4, 5);

    @Test
    void everyWayOfWritingTheSameInstantStoresTheSameValue() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.t DATETIME");

      database.transaction(() -> {
        database.newDocument("D").set("k", "java String").set("t", INSTANT).save();
        database.newDocument("D").set("k", "java OffsetDateTime").set("t", OffsetDateTime.parse(INSTANT)).save();
        database.newDocument("D").set("k", "java ZonedDateTime")
            .set("t", ZonedDateTime.parse("2020-01-02T03:04:05+02:00[Europe/Amsterdam]")).save();
        database.command("sql", "INSERT INTO D SET k = 'sql literal', t = '" + INSTANT + "'").close();
        database.command("sql", "INSERT INTO D SET k = 'sql String param', t = :t", Map.of("t", INSTANT)).close();
        database.command("sql", "INSERT INTO D SET k = 'sql OffsetDateTime param', t = :t", Map.of("t", OffsetDateTime.parse(INSTANT)))
            .close();
      });

      final Map<String, Object> stored = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT k, t FROM D")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          stored.put(row.getProperty("k"), row.getProperty("t"));
        }
      }

      assertThat(stored).hasSize(6);
      stored.forEach((k, v) -> assertThat(v).as(k).isEqualTo(UTC));
    }

    @Test
    void aRegionSuffixAndTheSpaceSeparatedSpellingNameTheSameInstant() {
      final DocumentType type = database.getSchema().createDocumentType("Spellings");
      type.createProperty("t", Type.DATETIME);

      database.transaction(() -> {
        for (final String literal : new String[] { "2020-07-02T03:04:05+02:00[Europe/Amsterdam]", "2020-07-02T03:04:05+02:00",
            "2020-07-02 03:04:05+02:00", "2020-07-02 03:04:05+02", "2020-07-02 03:04:05+0200" }) {
          final LocalDateTime stored = database.newDocument("Spellings").set("t", literal).save().getLocalDateTime("t");
          assertThat(stored).as(literal).isEqualTo(LocalDateTime.of(2020, 7, 2, 1, 4, 5));
        }
      });
    }

    /** A negative offset moves the wall clock forward, and a named zone applies the offset it has on that date (DST). */
    @Test
    void negativeOffsetsAndNamedZonesAcrossDstNameTheirInstant() {
      final DocumentType type = database.getSchema().createDocumentType("Negative");
      type.createProperty("t", Type.DATETIME);

      database.transaction(() -> {
        assertThat(database.newDocument("Negative").set("t", "2020-01-02T03:04:05-05:00").save().getLocalDateTime("t"))
            .isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
        // Amsterdam is +01:00 in January and +02:00 in July
        assertThat(database.newDocument("Negative").set("t", "2020-01-02T03:04:05+01:00[Europe/Amsterdam]").save()
            .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 1, 2, 2, 4, 5));
        assertThat(database.newDocument("Negative").set("t", "2020-07-02T03:04:05+02:00[Europe/Amsterdam]").save()
            .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 7, 2, 1, 4, 5));
        // New York is -05:00 in January and -04:00 in July
        assertThat(database.newDocument("Negative").set("t", "2020-01-02T03:04:05-05:00[America/New_York]").save()
            .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
        assertThat(database.newDocument("Negative").set("t", "2020-07-02T03:04:05-04:00[America/New_York]").save()
            .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 7, 2, 7, 4, 5));
      });

      // and what the read side answers for the same strings is what the write side stored
      assertThat(database.query("sql", "SELECT '2020-01-02T03:04:05-05:00'.asDatetime() AS d").next().<Object>getProperty("d"))
          .isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
      assertThat(database.query("sql", "SELECT date('2020-07-02T03:04:05-04:00[America/New_York]') AS d").next().<Object>getProperty("d"))
          .isEqualTo(LocalDateTime.of(2020, 7, 2, 7, 4, 5));
    }

    @Test
    void anOffsetNamingUtcAndAnInputWithoutOffsetKeepTheirWallClock() {
      final DocumentType type = database.getSchema().createDocumentType("Wall");
      type.createProperty("t", Type.DATETIME);

      final LocalDateTime wallClock = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
      database.transaction(() -> {
        // the Z that Cypher's datetime() renders (issue #4125), an explicit zero offset and no offset at all
        for (final String literal : new String[] { "2020-01-02T03:04:05Z", "2020-01-02T03:04:05+00:00", "2020-01-02T03:04:05",
            "2020-01-02 03:04:05" })
          assertThat(database.newDocument("Wall").set("t", literal).save().getLocalDateTime("t")).as(literal).isEqualTo(wallClock);
      });
    }

    @Test
    void theRuleDoesNotDependOnTheDatabaseZone() {
      final ZoneId original = database.getSchema().getZoneId();
      database.getSchema().setZoneId(ZoneId.of("America/Los_Angeles"));
      try {
        final DocumentType type = database.getSchema().createDocumentType("Zoned");
        type.createProperty("t", Type.DATETIME);

        database.transaction(() -> {
          assertThat(database.newDocument("Zoned").set("t", INSTANT).save().getLocalDateTime("t")).isEqualTo(UTC);
          assertThat(database.newDocument("Zoned").set("t", OffsetDateTime.parse(INSTANT)).save().getLocalDateTime("t"))
              .isEqualTo(UTC);
          assertThat(database.newDocument("Zoned").set("t", "2020-01-02T03:04:05Z").save().getLocalDateTime("t"))
              .isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
        });
      } finally {
        database.getSchema().setZoneId(original);
      }
    }

    @Test
    void theReadSideAnswersWhatTheWriteSideStores() {
      assertThat(Type.convert(database, INSTANT, LocalDateTime.class)).isEqualTo(UTC);
      assertThat(database.query("sql", "SELECT '" + INSTANT + "'.asDatetime() AS d").next().<Object>getProperty("d")).isEqualTo(UTC);
      assertThat(database.query("sql", "SELECT date('" + INSTANT + "') AS d").next().<Object>getProperty("d")).isEqualTo(UTC);
    }

    @Test
    void aDateStillReadsTheDayWritten() {
      // a LocalDate has no zone: the day written is the day meant, and nothing may shift it across midnight
      assertThat(Type.convert(database, "2020-01-02T23:30:00-05:00", LocalDate.class)).isEqualTo(LocalDate.of(2020, 1, 2));
    }
  }

  /**
   * Regression for #10910: under {@code arcadedb.dateImplementation=java.util.Date} (or Calendar, or LocalDateTime) a number written into a
   * DATE property was read as epoch milliseconds, while the default LocalDate implementation reads it as epoch days. The remote
   * client encodes a DATE as epoch days (#4601), so a date written through it was stored as 1970-01-01.
   */
  @Nested
  class DateNumberAsDays extends TestHelper {
    private static final LocalDate DAY        = LocalDate.of(2024, 1, 15);
    private static final long      DAY_MILLIS = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

    @AfterEach
    void restoreDefault() {
      serializer().setDateImplementation(LocalDate.class);
    }

    @Test
    void javaUtilDateReadsANumberAsDays() {
      serializer().setDateImplementation(Date.class);
      createType();

      assertThat(((Date) written(DAY.toEpochDay())).getTime()).isEqualTo(DAY_MILLIS);
      assertThat(((Date) written(String.valueOf(DAY.toEpochDay()))).getTime()).isEqualTo(DAY_MILLIS);
      assertThat(((Date) written((int) DAY.toEpochDay())).getTime()).isEqualTo(DAY_MILLIS);
      // the millis of a UTC midnight are that day, as for the LocalDate implementation (#10865)
      assertThat(((Date) written(DAY_MILLIS)).getTime()).isEqualTo(DAY_MILLIS);
      assertThat(((Date) written(0L)).getTime()).isZero();
      assertThat(((Date) written(-1L)).getTime()).isEqualTo(-86_400_000L);
    }

    @Test
    void javaUtilDateReadsANumberPastTheLargestDayItCanHoldAsMillis() {
      serializer().setDateImplementation(Date.class);
      createType();

      // no day count a java.util.Date can hold, so the milliseconds it is, as under every other implementation (issue #11025)
      assertThat(((Date) written(200_000_000_000L)).getTime()).isEqualTo(200_000_000_000L);
    }

    @Test
    void calendarAndListReadANumberPastTheLargestDayItCanHoldAsMillis() {
      serializer().setDateImplementation(Calendar.class);
      createType();
      database.transaction(() -> database.getSchema().getType("D").createProperty("ds", Type.LIST, "DATE"));

      assertThat(((Calendar) written(200_000_000_000L)).getTimeInMillis()).isEqualTo(200_000_000_000L);
      final Object list = inTx(() -> database.newDocument("D").set("ds", List.of(200_000_000_000L)).get("ds"));
      assertThat(((Calendar) ((List<?>) list).get(0)).getTimeInMillis()).isEqualTo(200_000_000_000L);
      // only milliseconds whose day no java.util.Date can hold are refused, under every implementation (issue #11025)
      assertThatThrownBy(() -> written(Long.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNegativeDayCountIsAPreEpochDay() {
      serializer().setDateImplementation(Date.class);
      createType();

      assertThat(((Date) written(-10L)).getTime()).isEqualTo(-10 * 86_400_000L);
    }

    @Test
    void aSmallNonMidnightMillisValueIsADayCount() {
      serializer().setDateImplementation(Date.class);
      createType();

      // the ambiguity window, as for the LocalDate implementation: a number below ~1.07e11 is days
      assertThat(((Date) written(1_000_000L)).getTime()).isEqualTo(1_000_000L * 86_400_000L);
    }

    @Test
    void calendarReadsANumberAsDays() {
      serializer().setDateImplementation(Calendar.class);
      createType();

      assertThat(((Calendar) written(DAY.toEpochDay())).getTimeInMillis()).isEqualTo(DAY_MILLIS);
    }

    @Test
    void localDateTimeReadsANumberAsDays() {
      serializer().setDateImplementation(LocalDateTime.class);
      createType();

      assertThat(written(DAY.toEpochDay())).isEqualTo(DAY.atStartOfDay());
      assertThat(written(DAY_MILLIS)).isEqualTo(DAY.atStartOfDay());
    }

    @Test
    void aNumberThatIsNoDayCountKeepsBeingReadAsMillis() {
      serializer().setDateImplementation(Date.class);
      createType();

      // a "now" in milliseconds (not a UTC midnight) was accepted as milliseconds before and still is
      assertThat(((Date) written(1_705_312_345_678L)).getTime()).isEqualTo(1_705_312_345_678L);
    }

    @Test
    void localDateKeepsReadingANumberAsDays() {
      createType();

      assertThat(written(DAY.toEpochDay())).isEqualTo(DAY);
      assertThat(written(DAY_MILLIS)).isEqualTo(DAY);
    }

    @Test
    void aDatetimeStillReadsANumberAsMillis() {
      serializer().setDateImplementation(Date.class);
      serializer().setDateTimeImplementation(Date.class);
      createType();

      final Object value = inTx(() -> database.newDocument("D").set("t", 1_705_276_800_000L).get("t"));
      assertThat(((Date) value).getTime()).isEqualTo(1_705_276_800_000L);
    }

    @Test
    void listOfDateReadsANumberAsDays() {
      serializer().setDateImplementation(Date.class);
      createType();
      database.transaction(() -> database.getSchema().getType("D").createProperty("ds", Type.LIST, "DATE"));

      final Object value = inTx(() -> database.newDocument("D").set("ds", List.of(DAY.toEpochDay())).get("ds"));
      assertThat(((List<?>) value).get(0)).isEqualTo(new Date(DAY_MILLIS));
    }

    @Test
    void sqlInsertOfADayCountThroughTheStatement() {
      serializer().setDateImplementation(Date.class);
      createType();

      database.transaction(() -> database.command("sql", "INSERT INTO D SET d = ?", DAY.toEpochDay()));
      final Object stored = database.query("sql", "SELECT d FROM D").next().getProperty("d");
      assertThat(((Date) stored).getTime()).isEqualTo(DAY_MILLIS);
    }

    private void createType() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("D");
        type.createProperty("d", Type.DATE);
        type.createProperty("t", Type.DATETIME);
      });
    }

    private Object written(final Object value) {
      return inTx(() -> {
        final MutableDocument doc = database.newDocument("D").set("d", value);
        return doc.get("d");
      });
    }

    private Object inTx(final Supplier<Object> supplier) {
      final Object[] result = new Object[1];
      database.transaction(() -> result[0] = supplier.get());
      return result[0];
    }

    private BinarySerializer serializer() {
      return ((DatabaseInternal) database).getSerializer();
    }
  }

  /**
   * Regression for #11025: a number written into a DATE property was read differently by the four classes a DATE can be held as. Under
   * {@code java.util.Date} and {@code Calendar} a number between ~1.07e11 and ~3.65e11 was a day count too large for a millisecond
   * {@code long} and was refused, while the default {@code LocalDate} stored it as a day hundreds of millions of years away; and a
   * "now" in milliseconds was stored by {@code java.util.Date} / {@code Calendar} / {@code LocalDateTime} and refused by
   * {@code LocalDate}. For one DATE property and one number the day must not depend on {@code arcadedb.dateImplementation}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DateNumberImplementationsAgree extends TestHelper {
    private static final List<Class<?>> IMPLEMENTATIONS = List.of(LocalDate.class, Date.class, Calendar.class, LocalDateTime.class);

    private static final long MAX_DAY_COUNT = Long.MAX_VALUE / DateUtils.MS_IN_A_DAY;
    private static final long NOW_MILLIS    = 1_705_312_345_678L;

    // the day-count window, the millis window, both edges of each and the band of the issue
    private static final long[] NUMBERS = { 0L, 1L, -1L, 19_737L, 1_000_000L, 1_705_276_800_000L, NOW_MILLIS, -NOW_MILLIS,
        MAX_DAY_COUNT, MAX_DAY_COUNT + 1, -MAX_DAY_COUNT, -MAX_DAY_COUNT - 1, 110_000_000_000L, 150_000_000_000L, 200_000_000_000L,
        LocalDate.MAX.toEpochDay(), LocalDate.MAX.toEpochDay() + 1, Long.MAX_VALUE / 2, Long.MAX_VALUE, -(Long.MAX_VALUE / 2) };

    @AfterEach
    void restoreDefault() {
      serializer().setDateImplementation(LocalDate.class);
    }

    @Test
    void everyImplementationHoldsTheSameDayForTheSameNumber() {
      createType();
      for (final long number : NUMBERS) {
        final Map<Class<?>, Long> days = new LinkedHashMap<>();
        for (final Class<?> implementation : IMPLEMENTATIONS) {
          serializer().setDateImplementation(implementation);
          final Object written = inTx(() -> database.newDocument("D").set("d", number).get("d"));
          assertThat(written).as("%s under %s", number, implementation.getSimpleName()).isInstanceOf(implementation);
          days.put(implementation, DateUtils.dateToEpochDays(written));
        }
        assertThat(days.values().stream().distinct().count()).as("days of %s by implementation: %s", number, days).isEqualTo(1L);
      }
    }

    @Test
    void everyImplementationStoresAndReadsBackTheSameDay() {
      createType();
      for (final long number : NUMBERS) {
        final List<Long> days = new ArrayList<>();
        for (final Class<?> implementation : IMPLEMENTATIONS) {
          serializer().setDateImplementation(implementation);
          final RID[] rid = new RID[1];
          database.transaction(() -> rid[0] = database.newDocument("D").set("d", number).save().getIdentity());
          days.add(DateUtils.dateToEpochDays(database.lookupByRID(rid[0], true).asDocument().get("d")));
        }
        assertThat(days.stream().distinct().count()).as("stored days of %s: %s", number, days).isEqualTo(1L);
      }
    }

    @Test
    void aNumberPastTheLargestDayCountIsTheMillisecondsItIs() {
      createType();
      for (final Class<?> implementation : IMPLEMENTATIONS) {
        serializer().setDateImplementation(implementation);
        // the band of the issue: 1976-05-03 as milliseconds, refused by java.util.Date before, a day of the year 547 million
        // under LocalDate
        assertThat(dayOf(200_000_000_000L)).as(implementation.getSimpleName()).isEqualTo(LocalDate.of(1976, 5, 3));
        // a "now" in milliseconds, refused by LocalDate before
        assertThat(dayOf(NOW_MILLIS)).as(implementation.getSimpleName()).isEqualTo(LocalDate.of(2024, 1, 15));
        // the largest day count is still a day count
        assertThat(dayOf(MAX_DAY_COUNT).toEpochDay()).as(implementation.getSimpleName()).isEqualTo(MAX_DAY_COUNT);
        // a small number is a day count, as it always was
        assertThat(dayOf(19_737L)).as(implementation.getSimpleName()).isEqualTo(LocalDate.of(2024, 1, 15));
      }
    }

    @Test
    void aListOfDateAgreesWithTheProperty() {
      createType();
      database.transaction(() -> database.getSchema().getType("D").createProperty("ds", Type.LIST, "DATE"));
      for (final Class<?> implementation : IMPLEMENTATIONS) {
        serializer().setDateImplementation(implementation);
        final Object list = inTx(() -> database.newDocument("D").set("ds", List.of(200_000_000_000L, NOW_MILLIS, 19_737L)).get("ds"));
        assertThat(((List<?>) list).stream().map(DateUtils::dateToEpochDays).map(LocalDate::ofEpochDay).toList())
            .as(implementation.getSimpleName())
            .containsExactly(LocalDate.of(1976, 5, 3), LocalDate.of(2024, 1, 15), LocalDate.of(2024, 1, 15));
      }
    }

    @Test
    void aNumberWhoseMillisecondsAreNoDayOfAnImplementationIsRefusedByAll() {
      createType();
      // Long.MIN_VALUE as milliseconds is a day before the first one a java.util.Date can hold: every implementation refuses it,
      // so none stores what another refuses
      for (final Class<?> implementation : IMPLEMENTATIONS) {
        serializer().setDateImplementation(implementation);
        assertThatThrownBy(() -> inTx(() -> database.newDocument("D").set("d", Long.MIN_VALUE).get("d"))).as(
            implementation.getSimpleName()).isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void theReadingOfANumberAtTheEdgesOfTheDayCountWindow() {
      assertThat(DateUtils.dateNumberToEpochDays(MAX_DAY_COUNT)).isEqualTo(MAX_DAY_COUNT);
      assertThat(DateUtils.dateNumberToEpochDays(-MAX_DAY_COUNT)).isEqualTo(-MAX_DAY_COUNT);
      assertThat(DateUtils.dateNumberToEpochDays(MAX_DAY_COUNT + 1)).isEqualTo(Math.floorDiv(MAX_DAY_COUNT + 1, DateUtils.MS_IN_A_DAY));
      assertThat(DateUtils.dateNumberToEpochDays(-MAX_DAY_COUNT - 1)).isEqualTo(Math.floorDiv(-MAX_DAY_COUNT - 1, DateUtils.MS_IN_A_DAY));
      assertThat(DateUtils.dateNumberToEpochDays(Long.MAX_VALUE)).isEqualTo(MAX_DAY_COUNT);
      // every answer is a day a java.util.Date can hold as milliseconds
      for (final long number : NUMBERS)
        assertThat(Math.abs(DateUtils.dateNumberToEpochDays(number))).as("%s", number).isLessThanOrEqualTo(MAX_DAY_COUNT);
      assertThatThrownBy(() -> DateUtils.dateNumberToEpochDays(Long.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aComparisonKeepsReadingANumberAsDays() {
      // no property behind the conversion: a number is a count of days, so a number never equals a date (#8913)
      assertThat(Type.convert(database, 19_737L, LocalDate.class)).isEqualTo(LocalDate.of(2024, 1, 15));
      assertThat(Type.convert(database, 200_000_000L, LocalDate.class)).isEqualTo(LocalDate.ofEpochDay(200_000_000L));
    }

    private LocalDate dayOf(final long number) {
      return LocalDate.ofEpochDay(DateUtils.dateToEpochDays(inTx(() -> database.newDocument("D").set("d", number).get("d"))));
    }

    private void createType() {
      database.transaction(() -> database.getSchema().createDocumentType("D").createProperty("d", Type.DATE));
    }

    private Object inTx(final Supplier<Object> supplier) {
      final Object[] result = new Object[1];
      database.transaction(() -> result[0] = supplier.get());
      return result[0];
    }

    private BinarySerializer serializer() {
      return ((DatabaseInternal) database).getSerializer();
    }
  }

  /**
   * Regression for #10899: the server writes a non-finite FLOAT/DOUBLE as one of the {@link NonFiniteNumbers} markers, and
   * {@link Type#convertOrKeep} - the conversion the remote client reads every typed value through - must turn the marker
   * back into the number instead of handing it to {@code Float.parseFloat}, which refuses {@code PosInfinity}.
   */
  @Nested
  class NonFiniteMarkerConversion {

    @Test
    void floatMarkersDecodeToFloat() {
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, Float.class)).isEqualTo(Float.POSITIVE_INFINITY);
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NEGATIVE_INFINITY, Float.class)).isEqualTo(Float.NEGATIVE_INFINITY);
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NAN, Float.class)).isEqualTo(Float.NaN);
    }

    @Test
    void doubleMarkersDecodeToDouble() {
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, Double.class)).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NEGATIVE_INFINITY, Double.class)).isEqualTo(Double.NEGATIVE_INFINITY);
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NAN, Double.class)).isEqualTo(Double.NaN);
    }

    @Test
    void primitiveTargetsDecodeToo() {
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, float.class)).isEqualTo(Float.POSITIVE_INFINITY);
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NEGATIVE_INFINITY, double.class)).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void markerForANonFloatingTargetIsNotDecoded() {
      // a STRING column that happens to hold the text stays the text
      assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, String.class)).isEqualTo(NonFiniteNumbers.POSITIVE_INFINITY);
    }

    @Test
    void finiteNumbersAreUnaffected() {
      assertThat(Type.convertOrKeep(null, "1.5", Float.class)).isEqualTo(1.5F);
      assertThat(Type.convertOrKeep(null, 2.5D, Float.class)).isEqualTo(2.5F);
      assertThat(Type.convertOrKeep(null, "2.25", Double.class)).isEqualTo(2.25D);
    }
  }

  /**
   * Regression for #11389: pgjdbc sends a {@code java.sql.Date} as untyped text with a zone suffix ({@code '2024-05-06 +01'}). PostgreSQL
   * reads it as a date and ignores the zone; the engine refused it for a DATE property and matched nothing in a predicate.
   */
  @Nested
  class DateWithZoneSuffix extends TestHelper {

    @Test
    void insertOfDateWithZoneSuffixKeepsTheDay() {
      database.getSchema().createDocumentType("D11389").createProperty("d", Type.DATE);
      for (final String text : new String[] { "2024-05-06 +01", "2024-05-06 +01:00", "2024-05-06 +0100", "2024-05-06 -08", "2024-05-06 Z" })
        database.transaction(() -> database.newDocument("D11389").set("d", text).save());

      try (final ResultSet rs = database.query("sql", "SELECT FROM D11389")) {
        int count = 0;
        while (rs.hasNext()) {
          assertThat((Object) rs.next().getProperty("d")).isEqualTo(LocalDate.of(2024, 5, 6));
          count++;
        }
        assertThat(count).isEqualTo(5);
      }
    }

    @Test
    void predicatesWithZoneSuffixMatchTheDay() {
      database.getSchema().createDocumentType("W11389").createProperty("d", Type.DATE);
      database.transaction(() -> database.command("sql", "INSERT INTO W11389 SET d = '2024-05-06'"));

      assertCount("d = ?", 1);
      assertCount("d >= ?", 1);
      assertCount("d <= ?", 1);
      assertCount("d < ?", 0);
      assertCount("d > ?", 0);
    }

    @Test
    void dateTimeStillReadsATimeAndNowReadsADateWithAnOffsetAsMidnightAtThatOffset() {
      database.getSchema().createDocumentType("T11389").createProperty("t", Type.DATETIME_SECOND);
      database.transaction(() -> {
        database.newDocument("T11389").set("t", "2024-05-06 10:00:00").save();
        database.newDocument("T11389").set("t", "2024-05-06 +01").save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT FROM T11389 ORDER BY t")) {
        assertThat(rs.next().<Object>getProperty("t").toString()).isEqualTo("2024-05-05T23:00");
        assertThat(rs.next().<Object>getProperty("t").toString()).isEqualTo("2024-05-06T10:00");
      }
    }

    private void assertCount(final String where, final int expected) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM W11389 WHERE " + where, "2024-05-06 +01")) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).as(where).isEqualTo(expected);
      }
    }
  }
}
