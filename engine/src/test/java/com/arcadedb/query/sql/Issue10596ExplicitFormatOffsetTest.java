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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.function.date.DateFields;
import com.arcadedb.function.date.DateParse;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10596: {@code asDateTime(format)} and {@code date(value, format)} kept the wall clock of an offset-bearing string, so the
 * same instant with a {@code +02:00} offset was stored as 03:04:05 from them and as 01:04:05 from the schema's own pattern, a bound
 * parameter or an {@code OffsetDateTime} (issues #10489, #10498). The rule is one: the instant a text names is stored as the UTC wall
 * clock. A caller that named a pattern did not reach the chain that applies it - {@code DateUtils.parse(text, format)} was a
 * one-line {@code LocalDateTime.parse}, which takes the LOCAL fields of the parse and discards the offset it just read - and the
 * {@code timezone} argument of {@code date(value, format, timezone)} was inert there for the same reason.
 * <p>
 * {@code asDate(format)} keeps the wall clock: its target has no zone to keep, and the day written is the day meant. The census at
 * the end makes a text-to-datetime parse with a pattern unreachable outside {@code DateUtils}, so that every conversion has to answer
 * what becomes of the offset.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10596ExplicitFormatOffsetTest extends TestHelper {

  private static final String         ISO_PATTERN    = "yyyy-MM-dd'T'HH:mm:ssXXX";
  private static final String         SCHEMA_PATTERN = "yyyy/MM/dd HH:mm:ss XXX";
  private static final LocalDateTime  UTC_WALL_CLOCK = LocalDateTime.of(2020, 1, 2, 1, 4, 5);
  private static final List<ZoneId>   DATABASE_ZONES = List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"));

  private Object scalar(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("r");
    }
  }

  @Test
  void anExplicitPatternStoresTheInstantTheTextNames() {
    for (final ZoneId zone : DATABASE_ZONES) {
      database.getSchema().setZoneId(zone);

      assertThat(scalar("select '2020-01-02T03:04:05+02:00'.asDateTime() as r")).as("no format, zone " + zone).isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select '2020-01-02T03:04:05+02:00'.asDateTime(\"" + ISO_PATTERN + "\") as r")).as("asDateTime(format), zone " + zone)
          .isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select date('2020-01-02T03:04:05+02:00') as r")).as("date(value), zone " + zone).isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select date('2020-01-02T03:04:05+02:00', \"" + ISO_PATTERN + "\") as r")).as("date(value, format), zone " + zone)
          .isEqualTo(UTC_WALL_CLOCK);
    }
  }

  @Test
  void theSamePatternGivesTheSameInstantWhetherTheSchemaAppliesItOrTheCallerNamesIt() {
    database.command("sql", "alter database dateTimeFormat \"" + SCHEMA_PATTERN + "\"").close();
    for (final ZoneId zone : DATABASE_ZONES) {
      database.getSchema().setZoneId(zone);
      final String text = "2020/01/02 03:04:05 +02:00";

      assertThat(scalar("select '" + text + "'.asDateTime() as r")).as("the schema's pattern, zone " + zone).isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select '" + text + "'.asDateTime('" + SCHEMA_PATTERN + "') as r")).as("the same pattern by name, zone " + zone)
          .isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select date('" + text + "') as r")).isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select date('" + text + "', '" + SCHEMA_PATTERN + "') as r")).isEqualTo(UTC_WALL_CLOCK);
    }
  }

  @Test
  void aWriteStoresTheSameValueWhetherThePatternIsNamedOrNot() {
    database.command("sql", "alter database dateTimeFormat \"" + SCHEMA_PATTERN + "\"").close();
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.ts DATETIME").close();
    database.transaction(() -> {
      database.command("sql", "insert into T set k = 'plain', ts = '2020/01/02 03:04:05 +02:00'").close();
      database.command("sql", "insert into T set k = 'named', ts = '2020/01/02 03:04:05 +02:00'.asDateTime('" + SCHEMA_PATTERN + "')").close();
    });

    for (final String key : List.of("plain", "named"))
      assertThat(scalar("select ts as r from T where k = '" + key + "'")).as(key).isEqualTo(UTC_WALL_CLOCK);
  }

  @Test
  void theTimezoneArgumentDoesNotChangeTheInstantATextNames() {
    final String text = "2020-01-02T03:04:05+02:00";
    for (final String zone : List.of("UTC", "Asia/Tokyo", "America/New_York"))
      assertThat(scalar("select date('" + text + "', \"" + ISO_PATTERN + "\", '" + zone + "') as r")).as(zone).isEqualTo(UTC_WALL_CLOCK);
    assertThat(scalar("select date('" + text + "', { format: \"" + ISO_PATTERN + "\", timezone: 'Asia/Tokyo' }) as r")).isEqualTo(UTC_WALL_CLOCK);
  }

  @Test
  void theTimezoneArgumentIsTheZoneATextThatNamesNoneIsReadIn() {
    final String pattern = "yyyy-MM-dd HH:mm:ss";
    // no zone named: the wall clock is kept, as the chain without a pattern keeps it - the database's zone plays no part in it
    for (final ZoneId zone : DATABASE_ZONES) {
      database.getSchema().setZoneId(zone);
      assertThat(scalar("select date('2020-01-02 03:04:05', '" + pattern + "') as r")).as("zone " + zone)
          .isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
    }
    // the zone named is where the text was written: 03:04:05 in Tokyo (+09:00) is 18:04:05 of the day before in UTC
    assertThat(scalar("select date('2020-01-02 03:04:05', '" + pattern + "', 'Asia/Tokyo') as r")).isEqualTo(LocalDateTime.of(2020, 1, 1, 18, 4, 5));
    assertThat(scalar("select date('2020-01-02 03:04:05', { format: '" + pattern + "', timezone: 'Asia/Tokyo' }) as r"))
        .isEqualTo(LocalDateTime.of(2020, 1, 1, 18, 4, 5));
    assertThat(scalar("select date('2020-01-02 03:04:05', '" + pattern + "', 'UTC') as r")).isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
  }

  @Test
  void aTextWithoutAnOffsetKeepsItsWallClock() {
    assertThat(scalar("select '2020-01-02 03:04:05'.asDateTime('yyyy-MM-dd HH:mm:ss') as r")).isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
    assertThat(scalar("select '2020-01-02'.asDateTime('yyyy-MM-dd') as r")).isEqualTo(LocalDateTime.of(2020, 1, 2, 0, 0, 0));
    assertThat(scalar("select '2020-01-02T03:04:05Z'.asDateTime(\"yyyy-MM-dd'T'HH:mm:ssX\") as r")).as("Z is UTC: the same wall clock")
        .isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
  }

  @Test
  void aPatternWithAZoneRegionConvertsToo() {
    assertThat(scalar("select '2020-01-02 03:04:05 Europe/Rome'.asDateTime('yyyy-MM-dd HH:mm:ss VV') as r")).isEqualTo(
        LocalDateTime.of(2020, 1, 2, 2, 4, 5));
    assertThat(scalar("select '2020-07-02 03:04:05 Europe/Rome'.asDateTime('yyyy-MM-dd HH:mm:ss VV') as r")).as("summer time, +02:00")
        .isEqualTo(LocalDateTime.of(2020, 7, 2, 1, 4, 5));
  }

  @Test
  void aTextThatDoesNotMatchThePatternAnswersNull() {
    assertThat(scalar("select 'not a date'.asDateTime(\"" + ISO_PATTERN + "\") as r")).isNull();
    assertThat(scalar("select date('not a date', \"" + ISO_PATTERN + "\") as r")).isNull();
    // a pattern with no date at all cannot make a datetime: the same refusal as before, not an exception
    assertThat(scalar("select '12'.asDateTime('MM') as r")).isNull();
  }

  @Test
  void asDateKeepsTheWallClockSoTheDayWrittenIsTheDayMeant() {
    // 2020-01-02T01:04:05+02:00 is 2020-01-01T23:04:05Z: converting to UTC would answer the day before the one the text names
    final Object day = scalar("select '2020-01-02T01:04:05+02:00'.asDate(\"" + ISO_PATTERN + "\") as r");
    assertThat(day).isEqualTo(LocalDate.of(2020, 1, 2));
    assertThat(scalar("select '2020-01-02T01:04:05+02:00'.asDate() as r")).as("the same without a pattern").isEqualTo(day);
  }

  @Test
  void theUtilityNamesWhatBecomesOfTheOffset() {
    final String text = "2020-01-02T03:04:05+02:00";
    assertThat(DateUtils.parseDateTimeWithFormat(text, ISO_PATTERN, null)).isEqualTo(UTC_WALL_CLOCK);
    assertThat(DateUtils.parseDateTimeKeepingWallClockWithFormat(text, ISO_PATTERN)).isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
    assertThat(DateUtils.parseDateTimeWithFormat("2020-01-02 03:04:05", "yyyy-MM-dd HH:mm:ss", ZoneId.of("Asia/Tokyo")))
        .isEqualTo(LocalDateTime.of(2020, 1, 1, 18, 4, 5));
    assertThatThrownBy(() -> DateUtils.parseDateTimeWithFormat("nope", ISO_PATTERN, null)).isInstanceOf(DateTimeParseException.class);
  }

  /** The function that answers an epoch for a text read with a pattern read the wall clock in the JVM's zone, and dropped the offset. */
  @Test
  void dateParseAnswersTheInstantTheTextNames() {
    final TimeZone previous = TimeZone.getDefault();
    try {
      for (final String jvmZone : List.of("UTC", "Asia/Tokyo", "America/New_York")) {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
        final Object epoch = new DateParse().execute(new Object[] { "2020-01-02T03:04:05+02:00", "ms", ISO_PATTERN }, null);
        assertThat(epoch).as("JVM zone " + jvmZone).isEqualTo(Instant.parse("2020-01-02T01:04:05Z").toEpochMilli());
      }
      // a text without an offset is read in the JVM's zone, as it always was
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
      assertThat(new DateParse().execute(new Object[] { "2020-01-02 03:04:05", "ms", "yyyy-MM-dd HH:mm:ss" }, null)).isEqualTo(
          Instant.parse("2020-01-01T18:04:05Z").toEpochMilli());
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void dateFieldsAreTheFieldsOfTheInstantTheTextNames() {
    final Map<String, Object> ownOffset = (Map<String, Object>) new DateFields().execute(
        new Object[] { "2020-01-02T03:04:05+02:00", ISO_PATTERN }, null);
    assertThat(ownOffset.get("hour")).as("the text's own offset when no zone is asked for").isEqualTo(3L);
    assertThat(ownOffset.get("timezone")).isEqualTo("+02:00");

    final Map<String, Object> inUtc = (Map<String, Object>) new DateFields().execute(
        new Object[] { "2020-01-02T03:04:05+02:00", ISO_PATTERN, "UTC" }, null);
    assertThat(inUtc.get("hour")).as("the same instant expressed in the zone asked for").isEqualTo(1L);
    assertThat(inUtc.get("timezone")).isEqualTo("UTC");

    final Map<String, Object> zoneLess = (Map<String, Object>) new DateFields().execute(
        new Object[] { "2020-01-02 03:04:05", "yyyy-MM-dd HH:mm:ss", "Asia/Tokyo" }, null);
    assertThat(zoneLess.get("hour")).as("a text with no zone is read in the zone asked for").isEqualTo(3L);
    assertThat(zoneLess.get("timezone")).isEqualTo("Asia/Tokyo");
  }

  // ---- the guard: no text-to-datetime conversion with a pattern outside DateUtils

  /**
   * {@code LocalDateTime.parse(text, formatter)} takes the LOCAL fields of the parse and discards an offset the pattern read, so a
   * second parse written next to {@code DateUtils}' own answers a different instant for the same text - which is how three of them
   * got there. Every text-to-datetime conversion with a pattern goes through {@code DateUtils}, whose methods name what becomes of the
   * offset. The parses listed here read a value no user writes.
   */
  @Test
  void noSourceParsesADatetimeWithAPatternOutsideDateUtils() throws IOException {
    // file name -> why it is not a user's text
    final Map<String, String> readsNoUserText = Map.of("BackupCoordinator.java",
        "reads the timestamp the engine itself put in the name of an archive, no zone in it");

    final List<String> offenders = new ArrayList<>();
    final Pattern call = Pattern.compile("(?<![\\w.])LocalDateTime\\.parse\\(");
    try (final Stream<Path> modules = Files.list(Path.of(".."))) {
      for (final Path module : (Iterable<Path>) modules.sorted()::iterator) {
        final Path sources = module.resolve("src/main/java");
        if (!Files.isDirectory(sources))
          continue;
        try (final Stream<Path> files = Files.walk(sources)) {
          for (final Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".java"))::iterator) {
            final String name = file.getFileName().toString();
            if (name.equals("DateUtils.java") || readsNoUserText.containsKey(name))
              continue;
            final String source = Files.readString(file);
            final Matcher matcher = call.matcher(source);
            while (matcher.find())
              if (hasTopLevelComma(source, matcher.end()))
                offenders.add(file + ":" + lineOf(source, matcher.start()));
          }
        }
      }
    }
    assertThat(offenders).as("LocalDateTime.parse(text, pattern) outside DateUtils: use DateUtils.parseDateTimeWithFormat, or "
        + "parseDateTimeKeepingWallClockWithFormat when the target keeps no zone").isEmpty();
  }

  /** True when the arguments that start at {@code from} (just after the opening parenthesis) have more than one. */
  private static boolean hasTopLevelComma(final String source, final int from) {
    int depth = 1;
    for (int i = from; i < source.length() && depth > 0; i++) {
      final char c = source.charAt(i);
      if (c == '(')
        depth++;
      else if (c == ')')
        depth--;
      else if (c == ',' && depth == 1)
        return true;
    }
    return false;
  }

  private static int lineOf(final String source, final int offset) {
    int line = 1;
    for (int i = 0; i < offset; i++)
      if (source.charAt(i) == '\n')
        line++;
    return line;
  }
}
