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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.utility.TableFormatter;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7921: {@code EXPLAIN} and {@code PROFILE} formatted their cost and row counts with a
 * bare {@code new DecimalFormat()}, whose grouping separator follows the JVM default FORMAT locale. The same query
 * over the same data therefore printed {@code 1,234,567μs} on an en_US server and {@code 1.234.567μs} on a de_DE
 * one, so anything diffing or parsing two servers' plans saw a difference that is not a difference.
 * <p>
 * The same sweep covers two more default-locale formatters on output surfaces the issue names: {@code Result}'s
 * {@code database == null} branch, which rendered a {@code Date} into JSON with a pattern-less, locale-less
 * {@code SimpleDateFormat}, and {@code TableFormatter}'s date column, whose pattern was fixed but whose calendar
 * system and digits were not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7921PlanNumberLocaleTest extends TestHelper {

  /**
   * Both numbers a plan carries, at the one place that formats them. Driven directly rather than through a real
   * PROFILE because a measured cost is not reproducible, and a cost under 1000μs carries no grouping separator at
   * all - the very character under test.
   */
  @Test
  void aPlansCostAndRowCountAreFormattedTheSameWayUnderEveryLocale() {
    final Step step = new Step();
    step.cost = 1_234_567_000L; // nanoseconds: the plan prints microseconds
    step.rowCount = 1_234L;

    for (final Locale locale : new Locale[] { Locale.ROOT, Locale.US, Locale.GERMANY, Locale.ITALY,
        Locale.forLanguageTag("th-TH-u-nu-thai") }) {
      final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
      try {
        Locale.setDefault(Locale.Category.FORMAT, locale);
        assertThat(step.costFormatted()).as("cost under %s", locale).isEqualTo("1,234,567μs");
        assertThat(step.rowCountFormatted()).as("row count under %s", locale).isEqualTo("1,234 rows");
      } finally {
        Locale.setDefault(Locale.Category.FORMAT, previous);
      }
    }
  }

  /** And the same through a real PROFILE, so the wiring from the plan to these two methods is covered too. */
  @Test
  void aProfiledPlanNeverGroupsWithTheServersSeparator() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.begin();
    try {
      for (int i = 0; i < 1500; i++)
        database.command("sql", "INSERT INTO T SET n = " + i);
    } finally {
      database.commit();
    }

    final String underGerman = profileOf("SELECT FROM T", Locale.GERMANY);

    assertThat(underGerman).contains("μs");
    assertThat(underGerman.replaceAll("[0-9,]+μs", "<cost>"))
        .as("a de_DE server must not group a plan's cost with '.': %s", underGerman)
        .doesNotContain("μs");
  }

  /** Reaches the two protected formatters {@code prettyPrint()} implementations go through. */
  private static class Step extends AbstractExecutionStep {
    Step() {
      super(null);
    }

    String costFormatted() {
      return getCostFormatted();
    }

    String rowCountFormatted() {
      return getRowCountFormatted();
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A {@code Date} rendered into JSON by a {@code Result} with no database - {@code Result.valueToJSON}'s fallback
   * branch - used a {@code SimpleDateFormat} built with neither a pattern nor a locale, so it emitted the server's
   * default SHORT date/time: {@code 9/18/26, 3:04 PM} on one host, {@code 18.09.26, 15:04} on another, and neither
   * shape parseable by the format every other {@code Date} in the same answer uses.
   */
  @Test
  void aDateInAResultWithNoDatabaseIsRenderedWithTheProductPatternUnderEveryLocale() {
    final Date date = dateOf(2026, Calendar.SEPTEMBER, 18, 15, 4, 5);

    for (final Locale locale : new Locale[] { Locale.ROOT, Locale.US, Locale.GERMANY,
        Locale.forLanguageTag("th-TH-u-ca-buddhist-nu-thai") }) {
      final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
      try {
        Locale.setDefault(Locale.Category.FORMAT, locale);

        final ResultInternal result = new ResultInternal((Database) null);
        result.setProperty("d", date);

        assertThat(result.toJSON().getString("d")).as("under %s", locale).isEqualTo("2026-09-18 15:04:05");
      } finally {
        Locale.setDefault(Locale.Category.FORMAT, previous);
      }
    }
  }

  /**
   * A {@code Date} column in a printed table. The pattern was always explicit here, but the locale never was, and
   * the locale decides the calendar system and the digits - under {@code th-TH} the same instant carried a
   * Buddhist-era year in Thai digits. This pins the rendering rather than reproducing the old failure: the
   * formatter it replaced was a {@code static final}, so it froze whatever locale the JVM started in and a test
   * could not move it afterwards.
   */
  @Test
  void aDateColumnIsRenderedTheSameWayUnderEveryLocale() {
    final Date date = dateOf(2026, Calendar.SEPTEMBER, 18, 15, 4, 5);

    final String underRoot = prettyDate(date, Locale.ROOT);
    final String underThai = prettyDate(date, Locale.forLanguageTag("th-TH-u-ca-buddhist-nu-thai"));

    assertThat(underRoot).isEqualTo("2026-09-18 15:04:05.000");
    assertThat(underThai)
        .as("the server's locale must not decide the calendar system or the digits of a rendered date")
        .isEqualTo(underRoot);
  }

  private static Date dateOf(final int year, final int month, final int day, final int hour, final int minute,
      final int second) {
    final Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.US);
    calendar.clear();
    calendar.set(year, month, day, hour, minute, second);
    return calendar.getTime();
  }

  private String profileOf(final String query, final Locale locale) {
    final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      try (final ResultSet rs = database.query("sql", "PROFILE " + query)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    } finally {
      Locale.setDefault(Locale.Category.FORMAT, previous);
    }
  }

  private static String prettyDate(final Date date, final Locale locale) {
    final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      return String.valueOf(TableFormatter.getPrettyFieldValue(date, 10));
    } finally {
      Locale.setDefault(Locale.Category.FORMAT, previous);
    }
  }
}
