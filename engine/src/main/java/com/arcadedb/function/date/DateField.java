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
package com.arcadedb.function.date;

import com.arcadedb.query.sql.executor.CommandContext;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.WeekFields;
import java.util.Locale;

/**
 * date.field(time, unit = 'd', timezone = 'UTC') - A field of a timestamp in milliseconds, read in {@code timezone}, as
 * APOC's apoc.date.field answers it (issue #12106). APOC's unit names: {@code ms}/{@code milli}/{@code millis}/
 * {@code milliseconds}, {@code s}/{@code second}/{@code seconds}, {@code m}/{@code minute}/{@code minutes},
 * {@code h}/{@code hour}/{@code hours}, {@code d}/{@code day}/{@code days} (day of the month), {@code w}/{@code weekday}/
 * {@code weekdays} (1 = Monday), {@code month}/{@code months}, {@code year}/{@code years}. ArcadeDB also accepts
 * {@code millisecond}, {@code dayOfMonth}, {@code dayOfWeek}, {@code dayOfYear}, {@code week}/{@code weekOfYear} (ISO-8601
 * week number) and {@code weekYear}/{@code weekBasedYear} (ISO-8601 week-based year).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateField extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "field";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Extract a field from a timestamp: date.field(time, unit = 'd', timezone = 'UTC') with unit ms, s, m, h, d, w "
        + "(weekday), month or year, also dayOfYear, week (ISO-8601 week number, alias weekOfYear) or weekYear (ISO-8601 week-based year)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long timestamp = toMillis(args[0]);
    final Object unit = args.length > 1 ? args[1] : "d";
    final Object timezone = args.length > 2 ? args[2] : UTC;
    if (unit == null)
      throw new IllegalArgumentException("Unknown date field: null");
    if (timezone == null)
      throw new IllegalArgumentException("Invalid timezone ID: null");

    final ZoneId zoneId;
    try {
      zoneId = ZoneId.of(timezone.toString());
    } catch (final DateTimeException e) {
      throw new IllegalArgumentException("Invalid timezone ID: " + timezone, e);
    }

    final ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(zoneId);
    final String field = unit.toString().toLowerCase(Locale.ROOT);
    return switch (field) {
      case "year", "years" -> (long) dateTime.getYear();
      case "month", "months" -> (long) dateTime.getMonthValue();
      case "d", "day", "days", "dayofmonth" -> (long) dateTime.getDayOfMonth();
      case "h", "hour", "hours" -> (long) dateTime.getHour();
      case "m", "minute", "minutes" -> (long) dateTime.getMinute();
      case "s", "second", "seconds" -> (long) dateTime.getSecond();
      case "ms", "milli", "millis", "millisecond", "milliseconds" -> (long) (dateTime.getNano() / 1_000_000);
      case "w", "weekday", "weekdays", "dayofweek" -> (long) dateTime.getDayOfWeek().getValue();
      case "dayofyear" -> (long) dateTime.getDayOfYear();
      case "week", "weekofyear" -> (long) dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());
      case "weekyear", "weekbasedyear" -> (long) dateTime.get(WeekFields.ISO.weekBasedYear());
      default -> throw new IllegalArgumentException("Unknown date field: " + field);
    };
  }
}
