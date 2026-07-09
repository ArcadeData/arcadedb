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
package com.arcadedb.server.backup;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.BitSet;

/**
 * Lightweight CRON expression parser supporting the standard 6-field format:
 * second minute hour day-of-month month day-of-week
 * <p>
 * Supports:
 * - Specific values: "5"
 * - Wildcards: "*"
 * - Ranges: "1-5"
 * - Lists: "1,3,5"
 * - Increments: "0/15" (every 15 starting at 0)
 * - Day-of-week: 0-6 (Sunday=0) or SUN-SAT
 * - Optional '?' for day-of-month or day-of-week (treated as '*')
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CronScheduleParser {
  private static final int FIELD_SECOND       = 0;
  private static final int FIELD_MINUTE       = 1;
  private static final int FIELD_HOUR         = 2;
  private static final int FIELD_DAY_OF_MONTH = 3;
  private static final int FIELD_MONTH        = 4;
  private static final int FIELD_DAY_OF_WEEK  = 5;

  private final BitSet seconds;     // 0-59
  private final BitSet minutes;     // 0-59
  private final BitSet hours;       // 0-23
  private final BitSet daysOfMonth; // 1-31
  private final BitSet months;      // 1-12
  private final BitSet daysOfWeek;  // 0-6 (Sunday=0)

  // Standard CRON ORs day-of-month and day-of-week when BOTH are restricted; when only one is
  // restricted the wildcard field is ignored. These flags record whether each field is restricted.
  private final boolean dayOfMonthRestricted;
  private final boolean dayOfWeekRestricted;

  private final String expression;

  public CronScheduleParser(final String expression) {
    this.expression = expression;
    this.seconds = new BitSet(60);
    this.minutes = new BitSet(60);
    this.hours = new BitSet(24);
    this.daysOfMonth = new BitSet(32);
    this.months = new BitSet(13);
    this.daysOfWeek = new BitSet(8); // 0-7, where both 0 and 7 mean Sunday

    final String[] fields = expression.trim().split("\\s+");

    if (fields.length != 6)
      throw new IllegalArgumentException(
          "Invalid CRON expression: expected 6 fields (second minute hour day-of-month month day-of-week), got " + fields.length);

    this.dayOfMonthRestricted = !isWildcard(fields[FIELD_DAY_OF_MONTH]);
    this.dayOfWeekRestricted = !isWildcard(fields[FIELD_DAY_OF_WEEK]);

    parse(fields);
  }

  private static boolean isWildcard(final String field) {
    return "*".equals(field) || "?".equals(field);
  }

  private void parse(final String[] fields) {
    parseField(fields[FIELD_SECOND], seconds, 0, 59, null);
    parseField(fields[FIELD_MINUTE], minutes, 0, 59, null);
    parseField(fields[FIELD_HOUR], hours, 0, 23, null);
    parseField(fields[FIELD_DAY_OF_MONTH], daysOfMonth, 1, 31, null);
    parseField(fields[FIELD_MONTH], months, 1, 12, new String[]{"JAN", "FEB", "MAR", "APR", "MAY", "JUN",
        "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"});
    // Day-of-week accepts 0-7 where both 0 and 7 mean Sunday, so ranges such as "1-7" (Mon-Sun) and
    // the lone value "7" are valid. Bit 7 is folded onto bit 0 after the whole field is parsed.
    parseField(fields[FIELD_DAY_OF_WEEK], daysOfWeek, 0, 7, new String[]{"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"});
    if (daysOfWeek.get(7)) {
      daysOfWeek.set(0);
      daysOfWeek.clear(7);
    }
  }

  private void parseField(final String field, final BitSet bitSet, final int min, final int max, final String[] names) {
    if (isWildcard(field)) {
      bitSet.set(min, max + 1);
      return;
    }

    // Handle lists (comma-separated)
    if (field.contains(",")) {
      for (final String part : field.split(","))
        parseField(part, bitSet, min, max, names);
      return;
    }

    // Handle increments (e.g., 0/15)
    if (field.contains("/")) {
      final String[] parts = field.split("/", -1);
      if (parts.length != 2)
        throw new IllegalArgumentException("Invalid CRON increment '" + field + "' in expression '" + expression + "'");
      final int start = "*".equals(parts[0]) ? min : parseValue(parts[0], min, max, names);
      final int increment = parseInt(parts[1], "increment");
      if (increment <= 0)
        throw new IllegalArgumentException(
            "Invalid CRON increment '" + increment + "' in expression '" + expression + "': must be greater than 0");
      for (int i = start; i <= max; i += increment)
        bitSet.set(i);
      return;
    }

    // Handle ranges (e.g., 1-5)
    if (field.contains("-")) {
      final String[] parts = field.split("-");
      if (parts.length != 2)
        throw new IllegalArgumentException("Invalid CRON range '" + field + "' in expression '" + expression + "'");
      final int rangeStart = parseValue(parts[0], min, max, names);
      final int rangeEnd = parseValue(parts[1], min, max, names);
      if (rangeStart > rangeEnd)
        throw new IllegalArgumentException(
            "Invalid CRON range '" + field + "' in expression '" + expression + "': start is greater than end");
      bitSet.set(rangeStart, rangeEnd + 1);
      return;
    }

    // Single value
    bitSet.set(parseValue(field, min, max, names));
  }

  private int parseValue(final String value, final int min, final int max, final String[] names) {
    int result = -1;
    if (names != null) {
      final String upper = value.toUpperCase();
      for (int i = 0; i < names.length; i++)
        if (names[i].equals(upper)) {
          result = i + min;
          break;
        }
    }
    if (result < 0)
      result = parseInt(value, "value");

    if (result < min || result > max)
      throw new IllegalArgumentException(
          "Invalid CRON value '" + value + "' in expression '" + expression + "': out of range [" + min + "-" + max + "]");

    return result;
  }

  private int parseInt(final String value, final String what) {
    try {
      return Integer.parseInt(value.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid CRON " + what + " '" + value + "' in expression '" + expression + "'");
    }
  }

  /**
   * Calculates the next execution time from the given start time.
   *
   * @param from The starting point for calculation
   * @return The next execution time
   */
  public LocalDateTime getNextExecutionTime(final LocalDateTime from) {
    LocalDateTime next = from.plusSeconds(1).withNano(0);

    // Find next matching time by incrementing fields
    // Use a reasonable limit: 4 years should cover any valid CRON expression
    // This prevents potential infinite loops from impossible CRON expressions
    final LocalDateTime maxDate = from.plusYears(4);

    while (next.isBefore(maxDate)) {
      // Check month
      if (!months.get(next.getMonthValue())) {
        next = next.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
        continue;
      }

      // Check day-of-month and day-of-week together. Standard CRON ORs the two fields when both
      // are restricted; when only one is restricted the wildcard field is ignored (its BitSet is
      // fully set, so the AND below reduces to the restricted field).
      final int cronDayOfWeek = next.getDayOfWeek().getValue() % 7; // Sunday=7 becomes 0
      final boolean domMatches = daysOfMonth.get(next.getDayOfMonth());
      final boolean dowMatches = daysOfWeek.get(cronDayOfWeek);
      final boolean dayMatches = (dayOfMonthRestricted && dayOfWeekRestricted)
          ? (domMatches || dowMatches)
          : (domMatches && dowMatches);
      if (!dayMatches) {
        next = next.plusDays(1).withHour(0).withMinute(0).withSecond(0);
        continue;
      }

      // Check hour
      if (!hours.get(next.getHour())) {
        next = next.plusHours(1).withMinute(0).withSecond(0);
        continue;
      }

      // Check minute
      if (!minutes.get(next.getMinute())) {
        next = next.plusMinutes(1).withSecond(0);
        continue;
      }

      // Check second
      if (!seconds.get(next.getSecond())) {
        next = next.plusSeconds(1);
        continue;
      }

      // All fields match
      return next;
    }

    throw new IllegalStateException(
        "Could not find next execution time within 4 years for CRON expression '" + expression +
            "'. The expression may be invalid or specify an impossible schedule.");
  }

  /**
   * Calculates the delay in milliseconds until the next execution time.
   *
   * @param from The starting point for calculation
   * @return The delay in milliseconds
   */
  public long getDelayMillis(final LocalDateTime from) {
    final LocalDateTime next = getNextExecutionTime(from);
    return Duration.between(from, next).toMillis();
  }

  public String getExpression() {
    return expression;
  }

  @Override
  public String toString() {
    return "CronScheduleParser[" + expression + "]";
  }
}
