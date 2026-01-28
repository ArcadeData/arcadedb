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

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
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

  private final String expression;

  public CronScheduleParser(final String expression) {
    this.expression = expression;
    this.seconds = new BitSet(60);
    this.minutes = new BitSet(60);
    this.hours = new BitSet(24);
    this.daysOfMonth = new BitSet(32);
    this.months = new BitSet(13);
    this.daysOfWeek = new BitSet(7);

    parse(expression);
  }

  private void parse(final String expression) {
    final String[] fields = expression.trim().split("\\s+");

    if (fields.length != 6)
      throw new IllegalArgumentException(
          "Invalid CRON expression: expected 6 fields (second minute hour day-of-month month day-of-week), got " + fields.length);

    parseField(fields[FIELD_SECOND], seconds, 0, 59, null);
    parseField(fields[FIELD_MINUTE], minutes, 0, 59, null);
    parseField(fields[FIELD_HOUR], hours, 0, 23, null);
    parseField(fields[FIELD_DAY_OF_MONTH], daysOfMonth, 1, 31, null);
    parseField(fields[FIELD_MONTH], months, 1, 12, new String[]{"JAN", "FEB", "MAR", "APR", "MAY", "JUN",
        "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"});
    parseField(fields[FIELD_DAY_OF_WEEK], daysOfWeek, 0, 6, new String[]{"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"});
  }

  private void parseField(final String field, final BitSet bitSet, final int min, final int max, final String[] names) {
    if (field.equals("*") || field.equals("?")) {
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
      final String[] parts = field.split("/");
      final int start = parts[0].equals("*") ? min : parseValue(parts[0], min, names);
      final int increment = Integer.parseInt(parts[1]);
      for (int i = start; i <= max; i += increment)
        bitSet.set(i);
      return;
    }

    // Handle ranges (e.g., 1-5)
    if (field.contains("-")) {
      final String[] parts = field.split("-");
      final int rangeStart = parseValue(parts[0], min, names);
      final int rangeEnd = parseValue(parts[1], min, names);
      bitSet.set(rangeStart, rangeEnd + 1);
      return;
    }

    // Single value
    bitSet.set(parseValue(field, min, names));
  }

  private int parseValue(final String value, final int offset, final String[] names) {
    if (names != null) {
      final String upper = value.toUpperCase();
      for (int i = 0; i < names.length; i++)
        if (names[i].equals(upper))
          return i + offset;
    }
    return Integer.parseInt(value);
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

      // Check day of month
      if (!daysOfMonth.get(next.getDayOfMonth())) {
        next = next.plusDays(1).withHour(0).withMinute(0).withSecond(0);
        continue;
      }

      // Check day of week (convert Java DayOfWeek 1-7 to cron 0-6)
      final int cronDayOfWeek = next.getDayOfWeek().getValue() % 7; // Sunday=7 becomes 0
      if (!daysOfWeek.get(cronDayOfWeek)) {
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
    return java.time.Duration.between(from, next).toMillis();
  }

  public String getExpression() {
    return expression;
  }

  @Override
  public String toString() {
    return "CronScheduleParser[" + expression + "]";
  }
}
