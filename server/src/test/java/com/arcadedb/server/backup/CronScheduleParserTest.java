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

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CronScheduleParserTest {

  @Test
  void parseSimpleExpression() {
    // Every minute at second 0
    final CronScheduleParser parser = new CronScheduleParser("0 * * * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 31, 0));
  }

  @Test
  void parseSpecificTime() {
    // Every day at 2:00:00 AM
    final CronScheduleParser parser = new CronScheduleParser("0 0 2 * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 16, 2, 0, 0));
  }

  @Test
  void parseRange() {
    // Every minute from 10-15, hour 10
    final CronScheduleParser parser = new CronScheduleParser("0 10-15 10 * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 8, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 10, 0));
  }

  @Test
  void parseList() {
    // At seconds 0,15,30,45 of every minute
    final CronScheduleParser parser = new CronScheduleParser("0,15,30,45 * * * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 30, 10);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 15));
  }

  @Test
  void parseIncrement() {
    // Every 15 minutes starting at 0
    final CronScheduleParser parser = new CronScheduleParser("0 0/15 * * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 20, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 0));
  }

  @Test
  void parseDayOfWeek() {
    // Every Monday at 3:00 AM
    final CronScheduleParser parser = new CronScheduleParser("0 0 3 * * MON");

    // 15th Jan 2024 is a Monday
    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    // Should be next Monday (22nd Jan)
    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 22, 3, 0, 0));
  }

  @Test
  void parseDayOfWeekNumeric() {
    // Every Sunday (0) at 4:00 AM
    final CronScheduleParser parser = new CronScheduleParser("0 0 4 * * 0");

    // 15th Jan 2024 is a Monday
    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    // Should be Sunday (21st Jan)
    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 21, 4, 0, 0));
  }

  @Test
  void parseWeekdaysRange() {
    // Monday through Friday at 2:30 AM
    final CronScheduleParser parser = new CronScheduleParser("0 30 2 * * 1-5");

    // Saturday 20th Jan 2024
    final LocalDateTime from = LocalDateTime.of(2024, 1, 20, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    // Should be Monday 22nd Jan
    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 22, 2, 30, 0));
  }

  @Test
  void parseQuestionMark() {
    // Question mark should be treated as wildcard
    final CronScheduleParser parser = new CronScheduleParser("0 0 3 ? * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 16, 3, 0, 0));
  }

  @Test
  void parseMonthNames() {
    // First day of January and July at midnight
    final CronScheduleParser parser = new CronScheduleParser("0 0 0 1 JAN,JUL *");

    final LocalDateTime from = LocalDateTime.of(2024, 2, 15, 0, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 7, 1, 0, 0, 0));
  }

  @Test
  void getDelayMillis() {
    final CronScheduleParser parser = new CronScheduleParser("0 0 3 * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 2, 59, 0);
    final long delayMillis = parser.getDelayMillis(from);

    // Should be 60 seconds (60000 ms)
    assertThat(delayMillis).isEqualTo(60000L);
  }

  @Test
  void invalidExpressionTooFewFields() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0 3 * *"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 6 fields");
  }

  @Test
  void invalidExpressionTooManyFields() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0 3 * * * *"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 6 fields");
  }

  @Test
  void getExpression() {
    final String expression = "0 0 2 * * *";
    final CronScheduleParser parser = new CronScheduleParser(expression);

    assertThat(parser.getExpression()).isEqualTo(expression);
  }

  @Test
  void testToString() {
    final String expression = "0 0 2 * * *";
    final CronScheduleParser parser = new CronScheduleParser(expression);

    assertThat(parser.toString()).contains(expression);
  }

  @Test
  void everyHour() {
    // Every hour at minute 0
    final CronScheduleParser parser = new CronScheduleParser("0 0 * * * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 15, 11, 0, 0));
  }

  @Test
  void monthBoundary() {
    // First of each month at midnight
    final CronScheduleParser parser = new CronScheduleParser("0 0 0 1 * *");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 2, 1, 0, 0, 0));
  }

  @Test
  void yearBoundary() {
    // First of January at midnight
    final CronScheduleParser parser = new CronScheduleParser("0 0 0 1 1 *");

    final LocalDateTime from = LocalDateTime.of(2024, 6, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2025, 1, 1, 0, 0, 0));
  }
}
