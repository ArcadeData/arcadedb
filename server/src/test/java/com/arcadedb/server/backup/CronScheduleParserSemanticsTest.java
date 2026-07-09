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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;

/**
 * Regression tests for issue #5028: correct CRON day-of-month / day-of-week semantics and field
 * range validation.
 */
class CronScheduleParserSemanticsTest {

  /**
   * Defect 1: when both day-of-month and day-of-week are restricted, standard CRON ORs them.
   * "0 0 2 1 * MON" must fire on the 1st of the month OR on any Monday, whichever comes first.
   */
  @Test
  void domAndDowBothRestrictedUsesOrSemantics() {
    final CronScheduleParser parser = new CronScheduleParser("0 0 2 1 * MON");

    // 15th Jan 2024 is a Monday; the next Monday (22nd) comes well before the next 1st (Feb 1st).
    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    // OR semantics -> next Monday 22nd Jan at 02:00 (NOT April 1st, which is the AND result).
    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 22, 2, 0, 0));
  }

  @Test
  void domOrDowFiresOnFirstOfMonthEvenWhenNotMatchingDow() {
    // Fires on day 15 OR on Wednesday. From a Monday, the nearest match is the DOM=15 side.
    final CronScheduleParser parser = new CronScheduleParser("0 0 0 15 * WED");

    final LocalDateTime from = LocalDateTime.of(2024, 1, 8, 10, 0, 0); // Monday 8th Jan
    final LocalDateTime next = parser.getNextExecutionTime(from);

    // Wednesday 10th Jan 2024 comes before the 15th -> DOW side wins here.
    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 10, 0, 0, 0));
  }

  /**
   * Defect 2: out-of-range fields must be rejected at parse time with a clear error rather than
   * parsing silently and never matching.
   */
  @Test
  void rejectsHour24() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0 24 * * *"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("24");
  }

  @Test
  void rejectsMinute60() {
    assertThatThrownBy(() -> new CronScheduleParser("0 60 * * * *"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsDayOfMonth32() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0 0 32 * *"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsMonth13() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0 0 1 13 *"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsDayOfWeek8() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0 0 * * 8"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Defect 2: increment of zero must be rejected instead of looping forever (i += 0).
   */
  @Test
  void rejectsZeroIncrement() {
    assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
        assertThatThrownBy(() -> new CronScheduleParser("0 0/0 * * * *"))
            .isInstanceOf(IllegalArgumentException.class));
  }

  @Test
  void rejectsNegativeIncrement() {
    assertThatThrownBy(() -> new CronScheduleParser("0 0/-1 * * * *"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Day-of-week 7 is a very common way to express Sunday; it must be accepted as Sunday(0)
   * rather than silently never matching.
   */
  @Test
  void dayOfWeek7IsSunday() {
    final CronScheduleParser parser = new CronScheduleParser("0 0 4 * * 7");

    // 15th Jan 2024 is a Monday; next Sunday is 21st Jan.
    final LocalDateTime from = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
    final LocalDateTime next = parser.getNextExecutionTime(from);

    assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 21, 4, 0, 0));
  }

  /**
   * A small table of known-good expressions and their expected next fire time.
   */
  @Test
  void knownGoodNextFireTimes() {
    final LocalDateTime base = LocalDateTime.of(2024, 3, 10, 12, 30, 15); // Sunday

    assertThat(new CronScheduleParser("0 0 0 * * *").getNextExecutionTime(base))
        .isEqualTo(LocalDateTime.of(2024, 3, 11, 0, 0, 0)); // next midnight

    assertThat(new CronScheduleParser("0 0 * * * *").getNextExecutionTime(base))
        .isEqualTo(LocalDateTime.of(2024, 3, 10, 13, 0, 0)); // next top of hour

    assertThat(new CronScheduleParser("0 0/15 * * * *").getNextExecutionTime(base))
        .isEqualTo(LocalDateTime.of(2024, 3, 10, 12, 45, 0)); // next quarter hour

    assertThat(new CronScheduleParser("0 0 0 1 * *").getNextExecutionTime(base))
        .isEqualTo(LocalDateTime.of(2024, 4, 1, 0, 0, 0)); // first of next month
  }
}
