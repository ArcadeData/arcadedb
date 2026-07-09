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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for issue #5028 defects 2 and 3: an invalid CRON config must fail fast (no
 * schedule, no propagated exception) instead of silently stopping backups, and a cancel followed by
 * a reschedule must not leave a duplicate schedule.
 */
class BackupSchedulerValidationTest {

  @TempDir
  Path tempDir;

  private BackupScheduler scheduler;

  @BeforeEach
  void setUp() {
    scheduler = new BackupScheduler(null, tempDir.toString(), null);
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null && scheduler.isRunning())
      scheduler.stop();
  }

  /**
   * A CRON expression with an out-of-range field (hour 24) can never match. Before the fix this
   * scheduled a task whose first delay computation threw and killed the reschedule chain (or
   * propagated an unchecked exception to the caller). After the fix it must be rejected up front:
   * no exception escapes and nothing is scheduled.
   */
  @Test
  void invalidCronRangeFailsFastWithoutScheduling() {
    scheduler.start();

    final DatabaseBackupConfig config = createCronConfig("testdb", "0 0 24 * * *");

    assertThatCode(() -> scheduler.scheduleBackup("testdb", config)).doesNotThrowAnyException();
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void zeroIncrementCronFailsFastWithoutScheduling() {
    scheduler.start();

    final DatabaseBackupConfig config = createCronConfig("testdb", "0 0/0 * * * *");

    assertThatCode(() -> scheduler.scheduleBackup("testdb", config)).doesNotThrowAnyException();
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  /**
   * Cancelling then rescheduling the same database must leave exactly one active schedule, never a
   * resurrected duplicate.
   */
  @Test
  void cancelThenRescheduleLeavesSingleSchedule() {
    scheduler.start();

    scheduler.scheduleBackup("testdb", createCronConfig("testdb", "0 0 3 * * *"));
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    scheduler.cancelBackup("testdb");
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);

    scheduler.scheduleBackup("testdb", createCronConfig("testdb", "0 0 4 * * *"));
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  /**
   * Repeated schedule/cancel cycles must never accumulate more than a single schedule per database.
   */
  @Test
  void repeatedScheduleCancelDoesNotAccumulate() {
    scheduler.start();

    for (int i = 0; i < 20; i++) {
      scheduler.scheduleBackup("testdb", createCronConfig("testdb", "0 0 2 * * *"));
      assertThat(scheduler.getScheduledCount()).isEqualTo(1);
      scheduler.cancelBackup("testdb");
      assertThat(scheduler.getScheduledCount()).isEqualTo(0);
    }
  }

  private DatabaseBackupConfig createCronConfig(final String databaseName, final String cronExpression) {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(databaseName);

    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    schedule.setCronExpression(cronExpression);
    config.setSchedule(schedule);

    return config;
  }
}
