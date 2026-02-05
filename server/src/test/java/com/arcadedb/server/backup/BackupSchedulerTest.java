/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

/**
 * Unit tests for BackupScheduler lifecycle and scheduling logic.
 */
class BackupSchedulerTest {

  @TempDir
  Path tempDir;

  private BackupScheduler scheduler;

  @BeforeEach
  void setUp() {
    // Create scheduler with null server (won't execute backups, just tests scheduling)
    scheduler = new BackupScheduler(null, tempDir.toString(), null);
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null && scheduler.isRunning())
      scheduler.stop();
  }

  @Test
  void initialStateNotRunning() {
    assertThat(scheduler.isRunning()).isFalse();
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void startMakesSchedulerRunning() {
    scheduler.start();
    assertThat(scheduler.isRunning()).isTrue();
  }

  @Test
  void stopMakesSchedulerNotRunning() {
    scheduler.start();
    assertThat(scheduler.isRunning()).isTrue();

    scheduler.stop();
    assertThat(scheduler.isRunning()).isFalse();
  }

  @Test
  void stopClearsScheduledTasks() {
    scheduler.start();

    // Schedule a backup
    final DatabaseBackupConfig config = createFrequencyConfig("testdb", 60);
    scheduler.scheduleBackup("testdb", config);
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    scheduler.stop();
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void scheduleBackupWhenNotRunningDoesNotSchedule() {
    // Scheduler not started
    final DatabaseBackupConfig config = createFrequencyConfig("testdb", 60);
    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void scheduleFrequencyBasedBackup() {
    scheduler.start();

    final DatabaseBackupConfig config = createFrequencyConfig("testdb", 60);
    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void scheduleCronBasedBackup() {
    scheduler.start();

    // Schedule daily at 2 AM
    final DatabaseBackupConfig config = createCronConfig("testdb", "0 0 2 * * *");
    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void scheduleMultipleDatabases() {
    scheduler.start();

    scheduler.scheduleBackup("db1", createFrequencyConfig("db1", 30));
    scheduler.scheduleBackup("db2", createFrequencyConfig("db2", 60));
    scheduler.scheduleBackup("db3", createCronConfig("db3", "0 0 3 * * *"));

    assertThat(scheduler.getScheduledCount()).isEqualTo(3);
  }

  @Test
  void cancelBackupRemovesFromSchedule() {
    scheduler.start();

    scheduler.scheduleBackup("testdb", createFrequencyConfig("testdb", 60));
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    scheduler.cancelBackup("testdb");
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void cancelNonExistentBackupDoesNothing() {
    scheduler.start();

    scheduler.scheduleBackup("testdb", createFrequencyConfig("testdb", 60));
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    // Cancel a backup that doesn't exist
    scheduler.cancelBackup("nonexistent");

    // Original backup should still be scheduled
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void reschedulingReplacesExistingSchedule() {
    scheduler.start();

    // Schedule initial backup
    scheduler.scheduleBackup("testdb", createFrequencyConfig("testdb", 30));
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    // Reschedule with different frequency - should replace, not add
    scheduler.scheduleBackup("testdb", createFrequencyConfig("testdb", 60));
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void disabledConfigDoesNotSchedule() {
    scheduler.start();

    final DatabaseBackupConfig config = createFrequencyConfig("testdb", 60);
    config.setEnabled(false);
    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void nullScheduleConfigDoesNotSchedule() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    // No schedule set - schedule is null
    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void triggerImmediateBackupWhenNotRunningDoesNothing() {
    // Scheduler not started
    final DatabaseBackupConfig config = createFrequencyConfig("testdb", 60);

    // Should not throw, just return silently
    scheduler.triggerImmediateBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void triggerImmediateBackupWhenRunning() {
    scheduler.start();

    final DatabaseBackupConfig config = createFrequencyConfig("testdb", 60);

    // Should not throw - the task will fail to execute backup since server is null,
    // but triggering should work
    scheduler.triggerImmediateBackup("testdb", config);

    // Immediate backups don't add to scheduled count - they run once
    // The scheduled count stays 0 unless we explicitly schedule
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void invalidCronExpressionDoesNotSchedule() {
    scheduler.start();

    // Invalid CRON expression (only 5 fields instead of 6)
    final DatabaseBackupConfig config = createCronConfig("testdb", "0 0 2 * *");
    scheduler.scheduleBackup("testdb", config);

    // Should not schedule due to invalid CRON
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  // Helper methods to create backup configurations

  private DatabaseBackupConfig createFrequencyConfig(final String databaseName, final int frequencyMinutes) {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(databaseName);

    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(frequencyMinutes);
    config.setSchedule(schedule);

    return config;
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
