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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * Manages scheduling of backup tasks using ScheduledExecutorService.
 * Supports both frequency-based and CRON scheduling.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BackupScheduler {
  private final    ArcadeDBServer                  server;
  private final    ScheduledExecutorService        executor;
  private final    Map<String, ScheduledFuture<?>> scheduledTasks;
  private final    Map<String, CronScheduleParser> cronParsers;
  private final    String                          backupDirectory;
  private final    BackupRetentionManager          retentionManager;
  private volatile boolean                         running;

  public BackupScheduler(final ArcadeDBServer server, final String backupDirectory,
                         final BackupRetentionManager retentionManager) {
    this.server = server;
    this.backupDirectory = backupDirectory;
    this.retentionManager = retentionManager;
    this.executor = Executors.newScheduledThreadPool(2, r -> {
      final Thread t = new Thread(r, "ArcadeDB-AutoBackup");
      t.setDaemon(true);
      return t;
    });
    this.scheduledTasks = new ConcurrentHashMap<>();
    this.cronParsers = new ConcurrentHashMap<>();
    this.running = false;
  }

  /**
   * Starts the scheduler.
   */
  public void start() {
    running = true;
    LogManager.instance().log(this, Level.INFO, "Backup scheduler started");
  }

  /**
   * Schedules a backup task for a database.
   *
   * @param databaseName The name of the database
   * @param config       The backup configuration for the database
   */
  public void scheduleBackup(final String databaseName, final DatabaseBackupConfig config) {
    if (!running) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot schedule backup for '%s' - scheduler not running", databaseName);
      return;
    }

    if (!config.isEnabled()) {
      LogManager.instance().log(this, Level.INFO,
          "Backup disabled for database '%s'", databaseName);
      return;
    }

    final DatabaseBackupConfig.ScheduleConfig schedule = config.getSchedule();
    if (schedule == null) {
      LogManager.instance().log(this, Level.WARNING,
          "No schedule configured for database '%s'", databaseName);
      return;
    }

    // Cancel any existing schedule for this database
    cancelBackup(databaseName);

    final BackupTask task = new BackupTask(server, databaseName, config, backupDirectory, retentionManager);

    switch (schedule.getType()) {
      case FREQUENCY:
        scheduleFrequencyBased(databaseName, task, schedule.getFrequencyMinutes());
        break;
      case CRON:
        scheduleCronBased(databaseName, task, schedule.getCronExpression());
        break;
    }
  }

  private void scheduleFrequencyBased(final String databaseName, final BackupTask task, final int frequencyMinutes) {
    LogManager.instance().log(this, Level.INFO,
        "Scheduling backup for database '%s' every %d minutes", databaseName, frequencyMinutes);

    final ScheduledFuture<?> future = executor.scheduleAtFixedRate(
        task,
        frequencyMinutes, // Initial delay equals frequency
        frequencyMinutes,
        TimeUnit.MINUTES
    );

    scheduledTasks.put(databaseName, future);
  }

  private void scheduleCronBased(final String databaseName, final BackupTask task, final String cronExpression) {
    LogManager.instance().log(this, Level.INFO,
        "Scheduling backup for database '%s' with CRON expression: %s", databaseName, cronExpression);

    try {
      final CronScheduleParser parser = new CronScheduleParser(cronExpression);
      cronParsers.put(databaseName, parser);

      // Schedule the first execution
      scheduleNextCronExecution(databaseName, task, parser);

    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Invalid CRON expression for database '%s': %s", databaseName, e.getMessage());
    }
  }

  private void scheduleNextCronExecution(final String databaseName, final BackupTask task,
                                         final CronScheduleParser parser) {
    if (!running)
      return;

    final long delayMillis = parser.getDelayMillis(LocalDateTime.now());

    LogManager.instance().log(this, Level.FINE,
        "Next backup for database '%s' scheduled in %d ms", databaseName, delayMillis);

    final ScheduledFuture<?> future = executor.schedule(() -> {
      try {
        task.run();
      } finally {
        // Schedule the next execution
        if (running)
          scheduleNextCronExecution(databaseName, task, parser);
      }
    }, delayMillis, TimeUnit.MILLISECONDS);

    scheduledTasks.put(databaseName, future);
  }

  /**
   * Cancels the scheduled backup for a database.
   *
   * @param databaseName The name of the database
   */
  public void cancelBackup(final String databaseName) {
    final ScheduledFuture<?> future = scheduledTasks.remove(databaseName);
    if (future != null) {
      future.cancel(false);
      LogManager.instance().log(this, Level.INFO,
          "Cancelled scheduled backup for database '%s'", databaseName);
    }
    cronParsers.remove(databaseName);
  }

  /**
   * Triggers an immediate backup for a database.
   *
   * @param databaseName The name of the database
   * @param config       The backup configuration
   */
  public void triggerImmediateBackup(final String databaseName, final DatabaseBackupConfig config) {
    if (!running)
      return;

    LogManager.instance().log(this, Level.INFO,
        "Triggering immediate backup for database '%s'", databaseName);

    final BackupTask task = new BackupTask(server, databaseName, config, backupDirectory, retentionManager);
    executor.submit(task);
  }

  /**
   * Stops the scheduler and cancels all scheduled tasks.
   */
  public void stop() {
    running = false;

    // Cancel all scheduled tasks
    for (final ScheduledFuture<?> future : scheduledTasks.values())
      future.cancel(false);
    scheduledTasks.clear();
    cronParsers.clear();

    // Shutdown the executor
    executor.shutdown();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS))
        executor.shutdownNow();
    } catch (final InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    LogManager.instance().log(this, Level.INFO, "Backup scheduler stopped");
  }

  /**
   * Checks if the scheduler is running.
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * Gets the number of scheduled backups.
   */
  public int getScheduledCount() {
    return scheduledTasks.size();
  }
}
