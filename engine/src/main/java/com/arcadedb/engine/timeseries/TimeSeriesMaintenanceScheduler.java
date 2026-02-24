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
package com.arcadedb.engine.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Background scheduler that automatically applies retention and downsampling
 * policies for TimeSeries types. Runs as a daemon thread and checks each
 * registered type at a configurable interval (default: 60 seconds).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesMaintenanceScheduler {

  private static final long DEFAULT_CHECK_INTERVAL_MS = 60_000; // 1 minute

  private final ScheduledExecutorService executor;
  private final Map<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

  /** Maximum number of concurrent maintenance tasks (compaction + retention per type). */
  private static final int MAX_THREADS = 4;

  public TimeSeriesMaintenanceScheduler() {
    this.executor = Executors.newScheduledThreadPool(MAX_THREADS, r -> {
      final Thread t = new Thread(r, "ArcadeDB-TS-Maintenance");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Schedules automatic compaction, retention, and downsampling for a TimeSeries type.
   * Compaction is always scheduled to prevent unbounded mutable-bucket growth.
   * Retention and downsampling steps are only executed when policies are configured.
   */
  public void schedule(final Database database, final LocalTimeSeriesType tsType) {
    final String typeName = tsType.getName();

    final WeakReference<Database> dbRef = new WeakReference<>(database);
    final WeakReference<LocalTimeSeriesType> typeRef = new WeakReference<>(tsType);

    // Cancel any existing task for this type (e.g., if retention policy was changed via ALTER)
    final ScheduledFuture<?> existing = tasks.remove(typeName);
    if (existing != null)
      existing.cancel(false);

    tasks.put(typeName, executor.scheduleAtFixedRate(() -> {
      final Database db = dbRef.get();
      final LocalTimeSeriesType type = typeRef.get();
      if (db == null || !db.isOpen() || type == null) {
        cancel(typeName);
        return;
      }

      // The maintenance thread is created by a ScheduledExecutorService and does not
      // have a DatabaseContext initialized.  We must initialize it before calling any
      // database operation (begin/commit) or we get "Transaction context not found".
      DatabaseContext.INSTANCE.init((DatabaseInternal) db);
      try {
        final TimeSeriesEngine engine = type.getEngine();
        if (engine == null)
          return;

        final long nowMs = System.currentTimeMillis();

        // Compact mutable data before retention/downsampling so that
        // all samples are in the sealed store and subject to truncation.
        engine.compactAll();

        // Apply retention policy
        if (type.getRetentionMs() > 0) {
          final long cutoff = nowMs - type.getRetentionMs();
          engine.applyRetention(cutoff);
        }

        // Apply downsampling tiers
        if (!type.getDownsamplingTiers().isEmpty())
          engine.applyDownsampling(type.getDownsamplingTiers(), nowMs);

      } catch (final Throwable e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error in TimeSeries maintenance for type '%s'", e, typeName);
      }
    }, 5_000, DEFAULT_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS));
  }

  /**
   * Cancels the maintenance task for a specific type.
   */
  public void cancel(final String typeName) {
    final ScheduledFuture<?> future = tasks.remove(typeName);
    if (future != null)
      future.cancel(false);
  }

  /**
   * Shuts down the scheduler and cancels all tasks.
   */
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS))
        executor.shutdownNow();
    } catch (final InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    tasks.clear();
  }
}
