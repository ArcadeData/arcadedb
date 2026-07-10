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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4519: {@link TimeSeriesMaintenanceScheduler} must not leak the
 * {@link DatabaseContext} ThreadLocal on the shared maintenance worker threads.
 *
 * <p>The scheduler runs maintenance passes on a fixed-size {@link ScheduledExecutorService} pool
 * that is reused across every database. If a pass installs a database into the worker thread's
 * {@code DatabaseContext} ThreadLocal but never removes it, the thread keeps a strong reference to
 * a (possibly closed) database and, worse, the next pass for a different database inherits the
 * previous database's transaction context, so commits target the wrong store.
 */
class TimeSeriesMaintenanceSchedulerContextLeakTest extends TestHelper {

  @Test
  void maintenancePassRemovesDatabaseContextFromWorkerThread() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Ticker");
    final String dbPath = ((DatabaseInternal) database).getDatabasePath();

    final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "TS-Maintenance-Test");
      t.setDaemon(true);
      return t;
    });
    try {
      final AtomicReference<DatabaseContext.DatabaseContextTL> contextAfterRun = new AtomicReference<>();

      worker.submit(() -> {
        // The pooled thread starts with no DatabaseContext installed.
        assertThat(DatabaseContext.INSTANCE.getContextIfExists(dbPath)).isNull();

        TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, "Ticker");

        // After the pass, the context MUST have been removed: nothing left behind on the thread.
        contextAfterRun.set(DatabaseContext.INSTANCE.getContextIfExists(dbPath));
      }).get(30, TimeUnit.SECONDS);

      assertThat(contextAfterRun.get())
          .as("DatabaseContext must be removed from the worker thread after a maintenance pass")
          .isNull();
    } finally {
      worker.shutdownNow();
    }
  }

  @Test
  void maintenancePassDoesNotLeakContextAcrossDatabasesOnSameThread() throws Exception {
    // DB-A is the base 'database' from TestHelper.
    database.command("sql",
        "CREATE TIMESERIES TYPE TickerA TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
    final LocalTimeSeriesType typeA = (LocalTimeSeriesType) database.getSchema().getType("TickerA");
    final String dbPathA = ((DatabaseInternal) database).getDatabasePath();

    // DB-B is a second, independent database that shares the same maintenance worker thread.
    final String dbBPath = "./target/databases/" + getClass().getSimpleName() + "-B";
    FileUtils.deleteRecursively(new File(dbBPath));
    final DatabaseFactory factoryB = new DatabaseFactory(dbBPath);
    final Database databaseB = factoryB.create();
    try {
      databaseB.command("sql",
          "CREATE TIMESERIES TYPE TickerB TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
      final LocalTimeSeriesType typeB = (LocalTimeSeriesType) databaseB.getSchema().getType("TickerB");
      final String dbPathB = ((DatabaseInternal) databaseB).getDatabasePath();

      final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "TS-Maintenance-Test");
        t.setDaemon(true);
        return t;
      });
      try {
        final AtomicReference<DatabaseContext.DatabaseContextTL> leakedAContext = new AtomicReference<>();
        final AtomicReference<DatabaseContext.DatabaseContextTL> bContext = new AtomicReference<>();

        worker.submit(() -> {
          // Pass for DB-A on the shared thread.
          TimeSeriesMaintenanceScheduler.runMaintenance(database, typeA, "TickerA");

          // Pass for DB-B on the SAME thread. If DB-A's context leaked, DB-B's pass would have run
          // with DB-A still installed in the ThreadLocal.
          assertThat(DatabaseContext.INSTANCE.getContextIfExists(dbPathA))
              .as("DB-A context must not survive into the DB-B maintenance pass on the same thread")
              .isNull();

          TimeSeriesMaintenanceScheduler.runMaintenance(databaseB, typeB, "TickerB");

          leakedAContext.set(DatabaseContext.INSTANCE.getContextIfExists(dbPathA));
          bContext.set(DatabaseContext.INSTANCE.getContextIfExists(dbPathB));
        }).get(30, TimeUnit.SECONDS);

        assertThat(leakedAContext.get())
            .as("DB-A context must not be present on the worker thread after the DB-B pass")
            .isNull();
        assertThat(bContext.get())
            .as("DB-B context must also be cleaned up after its own pass")
            .isNull();
      } finally {
        worker.shutdownNow();
      }
    } finally {
      databaseB.drop();
      factoryB.close();
    }
  }
}
