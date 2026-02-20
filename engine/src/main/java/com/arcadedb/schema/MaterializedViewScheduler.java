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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MaterializedViewScheduler {
  private final ScheduledExecutorService executor;
  private final Map<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

  public MaterializedViewScheduler() {
    this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "ArcadeDB-MV-Scheduler");
      t.setDaemon(true);
      return t;
    });
  }

  public void schedule(final Database database, final MaterializedViewImpl view) {
    final long interval = view.getRefreshInterval();
    if (interval <= 0)
      return;

    final WeakReference<Database> dbRef = new WeakReference<>(database);
    final ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
      final Database db = dbRef.get();
      if (db == null || !db.isOpen()) {
        cancel(view.getName());
        return;
      }
      try {
        MaterializedViewRefresher.fullRefresh(db, view);
      } catch (final Exception e) {
        view.setStatus(MaterializedViewStatus.ERROR);
        LogManager.instance().log(this, Level.SEVERE,
            "Error in periodic refresh for view '%s': %s", e, view.getName(), e.getMessage());
      }
    }, interval, interval, TimeUnit.MILLISECONDS);

    tasks.put(view.getName(), future);
  }

  public void cancel(final String viewName) {
    final ScheduledFuture<?> future = tasks.remove(viewName);
    if (future != null)
      future.cancel(false);
  }

  public void shutdown() {
    executor.shutdownNow();
    tasks.clear();
  }
}
