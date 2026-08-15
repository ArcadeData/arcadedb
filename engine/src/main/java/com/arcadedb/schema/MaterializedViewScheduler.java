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
import com.arcadedb.database.DatabaseContext;
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
  private final ScheduledExecutorService  executor;
  private final Map<String, RefreshTask>  tasks = new ConcurrentHashMap<>();

  /**
   * @param databaseName names the scheduler thread. One scheduler is created per {@link LocalSchema}, so a JVM with
   *                     several open databases has several of these threads, and an undiscriminated name leaves a
   *                     thread dump unable to say which database a thread belongs to.
   */
  public MaterializedViewScheduler(final String databaseName) {
    this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "ArcadeDB-MV-Scheduler-" + databaseName);
      t.setDaemon(true);
      return t;
    });
  }

  public void schedule(final Database database, final MaterializedViewImpl view) {
    final long interval = view.getRefreshInterval();
    if (interval <= 0)
      return;

    // Cancel any task already scheduled for this view, otherwise re-scheduling (for example
    // when the schema is reloaded) leaves the previous task running and refreshing forever,
    // as TimeSeriesMaintenanceScheduler.schedule() already does
    cancel(view.getName());

    final RefreshTask task = new RefreshTask(database, view);
    task.future = executor.scheduleAtFixedRate(task, interval, interval, TimeUnit.MILLISECONDS);

    tasks.put(view.getName(), task);
  }

  /**
   * Runs one periodic pass, and lets nothing out.
   * <p>
   * {@code scheduleAtFixedRate} cancels a task PERMANENTLY the first time it throws - no further execution, ever,
   * and nothing says so - so anything escaping here would stop this view refreshing for the life of the database
   * while it kept reporting the status of its last successful pass. {@code Throwable} rather than {@code Exception}
   * because "this view silently stopped refreshing" is a strictly worse outcome than a logged failure, whatever
   * class the failure belongs to, and because a guard that has to be re-argued every time the refresh path changes
   * is not a guard. The status is set here as well as in the refresher because the refresher's own bookkeeping
   * starts only once it owns the refresh: a failure in the state-machine handover that precedes it - or in any
   * future work that lands before that try - would otherwise leave the view reporting VALID after a pass that
   * never ran.
   * <p>
   * The deliberate part of the tradeoff: a {@link VirtualMachineError} is swallowed here like anything else, so a
   * view whose refresh keeps dying of memory pressure keeps retrying every refresh interval and logging, rather
   * than failing loudly once. That is the intended order of preference - a task cancelled forever with nothing
   * saying so is the worse outcome, and the retry is paced by the view's own interval rather than a spin - but it
   * is a choice, not an oversight, and the next person to find this thread busy during an OOM should know it was
   * made on purpose.
   * <p>
   * The refresh runs a transaction, which installs a {@link DatabaseContext} entry keyed by the database path on
   * whichever thread runs it, and that entry holds a strong reference to the database. On the scheduler thread -
   * which outlives every request and is never recycled - it has to be taken back down again, or a database
   * abandoned without {@code close()} stays reachable from a live thread, with its whole page cache, for the life
   * of the JVM, and {@link RefreshTask}'s weak references can never clear (issue #6203).
   * {@code TimeSeriesMaintenanceScheduler.runMaintenance()} documents the same hazard. Only a context this call
   * installed is removed: the method is package-visible so a test can drive one pass without waiting out a tick,
   * and tearing down a caller's own context would be a side effect nobody asked for.
   * <p>
   * Extracted from the scheduling above so the guarantee can be tested without waiting out a tick.
   */
  // @VisibleForTesting
  void runOneRefresh(final Database database, final MaterializedViewImpl view) {
    final String databasePath = database.getDatabasePath();
    final boolean contextInstalledHere = DatabaseContext.INSTANCE.getContextIfExists(databasePath) == null;
    try {
      MaterializedViewRefresher.fullRefresh(database, view);
    } catch (final Throwable e) {
      view.setStatus(MaterializedViewStatus.ERROR);
      LogManager.instance().log(this, Level.SEVERE,
          "Error in periodic refresh for view '%s': %s", e, view.getName(), e.getMessage());
    } finally {
      if (contextInstalledHere)
        DatabaseContext.INSTANCE.removeContext(databasePath);
    }
  }

  public void cancel(final String viewName) {
    final RefreshTask task = tasks.remove(viewName);
    if (task != null)
      task.future.cancel(false);
  }

  public void shutdown() {
    executor.shutdownNow();
    tasks.clear();
  }

  /**
   * One view's periodic pass, holding nothing it does not have to.
   * <p>
   * Both references are weak on purpose. A database closed the normal way already stops this task -
   * {@code LocalSchema.close()} calls {@link #shutdown()} - so they exist for exactly one case: a database
   * abandoned without ever being closed. For them to do that job NEITHER may be strong, because the two objects are
   * reachable from each other: {@link MaterializedViewImpl} holds its database, and the database's schema holds the
   * view. Holding the view strongly here would pin the database just as effectively as holding the database
   * strongly, which is why this leaked with a {@code WeakReference<Database>} already in place (issue #6203).
   * Everything else the task needs is a {@code String}. The enclosing scheduler, captured by this inner class, has
   * no reference of its own back to the database.
   */
  private final class RefreshTask implements Runnable {
    private final String                              viewName;
    private final WeakReference<Database>             databaseRef;
    private final WeakReference<MaterializedViewImpl> viewRef;
    /**
     * Assigned by {@code schedule()} as soon as the task is scheduled, and BEFORE the task is published to
     * {@code tasks} - so anything found in that map has one, and {@link #cancel} never has to test for it. Only
     * {@link #cancelSelf()}, which runs from inside the task itself, can observe it unset.
     */
    private volatile ScheduledFuture<?> future;

    private RefreshTask(final Database database, final MaterializedViewImpl view) {
      this.viewName = view.getName();
      this.databaseRef = new WeakReference<>(database);
      this.viewRef = new WeakReference<>(view);
    }

    @Override
    public void run() {
      final Database database = databaseRef.get();
      final MaterializedViewImpl view = viewRef.get();
      if (database == null || view == null || !database.isOpen()) {
        cancelSelf();
        return;
      }
      runOneRefresh(database, view);
    }

    /**
     * Cancels this task, and unregisters it only while it is still the task registered for the view. A blind
     * {@code cancel(viewName)} would cancel whatever is registered under that name, which after a re-schedule
     * (schema reload, ALTER MATERIALIZED VIEW) is the replacement task rather than this one - and the view would
     * then stop refreshing with nothing saying so.
     */
    private void cancelSelf() {
      final ScheduledFuture<?> mine = future;
      if (mine == null)
        // The first run beat schedule()'s assignment by the width of one statement, so there is nothing to cancel
        // yet and nothing registered under this name to unregister. The field is volatile and assigned right
        // after, so the next tick - one interval away - takes this branch's place and cancels then.
        return;
      tasks.remove(viewName, this);
      mine.cancel(false);
    }
  }
}
