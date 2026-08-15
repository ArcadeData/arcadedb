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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewSchedulerTest {

  @Test
  void reschedulingCancelsThePreviousTask() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = newPeriodicView(database);

        scheduler.schedule(database, view);
        final ScheduledFuture<?> first = scheduledTask(scheduler, view.getName());

        scheduler.schedule(database, view);
        final ScheduledFuture<?> second = scheduledTask(scheduler, view.getName());

        assertThat((Object) second).isNotSameAs(first);
        assertThat(first.isCancelled()).isTrue();
        assertThat(second.isCancelled()).isFalse();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  @Test
  void cancelStopsTheScheduledTask() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = newPeriodicView(database);

        scheduler.schedule(database, view);
        final ScheduledFuture<?> task = scheduledTask(scheduler, view.getName());

        scheduler.cancel(view.getName());

        assertThat(task.isCancelled()).isTrue();
        assertThat((Object) scheduledTask(scheduler, view.getName())).isNull();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * {@code scheduleAtFixedRate} cancels a task permanently the first time it throws, so a single failed pass used
   * to be able to stop a view refreshing for the life of the database with nothing saying so. The tick must
   * therefore let nothing out, whatever the refresh throws.
   */
  @Test
  void aRefreshThatKeepsFailingKeepsTheScheduleAlive() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        // Neither the backing type nor the source type exists, so every pass fails - on the scan of the backing
        // type that collects the previous snapshot, before the defining query is even run.
        final MaterializedViewImpl view = new MaterializedViewImpl(database, "FailingView", "SELECT name FROM Source",
            "FailingView_backing", List.of("Source"), MaterializedViewRefreshMode.PERIODIC, true, 50);

        scheduler.schedule(database, view);
        final ScheduledFuture<?> task = scheduledTask(scheduler, view.getName());

        Awaitility.await("the failing refresh runs more than once")
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(20))
            .until(() -> view.getErrorCount() >= 3);

        assertThat(task.isCancelled()).as("a failed pass must not cancel the schedule").isFalse();
        assertThat(task.isDone()).as("the task must still be pending its next run").isFalse();
        // Not the status: with a 50ms interval the sample can legitimately land on a pass that has just set
        // BUILDING. The error counter only ever moves on a failed pass, and it is what the wait above is on.
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * The same guarantee for the failure classes {@code catch (Exception)} never covered, checked without waiting
   * out a tick.
   */
  @Test
  void anErrorThrownByARefreshDoesNotEscapeTheTick() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = new ThrowingRefreshView(database, "ThrowingView",
            MaterializedViewRefreshMode.PERIODIC, TimeUnit.HOURS.toMillis(1));

        scheduler.runOneRefresh(database, view);

        assertThat(view.getErrorCount()).as("the failed pass is still counted").isEqualTo(1);
        assertThat(view.getStatus()).isEqualTo("ERROR");
        assertThat(view.tryBeginRefresh()).as("the next pass can still take ownership").isTrue();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  private static MaterializedViewImpl newPeriodicView(final Database database) {
    // interval is long enough that the refresh task never fires during the test
    return new MaterializedViewImpl(database, "SchedulerView", "SELECT name FROM Source", "SchedulerView_backing",
        List.of("Source"), MaterializedViewRefreshMode.PERIODIC, true, TimeUnit.HOURS.toMillis(1));
  }

  /**
   * The scheduler thread outlives every request and is never recycled, so a {@link DatabaseContext} entry left
   * installed on it by the refresh transaction pins the database - page cache included - for the life of the JVM,
   * and is what made the task's weak references unable to ever clear (issue #6203). Checked on the thread that ran
   * the pass, since the context is a ThreadLocal.
   */
  @Test
  void aPassLeavesNoDatabaseContextOnTheThreadThatRanIt() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        database.transaction(() -> database.getSchema().createDocumentType("ContextSource"));
        database.transaction(() -> database.getSchema().buildMaterializedView()
            .withName("ContextView")
            .withQuery("SELECT id FROM ContextSource")
            .create());
        final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("ContextView");

        final AtomicReference<Object> leftBehind = new AtomicReference<>();
        final Thread standIn = new Thread(() -> {
          scheduler.runOneRefresh(database, view);
          leftBehind.set(DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath()));
        }, "mv-scheduler-stand-in");
        standIn.start();
        standIn.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(view.getStatus()).as("the pass itself succeeded").isEqualTo("VALID");
        assertThat(leftBehind.get())
            .as("the refresh must not leave the database installed on the scheduler thread")
            .isNull();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * ...but only a context the pass installed itself. {@code runOneRefresh} is package-visible so a test can drive
   * one pass inline, and dismantling the caller's own context would be a side effect nobody asked for.
   */
  @Test
  void aPassLeavesAContextItDidNotInstallAlone() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        database.transaction(() -> database.getSchema().createDocumentType("KeptContextSource"));
        database.transaction(() -> database.getSchema().buildMaterializedView()
            .withName("KeptContextView")
            .withQuery("SELECT id FROM KeptContextSource")
            .create());
        final MaterializedViewImpl view =
            (MaterializedViewImpl) database.getSchema().getMaterializedView("KeptContextView");

        // The calls above already installed this thread's context.
        assertThat(DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath())).isNotNull();

        scheduler.runOneRefresh(database, view);

        assertThat(DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath()))
            .as("a context the pass found already installed is left where it was")
            .isNotNull();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * One scheduler is created per database, so a JVM with several open databases used to get several threads all
   * called {@code ArcadeDB-MV-Scheduler} and a thread dump that could not say which was which.
   */
  @Test
  void theSchedulerThreadIsNamedAfterItsDatabase() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = new MaterializedViewImpl(database, "NamedThreadView",
            "SELECT name FROM Source", "NamedThreadView_backing", List.of("Source"),
            MaterializedViewRefreshMode.PERIODIC, true, 20);
        scheduler.schedule(database, view);

        // The backing type does not exist, so every pass fails - which is enough to prove the thread ran.
        Awaitility.await("the scheduler thread ran a pass")
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(20))
            .until(() -> view.getErrorCount() >= 1);

        assertThat(Thread.getAllStackTraces().keySet().stream().map(Thread::getName))
            .anyMatch(name -> name.equals("ArcadeDB-MV-Scheduler-" + database.getName()));
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * The weak references exist for a database abandoned without {@code close()}: the tick has to notice and cancel
   * itself. Driven by clearing the references rather than by waiting on {@code System.gc()}, so the cancel path is
   * proved without the test depending on when a collection happens.
   */
  @Test
  void aTaskWhoseReferencesHaveClearedCancelsItself() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = newPeriodicView(database);
        scheduler.schedule(database, view);

        final Runnable task = refreshTask(scheduler, view.getName());
        final ScheduledFuture<?> future = scheduledTask(scheduler, view.getName());

        weakReferenceField(task, "viewRef").clear();
        task.run();

        assertThat(future.isCancelled()).as("the tick cancels itself once its references have gone").isTrue();
        assertThat((Object) refreshTask(scheduler, view.getName())).as("and unregisters itself").isNull();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * ...and unregisters only itself. A task that self-cancels after the view was re-scheduled - a schema reload or
   * an ALTER MATERIALIZED VIEW - must leave the replacement registered, or the view stops refreshing with nothing
   * saying so.
   */
  @Test
  void aSelfCancellingTaskLeavesAReplacementRegistered() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = newPeriodicView(database);
        scheduler.schedule(database, view);
        final Runnable stale = refreshTask(scheduler, view.getName());

        // The view is re-scheduled, so the map now holds a different task under the same name.
        scheduler.schedule(database, view);
        final Runnable current = refreshTask(scheduler, view.getName());
        final ScheduledFuture<?> currentFuture = scheduledTask(scheduler, view.getName());
        assertThat((Object) current).isNotSameAs(stale);

        weakReferenceField(stale, "viewRef").clear();
        stale.run();

        assertThat((Object) refreshTask(scheduler, view.getName()))
            .as("the replacement is still the registered task").isSameAs(current);
        assertThat(currentFuture.isCancelled()).as("and is still scheduled").isFalse();
      } finally {
        scheduler.shutdown();
      }
    });
  }

  /**
   * The reachability claim itself belongs in a heap check rather than in a {@code System.gc()}-dependent test, but
   * its precondition does not: the scheduled task must hold nothing strongly that leads back to the database. It
   * used to hold the database weakly and the view - which holds its own database - strongly, so the weak reference
   * could never clear and an abandoned database stayed pinned to a live thread with its page cache (issue #6203).
   */
  @Test
  void aScheduledTaskHoldsNothingStrongThatLeadsBackToTheDatabase() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final MaterializedViewImpl view = newPeriodicView(database);
        scheduler.schedule(database, view);

        final Runnable task = refreshTask(scheduler, view.getName());
        for (final Field field : task.getClass().getDeclaredFields()) {
          field.setAccessible(true);
          assertThat(field.get(task))
              .as("field '%s' of the scheduled task", field.getName())
              .isNotInstanceOfAny(Database.class, MaterializedView.class);
        }
      } finally {
        scheduler.shutdown();
      }
    });
  }

  private static ScheduledFuture<?> scheduledTask(final MaterializedViewScheduler scheduler, final String viewName)
      throws Exception {
    final Runnable task = refreshTask(scheduler, viewName);
    if (task == null)
      return null;
    final Field field = task.getClass().getDeclaredField("future");
    field.setAccessible(true);
    return (ScheduledFuture<?>) field.get(task);
  }

  @SuppressWarnings("unchecked")
  private static Runnable refreshTask(final MaterializedViewScheduler scheduler, final String viewName)
      throws Exception {
    final Field field = MaterializedViewScheduler.class.getDeclaredField("tasks");
    field.setAccessible(true);
    return ((Map<String, ? extends Runnable>) field.get(scheduler)).get(viewName);
  }

  private static WeakReference<?> weakReferenceField(final Runnable task, final String fieldName) throws Exception {
    final Field field = task.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return (WeakReference<?>) field.get(task);
  }
}
