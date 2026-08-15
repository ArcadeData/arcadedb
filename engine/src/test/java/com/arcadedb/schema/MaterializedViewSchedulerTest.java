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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewSchedulerTest {

  @Test
  void reschedulingCancelsThePreviousTask() throws Exception {
    TestHelper.executeInNewDatabase("MaterializedViewSchedulerTest", database -> {
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler();
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
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler();
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
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler();
      try {
        // The backing type does not exist, so every pass fails on the TRUNCATE that starts it.
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
      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler();
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

  @SuppressWarnings("unchecked")
  private static ScheduledFuture<?> scheduledTask(final MaterializedViewScheduler scheduler, final String viewName)
      throws Exception {
    final Field field = MaterializedViewScheduler.class.getDeclaredField("tasks");
    field.setAccessible(true);
    return ((Map<String, ScheduledFuture<?>>) field.get(scheduler)).get(viewName);
  }
}
