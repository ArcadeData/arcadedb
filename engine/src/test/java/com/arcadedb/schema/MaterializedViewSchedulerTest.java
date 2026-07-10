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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
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
