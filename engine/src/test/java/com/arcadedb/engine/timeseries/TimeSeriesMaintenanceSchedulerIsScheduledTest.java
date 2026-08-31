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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6948, engine half: {@code isScheduled()} is the observable the HA repair path's regression test asserts
 * on, so it has to be able to tell the two states apart on its own. Whether a type is maintained has no other
 * outward sign - the recurring task logs nothing when it finds nothing to do, and its three effects only become
 * visible once enough data has piled up for one of them to change something - so an accessor that answered
 * {@code true} (or {@code false}) unconditionally would leave that test passing for no reason.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class TimeSeriesMaintenanceSchedulerIsScheduledTest extends TestHelper {

  @Test
  void isScheduledDistinguishesScheduledFromUnscheduledAndCancelled() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Probe TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Probe");

    final TimeSeriesMaintenanceScheduler scheduler = new TimeSeriesMaintenanceScheduler();
    try {
      assertThat(scheduler.isScheduled("Probe")).as("a fresh scheduler maintains nothing").isFalse();
      assertThat(scheduler.isScheduled("NeverSeen")).isFalse();

      scheduler.schedule(database, tsType);
      assertThat(scheduler.isScheduled("Probe")).as("schedule() must be observable").isTrue();

      // Idempotent: schedule() cancels and replaces the task for the same name, and the type stays maintained.
      scheduler.schedule(database, tsType);
      assertThat(scheduler.isScheduled("Probe")).as("a repeated schedule() leaves the type maintained").isTrue();

      scheduler.cancel("Probe");
      assertThat(scheduler.isScheduled("Probe")).as("a cancelled task is no longer maintenance").isFalse();
    } finally {
      scheduler.shutdown();
    }
  }
}
