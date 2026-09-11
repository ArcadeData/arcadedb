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
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Companion of {@link Issue7457MaterializedViewLifecycleUnderWriteLockTest} for continuous aggregates, whose drop has
 * the same check-then-remove-under-the-lock shape since #7457: two concurrent drops of the same aggregate produce
 * exactly one success and one "not found", and nothing deadlocks against the schema saves that take the schema
 * monitor under the write lock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7457ContinuousAggregateConcurrentDropTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5");
      database.command("sql", "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.1");
    });
  }

  @Test
  void concurrentDropsOfTheSameAggregateProduceOneSuccessAndOneNotFound() throws Exception {
    final Schema schema = database.getSchema();
    schema.buildContinuousAggregate().withName("hourly_temps").withQuery(
        "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();
    assertThat(schema.existsContinuousAggregate("hourly_temps")).isTrue();
    assertThat(schema.existsType("hourly_temps")).isTrue();

    final int droppers = 4;
    final CyclicBarrier start = new CyclicBarrier(droppers);
    final AtomicInteger succeeded = new AtomicInteger();
    final AtomicInteger notFound = new AtomicInteger();
    final AtomicReference<Throwable> unexpected = new AtomicReference<>();
    final Thread[] threads = new Thread[droppers];
    for (int i = 0; i < droppers; i++) {
      threads[i] = new Thread(() -> {
        try {
          start.await(10, TimeUnit.SECONDS);
          schema.dropContinuousAggregate("hourly_temps");
          succeeded.incrementAndGet();
        } catch (final SchemaException e) {
          if (e.getMessage().contains("not found"))
            notFound.incrementAndGet();
          else
            unexpected.set(e);
        } catch (final Throwable e) {
          unexpected.set(e);
        }
      }, "dropper-" + i);
      threads[i].start();
    }
    for (final Thread thread : threads)
      thread.join(30_000);

    assertThat(unexpected.get()).isNull();
    assertThat(succeeded.get()).isEqualTo(1);
    assertThat(notFound.get()).isEqualTo(droppers - 1);
    assertThat(schema.existsContinuousAggregate("hourly_temps")).isFalse();
    assertThat(schema.existsType("hourly_temps")).as("the backing type went with the aggregate, once").isFalse();
    assertThat(schema.existsType("SensorReading")).isTrue();
  }

  @Test
  void secondDropOfTheSameAggregateFails() {
    final Schema schema = database.getSchema();
    schema.buildContinuousAggregate().withName("hourly_temps").withQuery(
        "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();
    schema.dropContinuousAggregate("hourly_temps");
    assertThatThrownBy(() -> schema.dropContinuousAggregate("hourly_temps")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("not found");
  }
}
