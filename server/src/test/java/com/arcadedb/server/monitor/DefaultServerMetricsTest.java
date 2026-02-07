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
package com.arcadedb.server.monitor;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for DefaultServerMetrics.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DefaultServerMetricsTest {

  @Test
  void shouldCreateNewMeter() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    final ServerMetrics.Meter meter = metrics.meter("test.metric");

    assertThat(meter).isNotNull();
    assertThat(meter).isInstanceOf(MetricMeter.class);
  }

  @Test
  void shouldReturnSameMeterForSameName() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    final ServerMetrics.Meter meter1 = metrics.meter("test.metric");
    final ServerMetrics.Meter meter2 = metrics.meter("test.metric");

    assertThat(meter1).isSameAs(meter2);
  }

  @Test
  void shouldCreateDifferentMetersForDifferentNames() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    final ServerMetrics.Meter meter1 = metrics.meter("test.metric1");
    final ServerMetrics.Meter meter2 = metrics.meter("test.metric2");

    assertThat(meter1).isNotSameAs(meter2);
  }

  @Test
  void shouldGetAllMeters() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    metrics.meter("metric1");
    metrics.meter("metric2");
    metrics.meter("metric3");

    final Map<String, ServerMetrics.Meter> allMeters = metrics.getMeters();

    assertThat(allMeters).hasSize(3);
    assertThat(allMeters).containsKeys("metric1", "metric2", "metric3");
  }

  @Test
  void shouldClearMetricsOnStop() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    metrics.meter("metric1");
    metrics.meter("metric2");

    assertThat(metrics.getMeters()).hasSize(2);

    metrics.stop();

    assertThat(metrics.getMeters()).isNull();
  }

  @Test
  void shouldHandleConcurrentAccess() throws InterruptedException {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    final Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      final int threadId = i;
      threads[i] = new Thread(() -> {
        for (int j = 0; j < 100; j++) {
          metrics.meter("thread." + threadId + ".metric");
          metrics.meter("shared.metric");
        }
      });
      threads[i].start();
    }

    for (final Thread thread : threads) {
      thread.join();
    }

    assertThat(metrics.getMeters()).hasSize(11); // 10 thread-specific + 1 shared
    assertThat(metrics.getMeters()).containsKey("shared.metric");
  }

  @Test
  void shouldHandleEmptyMetrics() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    final Map<String, ServerMetrics.Meter> allMeters = metrics.getMeters();

    assertThat(allMeters).isEmpty();
  }

  @Test
  void shouldRecordHitsInMeter() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    final ServerMetrics.Meter meter = metrics.meter("test.hits");
    meter.hit();
    meter.hit();
    meter.hit();

    assertThat(meter.getTotalCounter()).isEqualTo(3);
  }

  @Test
  void shouldHandleManyMeters() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    for (int i = 0; i < 1000; i++) {
      metrics.meter("metric." + i);
    }

    assertThat(metrics.getMeters()).hasSize(1000);
  }

  @Test
  void shouldHandleSpecialCharactersInMetricNames() {
    final DefaultServerMetrics metrics = new DefaultServerMetrics();

    metrics.meter("test-metric");
    metrics.meter("test.metric");
    metrics.meter("test_metric");
    metrics.meter("test:metric");

    assertThat(metrics.getMeters()).hasSize(4);
  }
}
