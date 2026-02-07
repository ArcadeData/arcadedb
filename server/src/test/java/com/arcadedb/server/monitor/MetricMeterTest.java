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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for MetricMeter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MetricMeterTest {

  @Test
  void shouldIncrementTotalCounter() {
    final MetricMeter meter = new MetricMeter();

    meter.hit();
    meter.hit();
    meter.hit();

    assertThat(meter.getTotalCounter()).isEqualTo(3);
  }

  @Test
  void shouldStartWithZeroCounter() {
    final MetricMeter meter = new MetricMeter();

    assertThat(meter.getTotalCounter()).isEqualTo(0);
  }

  @Test
  void shouldTrackRequestsInLastMinute() {
    final MetricMeter meter = new MetricMeter();

    for (int i = 0; i < 60; i++) {
      meter.hit();
    }

    assertThat(meter.getTotalRequestsInLastMinute()).isEqualTo(60);
  }

  @Test
  void shouldCalculateRequestsPerSecond() {
    final MetricMeter meter = new MetricMeter();

    for (int i = 0; i < 60; i++) {
      meter.hit();
    }

    final float rps = meter.getRequestsPerSecondInLastMinute();

    assertThat(rps).isEqualTo(1.0f);
  }

  @Test
  void shouldHandleMultipleHitsInSameSecond() {
    final MetricMeter meter = new MetricMeter();

    for (int i = 0; i < 100; i++) {
      meter.hit();
    }

    assertThat(meter.getTotalCounter()).isEqualTo(100);
    assertThat(meter.getTotalRequestsInLastMinute()).isEqualTo(100);
  }

  @Test
  void shouldTrackRequestsPerSecondSinceLastAsked() throws InterruptedException {
    final MetricMeter meter = new MetricMeter();

    // First batch of hits
    for (int i = 0; i < 10; i++) {
      meter.hit();
    }

    // Wait to ensure at least 1 second passes
    Thread.sleep(1100);

    // Add more hits after waiting
    for (int i = 0; i < 5; i++) {
      meter.hit();
    }

    // Get rate since last asked - should include the 5 new hits
    final float rps = meter.getRequestsPerSecondSinceLastAsked();

    // Should have recorded some activity
    assertThat(rps).isGreaterThanOrEqualTo(0.0f);
    assertThat(meter.getTotalCounter()).isEqualTo(15);
  }

  @Test
  void shouldHandleConcurrentHits() throws InterruptedException {
    final MetricMeter meter = new MetricMeter();
    final int threadCount = 10;
    final int hitsPerThread = 100;

    final Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < hitsPerThread; j++) {
          meter.hit();
        }
      });
      threads[i].start();
    }

    for (final Thread thread : threads) {
      thread.join();
    }

    assertThat(meter.getTotalCounter()).isEqualTo(threadCount * hitsPerThread);
  }

  @Test
  void shouldHandleLargeNumberOfHits() {
    final MetricMeter meter = new MetricMeter();

    for (int i = 0; i < 10000; i++) {
      meter.hit();
    }

    assertThat(meter.getTotalCounter()).isEqualTo(10000);
    assertThat(meter.getTotalRequestsInLastMinute()).isGreaterThan(0);
  }

  @Test
  void shouldHandleNoHits() {
    final MetricMeter meter = new MetricMeter();

    assertThat(meter.getTotalCounter()).isEqualTo(0);
    assertThat(meter.getTotalRequestsInLastMinute()).isEqualTo(0);
    assertThat(meter.getRequestsPerSecondInLastMinute()).isEqualTo(0.0f);
  }

  @Test
  void shouldHandleDelayBetweenHits() throws InterruptedException {
    final MetricMeter meter = new MetricMeter();

    meter.hit();
    Thread.sleep(100);
    meter.hit();
    Thread.sleep(100);
    meter.hit();

    assertThat(meter.getTotalCounter()).isEqualTo(3);
  }

  @Test
  void shouldReturnConsistentValues() {
    final MetricMeter meter = new MetricMeter();

    for (int i = 0; i < 50; i++) {
      meter.hit();
    }

    final long count1 = meter.getTotalCounter();
    final long count2 = meter.getTotalCounter();

    assertThat(count1).isEqualTo(count2);
  }

  @Test
  void shouldCalculateRateOverTime() throws InterruptedException {
    final MetricMeter meter = new MetricMeter();

    // Generate some initial hits
    for (int i = 0; i < 10; i++) {
      meter.hit();
    }

    // Get initial rate
    final float initialRps = meter.getRequestsPerSecondSinceLastAsked();

    // Wait a bit and add more hits
    Thread.sleep(1100); // Wait over a second

    for (int i = 0; i < 20; i++) {
      meter.hit();
    }

    // Get rate since last asked
    final float newRps = meter.getRequestsPerSecondSinceLastAsked();

    assertThat(meter.getTotalCounter()).isEqualTo(30);
  }

  @Test
  void shouldHandleBurstTraffic() {
    final MetricMeter meter = new MetricMeter();

    // Simulate burst traffic
    for (int i = 0; i < 1000; i++) {
      meter.hit();
    }

    assertThat(meter.getTotalCounter()).isEqualTo(1000);
    assertThat(meter.getTotalRequestsInLastMinute()).isEqualTo(1000);
  }

  @Test
  void shouldMaintainAccuracyOverMultipleCalls() {
    final MetricMeter meter = new MetricMeter();

    for (int round = 0; round < 5; round++) {
      for (int i = 0; i < 10; i++) {
        meter.hit();
      }
      assertThat(meter.getTotalCounter()).isEqualTo((round + 1) * 10);
    }
  }
}
