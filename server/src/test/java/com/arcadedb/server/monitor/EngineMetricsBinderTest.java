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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link EngineMetricsBinder}: every curated engine gauge is registered and reads a
 * finite numeric value from the live {@code Profiler} snapshot, whose stats are nested JSON objects.
 */
class EngineMetricsBinderTest {

  @Test
  void registersEngineGauges() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    final Gauge cacheHits = registry.find("arcadedb.engine.page.cache.hits").gauge();
    assertThat(cacheHits).isNotNull();

    final Gauge openFiles = registry.find("arcadedb.engine.files.open").gauge();
    assertThat(openFiles).isNotNull();

    final Gauge walBytes = registry.find("arcadedb.engine.wal.bytes.written").gauge();
    assertThat(walBytes).isNotNull();
  }

  @Test
  void gaugesReadNestedProfilerValuesWithoutThrowing() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    // Profiler nests each stat as {count|space|value: N}; reading the gauge must extract the inner
    // numeric, not throw on the wrapping object, and never return NaN.
    final Gauge cacheHits = registry.find("arcadedb.engine.page.cache.hits").gauge();
    assertThat(cacheHits).isNotNull();
    assertThat(Double.isNaN(cacheHits.value())).isFalse();

    final Gauge walFiles = registry.find("arcadedb.engine.wal.files").gauge();
    assertThat(walFiles).isNotNull();
    assertThat(Double.isNaN(walFiles.value())).isFalse();
  }
}
