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
package com.arcadedb.server;

import com.arcadedb.server.monitor.MicrometerQueryMetricsRecorder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the registry-side cardinality backstops {@link ArcadeDBServer} installs on the RED timers.
 * The recorder and the HTTP handler already collapse client-influenced tag values onto constants, so these
 * filters exist for the case where that collapse is bypassed by a future route, a new wire protocol, or a
 * regression: past the budget the meter must be denied rather than registered forever (issues #6805, #7122).
 * <p>
 * Applied to a throwaway registry rather than to {@code Metrics.globalRegistry}, because a
 * {@code MeterFilter} can never be removed from a registry once installed.
 */
class ArcadeDBServerMetricsFiltersTest {

  @Test
  void theLanguageTagOfTheQueryTimerIsCappedInTheRegistry() {
    final MeterRegistry registry = new SimpleMeterRegistry();
    ArcadeDBServer.installCardinalityMeterFilters(registry.config());

    final int budget = MicrometerQueryMetricsRecorder.MAX_LANGUAGE_TAG_VALUES
        + MicrometerQueryMetricsRecorder.RESERVED_LANGUAGE_TAG_VALUES;
    for (int i = 0; i < budget; i++)
      queryTimer(registry, "lang" + i, "graph");

    // Everything inside the budget is registered, so real languages keep their own series.
    assertThat(registry.find("arcadedb.query.duration").timers()).hasSize(budget);

    queryTimer(registry, "one-language-too-many", "graph");

    assertThat(registry.find("arcadedb.query.duration").tag("language", "one-language-too-many").timer())
        .as("past the budget the meter must be denied, never registered permanently").isNull();
    assertThat(registry.find("arcadedb.query.duration").timers()).hasSize(budget);
  }

  @Test
  void theDatabaseTagOfTheQueryTimerIsCappedInTheRegistry() {
    final MeterRegistry registry = new SimpleMeterRegistry();
    ArcadeDBServer.installCardinalityMeterFilters(registry.config());

    final int budget = MicrometerQueryMetricsRecorder.MAX_DB_TAG_VALUES
        + MicrometerQueryMetricsRecorder.RESERVED_DB_TAG_VALUES;
    for (int i = 0; i < budget; i++)
      queryTimer(registry, "sql", "db" + i);

    queryTimer(registry, "sql", "one-database-too-many");

    assertThat(registry.find("arcadedb.query.duration").tag("db", "one-database-too-many").timer())
        .as("database-name churn must not grow the registry without bound").isNull();
    assertThat(registry.find("arcadedb.query.duration").timers()).hasSize(budget);
  }

  @Test
  void thePathTagOfTheHttpTimerIsStillCapped() {
    // The #5025 filter must survive the extraction that moved all four into one method.
    final MeterRegistry registry = new SimpleMeterRegistry();
    ArcadeDBServer.installCardinalityMeterFilters(registry.config());

    for (int i = 0; i < 100; i++)
      Timer.builder("arcadedb.http.requests").tag("path", "/p" + i).tag("db", "none").register(registry);

    Timer.builder("arcadedb.http.requests").tag("path", "/one-too-many").tag("db", "none").register(registry);

    assertThat(registry.find("arcadedb.http.requests").tag("path", "/one-too-many").timer()).isNull();
  }

  private static void queryTimer(final MeterRegistry registry, final String language, final String db) {
    Timer.builder("arcadedb.query.duration")
        .tag("protocol", "http")
        .tag("db", db)
        .tag("language", language)
        .tag("type", "query")
        .register(registry);
  }
}
