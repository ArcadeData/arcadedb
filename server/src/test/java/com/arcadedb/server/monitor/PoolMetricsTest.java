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

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link PoolMetrics} {@code MeterBinder} registers the expected Micrometer gauges
 * for both the QueryEngineManager pool and the SparseVectorScoringPool. Uses a fresh
 * {@link SimpleMeterRegistry} so the test is isolated from the global registry that
 * {@link com.arcadedb.server.ArcadeDBServer} configures in production.
 */
class PoolMetricsTest {

  private static final Set<String> EXPECTED_GAUGE_NAMES = Set.of(
      "arcadedb.executor.pool.size",
      "arcadedb.executor.pool.active",
      "arcadedb.executor.queue.depth",
      "arcadedb.executor.queue.capacity_remaining",
      "arcadedb.executor.tasks.completed",
      "arcadedb.executor.tasks.caller_run_fallbacks");

  @Test
  void registersGaugesForBothPoolsWithPoolTag() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    // Every expected gauge must exist for both the "query" and "sparse_vector" pools. Anything
    // missing means a downstream dashboard would have a hole.
    final Set<String> registeredNames = registry.getMeters().stream()
        .map(m -> m.getId().getName())
        .collect(Collectors.toSet());
    assertThat(registeredNames).as("all six gauges should be registered (per-pool tagging adds duplicates)")
        .containsAll(EXPECTED_GAUGE_NAMES);

    for (final String poolTag : new String[] { "query", "sparse_vector" }) {
      for (final String gaugeName : EXPECTED_GAUGE_NAMES) {
        final Meter meter = registry.find(gaugeName).tag("pool", poolTag).meter();
        assertThat(meter)
            .as("gauge '%s' tagged pool=%s must be registered", gaugeName, poolTag)
            .isNotNull();
      }
    }
  }

  /**
   * Each gauge re-reads its source pool's stats on scrape. The values must be sane (non-NaN,
   * non-negative for everything except potentially null counters which we treat as zero) on a
   * freshly-constructed singleton.
   */
  @Test
  void gaugesProduceSensibleValuesAtRest() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    for (final String poolTag : new String[] { "query", "sparse_vector" }) {
      for (final String gaugeName : EXPECTED_GAUGE_NAMES) {
        final double value = registry.find(gaugeName).tag("pool", poolTag).gauge().value();
        assertThat(value).as("%s pool=%s should report a finite, non-negative value", gaugeName, poolTag)
            .isFinite()
            .isGreaterThanOrEqualTo(0.0);
      }
    }
  }

  /**
   * Studio's {@code studio-server.js} reads {@code metrics.executors.<pool>.<gauge>} from the
   * {@code GET /api/v1/server} JSON response - so the wire format produced by
   * {@link com.arcadedb.server.http.handler.GetServerHandler#buildExecutorsJSON} has to match
   * what the JS expects: one object per pool tag, each holding the six gauges keyed by their
   * post-prefix names ({@code "pool.size"}, {@code "pool.active"}, ...). This test pins that
   * contract without booting a full server - if the JSON shape ever changes, the dashboard
   * would silently render zeros and only this test would catch the regression.
   */
  @Test
  void executorsJsonMatchesStudioContract() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    final com.arcadedb.serializer.json.JSONObject executors =
        com.arcadedb.server.http.handler.GetServerHandler.buildExecutorsJSON(registry);

    for (final String poolTag : new String[] { "query", "sparse_vector" }) {
      assertThat(executors.has(poolTag))
          .as("executors JSON must have key '%s' (Studio dashboard reads this exact tag)", poolTag).isTrue();
      final com.arcadedb.serializer.json.JSONObject pool = executors.getJSONObject(poolTag);
      for (final String gaugeName : EXPECTED_GAUGE_NAMES) {
        // Studio reads e.g. metrics.executors.query["pool.size"] - the post-prefix name.
        final String shortName = gaugeName.substring("arcadedb.executor.".length());
        assertThat(pool.has(shortName))
            .as("pool=%s must expose gauge '%s' in JSON", poolTag, shortName).isTrue();
        // Sanity: numeric, finite. Studio rounds with Math.round, so a non-numeric value would
        // render as NaN.
        assertThat(pool.getDouble(shortName))
            .as("pool=%s gauge '%s' must be a finite number", poolTag, shortName).isFinite();
      }
    }
  }
}
