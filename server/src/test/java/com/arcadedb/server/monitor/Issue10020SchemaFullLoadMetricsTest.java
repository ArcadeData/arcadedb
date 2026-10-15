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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.schema.FullSchemaLoadReason;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaFullLoadStats;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10020: a replica falling back from the incremental to a full schema load on its apply thread was silent. The
 * fallback is published as the counter {@code arcadedb.ha.schema.full_load.fallbacks} and the timer
 * {@code arcadedb.ha.schema.full_load}, both tagged by reason.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10020SchemaFullLoadMetricsTest {
  private static final String FALLBACKS = "arcadedb.ha.schema.full_load.fallbacks";
  private static final String TIMER     = "arcadedb.ha.schema.full_load";

  @TempDir
  Path root;

  private static final class FakeHAPlugin implements ServerPlugin, HAReplicationStatsProvider {
    private final AtomicReference<SchemaFullLoadStats> stats = new AtomicReference<>(SchemaFullLoadStats.NONE);

    @Override
    public void startService() {
    }

    @Override
    public HAReplicationStats getHAReplicationStats() {
      return new HAReplicationStats(false, -1, -1, 0);
    }

    @Override
    public SchemaFullLoadStats getSchemaFullLoadStats() {
      return stats.get();
    }
  }

  private ArcadeDBServer serverWith(final List<ServerPlugin> plugins) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    return new ArcadeDBServer(configuration) {
      @Override
      public Collection<ServerPlugin> getPlugins() {
        return plugins;
      }
    };
  }

  @Test
  void everyReasonHasItsOwnSeriesFromTheFirstScrape() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(serverWith(List.of(new FakeHAPlugin())))) {
      metrics.bindTo(registry);

      for (final FullSchemaLoadReason reason : FullSchemaLoadReason.values()) {
        assertThat(registry.find(FALLBACKS).tag("reason", reason.getTag()).functionCounter().count())
            .as("fallbacks for %s", reason).isZero();
        assertThat(registry.find(TIMER).tag("reason", reason.getTag()).functionTimer().count())
            .as("timer for %s", reason).isZero();
      }
    }
  }

  @Test
  void metersFollowTheProvidersCountsOnEveryScrape() {
    final FakeHAPlugin plugin = new FakeHAPlugin();
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(serverWith(List.of(plugin)))) {
      metrics.bindTo(registry);

      final int n = FullSchemaLoadReason.values().length;
      final long[] fallbacks = new long[n];
      final long[] completed = new long[n];
      final long[] nanos = new long[n];
      // Three started, two finished (one threw), over 3 ms
      fallbacks[FullSchemaLoadReason.FILE_RETIRED.ordinal()] = 3;
      completed[FullSchemaLoadReason.FILE_RETIRED.ordinal()] = 2;
      nanos[FullSchemaLoadReason.FILE_RETIRED.ordinal()] = TimeUnit.MILLISECONDS.toNanos(3);
      plugin.stats.set(new SchemaFullLoadStats(fallbacks, completed, nanos));

      final String tag = FullSchemaLoadReason.FILE_RETIRED.getTag();
      assertThat(registry.find(FALLBACKS).tag("reason", tag).functionCounter().count()).isEqualTo(3.0);
      assertThat(registry.find(TIMER).tag("reason", tag).functionTimer().count()).isEqualTo(2.0);
      assertThat(registry.find(TIMER).tag("reason", tag).functionTimer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(3.0);

      // Another reason is untouched
      assertThat(registry.find(FALLBACKS).tag("reason", FullSchemaLoadReason.FILE_RENAMED.getTag()).functionCounter().count())
          .isZero();
    }
  }

  @Test
  void metersReadZeroWhenHAIsDisabled() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(serverWith(List.of()))) {
      metrics.bindTo(registry);

      assertThat(registry.find(FALLBACKS).functionCounters()).hasSize(FullSchemaLoadReason.values().length)
          .allSatisfy(counter -> assertThat(counter.count()).isZero());
    }
  }

  @Test
  void aProviderThatDoesNotOverrideTheDefaultReportsNone() {
    final HAReplicationStatsProvider provider = () -> new HAReplicationStats(false, -1, -1, 0);

    for (final FullSchemaLoadReason reason : FullSchemaLoadReason.values()) {
      assertThat(provider.getSchemaFullLoadStats().fallbacks(reason)).isZero();
      assertThat(provider.getSchemaFullLoadStats().completed(reason)).isZero();
      assertThat(provider.getSchemaFullLoadStats().totalNanos(reason)).isZero();
    }
  }
}
