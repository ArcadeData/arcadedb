/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.InPlaceRestartStats;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9429: the in-place Ratis restart counters (kept vs reformatted Raft storage) were reachable only through the
 * INFO line the restart logs, so the HA chaos harness scraped container logs for it and would have counted zero, and
 * passed, the day that line was reworded. {@link HAReplicationMetrics} publishes them as the counters
 * {@code arcadedb.ha.in_place_restarts.recovered} and {@code arcadedb.ha.in_place_restarts.reformatted}.
 */
class Issue9429InPlaceRestartMetricsTest {
  private static final String RECOVERED   = "arcadedb.ha.in_place_restarts.recovered";
  private static final String REFORMATTED = "arcadedb.ha.in_place_restarts.reformatted";

  @TempDir
  Path root;

  /** A plugin that is both discoverable and a stats provider, with restart counts the test moves by hand. */
  private static final class FakeHAPlugin implements ServerPlugin, HAReplicationStatsProvider {
    private final AtomicReference<InPlaceRestartStats> restarts = new AtomicReference<>(InPlaceRestartStats.NONE);

    @Override
    public void startService() {
    }

    @Override
    public HAReplicationStats getHAReplicationStats() {
      return new HAReplicationStats(false, -1, -1, 0);
    }

    @Override
    public InPlaceRestartStats getInPlaceRestartStats() {
      return restarts.get();
    }
  }

  /** An unstarted server whose plugin list is the given one: a real object rather than a mock (issue #9464). */
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
  void countersFollowTheProvidersCountsOnEveryScrape() {
    final FakeHAPlugin plugin = new FakeHAPlugin();
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(serverWith(List.of(plugin)))) {
      metrics.bindTo(registry);

      assertThat(registry.find(RECOVERED).functionCounter().count()).isZero();
      assertThat(registry.find(REFORMATTED).functionCounter().count()).isZero();

      // Read live, not captured at bind time: a restart after the binder was registered must show up on the next scrape
      plugin.restarts.set(new InPlaceRestartStats(3, 1));
      assertThat(registry.find(RECOVERED).functionCounter().count()).isEqualTo(3.0);
      assertThat(registry.find(REFORMATTED).functionCounter().count()).isEqualTo(1.0);
    }
  }

  @Test
  void countersReadZeroWhenHAIsDisabled() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(serverWith(List.of()))) {
      metrics.bindTo(registry);

      assertThat(registry.find(RECOVERED).functionCounter().count()).isZero();
      assertThat(registry.find(REFORMATTED).functionCounter().count()).isZero();
    }
  }

  @Test
  void aProviderThatDoesNotOverrideTheDefaultReportsNone() {
    final HAReplicationStatsProvider provider = () -> new HAReplicationStats(false, -1, -1, 0);

    assertThat(provider.getInPlaceRestartStats()).isEqualTo(new InPlaceRestartStats(0, 0));
  }
}
