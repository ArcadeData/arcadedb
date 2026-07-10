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

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies {@link HAReplicationMetrics} translates an {@link HAReplicationStatsProvider}'s snapshot
 * into the four {@code arcadedb.ha.*} gauges, and degrades to a not-leader placeholder when no HA
 * plugin is present (HA disabled). Uses a fresh {@link SimpleMeterRegistry} so it is isolated from
 * the global registry the real server configures.
 */
class HAReplicationMetricsTest {

  /** Minimal plugin that is both a ServerPlugin (discoverable) and a stats provider. */
  private static final class FakeHAPlugin implements ServerPlugin, HAReplicationStatsProvider {
    private final HAReplicationStats   stats;
    private final List<FollowerSample> samples;

    FakeHAPlugin(final HAReplicationStats stats) {
      this(stats, List.of());
    }

    FakeHAPlugin(final HAReplicationStats stats, final List<FollowerSample> samples) {
      this.stats = stats;
      this.samples = samples;
    }

    @Override
    public void startService() {
    }

    @Override
    public HAReplicationStats getHAReplicationStats() {
      return stats;
    }

    @Override
    public List<FollowerSample> getFollowerSamples() {
      return samples;
    }
  }

  @Test
  void gaugesReflectLeaderProviderSnapshot() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of(new FakeHAPlugin(
        new HAReplicationStats(true, 1234L, 7L, 2))));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new HAReplicationMetrics(server).bindTo(registry);

    assertThat(registry.find("arcadedb.ha.leader").gauge().value()).isEqualTo(1.0);
    assertThat(registry.find("arcadedb.ha.follower.max_last_contact_ms").gauge().value()).isEqualTo(1234.0);
    assertThat(registry.find("arcadedb.ha.follower.max_replication_lag").gauge().value()).isEqualTo(7.0);
    assertThat(registry.find("arcadedb.ha.followers.tracked").gauge().value()).isEqualTo(2.0);
  }

  @Test
  void gaugesReportNotApplicableWhenNoHAPlugin() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of());

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new HAReplicationMetrics(server).bindTo(registry);

    // HA disabled: leader=0 and the follower gauges are -1 (N/A) so dashboards can filter them out.
    assertThat(registry.find("arcadedb.ha.leader").gauge().value()).isEqualTo(0.0);
    assertThat(registry.find("arcadedb.ha.follower.max_last_contact_ms").gauge().value()).isEqualTo(-1.0);
    assertThat(registry.find("arcadedb.ha.follower.max_replication_lag").gauge().value()).isEqualTo(-1.0);
    assertThat(registry.find("arcadedb.ha.followers.tracked").gauge().value()).isEqualTo(0.0);
  }

  @Test
  void perFollowerGaugesAreTaggedByPeer() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of(new FakeHAPlugin(
        new HAReplicationStats(true, 900, 4200, 2),
        List.of(
            new FollowerSample("n1", 100, 101, 12, 30, "HEALTHY", 0),
            new FollowerSample("slow", 50, 51, 4200, 900, "FALLING_BEHIND", 8000)))));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new HAReplicationMetrics(server).bindTo(registry);

    // The initial refresh() runs synchronously in bindTo, so the per-peer rows exist immediately.
    assertThat(registry.find("arcadedb.ha.follower.replication_lag").tag("peer", "slow").gauge().value())
        .isEqualTo(4200.0);
    assertThat(registry.find("arcadedb.ha.follower.last_contact_ms").tag("peer", "slow").gauge().value())
        .isEqualTo(900.0);
    assertThat(registry.find("arcadedb.ha.follower.lagging_for_ms").tag("peer", "slow").gauge().value())
        .isEqualTo(8000.0);
    assertThat(registry.find("arcadedb.ha.follower.replication_lag").tag("peer", "n1").gauge().value())
        .isEqualTo(12.0);
  }

  @Test
  void gaugeValuesAreFinite() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of(new FakeHAPlugin(
        new HAReplicationStats(false, -1, -1, 0))));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new HAReplicationMetrics(server).bindTo(registry);

    for (final String gauge : new String[] {
        "arcadedb.ha.leader", "arcadedb.ha.follower.max_last_contact_ms",
        "arcadedb.ha.follower.max_replication_lag", "arcadedb.ha.followers.tracked" })
      assertThat(registry.find(gauge).gauge().value()).as("%s must be finite", gauge).isFinite();
  }
}
