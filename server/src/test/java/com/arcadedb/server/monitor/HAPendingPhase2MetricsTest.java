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

import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.PendingPhase2Stats;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5410, as #6965 and #11318 left it: {@link HAReplicationMetrics} publishes the transactions the Raft apply thread has
 * not reached yet as {@code arcadedb.ha.phase2.*} and degrades to "nothing pending" when HA is disabled. The replay floor gauge
 * stays for dashboards and is always -1, since nothing holds a snapshot checkpoint back any more.
 */
class HAPendingPhase2MetricsTest {

  /** Minimal plugin that is both a ServerPlugin (discoverable) and a stats provider. */
  private static final class FakeHAPlugin implements ServerPlugin, HAReplicationStatsProvider {
    private final PendingPhase2Stats phase2;

    FakeHAPlugin(final PendingPhase2Stats phase2) {
      this.phase2 = phase2;
    }

    @Override
    public void startService() {
    }

    @Override
    public HAReplicationStats getHAReplicationStats() {
      return new HAReplicationStats(true, -1, -1, 0);
    }

    @Override
    public PendingPhase2Stats getPendingPhase2Stats() {
      return phase2;
    }
  }

  @Test
  void gaugesExposeAnOutstandingPhase2Hold() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.plugins(new FakeHAPlugin(new PendingPhase2Stats(2, 620_000L)));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      assertThat(registry.find("arcadedb.ha.phase2.pending").gauge().value()).isEqualTo(2.0);
      assertThat(registry.find("arcadedb.ha.phase2.oldest_held_ms").gauge().value()).isEqualTo(620_000.0);
      assertThat(registry.find("arcadedb.ha.phase2.lowest_replay_floor").gauge().value())
          .as("since #6965 no checkpoint is held back, whatever is pending: the gauge stays for dashboards only")
          .isEqualTo(-1.0);
    }
  }

  @Test
  void gaugesReportNothingHeldOnAnIdleNode() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.plugins(new FakeHAPlugin(new PendingPhase2Stats(0, 0)));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      assertThat(registry.find("arcadedb.ha.phase2.pending").gauge().value()).isZero();
      assertThat(registry.find("arcadedb.ha.phase2.oldest_held_ms").gauge().value()).isZero();
      assertThat(registry.find("arcadedb.ha.phase2.lowest_replay_floor").gauge().value())
          .as("the value a dashboard filters out")
          .isEqualTo(-1.0);
    }
  }

  @Test
  void gaugesDegradeWhenHAIsDisabled() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      assertThat(registry.find("arcadedb.ha.phase2.pending").gauge().value()).isZero();
      assertThat(registry.find("arcadedb.ha.phase2.oldest_held_ms").gauge().value()).isZero();
      assertThat(registry.find("arcadedb.ha.phase2.lowest_replay_floor").gauge().value()).isEqualTo(-1.0);
    }
  }

  /** A provider that never overrides the default must still publish usable numbers. */
  @Test
  void providerDefaultReportsNothingHeld() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new ServerPlugin() {
      @Override
      public void startService() {
      }
    });

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      for (final String gauge : new String[] {
          "arcadedb.ha.phase2.pending", "arcadedb.ha.phase2.oldest_held_ms",
          "arcadedb.ha.phase2.lowest_replay_floor" })
        assertThat(registry.find(gauge).gauge().value()).as("%s must be finite", gauge).isFinite();
    }
  }
}
