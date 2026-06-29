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
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binding for High-Availability replication health, so the heartbeat-lag and
 * replication-lag signals show up in {@code /api/v1/metrics} alongside the JVM and executor-pool
 * gauges. The data is sourced live, on each scrape, from whichever started plugin implements
 * {@link HAReplicationStatsProvider} (the Raft HA plugin); when HA is disabled or this node is not
 * the leader the gauges report {@code -1} (N/A) so dashboards can filter on {@code arcadedb.ha.leader}.
 * <p>
 * The single most actionable gauge is {@code arcadedb.ha.follower.max_last_contact_ms}: when it
 * climbs toward {@code arcadedb.ha.electionTimeoutMin} the leader is failing to keep heartbeats
 * flowing to a follower and an election (leader churn) is imminent. This makes the cause measurable
 * instead of guessed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class HAReplicationMetrics implements MeterBinder {
  private final ArcadeDBServer server;

  public HAReplicationMetrics(final ArcadeDBServer server) {
    this.server = server;
  }

  @Override
  public void bindTo(final MeterRegistry registry) {
    Gauge.builder("arcadedb.ha.leader", () -> stats().leader() ? 1 : 0)
        .description("1 when this node is the Raft leader, 0 otherwise. Use to scope the other arcadedb.ha.* gauges.")
        .register(registry);

    Gauge.builder("arcadedb.ha.follower.max_last_contact_ms", () -> stats().maxFollowerLastContactMs())
        .description(
            "Worst time (ms) since the leader last exchanged an RPC with any follower. Leading indicator of election churn: "
                + "as it approaches arcadedb.ha.electionTimeoutMin a follower is about to start a new election. -1 when not leader.")
        .baseUnit("milliseconds")
        .register(registry);

    Gauge.builder("arcadedb.ha.follower.max_replication_lag", () -> stats().maxFollowerReplicationLag())
        .description(
            "Worst number of committed entries any follower is behind the leader's commit index. Sustained growth means a "
                + "follower cannot keep up with the write rate (apply backpressure / reduce batch size). -1 when not leader.")
        .register(registry);

    Gauge.builder("arcadedb.ha.followers.tracked", () -> stats().trackedFollowers())
        .description("Number of followers the leader is currently tracking. 0 when not leader.")
        .register(registry);
  }

  /**
   * Reads a live replication-health snapshot from the started HA plugin, or a not-leader placeholder
   * when no HA plugin is present (HA disabled). Cheap enough to call once per gauge per scrape.
   */
  private HAReplicationStats stats() {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof HAReplicationStatsProvider provider)
        return provider.getHAReplicationStats();
    return new HAReplicationStats(false, -1, -1, 0);
  }
}
