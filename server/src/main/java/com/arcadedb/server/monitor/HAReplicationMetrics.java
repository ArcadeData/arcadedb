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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

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
            """
            Worst time (ms) since the leader last exchanged an RPC with any follower. Leading indicator of election churn: \
            as it approaches arcadedb.ha.electionTimeoutMin a follower is about to start a new election. -1 when not leader.""")
        .baseUnit("milliseconds")
        .register(registry);

    Gauge.builder("arcadedb.ha.follower.max_replication_lag", () -> stats().maxFollowerReplicationLag())
        .description(
            """
            Worst number of committed entries any follower is behind the leader's commit index. Sustained growth means a \
            follower cannot keep up with the write rate (apply backpressure / reduce batch size). -1 when not leader.""")
        .register(registry);

    Gauge.builder("arcadedb.ha.followers.tracked", () -> stats().trackedFollowers())
        .description("Number of followers the leader is currently tracking. 0 when not leader.")
        .register(registry);

    bindPerFollowerGauges(registry);
  }

  /**
   * Registers per-follower gauges tagged with {@code peer=<id>} so Grafana can alert on a SPECIFIC
   * slow node (issue #4812), complementing the aggregate-max gauges above. Uses a {@link MultiGauge}
   * re-registered every 5s from a small daemon thread, which both refreshes the values and follows
   * membership changes (peers added/removed). Only meaningful on the leader. The 5s cadence matches
   * the leader's lag monitor, so the per-peer gauges and the cluster status table stay in step.
   */
  private void bindPerFollowerGauges(final MeterRegistry registry) {
    final MultiGauge lastContact = MultiGauge.builder("arcadedb.ha.follower.last_contact_ms")
        .description("""
            Per-follower ms since the leader last exchanged an RPC with it (tag peer=<id>). \
            Approaching arcadedb.ha.electionTimeoutMin means that node is about to trigger an election.""")
        .baseUnit("milliseconds")
        .register(registry);
    final MultiGauge replicationLag = MultiGauge.builder("arcadedb.ha.follower.replication_lag")
        .description("""
            Per-follower committed-entry lag behind the leader (tag peer=<id>). \
            Sustained growth on one node identifies the cluster's replication bottleneck.""")
        .register(registry);
    final MultiGauge laggingFor = MultiGauge.builder("arcadedb.ha.follower.lagging_for_ms")
        .description("""
            Per-follower ms it has been continuously non-HEALTHY (tag peer=<id>); 0 when healthy. \
            Distinguishes a constantly-slow node from a transient blip.""")
        .baseUnit("milliseconds")
        .register(registry);

    final Runnable refresh = () -> {
      try {
        final List<FollowerSample> samples = followerSamples();
        lastContact.register(samples.stream()
            .map(s -> MultiGauge.Row.of(Tags.of("peer", s.peerId()), s.lastContactMs())).toList(), true);
        replicationLag.register(samples.stream()
            .map(s -> MultiGauge.Row.of(Tags.of("peer", s.peerId()), s.replicationLag())).toList(), true);
        laggingFor.register(samples.stream()
            .map(s -> MultiGauge.Row.of(Tags.of("peer", s.peerId()), s.laggingForMs())).toList(), true);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Failed to refresh per-follower HA gauges: %s", e.getMessage());
      }
    };

    refresh.run(); // publish an initial (possibly empty) set immediately

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-ha-follower-metrics");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleAtFixedRate(refresh, 5, 5, TimeUnit.SECONDS);
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

  /** Per-follower samples from the started HA plugin, or empty when HA is disabled / not the leader. */
  private List<FollowerSample> followerSamples() {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof HAReplicationStatsProvider provider)
        return provider.getFollowerSamples();
    return List.of();
  }
}
