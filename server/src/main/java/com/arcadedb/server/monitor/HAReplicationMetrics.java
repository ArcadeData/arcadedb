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
import com.arcadedb.server.monitor.HAReplicationStatsProvider.PendingPhase2Stats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaInstalmentSample;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.UnreferencedFilesSample;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.io.Closeable;
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
public final class HAReplicationMetrics implements MeterBinder, Closeable {
  private final ArcadeDBServer server;

  private volatile ScheduledExecutorService followerMetricsScheduler;

  public HAReplicationMetrics(final ArcadeDBServer server) {
    this.server = server;
  }

  /**
   * Shuts down the per-follower gauge refresh scheduler started by {@link #bindTo(MeterRegistry)}.
   * The caller that binds this instance to a server's registry on plugin start must close it on
   * plugin stop, the same lifecycle the sibling {@code stopService()} of the metrics plugins follow,
   * or the daemon thread pool leaks one instance per restart.
   */
  @Override
  public void close() {
    final ScheduledExecutorService scheduler = followerMetricsScheduler;
    if (scheduler != null) {
      scheduler.shutdownNow();
      followerMetricsScheduler = null;
    }
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

    bindPendingPhase2Gauges(registry);

    // One scheduler for every re-registering MultiGauge on this binder, rather than one each: they all refresh at
    // the same cadence and none of them blocks, so a second thread would buy nothing.
    startMultiGaugeRefresh(bindPerFollowerGauges(registry), bindPerDatabaseGauges(registry));
  }

  /**
   * Registers the phase-2 hold gauges (issue #5410). A locally-originated entry that Raft committed
   * but this node has not written yet holds the snapshot checkpoint back so it stays replayable
   * (issue #5407). A hold that never clears pins Raft log purging until the node restarts, which
   * surfaces to operators as a Raft log that stops shrinking (issue #5345). Without these gauges the
   * only signal is a throttled WARNING emitted at most once per compaction interval.
   */
  private void bindPendingPhase2Gauges(final MeterRegistry registry) {
    Gauge.builder("arcadedb.ha.phase2.pending", () -> pendingPhase2().pending())
        .description("Local phase-2 applies still holding the Raft snapshot checkpoint back. "
            + "Sustained non-zero means log compaction is pinned until this node restarts.")
        .register(registry);

    Gauge.builder("arcadedb.ha.phase2.oldest_held_ms", () -> pendingPhase2().oldestHeldMs())
        .description("Age (ms) of the oldest phase-2 hold; 0 when none. A value that keeps growing "
            + "identifies a stuck hold rather than ordinary in-flight commits.")
        .baseUnit("milliseconds")
        .register(registry);

    Gauge.builder("arcadedb.ha.phase2.lowest_replay_floor", () -> pendingPhase2().lowestReplayFloor())
        .description("Raft index past which the log cannot be purged while a phase-2 hold is "
            + "outstanding; -1 when nothing is held.")
        .register(registry);
  }

  /**
   * Registers per-follower gauges tagged with {@code peer=<id>} so Grafana can alert on a SPECIFIC
   * slow node (issue #4812), complementing the aggregate-max gauges above. Uses a {@link MultiGauge}
   * re-registered every 5s from a small daemon thread, which both refreshes the values and follows
   * membership changes (peers added/removed). Only meaningful on the leader. The 5s cadence matches
   * the leader's lag monitor, so the per-peer gauges and the cluster status table stay in step.
   */
  private Runnable bindPerFollowerGauges(final MeterRegistry registry) {
    final MultiGauge lastContact = MultiGauge.builder("arcadedb.ha.follower.last_contact_ms")
        .description("Per-follower ms since the leader last exchanged an RPC with it (tag peer=<id>). "
            + "Approaching arcadedb.ha.electionTimeoutMin means that node is about to trigger an election.")
        .baseUnit("milliseconds")
        .register(registry);
    final MultiGauge replicationLag = MultiGauge.builder("arcadedb.ha.follower.replication_lag")
        .description("Per-follower committed-entry lag behind the leader (tag peer=<id>). "
            + "Sustained growth on one node identifies the cluster's replication bottleneck.")
        .register(registry);
    final MultiGauge laggingFor = MultiGauge.builder("arcadedb.ha.follower.lagging_for_ms")
        .description("Per-follower ms it has been continuously non-HEALTHY (tag peer=<id>); 0 when healthy. "
            + "Distinguishes a constantly-slow node from a transient blip.")
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

    return refresh;
  }

  /**
   * Registers the per-database gauges: what a schema session's WAL instalments have cost (issue #6144) and how many
   * files this node holds that no schema component claims (issue #6143).
   * <p>
   * Tagged {@code database=<name>} because neither number means anything aggregated over a multi-database server -
   * "which database stalls its writers while it ships instalments" and "which database is leaking files" are the
   * questions, and a single JVM-wide counter cannot answer either. Refreshed on the shared timer rather than read
   * per scrape: the unreferenced-files count walks the file list, and a scrape is not the place to pay for that.
   */
  private Runnable bindPerDatabaseGauges(final MeterRegistry registry) {
    final MultiGauge instalments = MultiGauge.builder("arcadedb.ha.schema.instalments")
        .description("Schema-WAL instalments this database has shipped since it opened (tag database=<name>). "
            + "Non-zero means a schema session - typically an index rebuild, including the one CHECK DATABASE FIX "
            + "performs - was too large for one Raft entry and shipped incrementally.")
        .register(registry);
    final MultiGauge instalmentTime = MultiGauge.builder("arcadedb.ha.schema.instalment_time_ms")
        .description("Total ms this database spent shipping schema-WAL instalments (tag database=<name>). Each one "
            + "is a quorum round trip taken with the database write lock held, so this is write-lock time every "
            + "other writer on the database waited out.")
        .baseUnit("milliseconds")
        .register(registry);
    final MultiGauge instalmentMaxTime = MultiGauge.builder("arcadedb.ha.schema.instalment_max_time_ms")
        .description("Longest single schema-WAL instalment for this database (tag database=<name>). Separates many "
            + "fast round trips from a few that each waited on a slow quorum member; approaching "
            + "arcadedb.ha.quorumTimeout means one of them nearly timed out.")
        .baseUnit("milliseconds")
        .register(registry);
    final MultiGauge unreferencedFiles = MultiGauge.builder("arcadedb.ha.schema.unreferenced_files")
        .description("Paginated files this node holds that no schema component claims (tag database=<name>). "
            + "Nothing reads them, so this is wasted disk rather than a correctness problem; the usual cause is a "
            + "schema session that shipped instalments and lost leadership before it could retire them. "
            + "CHECK DATABASE names them.")
        .register(registry);

    return () -> {
      try {
        final List<SchemaInstalmentSample> schemaSamples = schemaInstalmentSamples();
        instalments.register(schemaSamples.stream()
            .map(s -> MultiGauge.Row.of(Tags.of("database", s.database()), s.instalments())).toList(), true);
        instalmentTime.register(schemaSamples.stream()
            .map(s -> MultiGauge.Row.of(Tags.of("database", s.database()), s.totalTimeMs())).toList(), true);
        instalmentMaxTime.register(schemaSamples.stream()
            .map(s -> MultiGauge.Row.of(Tags.of("database", s.database()), s.maxTimeMs())).toList(), true);

        unreferencedFiles.register(unreferencedFilesSamples().stream()
            .map(s -> MultiGauge.Row.of(Tags.of("database", s.database()), s.unreferencedFiles())).toList(), true);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Failed to refresh per-database HA gauges: %s", e.getMessage());
      }
    };
  }

  /**
   * Publishes an initial set immediately, then re-registers every {@code refresh} on one shared daemon timer.
   * <p>
   * EVERY RUN IS GUARDED, and that is what makes sharing one task safe. {@code scheduleAtFixedRate} cancels a task
   * PERMANENTLY the first time it throws - no further execution, ever, and nothing says so - so one refresh throwing
   * would silently freeze every gauge on this binder for the rest of the process, including the follower-lag ones
   * that have nothing to do with it. The refreshes each catch {@code Exception} themselves today; the guard here
   * catches {@link Throwable} because it is the one that must not be conditional on a future refresh remembering to
   * write its own, and because "this metric stopped updating" is a strictly worse outcome than a logged failure.
   * Each refresh is guarded SEPARATELY, so a failing one cannot take its siblings' turn with it either.
   */
  private void startMultiGaugeRefresh(final Runnable... refreshes) {
    // A second bindTo() on the same instance used to overwrite the field and leak the first scheduler's thread for
    // the life of the process. Nothing calls it twice today - the binder is created per plugin start - but the leak
    // was silent and this costs one line.
    close();

    for (final Runnable refresh : refreshes)
      refresh.run(); // publish an initial (possibly empty) set immediately

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      // Named for what it drives rather than for the first thing that needed it: since #6144 the same tick also
      // refreshes the per-database schema-instalment and unreferenced-file gauges, and a thread dump that still
      // said "follower" would send whoever is debugging those to the wrong place.
      final Thread t = new Thread(r, "arcadedb-ha-metrics-refresh");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleAtFixedRate(() -> runGuarded(refreshes), 5, 5, TimeUnit.SECONDS);
    followerMetricsScheduler = scheduler;
  }

  /**
   * One tick: every refresh runs, and a failing one costs only its own gauges. Extracted from the scheduling above
   * so the guarantee can be tested without waiting out a tick.
   */
  // @VisibleForTesting
  void runGuarded(final Runnable... refreshes) {
    for (final Runnable refresh : refreshes)
      try {
        refresh.run();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.WARNING,
            "A HA metrics refresh failed; the gauges it publishes keep their previous values and the next tick "
                + "will try again: %s", t.toString());
      }
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

  /** Phase-2 hold state from the started HA plugin, or "nothing held" when HA is disabled. */
  private PendingPhase2Stats pendingPhase2() {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof HAReplicationStatsProvider provider)
        return provider.getPendingPhase2Stats();
    return new PendingPhase2Stats(0, 0, -1);
  }

  /** Per-follower samples from the started HA plugin, or empty when HA is disabled / not the leader. */
  private List<FollowerSample> followerSamples() {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof HAReplicationStatsProvider provider)
        return provider.getFollowerSamples();
    return List.of();
  }

  /** Per-database schema-instalment samples from the started HA plugin, or empty when HA is disabled. */
  private List<SchemaInstalmentSample> schemaInstalmentSamples() {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof HAReplicationStatsProvider provider)
        return provider.getSchemaInstalmentSamples();
    return List.of();
  }

  /** Per-database unreferenced-file counts from the started HA plugin, or empty when HA is disabled. */
  private List<UnreferencedFilesSample> unreferencedFilesSamples() {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof HAReplicationStatsProvider provider)
        return provider.getUnreferencedFilesSamples();
    return List.of();
  }
}
