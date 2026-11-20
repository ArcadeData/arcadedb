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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.HAReplicationStats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.InPlaceRestartStats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaFullLoadStats;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaInstalmentSample;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.UnreferencedFilesSample;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
    final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(
        new HAReplicationStats(true, 1234L, 7L, 2)));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      assertThat(registry.find("arcadedb.ha.leader").gauge().value()).isEqualTo(1.0);
      assertThat(registry.find("arcadedb.ha.follower.max_last_contact_ms").gauge().value()).isEqualTo(1234.0);
      assertThat(registry.find("arcadedb.ha.follower.max_replication_lag").gauge().value()).isEqualTo(7.0);
      assertThat(registry.find("arcadedb.ha.followers.tracked").gauge().value()).isEqualTo(2.0);
    }
  }

  @Test
  void gaugesReportNotApplicableWhenNoHAPlugin() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      // HA disabled: leader=0 and the follower gauges are -1 (N/A) so dashboards can filter them out.
      assertThat(registry.find("arcadedb.ha.leader").gauge().value()).isEqualTo(0.0);
      assertThat(registry.find("arcadedb.ha.follower.max_last_contact_ms").gauge().value()).isEqualTo(-1.0);
      assertThat(registry.find("arcadedb.ha.follower.max_replication_lag").gauge().value()).isEqualTo(-1.0);
      assertThat(registry.find("arcadedb.ha.followers.tracked").gauge().value()).isEqualTo(0.0);
    }
  }

  @Test
  void perFollowerGaugesAreTaggedByPeer() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(
        new HAReplicationStats(true, 900, 4200, 2),
        List.of(
            new FollowerSample("n1", 100, 101, 12, 30, "HEALTHY", 0),
            new FollowerSample("slow", 50, 51, 4200, 900, "FALLING_BEHIND", 8000))));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

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
  }

  @Test
  void gaugeValuesAreFinite() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(
        new HAReplicationStats(false, -1, -1, 0)));

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
      metrics.bindTo(registry);

      for (final String gauge : new String[] {
          "arcadedb.ha.leader", "arcadedb.ha.follower.max_last_contact_ms",
          "arcadedb.ha.follower.max_replication_lag", "arcadedb.ha.followers.tracked" })
        assertThat(registry.find(gauge).gauge().value()).as("%s must be finite", gauge).isFinite();
    }
  }

  @Test
  void closeShutsDownTheFollowerMetricsScheduler() throws Exception {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    final HAReplicationMetrics metrics = new HAReplicationMetrics(server);
    metrics.bindTo(new SimpleMeterRegistry());

    final ScheduledExecutorService scheduler = schedulerField(metrics);
    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isFalse();

    metrics.close();

    assertThat(scheduler.isShutdown()).isTrue();
    assertThat(schedulerField(metrics)).isNull();
  }

  @Test
  void closeIsNullSafeWhenNeverBound() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    new HAReplicationMetrics(server).close();
  }

  private static ScheduledExecutorService schedulerField(final HAReplicationMetrics metrics) throws Exception {
    final Field field = HAReplicationMetrics.class.getDeclaredField("followerMetricsScheduler");
    field.setAccessible(true);
    return (ScheduledExecutorService) field.get(metrics);
  }

  /**
   * Issue #6144: schema-WAL instalments, and what they cost, as operator-facing metrics.
   * <p>
   * A schema session too large for one Raft entry ships it in instalments, and each one is a quorum round trip taken
   * WHILE THE DATABASE WRITE LOCK IS HELD. Before this the only signals were a JVM-wide counter whose sole consumer
   * was a test and a detailed-level HA log line - which an operator debugging "every write on this database stalled
   * for a while" could only enable by reproducing the stall.
   * <p>
   * The dimension is what makes the numbers mean anything: a server hosting several databases needs to know WHICH one
   * stalled its writers, and a process-wide counter cannot say. Issue #6143's unreferenced-file count rides the same
   * per-database refresh, for the same reason.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaInstalments {
    /** Minimal plugin that is both a ServerPlugin (discoverable) and a stats provider. */
    private record FakeHAPlugin(List<SchemaInstalmentSample> instalments,
                                List<UnreferencedFilesSample> unreferenced) implements ServerPlugin,
        HAReplicationStatsProvider {
      @Override
      public void startService() {
      }

      @Override
      public HAReplicationStats getHAReplicationStats() {
        return new HAReplicationStats(true, -1, -1, 0);
      }

      @Override
      public List<SchemaInstalmentSample> getSchemaInstalmentSamples() {
        return instalments;
      }

      @Override
      public List<UnreferencedFilesSample> getUnreferencedFilesSamples() {
        return unreferenced;
      }
    }

    @Test
    void everyDatabaseGetsItsOwnInstalmentCountAndDuration() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(
          List.of(new SchemaInstalmentSample("busy", 37, 4_200, 900),
              new SchemaInstalmentSample("idle", 0, 0, 0)),
          List.of()));

      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        assertThat(gauge(registry, "arcadedb.ha.schema.instalments", "busy")).isEqualTo(37.0);
        assertThat(gauge(registry, "arcadedb.ha.schema.instalment_time_ms", "busy"))
            .as("the duration is the number that matters: it is write-lock time every other writer waited out")
            .isEqualTo(4_200.0);
        assertThat(gauge(registry, "arcadedb.ha.schema.instalment_max_time_ms", "busy"))
            .as("and the max is what separates many fast round trips from a few that waited on a slow quorum member")
            .isEqualTo(900.0);

        assertThat(gauge(registry, "arcadedb.ha.schema.instalments", "idle"))
            .as("a database that never crossed the threshold reports zero rather than nothing, so a dashboard can "
                + "tell 'never happened' from 'not being measured'")
            .isZero();
      }
    }

    @Test
    void unreferencedFilesAreReportedPerDatabaseToo() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(List.of(),
          List.of(new UnreferencedFilesSample("leaky", 2), new UnreferencedFilesSample("clean", 0))));

      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        assertThat(gauge(registry, "arcadedb.ha.schema.unreferenced_files", "leaky"))
            .as("only the node that LOST leadership logs the files it could not retire; this is how the nodes still "
                + "holding them say so")
            .isEqualTo(2.0);
        assertThat(gauge(registry, "arcadedb.ha.schema.unreferenced_files", "clean")).isZero();
      }
    }

    /** With HA disabled there is no per-database row at all, rather than a row full of placeholders. */
    @Test
    void nothingIsPublishedWhenHAIsDisabled() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();

      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        assertThat(registry.find("arcadedb.ha.schema.instalments").gauges()).isEmpty();
        assertThat(registry.find("arcadedb.ha.schema.unreferenced_files").gauges()).isEmpty();
      }
    }

    /**
     * The per-database gauges share one scheduled task with the per-follower ones, and
     * {@code scheduleAtFixedRate} cancels a task PERMANENTLY the first time it throws - no further execution, ever,
     * and nothing says so. A refresh that fails must therefore cost only its own gauges, or one bad tick would freeze
     * every gauge on this binder for the rest of the process.
     */
    @Test
    void aFailingRefreshCostsOnlyItsOwnGauges() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();

      final boolean[] laterRefreshRan = { false };
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.runGuarded(() -> {
          throw new StackOverflowError("a refresh blew up");
        }, () -> laterRefreshRan[0] = true);
      }

      assertThat(laterRefreshRan[0])
          .as("the refreshes after a failing one must still run, and the failure must not leave the tick")
          .isTrue();
    }

    /**
     * Issue #10969: a refresh must UPDATE the meter of a database it already publishes, not remove and re-register it.
     * Re-registering swaps the meter inside the registry while {@code GET /prometheus} may be iterating it, and the
     * scrape then sees one label set twice and answers 400 {@code DuplicateLabelsException} for the whole node. The
     * observable contract is that the meter of a database that stays open is the SAME instance across refreshes
     * while its value follows the new sample, and that a database that disappears loses its meter.
     */
    @Test
    void aRefreshKeepsTheMeterOfADatabaseThatStaysAndOnlyChangesItsValue() {
      final List<SchemaInstalmentSample> instalments = new ArrayList<>(List.of(new SchemaInstalmentSample("busy", 1, 10, 10),
          new SchemaInstalmentSample("gone", 2, 20, 20)));
      final List<UnreferencedFilesSample> unreferenced = new ArrayList<>(List.of(new UnreferencedFilesSample("busy", 3)));
      final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(instalments, unreferenced));

      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        final Gauge instalmentsBefore = registry.get("arcadedb.ha.schema.instalments").tag("database", "busy").gauge();
        final Gauge unreferencedBefore = registry.get("arcadedb.ha.schema.unreferenced_files").tag("database", "busy").gauge();

        instalments.set(0, new SchemaInstalmentSample("busy", 5, 50, 30));
        instalments.remove(1);
        unreferenced.set(0, new UnreferencedFilesSample("busy", 7));
        metrics.refreshMultiGauges();

        assertThat(registry.get("arcadedb.ha.schema.instalments").tag("database", "busy").gauge())
            .as("re-registering the meter of a database that stayed is what races a concurrent scrape")
            .isSameAs(instalmentsBefore);
        assertThat(registry.get("arcadedb.ha.schema.unreferenced_files").tag("database", "busy").gauge())
            .isSameAs(unreferencedBefore);
        assertThat(instalmentsBefore.value()).isEqualTo(5.0);
        assertThat(gauge(registry, "arcadedb.ha.schema.instalment_max_time_ms", "busy")).isEqualTo(30.0);
        assertThat(unreferencedBefore.value()).isEqualTo(7.0);
        assertThat(registry.find("arcadedb.ha.schema.instalments").tag("database", "gone").gauge())
            .as("a database no longer reported must not keep a stale meter").isNull();

        instalments.add(new SchemaInstalmentSample("fresh", 9, 90, 90));
        metrics.refreshMultiGauges();
        assertThat(gauge(registry, "arcadedb.ha.schema.instalments", "fresh")).isEqualTo(9.0);
        assertThat(registry.get("arcadedb.ha.schema.instalments").tag("database", "busy").gauge()).isSameAs(instalmentsBefore);
      }
    }

    /** The same database listed twice in one sample set must publish one meter, not fail the refresh. */
    @Test
    void aDatabaseListedTwiceInOneRefreshPublishesOneMeter() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create().plugins(new FakeHAPlugin(
          List.of(new SchemaInstalmentSample("dup", 1, 1, 1), new SchemaInstalmentSample("dup", 4, 4, 4)), List.of()));

      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        assertThat(registry.find("arcadedb.ha.schema.instalments").tag("database", "dup").gauges()).hasSize(1);
        assertThat(gauge(registry, "arcadedb.ha.schema.instalments", "dup")).isEqualTo(4.0);
      }
    }

    private static double gauge(final SimpleMeterRegistry registry, final String name, final String database) {
      return registry.get(name).tags(Tags.of("database", database)).gauge().value();
    }
  }

  /**
   * Issue #7529: the replicated-permission refresh counters have to reach a scrape, or an operator still has
   * nothing to alert on. Pins the {@code arcadedb.ha.security.*} meters {@link HAReplicationMetrics} registers, and
   * the reading they give on a server whose security service is not installed - which is what a scrape taken during
   * startup or shutdown sees.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SecurityRefresh {
    private static final String CONFIG_PATH = "target/test-security-7529-metrics";

    private ServerSecurity      security;
    private FakeArcadeDBServer  server;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
      final File dir = new File(CONFIG_PATH);
      if (dir.exists())
        FileUtils.deleteRecursively(dir);
      assertThat(dir.mkdirs()).isTrue();

      server = FakeArcadeDBServer.create();

      security = new ServerSecurity(server, new ContextConfiguration(), CONFIG_PATH);
      server.security(security);

      registry = new SimpleMeterRegistry();
    }

    @AfterEach
    void tearDown() {
      if (security != null)
        security.stopService();
      if (registry != null)
        registry.close();
      GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
      FileUtils.deleteRecursively(new File(CONFIG_PATH));
    }

    @Test
    void theGaugesReadThroughToTheLiveCounters() {
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        assertThat(registry.find("arcadedb.ha.security.entries_applied").gauge().value()).isZero();
        assertThat(registry.find("arcadedb.ha.security.last_entry_applied_at").gauge().value()).isZero();

        security.applyReplicatedGroups(groupDocument());

        // Re-read on scrape, not captured at bind time: a gauge that snapshotted its value would report the node
        // as never having applied anything for the life of the process.
        assertThat(registry.find("arcadedb.ha.security.entries_applied").gauge().value()).isEqualTo(1.0);
        assertThat(registry.find("arcadedb.ha.security.last_entry_applied_at").gauge().value()).isPositive();
      }
    }

    @Test
    void everyGaugeThisIssueAddsIsRegistered() {
      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(server)) {
        metrics.bindTo(registry);

        // One gauge per member of PermissionRefreshMetrics.Snapshot, and the list is exhaustive on purpose:
        // CoreApiSpec tells operators that every value of the ha.securityRefresh section is scrapeable under
        // arcadedb.ha.security.*, so a member added to the record without a gauge makes that sentence false.
        for (final String name : List.of("arcadedb.ha.security.entries_applied",
            "arcadedb.ha.security.refreshes_requested", "arcadedb.ha.security.refreshes_coalesced",
            "arcadedb.ha.security.sweeps_completed", "arcadedb.ha.security.sweeps_failed",
            "arcadedb.ha.security.databases_refreshed", "arcadedb.ha.security.database_refresh_failures",
            "arcadedb.ha.security.last_entry_applied_at", "arcadedb.ha.security.last_sweep_at"))
          assertThat(registry.find(name).gauge()).as("%s must be registered", name).isNotNull();
      }
    }

    /** A scrape that lands before the security service is installed, or after it is gone, reads zeros - not a throw. */
    @Test
    void theGaugesDegradeWhenThereIsNoSecurityService() {
      final FakeArcadeDBServer bare = FakeArcadeDBServer.create();

      try (final HAReplicationMetrics metrics = new HAReplicationMetrics(bare)) {
        metrics.bindTo(registry);

        assertThat(registry.find("arcadedb.ha.security.entries_applied").gauge().value()).isZero();
        assertThat(registry.find("arcadedb.ha.security.sweeps_failed").gauge().value()).isZero();
      }
    }

    private static String groupDocument() {
      return new JSONObject()
          .put("version", ServerSecurity.LATEST_VERSION)
          .put("databases", new JSONObject().put("graph", new JSONObject().put("groups",
              new JSONObject().put("editors", new JSONObject()
                  .put("access", new JSONArray())
                  .put("resultSetLimit", -1L)
                  .put("readTimeout", -1L)))))
          .toString();
    }
  }

  /**
   * Issue #9429: the in-place Ratis restart counters (kept vs reformatted Raft storage) were reachable only through the
   * INFO line the restart logs, so the HA chaos harness scraped container logs for it and would have counted zero, and
   * passed, the day that line was reworded. {@link HAReplicationMetrics} publishes them as the counters
   * {@code arcadedb.ha.in_place_restarts.recovered} and {@code arcadedb.ha.in_place_restarts.reformatted}.
   */
  @Nested
  class InPlaceRestarts {
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

  /**
   * Issue #10020: a replica falling back from the incremental to a full schema load on its apply thread was silent. The
   * fallback is published as the counter {@code arcadedb.ha.schema.full_load.fallbacks} and the timer
   * {@code arcadedb.ha.schema.full_load}, both tagged by reason.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaFullLoad {
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
}
