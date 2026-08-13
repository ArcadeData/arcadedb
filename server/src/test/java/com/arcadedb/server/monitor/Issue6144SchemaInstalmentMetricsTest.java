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
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaInstalmentSample;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.UnreferencedFilesSample;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
class Issue6144SchemaInstalmentMetricsTest {

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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of(new FakeHAPlugin(
        List.of(new SchemaInstalmentSample("busy", 37, 4_200, 900),
            new SchemaInstalmentSample("idle", 0, 0, 0)),
        List.of())));

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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of(new FakeHAPlugin(List.of(),
        List.of(new UnreferencedFilesSample("leaky", 2), new UnreferencedFilesSample("clean", 0)))));

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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of());

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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of());

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

  private static double gauge(final SimpleMeterRegistry registry, final String name, final String database) {
    return registry.get(name).tags(Tags.of("database", database)).gauge().value();
  }
}
