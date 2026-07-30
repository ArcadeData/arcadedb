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
import com.arcadedb.database.QueryMetricsRecorder;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the lifecycle of the server-side Micrometer subsystem.
 * <p>
 * Micrometer's global composite registry, the meter binders and the per-tuple timer caches are JVM-wide,
 * while a server start/stop pair is not. A start that adds a backing registry and a stop that removes
 * none makes the composite grow one child per in-process restart, and a composite meter answers
 * {@code count()} from a single, arbitrarily chosen child: a child added after a recording starts at
 * zero, so a meter fed by an earlier generation still resolves through {@code find()} but reads back 0.
 * The subsystem must therefore be dismantled symmetrically - yet only by the last server out, because
 * HA and embedded setups run several servers in one JVM.
 */
class ServerMetricsLifecycleTest extends StaticBaseServerTest {
  // Protocol tag used only here, so these meters can never collide with another test's.
  private static final String PROTOCOL = "metrics-lifecycle";

  private final List<ArcadeDBServer> servers = new ArrayList<>();

  @AfterEach
  @Override
  public void endTest() {
    for (final ArcadeDBServer server : servers)
      if (server.isStarted())
        server.stop();
    servers.clear();
    super.endTest();
  }

  @Test
  void meterRegistryIsRemovedWhenTheServerStops() {
    final List<MeterRegistry> before = registries();

    final ArcadeDBServer server = startServer(0);
    assertThat(registries()).as("the server must install its own backing registry").hasSizeGreaterThan(before.size());

    server.stop();

    assertThat(registries()).as("a stopped server must not leave its registry in the global composite")
        .containsExactlyInAnyOrderElementsOf(before);
  }

  @Test
  void queryTimersDoNotOutliveTheServerThatRecordedThem() {
    final ArcadeDBServer server = startServer(0);
    recordQuery();
    assertThat(queryTimer()).as("the running server must record the query").isNotNull();

    server.stop();

    assertThat(queryTimer()).as("a meter whose backing registry is gone must not survive the shutdown").isNull();
  }

  @Test
  void queryTimerRecordsIntoTheLiveRegistryAfterARestart() {
    final List<MeterRegistry> beforeAnyStart = registries();

    startServer(0).stop();

    startServer(0);
    recordQuery();

    final List<MeterRegistry> installed = registries();
    installed.removeAll(beforeAnyStart);
    assertThat(installed).as("a restart must not leave the previous generation's registry behind").hasSize(1);

    // The recording must reach the registry that is actually being scraped now: neither the timer cached
    // by the previous generation nor its meter may be handed back.
    final Timer live = installed.get(0).find("arcadedb.query.duration").tag("protocol", PROTOCOL).timer();
    assertThat(live).as("the restarted server must record into its own registry").isNotNull();
    assertThat(live.count()).isEqualTo(1L);
  }

  @Test
  void queryRecorderIsRetiredWhenTheServerStops() {
    final ArcadeDBServer server = startServer(0);
    assertThat(QueryMetricsRecorder.Holder.get()).isNotSameAs(QueryMetricsRecorder.NO_OP);

    server.stop();

    assertThat(QueryMetricsRecorder.Holder.get()).as("with no server left the engine must stop timing queries")
        .isSameAs(QueryMetricsRecorder.NO_OP);
  }

  @Test
  void metricsSurviveWhileAnotherServerInTheSameJvmIsRunning() {
    final ArcadeDBServer first = startServer(0);
    final ArcadeDBServer second = startServer(1);
    recordQuery();

    first.stop();

    assertThat(queryTimer()).as("a still-running server keeps owning the metrics subsystem").isNotNull();
    assertThat(QueryMetricsRecorder.Holder.get()).isNotSameAs(QueryMetricsRecorder.NO_OP);

    second.stop();

    assertThat(queryTimer()).as("the last server out dismantles the subsystem").isNull();
  }

  @Test
  void aFailedStartDoesNotLeaveTheMetricsSubsystemInstalled() {
    // SSL requested with no key store: the HTTP service fails to start, which happens well after the
    // metrics install and does not go through stop().
    final ContextConfiguration config = serverConfiguration(0);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);

    final ArcadeDBServer failing = new ArcadeDBServer(config);
    servers.add(failing);
    assertThatThrownBy(failing::start).isInstanceOf(RuntimeException.class);

    // The install of the server that never came up must have been released, or the reference count could
    // never return to zero and no later server would be able to dismantle the subsystem.
    final ArcadeDBServer server = startServer(0);
    recordQuery();
    server.stop();

    assertThat(queryTimer()).as("a failed start must not pin the metrics subsystem").isNull();
    assertThat(QueryMetricsRecorder.Holder.get()).isSameAs(QueryMetricsRecorder.NO_OP);
  }

  private ArcadeDBServer startServer(final int index) {
    final ArcadeDBServer server = new ArcadeDBServer(serverConfiguration(index));
    servers.add(server);
    server.start();
    return server;
  }

  private static ContextConfiguration serverConfiguration(final int index) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "metrics_lifecycle_" + index);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + index);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    return config;
  }

  private static void recordQuery() {
    QueryMetricsRecorder.Holder.get().record(PROTOCOL, "graph", "sql", "query", 1_000_000L);
  }

  private static Timer queryTimer() {
    return Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", PROTOCOL).timer();
  }

  private static List<MeterRegistry> registries() {
    return new ArrayList<>(Metrics.globalRegistry.getRegistries());
  }
}
