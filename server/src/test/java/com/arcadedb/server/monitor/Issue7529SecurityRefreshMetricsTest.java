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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7529: the replicated-permission refresh counters have to reach a scrape, or an operator still has
 * nothing to alert on. Pins the {@code arcadedb.ha.security.*} meters {@link HAReplicationMetrics} registers, and
 * the reading they give on a server whose security service is not installed - which is what a scrape taken during
 * startup or shutdown sees.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7529SecurityRefreshMetricsTest {

  private static final String CONFIG_PATH = "target/test-security-7529-metrics";

  private ServerSecurity      security;
  private ArcadeDBServer      server;
  private SimpleMeterRegistry registry;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    server = mock(ArcadeDBServer.class);
    when(server.getPlugins()).thenReturn(List.of());
    when(server.getDatabaseNames()).thenReturn(Set.of());

    security = new ServerSecurity(server, new ContextConfiguration(), CONFIG_PATH);
    when(server.getSecurity()).thenReturn(security);

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
    final ArcadeDBServer bare = mock(ArcadeDBServer.class);
    when(bare.getPlugins()).thenReturn(List.of());
    when(bare.getSecurity()).thenReturn(null);

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
