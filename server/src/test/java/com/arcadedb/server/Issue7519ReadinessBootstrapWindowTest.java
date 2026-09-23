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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the production half of issue #7519, the residue of #7259.
 * <p>
 * #7259 was closed by a test-harness gate: a test body no longer starts while the cluster's first-formation
 * bootstrap is still replacing database directories. Nothing gated a PRODUCTION client out of the same window.
 * The window is real and it is long: {@code SnapshotInstaller.install} downloads the leader's snapshot before it
 * touches the live files, so the local copy - the one the committed baseline has just decided against - stays
 * open and serving for the whole download, on every protocol. The node-wide {@code snapshotInstallInProgress}
 * 503 covers only the file swap at the end of it, and only HTTP.
 * <p>
 * {@code ServerControlPlane.notReadyReason()} now consults {@link HAServerPlugin#getBootstrapWindowReason()}, so
 * an orchestrator takes the pod out of the Service for the duration - which gates every protocol at once, not
 * just the one that has a 503 to send. Same shape, and the same OUTSIDE-{@code readinessRequiresHA} placement,
 * as the #7118 log-failure gate: that switch is opt-in because it gates a node that is merely BEHIND, and this
 * node is not behind, it is serving something the cluster did not adopt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7519ReadinessBootstrapWindowTest {

  private static final String INSTALLING =
      "The cluster's first-formation bootstrap is replacing 1 database(s) on this node from the leader's snapshot";

  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getHA()).thenReturn(ha);
    return server;
  }

  private static ContextConfiguration haConfiguration(final boolean readinessRequiresHA) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHA);
    return configuration;
  }

  /** A caught-up node with no bootstrap install and nothing unreconciled is ready, as it always was. */
  @Test
  void readyWhileNoBootstrapWindowIsOpen() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getBootstrapWindowReason()).thenReturn(null);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason()).isNull();
  }

  /** The reported hole: a node whose directory is being replaced under it kept answering 200 on /ready. */
  @Test
  void notReadyWhileABootstrapInstallIsReplacingADatabase() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);

    final String reason = new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason();

    assertThat(reason).isEqualTo(INSTALLING);
  }

  /**
   * The point of the gate: {@code readinessRequiresHA} is off by default, so a gate behind it would leave the
   * deployment the issue describes - a cluster behind a Service, forming for the first time - exactly as it was.
   */
  @Test
  void notReadyDuringTheBootstrapWindowEvenWithoutReadinessRequiresHA() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
        .as("a node mid-bootstrap leaves the Service on a default-configured cluster too")
        .isEqualTo(INSTALLING);
  }

  /**
   * The state the window leaves behind (issue #6124): the peer that refused to overwrite its fresher copy keeps
   * serving a database whose file ids no other peer shares, durably, across restarts.
   */
  @Test
  void notReadyWhileHoldingACopyTheBootstrapBaselineDidNotAdopt() {
    final String unreconciled = "1 database(s) on this node hold a copy the cluster's committed bootstrap "
        + "baseline did not adopt";
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getBootstrapWindowReason()).thenReturn(unreconciled);

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
        .isEqualTo(unreconciled);
  }

  /** A probe must answer, not 500. A plugin that throws is treated as having no signal. */
  @Test
  void aSignalThatCannotBeReadDoesNotFailTheProbe() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getBootstrapWindowReason()).thenThrow(new IllegalStateException("plugin not initialized"));

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason()).isNull();
  }

  /** No HA layer at all: nothing to gate, and {@code getHA()} is null rather than a plugin answering null. */
  @Test
  void readyWhenThereIsNoHaLayerAtAll() {
    assertThat(new ServerControlPlane(onlineServerWith(null, new ContextConfiguration())).notReadyReason()).isNull();
  }

  /**
   * Order, pinned: the wedged log writer is the more fundamental report, because a node in both states cannot
   * finish the bootstrap install either. A future edit that reorders the two would report the less useful reason.
   */
  @Test
  void aWedgedLogWriterIsReportedBeforeTheBootstrapWindow() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("at index 4711: java.io.IOException: No space left on device");
    when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
        .contains("No space left on device");
  }

  /**
   * A server that has not started yet answers with its status: a node that never came up is not a node whose
   * bootstrap is replacing a directory.
   */
  @Test
  void aServerStillStartingReportsItsStatusNotTheBootstrapWindow() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.STARTING);
    when(server.getHA()).thenReturn(ha);

    assertThat(new ServerControlPlane(server).notReadyReason()).isEqualTo("Server not started yet");
  }
}
