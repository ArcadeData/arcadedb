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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7118, the follow-up to #7037.
 * <p>
 * #7037 gave a Raft log-writer failure a detection signal and a bounded in-place restart driven by the health
 * monitor. Nothing wired that signal to readiness, so a follower whose log writer was wedged kept answering 200
 * on {@code /api/v1/ready}: Kubernetes left the pod in the Service and kept routing reads to a replica frozen at
 * the moment the writer failed - stale reads, no error anywhere on the request path, and on a 3-node cluster
 * roughly a third of the reads.
 * <p>
 * {@code ServerControlPlane.notReadyReason()} now consults {@link HAServerPlugin#getRaftLogFailure()}. The gate is
 * OUTSIDE {@code arcadedb.server.readinessRequiresHA} on purpose, and the last test here pins that: that switch is
 * opt-in because it gates a node that is merely BEHIND, which a deployment can reasonably serve from, whereas a
 * node whose log writer has failed cannot catch up at all. Putting this behind the switch would leave every
 * default deployment - the one the issue describes - exactly as it was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7118ReadinessRaftLogFailureTest {

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

  @Test
  void readyWhenThereIsNoHaLayerAtAll() {
    assertThat(new ServerControlPlane(onlineServerWith(null, new ContextConfiguration())).notReadyReason()).isNull();
  }

  @Test
  void readyWhileTheLogWriterIsHealthy() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn(null);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(org.mockito.ArgumentMatchers.anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason()).isNull();
  }

  @Test
  void notReadyOnceTheLogWriterHasFailed() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("at index 4711: java.io.IOException: No space left on device");

    final String reason = new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason();

    assertThat(reason).isNotNull();
    assertThat(reason).contains("No space left on device");
  }

  /**
   * The point of the issue: the gate must not be behind {@code readinessRequiresHA}, which is off by default.
   * With the gate inside it, this exact configuration - a wedged follower on a cluster that never opted in - kept
   * answering Ready, which is the deployment the report describes.
   */
  @Test
  void notReadyOnceTheLogWriterHasFailedEvenWithoutReadinessRequiresHA() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("on a log segment: java.io.IOException: disk full");

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
        .as("a wedged log writer removes the node from the Service on a default-configured cluster too")
        .isNotNull();
  }

  /**
   * A probe must answer, not 500. A plugin that throws while being asked is treated as having no signal, leaving
   * the remaining gates to decide.
   */
  @Test
  void aSignalThatCannotBeReadDoesNotFailTheProbe() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenThrow(new IllegalStateException("plugin not initialized"));

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason()).isNull();
  }

  /**
   * The failure is reported BEFORE the server-status gate has been passed? No: a server that is not ONLINE yet
   * still answers with the status reason, because a node that has not started is not a node whose log writer
   * wedged. Pins the order so a future edit does not report the more alarming reason for the ordinary case.
   */
  @Test
  void aServerStillStartingReportsItsStatusNotTheLogFailure() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("at index 1: boom");
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.STARTING);
    when(server.getHA()).thenReturn(ha);

    assertThat(new ServerControlPlane(server).notReadyReason()).isEqualTo("Server not started yet");
  }
}
