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
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8555: {@code ServerControlPlane.notReadyReason()} has five sources of a 503, and the
 * security-convergence gate was the one {@code GET /api/v1/cluster} did not publish, so a node held by it read fully
 * green in the status document. {@link ServerControlPlane#getSecurityConvergenceStatus()} is what the document now
 * renders; it must agree with the readiness probe on every state of the gate, and on one shared window: the HTTP probe,
 * the gRPC probe and the status handler each build their own control plane.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8555SecurityConvergenceStatusTest {

  private static final long LONG_WINDOW_MS = 60_000L;
  private static final long INSTALL_INDEX  = 5_000L;

  @Test
  void aHeldNodeIsPublishedWithTheDocumentsTheIndexAndTheWindow() {
    final HAServerPlugin ha = staticMemberHa("users", "groups");
    final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)));

    final String reason = controlPlane.notReadyReason();
    final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();

    assertThat(reason).as("the 503 the document must explain").contains("users, groups");
    assertThat(status.held()).isTrue();
    assertThat(status.reason()).isEqualTo(reason);
    assertThat(status.unconvergedDocuments()).containsExactly("users", "groups");
    assertThat(status.armed()).as("a static member held on a snapshot install").isFalse();
    assertThat(status.sinceIndex()).isEqualTo(INSTALL_INDEX);
    assertThat(status.windowOpenedAt()).isPositive();
    assertThat(status.gaveUp()).isFalse();
    assertThat(status.skippedBecauseLeading()).isFalse();
  }

  @Test
  void anArmedJoinerIsPublishedAsArmedWithItsJoinIndex() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(true);
    when(ha.getRuntimeJoinIndex()).thenReturn(INSTALL_INDEX);
    when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of("API tokens"));
    final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)));

    final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();

    assertThat(status.held()).isTrue();
    assertThat(status.armed()).isTrue();
    assertThat(status.sinceIndex()).isEqualTo(INSTALL_INDEX);
    assertThat(status.unconvergedDocuments()).containsExactly("API tokens");
  }

  /**
   * The state most worth publishing: past its window the node is READY and enforcing its own copies, and nothing but one
   * SEVERE line, emitted once, ever said so.
   */
  @Test
  void aNodeThatGaveUpIsPublishedAsGaveUpAndNotHeld() throws InterruptedException {
    final HAServerPlugin ha = staticMemberHa("users");
    final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(1L)));

    assertThat(controlPlane.getSecurityConvergenceStatus().held()).as("the window opens").isTrue();
    Thread.sleep(5L);

    final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();
    assertThat(controlPlane.notReadyReason()).as("READY").isNull();
    assertThat(status.held()).isFalse();
    assertThat(status.gaveUp()).isTrue();
    assertThat(status.unconvergedDocuments()).as("still unconfirmed, which is the point").containsExactly("users");
    assertThat(status.reason()).isNull();
  }

  @Test
  void aLeaderIsPublishedAsSkippedBecauseLeading() {
    final HAServerPlugin ha = staticMemberHa("groups");
    when(ha.isLeader()).thenReturn(true);
    final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)));

    final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();

    assertThat(status.held()).isFalse();
    assertThat(status.skippedBecauseLeading()).isTrue();
    assertThat(status.unconvergedDocuments()).containsExactly("groups");
    assertThat(status.gaveUp()).isFalse();
  }

  @Test
  void aConvergedNodeAndAGateThatDoesNotApplyPublishNothingToWaitFor() {
    final ServerControlPlane converged = new ServerControlPlane(
        onlineServerWith(staticMemberHa(), configurationWith(LONG_WINDOW_MS)));
    final SecurityConvergenceStatus status = converged.getSecurityConvergenceStatus();
    assertThat(status.held()).isFalse();
    assertThat(status.unconvergedDocuments()).isEmpty();
    assertThat(status.windowOpenedAt()).isZero();

    final ContextConfiguration notRequiringHa = configurationWith(LONG_WINDOW_MS);
    notRequiringHa.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);
    assertThat(new ServerControlPlane(onlineServerWith(staticMemberHa("users"), notRequiringHa)).getSecurityConvergenceStatus())
        .isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);

    assertThat(new ServerControlPlane(onlineServerWith(null, configurationWith(LONG_WINDOW_MS))).getSecurityConvergenceStatus())
        .as("no HA layer").isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);

    assertThat(new ServerControlPlane(onlineServerWith(staticMemberHa("users"), configurationWith(0L)))
        .getSecurityConvergenceStatus()).as("timeout 0 disables the gate").isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);
  }

  /**
   * The HTTP probe, the gRPC probe and the status handler each build their own control plane: the window they consult has
   * to be the server's, or the document could say "held" while the probe said READY, or the reverse.
   */
  @Test
  void everyControlPlaneOfAServerSeesTheSameWindow() throws InterruptedException {
    final ArcadeDBServer server = onlineServerWith(staticMemberHa("users"), configurationWith(1L));
    final ServerControlPlane readinessProbe = new ServerControlPlane(server);
    final ServerControlPlane statusHandler = new ServerControlPlane(server);

    assertThat(readinessProbe.notReadyReason()).as("the probe opens the window").isNotNull();
    final long opened = statusHandler.getSecurityConvergenceStatus().windowOpenedAt();
    assertThat(opened).as("the status handler reads that window, it does not open its own").isPositive();

    Thread.sleep(5L);
    assertThat(statusHandler.getSecurityConvergenceStatus().gaveUp()).as("expired for the handler").isTrue();
    assertThat(readinessProbe.notReadyReason()).as("and for the probe").isNull();
    assertThat(statusHandler.getSecurityConvergenceStatus().windowOpenedAt()).isEqualTo(opened);
  }

  /**
   * The gate opens its window on first sight, and the probe only reaches it once every earlier
   * check passed. A status poll during catch-up must not start the clock, or the window is already spent when the
   * probe first evaluates the gate and the node goes straight to give-up.
   */
  @Test
  void aStatusPollBeforeTheNodeIsConsensusReadyDoesNotOpenTheWindow() {
    final HAServerPlugin ha = staticMemberHa("users");
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.VOTING_FOR_ME);
    final SecurityConvergenceGate gate = new SecurityConvergenceGate();
    final ArcadeDBServer server = onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)).securityConvergenceGate(gate);
    final ServerControlPlane controlPlane = new ServerControlPlane(server);

    assertThat(controlPlane.getSecurityConvergenceStatus()).isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);
    assertThat(controlPlane.notReadyReason()).contains("Raft group");
    assertThat(gate.windowOpenedAt).as("still catching up: the window is not opened").isZero();

    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    assertThat(controlPlane.getSecurityConvergenceStatus().held()).isTrue();
    assertThat(gate.windowOpenedAt).as("opened once the probe would reach the gate").isPositive();
  }

  /**
   * A node that gave up keeps being reported as given up while it is briefly held for another reason, and reading it does
   * not evaluate the gate: a join index that moved forward (a re-add) must not reset the window or open a new one.
   */
  @Test
  void aGaveUpNodeStaysReportedWhileHeldForAnotherReasonWithoutStartingAClock() throws InterruptedException {
    final HAServerPlugin ha = staticMemberHa("users");
    final SecurityConvergenceGate gate = new SecurityConvergenceGate();
    final ArcadeDBServer server = onlineServerWith(ha, configurationWith(1L)).securityConvergenceGate(gate);
    final ServerControlPlane controlPlane = new ServerControlPlane(server);

    assertThat(controlPlane.notReadyReason()).isNotNull();
    Thread.sleep(5L);
    assertThat(controlPlane.getSecurityConvergenceStatus().gaveUp()).isTrue();
    final long opened = gate.windowOpenedAt;

    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.NOT_READY);
    when(ha.getLastSnapshotInstallIndex()).thenReturn(INSTALL_INDEX + 1000L);

    final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();
    assertThat(controlPlane.notReadyReason()).as("held for an earlier reason").contains("caught up");
    assertThat(status.gaveUp()).as("the critical alert does not flap").isTrue();
    assertThat(status.unconvergedDocuments()).containsExactly("users");
    assertThat(gate.windowOpenedAt).as("no clock started or reset by the poll").isEqualTo(opened);
    assertThat(gate.giveUpLogged).isTrue();
  }

  // -----------------------------------------------------------------------------------------------------------

  /** An unarmed, caught-up HA plugin held after a snapshot install at {@link #INSTALL_INDEX}. */
  private static HAServerPlugin staticMemberHa(final String... unconfirmed) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(3);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(false);
    when(ha.getLastSnapshotInstallIndex()).thenReturn(INSTALL_INDEX);
    when(ha.securityDocumentsNotConfirmedSinceSnapshotInstall()).thenReturn(List.of(unconfirmed));
    return ha;
  }

  private static ContextConfiguration configurationWith(final long windowMs) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, windowMs);
    return configuration;
  }

  private static FakeArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
    final ServerSecurity security = TestServerHelper.securityConvergedExcept();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
    server.online();
    server.setHA(ha);
    server.security(security);
    return server;
  }
}
