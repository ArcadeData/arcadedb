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
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8465, second half: a node that LEADS while its security documents are unconfirmed - a
 * static member that won the election right after its snapshot install (#8432), or right after a restart that
 * restored the hold - was held for the whole security-convergence window, because the evidence that releases the gate
 * always comes from a leader that is not this node, and its catch-up asks nobody while it leads. It then reported a
 * give-up whose advice ('connect cluster', re-POST the peer) did not fit.
 * <p>
 * A leader is now not held: nobody can confirm it, and the followers forward their writes to it whatever its readiness
 * says. The window restarts instead of being spent, so when it steps down it is held again, for a full window, until
 * the catch-up it then runs is answered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8465LeaderSecurityConvergenceGateTest {

  private static final long LONG_WINDOW_MS = 60_000L;
  private static final long INSTALL_INDEX  = 5_000L;

  @Test
  void aStaticMemberThatLeadsIsNotHeldForAConfirmationNobodyCanGive() {
    final HAServerPlugin ha = staticMemberHa("users", "groups", "API tokens");
    when(ha.isLeader()).thenReturn(true);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).as("the defect: held for the whole window").isNull();
  }

  @Test
  void aStaticMemberThatStepsDownIsHeldAgainUntilConfirmed() {
    final HAServerPlugin ha = staticMemberHa("users");
    when(ha.isLeader()).thenReturn(true, false);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).as("leading").isNull();
    assertThat(controlPlane.notReadyReason()).as("a follower again, still unconfirmed").contains("users");
  }

  /**
   * Time spent leading is not time spent waiting: a window that opened as a follower and would have expired while the
   * node led is restarted, so the step-down gets a full window for its catch-up to be answered.
   */
  @Test
  @Tag("slow")
  void leadingRestartsTheWindowInsteadOfSpendingIt() throws InterruptedException {
    final long windowMs = 2_000L;
    final HAServerPlugin ha = staticMemberHa("users");
    when(ha.isLeader()).thenReturn(false, true, false);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(windowMs)));

    assertThat(controlPlane.notReadyReason()).as("a follower: the window opens").isNotNull();
    Thread.sleep(windowMs + 100L);
    assertThat(controlPlane.notReadyReason()).as("leading past the first window").isNull();
    assertThat(controlPlane.notReadyReason()).as("stepped down: a fresh window, not an expired one").isNotNull();
  }

  /**
   * A window that already gave up stays given up (review of PR #8477): a node that has reported READY since its give-up
   * must not be pulled out of the Service for a new window just because it later led an election and stepped down.
   */
  @Test
  void aWindowThatGaveUpIsNotRestartedByALeadershipSpell() throws InterruptedException {
    final HAServerPlugin ha = staticMemberHa("users");
    when(ha.isLeader()).thenReturn(false, false, true, false);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    assertThat(controlPlane.notReadyReason()).as("a follower: the window opens").isNotNull();
    Thread.sleep(5L);
    assertThat(controlPlane.notReadyReason()).as("the window expired: gave up, READY").isNull();
    assertThat(controlPlane.notReadyReason()).as("leading").isNull();
    assertThat(controlPlane.notReadyReason()).as("stepped down: still READY, not a fresh window").isNull();
  }

  /** The same holds for an armed runtime joiner that leads (#8353): nobody can seed or confirm it either. */
  @Test
  void anArmedJoinerThatLeadsIsNotHeldEither() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(true);
    when(ha.getRuntimeJoinIndex()).thenReturn(INSTALL_INDEX);
    when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of("groups"));
    when(ha.isLeader()).thenReturn(true, false);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).isNull();
    assertThat(controlPlane.notReadyReason()).contains("groups");
  }

  /** A converged leader is ready as before; nothing about leading changes a converged reading. */
  @Test
  void aConvergedLeaderIsReady() {
    final HAServerPlugin ha = staticMemberHa();
    when(ha.isLeader()).thenReturn(true);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).isNull();
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

  private static ServerSecurity fingerprintsMissing(final String... documents) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenReturn(List.of(documents));
    return security;
  }

  private static ContextConfiguration configurationWith(final long windowMs) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, windowMs);
    return configuration;
  }

  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ServerSecurity security,
      final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getHA()).thenReturn(ha);
    when(server.getSecurity()).thenReturn(security);
    return server;
  }
}
