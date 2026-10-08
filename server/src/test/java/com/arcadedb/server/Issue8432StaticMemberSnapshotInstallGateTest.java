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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8432: a STATICALLY configured member - never armed as a runtime joiner (#7819) - that
 * was removed while down, re-added with its config volume retained and caught up by a leader-driven snapshot install
 * past the leader's compaction point was never gated for security convergence. The readiness gate returned early on
 * {@code !hasJoinedClusterAtRuntime()} and the node reported READY while it could still hold a user dropped, a group
 * narrowed or a token revoked while it was out.
 * <p>
 * The gate now holds such a node, transiently, until the documents are confirmed after the install - the
 * leader-confirmed match or a seed applied from the log - or until the same bounded window expires. It does not arm
 * the node, so the fingerprint half of the gate (which a cluster that never replicated a security document fails on
 * every node) is not consulted.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8432StaticMemberSnapshotInstallGateTest {

  private static final long LONG_WINDOW_MS = 60_000L;
  private static final long INSTALL_INDEX  = 5_000L;

  /** The issue's scenario: unarmed, a snapshot install this process, the documents not yet confirmed after it. */
  @Test
  void aStaticMemberCaughtUpBySnapshotIsHeldUntilItsDocumentsAreConfirmed() {
    final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX, "users", "groups", "API tokens");
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    final String reason = controlPlane.notReadyReason();
    assertThat(reason).as("the defect: an unarmed node reported READY right after the install").isNotNull();
    assertThat(reason).contains("users", "groups", "API tokens");
  }

  /** Released as soon as the catch-up confirms every document. */
  @Test
  void theConfirmationReleasesIt() {
    final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX);
    when(ha.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .thenReturn(List.of("groups"), List.of());
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).contains("groups");
    assertThat(controlPlane.notReadyReason()).isNull();
  }

  /**
   * The fingerprint half of the gate is NOT consulted on an unarmed node: a cluster that never replicated a security
   * document has none on any node, and #7819 exists so that such a cluster is not held. Once the install is
   * confirmed, the node is ready whatever the fingerprints say.
   */
  @Test
  void theFingerprintsAreNotConsultedOnAnUnarmedNode() {
    final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing("users", "groups", "API tokens"), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).isNull();
  }

  /** A static member that never caught up by snapshot install in this process is not gated, as before (#7819). */
  @Test
  void noInstallNoHold() {
    final HAServerPlugin ha = staticMemberHa(-1L, "users", "groups", "API tokens");
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing("users"), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).isNull();
  }

  /** Bounded by the same window: a static member the leader never answers is not held forever. */
  @Test
  void theHoldIsBoundedByTheSameWindow() {
    final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX, "users");
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    assertThat(controlPlane.notReadyReason()).as("opens the window").isNotNull();
    await(2L);
    assertThat(controlPlane.notReadyReason()).as("the window expired").isNull();
  }

  /** A later snapshot install is a fresh window, the way a re-add is on an armed node (#8414). */
  @Test
  void aLaterInstallOpensAFreshWindow() {
    final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX, "users");
    when(ha.getLastSnapshotInstallIndex()).thenReturn(INSTALL_INDEX, INSTALL_INDEX, INSTALL_INDEX + 1_000L);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    assertThat(controlPlane.notReadyReason()).isNotNull();
    await(2L);
    assertThat(controlPlane.notReadyReason()).as("the first install's window expired").isNull();
    assertThat(controlPlane.notReadyReason()).as("the second install is held again").isNotNull();
  }

  /** An HA implementation that predates the signal keeps the interface defaults: no install known, nothing held. */
  @Test
  void theInterfaceDefaultsReportNoInstall() {
    final HAServerPlugin legacy = mock(HAServerPlugin.class,
        invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
    assertThat(legacy.getLastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(legacy.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  // -----------------------------------------------------------------------------------------------------------

  /** An unarmed, caught-up HA plugin that installed a snapshot at {@code installIndex} ({@code -1}: none). */
  private static HAServerPlugin staticMemberHa(final long installIndex, final String... unconfirmed) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(3);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(false);
    when(ha.getLastSnapshotInstallIndex()).thenReturn(installIndex);
    when(ha.securityDocumentsNotConfirmedSinceSnapshotInstall()).thenReturn(List.of(unconfirmed));
    return ha;
  }

  private static ServerSecurity fingerprintsMissing(final String... documents) {
    return TestServerHelper.securityConvergedExcept(documents);
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
    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
    server.online();
    server.setHA(ha);
    server.security(security);
    return server;
  }

  private static void await(final long ms) {
    final long until = System.currentTimeMillis() + ms;
    while (System.currentTimeMillis() < until)
      Thread.onSpinWait();
  }
}
