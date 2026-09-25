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
 * Issue #8317: a node re-added to the cluster with its config volume retained holds a recorded replicated
 * fingerprint for every security document from its previous membership, so
 * {@code ServerSecurity.unconvergedClusterSecurityDocuments()} is empty and the security-convergence gate released
 * it on the first probe, while it could still enforce a user dropped, a group narrowed or a token revoked while it
 * was out. On a runtime joiner the gate now also waits for
 * {@link HAServerPlugin#securityDocumentsNotInstalledSinceRuntimeJoin()}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8317ReAddedNodeSecurityGateTest {

  /** The issue's entry point: armed, fingerprints on disk from before, nothing installed since the re-add. */
  @Test
  void aReAddedNodeWithFingerprintsFromItsPreviousMembershipIsHeld() {
    final String reason = new ServerControlPlane(onlineServerWith(
        reAddedHa(true, "users", "groups", "API tokens"), fingerprintsMissing())).notReadyReason();

    assertThat(reason).isNotNull();
    assertThat(reason).contains("users", "groups", "API tokens");
  }

  /** The seed lands: every document installed after the re-add, and the node is READY. */
  @Test
  void aReAddedNodeIsReadyOnceEveryDocumentWasInstalledSinceTheReAdd() {
    assertThat(new ServerControlPlane(onlineServerWith(reAddedHa(true), fingerprintsMissing())).notReadyReason())
        .isNull();
  }

  /** Only the documents not installed since the re-add are named, in the usual order. */
  @Test
  void onlyTheDocumentsStillAwaitedAreNamed() {
    final String reason = new ServerControlPlane(onlineServerWith(
        reAddedHa(true, "groups", "API tokens"), fingerprintsMissing())).notReadyReason();

    assertThat(reason).contains("groups, API tokens");
    assertThat(reason).doesNotContain("users");
  }

  /** The two signals are a union: a document never installed at all is still awaited. */
  @Test
  void theFingerprintSignalStillCounts() {
    final String reason = new ServerControlPlane(onlineServerWith(
        reAddedHa(true, "API tokens"), fingerprintsMissing("users"))).notReadyReason();

    assertThat(reason).contains("users, API tokens");
  }

  /**
   * A static member is not armed, so the join-relative signal is never consulted on it, whatever it would answer:
   * #7819's regression guard for a cluster that never replicated a security document is unchanged.
   */
  @Test
  void aStaticMemberIsNotHeldByTheJoinRelativeSignal() {
    assertThat(new ServerControlPlane(onlineServerWith(
        reAddedHa(false, "users", "groups", "API tokens"), fingerprintsMissing())).notReadyReason()).isNull();
  }

  /**
   * An HA implementation that predates the signal keeps the interface default, which awaits nothing, leaving the
   * gate on the fingerprints alone - exactly as before this issue.
   */
  @Test
  void theInterfaceDefaultAwaitsNothing() {
    final HAServerPlugin legacy = mock(HAServerPlugin.class,
        invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
    assertThat(legacy.securityDocumentsNotInstalledSinceRuntimeJoin()).isEmpty();
  }

  // -----------------------------------------------------------------------------------------------------------

  private static HAServerPlugin reAddedHa(final boolean joinedAtRuntime, final String... notInstalledSinceJoin) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(3);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(joinedAtRuntime);
    when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of(notInstalledSinceJoin));
    return ha;
  }

  private static ServerSecurity fingerprintsMissing(final String... documents) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenReturn(List.of(documents));
    return security;
  }

  /** The window is left at its default, which is on (issue #7819). */
  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ServerSecurity security) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getHA()).thenReturn(ha);
    when(server.getSecurity()).thenReturn(security);
    return server;
  }
}
