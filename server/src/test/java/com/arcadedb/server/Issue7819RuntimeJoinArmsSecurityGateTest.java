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
 * Issue #7819: the security-convergence readiness gate of issue #7532 could not default on, because "this node has
 * never installed a replicated security document" is true both of a peer admitted at runtime whose seed has not
 * landed and of every node of a cluster that has simply never replicated one. The gate is now armed only on the
 * first - a node that {@link HAServerPlugin#hasJoinedClusterAtRuntime() joined the cluster at runtime} - and the
 * window defaults on.
 * <p>
 * Every test here runs on the DEFAULT window, because the default is what changed: a test that set the window
 * explicitly would pass on the code before the fix too.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7819RuntimeJoinArmsSecurityGateTest {

  /**
   * The case #7532 is about, now held without an operator having to know to switch anything on: a peer added at
   * runtime that holds none of the cluster's documents is NOT READY on the default configuration.
   */
  @Test
  void aRuntimeJoinerIsHeldOnTheDefaultConfiguration() {
    final String reason = new ServerControlPlane(
        onlineServerWith(caughtUpHa(true), securityMissing("users", "groups", "API tokens"))).notReadyReason();

    assertThat(reason).isNotNull();
    assertThat(reason).contains("users", "groups", "API tokens");
  }

  /**
   * The regression the default-off existed to avoid: a statically configured cluster where nobody has run a
   * {@code create user} since it was built has no node with a replicated document. Its nodes did not join at
   * runtime, so they are READY at every start, with the window on.
   */
  @Test
  void aStaticMemberOfAClusterThatNeverReplicatedASecurityDocumentIsReady() {
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHa(false), securityMissing("users", "groups", "API tokens"))).notReadyReason())
        .isNull();
  }

  /**
   * An HA implementation that cannot tell - the interface default, which every non-Raft implementation keeps -
   * leaves the gate disarmed rather than holding every node of it.
   */
  @Test
  void anImplementationThatCannotTellLeavesTheGateDisarmed() {
    final HAServerPlugin legacy = mock(HAServerPlugin.class,
        invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
    when(legacy.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(legacy.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(legacy.getConfiguredServers()).thenReturn(3);

    assertThat(legacy.hasJoinedClusterAtRuntime()).isFalse();
    assertThat(new ServerControlPlane(onlineServerWith(legacy, securityMissing("users"))).notReadyReason())
        .isNull();
  }

  /**
   * Replay is inert. A runtime joiner that restarts replays the configuration entry that added it and is armed
   * again, but it converged the first time round and the fingerprint recording that is on disk: armed and
   * converged is READY, so a converged node is never held by the re-arm.
   */
  @Test
  void anArmedNodeThatHasConvergedIsReady() {
    assertThat(new ServerControlPlane(onlineServerWith(caughtUpHa(true), securityMissing())).notReadyReason())
        .isNull();
  }

  /**
   * The joiner is armed on the evidence that it was added to a configuration that did not contain it, which
   * proves a peer to converge WITH. The static peer count is the node's own declared server list, not the live
   * configuration, and must not disarm it: a joiner's declared list is whatever its operator wrote.
   */
  @Test
  void aRuntimeJoinerIsHeldWhateverItsOwnDeclaredServerListSays() {
    final HAServerPlugin ha = caughtUpHa(true);
    when(ha.getConfiguredServers()).thenReturn(1);

    assertThat(new ServerControlPlane(onlineServerWith(ha, securityMissing("users"))).notReadyReason())
        .isNotNull();
  }

  /** The default is the one thing this issue moved, so pin it: on, and bounded. */
  @Test
  void theDefaultWindowIsOnAndBounded() {
    final long defaultWindow = new ContextConfiguration().getValueAsLong(
        GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT);
    assertThat(defaultWindow).isPositive();
    assertThat(defaultWindow).isLessThanOrEqualTo(120_000L);
  }

  // -----------------------------------------------------------------------------------------------------------

  private static HAServerPlugin caughtUpHa(final boolean joinedAtRuntime) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(3);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(joinedAtRuntime);
    return ha;
  }

  private static ServerSecurity securityMissing(final String... documents) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenReturn(List.of(documents));
    return security;
  }

  /** The window is left at its default on purpose: see the class javadoc. */
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
