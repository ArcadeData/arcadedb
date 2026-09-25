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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8109, absorbing #7827: the compare-and-set decision of a security mutation runs a synchronous capability
 * probe round whenever the cached answer is not a full "yes" - which it never is while a peer is DOWN - and every
 * caller holds the {@code ServerSecurity} monitor across it. The #7511 gate had an off switch for its own round
 * ({@code arcadedb.ha.securityEntryCapabilityGate}); the precondition decision had none, so with the gate off a
 * group change still paid one probe round and a user change, which has no gate, always did.
 * <p>
 * With the switch off the decision now reads the cached answer, which the capability monitor keeps current on every
 * node (#7549), and never dials.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8109PreconditionProbeOffSwitchTest {

  private static final String FINGERPRINT = "7f".repeat(32);
  private static final String USERS       = "[{\"name\":\"root\"}]";
  private static final String GROUPS      = "{\"databases\":{}}";

  @Test
  void withTheGateOffAUserChangeNeverRunsAProbeRound() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = raftWhoseCacheSays(broker, List.of());

    pluginOn(raft, false).replicateSecurityUsers(USERS, FINGERPRINT);

    verify(raft, never()).peersMissingCapabilityNow(anyString());
    verify(broker).replicateSecurityUsers(USERS, FINGERPRINT);
  }

  /** The gated documents' own round was already switched off by the gate; the precondition's must be too. */
  @Test
  void withTheGateOffAGroupChangeNeverRunsAProbeRound() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = raftWhoseCacheSays(broker, List.of());

    pluginOn(raft, false).replicateSecurityGroups(GROUPS, FINGERPRINT);

    verify(raft, never()).peersMissingCapabilityNow(anyString());
    verify(broker).replicateSecurityGroups(GROUPS, FINGERPRINT);
  }

  /**
   * The direction the cached answer can cost in is the safe one: a peer the cache does not know withholds the
   * precondition - the pre-#7509 behaviour - rather than being dialled with the monitor held.
   */
  @Test
  void withTheGateOffAPeerTheCacheDoesNotKnowWithholdsThePrecondition() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = raftWhoseCacheSays(broker, List.of("arcadedb2"));

    pluginOn(raft, false).replicateSecurityUsers(USERS, FINGERPRINT);

    verify(raft, never()).peersMissingCapabilityNow(anyString());
    verify(broker).replicateSecurityUsers(USERS, null);
  }

  /** The control: with the gate on - the default - the decision still asks now, as #7559 requires. */
  @Test
  void withTheGateOnTheDecisionStillAsksNow() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = raftWhoseCacheSays(broker, List.of("arcadedb2"));
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());

    pluginOn(raft, true).replicateSecurityUsers(USERS, FINGERPRINT);

    verify(raft).peersMissingCapabilityNow(PeerCapabilities.SECURITY_PRECONDITION);
    verify(broker).replicateSecurityUsers(USERS, FINGERPRINT);
  }

  private static RaftHAServer raftWhoseCacheSays(final RaftTransactionBroker broker, final List<String> missing) {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapability(anyString())).thenReturn(missing);
    return raft;
  }

  private static RaftHAPlugin pluginOn(final RaftHAServer raft, final boolean gate) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, gate);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    plugin.configure(server, configuration);
    plugin.setRaftHAServer(raft);
    return plugin;
  }
}
