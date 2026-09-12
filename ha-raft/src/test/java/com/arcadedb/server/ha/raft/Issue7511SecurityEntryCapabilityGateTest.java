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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7511: a group or API-token change committed on an already-upgraded node halted every node older than the
 * two Raft log entry types #7373 added.
 * <p>
 * The halt itself is correct and stays - {@code ArcadeStateMachine} must not skip a committed entry it cannot
 * decode (#4798), because skipping would diverge the cluster's security state silently. What was missing is
 * anything that stops the entry being written while a peer in the configuration cannot read it, which during a
 * rolling upgrade is the normal state of the cluster. The fix refuses the operation instead, so nothing is
 * submitted and nothing halts.
 * <p>
 * The interesting half is the LAST test: a gate helper nothing calls is a gate that is not there, so
 * {@code replicateSecurityGroups} and {@code replicateSecurityApiTokens} - the single chokepoint every HTTP and
 * gRPC entry point funnels through - are driven directly, and the assertion is that the transaction broker was
 * never handed anything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7511SecurityEntryCapabilityGateTest {

  private static final String LAGGING_PEER = "arcadedb2";

  // -------------------------------------------------------------------------------------------------------
  // 1. The vocabulary: the tokens exist, are advertised, and keep their exact spelling
  // -------------------------------------------------------------------------------------------------------

  /**
   * The spellings are pinned deliberately. {@code PeerCapabilities} states that a token which has shipped keeps
   * its exact spelling for as long as any supported version might advertise it: renaming one makes every older
   * peer read as incapable, which for THIS consumer does not silently disable a feature - it refuses every group
   * change and every token mint in the cluster.
   */
  @Test
  void theTwoEntryTypesOfIssue7373HaveStableCapabilityTokens() {
    assertThat(PeerCapabilities.SECURITY_GROUPS_ENTRY).isEqualTo("security-groups-entry");
    assertThat(PeerCapabilities.SECURITY_API_TOKENS_ENTRY).isEqualTo("security-api-tokens-entry");
  }

  /**
   * A capability names what THIS build can decode, and this build has both applies
   * ({@code ArcadeStateMachine.applySecurityGroupsEntry} / {@code applySecurityApiTokensEntry}), so it must say
   * so. A build that decoded the entries without advertising them would deadlock a cluster of its own peers:
   * every node capable, none of them saying it, so no group could ever be created.
   */
  @Test
  void thisBuildAdvertisesBothEntryTypesAlongsideTheSchemaDelta() {
    assertThat(PeerCapabilities.LOCAL).containsExactlyInAnyOrder(PeerCapabilities.SCHEMA_DELTA,
        PeerCapabilities.SECURITY_GROUPS_ENTRY, PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  // -------------------------------------------------------------------------------------------------------
  // 2. Which entry types need a token, and which deliberately do not
  // -------------------------------------------------------------------------------------------------------

  @Test
  void onlyTheTwoEntryTypesIssue7373AddedRequireACapability() {
    assertThat(SecurityEntryCapabilityGate.capabilityFor(RaftLogEntryType.SECURITY_GROUPS_ENTRY))
        .isEqualTo(PeerCapabilities.SECURITY_GROUPS_ENTRY);
    assertThat(SecurityEntryCapabilityGate.capabilityFor(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY))
        .isEqualTo(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  /**
   * Ids 1-5 came in with the Raft HA work itself, so no build that has ever spoken this protocol fails to decode
   * them. Id 6 shipped in 26.5.1 and is written only at first cluster formation, where refusing it would stop the
   * cluster forming rather than protect a peer - the opposite of what the gate is for.
   * <p>
   * Asserted over {@code values()} rather than by listing six constants, so this test is the one that fails the
   * day a ninth entry type is added without a decision about it - alongside the exhaustive switch in
   * {@code capabilityFor}, which fails the compile.
   */
  @Test
  void everyEntryTypeThatPredatesTheMechanismIsDeliberatelyUngated() {
    for (final RaftLogEntryType type : RaftLogEntryType.values()) {
      if (type == RaftLogEntryType.SECURITY_GROUPS_ENTRY || type == RaftLogEntryType.SECURITY_API_TOKENS_ENTRY)
        continue;
      assertThat(SecurityEntryCapabilityGate.capabilityFor(type))
          .as("%s (id %d) predates the capability mechanism and must not be gated", type, type.getId())
          .isNull();
    }
  }

  /** An ungated type never asks the cluster anything, so it cannot be refused - or slowed down - by this gate. */
  @Test
  void anUngatedEntryTypeNeverQueriesThePeers() {
    final RaftHAServer raft = mock(RaftHAServer.class);

    SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY, "bootstrap fingerprint");

    verify(raft, never()).peersMissingCapabilityNow(anyString());
  }

  // -------------------------------------------------------------------------------------------------------
  // 3. The decision
  // -------------------------------------------------------------------------------------------------------

  @Test
  void aClusterWhereEveryPeerAdvertisesTheCapabilityIsNotRefused() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_GROUPS_ENTRY)).thenReturn(List.of());

    assertThatCode(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document")).doesNotThrowAnyException();
  }

  @Test
  void aPeerThatHasNotAdvertisedTheCapabilityRefusesTheGroupChange() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_GROUPS_ENTRY)).thenReturn(List.of(LAGGING_PEER));
    when(raft.getPeerCapabilityRegistry()).thenReturn(registryWhereThePeerAnswered404());

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(LAGGING_PEER)
        .hasMessageContaining(PeerCapabilities.SECURITY_GROUPS_ENTRY);
  }

  @Test
  void aPeerThatHasNotAdvertisedTheCapabilityRefusesTheTokenChangeToo() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_API_TOKENS_ENTRY)).thenReturn(List.of(LAGGING_PEER));

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.SECURITY_API_TOKENS_ENTRY, "API-token document"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  /**
   * The interlock is on by default and stays on when nothing was wired: a gate that fails open because a field
   * was null is the failure this class exists to prevent.
   */
  @Test
  void theGateIsOnWhenNoServerConfigurationCanBeRead() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_GROUPS_ENTRY)).thenReturn(List.of(LAGGING_PEER));

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(null, raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);
  }

  /** Turned off, the operation goes through as it did before #7511 - and the peers are not even asked. */
  @Test
  void theInterlockCanBeTurnedOffForAnOperatorWhoKnowsBetter() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_GROUPS_ENTRY)).thenReturn(List.of(LAGGING_PEER));

    assertThatCode(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(false), raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document")).doesNotThrowAnyException();

    verify(raft, never()).peersMissingCapabilityNow(anyString());
  }

  // -------------------------------------------------------------------------------------------------------
  // 4. The sentence an operator is left holding
  // -------------------------------------------------------------------------------------------------------

  /**
   * The refusal is the entire remedy this issue delivers. "The cluster is not ready" sends an operator nowhere;
   * the peer's name, the reason its answer is missing, and the knob that overrides the decision are the three
   * things they can act on, so all three are pinned.
   */
  @Test
  void theRefusalNamesThePeerTheReasonAndTheOverride() {
    final String message = SecurityEntryCapabilityGate.refusal("group document",
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, PeerCapabilities.SECURITY_GROUPS_ENTRY, List.of(LAGGING_PEER),
        registryWhereThePeerAnswered404());

    assertThat(message).contains(LAGGING_PEER);
    assertThat(message).contains(PeerCapabilities.SECURITY_GROUPS_ENTRY);
    assertThat(message).contains("SECURITY_GROUPS_ENTRY");
    assertThat(message)
        .as("an operator must be told the reason the peer is unknown: an old build and an unreachable node have "
            + "nothing in common as remedies")
        .contains("404");
    assertThat(message).contains(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey());
    assertThat(message)
        .as("nothing was submitted is the half that decides whether the operator has to clean anything up")
        .containsIgnoringCase("nothing was submitted");
  }

  /** A peer with no recorded reason still has to be named; only the explanatory clause is dropped. */
  @Test
  void theRefusalStillNamesAPeerWhoseReasonWasNeverRecorded() {
    final String message = SecurityEntryCapabilityGate.refusal("API-token document",
        RaftLogEntryType.SECURITY_API_TOKENS_ENTRY, PeerCapabilities.SECURITY_API_TOKENS_ENTRY,
        List.of(LAGGING_PEER), new PeerCapabilityRegistry());

    assertThat(message).contains(LAGGING_PEER);
    assertThat(message).contains(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey());
  }

  /**
   * The refusal is also reported in this node's own log, because the caller may be a deployment script that
   * swallows the 409 while the operator watching the rolling upgrade is the one who has to act (PR #7555 review).
   * Throttled per capability, so a script retrying in a loop cannot bury the line that matters - and per
   * CAPABILITY rather than globally, so a group refusal does not silence the token refusal behind it.
   */
  @Test
  void aRefusalIsReportedOnceAndThenThrottledPerCapability() {
    SecurityEntryCapabilityGate.resetRefusalLogThrottle();
    final long t0 = 1_000_000L;

    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_GROUPS_ENTRY, t0))
        .as("the first refusal is always reported").isTrue();
    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_GROUPS_ENTRY, t0 + 1_000L))
        .as("a retry a second later is the same condition and is not reported again").isFalse();
    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_API_TOKENS_ENTRY, t0 + 1_000L))
        .as("but the other capability's first refusal is its own event, not a repeat of this one").isTrue();
    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_GROUPS_ENTRY, t0 + 60_001L))
        .as("and once the window has passed the condition is reported again").isTrue();
  }

  // -------------------------------------------------------------------------------------------------------
  // 5. The caller: nothing is submitted on a refusal
  // -------------------------------------------------------------------------------------------------------

  /**
   * The bug is not "the gate says no", it is "the entry reaches the Raft log". Driven through the plugin method
   * every HTTP and gRPC entry point ends at, so the assertion is the one that matters: the broker - the only
   * thing in {@code src/main} that encodes either entry type - was never handed the payload.
   */
  @Test
  void aRefusedGroupChangeSubmitsNothingToTheRaftLog() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_GROUPS_ENTRY)).thenReturn(List.of(LAGGING_PEER));

    final RaftHAPlugin plugin = pluginOn(raft, true);

    assertThatThrownBy(() -> plugin.replicateSecurityGroups("{\"databases\":{}}"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    verify(broker, never()).replicateSecurityGroups(anyString());
  }

  /**
   * The same for a token document, which is the case that matters most: a REVOCATION whose entry halts the nodes
   * still serving the token has not revoked anything.
   */
  @Test
  void aRefusedApiTokenChangeSubmitsNothingToTheRaftLog() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapabilityNow(PeerCapabilities.SECURITY_API_TOKENS_ENTRY)).thenReturn(List.of(LAGGING_PEER));

    final RaftHAPlugin plugin = pluginOn(raft, true);

    assertThatThrownBy(() -> plugin.replicateSecurityApiTokens("{\"version\":1,\"tokens\":[]}"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    verify(broker, never()).replicateSecurityApiTokens(anyString());
  }

  /** The other direction, so the test above cannot pass because the plugin submits nothing under any condition. */
  @Test
  void aFullyUpgradedClusterStillReplicatesBothDocuments() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());

    final RaftHAPlugin plugin = pluginOn(raft, true);

    plugin.replicateSecurityGroups("{\"databases\":{}}");
    plugin.replicateSecurityApiTokens("{\"version\":1,\"tokens\":[]}");

    verify(broker).replicateSecurityGroups("{\"databases\":{}}");
    verify(broker).replicateSecurityApiTokens("{\"version\":1,\"tokens\":[]}");
  }

  /**
   * {@code SECURITY_USERS_ENTRY} predates every build this cluster can contain, so the users path must not have
   * acquired a gate along with the other two - a rolling upgrade would otherwise stop being able to change a
   * password, which is strictly worse than what #7511 set out to fix.
   */
  @Test
  void theUsersEntryIsNotGatedAndStillReplicatesOnAMixedCluster() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of(LAGGING_PEER));

    final RaftHAPlugin plugin = pluginOn(raft, true);

    plugin.replicateSecurityUsers("[{\"name\":\"root\"}]");

    verify(broker).replicateSecurityUsers("[{\"name\":\"root\"}]");
  }

  // -------------------------------------------------------------------------------------------------------

  private static RaftHAPlugin pluginOn(final RaftHAServer raft, final boolean gateEnabled) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, gateEnabled);
    plugin.configure(serverWith(configuration), configuration);
    plugin.setRaftHAServer(raft);
    return plugin;
  }

  private static ArcadeDBServer serverWithGate(final boolean enabled) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, enabled);
    return serverWith(configuration);
  }

  private static ArcadeDBServer serverWith(final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    return server;
  }

  /** A registry holding the answer a node running a build without the capability route actually gives: a 404. */
  private static PeerCapabilityRegistry registryWhereThePeerAnswered404() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.forget(registry.generation(), LAGGING_PEER, "the capability route answered HTTP 404");
    return registry;
  }
}
