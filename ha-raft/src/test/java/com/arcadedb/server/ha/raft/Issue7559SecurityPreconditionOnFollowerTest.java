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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7559: the compare-and-set of issue #7509 engaged only when the mutation happened to be submitted on the
 * leader, because the answer it was gated on came from a registry only a leader fills.
 * <p>
 * {@code RaftHAServer.startCapabilityMonitor} used to run on gaining leadership and stop on losing it, and what it
 * recorded was deliberately left to age out (#7301). That is the right shape for #7219's consumer - only the leader
 * writes a schema delta - but a security entry is not a schema delta: <b>any node can submit one</b>. On a follower
 * the registry was empty, and on a recently demoted leader it was stale, so
 * {@code peersMissingCapability(SECURITY_PRECONDITION)} named peers that are in fact perfectly capable, the
 * precondition was withheld, and the mutation replicated unconditionally - the pre-#7509 behaviour, announced by
 * nothing louder than a throttled INFO line.
 * <p>
 * The inversion is what made it worth a fix rather than a caveat: the entry points the gate switched off - the
 * REST group and API-token routes, and openCypher {@code CREATE USER} / {@code ALTER USER} / {@code DROP USER}
 * over Bolt or {@code /api/v1/command} - are exactly the ones that do NOT forward to the leader, so they are the
 * ones not already serialised onto a single node, and therefore the ones that needed a compare-and-set most.
 * ({@code DeleteDropUserHandler}, which issue #7559's own table names as a fourth such door, turned out to be
 * constructed nowhere - issue #7829.)
 *
 * <h2>What the fix is</h2>
 *
 * The decision now reads {@link RaftHAServer#peersMissingCapabilityNow} - the ask-now variant #7511 already added
 * for exactly this reason - instead of the cached, leader-only {@link RaftHAServer#peersMissingCapability}. The
 * cached answer is still consulted first inside it, so a warm leader pays nothing; a follower pays one bounded
 * probe round on an operation that is rare by construction. (Issue #7549 has since moved the monitor onto every
 * node, so the follower's cached answer is normally warm too and the round below is only the cold-start case.)
 *
 * <h2>What these tests pin</h2>
 *
 * Both directions, because only one of them is about engaging the check:
 * <ul>
 *   <li>a node whose cached answer says "peers missing" while the cluster is in fact uniform must still write the
 *       precondition - the follower case, the bug;</li>
 *   <li>a node whose cached answer says "nothing missing" while a peer genuinely cannot read a precondition must
 *       still withhold it - the stale-registry case, and the half #7540 contributed: every node has to reach the
 *       SAME verdict on the same entry, or a mixed cluster diverges its security state instead of merely losing an
 *       update.</li>
 * </ul>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7559SecurityPreconditionOnFollowerTest {

  private static final String FINGERPRINT = "7f".repeat(32);
  private static final String LAGGING_PEER = "arcadedb2";
  private static final String USERS = "[{\"name\":\"root\"}]";
  private static final String GROUPS = "{\"databases\":{}}";
  private static final String API_TOKENS = "{\"version\":1,\"tokens\":[]}";

  // ---------------------------------------------------------------------------------------------------------
  // 1. The follower case: an empty cache must not be mistaken for an incapable cluster
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aFollowerStillWritesThePreconditionOnAUsersEntry() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAPlugin plugin = pluginOn(followerWhoseCacheIsEmptyButWhoseClusterIsUniform(broker));

    plugin.replicateSecurityUsers(USERS, FINGERPRINT);

    verify(broker).replicateSecurityUsers(USERS, FINGERPRINT);
  }

  @Test
  void aFollowerStillWritesThePreconditionOnAGroupsEntry() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAPlugin plugin = pluginOn(followerWhoseCacheIsEmptyButWhoseClusterIsUniform(broker));

    plugin.replicateSecurityGroups(GROUPS, FINGERPRINT);

    verify(broker).replicateSecurityGroups(GROUPS, FINGERPRINT);
  }

  @Test
  void aFollowerStillWritesThePreconditionOnAnApiTokensEntry() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAPlugin plugin = pluginOn(followerWhoseCacheIsEmptyButWhoseClusterIsUniform(broker));

    plugin.replicateSecurityApiTokens(API_TOKENS, FINGERPRINT);

    verify(broker).replicateSecurityApiTokens(API_TOKENS, FINGERPRINT);
  }

  // ---------------------------------------------------------------------------------------------------------
  // 2. The stale case: a cache that says "all clear" must not outrank a peer that cannot read a precondition
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The other direction, and the one #7540 contributed. A node demoted from leadership keeps the advertisements it
   * collected until they age out, so its cache can answer "nobody is missing" about a cluster that has since taken
   * on a node predating the section. Writing the precondition on that answer is worse than not writing it: the
   * upgraded nodes refuse the superseded entry and the older one installs it, so the cluster's security state
   * DIVERGES rather than losing an update.
   */
  @Test
  void aStaleAllClearCacheDoesNotOutrankAPeerThatCannotReadAPrecondition() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    // What a recently demoted leader still believes...
    when(raft.peersMissingCapability(PeerCapabilities.SECURITY_PRECONDITION)).thenReturn(List.of());
    // ...and what the cluster actually answers when asked now.
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of(LAGGING_PEER));

    pluginOn(raft).replicateSecurityUsers(USERS, FINGERPRINT);

    verify(broker).replicateSecurityUsers(USERS, null);
  }

  /**
   * A gate helper nothing calls is a gate that is not there, and a gate called through the WRONG accessor is the
   * same thing one indirection further down. The cached, leader-only answer must not be what the decision is made
   * on - on any node, in any direction.
   */
  @Test
  void theDecisionIsNeverTakenOnTheLeaderOnlyCachedAnswer() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(mock(RaftTransactionBroker.class));
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());

    pluginOn(raft).replicateSecurityUsers(USERS, FINGERPRINT);

    verify(raft, never()).peersMissingCapability(PeerCapabilities.SECURITY_PRECONDITION);
    verify(raft).peersMissingCapabilityNow(PeerCapabilities.SECURITY_PRECONDITION);
  }

  // ---------------------------------------------------------------------------------------------------------
  // 3. What the fix must NOT cost
  // ---------------------------------------------------------------------------------------------------------

  /**
   * {@code peersMissingCapabilityNow} can fan out to every peer, bounded but not free, and it runs with the
   * {@code ServerSecurity} monitor held. A submission that carries no fingerprint has no precondition to decide
   * about, so it must not pay for one: that is every seed path - {@code PostAddPeerHandler},
   * {@code ServerControlPlane.connectCluster}, {@code ServerSecurity}'s bootstrap republish - which run at
   * cluster formation, the moment peers are least likely to answer a probe.
   */
  @Test
  void aSeedSubmissionWithNoFingerprintNeverAsksTheClusterAboutAPrecondition() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(mock(RaftTransactionBroker.class));
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());

    pluginOn(raft).replicateSecurityUsers(USERS);

    verify(raft, never()).peersMissingCapabilityNow(PeerCapabilities.SECURITY_PRECONDITION);
    verify(raft, never()).peersMissingCapability(PeerCapabilities.SECURITY_PRECONDITION);
  }

  /**
   * The same for the two gated documents, whose #7511 gate does query the cluster: it must keep asking about the
   * ENTRY TYPE and still not ask about the precondition, so a seed republish of the group document costs one round
   * rather than two.
   */
  @Test
  void aSeedGroupsSubmissionAsksOnlyAboutTheEntryTypeAndNotAboutAPrecondition() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(mock(RaftTransactionBroker.class));
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());

    pluginOn(raft).replicateSecurityGroups(GROUPS);

    verify(raft).peersMissingCapabilityNow(PeerCapabilities.SECURITY_GROUPS_ENTRY);
    verify(raft, never()).peersMissingCapabilityNow(PeerCapabilities.SECURITY_PRECONDITION);
  }

  /**
   * And the two rounds a gated document WITH a fingerprint does make are not two fan-outs in practice: the entry
   * gate's round has just refreshed the registry, so the precondition's own call finds a warm cache and returns
   * without dialling anything. Pinned here as the call shape, since the warm-cache short circuit itself lives in
   * {@code peersMissingCapabilityNow} and is pinned by #7511's tests.
   */
  @Test
  void aGatedDocumentWithAFingerprintAsksAboutBothTokens() {
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapability(anyString())).thenReturn(List.of(LAGGING_PEER));
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());

    pluginOn(raft).replicateSecurityApiTokens(API_TOKENS, FINGERPRINT);

    verify(raft).peersMissingCapabilityNow(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
    verify(raft).peersMissingCapabilityNow(PeerCapabilities.SECURITY_PRECONDITION);
    verify(broker).replicateSecurityApiTokens(API_TOKENS, FINGERPRINT);
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * A node whose capability cache holds nothing - a follower, which is every node that has not been elected - in a
   * cluster where every peer does in fact answer with the capability when asked.
   */
  private static RaftHAServer followerWhoseCacheIsEmptyButWhoseClusterIsUniform(
      final RaftTransactionBroker broker) {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(raft.peersMissingCapability(anyString())).thenReturn(List.of(LAGGING_PEER, "arcadedb1"));
    when(raft.peersMissingCapabilityNow(anyString())).thenReturn(List.of());
    return raft;
  }

  private static RaftHAPlugin pluginOn(final RaftHAServer raft) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, true);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    plugin.configure(server, configuration);
    plugin.setRaftHAServer(raft);
    return plugin;
  }
}
