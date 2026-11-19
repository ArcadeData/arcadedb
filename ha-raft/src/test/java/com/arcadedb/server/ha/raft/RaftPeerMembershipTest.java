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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.SetConfigurationException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Peer membership handling: concurrent adds of one address, the gap hand-off target and the precondition probe
 * off switch.
 */
class RaftPeerMembershipTest {

  /**
   * Issue #7802: two concurrent add-peer requests naming one address under two ids used to both commit, because the
   * uniqueness check read the configuration before a {@code Mode.ADD} that carried no predicate. The add is now a
   * compare-and-set built from the configuration the check read, so the loser is rebuilt against the winner and refused.
   */
  @Nested
  class ConcurrentAddSameAddress {

    private static RaftPeer peer(final String id, final String address) {
      return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(address).build();
    }

    /**
     * The race: this request read configuration {A,B,C}, and before its change reached the leader another request
     * committed {A,B,C,X} on the same address. The leader refuses the compare-and-set, the retry re-reads, and the
     * address check now refuses the request instead of committing a second entry for one process.
     */
    // Regression for #7802: the loser of the race is refused against the configuration holding the winner
    @Test
    void theLoserOfTheRaceIsRefusedAgainstTheConfigurationHoldingTheWinner() throws Exception {
      final List<RaftPeer> live = new ArrayList<>(List.of(peer("A", "h1:2434"), peer("B", "h2:2434"), peer("C", "h3:2434")));
      final FakeRaftHAServer server = FakeRaftHAServer.detached();
      final RaftClient client = mock(RaftClient.class);
      final AdminApi admin = mock(AdminApi.class);
      final AtomicInteger attempts = new AtomicInteger();

      server.returns("getClient", client);
      when(client.admin()).thenReturn(admin);
      server.httpAddresses(new HashMap<>());
      server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));
      server.on("getLivePeers", args -> List.copyOf(live));
      server.on("getCommittedPeersOrNull", args -> List.copyOf(live));
      when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
        attempts.incrementAndGet();
        // The other admin request commits first: the leader's configuration moves under this request's precondition.
        live.add(peer("X", "h4:2434"));
        final RaftClientReply reply = mock(RaftClientReply.class);
        when(reply.isSuccess()).thenReturn(false);
        // The Ratis type for a rejected configuration change: the retry loop must treat it as transient (only a group
        // mismatch is permanent, see RaftClusterManager.isPermanent).
        when(reply.getException()).thenReturn(new SetConfigurationException("the current configuration does not match the request"));
        return reply;
      });

      assertThatThrownBy(() -> new RaftClusterManager(server).addPeer("Y", "h4:2434"))
          .isInstanceOf(DuplicatePeerAddressException.class)
          .hasMessageContaining("'X'");
      assertThat(attempts.get()).as("the refused request must not be sent a second time").isEqualTo(1);
      assertThat(live).extracting(p -> p.getId().toString()).containsExactly("A", "B", "C", "X");
    }

    /** Concurrent adds of DIFFERENT peers must still both land: the loser is rebuilt on top of the winner. */
    // Regression for #7802: a race between two different peers still commits both
    @Test
    void aRaceBetweenTwoDifferentPeersStillCommitsBoth() throws Exception {
      final List<RaftPeer> live = new ArrayList<>(List.of(peer("A", "h1:2434"), peer("B", "h2:2434"), peer("C", "h3:2434")));
      final FakeRaftHAServer server = FakeRaftHAServer.detached();
      final RaftClient client = mock(RaftClient.class);
      final AdminApi admin = mock(AdminApi.class);
      final List<SetConfigurationRequest.Arguments> sent = new ArrayList<>();

      server.returns("getClient", client);
      when(client.admin()).thenReturn(admin);
      server.httpAddresses(new HashMap<>());
      server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));
      server.on("getLivePeers", args -> List.copyOf(live));
      server.on("getCommittedPeersOrNull", args -> List.copyOf(live));
      when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
        final SetConfigurationRequest.Arguments args = invocation.getArgument(0);
        sent.add(args);
        final RaftClientReply reply = mock(RaftClientReply.class);
        if (sent.size() == 1) {
          live.add(peer("X", "h4:2434")); // the other request wins the race
          when(reply.isSuccess()).thenReturn(false);
          when(reply.getException()).thenReturn(new SetConfigurationException("the current configuration does not match the request"));
        } else {
          live.clear();
          live.addAll(args.getServersInNewConf());
          when(reply.isSuccess()).thenReturn(true);
        }
        return reply;
      });

      new RaftClusterManager(server).addPeer("Y", "h5:2434");

      assertThat(sent).hasSize(2);
      assertThat(sent.get(1).getServersInCurrentConf()).extracting(p -> p.getId().toString())
          .as("the retry is rebuilt on top of the winner").containsExactly("A", "B", "C", "X");
      assertThat(live).extracting(p -> p.getId().toString()).containsExactly("A", "B", "C", "X", "Y");
    }

    /** A configuration that cannot be read is reported as such, not papered over with the declared list; the wait is cut short by an interrupt. */
    // Regression for #7802: an unreadable configuration is reported not guessed
    @Test
    void anUnreadableConfigurationIsReportedNotGuessed() {
      final FakeRaftHAServer server = FakeRaftHAServer.detached();
      server.returns("getCommittedPeersOrNull", null);
      server.returns("getClient", mock(RaftClient.class));
      server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));

      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> new RaftClusterManager(server).addPeer("Y", "h5:2434"))
            .isInstanceOf(ConfigurationException.class).hasMessageContaining("cannot read the live Raft configuration");
        assertThat(Thread.currentThread().isInterrupted()).as("the interrupt is preserved").isTrue();
      } finally {
        Thread.interrupted();
      }
    }

    // Regression for #7802: a configuration holding a shared address is reported
    @Test
    void aConfigurationHoldingASharedAddressIsReported() {
      final List<List<String>> shared = ClusterMembership.findSharedAddresses(
          List.of(peer("A", "127.0.0.1:2434"), peer("B", "h2:2434"), peer("A2", "localhost:2434"), peer("A3", "127.0.0.1:2434"),
              peer("B2", "H2:2434"), peer("C", "h3:2434")));

      assertThat(shared).containsExactly(List.of("A", "A2", "A3"), List.of("B", "B2"));
    }

    // Regression for #7802: a healthy configuration raises no alert
    @Test
    void aHealthyConfigurationRaisesNoAlert() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addSharedPeerAddressAlert(ClusterMembership.findSharedAddresses(
          List.of(peer("A", "h1:2434"), peer("B", "h2:2434"))), alerts);
      assertThat(alerts.length()).isZero();
    }

    // Regression for #7802: the alert names every id of the shared address and the remedy
    @Test
    void theAlertNamesEveryIdOfTheSharedAddressAndTheRemedy() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addSharedPeerAddressAlert(ClusterMembership.findSharedAddresses(
          List.of(peer("A", "h1:2434"), peer("B", "h1:2434"))), alerts);

      assertThat(alerts.length()).isEqualTo(1);
      assertThat(alerts.getJSONObject(0).getString("id")).isEqualTo("peers-share-address");
      assertThat(alerts.getJSONObject(0).getString("message")).contains("[[A, B]]");
      assertThat(alerts.getJSONObject(0).getString("recommendation")).contains("DELETE /api/v1/cluster/peer/{id}");
    }
  }

  /**
   * Regression test for issue #8665: the leader service-gap hand-off (#8529) picked its target from the log index alone,
   * which a service gap does not show in, so it could not tell a target that shares the gap. A database missing on every
   * node, or a node-global stale-snapshot gap after a cluster-wide crash, made each hand-off succeed and the next leader
   * hand off in turn. Each peer now reports its own gap on the capability poll the leader already runs, the leader
   * screens those peers out of the hand-off candidates, and a hand-off with no eligible peer no longer falls back to a
   * step-down with no target.
   */
  @Nested
  class GapHandOffTarget {

    private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
    private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
    private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

    // -- the wire: a peer says it has a gap ---------------------------------------------------------------------------

    // Regression for #8665: a node with a gap says so
    @Test
    void aNodeWithAGapSaysSo() {
      final JSONObject json = PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of("schema-delta"), true);

      assertThat(json.getBoolean(PostCapabilitiesHandler.SERVICE_GAP, false)).isTrue();
    }

    // Regression for #8665: a healthy nodes document carries no gap field
    @Test
    void aHealthyNodesDocumentCarriesNoGapField() {
      assertThat(PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of("schema-delta"), false)
          .has(PostCapabilitiesHandler.SERVICE_GAP)).isFalse();
      assertThat(PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of("schema-delta")).has(PostCapabilitiesHandler.SERVICE_GAP))
          .isFalse();
    }

    // Regression for #8665: the query reads the gap and a peer that predates it reads as none
    @Test
    void theQueryReadsTheGapAndAPeerThatPredatesItReadsAsNone() throws Exception {
      final String url = "http://peer-b:2480/api/v1/cluster/capabilities";

      assertThat(PeerCapabilityQuery.parse("peer-b_2435",
          PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of(), true).toString(), url).serviceGap()).isTrue();
      assertThat(PeerCapabilityQuery.parse("peer-b_2435",
          "{\"peerId\":\"peer-b_2435\",\"version\":\"26.5.1\",\"capabilities\":[]}", url).serviceGap())
          .as("a build that predates the field").isFalse();
    }

    // -- what the leader remembers ------------------------------------------------------------------------------------

    // Regression for #8665: the registry reports the peers that have a gap
    @Test
    void theRegistryReportsThePeersThatHaveAGap() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long generation = registry.generation();
      registry.record(generation, B.toString(), Set.of(), "26.10.1", true);
      registry.record(generation, C.toString(), Set.of(), "26.10.1", false);

      assertThat(registry.peersWithServiceGap()).containsExactly(B.toString());

      registry.record(generation, B.toString(), Set.of(), "26.10.1", false);
      assertThat(registry.peersWithServiceGap()).as("the gap closed, the next poll says so").isEmpty();
    }

    // Regression for #8665: a stale answer is not believed and an old build is no gap
    @Test
    void aStaleAnswerIsNotBelievedAndAnOldBuildIsNoGap() {
      final AtomicLong clock = new AtomicLong(1_000L);
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(5_000L);
      registry.setClock(clock::get);
      final long generation = registry.generation();
      registry.record(generation, B.toString(), Set.of(), "26.10.1", true);
      registry.record(generation, C.toString(), Set.of(), "26.5.1");

      clock.addAndGet(5_001L);
      assertThat(registry.peersWithServiceGap()).as("an answer past its TTL says nothing").isEmpty();
    }

    // Regression for #8665: an unknown peer is not counted as having a gap
    @Test
    void anUnknownPeerIsNotCountedAsHavingAGap() {
      assertThat(new PeerCapabilityRegistry().peersWithServiceGap()).isEmpty();
    }

    // -- the selection ------------------------------------------------------------------------------------------------

    // Regression for #8665: a peer that shares the gap is not a hand off candidate
    @Test
    void aPeerThatSharesTheGapIsNotAHandOffCandidate() {
      final Set<String> reachable = Set.of(B.toString(), C.toString());

      final Set<String> eligible = StepDownTargets.withoutServiceGapPeers(reachable, Set.of(B.toString()));

      assertThat(eligible).containsExactly(C.toString());
      assertThat(StepDownTargets.selectStepDownTargets(peers(), SELF, null, eligible)).extracting(p -> p.getId().toString())
          .containsExactly(C.toString());
      assertThat(reachable).as("the caller's set is not modified").hasSize(2);
    }

    // Regression for #8665: when every peer shares the gap no one is a candidate
    @Test
    void whenEveryPeerSharesTheGapNoOneIsACandidate() {
      final Set<String> eligible = StepDownTargets.withoutServiceGapPeers(Set.of(B.toString(), C.toString()),
          Set.of(B.toString(), C.toString()));

      assertThat(PeerChannelRecovery.hasHandoffTarget(peers(), SELF, null, eligible)).isFalse();
    }

    // Regression for #8665: a healthy cluster keeps its reachable set as is
    @Test
    void aHealthyClusterKeepsItsReachableSetAsIs() {
      final Set<String> reachable = Set.of(B.toString(), C.toString());

      assertThat(StepDownTargets.withoutServiceGapPeers(reachable, Set.of())).isSameAs(reachable);
    }

    // -- no eligible peer: back off, do not rotate --------------------------------------------------------------------

    // Regression for #8665: with no eligible peer the hand off does not step down with no target
    @Test
    void withNoEligiblePeerTheHandOffDoesNotStepDownWithNoTarget() throws Exception {
      final AdminApi admin = mock(AdminApi.class);
      final RaftClient client = mock(RaftClient.class);
      when(client.admin()).thenReturn(admin);
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.returns("getClient", client);
      raft.localPeerId(SELF);
      raft.leader(true);
      raft.returns("getLivePeers", peers());
      raft.returns("handoffReachablePeers", Set.of()); // both peers report the gap this leader has
      final RaftClusterManager manager = new RaftClusterManager(raft);

      assertThat(manager.transferLeadership(10_000L, false)).isFalse();

      verify(admin, never()).transferLeadership(org.mockito.ArgumentMatchers.any(), anyLong());
      verify(client, never()).getGroupId();
    }

    // Regression for #8665: the gap hand off asks for no bare step down
    @Test
    void theGapHandOffAsksForNoBareStepDown() throws Exception {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(true);
      raft.returns("transferLeadership", false);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setRaftHAServer(raft);

      sm.runUnderInstallGate("db", () -> assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isFalse());

      // The (timeout, bareStepDownFallback) overload, asked once with no bare step-down; the 1-argument overload never
      assertThat(raft.calls("transferLeadership")).filteredOn(args -> args.size() == 2 && Boolean.FALSE.equals(args.get(1)))
          .hasSize(1);
      assertThat(raft.calls("transferLeadership")).noneMatch(args -> args.size() == 1);
    }

    private static List<RaftPeer> peers() {
      return List.of(peer(SELF), peer(B), peer(C));
    }

    private static RaftPeer peer(final RaftPeerId id) {
      return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
    }
  }

  /**
   * Issue #8109, absorbing #7827: the compare-and-set decision of a security mutation runs a synchronous capability
   * probe round whenever the cached answer is not a full "yes" - which it never is while a peer is DOWN - and every
   * caller holds the {@code ServerSecurity} monitor across it. The #7511 gate had an off switch for its own round
   * ({@code arcadedb.ha.securityEntryCapabilityGate}); the precondition decision had none, so with the gate off a
   * group change still paid one probe round and a user change, which has no gate, always did.
   * <p>
   * With the switch off the decision now reads the cached answer, which the capability monitor keeps current on every
   * node (#7549), and never dials.
   */
  @Nested
  class PreconditionProbeOffSwitch {

    private static final String FINGERPRINT = "7f".repeat(32);
    private static final String USERS       = "[{\"name\":\"root\"}]";
    private static final String GROUPS      = "{\"databases\":{}}";

    // Regression for #8109: with the gate off a user change never runs a probe round
    @Test
    void withTheGateOffAUserChangeNeverRunsAProbeRound() {
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      final FakeRaftHAServer raft = raftWhoseCacheSays(broker, List.of());

      pluginOn(raft, false).replicateSecurityUsers(USERS, FINGERPRINT);

      assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
      assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, FINGERPRINT));
    }

    /** The gated documents' own round was already switched off by the gate; the precondition's must be too. */
    // Regression for #8109: with the gate off a group change never runs a probe round
    @Test
    void withTheGateOffAGroupChangeNeverRunsAProbeRound() {
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      final FakeRaftHAServer raft = raftWhoseCacheSays(broker, List.of());

      pluginOn(raft, false).replicateSecurityGroups(GROUPS, FINGERPRINT);

      assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
      assertThat(broker.calls("replicateSecurityGroups")).containsOnlyOnce(Arrays.asList(GROUPS, FINGERPRINT));
    }

    /**
     * The direction the cached answer can cost in is the safe one: a peer the cache does not know withholds the
     * precondition - the pre-#7509 behaviour - rather than being dialled with the monitor held.
     */
    // Regression for #8109: with the gate off a peer the cache does not know withholds the precondition
    @Test
    void withTheGateOffAPeerTheCacheDoesNotKnowWithholdsThePrecondition() {
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      final FakeRaftHAServer raft = raftWhoseCacheSays(broker, List.of("arcadedb2"));

      pluginOn(raft, false).replicateSecurityUsers(USERS, FINGERPRINT);

      assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
      assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, null));
    }

    /** The control: with the gate on - the default - the decision still asks now, as #7559 requires. */
    // Regression for #8109: with the gate on the decision still asks now
    @Test
    void withTheGateOnTheDecisionStillAsksNow() {
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      final FakeRaftHAServer raft = raftWhoseCacheSays(broker, List.of("arcadedb2"));
      raft.returns("peersMissingCapabilityNow", List.of());

      pluginOn(raft, true).replicateSecurityUsers(USERS, FINGERPRINT);

      assertThat(raft.calls("peersMissingCapabilityNow")).containsOnlyOnce(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
      assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, FINGERPRINT));
    }

    private static FakeRaftHAServer raftWhoseCacheSays(final RaftTransactionBroker broker, final List<String> missing) {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.transactionBroker(broker);
      raft.returns("peersMissingCapability", missing);
      return raft;
    }

    private static RaftHAPlugin pluginOn(final RaftHAServer raft, final boolean gate) {
      final RaftHAPlugin plugin = new RaftHAPlugin();
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, gate);
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
      plugin.configure(server, configuration);
      plugin.setRaftHAServer(raft);
      return plugin;
    }
  }
}
