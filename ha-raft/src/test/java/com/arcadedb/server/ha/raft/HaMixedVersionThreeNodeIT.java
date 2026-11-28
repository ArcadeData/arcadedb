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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7511 against a real cluster: a group change or a token mint on a MIXED-VERSION cluster is refused rather
 * than committed, and starts working by itself once the last node is upgraded.
 * <p>
 * This is the test the issue asks for. The unit tests pin the decision and the two transports' statuses; what only
 * a cluster can show is that the gate reaches its answer through the actual capability RPC, over the wire, from a
 * FOLLOWER as well as from the leader - the path that would otherwise refuse every group change ever made on a
 * follower, because the background capability monitor ran on the leader alone until issue #7549 moved it onto
 * every node.
 *
 * <h2>How an "old" node is built inside one JVM</h2>
 *
 * The same way {@link HaMixedVersionSchemaDeltaIT} does it: {@link RaftHAServer#setAdvertisedCapabilities}
 * makes a node answer the capability RPC exactly as a build without the two decoders would. It is a faithful
 * stand-in because the decision consults nothing else - not a version string, not a build number, only what the
 * peer said it can decode. The old node keeps advertising {@link PeerCapabilities#SCHEMA_DELTA}, so the cluster is
 * mixed in the one dimension under test and unchanged in every other.
 *
 * <h2>Why three nodes</h2>
 *
 * The gate asks about the OTHER peers, never about the node running it - a node cannot fail to decode what its own
 * build writes, and a genuinely old node has no gate at all, since the gate ships with the decoder. To exercise the
 * follower arm there has to be a follower that is NOT the lagging node, which needs a third node.
 * <p>
 * Sibling of {@link HaMixedVersionIT}, renamed from {@code Issue7511MixedVersionSecurityEntryIT}. It keeps a fresh cluster
 * per test because it runs on three servers, not two, and because its second test (issue #13447) stops the API-token
 * file watchers of its servers and leaves one of them withholding capabilities while it runs.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class HaMixedVersionThreeNodeIT extends BaseRaftHATest {

  /** How long a node is given to observe a peer's advertisement change (a few refresh rounds). */
  private static final long CAPABILITY_CONVERGENCE_TIMEOUT_MS = 30_000L;

  /**
   * A hang detector for the waits on the security seed, not a latency bound: the rejoining node's four attempts and
   * their backoff take about half a minute when every one of them is refused.
   */
  private static final long SEED_WAIT_MS = 180_000L;

  /** What a build that predates #7373 advertises: it decodes schema deltas, and neither new entry type. */
  private static final Set<String> PRE_7373_BUILD = Set.of(PeerCapabilities.SCHEMA_DELTA);

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // arcadedb.ha.securityEntryCapabilityGate is deliberately NOT set: it defaults to on, and a test that turned
    // it on by hand could not tell "the default protects an operator who did nothing" from "the test did it".
  }

  /** Issue #7511, from {@code Issue7511MixedVersionSecurityEntryIT}. */
  @Test
  void aGroupOrTokenChangeIsRefusedWhileOneNodeCannotDecodeTheEntryAndResumesAfterTheUpgrade()
      throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int laggingIndex = (leaderIndex + 1) % getServerCount();
    final int upgradedFollowerIndex = (leaderIndex + 2) % getServerCount();
    final String laggingPeerId = peerIdForIndex(laggingIndex);

    // One node is mid-rolling-upgrade: still on the build that predates #7373.
    raftServerOf(laggingIndex).setAdvertisedCapabilities(PRE_7373_BUILD);
    awaitObserved(leaderIndex, laggingPeerId, hasNeither(), "the leader must be told peer '" + laggingPeerId
        + "' cannot decode either new entry type");

    // ---- the leader refuses, and says which node is holding the cluster back -------------------------------
    assertThatThrownBy(() -> controlPlaneOn(leaderIndex).saveGroup("*", "reader", readerGroup()))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(laggingPeerId)
        .hasMessageContaining(PeerCapabilities.SECURITY_GROUPS_ENTRY);

    assertThatThrownBy(() -> controlPlaneOn(leaderIndex).createApiToken("ci", "*", 0, new JSONObject()))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);

    // ---- and so does an UPGRADED FOLLOWER, which keeps a capability cache of its own ----------------------
    // The REST routes for groups and API tokens do not forward to the leader, so this is not a hypothetical
    // path: it is what happens whenever a load balancer sends the request to a node that is not the leader.
    // Since #7549 the follower refreshes its own registry, and the gate only re-probes when that registry
    // reports the capability MISSING: an advertisement recorded before the downgrade above stays fresh for up to
    // PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS and lets the change through. A real downgrade restarts the node,
    // which fails the probes in between; the in-JVM flip does not, so wait for the follower to see it too.
    awaitObserved(upgradedFollowerIndex, laggingPeerId, hasNeither(), "the upgraded follower must be told peer '"
        + laggingPeerId + "' cannot decode either new entry type");
    assertThatThrownBy(() -> controlPlaneOn(upgradedFollowerIndex).saveGroup("*", "reader", readerGroup()))
        .as("a follower must reach the same answer, by asking the peers itself")
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(laggingPeerId);

    // ---- nothing was submitted, so nothing halted ---------------------------------------------------------
    assertThat(groupsOn(laggingIndex).has("reader"))
        .as("the refused group must exist on no node at all")
        .isFalse();
    assertThat(groupsOn(leaderIndex).has("reader")).isFalse();
    assertThat(getServer(laggingIndex).isStarted())
        .as("the node the entry would have halted is still running, which is the whole point of #7511")
        .isTrue();
    assertThat(raftServerOf(laggingIndex).isLeader() || getRaftPlugin(laggingIndex) != null)
        .as("and still a member of the cluster")
        .isTrue();

    // ---- finish the rolling upgrade; no setting change, no restart, no operator step ------------------------
    raftServerOf(laggingIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    awaitObserved(leaderIndex, laggingPeerId, hasBoth(), "the leader must be told peer '" + laggingPeerId
        + "' can now decode both entry types");

    controlPlaneOn(leaderIndex).saveGroup("*", "reader", readerGroup());

    for (int i = 0; i < getServerCount(); i++)
      assertThat(groupsOn(i).has("reader"))
          .as("once the cluster is uniform the change goes through and reaches node %d", i)
          .isTrue();
  }

  /**
   * Issue #13447: a security seed the capability gate refused during a rolling restart is run again once the peer it was
   * refused for advertises its capabilities, so the node that rejoined does not keep a revoked token.
   * <p>
   * The reported sequence, on its own timeline. A node rejoins holding a token the cluster no longer has (the stand-in
   * for one revoked while it was away) and asks the leader to re-seed, while the NEXT peer of the rolling restart is
   * still booting and has not advertised the two security entry capabilities. The gate refuses the group and API-token
   * entries, the seed's retry budget runs out, and so do the rejoining node's own retries: the restarting peer answers
   * only after all of that, as it did 25 s later in the report. Nothing ran the seed again, and the token went on
   * authenticating on that node for two days.
   * <p>
   * The restarting peer is built the way the test above builds an old one: what the gate consults is the advertisement,
   * and a peer that has not answered yet and a peer that answers without the capability are one "no" to it.
   * <p>
   * The peer is held back until the rejoining node has given up, on purpose: from there only the leader's own re-run
   * can deliver the documents, which is the half of the fix a shorter wait would let the node's retries hide.
   */
  @Test
  void aSeedRefusedWhileAPeerRestartsIsRunAgainOnceThatPeerAdvertises() throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int rejoinedIndex = (leaderIndex + 1) % getServerCount();
    final int restartingIndex = (leaderIndex + 2) % getServerCount();
    final String restartingPeerId = peerIdForIndex(restartingIndex);

    final RaftHAServer rejoinedRaft = raftServerOf(rejoinedIndex);
    final SecurityCatchUp catchUp = rejoinedRaft.getStateMachine().getSecurityCatchUp();
    final MembershipSecuritySeeder leaderSeeder = raftServerOf(leaderIndex).getStateMachine().getMembershipSecuritySeeder();

    // Whatever the servers' own startup asked for has to be over, so the request below is the one observed.
    HaSecurityMutationRoutingTwoNodeIT.waitUntil(() -> {
      final PoolStats pool = catchUp.getPoolStats();
      return pool.activeThreads() == 0 && pool.queueDepth() == 0;
    }, SEED_WAIT_MS, "the rejoined node's startup catch-up must finish");

    // The servers of this fixture share one config directory, which three real nodes never do: without this, the
    // other two would re-read the file the rejoined node is about to write and take its token for their own.
    for (int i = 0; i < getServerCount(); i++)
      getServer(i).getSecurity().getApiTokenConfiguration().stop();

    // The node came back holding a token the cluster does not: on this node alone, with no replication.
    final String staleToken = getServer(rejoinedIndex).getSecurity().getApiTokenConfiguration()
        .createToken("revoked-while-away", "*", 0L, new JSONObject()).getString("token");
    assertThat(tokenOn(rejoinedIndex, staleToken)).as("the stale token authenticates on the rejoined node").isNotNull();
    assertThat(tokenOn(leaderIndex, staleToken)).as("and nowhere else").isNull();

    // The next peer of the rolling restart is still booting: it has not advertised the security entry capabilities.
    raftServerOf(restartingIndex).setAdvertisedCapabilities(PRE_7373_BUILD);
    awaitObserved(leaderIndex, restartingPeerId, hasNeither(), "the leader must see peer '" + restartingPeerId
        + "' without the security entry capabilities");

    // The rejoined node's catch-up: its fingerprints do not match, so the leader seeds, and the gate refuses.
    catchUp.rearmForTests();
    catchUp.onFirstLeaderObserved(getServer(rejoinedIndex), rejoinedRaft);

    HaSecurityMutationRoutingTwoNodeIT.waitUntil(() -> leaderSeeder.owedSeedReasonForTest() != null, SEED_WAIT_MS,
        "the leader must keep the seed the capability gate refused as owed");
    HaSecurityMutationRoutingTwoNodeIT.waitUntil(() -> {
      final PoolStats pool = catchUp.getPoolStats();
      return !catchUp.hasRequestedSinceStart() && pool.activeThreads() == 0 && pool.queueDepth() == 0;
    }, SEED_WAIT_MS, "the rejoined node must run out of retries while the peer is still restarting");

    assertThat(raftServerOf(leaderIndex).isLeader()).as("this test pins the leader it elected at the start").isTrue();
    assertThat(tokenOn(rejoinedIndex, staleToken))
        .as("nothing could be seeded while the peer withholds the capability, so the token is still good here")
        .isNotNull();
    assertThat(leaderSeeder.owedSeedReasonForTest()).as("and the seed is still owed").isNotNull();

    // The restarting peer comes up. No operator step, no leader change, no new request from the rejoined node.
    raftServerOf(restartingIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);

    HaSecurityMutationRoutingTwoNodeIT.waitUntil(() -> tokenOn(rejoinedIndex, staleToken) == null, SEED_WAIT_MS,
        "the leader must run the owed seed again once the peer advertises, so the stale token stops authenticating "
            + "on the rejoined node");
    assertThat(getServer(rejoinedIndex).getSecurity().apiTokensFingerprint())
        .as("the rejoined node holds the leader's API-token document")
        .isEqualTo(getServer(leaderIndex).getSecurity().apiTokensFingerprint());
    HaSecurityMutationRoutingTwoNodeIT.waitUntil(() -> leaderSeeder.owedSeedReasonForTest() == null, SEED_WAIT_MS,
        "a seed that committed leaves nothing owed");
  }

  // -----------------------------------------------------------------------------------------------------------

  private JSONObject tokenOn(final int serverIndex, final String plaintextToken) {
    return getServer(serverIndex).getSecurity().getApiTokenConfiguration().getToken(plaintextToken);
  }

  private static Predicate<Set<String>> hasNeither() {
    return capabilities -> capabilities != null
        && !capabilities.contains(PeerCapabilities.SECURITY_GROUPS_ENTRY)
        && !capabilities.contains(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  private static Predicate<Set<String>> hasBoth() {
    return capabilities -> capabilities != null
        && capabilities.contains(PeerCapabilities.SECURITY_GROUPS_ENTRY)
        && capabilities.contains(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  private ServerControlPlane controlPlaneOn(final int serverIndex) {
    return new ServerControlPlane(getServer(serverIndex));
  }

  private JSONObject groupsOn(final int serverIndex) {
    return getServer(serverIndex).getSecurity().groupsToJSON().getJSONObject("databases").getJSONObject("*")
        .getJSONObject("groups");
  }

  private static JSONObject readerGroup() {
    return new JSONObject()
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("access", new JSONArray())
        .put("types", new JSONObject());
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    assertThat(plugin).as("server %d runs the Raft HA plugin", serverIndex).isNotNull();
    assertThat(plugin.getRaftHAServer()).as("server %d has started its Raft server", serverIndex).isNotNull();
    return plugin.getRaftHAServer();
  }

  /**
   * Waits until {@code observerIndex} has actually PROBED {@code peerId} and recorded an advertisement satisfying
   * {@code settled}.
   * <p>
   * Deliberately not "until the observer reports the peer as missing the capability": a peer that was never
   * successfully probed reports exactly the same way, so a run in which the capability RPC was broken end to end
   * would satisfy that weaker condition instantly - and the refusals below would then pass for the wrong reason,
   * proving nothing about negotiation. Waiting for a RECORDED advertisement means the probe demonstrably worked
   * and the answer it carried is the one the test planted.
   */
  private void awaitObserved(final int observerIndex, final String peerId, final Predicate<Set<String>> settled,
      final String what) throws InterruptedException {
    final RaftHAServer observer = raftServerOf(observerIndex);
    final boolean observerIsLeader = observer.isLeader();
    final long deadline = System.currentTimeMillis() + CAPABILITY_CONVERGENCE_TIMEOUT_MS;
    Set<String> observed = observedCapabilities(observer, peerId);
    while (!settled.test(observed) && (!observerIsLeader || observer.isLeader())
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      observed = observedCapabilities(observer, peerId);
    }
    // The refresh runs in every role since #7549, but the leader-side assertions above address the node elected at
    // the start: an election mid-test would make them assert on the wrong role, which would read as "the
    // negotiation is broken" and cost the next reader an hour. So an election is a failed PRECONDITION and says so.
    if (observerIsLeader)
      assertThat(observer.isLeader())
          .as("this test pins the leader it elected at the start; %s lost leadership mid-test",
              observer.getLocalPeerId())
          .isTrue();
    // A wall-clock bound used as a hang detector, not as a latency assertion: the refresh runs every
    // PeerCapabilityRegistry.REFRESH_PERIOD_MS, so a budget of several rounds only fires when it stopped asking.
    assertThat(settled.test(observed))
        .as(what + " (last observed advertisement: %s)", observed)
        .isTrue();
  }

  private static Set<String> observedCapabilities(final RaftHAServer observer, final String peerId) {
    final PeerCapabilityRegistry.Advertisement advertisement =
        observer.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
    return advertisement != null ? advertisement.capabilities() : null;
  }
}
