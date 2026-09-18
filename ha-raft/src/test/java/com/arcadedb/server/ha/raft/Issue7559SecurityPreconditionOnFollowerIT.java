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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7559 against a real cluster: the #7509 compare-and-set has to reach the same verdict on every node, and
 * until this fix it reached it only on the leader.
 * <p>
 * The unit tests pin the decision against a mocked Raft server. What only a cluster can show is the half the mock
 * cannot lie about: that a FOLLOWER - which never runs the background capability monitor, so its registry holds
 * nothing - gets the right answer by asking its peers over the actual capability RPC, and that a mixed-version
 * cluster makes the leader and that follower withhold the precondition <b>together</b>.
 * <p>
 * The second half is the one #7540 contributed and it is not a nicety. If two nodes disagree about whether to
 * write the precondition, a losing entry is REFUSED on the nodes that read one and APPLIED on the node that does
 * not, so the cluster's security state diverges - strictly worse than the lost update #7509 set out to fix.
 *
 * <h2>How an "old" node is built inside one JVM</h2>
 *
 * The same way {@code Issue7219MixedVersionSchemaDeltaIT} and {@code Issue7511MixedVersionSecurityEntryIT} do it:
 * {@link RaftHAServer#setAdvertisedCapabilities} makes a node answer the capability RPC exactly as a build
 * predating the section would. It is a faithful stand-in because the decision consults nothing else - not a
 * version string, not a build number, only what the peer said it can read. The stand-in keeps advertising every
 * OTHER token, so the cluster is mixed in the one dimension under test.
 *
 * <h2>Why the assertions poll</h2>
 *
 * {@code peersMissingCapabilityNow} consults the cache before it dials, and a node that has recently been leader
 * holds advertisements for up to {@link PeerCapabilityRegistry#ADVERTISEMENT_TTL_MS}. That window is bounded and
 * deliberate (#7301), so the assertions wait it out rather than pretending it is not there - and the wait is a
 * hang detector, not a latency claim.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue7559SecurityPreconditionOnFollowerIT extends BaseRaftHATest {

  private static final String FINGERPRINT = "7f".repeat(32);

  /** How long a node is given to reach a verdict, generously over the advertisement TTL plus a few rounds. */
  private static final long VERDICT_TIMEOUT_MS = 60_000L;

  /** How long a node is given to observe a peer's advertisement change (a few refresh rounds). */
  private static final long CAPABILITY_CONVERGENCE_TIMEOUT_MS = 30_000L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void aFollowerEngagesTheCompareAndSetAndWithholdsItWithTheLeaderOnAMixedCluster() throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int laggingIndex = (leaderIndex + 1) % getServerCount();
    final int upgradedFollowerIndex = (leaderIndex + 2) % getServerCount();
    final String laggingPeerId = peerIdForIndex(laggingIndex);

    assertThat(raftServerOf(upgradedFollowerIndex).isLeader())
        .as("the node this test calls a follower must not be the leader")
        .isFalse();

    // ---- a uniform cluster: the check engages wherever the mutation was submitted -------------------------
    // This is the bug. The follower runs no capability monitor, so its registry names every peer as missing and
    // the precondition was dropped - silently, on exactly the routes that do NOT forward to the leader.
    awaitVerdict(leaderIndex, FINGERPRINT,
        "the leader writes the precondition on a cluster where every node can read one");
    awaitVerdict(upgradedFollowerIndex, FINGERPRINT,
        "a follower must reach the same answer, by asking its peers itself");

    // ---- a mixed cluster: both nodes withhold it, and withhold it together --------------------------------
    raftServerOf(laggingIndex).setAdvertisedCapabilities(withoutPrecondition(PeerCapabilities.LOCAL));
    awaitObserved(leaderIndex, laggingPeerId,
        "the leader must be told peer '" + laggingPeerId + "' cannot read a precondition");

    awaitVerdict(leaderIndex, null,
        "the leader withholds the precondition while one node predates the section");
    awaitVerdict(upgradedFollowerIndex, null,
        "and the follower reaches the SAME verdict, which is what stops the security state diverging");

    // ---- finish the rolling upgrade; no setting change, no restart, no operator step -----------------------
    raftServerOf(laggingIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);

    awaitVerdict(leaderIndex, FINGERPRINT, "the check resumes on the leader by itself");
    awaitVerdict(upgradedFollowerIndex, FINGERPRINT, "and on the follower by itself");
  }

  // -----------------------------------------------------------------------------------------------------------

  /** Everything this build advertises except the one token under test. */
  private static Set<String> withoutPrecondition(final Set<String> capabilities) {
    final Set<String> reduced = new HashSet<>(capabilities);
    reduced.remove(PeerCapabilities.SECURITY_PRECONDITION);
    return Set.copyOf(reduced);
  }

  /**
   * Waits until {@code serverIndex} decides {@code expected} about a submission carrying {@link #FINGERPRINT}.
   * <p>
   * The real decision, driven through the plugin method every security mutation funnels into, rather than a
   * re-implementation of it in the test: the whole defect was which accessor that method reads.
   */
  private void awaitVerdict(final int serverIndex, final String expected, final String what)
      throws InterruptedException {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    assertThat(plugin).as("server %d runs the Raft HA plugin", serverIndex).isNotNull();

    final long deadline = System.currentTimeMillis() + VERDICT_TIMEOUT_MS;
    String verdict = plugin.preconditionEveryPeerCanRead(FINGERPRINT);
    while (!Objects.equals(expected, verdict) && System.currentTimeMillis() < deadline) {
      Thread.sleep(500);
      verdict = plugin.preconditionEveryPeerCanRead(FINGERPRINT);
    }

    assertThat(verdict).as(what).isEqualTo(expected);
  }

  /**
   * Waits until the LEADER has actually probed {@code peerId} and recorded an advertisement without the
   * precondition token.
   * <p>
   * Deliberately not "until the leader reports the peer as missing the capability": a peer that was never
   * successfully probed reports exactly the same way, so a run in which the capability RPC was broken end to end
   * would satisfy that weaker condition instantly and the withholding below would pass for the wrong reason.
   */
  private void awaitObserved(final int observerIndex, final String peerId, final String what)
      throws InterruptedException {
    final RaftHAServer observer = raftServerOf(observerIndex);
    final Predicate<Set<String>> settled =
        capabilities -> capabilities != null && !capabilities.contains(PeerCapabilities.SECURITY_PRECONDITION);

    final long deadline = System.currentTimeMillis() + CAPABILITY_CONVERGENCE_TIMEOUT_MS;
    Set<String> observed = observedCapabilities(observer, peerId);
    while (!settled.test(observed) && observer.isLeader() && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      observed = observedCapabilities(observer, peerId);
    }

    // Only the leader runs the background refresh, so a node that lost leadership mid-test stops asking and its
    // advertisements go stale - which would read here as "the negotiation is broken" and cost the next reader an
    // hour. This test pins the leader it elected, so an election is a failed PRECONDITION and says so.
    assertThat(observer.isLeader())
        .as("this test pins the leader it elected at the start; %s lost leadership mid-test, so the capability "
            + "refresh it drives stopped", observer.getLocalPeerId())
        .isTrue();
    assertThat(settled.test(observed))
        .as(what + " (last observed advertisement: %s)", observed)
        .isTrue();
  }

  private static Set<String> observedCapabilities(final RaftHAServer observer, final String peerId) {
    final PeerCapabilityRegistry.Advertisement advertisement =
        observer.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
    return advertisement != null ? advertisement.capabilities() : null;
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    assertThat(plugin).as("server %d runs the Raft HA plugin", serverIndex).isNotNull();
    assertThat(plugin.getRaftHAServer()).as("server %d has started its Raft server", serverIndex).isNotNull();
    return plugin.getRaftHAServer();
  }
}
