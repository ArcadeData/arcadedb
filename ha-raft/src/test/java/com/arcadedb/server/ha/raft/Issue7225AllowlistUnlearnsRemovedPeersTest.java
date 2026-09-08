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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7225, a follow-up to #7132: the allowlist learned hosts from the live Raft
 * configuration but never unlearned them, so a peer deliberately removed from the cluster kept inbound
 * access to the Raft gRPC port - and a steady DNS lookup per refresh tick - until the process restarted.
 * <p>
 * Reconciliation is now replace-semantics ({@code setMemberHosts}) rather than a merge, with two hosts
 * deliberately exempt: the Kubernetes headless-service domain pinned at install time (#7132), which is what
 * admits a scale-up pod <i>before</i> it is a member, and everything declared in
 * {@code arcadedb.ha.serverList}, which is configuration and not membership.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7225AllowlistUnlearnsRemovedPeersTest {

  // ---------------------------------------------------------------------------
  // PeerAddressAllowlistFilter: replace semantics
  // ---------------------------------------------------------------------------

  /** Finding 1: a peer removed from the Raft configuration loses its Raft gRPC access on the next tick. */
  @Test
  void aHostThatLeftTheConfigurationIsUnlearnedAndItsIpsAreRejected() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("arcadedb-0", List.of("10.1.13.1"));
    dns.table.put("arcadedb-3", List.of("10.1.13.4"));

    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("arcadedb-0"), 30_000L, 0L,
        300_000L, clock::get, dns);

    // Scale-up: pod 3 joins, the tick feeds the new membership in, and it is admitted.
    assertThat(f.setMemberHosts(List.of("arcadedb-0", "arcadedb-3"))).isTrue();
    assertThat(f.getLearnedHosts()).containsExactly("arcadedb-3");
    assertThat(f.isAllowed("10.1.13.4")).isTrue();

    // Scale-down: pod 3 is removed from the group. The next tick reports the smaller membership.
    assertThat(f.setMemberHosts(List.of("arcadedb-0"))).isTrue();
    assertThat(f.getLearnedHosts()).isEmpty();
    assertThat(f.getAllowedIps()).doesNotContain("10.1.13.4");
    assertThat(f.isAllowed("10.1.13.4")).as("a removed peer must not keep Raft gRPC access").isFalse();
    assertThat(f.isAllowed("10.1.13.1")).as("the surviving configured peer is untouched").isTrue();
  }

  /**
   * The Kubernetes headless-service domain seeded by #7132 is how a scale-up pod is admitted BEFORE it is a
   * member, so replace semantics has to retain it explicitly rather than treat it as a learned host.
   */
  @Test
  void thePinnedHeadlessServiceDomainSurvivesAMembershipShrink() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("arcadedb-0", List.of("10.1.13.1"));
    dns.table.put("arcadedb.myns.svc.cluster.local", List.of("10.1.13.1", "10.1.13.2"));
    dns.table.put("arcadedb-1", List.of("10.1.13.2"));

    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("arcadedb-0"), 30_000L, 0L,
        300_000L, clock::get, dns);
    f.learnPeerHosts(List.of("arcadedb.myns.svc.cluster.local"));

    f.setMemberHosts(List.of("arcadedb-0", "arcadedb-1"));
    assertThat(f.getLearnedHosts()).containsExactlyInAnyOrder("arcadedb.myns.svc.cluster.local", "arcadedb-1");

    f.setMemberHosts(List.of("arcadedb-0"));
    assertThat(f.getLearnedHosts()).containsExactly("arcadedb.myns.svc.cluster.local");
    assertThat(f.getPinnedHosts()).containsExactly("arcadedb.myns.svc.cluster.local");
    assertThat(f.getMemberHosts()).isEmpty();
  }

  /** Reconciling to the same membership must not touch DNS: the tick runs on every node, forever. */
  @Test
  void reconcilingTheSameMembershipIsANoOp() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("peerB", List.of("10.0.0.2"));
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, dns);

    assertThat(f.setMemberHosts(List.of("peerA", "peerB"))).isTrue();
    assertThat(f.setMemberHosts(List.of("peerB", "peerA"))).as("order is irrelevant").isFalse();
    assertThat(f.setMemberHosts(List.of("peerA"))).isTrue();
    assertThat(f.setMemberHosts(List.of())).as("a configured host is never a learned host").isFalse();
  }

  /**
   * The sticky last-known-good IPs must be pruned along with the host. Otherwise a departed peer whose name
   * no longer resolves is readmitted the moment anything re-learns it, from a retention entry that outlived
   * the membership it belonged to.
   */
  @Test
  void aDroppedHostLosesItsStickyLastKnownIps() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("gone", List.of("10.0.0.9"));
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, dns);

    f.setMemberHosts(List.of("gone"));
    assertThat(f.getAllowedIps()).contains("10.0.0.9");

    f.setMemberHosts(List.of());
    assertThat(f.getAllowedIps()).doesNotContain("10.0.0.9");

    // The peer is gone from DNS too, as a decommissioned pod is. Re-learning the name must resolve nothing:
    // its pre-removal IPs are no longer retained on its behalf.
    dns.table.remove("gone");
    f.setMemberHosts(List.of("gone"));
    assertThat(f.getAllowedIps()).as("a sticky entry must not outlive the membership that created it")
        .doesNotContain("10.0.0.9");
  }

  /**
   * Pinning and membership can name the same host, and the contract is that the pin wins: it keeps the host
   * admitted after the member leaves. Refusing the pin instead would make it evaporate at the next shrink,
   * which is exactly when the caller wanted it. Nothing in the tree pins a peer hostname today - the one
   * production caller pins the static Kubernetes headless-service domain - so this pins the contract rather
   * than a live scenario, and it is the interaction the rest of the suite does not touch.
   */
  @Test
  void aPinnedHostStaysAdmittedEvenWhenMembershipDropsIt() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("both", List.of("10.0.0.7"));
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, dns);

    f.setMemberHosts(List.of("both"));
    assertThat(f.learnPeerHosts(List.of("both"))).as("a pin is accepted even for a host that is a member").isTrue();
    assertThat(f.getPinnedHosts()).containsExactly("both");
    assertThat(f.getMemberHosts()).containsExactly("both");

    f.setMemberHosts(List.of());
    assertThat(f.getMemberHosts()).isEmpty();
    assertThat(f.getPinnedHosts()).as("a pin outlives the membership that shared its name").containsExactly("both");
    assertThat(f.isAllowed("10.0.0.7")).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Finding 2: the startup fail-open log line counts configured hosts only
  // ---------------------------------------------------------------------------

  @Test
  void theResolvedHostCountNeverExceedsTheConfiguredHostCount() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("learned-1", List.of("10.0.0.11"));
    dns.table.put("learned-2", List.of("10.0.0.12"));
    // peerB and peerC are not Ready yet, so the filter is still below quorum and still failing open.
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA", "peerB", "peerC"),
        30_000L, 60_000L, 300_000L, clock::get, dns);

    f.learnPeerHosts(List.of("learned-1"));
    f.setMemberHosts(List.of("learned-2"));

    assertThat(f.isQuorumResolved()).isFalse();
    assertThat(f.getResolvedPeerHostCount())
        .as("the count an operator reads against the configured total must count configured hosts only")
        .isEqualTo(1);
    assertThat(f.getResolvedPeerHostCount()).isLessThanOrEqualTo(3);
  }

  // ---------------------------------------------------------------------------
  // Wiring in RaftHAServer
  // ---------------------------------------------------------------------------

  @Test
  void reconcilingFromTheCommittedConfigurationUnlearnsARemovedPeer() {
    final ContextConfiguration config = kubernetesConfiguration();
    final RaftHAServer server = detachedServer(config);
    server.buildParameters(config);
    final PeerAddressAllowlistFilter filter = server.allowlistFilterForTest();

    server.reconcileAllowlistMembership(List.of(peer("arcadedb-0"), peer("arcadedb-1")));
    assertThat(filter.getLearnedHosts())
        .containsExactlyInAnyOrder("arcadedb.myns.svc.cluster.local", "arcadedb-1.arcadedb.myns.svc.cluster.local");

    server.reconcileAllowlistMembership(List.of(peer("arcadedb-0")));
    assertThat(filter.getLearnedHosts()).as("the removed peer is unlearned, the seeded service domain is not")
        .containsExactly("arcadedb.myns.svc.cluster.local");
  }

  /**
   * {@code getCommittedPeersOrNull()} returns null while the division cannot be read (issue #5271), and
   * {@code getLivePeers()} substitutes the DECLARED server list there. Replacing membership with the declared
   * list would unlearn every runtime-joined peer on every restart window - exactly the #7132 regression.
   */
  @Test
  void anUnreadableMembershipLeavesTheLearnedHostsAlone() {
    final ContextConfiguration config = kubernetesConfiguration();
    final RaftHAServer server = detachedServer(config); // Ratis server never started: membership is unreadable
    server.buildParameters(config);
    final PeerAddressAllowlistFilter filter = server.allowlistFilterForTest();

    server.reconcileAllowlistMembership(List.of(peer("arcadedb-0"), peer("arcadedb-1")));
    assertThat(filter.getMemberHosts()).contains("arcadedb-1.arcadedb.myns.svc.cluster.local");

    assertThat(server.getCommittedPeersOrNull()).isNull();
    server.refreshPeerAllowlist();

    assertThat(filter.getMemberHosts()).as("no membership information is not the same as no members")
        .contains("arcadedb-1.arcadedb.myns.svc.cluster.local");
  }

  /** A committed configuration always carries this node; a peer list that reduces to nothing is not membership. */
  @Test
  void aMembershipThatCarriesNoUsableHostIsIgnored() {
    final ContextConfiguration config = kubernetesConfiguration();
    final RaftHAServer server = detachedServer(config);
    server.buildParameters(config);
    final PeerAddressAllowlistFilter filter = server.allowlistFilterForTest();

    server.reconcileAllowlistMembership(List.of(peer("arcadedb-1")));
    assertThat(filter.getMemberHosts()).contains("arcadedb-1.arcadedb.myns.svc.cluster.local");

    server.reconcileAllowlistMembership(List.of());
    assertThat(filter.getMemberHosts()).contains("arcadedb-1.arcadedb.myns.svc.cluster.local");
  }

  private static RaftPeer peer(final String podName) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(podName))
        .setAddress(podName + ".arcadedb.myns.svc.cluster.local:2434").build();
  }

  private static ContextConfiguration kubernetesConfiguration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "arcadedb-0:2434:2480");
    config.setValue(GlobalConfiguration.HA_K8S, true);
    config.setValue(GlobalConfiguration.HA_K8S_DNS_SUFFIX, ".arcadedb.myns.svc.cluster.local");
    return config;
  }

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private static RaftHAServer detachedServer(final ContextConfiguration config) {
    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("arcadedb-0");
    return new RaftHAServer(arcadeServer, config);
  }
}
