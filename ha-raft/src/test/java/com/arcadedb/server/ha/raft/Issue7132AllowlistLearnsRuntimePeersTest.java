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
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7132: {@link PeerAddressAllowlistFilter} was built once from the STATIC
 * {@code arcadedb.ha.serverList} and its host list was immutable for the life of the filter, so it never
 * learned a peer that joined at runtime. Since the allowlist defaults to enabled and stops failing open as
 * soon as a quorum of the static peers resolves, the Kubernetes scale-up auto-join of #4836 could not
 * complete: the new pod's inbound Raft gRPC connections were rejected by every existing pod, so it never
 * joined and stayed NOT_READY - and the rejection was logged on the RECEIVING pods, not on the new one.
 * <p>
 * Two things were wrong and both are covered here: the frozen host list, and the fact that on Kubernetes the
 * configured hosts were resolved WITHOUT the DNS suffix {@code parsePeerList} applies to the peer addresses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7132AllowlistLearnsRuntimePeersTest {

  @Test
  void aPeerThatJoinedAfterStartupIsAdmittedOnceItsHostIsLearned() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("arcadedb-0", List.of("10.1.13.1"));
    dns.table.put("arcadedb-1", List.of("10.1.13.2"));
    dns.table.put("arcadedb-2", List.of("10.1.13.3"));

    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(
        List.of("arcadedb-0", "arcadedb-1", "arcadedb-2"), 30_000L, 60_000L, 300_000L, clock::get, dns);

    // The static peers all resolve, so the fail-open grace has already ended: this is the steady state a
    // healthy cluster reaches about a second after startup.
    assertThat(f.isQuorumResolved()).isTrue();
    assertThat(f.isEverCompletelyResolved()).isTrue();

    // The StatefulSet is scaled to 4. Pod 3 dials this node to join; its host is in nobody's server list.
    dns.table.put("arcadedb-3", List.of("10.1.13.4"));
    clock.set(120_000); // well past the grace window
    assertThat(f.isAllowed("10.1.13.4")).as("the frozen allowlist could never admit an unknown host").isFalse();

    // The health monitor tick feeds the live Raft configuration in; the joiner is admitted from here on.
    assertThat(f.learnPeerHosts(List.of("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"))).isTrue();
    assertThat(f.getLearnedHosts()).containsExactly("arcadedb-3");
    assertThat(f.isAllowed("10.1.13.4")).isTrue();
  }

  @Test
  void learningIsIdempotentAndNeverRelearnsAConfiguredHost() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, dns);

    assertThat(f.learnPeerHosts(List.of("peerA"))).as("a configured host is not a new host").isFalse();
    assertThat(f.learnPeerHosts(List.of())).isFalse();
    assertThat(f.learnPeerHosts(null)).isFalse();

    dns.table.put("peerB", List.of("10.0.0.2"));
    assertThat(f.learnPeerHosts(List.of("peerB"))).isTrue();
    assertThat(f.learnPeerHosts(List.of("peerB"))).as("a second tick with the same membership is a no-op").isFalse();
    assertThat(f.isAllowed("10.0.0.2")).isTrue();
  }

  /**
   * The quorum and completeness latches describe the CONFIGURED cluster (issue #4828). A learned host that
   * does not resolve - a scale-up pod that has not been created yet - must not hold the fail-open window
   * open, and one that does resolve must not widen the quorum a returning peer has to clear.
   */
  @Test
  void learnedHostsDoNotCountTowardsTheQuorumAndCompletenessLatches() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 60_000L,
        300_000L, clock::get, dns);
    assertThat(f.isEverCompletelyResolved()).isTrue();

    // A host that does not resolve at all: no warning, no effect on the latches, no effect on the allowlist.
    assertThat(f.learnPeerHosts(List.of("does-not-exist"))).isTrue();
    assertThat(f.isEverCompletelyResolved()).isTrue();
    assertThat(f.isQuorumResolved()).isTrue();
    assertThat(f.getAllowedIps()).contains("10.0.0.1");
  }

  /** A freshly started node whose configured peers do not resolve yet must still not latch on a learned host. */
  @Test
  void aLearnedHostDoesNotTripTheQuorumLatchOfAnIncompleteAllowlist() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("learned", List.of("10.0.9.9"));
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA", "peerB", "peerC"),
        30_000L, 60_000L, 300_000L, clock::get, dns);
    assertThat(f.isQuorumResolved()).isFalse();

    f.learnPeerHosts(List.of("learned"));

    assertThat(f.isQuorumResolved()).as("a learned host is not a configured peer").isFalse();
    assertThat(f.isEverCompletelyResolved()).isFalse();
    assertThat(f.getAllowedIps()).contains("10.0.9.9");
  }

  // ---------------------------------------------------------------------------
  // Wiring in RaftHAServer
  // ---------------------------------------------------------------------------

  @Test
  void theAllowlistResolvesKubernetesPeerHostsWithTheConfiguredDnsSuffix() {
    final ContextConfiguration config = kubernetesConfiguration();
    final RaftHAServer server = detachedServer(config);

    server.buildParameters(config);

    final PeerAddressAllowlistFilter filter = server.allowlistFilterForTest();
    assertThat(filter).isNotNull();
    assertThat(filter.toString())
        .as("the bare pod name from the server list only resolves when the namespace happens to match the "
            + "headless service name; the suffix is what makes it resolve everywhere")
        .contains("arcadedb-0.arcadedb.myns.svc.cluster.local");
  }

  /**
   * The headless service domain is the StatefulSet's own membership record: it resolves to every pod backing
   * it, at any replica count, which is what lets a scale-up pod's very first inbound connection through.
   */
  @Test
  void theAllowlistSeedsTheHeadlessServiceDomainOnKubernetes() {
    final ContextConfiguration config = kubernetesConfiguration();
    final RaftHAServer server = detachedServer(config);

    server.buildParameters(config);

    assertThat(server.allowlistFilterForTest().getLearnedHosts())
        .containsExactly("arcadedb.myns.svc.cluster.local");
  }

  @Test
  void nothingIsSeededWhenNotRunningUnderKubernetes() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");
    final RaftHAServer server = detachedServer(config);

    server.buildParameters(config);

    assertThat(server.allowlistFilterForTest().getLearnedHosts()).isEmpty();
  }

  @Test
  void headlessServiceDomainStripsTheLeadingDotAndRejectsBlanks() {
    assertThat(RaftHAServer.headlessServiceDomain(".arcadedb.myns.svc.cluster.local"))
        .isEqualTo("arcadedb.myns.svc.cluster.local");
    assertThat(RaftHAServer.headlessServiceDomain("arcadedb.myns.svc.cluster.local"))
        .isEqualTo("arcadedb.myns.svc.cluster.local");
    assertThat(RaftHAServer.headlessServiceDomain("")).isNull();
    assertThat(RaftHAServer.headlessServiceDomain("   ")).isNull();
    assertThat(RaftHAServer.headlessServiceDomain(".")).isNull();
    assertThat(RaftHAServer.headlessServiceDomain(null)).isNull();
  }

  /** The resolver takes a host, not an address, and rejects the brackets of an IPv6 literal. */
  @Test
  void memberAddressesAreReducedToResolvableHosts() {
    assertThat(RaftHAServer.allowlistHostOf("arcadedb-3.arcadedb.myns.svc.cluster.local:2434"))
        .isEqualTo("arcadedb-3.arcadedb.myns.svc.cluster.local");
    assertThat(RaftHAServer.allowlistHostOf("10.1.13.4:2434")).isEqualTo("10.1.13.4");
    assertThat(RaftHAServer.allowlistHostOf("[fd00::4]:2434")).isEqualTo("fd00::4");
    assertThat(RaftHAServer.allowlistHostOf("")).isNull();
    assertThat(RaftHAServer.allowlistHostOf(null)).isNull();
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
