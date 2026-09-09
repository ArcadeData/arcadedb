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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7302, item 1: the inverse of the exemption #7225 was asked to keep.
 * <p>
 * #7225 made a peer removed from the Raft configuration lose its inbound Raft gRPC access, and exempted the
 * pinned Kubernetes headless-service domain so that #7132's scale-up - a pod admitted BEFORE it is a member -
 * kept working. On Kubernetes those two meet: the headless service publishes the A record of every pod backing
 * the StatefulSet, a not-Ready one included, and Kubernetes drops a pod's address when the POD terminates rather
 * than when Raft removes it from the configuration. Expanding the pinned domain into the same set as the
 * membership hosts therefore re-added the removed pod's address on every resolve, while the INFO line written to
 * confirm the revocation reported one that had not happened.
 * <p>
 * What must not regress in fixing it is the reason the exemption exists: a pod that is not a member yet - a
 * scale-up pod, or one that inherited a decommissioned pod's address after it was released - is still admitted
 * by the pinned domain. That is the second half of every test below.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7302AllowlistRevokesPinnedPodAddressTest {

  private static final String SERVICE = "arcadedb.myns.svc.cluster.local";
  private static final String POD_0   = "arcadedb-0." + SERVICE;
  private static final String POD_1   = "arcadedb-1." + SERVICE;
  private static final String IP_0    = "10.1.13.1";
  private static final String IP_1    = "10.1.13.2";

  /**
   * {@code DELETE /api/v1/cluster/peer/arcadedb-1} against a pod that keeps running. The headless service still
   * publishes its address, and it must stop being admitted anyway.
   */
  @Test
  void aRemovedButStillRunningPodLosesItsRaftAccessEvenThoughThePinnedDomainStillPublishesIt() {
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put(POD_0, List.of(IP_0));
    dns.table.put(POD_1, List.of(IP_1));
    dns.table.put(SERVICE, List.of(IP_0, IP_1));

    final PeerAddressAllowlistFilter f = kubernetesFilter(dns);
    f.setMemberHosts(List.of(POD_0, POD_1));
    assertThat(f.isAllowed(IP_1)).as("a member is admitted").isTrue();

    f.setMemberHosts(List.of(POD_0));

    assertThat(f.getAllowedIps())
        .as("the pinned headless service still resolves to the pod, and that is exactly what must not readmit it")
        .doesNotContain(IP_1);
    assertThat(f.isAllowed(IP_1)).isFalse();
    assertThat(f.isAllowed(IP_0)).as("the surviving member is untouched").isTrue();
  }

  /** The revocation survives the periodic re-resolution, which is where the address was being re-added. */
  @Test
  void theRevocationSurvivesEveryLaterResolve() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put(POD_0, List.of(IP_0));
    dns.table.put(POD_1, List.of(IP_1));
    dns.table.put(SERVICE, List.of(IP_0, IP_1));

    final PeerAddressAllowlistFilter f = kubernetesFilter(dns, clock);
    f.setMemberHosts(List.of(POD_0, POD_1));
    f.setMemberHosts(List.of(POD_0));

    for (int tick = 0; tick < 5; tick++) {
      clock.addAndGet(60_000L);
      f.proactiveRefresh();
      assertThat(f.getAllowedIps()).as("resolve %d re-added it before #7302", tick).doesNotContain(IP_1);
    }
  }

  /**
   * The revocation is not permanent, and its end is a fact rather than a timer: once the pod terminates its
   * address stops being published, and whatever gets that address next is a different pod - a scale-up one,
   * which the pinned domain exists to admit before it is a member.
   */
  @Test
  void anAddressThePinnedDomainStopsPublishingIsNoLongerRevoked() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put(POD_0, List.of(IP_0));
    dns.table.put(POD_1, List.of(IP_1));
    dns.table.put(SERVICE, List.of(IP_0, IP_1));

    final PeerAddressAllowlistFilter f = kubernetesFilter(dns, clock);
    f.setMemberHosts(List.of(POD_0, POD_1));
    f.setMemberHosts(List.of(POD_0));
    assertThat(f.isAllowed(IP_1)).isFalse();

    // The pod terminates: Kubernetes drops its address from the service.
    dns.table.remove(POD_1);
    dns.table.put(SERVICE, List.of(IP_0));
    clock.addAndGet(60_000L);
    f.proactiveRefresh();
    assertThat(f.getRevokedPinnedIps())
        .as("nothing publishes the address any more, so there is nothing left to hold back")
        .isEmpty();

    // A new pod is scheduled and inherits the released address. It is not a member yet - that is the whole
    // point of pinning the service domain - and it must be admitted so it can join.
    dns.table.put(SERVICE, List.of(IP_0, IP_1));
    clock.addAndGet(60_000L);
    f.proactiveRefresh();

    assertThat(f.isAllowed(IP_1))
        .as("a scale-up pod behind the pinned domain is still admitted before it is a member (#7132)")
        .isTrue();
  }

  /** A peer removed and put back is admitted again as soon as the membership says so. */
  @Test
  void aReAddedPeerIsAdmittedAgain() {
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put(POD_0, List.of(IP_0));
    dns.table.put(POD_1, List.of(IP_1));
    dns.table.put(SERVICE, List.of(IP_0, IP_1));

    final PeerAddressAllowlistFilter f = kubernetesFilter(dns);
    f.setMemberHosts(List.of(POD_0, POD_1));
    f.setMemberHosts(List.of(POD_0));
    assertThat(f.isAllowed(IP_1)).isFalse();

    f.setMemberHosts(List.of(POD_0, POD_1));
    assertThat(f.isAllowed(IP_1)).as("the membership is the authority, and it says yes again").isTrue();
  }

  /**
   * A revoked address that a CONFIGURED host resolves to stays admitted: {@code arcadedb.ha.serverList} is
   * configuration, and no Raft configuration commit revokes a configuration entry - the same rule
   * {@code setMemberHosts} already applies on the way in.
   */
  @Test
  void aDeclaredHostsAddressIsNeverRevoked() {
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put(POD_0, List.of(IP_0));
    dns.table.put(SERVICE, List.of(IP_0, IP_1));
    // A second declared host that happens to resolve to the address a departing member also used.
    dns.table.put("declared-twin", List.of(IP_1));
    dns.table.put(POD_1, List.of(IP_1));

    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of(POD_0, "declared-twin"), 30_000L,
        0L, 300_000L, clock::get, dns);
    f.learnPeerHosts(List.of(SERVICE));

    f.setMemberHosts(List.of(POD_1));
    f.setMemberHosts(List.of());

    assertThat(f.isAllowed(IP_1)).as("configuration outranks membership, as it does everywhere else here").isTrue();
  }

  /** Off Kubernetes nothing is pinned, so the whole mechanism is inert and the #7225 behaviour is unchanged. */
  @Test
  void withNoPinnedHostTheRevocationSetStaysEmpty() {
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("peerB", List.of("10.0.0.2"));

    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, dns);

    f.setMemberHosts(List.of("peerB"));
    assertThat(f.isAllowed("10.0.0.2")).isTrue();
    f.setMemberHosts(List.of());
    assertThat(f.isAllowed("10.0.0.2")).isFalse();
    assertThat(f.getRevokedPinnedIps()).as("nothing pins the address, so nothing has to hold it back").isEmpty();
  }

  private static PeerAddressAllowlistFilter kubernetesFilter(final PeerAddressAllowlistFilterTest.FakeResolver dns) {
    return kubernetesFilter(dns, new AtomicLong(0));
  }

  private static PeerAddressAllowlistFilter kubernetesFilter(final PeerAddressAllowlistFilterTest.FakeResolver dns,
      final AtomicLong clock) {
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of(POD_0), 30_000L, 0L, 300_000L,
        clock::get, dns);
    // What RaftHAServer.installPeerAllowlist seeds on Kubernetes.
    f.learnPeerHosts(List.of(SERVICE));
    return f;
  }
}
