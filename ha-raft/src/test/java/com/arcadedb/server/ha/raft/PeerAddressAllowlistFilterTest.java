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

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PeerAddressAllowlistFilter} focusing on host extraction parsing
 * and loopback detection that does not require network I/O.
 */
class PeerAddressAllowlistFilterTest {

  // ---------------------------------------------------------------------------
  // extractPeerHosts tests
  // ---------------------------------------------------------------------------

  @Test
  void extractPeerHostsReturnsEmptyForNull() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts(null)).isEmpty();
  }

  @Test
  void extractPeerHostsReturnsEmptyForBlank() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("   ")).isEmpty();
  }

  @Test
  void extractPeerHostsReturnsEmptyForEmptyString() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("")).isEmpty();
  }

  @Test
  void extractPeerHostsSingleHostWithRaftPort() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("node1:2434");
    assertThat(hosts).containsExactly("node1");
  }

  @Test
  void extractPeerHostsSingleHostWithRaftAndHttpPort() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("node1:2434:2480");
    assertThat(hosts).containsExactly("node1");
  }

  @Test
  void extractPeerHostsSingleHostWithAllParts() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("node1:2434:2480:10");
    assertThat(hosts).containsExactly("node1");
  }

  @Test
  void extractPeerHostsMultipleIPv4Entries() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("10.0.0.1:2434,10.0.0.2:2434,10.0.0.3:2434");
    assertThat(hosts).containsExactly("10.0.0.1", "10.0.0.2", "10.0.0.3");
  }

  @Test
  void extractPeerHostsIPv6Bracketed() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("[::1]:2434");
    assertThat(hosts).containsExactly("::1");
  }

  @Test
  void extractPeerHostsIPv6BracketedWithHttpPort() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("[fe80::1]:2434:2480");
    assertThat(hosts).containsExactly("fe80::1");
  }

  @Test
  void extractPeerHostsHostnameOnly() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("arcadedb-0");
    assertThat(hosts).containsExactly("arcadedb-0");
  }

  @Test
  void extractPeerHostsMixedFormats() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("arcadedb-0:2434,arcadedb-1:2434:2480,[::1]:2434");
    assertThat(hosts).containsExactly("arcadedb-0", "arcadedb-1", "::1");
  }

  @Test
  void extractPeerHostsStripsNamePrefixIPv4() {
    // Regression: the optional "name@" prefix must not be treated as part of the host, otherwise
    // the allowlist tries to DNS-resolve "arcadesplit1@10.70.2.53" and rejects all peers.
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts(
        "arcadesplit1@10.70.2.53:2434:2480,arcadesplit2@10.70.2.70:2434:2480,arcadesplit3@10.70.2.40:2434:2480");
    assertThat(hosts).containsExactly("10.70.2.53", "10.70.2.70", "10.70.2.40");
  }

  @Test
  void extractPeerHostsStripsNamePrefixHostname() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("frankfurt@node1:2434:2480");
    assertThat(hosts).containsExactly("node1");
  }

  @Test
  void extractPeerHostsStripsNamePrefixIPv6Bracketed() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("london@[fe80::1]:2434:2480");
    assertThat(hosts).containsExactly("fe80::1");
  }

  @Test
  void extractPeerHostsSkipsBlankEntries() {
    // Trailing comma creates an empty entry
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("node1:2434, ,node2:2434");
    assertThat(hosts).containsExactly("node1", "node2");
  }

  @Test
  void extractPeerHostsLocalhostAddress() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("127.0.0.1:2434");
    assertThat(hosts).containsExactly("127.0.0.1");
  }

  @Test
  void extractPeerHostsObjectFormSingleEntry() {
    // Issue #4470: commas inside the {raft:..,http:..,https:..} block must not split the entry,
    // otherwise 'http' and 'https' get extracted as bogus hosts.
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("node1:{raft:2434,http:2480,https:2490}");
    assertThat(hosts).containsExactly("node1");
  }

  @Test
  void extractPeerHostsObjectFormMultipleEntries() {
    // Reproduces the exact serverList from issue #4470 comment that produced the bogus
    // 'http'/'https' allowlist hosts.
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts(
        "wxg-arcadedb-0.svc.local:{raft:2434,http:2480,https:2490},"
            + "wxg-arcadedb-1.svc.local:{raft:2434,http:2480,https:2490},"
            + "wxg-arcadedb-2.svc.local:{raft:2434,http:2480,https:2490}");
    assertThat(hosts).containsExactly(
        "wxg-arcadedb-0.svc.local", "wxg-arcadedb-1.svc.local", "wxg-arcadedb-2.svc.local");
    assertThat(hosts).doesNotContain("http", "https");
  }

  @Test
  void extractPeerHostsObjectFormWithNamePrefix() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts(
        "frankfurt@node1:{raft:2434,http:2480,https:2490,priority:10}");
    assertThat(hosts).containsExactly("node1");
  }

  @Test
  void extractPeerHostsMixesObjectAndPositionalForms() {
    final List<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts(
        "node1:{raft:2434,http:2480},node2:2434:2480,[::1]:2434");
    assertThat(hosts).containsExactly("node1", "node2", "::1");
  }

  // ---------------------------------------------------------------------------
  // Constructor / validation tests
  // ---------------------------------------------------------------------------

  @Test
  void constructorRejectsNullPeerHosts() {
    assertThatThrownBy(() -> new PeerAddressAllowlistFilter(null, 30_000L))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructorRejectsEmptyPeerHosts() {
    assertThatThrownBy(() -> new PeerAddressAllowlistFilter(List.of(), 30_000L))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructorNegativeRefreshIntervalClampsToZero() {
    // Should not throw; negative refresh becomes 0 (always re-resolve on miss)
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("localhost"), -1L);
    assertThat(filter).isNotNull();
  }

  // ---------------------------------------------------------------------------
  // getAllowedIps includes loopbacks after construction
  // ---------------------------------------------------------------------------

  @Test
  void allowedIpsAlwaysContainsLoopbackAfterConstruction() {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("localhost"), 30_000L);
    final var ips = filter.getAllowedIps();
    // localhost resolves to 127.0.0.1 on any standard environment;
    // either the loopback set or the resolved localhost address must be present.
    assertThat(ips).isNotEmpty();
  }

  @Test
  void toStringContainsPeerHosts() {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("node1", "node2"), 30_000L);
    assertThat(filter.toString()).contains("node1", "node2");
  }

  // ---------------------------------------------------------------------------
  // Startup DNS-race hardening (issue #4471) - deterministic via injected clock/resolver
  // ---------------------------------------------------------------------------

  /** In-memory DNS: hosts map to a list of literal IPs; an absent/empty host throws like real DNS. */
  static final class FakeResolver implements PeerAddressAllowlistFilter.HostResolver {
    final Map<String, List<String>> table = new HashMap<>();

    @Override
    public InetAddress[] resolve(final String host) throws UnknownHostException {
      final List<String> ips = table.get(host);
      if (ips == null || ips.isEmpty())
        throw new UnknownHostException(host + ": Name does not resolve");
      final InetAddress[] out = new InetAddress[ips.size()];
      for (int i = 0; i < ips.size(); i++)
        out[i] = InetAddress.getByName(ips.get(i)); // literal IP: no network lookup
      return out;
    }
  }

  private static PeerAddressAllowlistFilter filter(final List<String> hosts, final long refreshMs, final long graceMs,
      final long stickyMs, final AtomicLong clock, final FakeResolver resolver) {
    return new PeerAddressAllowlistFilter(hosts, refreshMs, graceMs, stickyMs, clock::get, resolver);
  }

  @Test
  void failsOpenWhilePeersUnresolvableDuringStartupGrace() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver(); // nothing resolves yet (pods not Ready)
    final PeerAddressAllowlistFilter f = filter(List.of("peerA", "peerB"), 30_000L, 60_000L, 300_000L, clock, dns);

    assertThat(f.isEverCompletelyResolved()).isFalse();
    // A legitimate peer connects before its own DNS record is published: accepted during the grace window.
    assertThat(f.isAllowed("10.1.13.9")).isTrue();
  }

  @Test
  void enforcesAfterStartupGraceExpiresIfStillIncomplete() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver(); // never resolves
    final PeerAddressAllowlistFilter f = filter(List.of("peerA", "peerB"), 30_000L, 60_000L, 300_000L, clock, dns);

    clock.set(60_001); // grace window elapsed
    assertThat(f.isAllowed("10.1.13.9")).isFalse();
  }

  @Test
  void failOpenDisabledWhenGraceZero() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);

    assertThat(f.isAllowed("10.1.13.9")).isFalse(); // strict from the first connection
  }

  @Test
  void missBypassesRefreshRateLimitWhileAllowlistIncomplete() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver(); // peerA unresolvable at construction
    // grace=0 so the only way to be accepted is a successful (bypassing) re-resolution.
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.isAllowed("10.1.13.9")).isFalse();

    // peerA's DNS record is now published; only 1.1s later (well under the 30s refresh interval).
    dns.table.put("peerA", List.of("10.1.13.9"));
    clock.set(1_100);

    // Because the allowlist is still incomplete, the miss re-resolves on the short floor and converges.
    assertThat(f.isAllowed("10.1.13.9")).isTrue();
    assertThat(f.isEverCompletelyResolved()).isTrue();
  }

  @Test
  void steadyStateRejectsUnknownAndStopsFailingOpenOnceComplete() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    dns.table.put("peerB", List.of("10.1.13.9"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA", "peerB"), 30_000L, 60_000L, 300_000L, clock, dns);

    assertThat(f.isEverCompletelyResolved()).isTrue();
    assertThat(f.isAllowed("10.1.13.8")).isTrue();
    assertThat(f.isAllowed("10.1.13.9")).isTrue();
    // A stranger is rejected even though we are still within the wall-clock grace window, because the
    // allowlist has already been complete once (fail-open only covers the never-yet-complete startup).
    assertThat(f.isAllowed("10.99.99.99")).isFalse();
  }

  @Test
  void stickyRetainsLastKnownIpOnTransientDnsFailureThenExpires() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.getAllowedIps()).contains("10.1.13.8");

    // Transient DNS outage for peerA.
    dns.table.remove("peerA");
    clock.set(100_000); // within sticky TTL (300s)
    f.refresh();
    assertThat(f.getAllowedIps()).contains("10.1.13.8"); // retained

    clock.set(500_000); // past sticky TTL since last good resolution (t=0)
    f.refresh();
    assertThat(f.getAllowedIps()).doesNotContain("10.1.13.8"); // evicted
  }

  @Test
  void stickinessDisabledDropsHostImmediatelyOnFailure() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 0L, clock, dns);
    assertThat(f.getAllowedIps()).contains("10.1.13.8");

    dns.table.remove("peerA");
    clock.set(1_000);
    f.refresh();
    assertThat(f.getAllowedIps()).doesNotContain("10.1.13.8");
  }

  @Test
  void picksUpRestartedPeerNewIpAfterRefreshInterval() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.isAllowed("10.1.13.8")).isTrue();

    // peerA pod restarts with a new IP; DNS now points to it.
    dns.table.put("peerA", List.of("10.1.13.50"));
    clock.set(31_000); // past the steady-state refresh interval
    assertThat(f.isAllowed("10.1.13.50")).isTrue();
  }
}
