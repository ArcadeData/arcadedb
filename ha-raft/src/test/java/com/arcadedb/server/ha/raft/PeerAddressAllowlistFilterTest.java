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
  void stopsFailingOpenOnceAQuorumOfPeersResolveEvenIfOnePeerStaysDown() {
    // Issue #4828: with one peer permanently down at startup, the all-peers-resolved latch never
    // trips, so the fail-open branch used to stay active for the ENTIRE configured grace window,
    // admitting any namespace IP. Once a quorum of declared peers resolve, the cluster can already
    // form a Raft majority, so fail-open must end early instead of waiting for the down peer.
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    // Three-node cluster: self (peerA) + peerB are up; peerC's pod is permanently down so its DNS
    // record never publishes. 2/3 resolved = a Raft quorum.
    dns.table.put("peerA", List.of("10.1.13.7"));
    dns.table.put("peerB", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA", "peerB", "peerC"), 30_000L, 60_000L, 300_000L, clock, dns);

    // Genuine peers are admitted.
    assertThat(f.isAllowed("10.1.13.7")).isTrue();
    assertThat(f.isAllowed("10.1.13.8")).isTrue();
    // A quorum (2/3) has resolved, so a stranger is rejected immediately - well inside the grace
    // window - rather than being admitted for the full window because peerC never resolves.
    assertThat(f.isQuorumResolved()).isTrue();
    assertThat(f.isAllowed("10.99.99.99")).isFalse();
  }

  @Test
  void keepsFailingOpenWhileBelowQuorumDuringStartupGrace() {
    // Only self has resolved (1/3) - no quorum yet - so a legitimately-restarting peer whose DNS is
    // not published must still be admitted during the grace window (issue #4471 self-partition guard).
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.7"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA", "peerB", "peerC"), 30_000L, 60_000L, 300_000L, clock, dns);

    assertThat(f.isQuorumResolved()).isFalse();
    assertThat(f.isAllowed("10.1.13.9")).isTrue(); // still failing open below quorum
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

  // ---------------------------------------------------------------------------
  // Proactive (background) refresh - issue #4696
  // The allowlist must reconcile a returned peer's new pod IP on its own cadence, NOT only after a
  // rejected ("miss") inbound connection. On a leader whose outbound appender channel is also wedged,
  // the returned peer's inbound connection may never arrive, so a reactive-only refresh strands it.
  // ---------------------------------------------------------------------------

  @Test
  void proactiveRefreshAdmitsReturnedPeerNewIpWithoutAnyInboundMiss() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.getAllowedIps()).contains("10.1.13.8");

    // peerA pod is rescheduled and comes back with a new IP. No connection from it has been attempted
    // yet (the leader's outbound appender is wedged), so the miss path never fires.
    dns.table.put("peerA", List.of("10.1.13.50"));
    clock.set(31_000); // past the steady-state refresh interval

    f.proactiveRefresh();

    // The background refresh reconciled the new IP and dropped the stale pre-restart one - all without
    // a single rejected inbound connection.
    assertThat(f.isAllowed("10.1.13.50")).isTrue();
    assertThat(f.getAllowedIps()).doesNotContain("10.1.13.8");
  }

  @Test
  void proactiveRefreshRespectsRefreshIntervalFloorToAvoidHammeringDns() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.isEverCompletelyResolved()).isTrue();

    dns.table.put("peerA", List.of("10.1.13.50"));

    // A tick inside the refresh interval (health monitor fires far more often than the interval) must
    // NOT re-resolve - the old IP is still the only one allowed.
    clock.set(10_000);
    f.proactiveRefresh();
    assertThat(f.getAllowedIps()).contains("10.1.13.8");
    assertThat(f.getAllowedIps()).doesNotContain("10.1.13.50");

    // Once the interval elapses, the next proactive tick reconciles.
    clock.set(31_000);
    f.proactiveRefresh();
    assertThat(f.getAllowedIps()).contains("10.1.13.50");
  }

  @Test
  void proactiveRefreshDropsStaleStickyIpOnceNameResolvesToNewIp() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.1.13.8"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.getAllowedIps()).contains("10.1.13.8");

    // Mid-restart: peerA's name briefly fails to resolve. Sticky retention keeps the old IP so a purely
    // transient DNS blip does not evict a live peer.
    dns.table.remove("peerA");
    clock.set(31_000);
    f.proactiveRefresh();
    assertThat(f.getAllowedIps()).contains("10.1.13.8"); // retained (sticky)

    // DNS now publishes the new pod IP. The next proactive refresh must admit it AND drop the stale
    // sticky IP immediately - the sticky entry must not keep masking the live address.
    dns.table.put("peerA", List.of("10.1.13.50"));
    clock.set(62_000);
    f.proactiveRefresh();
    assertThat(f.getAllowedIps()).contains("10.1.13.50");
    assertThat(f.getAllowedIps()).doesNotContain("10.1.13.8");
  }

  @Test
  void proactiveRefreshConvergesFastWhileAllowlistIncompleteAtStartup() {
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver(); // peerA not yet resolvable at construction
    final PeerAddressAllowlistFilter f = filter(List.of("peerA"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.isEverCompletelyResolved()).isFalse();

    // peerA's DNS record is published shortly after startup, well under the 30s steady-state interval.
    dns.table.put("peerA", List.of("10.1.13.9"));
    clock.set(1_100); // past the short incomplete-allowlist floor (1s), under the full interval

    // Because the allowlist has never been complete, the proactive refresh uses the short startup floor
    // and converges without waiting the full interval.
    f.proactiveRefresh();
    assertThat(f.isEverCompletelyResolved()).isTrue();
    assertThat(f.getAllowedIps()).contains("10.1.13.9");
  }

  @Test
  void missReresolvesImmediatelyAfterPodRecreation() {
    // Issue #5268: a StatefulSet pod is recreated with a new IP. The allowlist is fully resolved
    // (holding the OLD incarnation's IP), and the DNS-gap resolutions during the pod recreation keep
    // bumping the last-resolve timestamp - so the new incarnation's one-shot join probe used to land
    // inside refreshIntervalMs and be rejected WITHOUT re-resolving, losing the race essentially
    // every time (3/3 measured in the reporter's environment). An unknown IP must trigger a
    // re-resolve on the short miss floor instead of waiting out the steady-state interval.
    final AtomicLong clock = new AtomicLong(0);
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("peerB", List.of("10.0.0.2"));
    final PeerAddressAllowlistFilter f = filter(List.of("peerA", "peerB"), 30_000L, 0L, 300_000L, clock, dns);
    assertThat(f.isEverCompletelyResolved()).isTrue();
    assertThat(f.isAllowed("10.0.0.2")).isTrue();

    // peerB's pod is recreated: DNS now serves the new IP.
    dns.table.put("peerB", List.of("10.0.0.99"));
    clock.set(5_000); // past the 1s miss floor, well inside the 30s refresh interval

    assertThat(f.isAllowed("10.0.0.99"))
        .as("a connection from an unknown IP must re-resolve peer DNS before rejecting")
        .isTrue();
  }

  @Test
  void missReresolveIsRateLimitedByTheShortFloor() {
    // The miss-path re-resolve must still be rate-limited (the short floor) so a connection flood
    // from a non-peer cannot hammer DNS: within the floor a genuinely-unknown IP is rejected against
    // the cached allowlist without another resolution.
    final AtomicLong clock = new AtomicLong(0);
    final int[] resolutions = { 0 };
    final FakeResolver dns = new FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter.HostResolver counting = host -> {
      resolutions[0]++;
      return dns.resolve(host);
    };
    final PeerAddressAllowlistFilter f = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, counting);
    final int afterConstruction = resolutions[0];

    clock.set(2_000);
    assertThat(f.isAllowed("10.9.9.9")).isFalse(); // miss past the floor: one re-resolve
    final int afterFirstMiss = resolutions[0];
    assertThat(afterFirstMiss).isGreaterThan(afterConstruction);

    clock.set(2_500); // within the 1s floor of the previous re-resolve
    assertThat(f.isAllowed("10.9.9.8")).isFalse();
    assertThat(resolutions[0]).as("misses inside the floor must not re-resolve again").isEqualTo(afterFirstMiss);
  }
}
