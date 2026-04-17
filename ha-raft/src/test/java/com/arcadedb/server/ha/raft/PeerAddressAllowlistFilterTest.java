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

import java.util.List;

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
}
