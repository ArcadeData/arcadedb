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

import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PeerAddressAllowlistFilter}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PeerAddressAllowlistFilterTest {

  @Test
  void allowsLoopbackEvenWhenNotInPeerList() throws Exception {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("192.168.255.254"), 5_000L);
    final Attributes attrs = attrsFor(InetAddress.getByName("127.0.0.1"));
    assertThat(filter.transportReady(attrs)).isSameAs(attrs);
  }

  @Test
  void allowsIpv6LoopbackEvenWhenNotInPeerList() throws Exception {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("192.168.255.254"), 5_000L);
    final Attributes attrs = attrsFor(InetAddress.getByName("::1"));
    assertThat(filter.transportReady(attrs)).isSameAs(attrs);
  }

  @Test
  void allowsResolvedPeerIp() throws Exception {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("127.0.0.1"), 5_000L);
    final Attributes attrs = attrsFor(InetAddress.getByName("127.0.0.1"));
    assertThat(filter.transportReady(attrs)).isSameAs(attrs);
  }

  @Test
  void rejectsUnknownIp() throws Exception {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("127.0.0.1"), 5_000L);
    final Attributes attrs = attrsFor(InetAddress.getByName("203.0.113.7")); // TEST-NET-3
    assertThatThrownBy(() -> filter.transportReady(attrs))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("203.0.113.7")
        .hasMessageContaining("not in the cluster peer allowlist");
  }

  @Test
  void exposesResolvedAllowlist() {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("127.0.0.1"), 5_000L);
    final Set<String> allowed = filter.getAllowedIps();
    assertThat(allowed).contains("127.0.0.1");
  }

  @Test
  void refreshPicksUpDnsChanges() {
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("127.0.0.1"), 0L);
    filter.refresh(); // re-resolve unconditionally
    assertThat(filter.getAllowedIps()).contains("127.0.0.1");
  }

  @Test
  void constructorRejectsEmptyPeerList() {
    assertThatThrownBy(() -> new PeerAddressAllowlistFilter(List.of(), 5_000L))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void extractPeerHostsHandlesSimpleEntries() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("localhost:2424,127.0.0.1:2425"))
        .containsExactly("localhost", "127.0.0.1");
  }

  @Test
  void extractPeerHostsHandlesThreePartEntries() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("host-a:2424:2480,host-b:2425:2481"))
        .containsExactly("host-a", "host-b");
  }

  @Test
  void extractPeerHostsHandlesPriorityEntries() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("host-a:2424:2480:10,host-b:2425:2481:5"))
        .containsExactly("host-a", "host-b");
  }

  @Test
  void extractPeerHostsStripsIpv6Brackets() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("[::1]:2424,[fe80::1]:2425"))
        .containsExactly("::1", "fe80::1");
  }

  @Test
  void extractPeerHostsIgnoresEmptyEntries() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts(""))
        .isEmpty();
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts(null))
        .isEmpty();
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts(" , "))
        .isEmpty();
  }

  private static Attributes attrsFor(final InetAddress address) {
    return Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(address, 55_555))
        .build();
  }
}
