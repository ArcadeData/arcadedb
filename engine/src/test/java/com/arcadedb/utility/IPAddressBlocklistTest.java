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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link IPAddressBlocklist} CIDR matching used by the LOAD CSV SSRF defense.
 */
class IPAddressBlocklistTest {

  private static final String DEFAULT =
      "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,0.0.0.0/8,100.64.0.0/10,192.0.0.0/24,198.18.0.0/15,"
          + "224.0.0.0/4,240.0.0.0/4,255.255.255.255/32,::1/128,::/128,fe80::/10,fc00::/7,ff00::/8";

  private static InetAddress ip(final String literal) throws UnknownHostException {
    return InetAddress.getByName(literal);
  }

  @Test
  void blocksLoopbackAndPrivateAndMetadata() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);
    assertThat(bl.isBlocked(ip("127.0.0.1"))).isTrue();
    assertThat(bl.isBlocked(ip("127.0.0.53"))).isTrue();
    assertThat(bl.isBlocked(ip("10.0.0.5"))).isTrue();
    assertThat(bl.isBlocked(ip("172.16.5.5"))).isTrue();
    assertThat(bl.isBlocked(ip("172.31.255.255"))).isTrue();
    assertThat(bl.isBlocked(ip("192.168.1.1"))).isTrue();
    assertThat(bl.isBlocked(ip("169.254.169.254"))).isTrue(); // AWS/GCP/Azure IMDS
    assertThat(bl.isBlocked(ip("100.64.1.1"))).isTrue();      // CGNAT
    assertThat(bl.isBlocked(ip("0.0.0.0"))).isTrue();
  }

  @Test
  void allowsPublicAddresses() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);
    assertThat(bl.isBlocked(ip("8.8.8.8"))).isFalse();
    assertThat(bl.isBlocked(ip("1.1.1.1"))).isFalse();
    assertThat(bl.isBlocked(ip("93.184.216.34"))).isFalse(); // example.com
    assertThat(bl.isBlocked(ip("172.32.0.1"))).isFalse();     // just outside 172.16/12
  }

  @Test
  void blocksIPv6LoopbackLinkLocalAndUla() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);
    assertThat(bl.isBlocked(ip("::1"))).isTrue();
    assertThat(bl.isBlocked(ip("fe80::1"))).isTrue();
    assertThat(bl.isBlocked(ip("fd00:ec2::254"))).isTrue(); // IPv6 IMDS (ULA)
    assertThat(bl.isBlocked(ip("2001:4860:4860::8888"))).isFalse(); // public IPv6 DNS
  }

  @Test
  void blocksIPv4MappedIPv6Bypass() throws Exception {
    // ::ffff:127.0.0.1 must be treated as 127.0.0.1 and therefore blocked.
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);
    assertThat(bl.isBlocked(ip("::ffff:127.0.0.1"))).isTrue();
    assertThat(bl.isBlocked(ip("::ffff:169.254.169.254"))).isTrue();
    assertThat(bl.isBlocked(ip("::ffff:8.8.8.8"))).isFalse();
  }

  @Test
  void emptyBlocklistNeverBlocks() throws Exception {
    assertThat(IPAddressBlocklist.parse("").isEmpty()).isTrue();
    assertThat(IPAddressBlocklist.parse(null).isEmpty()).isTrue();
    assertThat(IPAddressBlocklist.parse("  ").isBlocked(ip("127.0.0.1"))).isFalse();
  }

  @Test
  void bareAddressIsSingleHost() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse("203.0.113.5");
    assertThat(bl.isBlocked(ip("203.0.113.5"))).isTrue();
    assertThat(bl.isBlocked(ip("203.0.113.6"))).isFalse();
  }

  @Test
  void ignoresBlankEntriesAndWhitespace() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(" 127.0.0.0/8 , , 10.0.0.0/8 ");
    assertThat(bl.isBlocked(ip("127.0.0.1"))).isTrue();
    assertThat(bl.isBlocked(ip("10.1.2.3"))).isTrue();
  }

  @Test
  void rejectsInvalidEntries() {
    assertThatThrownBy(() -> IPAddressBlocklist.parse("127.0.0.0/99")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IPAddressBlocklist.parse("not-an-ip/8")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IPAddressBlocklist.parse("10.0.0.0/abc")).isInstanceOf(IllegalArgumentException.class);
  }
}
