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

  private static final String DEFAULT = IPAddressBlocklist.DEFAULT_RESERVED_RANGES;

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

  /**
   * The deprecated "IPv4-compatible IPv6 address" form (RFC 4291 2.5.5.1, {@code ::a.b.c.d}) has no {@code ffff}
   * marker, so it is a distinct case from the still-current IPv4-mapped form above. Flagged by code review on the
   * original GHSA-67m7-7w7g-mpmh fix as a residual gap in the same encoding family.
   */
  @Test
  void blocksDeprecatedIPv4CompatibleBypass() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);
    assertThat(bl.isBlocked(ip("::127.0.0.1"))).isTrue();
    assertThat(bl.isBlocked(ip("::169.254.169.254"))).isTrue();
    assertThat(bl.isBlocked(ip("::8.8.8.8"))).isFalse();
  }

  /**
   * Code review on the transition-encoding unwrap found that normalizing the probe address unconditionally, before
   * matching, lets the unwrap silently pre-empt a literal 16-byte IPv6 CIDR entry: {@code isBlocked} converts
   * {@code ::1} to the 4-byte form {@code 0.0.0.1} before the loop runs, and {@code Cidr.matches} rejects a 4-byte
   * probe against a 16-byte network on the length check alone, so a custom block-list of just {@code ::1/128}
   * (with no {@code 0.0.0.0/8} entry to catch the unwrapped fallback) never matches {@code ::1}. This only stayed
   * invisible for {@link #DEFAULT} because {@code 0.0.0.0/8} happens to be present there too. A custom, narrower
   * list - exactly what {@code OPENCYPHER_LOAD_CSV_BLOCKED_IP_RANGES} is designed to accept - has no such safety net.
   */
  @Test
  void literalIPv6CidrEntryIsNotShadowedByTheUnwrap() throws Exception {
    assertThat(IPAddressBlocklist.parse("::1/128").isBlocked(ip("::1"))).isTrue();
    assertThat(IPAddressBlocklist.parse("::/128").isBlocked(ip("::"))).isTrue();
    assertThat(IPAddressBlocklist.parse("64:ff9b::/96").isBlocked(ip("64:ff9b::c0a8:0101"))).isTrue();
    assertThat(IPAddressBlocklist.parse("2002::/16").isBlocked(ip("2002:c0a8:0101::1"))).isTrue();
    assertThat(IPAddressBlocklist.parse("2001::/32").isBlocked(ip("2001:0000:4136:e378:8000:63bf:3f57:fefe"))).isTrue();
    // A literal range for a different transition prefix must still not match an address outside it.
    assertThat(IPAddressBlocklist.parse("2002::/16").isBlocked(ip("64:ff9b::c0a8:0101"))).isFalse();
  }

  /**
   * GHSA-67m7-7w7g-mpmh: IPv6 transition mechanisms (NAT64, 6to4, Teredo) each embed an IPv4 address inside an
   * IPv6 literal through a different, non-{@code ::ffff:}-prefixed encoding. None of these tripped
   * {@code isIPv4Mapped}, so a private/loopback/link-local IPv4 payload smuggled through one of them reached the
   * SSRF guard unblocked. Addresses below reuse the exact examples from the advisory.
   */
  @Test
  void blocksIPv6TransitionAddressBypass() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);

    // NAT64 (RFC 6052), 64:ff9b::/96, IPv4 at bytes 12-15.
    assertThat(bl.isBlocked(ip("64:ff9b::c0a8:0101"))).isTrue();  // embeds 192.168.1.1
    assertThat(bl.isBlocked(ip("64:ff9b::a9fe:a9fe"))).isTrue();  // embeds 169.254.169.254 (cloud metadata)
    assertThat(bl.isBlocked(ip("64:ff9b::7f00:1"))).isTrue();     // embeds 127.0.0.1

    // 6to4 (RFC 3056), 2002::/16, IPv4 at bytes 2-5.
    assertThat(bl.isBlocked(ip("2002:c0a8:0101::1"))).isTrue();   // embeds 192.168.1.1
    assertThat(bl.isBlocked(ip("2002:a9fe:a9fe::1"))).isTrue();   // embeds 169.254.169.254

    // Teredo (RFC 4380), 2001::/32, IPv4 at bytes 12-15, bitwise-inverted.
    assertThat(bl.isBlocked(ip("2001:0000:4136:e378:8000:63bf:3f57:fefe"))).isTrue(); // embeds 192.168.1.1
  }

  @Test
  void allowsIPv6TransitionAddressesEmbeddingPublicIPv4() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.parse(DEFAULT);

    assertThat(bl.isBlocked(ip("64:ff9b::808:808"))).isFalse();    // NAT64 for 8.8.8.8
    assertThat(bl.isBlocked(ip("2002:0808:0808::1"))).isFalse();   // 6to4 for 8.8.8.8
    // Teredo for 8.8.8.8: embedded bytes are ~8,~8,~8,~8 = f7f7f7f7
    assertThat(bl.isBlocked(ip("2001:0000:0000:0000:0000:0000:f7f7:f7f7"))).isFalse();
  }

  @Test
  void defaultReservedRangesFactoryMatchesConstant() throws Exception {
    final IPAddressBlocklist bl = IPAddressBlocklist.defaultReservedRanges();
    assertThat(bl.isBlocked(ip("169.254.169.254"))).isTrue();
    assertThat(bl.isBlocked(ip("64:ff9b::a9fe:a9fe"))).isTrue();
    assertThat(bl.isBlocked(ip("8.8.8.8"))).isFalse();
  }

  /**
   * Review suggestion: memoize {@code defaultReservedRanges()} as a true singleton (the block-list is immutable)
   * instead of relying on every caller remembering to cache it, so a future caller can't accidentally reparse per
   * request without noticing.
   */
  @Test
  void defaultReservedRangesIsMemoizedAsASingleton() {
    assertThat(IPAddressBlocklist.defaultReservedRanges()).isSameAs(IPAddressBlocklist.defaultReservedRanges());
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
