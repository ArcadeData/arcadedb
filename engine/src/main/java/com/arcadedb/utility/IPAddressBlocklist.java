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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable, thread-safe matcher for a set of CIDR ranges (both IPv4 and IPv6). Used to block outbound requests
 * to non-public or otherwise restricted IP addresses (loopback, RFC 1918 private ranges, link-local/cloud-metadata,
 * carrier-grade NAT, multicast, reserved, ...) as a defense against Server-Side Request Forgery (SSRF).
 * <p>
 * IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) are normalized to their embedded IPv4 form before matching so an
 * attacker cannot bypass an IPv4 range by expressing the same address in IPv6 notation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IPAddressBlocklist {
  private final List<Cidr> ranges;

  private record Cidr(byte[] network, int prefixBits) {
    boolean matches(final byte[] addr) {
      if (addr.length != network.length)
        return false;
      final int fullBytes = prefixBits / 8;
      for (int i = 0; i < fullBytes; i++)
        if (addr[i] != network[i])
          return false;
      final int remBits = prefixBits % 8;
      if (remBits > 0) {
        final int mask = (0xFF << (8 - remBits)) & 0xFF;
        return (addr[fullBytes] & mask) == (network[fullBytes] & mask);
      }
      return true;
    }
  }

  private IPAddressBlocklist(final List<Cidr> ranges) {
    this.ranges = ranges;
  }

  /**
   * Parses a comma-separated list of CIDR ranges (e.g. {@code "127.0.0.0/8, ::1/128, 10.0.0.0/8"}). A bare address
   * without a prefix is treated as a single-host range (/32 for IPv4, /128 for IPv6). Blank entries are ignored.
   * A null or empty string yields an empty (never-blocking) block-list.
   *
   * @throws IllegalArgumentException if an entry cannot be parsed
   */
  public static IPAddressBlocklist parse(final String csv) {
    final List<Cidr> ranges = new ArrayList<>();
    if (csv != null && !csv.isBlank()) {
      for (final String raw : csv.split(",")) {
        final String entry = raw.trim();
        if (entry.isEmpty())
          continue;
        ranges.add(parseCidr(entry));
      }
    }
    return new IPAddressBlocklist(ranges);
  }

  private static Cidr parseCidr(final String entry) {
    final int slash = entry.indexOf('/');
    final String host = slash < 0 ? entry : entry.substring(0, slash);
    final byte[] network;
    try {
      // Use getByName only for literal IPs: reject anything that would trigger a DNS lookup.
      if (!isNumericAddress(host))
        throw new IllegalArgumentException("CIDR entry is not a literal IP address: '" + entry + "'");
      network = InetAddress.getByName(host).getAddress();
    } catch (final Exception e) {
      throw new IllegalArgumentException("Invalid CIDR range in block-list: '" + entry + "'", e);
    }
    final int maxBits = network.length * 8;
    int prefixBits = maxBits;
    if (slash >= 0) {
      try {
        prefixBits = Integer.parseInt(entry.substring(slash + 1).trim());
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException("Invalid CIDR prefix in block-list: '" + entry + "'", e);
      }
      if (prefixBits < 0 || prefixBits > maxBits)
        throw new IllegalArgumentException("CIDR prefix out of range (0-" + maxBits + ") in block-list: '" + entry + "'");
    }
    return new Cidr(network, prefixBits);
  }

  /**
   * Returns true if the given address falls inside any configured range. An empty block-list never blocks.
   */
  public boolean isBlocked(final InetAddress address) {
    if (address == null)
      return false;
    final byte[] addr = normalize(address.getAddress());
    for (final Cidr range : ranges)
      if (range.matches(addr))
        return true;
    return false;
  }

  public boolean isEmpty() {
    return ranges.isEmpty();
  }

  /**
   * Collapses an IPv4-mapped IPv6 address (::ffff:a.b.c.d) to its 4-byte IPv4 form so IPv4 ranges still apply.
   */
  private static byte[] normalize(final byte[] addr) {
    if (addr.length == 16 && isIPv4Mapped(addr)) {
      final byte[] v4 = new byte[4];
      System.arraycopy(addr, 12, v4, 0, 4);
      return v4;
    }
    return addr;
  }

  private static boolean isIPv4Mapped(final byte[] b) {
    for (int i = 0; i < 10; i++)
      if (b[i] != 0)
        return false;
    return (b[10] & 0xFF) == 0xFF && (b[11] & 0xFF) == 0xFF;
  }

  private static boolean isNumericAddress(final String host) {
    // IPv6 literals contain ':'; IPv4 literals are only digits and dots.
    if (host.indexOf(':') >= 0)
      return true;
    for (int i = 0; i < host.length(); i++) {
      final char c = host.charAt(i);
      if (c != '.' && (c < '0' || c > '9'))
        return false;
    }
    return !host.isEmpty();
  }
}
