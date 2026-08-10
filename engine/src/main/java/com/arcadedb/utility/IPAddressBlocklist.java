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
import java.util.Arrays;
import java.util.List;

/**
 * Immutable, thread-safe matcher for a set of CIDR ranges (both IPv4 and IPv6). Used to block outbound requests
 * to non-public or otherwise restricted IP addresses (loopback, RFC 1918 private ranges, link-local/cloud-metadata,
 * carrier-grade NAT, multicast, reserved, ...) as a defense against Server-Side Request Forgery (SSRF).
 * <p>
 * An IPv6 address that merely encodes an IPv4 address is normalized to that plain IPv4 form before matching, so an
 * attacker cannot bypass an IPv4 range by expressing the same address through an IPv6 transition mechanism. Five
 * encodings are recognised: IPv4-mapped ({@code ::ffff:a.b.c.d}, RFC 4291 2.5.5.2), the deprecated IPv4-compatible
 * form ({@code ::a.b.c.d}, RFC 4291 2.5.5.1), NAT64 ({@code 64:ff9b::/96}, RFC 6052), 6to4 ({@code 2002::/16},
 * RFC 3056) and Teredo ({@code 2001::/32}, RFC 4380 - the embedded IPv4 is bitwise-inverted). This closes the
 * bypass in GHSA-67m7-7w7g-mpmh, where none of those encodings tripped the flag-based checks a caller ran instead
 * of this class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IPAddressBlocklist {
  /**
   * The reserved/private ranges every SSRF guard in the codebase blocks by default: loopback, RFC 1918 private,
   * link-local (including the cloud metadata address 169.254.169.254), carrier-grade NAT, IETF protocol assignments,
   * benchmarking, multicast, reserved and broadcast, for both IPv4 and IPv6.
   */
  public static final String DEFAULT_RESERVED_RANGES =
      "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,0.0.0.0/8,100.64.0.0/10,192.0.0.0/24,198.18.0.0/15,"
          + "224.0.0.0/4,240.0.0.0/4,255.255.255.255/32,::1/128,::/128,fe80::/10,fc00::/7,ff00::/8";

  private static final Cidr NAT64_PREFIX       = parseCidr("64:ff9b::/96");
  private static final Cidr SIX_TO_FOUR_PREFIX = parseCidr("2002::/16");
  private static final Cidr TEREDO_PREFIX      = parseCidr("2001::/32");

  // The block-list is immutable, so parse DEFAULT_RESERVED_RANGES exactly once and hand out the same instance.
  private static final IPAddressBlocklist DEFAULT_RESERVED_RANGES_INSTANCE = parse(DEFAULT_RESERVED_RANGES);

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

  /**
   * Returns the shared, immutable block-list of {@link #DEFAULT_RESERVED_RANGES}, parsed once at class-init.
   * Safe to call per address: every caller gets the same instance, so there is no reparse cost to guard against.
   */
  public static IPAddressBlocklist defaultReservedRanges() {
    return DEFAULT_RESERVED_RANGES_INSTANCE;
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
   * <p>
   * The raw address is matched first, before any transition-encoding unwrap: a range list can legitimately contain
   * a literal 16-byte IPv6 entry (e.g. {@code ::1/128}, or a whole transition prefix like {@code 2002::/16}) that
   * targets the address as written, and {@link Cidr#matches} rejects on a byte-length mismatch alone, so unwrapping
   * unconditionally before matching would let the 4-byte normalized form silently shadow that entry. The unwrapped
   * form is tried only as a fallback, once the raw address didn't match anything.
   */
  public boolean isBlocked(final InetAddress address) {
    if (address == null)
      return false;
    final byte[] raw = address.getAddress();
    for (final Cidr range : ranges)
      if (range.matches(raw))
        return true;
    final byte[] normalized = normalize(raw);
    if (normalized != null)
      for (final Cidr range : ranges)
        if (range.matches(normalized))
          return true;
    return false;
  }

  public boolean isEmpty() {
    return ranges.isEmpty();
  }

  /**
   * Collapses an IPv6 address that merely encodes an IPv4 address (IPv4-mapped, the deprecated IPv4-compatible
   * form, NAT64, 6to4 or Teredo) to that 4-byte IPv4 form, so an IPv4 range still applies to the same address
   * expressed through IPv6. Returns {@code null}, not the input array, when none of those encodings apply: the
   * caller uses that to skip re-matching a form identical to the one already checked.
   */
  private static byte[] normalize(final byte[] addr) {
    if (addr.length != 16)
      return null;

    if (isIPv4Mapped(addr) || isIPv4Compatible(addr))
      return Arrays.copyOfRange(addr, 12, 16);

    // NAT64 Well-Known Prefix 64:ff9b::/96 (RFC 6052): IPv4 at bytes 12-15.
    if (NAT64_PREFIX.matches(addr))
      return Arrays.copyOfRange(addr, 12, 16);

    // 6to4 2002::/16 (RFC 3056): IPv4 at bytes 2-5.
    if (SIX_TO_FOUR_PREFIX.matches(addr))
      return Arrays.copyOfRange(addr, 2, 6);

    // Teredo 2001::/32 (RFC 4380): IPv4 at bytes 12-15, bitwise-inverted.
    if (TEREDO_PREFIX.matches(addr)) {
      final byte[] v4 = new byte[4];
      for (int i = 0; i < 4; i++)
        v4[i] = (byte) (~addr[12 + i] & 0xff);
      return v4;
    }

    return null;
  }

  private static boolean isIPv4Mapped(final byte[] b) {
    for (int i = 0; i < 10; i++)
      if (b[i] != 0)
        return false;
    return (b[10] & 0xFF) == 0xFF && (b[11] & 0xFF) == 0xFF;
  }

  /**
   * The deprecated "IPv4-compatible IPv6 address" form (RFC 4291 2.5.5.1: {@code ::a.b.c.d}, no {@code ffff}
   * marker in bytes 10-11, unlike the still-current IPv4-mapped form). No real allocated global-unicast range
   * starts with 96 zero bits (those all begin {@code 2000::/3}), so treating any such address as an embedded
   * IPv4 payload cannot misclassify legitimate public IPv6 traffic.
   */
  private static boolean isIPv4Compatible(final byte[] b) {
    for (int i = 0; i < 12; i++)
      if (b[i] != 0)
        return false;
    return true;
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
