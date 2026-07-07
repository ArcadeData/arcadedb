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

/**
 * Shared helpers for Server-Side Request Forgery (SSRF, CWE-918) mitigation. Keeping the blocked-range
 * classification in one place avoids drift between the several call sites that fetch from a
 * user-supplied URL (e.g. {@code IMPORT DATABASE} and the server {@code restore}/{@code import}
 * commands).
 */
public class SsrfProtectionUtils {
  private SsrfProtectionUtils() {
  }

  /**
   * Returns {@code true} if the address belongs to a network range that must not be reachable from a
   * user-supplied URL: loopback, link-local, site-local/private, wildcard, multicast, IPv6 Unique
   * Local Addresses (ULA, {@code fc00::/7}, RFC 4193) or IPv4 Carrier-Grade NAT (CGNAT,
   * {@code 100.64.0.0/10}, RFC 6598). The last two are not covered by the {@link InetAddress} flags.
   */
  public static boolean isPrivateOrLocalAddress(final InetAddress address) {
    if (address.isLoopbackAddress()         // 127.0.0.0/8, ::1
        || address.isLinkLocalAddress()     // 169.254.0.0/16 (cloud metadata), fe80::/10
        || address.isSiteLocalAddress()     // 10/8, 172.16/12, 192.168/16
        || address.isAnyLocalAddress()      // 0.0.0.0, ::
        || address.isMulticastAddress())    // 224.0.0.0/4, ff00::/8
      return true;

    final byte[] bytes = address.getAddress();
    // IPv6 Unique Local Addresses (ULA) fc00::/7 (RFC 4193).
    if (bytes.length == 16 && (bytes[0] & 0xfe) == 0xfc)
      return true;
    // IPv4 Carrier-Grade NAT (CGNAT) 100.64.0.0/10 (RFC 6598).
    return bytes.length == 4 && (bytes[0] & 0xff) == 100 && (bytes[1] & 0xc0) == 64;
  }
}
