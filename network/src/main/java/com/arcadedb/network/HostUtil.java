package com.arcadedb.network;/*
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

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HostUtil {
  public static final String CLIENT_DEFAULT_PORT = "2480";
  public static final String HA_DEFAULT_PORT     = "2424";

  public static String[] parseHostAddress(String host, final String defaultPort) {
    if (host == null)
      throw new IllegalArgumentException("Host null");

    host = host.trim();

    if (host.isEmpty())
      throw new IllegalArgumentException("Host is empty");

    // Bracketed IPv6 per RFC 3986: [addr] or [addr]:port
    if (host.startsWith("[")) {
      final int closeBracket = host.indexOf(']');
      if (closeBracket < 0)
        throw new IllegalArgumentException("Invalid host " + host);

      final String addr = host.substring(1, closeBracket);
      if (closeBracket == host.length() - 1)
        return new String[] { addr, defaultPort };
      if (host.charAt(closeBracket + 1) == ':')
        return new String[] { addr, host.substring(closeBracket + 2) };

      throw new IllegalArgumentException("Invalid host " + host);
    }

    // Legacy unbracketed format: colon-count heuristic for fully-expanded IPv6
    final String[] parts = host.split(":");
    if (parts.length == 1 || parts.length == 8)
      // ( IPV4 OR IPV6 ) NO PORT
      return new String[] { host, defaultPort };
    else if (parts.length == 2 || parts.length == 9) {
      // ( IPV4 OR IPV6 ) + PORT
      final int pos = host.lastIndexOf(":");
      return new String[] { host.substring(0, pos), host.substring(pos + 1) };
    }

    throw new IllegalArgumentException("Invalid host " + host);
  }
}
