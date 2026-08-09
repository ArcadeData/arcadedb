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
      if (addr.isEmpty() || addr.indexOf('[') >= 0 || addr.indexOf(']') >= 0)
        throw new IllegalArgumentException("Invalid host " + host);

      if (closeBracket == host.length() - 1)
        return new String[] { addr, defaultPort };
      if (host.charAt(closeBracket + 1) == ':')
        return new String[] { addr, validatePort(host, host.substring(closeBracket + 2)) };

      throw new IllegalArgumentException("Invalid host " + host);
    }

    // A bracket outside the RFC 3986 bracketed-IPv6 form above is always malformed.
    if (host.indexOf('[') >= 0 || host.indexOf(']') >= 0)
      throw new IllegalArgumentException("Invalid host " + host);

    // Legacy unbracketed format: colon-count heuristic for fully-expanded IPv6.
    // Split with limit -1 so a trailing colon (e.g. "host:") surfaces as a trailing
    // empty part instead of being silently dropped.
    final String[] parts = host.split(":", -1);
    if (parts.length == 1)
      // IPV4 OR HOST NAME, NO PORT
      return new String[] { host, defaultPort };
    else if (parts.length == 2) {
      // ( IPV4 OR HOST NAME ) + PORT
      if (parts[0].isEmpty())
        throw new IllegalArgumentException("Invalid host " + host);
      return new String[] { parts[0], validatePort(host, parts[1]) };
    } else if (parts.length == 8 && !host.endsWith(":"))
      // IPV6 NO PORT
      return new String[] { host, defaultPort };
    else if (parts.length == 9 && !host.endsWith(":")) {
      // IPV6 + PORT
      final int pos = host.lastIndexOf(':');
      return new String[] { host.substring(0, pos), validatePort(host, host.substring(pos + 1)) };
    }

    throw new IllegalArgumentException("Invalid host " + host);
  }

  /**
   * Validates that {@code port} is a non-empty integer in the range 1-65535. The full original
   * {@code host} string is only used to name the offending value in the exception message.
   */
  private static String validatePort(final String host, final String port) {
    if (port.isEmpty())
      throw new IllegalArgumentException("Invalid host " + host);

    final int value;
    try {
      value = Integer.parseInt(port);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid host " + host);
    }

    if (value < 1 || value > 65535)
      throw new IllegalArgumentException("Invalid host " + host);

    return port;
  }
}
