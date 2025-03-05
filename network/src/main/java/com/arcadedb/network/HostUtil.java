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
 * Utility class for parsing host addresses.
 */
public class HostUtil {
  public static final String CLIENT_DEFAULT_PORT = "2480";
  public static final String HA_DEFAULT_PORT     = "2424";

  /**
   * Parses the host address and returns the host and port. If the port is not specified, it returns the default port.
   *
   * @param host        The host address to parse.
   * @param defaultPort The default port to use if no port is specified in the host address.
   *
   * @return An array containing the host and port.
   */
  public static String[] parseHostAddress(String host, final String defaultPort) {
    if (host == null)
      throw new IllegalArgumentException("Host null");

    host = host.trim();

    if (host.isEmpty())
      throw new IllegalArgumentException("Host is empty");

    String alias = "";
    if (host.startsWith("{")) {
      alias = host.substring(host.indexOf("{") + 1, host.indexOf("}"));
      // REMOVE ALIAS
      host = host.substring(host.indexOf("}") + 1);
    }

    final String[] parts = host.split(":");
    if (parts.length == 1 || parts.length == 8) {
      // ( IPV4 OR IPV6 ) NO PORT
      if (alias.isEmpty())
        alias = host;
      return new String[] { host, defaultPort, alias };
    } else if (parts.length == 2 || parts.length == 9) {
      // ( IPV4 OR IPV6 ) + PORT
      final int pos = host.lastIndexOf(":");
      if (alias.isEmpty())
        alias = host.substring(0, pos);
      return new String[] { host.substring(0, pos), host.substring(pos + 1), alias };
    }

    throw new IllegalArgumentException("Invalid host " + host);
  }
}
