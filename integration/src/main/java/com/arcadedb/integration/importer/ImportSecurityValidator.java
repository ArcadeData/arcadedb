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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;

/**
 * Validates the source URL/path of an {@code IMPORT DATABASE} command to mitigate Server-Side Request Forgery (SSRF,
 * CWE-918) and arbitrary local file read (path traversal, CWE-22).
 *
 * <p>Two independent, configuration-driven policies are enforced:</p>
 * <ul>
 *   <li><b>SSRF</b>: when {@link GlobalConfiguration#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS} is enabled (default),
 *   HTTP(S) URLs that resolve to loopback, link-local, private (site-local), wildcard or multicast addresses are
 *   rejected. This blocks access to cloud metadata endpoints (e.g. 169.254.169.254) and internal services.</li>
 *   <li><b>Local file read</b>: when {@link GlobalConfiguration#SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS} is set,
 *   {@code file://} and plain-path imports are restricted to the listed directories. {@code classpath://} resources are
 *   always allowed because they are bundled with the server, not attacker controlled.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ImportSecurityValidator {
  private static final String RESOURCE_SEPARATOR = ":::";
  private static final String FILE_PREFIX        = "file://";
  private static final String CLASSPATH_PREFIX   = "classpath://";

  private ImportSecurityValidator() {
  }

  /**
   * Validates an HTTP(S) import URL. Throws {@link SecurityException} if the host resolves to a blocked
   * private/local network address and the SSRF protection is enabled.
   */
  public static void validateRemoteURL(final String url) {
    if (!GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean())
      return;

    final int sep = url.lastIndexOf(RESOURCE_SEPARATOR);
    final String urlPath = sep > -1 ? url.substring(0, sep) : url;

    final String host;
    try {
      host = new URL(urlPath).getHost();
    } catch (final MalformedURLException e) {
      throw new SecurityException("IMPORT DATABASE: malformed URL");
    }

    if (host == null || host.isEmpty())
      throw new SecurityException("IMPORT DATABASE: URL is missing a host");

    final InetAddress[] addresses;
    try {
      addresses = InetAddress.getAllByName(host);
    } catch (final UnknownHostException e) {
      throw new SecurityException("IMPORT DATABASE: cannot resolve host '" + host + "'");
    }

    for (final InetAddress address : addresses) {
      if (isBlockedAddress(address))
        throw new SecurityException("IMPORT DATABASE: access to host '" + host + "' (" + address.getHostAddress()
            + ") is blocked because it resolves to a private/local network address. Set '"
            + GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getKey()
            + "=false' to allow imports from internal hosts in trusted environments");
    }
  }

  /**
   * Validates a local file import URL/path against the configured allow-list. {@code classpath://} resources are
   * always allowed. When the allow-list is empty (default) no restriction is applied.
   */
  public static void validateLocalURL(final String url) throws IOException {
    final String allowed = GlobalConfiguration.SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS.getValueAsString();
    if (allowed == null || allowed.isBlank())
      // NO RESTRICTION CONFIGURED: BACKWARD COMPATIBLE BEHAVIOUR
      return;

    final int sep = url.lastIndexOf(RESOURCE_SEPARATOR);
    String filePath = sep > -1 ? url.substring(0, sep) : url;

    if (filePath.startsWith(CLASSPATH_PREFIX))
      // SERVER-BUNDLED RESOURCES ARE ALWAYS ALLOWED
      return;

    if (filePath.startsWith(FILE_PREFIX))
      filePath = filePath.substring(FILE_PREFIX.length());

    // CANONICALIZE TO RESOLVE SYMLINKS AND '..' TRAVERSAL
    final Path target = new File(filePath).getCanonicalFile().toPath();

    for (final String dir : allowed.split(",")) {
      final String trimmed = dir.trim();
      if (trimmed.isEmpty())
        continue;

      final Path base = new File(trimmed).getCanonicalFile().toPath();
      if (target.startsWith(base))
        return;
    }

    throw new SecurityException("IMPORT DATABASE: access to local path '" + filePath
        + "' is not allowed. Permitted directories: " + allowed);
  }

  /**
   * Returns {@code true} if the address belongs to a network range that must not be reachable through
   * {@code IMPORT DATABASE} (loopback, link-local, site-local/private, wildcard, multicast, IPv6 ULA
   * or IPv4 CGNAT).
   * <p>
   * The blocked-range logic mirrors {@code PostServerCommandHandler.isBlockedHost} in the server
   * module; keep the two in sync when adding ranges.
   */
  static boolean isBlockedAddress(final InetAddress address) {
    if (address.isLoopbackAddress()         // 127.0.0.0/8, ::1
        || address.isLinkLocalAddress()     // 169.254.0.0/16 (cloud metadata), fe80::/10
        || address.isSiteLocalAddress()     // 10/8, 172.16/12, 192.168/16
        || address.isAnyLocalAddress()      // 0.0.0.0, ::
        || address.isMulticastAddress())    // 224.0.0.0/4, ff00::/8
      return true;

    // InetAddress flags miss two modern private ranges, so check the raw bytes explicitly.
    final byte[] bytes = address.getAddress();
    // IPv6 Unique Local Addresses (ULA) fc00::/7 (RFC 4193).
    if (bytes.length == 16 && (bytes[0] & 0xfe) == 0xfc)
      return true;
    // IPv4 Carrier-Grade NAT (CGNAT) 100.64.0.0/10 (RFC 6598).
    return bytes.length == 4 && (bytes[0] & 0xff) == 100 && (bytes[1] & 0xc0) == 64;
  }
}
