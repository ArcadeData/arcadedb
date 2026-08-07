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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Opens an outbound HTTP(S) connection for a server-side fetch of a caller-supplied URL, with Server-Side Request
 * Forgery (SSRF, CWE-918) protection applied to <b>every</b> hop.
 * <p>
 * A validator that checks the original URL and then hands the raw URL string to {@code URL.openConnection()} does not
 * actually constrain where the request lands, for two independent reasons:
 * <ul>
 *   <li><b>Redirects.</b> {@link HttpURLConnection} follows same-protocol 3xx responses by default, and the redirect
 *   target is never revalidated. An attacker-controlled public host that answers
 *   {@code 302 Location: http://169.254.169.254/...} reaches the cloud metadata endpoint through a validator that
 *   passed the original hostname. This is the trivially reproducible bypass.</li>
 *   <li><b>Re-resolution (DNS rebinding / TOCTOU).</b> The validator resolves the hostname and then discards the
 *   addresses; the connection resolves the name again, independently, and can get a different answer.</li>
 * </ul>
 * This helper closes the first by following redirects manually and re-running the full scheme + address check on every
 * hop, and by refusing any non-HTTP(S) scheme anywhere in the chain (so a redirect to {@code file://}, {@code jar://},
 * {@code ftp://} cannot smuggle a local read out of a remote fetch).
 * <p>
 * <b>DNS rebinding.</b> The connection is deliberately established by hostname rather than to a literal IP: connecting
 * to an address would mean setting the {@code Host} header, which the JDK silently drops as a restricted header, and
 * for HTTPS would additionally require reproducing SNI and certificate-hostname verification by hand - far more likely
 * to weaken TLS than to strengthen it. The connection therefore resolves the name again, independently of the
 * validation, which is the classic rebinding (TOCTOU) window.
 * <p>
 * That window is closed by constraining resolution instead of the connection: the addresses validated here are pinned
 * for the duration of the connect through {@link PinnedDnsResolution}, so the lookup the connection performs internally
 * returns only what was already checked. Because the substitution happens below the socket layer, the request is still
 * made by hostname and TLS is completely untouched. Pinning requires the server's
 * {@code java.net.spi.InetAddressResolverProvider} to be installed; it is a JVM-global, single-slot decision, so it
 * ships with the server distribution and is never imposed on an application that embeds {@code arcadedb-engine}.
 * <p>
 * Where the provider is absent (embedded use), the fallback is the JVM positive DNS cache: the
 * {@link InetAddress#getAllByName} performed here populates it and the immediately following connect reuses the
 * validated answer for {@code networkaddress.cache.ttl} seconds (30 by default). Embedders that fetch untrusted URLs
 * should keep that property at its default or higher; setting it to {@code 0} re-opens the rebinding window.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SafeHttpFetcher {

  /** Maximum number of redirect hops followed before the fetch is abandoned. */
  public static final int DEFAULT_MAX_REDIRECTS      = 5;
  public static final int DEFAULT_CONNECT_TIMEOUT_MS = 30_000;
  public static final int DEFAULT_READ_TIMEOUT_MS    = 30_000;

  private SafeHttpFetcher() {
  }

  /**
   * Opens a connected {@link HttpURLConnection} for {@code url}, validating the scheme and every resolved IP address of
   * every hop in the redirect chain. The returned connection is positioned on a non-redirect, non-error response; the
   * caller owns it and must {@code disconnect()} it.
   *
   * @param url     the caller-supplied URL to fetch
   * @param blocked predicate returning true for an address this fetch must not reach. Supply the caller's own policy
   *                (a parsed {@link IPAddressBlocklist}, or an {@link InetAddress}-flag check) so each feature keeps
   *                its own configuration surface. A predicate that never blocks disables the address check but NOT the
   *                scheme and redirect handling.
   * @param context short human-readable label for the calling feature, used to prefix error messages
   *                (e.g. {@code "LOAD CSV"}, {@code "IMPORT DATABASE"})
   *
   * @throws SecurityException if the scheme is not http(s) or an address is blocked, on any hop
   * @throws IOException       if the host cannot be resolved, the redirect limit is exceeded, or the server answers 4xx/5xx
   */
  public static HttpURLConnection open(final String url, final Predicate<InetAddress> blocked, final String context)
      throws IOException {
    return open(url, blocked, context, DEFAULT_MAX_REDIRECTS, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
  }

  public static HttpURLConnection open(final String url, final Predicate<InetAddress> blocked, final String context,
      final int maxRedirects, final int connectTimeoutMs, final int readTimeoutMs) throws IOException {

    String current = url;
    for (int hop = 0; hop <= maxRedirects; hop++) {
      final URL netUrl = URI.create(current).toURL();

      final String protocol = netUrl.getProtocol().toLowerCase(Locale.ROOT);
      if (!protocol.equals("http") && !protocol.equals("https"))
        throw new SecurityException(context + ": blocked disallowed URL scheme '" + protocol + "' in the redirect chain");

      final InetAddress[] validated = validateHost(netUrl.getHost(), blocked, context);

      final HttpURLConnection connection;
      final int status;

      // Pin the hostname to the addresses just validated for the duration of the connect, so the resolution the
      // connection performs internally cannot return a different answer than the one that was checked. This is a no-op
      // unless the server has installed the resolver provider; see PinnedDnsResolution.
      PinnedDnsResolution.bind(netUrl.getHost(), validated);
      try {
        final URLConnection rawConnection = netUrl.openConnection();
        if (!(rawConnection instanceof final HttpURLConnection httpConnection))
          throw new SecurityException(context + ": blocked non-HTTP connection for URL: " + current);
        connection = httpConnection;

        // The whole point: do NOT let the JDK follow the redirect for us, or the target is never revalidated.
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(connectTimeoutMs);
        connection.setReadTimeout(readTimeoutMs);
        connection.setRequestMethod("GET");

        // Forces the request to be sent and the response headers read, so the socket is connected before the pin is
        // released. The caller's later getInputStream() reuses that established connection and resolves nothing.
        status = connection.getResponseCode();
      } finally {
        PinnedDnsResolution.clear();
      }

      if (status >= 300 && status < 400) {
        final String location = connection.getHeaderField("Location");
        connection.disconnect();
        if (location == null || location.isEmpty())
          throw new IOException(context + ": received a redirect with no Location header from: " + current);
        // Resolve a relative Location against the current URL; the next iteration revalidates scheme and addresses.
        current = new URL(netUrl, location).toString();
        continue;
      }

      if (status >= 400) {
        connection.disconnect();
        throw new IOException(context + ": received HTTP " + status + " fetching: " + current);
      }

      return connection;
    }

    throw new SecurityException(context + ": exceeded the maximum number of redirects (" + maxRedirects + ")");
  }

  /**
   * Resolves the host to all of its IP addresses and refuses the request if any of them is blocked. Validating every
   * resolved address rather than just the first closes the multi-record DNS bypass, where a name resolves to both a
   * public address and an internal one and the connection may pick either.
   *
   * @return the resolved addresses, all of which passed the check, so the caller can pin the connection to exactly
   * these rather than letting it resolve the name again
   */
  public static InetAddress[] validateHost(final String rawHost, final Predicate<InetAddress> blocked, final String context)
      throws IOException {
    if (rawHost == null || rawHost.isEmpty())
      throw new SecurityException(context + ": blocked remote URL with no host");

    // java.net.URL#getHost() keeps the brackets around IPv6 literals; strip them before resolving.
    String host = rawHost;
    if (host.startsWith("[") && host.endsWith("]"))
      host = host.substring(1, host.length() - 1);

    final InetAddress[] addresses;
    try {
      addresses = InetAddress.getAllByName(host);
    } catch (final UnknownHostException e) {
      throw new IOException(context + ": could not resolve host '" + host + "'", e);
    }

    for (final InetAddress address : addresses)
      if (blocked.test(address))
        throw new SecurityException(context + ": blocked request to a non-public or restricted address: " + host + " -> "
            + address.getHostAddress());

    return addresses;
  }
}
