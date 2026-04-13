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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurityException;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

/**
 * Proxies an HTTP request from a follower node to the current Raft leader. Used when a write
 * operation arrives at a non-leader node in the Ratis HA cluster.
 * <p>
 * Auth handling: Bearer tokens (session and API) are resolved locally and converted to a
 * cluster-token + forwarded-user pair for the inter-node hop. Basic auth is forwarded as-is.
 * Multi-hop proxying is supported by preserving the forwarded-user header across intermediate nodes.
 * <p>
 * SECURITY NOTE: Inter-node communication currently uses plain HTTP. In production deployments,
 * nodes should be connected via a secure overlay network (e.g., VPN, mTLS sidecar, or private subnet)
 * to protect WAL data and authentication credentials in transit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LeaderProxy {

  /** Header carrying the shared cluster secret used for inter-node authentication. */
  static final String HEADER_CLUSTER_TOKEN  = "X-ArcadeDB-Cluster-Token";
  /** Header carrying the original username when forwarding through the cluster. */
  static final String HEADER_FORWARDED_USER = "X-ArcadeDB-Forwarded-User";

  private static final int PROXY_CONNECT_TIMEOUT_MS = 5_000;

  private final HttpServer httpServer;

  public LeaderProxy(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  /**
   * Forwards the current exchange to the given leader address.
   * <p>
   * On success the exchange is fully handled (response written to client).
   *
   * @param exchange     the incoming HTTP exchange (must not have started sending a response)
   * @param leaderAddr   host:port of the leader (e.g. {@code "10.0.0.1:2480"})
   * @param savedPayload raw request body (already read from the exchange input stream)
   * @throws ProxyAuthException if the request's credentials cannot be resolved - caller should send 401
   * @throws Exception          for any connection or I/O failure - caller should send 503
   */
  public void proxy(final HttpServerExchange exchange, final String leaderAddr, final String savedPayload) throws Exception {
    final String path = exchange.getRequestPath();
    final String query = exchange.getQueryString();
    final String protocol = httpServer.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL) ?
        "https" : "http";
    final String targetUrl = protocol + "://" + leaderAddr + path + (query != null && !query.isEmpty() ? "?" + query : "");

    LogManager.instance().log(this, Level.FINE, "Proxying request to leader: %s", targetUrl);

    final HttpURLConnection conn = (HttpURLConnection) new URI(targetUrl).toURL().openConnection();
    conn.setRequestMethod(exchange.getRequestMethod().toString());
    conn.setConnectTimeout(PROXY_CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(httpServer.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.HA_PROXY_READ_TIMEOUT));

    // Forward auth using cluster token for inter-node identity.
    // The cluster token is a shared secret derived from the cluster name + root password.
    // For session-based auth (Bearer), we use cluster token + forwarded user instead of
    // forwarding the per-node session token. For Basic/API tokens, we forward as-is.
    final var haPlugin = httpServer.getServer().getHA();
    final var authHeader = exchange.getRequestHeaders().get(Headers.AUTHORIZATION);
    if (haPlugin != null && haPlugin.getClusterToken() != null) {
      final String auth = authHeader != null && !authHeader.isEmpty() ? authHeader.getFirst() : null;
      if (auth != null && auth.startsWith("Bearer")) {
        final String token = auth.substring("Bearer".length()).trim();

        if (ApiTokenConfiguration.isApiToken(token)) {
          // API token (at- prefix): resolve locally and forward as cluster token + user.
          // This avoids sending long-lived API tokens in plain text over the inter-node channel.
          try {
            final var user = httpServer.getServer().getSecurity().authenticateByApiToken(token);
            conn.setRequestProperty(HEADER_CLUSTER_TOKEN, haPlugin.getClusterToken());
            conn.setRequestProperty(HEADER_FORWARDED_USER, user.getName());
          } catch (final ServerSecurityException ex) {
            conn.disconnect();
            throw new ProxyAuthException(401, "Invalid or expired API token");
          }
        } else {
          // Session token (AU- prefix): resolve locally and forward as cluster token + user
          final HttpAuthSession session = httpServer.getAuthSessionManager().getSessionByToken(token);
          if (session == null) {
            conn.disconnect();
            throw new ProxyAuthException(401, "Session expired or invalid");
          }
          conn.setRequestProperty(HEADER_CLUSTER_TOKEN, haPlugin.getClusterToken());
          conn.setRequestProperty(HEADER_FORWARDED_USER, session.getUser().getName());
        }
      } else if (auth != null)
        // Basic auth: forward as-is (credentials are per-request, not long-lived)
        conn.setRequestProperty("Authorization", auth);
      else {
        // No Authorization header - this is a multi-hop proxy where the original request
        // was authenticated via cluster token + forwarded user. Preserve the forwarded user
        // from the incoming request. Reject if no forwarded user is present to prevent
        // unauthenticated requests from gaining root privileges.
        final var forwardedUser = exchange.getRequestHeaders().get(HEADER_FORWARDED_USER);
        if (forwardedUser == null || forwardedUser.isEmpty()) {
          conn.disconnect();
          throw new ProxyAuthException(401, "No authentication credentials provided");
        }
        conn.setRequestProperty(HEADER_CLUSTER_TOKEN, haPlugin.getClusterToken());
        conn.setRequestProperty(HEADER_FORWARDED_USER, forwardedUser.getFirst());
      }
    } else if (authHeader != null && !authHeader.isEmpty())
      conn.setRequestProperty("Authorization", authHeader.getFirst());

    conn.setRequestProperty("Content-Type", "application/json");

    // Forward request body for POST/PUT (use saved payload since input stream was already consumed)
    final String method = exchange.getRequestMethod().toString();
    if ("POST".equals(method) || "PUT".equals(method)) {
      conn.setDoOutput(true);
      if (savedPayload != null && !savedPayload.isEmpty())
        try (final var os = conn.getOutputStream()) {
          os.write(savedPayload.getBytes(StandardCharsets.UTF_8));
        }
    }

    // Send leader's response back to the client
    final int status = conn.getResponseCode();
    exchange.setStatusCode(status);

    final String contentType = conn.getContentType();
    if (contentType != null)
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, contentType);

    // Forward the commit index header for READ_YOUR_WRITES bookmark tracking
    final String commitIndex = conn.getHeaderField(RemoteHttpComponent.HEADER_COMMIT_INDEX);
    if (commitIndex != null)
      exchange.getResponseHeaders().put(new HttpString(RemoteHttpComponent.HEADER_COMMIT_INDEX), commitIndex);

    try (final var in = status < 400 ? conn.getInputStream() : conn.getErrorStream()) {
      if (in != null) {
        exchange.startBlocking();
        try (final var out = exchange.getOutputStream()) {
          in.transferTo(out);
        }
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Thrown when the proxy cannot proceed because the incoming request's credentials cannot be
   * resolved (e.g., an expired session or invalid API token).
   * The caller should forward {@link #getStatusCode()} to the client.
   */
  public static class ProxyAuthException extends Exception {
    private final int statusCode;

    public ProxyAuthException(final int statusCode, final String message) {
      super(message);
      this.statusCode = statusCode;
    }

    public int getStatusCode() {
      return statusCode;
    }
  }
}
