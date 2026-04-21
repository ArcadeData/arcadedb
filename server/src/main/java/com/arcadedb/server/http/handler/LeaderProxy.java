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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.logging.Level;

/**
 * Transparent HTTP proxy from a follower to the current leader.
 * Invoked from {@link AbstractServerHttpHandler} when a write request
 * arrives on a non-leader and would otherwise return HTTP 400.
 */
public final class LeaderProxy {

  private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
      "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
      "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length");

  private final HttpServer httpServer;
  private final HttpClient client;
  private final long readTimeoutMs;
  private final int maxBodySize;

  public LeaderProxy(final HttpServer httpServer) {
    this.httpServer = httpServer;
    final long connectTimeoutMs = httpServer.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT);
    this.readTimeoutMs = httpServer.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_PROXY_READ_TIMEOUT);
    this.maxBodySize = httpServer.getServer().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.HA_PROXY_MAX_BODY_SIZE);
    this.client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofMillis(connectTimeoutMs))
        .build();
  }

  /**
   * Attempts to proxy the current exchange to the given leader address.
   *
   * @return {@code true} if the response has been written to the exchange,
   *         {@code false} if the caller should fall back to its default error response.
   */
  public boolean tryProxy(final HttpServerExchange exchange, final String leaderAddress,
      final ServerSecurityUser user) {
    if (leaderAddress == null || leaderAddress.isBlank())
      return false;

    // Loop prevention: already-forwarded requests must not be re-forwarded.
    final HeaderValues existingToken = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
    if (existingToken != null && !existingToken.isEmpty()) {
      LogManager.instance().log(this, Level.WARNING,
          "Leader proxy refused to forward already-forwarded request (loop prevention)");
      return false;
    }

    final byte[] body;
    try {
      body = readBodyCapped(exchange);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy failed to read request body: %s", e, e.getMessage());
      return false;
    } catch (final BodyTooLargeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Leader proxy refused to forward request: body too large (> %d bytes)", maxBodySize);
      return false;
    }

    final String path = exchange.getRequestPath();
    final String query = exchange.getQueryString();
    final String urlString = "http://" + leaderAddress + path + (query == null || query.isEmpty() ? "" : "?" + query);

    final HttpRequest.Builder builder;
    try {
      builder = HttpRequest.newBuilder(URI.create(urlString));
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy URL invalid: %s (%s)", urlString, e.getMessage());
      return false;
    }
    builder.timeout(Duration.ofMillis(readTimeoutMs));

    // Copy request headers minus hop-by-hop and Authorization.
    exchange.getRequestHeaders().forEach(hv -> {
      final String name = hv.getHeaderName().toString();
      final String lower = name.toLowerCase();
      if (HOP_BY_HOP_HEADERS.contains(lower) || lower.equals("authorization"))
        return;
      builder.header(name, hv.getFirst());
    });

    // Inject cluster-forwarded auth.
    final String clusterToken = httpServer.getServer().getConfiguration()
        .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (clusterToken == null || clusterToken.isBlank()) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy refused: HA_CLUSTER_TOKEN is not configured");
      return false;
    }
    builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    if (user != null)
      builder.header("X-ArcadeDB-Forwarded-User", user.getName());

    builder.method(exchange.getRequestMethod().toString(), HttpRequest.BodyPublishers.ofByteArray(body));

    final HttpResponse<byte[]> response;
    try {
      response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    } catch (final HttpTimeoutException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy timeout to %s: %s", leaderAddress, e.getMessage());
      return false;
    } catch (final ConnectException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy cannot connect to %s: %s", leaderAddress, e.getMessage());
      return false;
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy IO error to %s: %s", leaderAddress, e.getMessage());
      return false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LogManager.instance().log(this, Level.WARNING, "Leader proxy interrupted to %s", leaderAddress);
      return false;
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.SEVERE, "Leader proxy unexpected error to %s: %s", t, leaderAddress, t.getMessage());
      return false;
    }

    // Relay response.
    exchange.setStatusCode(response.statusCode());
    response.headers().map().forEach((name, values) -> {
      final String lower = name.toLowerCase();
      if (HOP_BY_HOP_HEADERS.contains(lower) || lower.equals("content-length"))
        return;
      for (final String value : values)
        exchange.getResponseHeaders().add(new HttpString(name), value);
    });
    exchange.getResponseSender().send(ByteBuffer.wrap(response.body()));
    return true;
  }

  private byte[] readBodyCapped(final HttpServerExchange exchange) throws IOException, BodyTooLargeException {
    exchange.startBlocking();
    final InputStream in = exchange.getInputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final byte[] buf = new byte[8192];
    int total = 0;
    int read;
    while ((read = in.read(buf)) != -1) {
      total += read;
      if (total > maxBodySize)
        throw new BodyTooLargeException();
      out.write(buf, 0, read);
    }
    return out.toByteArray();
  }

  private static final class BodyTooLargeException extends Exception {
    private static final long serialVersionUID = 1L;
  }
}
