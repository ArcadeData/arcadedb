/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Lists the (user) databases a peer holds, by calling its {@code POST /api/v1/cluster/bootstrap-state} RPC
 * (issue #4727). Reuses the same cluster-token + forwarded-user authentication as every other peer-to-peer
 * cluster RPC (see {@link PostBootstrapStateHandler}). The handler already excludes reserved internal
 * databases (names starting with {@code .}), so the returned set is the operator-visible database list.
 * <p>
 * <b>Transport:</b> mirrors {@link SnapshotInstaller#downloadWithRetry} - when {@code arcadedb.ssl.enabled} is
 * set it prefers the peer's HTTPS endpoint and trusts the cluster keystore via
 * {@link SnapshotInstaller#buildSSLContext(ArcadeDBServer)}, falling back to plain HTTP (with a one-time warning)
 * only when no HTTPS address is known. This keeps the feature working on SSL-only clusters, where the plain-HTTP
 * listener is typically disabled - exactly the StatefulSet/empty-node deployments this feature targets (#4470).
 * <p>
 * Used by:
 * <ul>
 *   <li>the join-time reconcile in {@code ArcadeStateMachine.notifyInstallSnapshotFromLeader} to learn which
 *       databases the leader holds and pull the ones this node is missing;</li>
 *   <li>the optional presence fan-out in {@code GetClusterHandler} to build the Studio per-node/per-database
 *       presence matrix.</li>
 * </ul>
 */
public final class LeaderDatabaseQuery {

  /** A single database entry reported by a peer. {@code lastTxId} is {@code -1} when unknown/unreadable. */
  public record DatabaseInfo(String name, long lastTxId) {
  }

  /** The chosen endpoint and scheme for a query; package-private so scheme selection is unit-testable. */
  record Endpoint(String url, boolean https) {
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  /** One-time warning that SSL is enabled but the query fell back to plain HTTP for lack of an HTTPS address. */
  private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = new AtomicBoolean(false);

  private LeaderDatabaseQuery() {
  }

  /**
   * Synchronously queries a peer for the list of databases it holds.
   *
   * @param httpAddr     the peer plain-HTTP address ({@code host:port}).
   * @param httpsAddr    the peer HTTPS address ({@code host:port}), or {@code null} when none is known. Preferred
   *                     when SSL is enabled.
   * @param clusterToken the inter-node cluster token, may be {@code null}/blank if not configured.
   * @param timeoutMs    per-request timeout in milliseconds.
   * @param server       the local server, used to read {@code arcadedb.ssl.enabled} and build the trust context.
   * @return the databases reported by the peer (never {@code null}).
   * @throws IOException          on transport error or a non-200 response.
   * @throws InterruptedException if the calling thread is interrupted while waiting.
   */
  public static List<DatabaseInfo> fetch(final String httpAddr, final String httpsAddr, final String clusterToken,
      final long timeoutMs, final ArcadeDBServer server) throws IOException, InterruptedException {

    final boolean useSSL = server != null && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final Endpoint endpoint = chooseEndpoint(httpAddr, httpsAddr, useSSL);
    if (endpoint == null)
      throw new IOException("no peer address available for bootstrap-state query");
    if (useSSL && !endpoint.https() && PLAIN_HTTP_FALLBACK_WARNED.compareAndSet(false, true))
      LogManager.instance().log(LeaderDatabaseQuery.class, Level.WARNING,
          "SSL is enabled but no HTTPS address is known for a peer; querying its database list over plain HTTP.");

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(endpoint.url()))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    builder.header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER);
    final HttpRequest request = builder.build();

    if (endpoint.https()) {
      // A dedicated client carrying the cluster trust context. HttpClient is AutoCloseable on Java 21, so the
      // selector thread is released after the (rare, opt-in) query rather than leaked. Building one per call is
      // fine for these infrequent paths (reconcile / opt-in presence); if this ever moves onto a hot path, cache
      // an SSL-configured client instead.
      try (final HttpClient client = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(5))
          .sslContext(SnapshotInstaller.buildSSLContext(server))
          .build()) {
        return parse(client.send(request, HttpResponse.BodyHandlers.ofString()), endpoint.url());
      }
    }
    return parse(HTTP.send(request, HttpResponse.BodyHandlers.ofString()), endpoint.url());
  }

  /**
   * Picks the endpoint URL and scheme. Prefers HTTPS when SSL is enabled and an HTTPS address is available;
   * otherwise uses plain HTTP. Returns {@code null} when no usable address is provided. Package-private and pure
   * for unit testing.
   */
  static Endpoint chooseEndpoint(final String httpAddr, final String httpsAddr, final boolean useSSL) {
    if (useSSL && httpsAddr != null)
      return new Endpoint("https://" + httpsAddr + "/api/v1/cluster/bootstrap-state", true);
    if (httpAddr != null)
      return new Endpoint("http://" + httpAddr + "/api/v1/cluster/bootstrap-state", false);
    return null;
  }

  private static List<DatabaseInfo> parse(final HttpResponse<String> resp, final String url) throws IOException {
    if (resp.statusCode() != 200)
      throw new IOException("bootstrap-state query to " + url + " returned HTTP " + resp.statusCode());

    final JSONObject json = new JSONObject(resp.body());
    final JSONArray dbs = json.getJSONArray("databases");
    final List<DatabaseInfo> out = new ArrayList<>(dbs.length());
    for (int i = 0; i < dbs.length(); i++) {
      final JSONObject db = dbs.getJSONObject(i);
      out.add(new DatabaseInfo(db.getString("name"), db.getLong("lastTxId", -1L)));
    }
    return out;
  }
}
