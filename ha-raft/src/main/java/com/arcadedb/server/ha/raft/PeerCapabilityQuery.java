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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Asks one peer what wire-format sections it can decode, by calling its
 * {@code POST /api/v1/cluster/capabilities} RPC (issue #7219).
 * <p>
 * Transport, authentication and endpoint selection are {@link LeaderDatabaseQuery}'s, because this is the same
 * kind of call: the {@code X-ArcadeDB-Cluster-Token} + {@code X-ArcadeDB-Forwarded-User} pair every peer-to-peer
 * cluster RPC uses, and the peer's HTTPS endpoint preferred when {@code arcadedb.ssl.enabled} is set.
 * <p>
 * <b>A node that predates this route answers 404</b>, which arrives here as an {@link IOException} exactly like an
 * unreachable peer. That is the whole discriminator: no separate version comparison is needed, and none would be
 * safe - a version string tells you what a build calls itself, not what its decoder actually handles.
 * <p>
 * <b>The answer is bound to the peer that gave it.</b> {@link #parse} refuses an advertisement whose {@code peerId}
 * is not the one being probed. With no {@code http} port declared in {@code arcadedb.ha.serverList} a peer's
 * endpoint is DERIVED, and on such a cluster several peers collapse onto one address (issues #6202, #6267); the
 * dial is already guarded by {@link PeerDialAddress}, and this is the second half of the same guard - the guard
 * withholds an address that identifies no single peer, this refuses an answer that came back from the wrong one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PeerCapabilityQuery {

  /** What a peer says about itself. {@code version} is operator-facing only; nothing decides on it. */
  public record Advertisement(String peerId, String version, Set<String> capabilities) {
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private PeerCapabilityQuery() {
  }

  /**
   * Synchronously asks {@code expectedPeerId} what it can decode.
   *
   * @param expectedPeerId the peer this address is believed to name; an answer from any other peer is refused.
   * @param httpAddr       the peer plain-HTTP address ({@code host:port}).
   * @param httpsAddr      the peer HTTPS address ({@code host:port}), or {@code null} when none is known.
   * @param clusterToken   the inter-node cluster token, may be {@code null}/blank if not configured.
   * @param timeoutMs      per-request timeout in milliseconds.
   * @param server         the local server, used to read {@code arcadedb.ssl.enabled} and build the trust context.
   *
   * @throws IOException          on transport error, a non-200 response (which is what a peer without this route
   *                              answers), or an advertisement that names another peer.
   * @throws InterruptedException if the calling thread is interrupted while waiting.
   */
  public static Advertisement fetch(final String expectedPeerId, final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server)
      throws IOException, InterruptedException {

    final boolean useSSL = server != null && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String url = chooseUrl(httpAddr, httpsAddr, useSSL);
    if (url == null)
      throw new IOException("no peer address available for a capability query");

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    builder.header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER);
    final HttpRequest request = builder.build();

    if (url.startsWith("https://")) {
      // A dedicated client carrying the cluster trust context, closed after the call - same reasoning as
      // LeaderDatabaseQuery. This runs once per peer per refresh period, not on a hot path.
      try (final HttpClient client = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(5))
          .sslContext(SnapshotInstaller.buildSSLContext(server))
          .build()) {
        return parse(expectedPeerId, client.send(request, HttpResponse.BodyHandlers.ofString()), url);
      }
    }
    return parse(expectedPeerId, HTTP.send(request, HttpResponse.BodyHandlers.ofString()), url);
  }

  /**
   * Picks the endpoint URL. Prefers HTTPS when SSL is enabled and an HTTPS address is available; otherwise plain
   * HTTP. {@code null} when no usable address was provided. Package-private and pure for unit testing.
   */
  static String chooseUrl(final String httpAddr, final String httpsAddr, final boolean useSSL) {
    if (useSSL && httpsAddr != null)
      return "https://" + httpsAddr + "/api/v1/cluster/capabilities";
    if (httpAddr != null)
      return "http://" + httpAddr + "/api/v1/cluster/capabilities";
    return null;
  }

  private static Advertisement parse(final String expectedPeerId, final HttpResponse<String> response, final String url)
      throws IOException {
    if (response.statusCode() != 200)
      throw new IOException("capability query to " + url + " returned HTTP " + response.statusCode());
    return parse(expectedPeerId, response.body(), url);
  }

  /**
   * Reads one advertisement document, refusing one that names a peer other than {@code expectedPeerId}.
   * Package-private and pure for unit testing.
   */
  // @VisibleForTesting
  static Advertisement parse(final String expectedPeerId, final String body, final String url) throws IOException {
    final JSONObject json = new JSONObject(body);
    final String peerId = json.getString("peerId", "");
    if (!peerId.equals(expectedPeerId))
      throw new IOException("capability query to " + url + " for peer '" + expectedPeerId
          + "' was answered by peer '" + peerId + "'; the address does not identify the peer it was meant for "
          + "(declare each node's 'http' port explicitly in " + GlobalConfiguration.HA_SERVER_LIST.getKey() + ")");

    final Set<String> capabilities = new LinkedHashSet<>();
    final JSONArray array = json.has("capabilities") ? json.getJSONArray("capabilities") : new JSONArray();
    for (int i = 0; i < array.length(); i++)
      capabilities.add(array.getString(i));

    return new Advertisement(peerId, json.getString("version", ""), capabilities);
  }
}
