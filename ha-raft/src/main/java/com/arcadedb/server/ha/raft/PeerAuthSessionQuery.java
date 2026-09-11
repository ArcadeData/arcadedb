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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAServerPlugin;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * Asks the node that issued an authentication token whether it still holds the session, and tells every node to
 * drop its copy on logout, through {@code POST /api/v1/cluster/auth-session} (issue #7424).
 * <p>
 * Transport and authentication are {@link PeerCapabilityQuery}'s: the {@code X-ArcadeDB-Cluster-Token} +
 * {@code X-ArcadeDB-Forwarded-User} pair every peer-to-peer RPC uses, the peer's HTTPS endpoint preferred when
 * {@code arcadedb.ssl.enabled} is set, and the dial guarded by {@link PeerDialAddress} so a question about a
 * token is never put to a node that was not asked - which matters more here than for a capability probe, because
 * the answer decides whether a client is authenticated.
 * <p>
 * <b>A node that predates this route answers 404</b>, the same status as "I do not hold this token". A cluster in
 * the middle of a rolling upgrade therefore refuses the tokens its old nodes issued on its new ones, exactly as it
 * did before the route existed; nothing is granted on a guess.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerAuthSessionQuery {
  static final String ROUTE = "/api/v1/cluster/auth-session";

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private PeerAuthSessionQuery() {
  }

  /**
   * Asks {@code issuer} about {@code token}.
   *
   * @return the session as the issuer holds it, or {@code null} when the issuer answered that it does not hold
   * it (404)
   *
   * @throws IOException when the issuer could not be asked: no address identifies it on its own, transport
   *                     failure, timeout, or any status other than 200 and 404
   */
  static HAServerPlugin.PeerAuthSession validate(final RaftHAServer raft, final RaftPeerId issuer, final String token,
      final long timeoutMs) throws IOException {
    final PeerDialAddress dial = PeerDialAddress.resolve(raft, issuer, "issuer of the authentication token");
    if (dial.refused())
      throw new IOException(dial.refusal());
    final HttpRequest request = request(raft, dial, token, "validate", timeoutMs);
    final HttpResponse<String> response;
    try {
      response = client(raft, request).send(request, HttpResponse.BodyHandlers.ofString());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while asking " + issuer + " about an authentication token", e);
    }
    if (response.statusCode() == 404)
      return null;
    if (response.statusCode() != 200)
      throw new IOException("authentication-session query to " + request.uri() + " returned HTTP " + response.statusCode());
    final JSONObject body = new JSONObject(response.body());
    final String userName = body.getString("user", null);
    if (userName == null || userName.isBlank())
      throw new IOException("authentication-session query to " + request.uri() + " answered without a user");
    return new HAServerPlugin.PeerAuthSession(userName, body.getLong("createdAt", 0L));
  }

  /**
   * Tells every other peer of the configured group to drop its copy of {@code token}. All peers are dialled at
   * once and the call returns when they have answered or {@code timeoutMs} has passed, whichever is first: a
   * peer that is down must not hold a logout hostage, and it drops the copy on its own at the next renewal.
   */
  static void revokeEverywhere(final RaftHAServer raft, final String token, final long timeoutMs) {
    final List<CompletableFuture<HttpResponse<Void>>> pending = new ArrayList<>();
    for (final RaftPeer peer : raft.getRaftGroup().getPeers()) {
      final RaftPeerId peerId = peer.getId();
      if (peerId.equals(raft.getLocalPeerId()))
        continue;
      final PeerDialAddress dial = PeerDialAddress.resolve(raft, peerId, "holder of an authentication session copy");
      if (dial.refused()) {
        LogManager.instance().log(PeerAuthSessionQuery.class, Level.FINE,
            "Not asking %s to drop an authentication session copy: %s", peerId, dial.refusal());
        continue;
      }
      try {
        final HttpRequest request = request(raft, dial, token, "revoke", timeoutMs);
        pending.add(client(raft, request).sendAsync(request, HttpResponse.BodyHandlers.discarding()));
      } catch (final IOException e) {
        LogManager.instance().log(PeerAuthSessionQuery.class, Level.FINE,
            "Cannot ask %s to drop an authentication session copy: %s", peerId, e.getMessage());
      }
    }
    if (pending.isEmpty())
      return;
    try {
      CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final ExecutionException | TimeoutException e) {
      // Best effort: the peers that did not answer drop the copy at their next renewal with the issuer.
      LogManager.instance().log(PeerAuthSessionQuery.class, Level.FINE,
          "Not every peer confirmed dropping an authentication session copy within %d ms: %s", timeoutMs,
          e.getMessage());
    }
  }

  private static HttpRequest request(final RaftHAServer raft, final PeerDialAddress dial, final String token,
      final String action, final long timeoutMs) {
    final boolean useSSL = raft.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String url = useSSL && dial.httpsAddress() != null ? "https://" + dial.httpsAddress() + ROUTE
        : "http://" + dial.httpAddress() + ROUTE;
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER)
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("action", action).put("token", token).toString()));
    final String clusterToken = raft.getClusterToken();
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    return builder.build();
  }

  private static HttpClient client(final RaftHAServer raft, final HttpRequest request) throws IOException {
    return "https".equals(request.uri().getScheme()) ? raft.getHttpsClients().clientFor(raft.getServer()) : HTTP;
  }
}
