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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Sends an administrative write that arrived on an HA follower to the cluster leader instead, and relays
 * the leader's answer back to the client unchanged.
 * <p>
 * This is the HTTP control plane's answer to "this operation may only run on the leader". It used to be a
 * private method of {@link PostServerCommandHandler}, reachable only by the commands of
 * {@code POST /api/v1/server}, which left the REST routes that perform the same operations - {@code POST},
 * {@code PUT} and {@code DELETE /api/v1/server/users} - running {@code ServerSecurity.*ClusterWide} on
 * whichever node served the request, and from there submitting a Raft entry from a follower (issue #7380).
 * One HTTP API cannot answer the same request two ways depending on which route the client picked, so the
 * forwarding moved here and all four call sites share it.
 * <p>
 * gRPC refuses these calls rather than forwarding them ({@code ArcadeDbGrpcAdminService.requireLeader},
 * issues #7304 and #7309). That is not a different policy: gRPC has no request proxy, so a refusal that
 * names the leader is the closest it can get.
 * <p>
 * <b>This class performs no authorization.</b> The caller checks it first - every current call site runs
 * {@code AbstractServerHttpHandler.checkRootUser} before asking to forward.
 */
public final class LeaderCommandForwarder {
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  private final HttpServer httpServer;

  /**
   * Emits the "a peer forwarded a request here and this node is not the leader either" notice only once
   * (issue #6191). One instance per {@link HttpServer}, so each server in an in-process cluster says it
   * once no matter which route the request came in on.
   */
  private final AtomicBoolean forwardedAgainWarned = new AtomicBoolean(false);

  public LeaderCommandForwarder(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  /**
   * The path plus query string of the request being served, which is what a forward of that same request
   * has to dial on the leader. {@code getRequestPath()} keeps the {@code /api/v1} prefix that
   * {@code PathHandler} resolved away from {@code getRelativePath()}.
   */
  public static String currentPathWithQuery(final HttpServerExchange exchange) {
    final String query = exchange.getQueryString();
    return query == null || query.isEmpty() ? exchange.getRequestPath() : exchange.getRequestPath() + "?" + query;
  }

  /**
   * If this node is an HA replica, forwards the request to the leader and returns its response.
   *
   * @param exchange   the exchange being served, read for the request method and the caller's credentials
   * @param user       the authenticated principal, used to name the caller when the credentials are a
   *                   per-node session token that the leader cannot resolve
   * @param targetPath path (with query string) to dial on the leader, e.g. {@code /api/v1/server/users?name=bob}
   * @param body       request body to relay, or null for a request that has none
   *
   * @return the leader's response, or null when this node is the leader or HA is not enabled - in which
   * case the caller executes the operation locally
   */
  public ExecutionResponse forwardIfReplica(final HttpServerExchange exchange, final ServerSecurityUser user,
      final String targetPath, final String body) throws IOException {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha == null || ha.isLeader())
      return null;

    // A peer already forwarded this request to what it believed was the leader and it arrived here, on a node
    // that is not the leader either. Forwarding it on would send it round the cycle that wrong address
    // created; refuse in one hop with the typed error instead (issue #6191).
    if (LeaderForwardContext.isAlreadyForwarded()) {
      // Also said once in this node's log: the refusal goes back to the peer and from there to the client, so
      // otherwise the only node that can name the misconfiguration never mentions it.
      if (forwardedAgainWarned.compareAndSet(false, true))
        LogManager.instance().log(this, Level.WARNING,
            "A cluster peer forwarded a server command to this node as the leader, but this node is not the leader. "
                + "Unless leadership just moved, the HTTP address that peer resolved for the leader does not identify "
                + "it: declare every node's HTTP port explicitly with the 'host:raftPort:httpPort' syntax in %s. The "
                + "command is refused rather than forwarded on. This notice is logged only once.",
            GlobalConfiguration.HA_SERVER_LIST.getKey());
      throw new ServerIsNotTheLeaderException(
          "Refusing to forward a server command that a cluster peer already forwarded to the leader: it arrived on "
              + "this node, which is not the leader. Either leadership moved while the request was in flight - retry - "
              + "or the HTTP address that peer resolved for the leader does not identify it, which is what declaring "
              + "every node's HTTP port ('host:raftPort:httpPort') in " + GlobalConfiguration.HA_SERVER_LIST.getKey()
              + " prevents", ha.getLeaderName());
    }

    final String leaderHttpAddress = ha.getLeaderAddress();
    if (leaderHttpAddress == null)
      throw new ServerIsNotTheLeaderException("Leader address is unknown", ha.getLeaderName());

    // Dialing an address that resolves to this node comes straight back here, and this node is not the
    // leader. That is what the derive fallback produces when the peers share a host and no HTTP port is
    // declared: it pairs the leader's Raft host with THIS node's HTTP port (issue #6191).
    if (ha.isOwnHttpAddress(leaderHttpAddress))
      throw new ServerIsNotTheLeaderException(
          "Cannot forward the server command: the HTTP address resolved for the leader (" + leaderHttpAddress
              + ") is this node's own, and this node is not the leader. Declare every node's HTTP port explicitly with "
              + "the 'host:raftPort:httpPort' syntax in " + GlobalConfiguration.HA_SERVER_LIST.getKey(),
          ha.getLeaderName());

    final HeaderValues authValues = exchange.getRequestHeaders().get("Authorization");
    final String authHeader = authValues != null ? authValues.getFirst() : null;

    final URI leaderUri;
    try {
      leaderUri = URI.create("http://" + leaderHttpAddress + targetPath);
    } catch (final IllegalArgumentException e) {
      // URI.create is the one call on this path that throws an UNCHECKED exception, so without this it
      // would leave as a 500 - a server fault - for a request target that is simply not a URI.
      //
      // Defence in depth, not a fix for a path anyone has reached: Undertow's request-line parser rejects
      // every character RFC 3986 forbids before a handler runs, because ArcadeDB never sets
      // UndertowOptions.ALLOW_UNESCAPED_CHARACTERS_IN_URL and it defaults to false (verified against a live
      // 26.10.1 server: '?x=a{b}' is answered 400 by the parser, and URI.create rejects the same string).
      // What is guarded is the gap between those two allowances drifting apart - an Undertow upgrade, that
      // option being turned on, or an HTTP/2 ':path' that does not travel through the same parser.
      // LeaderProxy already guards the identical call the same way, which is why this is not left to chance.
      return new ExecutionResponse(400, new JSONObject()
          .put("error", "The request target cannot be forwarded to the cluster leader: " + e.getMessage())
          .toString());
    }

    final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(leaderUri);

    if (body != null) {
      builder.header("Content-Type", "application/json");
      builder.method(exchange.getRequestMethod().toString(), HttpRequest.BodyPublishers.ofString(body));
    } else
      builder.method(exchange.getRequestMethod().toString(), HttpRequest.BodyPublishers.noBody());

    if (authHeader != null && authHeader.startsWith("Bearer AU-")) {
      // Per-node session token: convert to cluster-internal identity headers
      final String clusterToken = httpServer.getServer().getConfiguration()
          .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
      final String userName = user != null ? user.getName() : null;
      if (userName != null)
        builder.header("X-ArcadeDB-Forwarded-User", userName);
      if (clusterToken != null && !clusterToken.isBlank()) {
        builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
        // One hop only: whichever node this address really names refuses the command if it is not the leader,
        // instead of resolving the same address and forwarding it again (issue #6191). Sent only alongside
        // the cluster token, because that is the only form in which the receiving node trusts it - see
        // LeaderForwardContext. The other branch below relays the client's own credentials and carries no
        // marker; its loop protection is the self-address check above.
        builder.header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      }
    } else if (authHeader != null) {
      // Basic or API token: stateless, forward as-is
      builder.header("Authorization", authHeader);
    }

    try {
      final HttpResponse<String> response = HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      return new ExecutionResponse(response.statusCode(), response.body());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while forwarding server command to leader at " + leaderHttpAddress, e);
    }
  }
}
