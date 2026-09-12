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

import com.arcadedb.ContextConfiguration;
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
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * <p>
 * <b>Every forward is bounded.</b> {@code forwardIfReplica} runs on an Undertow worker thread - all four call
 * sites return {@code true} from {@code mustExecuteOnWorkerThread()} - so a leader that accepts the connection
 * and then never answers would hold that worker until the OS tore the socket down, and enough of them would
 * stop the follower serving anything (issue #7507). {@link Transport} therefore gives the client a connect
 * timeout and every request a response deadline, and turns a blown deadline into an HTTP 504 rather than a
 * parked thread.
 */
public final class LeaderCommandForwarder {
  private final HttpServer httpServer;
  private final Transport  transport;

  /**
   * Emits the "a peer forwarded a request here and this node is not the leader either" notice only once
   * (issue #6191). One instance per {@link HttpServer}, so each server in an in-process cluster says it
   * once no matter which route the request came in on.
   */
  private final AtomicBoolean forwardedAgainWarned = new AtomicBoolean(false);

  public LeaderCommandForwarder(final HttpServer httpServer) {
    this.httpServer = httpServer;
    this.transport = new Transport(httpServer.getServer().getConfiguration());
  }

  /**
   * Releases the HTTP client this forwarder dials the leader with. Called when the {@link HttpServer} stops.
   */
  public void close() {
    transport.close();
  }

  /** The bounded transport this forwarder sends through. Package-private, for tests. */
  Transport transport() {
    return transport;
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
    return forwardIfReplica(exchange, user, targetPath, body, false);
  }

  /**
   * As {@link #forwardIfReplica(HttpServerExchange, ServerSecurityUser, String, String)}, choosing which of the
   * two response deadlines the forward is given.
   *
   * @param longRunningCommand true for a forwarded command that legitimately runs for minutes - {@code restore
   *                           backup}, {@code restore database}, {@code import database} - which is given
   *                           {@link GlobalConfiguration#HA_PROXY_LONG_COMMAND_TIMEOUT} instead of
   *                           {@link GlobalConfiguration#HA_PROXY_READ_TIMEOUT}. Applying the short deadline to
   *                           those would abort exactly the operations that most need to reach the leader
   *                           (issue #7507); both deadlines are finite, because either way it is a worker
   *                           thread that waits.
   */
  public ExecutionResponse forwardIfReplica(final HttpServerExchange exchange, final ServerSecurityUser user,
      final String targetPath, final String body, final boolean longRunningCommand) throws IOException {
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

    // Transport.newRequest is the only way this class builds a request, and it attaches the response deadline
    // unconditionally - so no call site can end up issuing an unbounded forward (issue #7507).
    final HttpRequest.Builder builder = transport.newRequest(leaderUri, exchange.getRequestMethod().toString(), body,
        longRunningCommand);

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

    return transport.send(builder.build(), leaderHttpAddress, longRunningCommand);
  }

  /**
   * The bounded HTTP call to the leader: an {@link HttpClient} that cannot wait forever to connect, a response
   * deadline on every request, and HTTP 504 in place of a parked worker thread when the deadline blows
   * (issue #7507).
   * <p>
   * Package-private so the bounds can be asserted without standing up a cluster.
   */
  static final class Transport {
    /**
     * A {@code Duration} of zero or less is rejected by both {@code HttpClient.Builder.connectTimeout} and
     * {@code HttpRequest.Builder.timeout}. Clamping to 1 ms rather than falling back to the default is
     * deliberate: 0 must not become a back door to the unbounded behaviour this class exists to remove.
     */
    private static final long MIN_TIMEOUT_MS = 1L;

    private final ContextConfiguration configuration;
    private final HttpClient           client;

    Transport(final ContextConfiguration configuration) {
      this.configuration = configuration;
      // An HttpClient's connect timeout is fixed at build time, so this one is read once. The response
      // deadlines below are read per request instead, so SET SERVER SETTING moves them without a restart.
      this.client = HttpClient.newBuilder()
          .connectTimeout(millis(configuration.getValueAsLong(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT)))
          .build();
    }

    HttpClient client() {
      return client;
    }

    /**
     * The deadline a forward of this kind gets, re-read from the configuration on every call.
     */
    Duration responseTimeout(final boolean longRunningCommand) {
      return millis(configuration.getValueAsLong(deadlineSetting(longRunningCommand)));
    }

    /**
     * Builds the request to send to the leader, with the response deadline already attached.
     *
     * @param method the HTTP method of the request being forwarded
     * @param body   the body to relay, or null for a request that has none
     */
    HttpRequest.Builder newRequest(final URI leaderUri, final String method, final String body,
        final boolean longRunningCommand) {
      final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(leaderUri)
          .timeout(responseTimeout(longRunningCommand));

      if (body != null) {
        builder.header("Content-Type", "application/json");
        builder.method(method, HttpRequest.BodyPublishers.ofString(body));
      } else
        builder.method(method, HttpRequest.BodyPublishers.noBody());

      return builder;
    }

    /**
     * Sends the request and relays the leader's answer, giving up after the deadline the request carries.
     * <p>
     * The wait is bounded twice over, and it has to be. {@code HttpRequest.timeout} bounds only the wait for the
     * response <b>headers</b>: a peer that answers {@code 200 OK} with a {@code Content-Length} and then stops
     * writing leaves {@code HttpClient.send} blocked for as long as it holds the socket open, deadline or no
     * deadline (measured against a socket that sent headers plus five bytes of a declared hundred - the send did
     * not return). So the exchange is issued asynchronously and awaited with the same deadline applied to the
     * whole of it, body included, and the future is cancelled when that expires. The request-level timeout is
     * kept because it aborts the exchange at the JDK level rather than only releasing this thread.
     * <p>
     * A blown deadline - either one, plus the connect timeout arriving as {@code HttpConnectTimeoutException} -
     * comes back as HTTP 504 naming the leader and the setting that bounds the wait, instead of falling through
     * to the generic 500 an {@link IOException} would produce.
     */
    ExecutionResponse send(final HttpRequest request, final String leaderHttpAddress,
        final boolean longRunningCommand) throws IOException {
      // The deadline that actually applied, taken from the request rather than re-read, so the message cannot
      // quote a number the forward was never given.
      final long deadlineMs = request.timeout().orElseGet(() -> responseTimeout(longRunningCommand)).toMillis();

      final CompletableFuture<HttpResponse<String>> pending = client.sendAsync(request,
          HttpResponse.BodyHandlers.ofString());
      try {
        final HttpResponse<String> response = pending.get(deadlineMs, TimeUnit.MILLISECONDS);
        return new ExecutionResponse(response.statusCode(), response.body());
      } catch (final TimeoutException e) {
        pending.cancel(true);
        return gaveUp(leaderHttpAddress, deadlineMs, longRunningCommand);
      } catch (final ExecutionException e) {
        final Throwable cause = e.getCause();
        // The connect timeout arrives as a subclass of HttpTimeoutException, and it is a different failure with
        // a different setting behind it - saying "did not answer within proxyReadTimeout" when the socket was
        // never established would send the operator to the wrong knob.
        if (cause instanceof HttpConnectTimeoutException)
          return couldNotConnect(leaderHttpAddress);
        if (cause instanceof HttpTimeoutException)
          return gaveUp(leaderHttpAddress, deadlineMs, longRunningCommand);
        if (cause instanceof IOException io)
          throw io;
        if (cause instanceof RuntimeException runtime)
          throw runtime;
        throw new IOException("Error forwarding server command to leader at " + leaderHttpAddress, cause);
      } catch (final InterruptedException e) {
        pending.cancel(true);
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while forwarding server command to leader at " + leaderHttpAddress, e);
      }
    }

    /**
     * The answer to a forward that ran out of time. 504 rather than 500: this node is acting as a gateway and it
     * is the gateway's peer that did not answer.
     */
    private ExecutionResponse gaveUp(final String leaderHttpAddress, final long deadlineMs,
        final boolean longRunningCommand) {
      final String setting = deadlineSetting(longRunningCommand).getKey();
      LogManager.instance().log(this, Level.WARNING,
          "Gave up waiting for the cluster leader at %s to answer a forwarded server command after %,d ms. "
              + "The command may still be running there. Raise %s if the operation is legitimately slower than "
              + "that, otherwise the leader is unresponsive.", leaderHttpAddress, deadlineMs, setting);
      return new ExecutionResponse(504, new JSONObject()
          .put("error", "The cluster leader at " + leaderHttpAddress + " did not answer the forwarded server "
              + "command within " + deadlineMs + " ms (" + setting + "). The command was not executed on this "
              + "node, but it may still be running on the leader: check there before retrying, or raise "
              + setting + " if the operation is legitimately slower than that")
          .toString());
    }

    /**
     * The answer to a forward that never got a connection to the leader at all, within
     * {@link GlobalConfiguration#HA_PROXY_CONNECT_TIMEOUT}. 504 as well - same gateway, earlier failure - but it
     * names the connect timeout and says the command certainly did not run.
     * <p>
     * The JDK reports this as {@code HttpConnectTimeoutException} - a subclass of {@code HttpTimeoutException},
     * carrying a {@code ConnectException} as its own cause - which is why the arm above has to be tested first
     * (measured against a client with an 800 ms connect timeout dialling 192.0.2.1, the RFC 5737 TEST-NET-1
     * address, on a JDK 21 runtime).
     */
    ExecutionResponse couldNotConnect(final String leaderHttpAddress) {
      final String setting = GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT.getKey();
      final long connectMs = client.connectTimeout().map(Duration::toMillis).orElse(0L);
      LogManager.instance().log(this, Level.WARNING,
          "Could not connect to the cluster leader at %s within %,d ms (%s) to forward a server command.",
          leaderHttpAddress, connectMs, setting);
      return new ExecutionResponse(504, new JSONObject()
          .put("error", "Could not connect to the cluster leader at " + leaderHttpAddress + " within " + connectMs
              + " ms (" + setting + ") to forward the server command. The command did not run: retry it, or "
              + "check that the address resolved for the leader is reachable from this node")
          .toString());
    }

    /**
     * Releases the client's selector thread and executor. One client per {@link HttpServer}, so without this an
     * in-process cluster - or a test suite that starts and stops servers - would accumulate them.
     * {@code shutdownNow} rather than {@code close}: the latter waits for in-flight exchanges, and the whole
     * point of the deadlines above is that this node stops waiting on the leader.
     */
    void close() {
      client.shutdownNow();
    }

    private static GlobalConfiguration deadlineSetting(final boolean longRunningCommand) {
      return longRunningCommand ?
          GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT :
          GlobalConfiguration.HA_PROXY_READ_TIMEOUT;
    }

    private static Duration millis(final long configuredMs) {
      return Duration.ofMillis(Math.max(MIN_TIMEOUT_MS, configuredMs));
    }
  }
}
