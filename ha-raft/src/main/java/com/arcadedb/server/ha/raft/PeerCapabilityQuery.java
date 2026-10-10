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
import com.arcadedb.server.http.handler.LeaderDial;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Asks one peer what wire-format sections it can decode, by calling its
 * {@code POST /api/v1/cluster/capabilities} RPC (issue #7219).
 * <p>
 * Transport, authentication and endpoint selection are {@link LeaderDatabaseQuery}'s, because this is the same
 * kind of call: the {@code X-ArcadeDB-Cluster-Token} + {@code X-ArcadeDB-Forwarded-User} pair every peer-to-peer
 * cluster RPC uses, the peer's HTTPS endpoint preferred when {@code arcadedb.ssl.enabled} is set, and the same
 * one-time warning when that preference cannot be honoured and the token goes over plain HTTP instead. That
 * warning carries more weight here than at its origin: this query repeats for as long as the node leads, so the
 * fallback is a standing condition rather than one request.
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
 * <p>
 * <b>Which is why the shared address can be dialled anyway.</b> {@link #fetchFromSharedEndpoint} asks the same
 * question of an address that identifies no single peer, and it is safe for the same reason the check above works:
 * the answer NAMES its author, so the caller binds it to whoever answered rather than to whoever it was meant for.
 * That turns "several peers collapse onto one address" from a permanent unknown into an answer for the one peer
 * that really is there, on a cluster where the negotiation would otherwise never run at all (issue #7256). It
 * works only because this request is read-only and its reply is self-identifying; nothing that acts on the peer it
 * addressed may take this route.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PeerCapabilityQuery {

  /**
   * What a peer says about itself. {@code version} is operator-facing only; nothing decides on it.
   * <p>
   * {@code serviceGap} is the peer's own {@code ArcadeStateMachine#hasLeaderServiceGap()} (issue #8665): true when the
   * peer, were it the leader, would hold a database it cannot serve. It is NOT a capability - it moves with the
   * peer's state, not its build - and it rides on this poll only because the leader already asks every peer this
   * question every few seconds, over an authenticated route that binds the answer to its author. A peer that predates
   * the field omits it, which reads as no gap: the pre-#8665 behaviour, where a peer's gap was invisible.
   * <p>
   * {@code quarantined} is the set of databases the peer holds quarantined (issue #9553), carried for the same reason:
   * a node that has quarantined a database can then tell whether any voter still holds a copy it could resync from.
   * A peer that predates the field omits it, which reads as "nothing quarantined" - so the all-voters-quarantined
   * alert never fires on its account, the safe side for an alert that tells the operator to force-accept a copy.
   * <p>
   * {@code peerHttpAddresses} are the HTTP addresses the peer holds for the OTHER members, declared by an operator or
   * confirmed by a probe they answered (issue #9255). They are relayed, not believed: the caller offers each one as a
   * candidate and records it only once the member it names answers a probe on it. A peer that predates the field
   * omits it, which relays nothing.
   */
  public record Advertisement(String peerId, String version, Set<String> capabilities, boolean serviceGap,
      Set<String> quarantined, Map<String, String> peerHttpAddresses) {
    public Advertisement {
      if (quarantined == null)
        quarantined = Set.of();
      if (peerHttpAddresses == null)
        peerHttpAddresses = Map.of();
    }

    public Advertisement(final String peerId, final String version, final Set<String> capabilities) {
      this(peerId, version, capabilities, false, Set.of(), Map.of());
    }

    public Advertisement(final String peerId, final String version, final Set<String> capabilities,
        final boolean serviceGap) {
      this(peerId, version, capabilities, serviceGap, Set.of(), Map.of());
    }

    public Advertisement(final String peerId, final String version, final Set<String> capabilities,
        final boolean serviceGap, final Set<String> quarantined) {
      this(peerId, version, capabilities, serviceGap, quarantined, Map.of());
    }
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  /** One-time warning that SSL is enabled but the probe fell back to plain HTTP for lack of an HTTPS address. */
  private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = new AtomicBoolean(false);

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
   * @param httpsClients   the caller's HTTPS client cache, used only when the HTTPS endpoint is the one dialled.
   *
   * @throws IOException          on transport error, a non-200 response (which is what a peer without this route
   *                              answers), or an advertisement that names another peer.
   * @throws InterruptedException if the calling thread is interrupted while waiting.
   */
  public static Advertisement fetch(final String expectedPeerId, final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server,
      final TrustedHttpClientCache httpsClients) throws IOException, InterruptedException {
    return fetch(expectedPeerId, httpAddr, httpsAddr, clusterToken, timeoutMs, server, httpsClients, null);
  }

  /**
   * As {@link #fetch(String, String, String, String, long, ArcadeDBServer, TrustedHttpClientCache)}, sending
   * {@code caller} as request headers: the calling node's own id and HTTP endpoint, which the peer offers as a candidate
   * address for it (issue #9255). A peer that predates them ignores the headers.
   *
   * @param caller the calling node's self-description as header name to value, or {@code null} to send none
   */
  public static Advertisement fetch(final String expectedPeerId, final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server,
      final TrustedHttpClientCache httpsClients, final Map<String, String> caller) throws IOException, InterruptedException {
    return ask(Objects.requireNonNull(expectedPeerId, "expectedPeerId"), httpAddr, httpsAddr, clusterToken, timeoutMs,
        server, httpsClients, caller);
  }

  /**
   * Asks whoever answers at {@code httpAddr} what it can decode, accepting the advertisement whatever peer it
   * names (issue #7256).
   * <p>
   * For an address that {@link PeerDialAddress} withheld because two or more peers resolve to it. The caller gets
   * back an {@link Advertisement#peerId()} it MUST check against its own peer list and record the answer against -
   * never against the peer it happened to be resolving when it found the address. Every other guarantee is
   * {@link #fetch}'s: same route, same authentication, same timeout, and a non-200 still means "no".
   */
  public static Advertisement fetchFromSharedEndpoint(final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server,
      final TrustedHttpClientCache httpsClients) throws IOException, InterruptedException {
    return fetchFromSharedEndpoint(httpAddr, httpsAddr, clusterToken, timeoutMs, server, httpsClients, null);
  }

  /** As {@link #fetchFromSharedEndpoint(String, String, String, long, ArcadeDBServer, TrustedHttpClientCache)}, sending {@code caller}. */
  public static Advertisement fetchFromSharedEndpoint(final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server,
      final TrustedHttpClientCache httpsClients, final Map<String, String> caller) throws IOException, InterruptedException {
    return ask(null, httpAddr, httpsAddr, clusterToken, timeoutMs, server, httpsClients, caller);
  }

  private static Advertisement ask(final String expectedPeerId, final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server,
      final TrustedHttpClientCache httpsClients, final Map<String, String> caller) throws IOException, InterruptedException {

    final boolean useSSL = server != null && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String url = chooseUrl(httpAddr, httpsAddr, useSSL);
    if (url == null)
      throw new IOException("no peer address available for a capability query");
    if (useSSL && !url.startsWith("https://") && PLAIN_HTTP_FALLBACK_WARNED.compareAndSet(false, true))
      // The same one-time warning LeaderDatabaseQuery emits on the same fallback, and it matters MORE here: that
      // query runs at a join or on an operator's request, while this one repeats every
      // PeerCapabilityRegistry.REFRESH_PERIOD_MS for as long as the node leads. On a cluster that enables SSL but
      // declares no 'https' ports, the cluster token would otherwise go out in clear text on this RPC
      // indefinitely with nothing in the log to point at it. Named for the peer that first hit it, because the
      // remedy is per-peer configuration; one line, because the cause is a configuration fact and not an event.
      LogManager.instance().log(PeerCapabilityQuery.class, Level.WARNING,
          "SSL is enabled but no HTTPS address is known for peer '%s'; its capability query - and the cluster "
              + "token it carries - go over plain HTTP. Declare each node's 'https' port in %s.",
          expectedPeerId != null ? expectedPeerId : httpAddr, GlobalConfiguration.HA_SERVER_LIST.getKey());

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    if (caller != null)
      for (final Map.Entry<String, String> header : caller.entrySet())
        builder.header(header.getKey(), header.getValue());
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    builder.header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER);
    final HttpRequest request = builder.build();

    // The client carrying the cluster trust context, built once per server and reused until its truststore changes
    // (issue #7301). Owned by the caller rather than by this class, so several servers in one JVM - the shape every
    // HA test takes - cannot invalidate and close each other's (issue #7314 review).
    final HttpClient client = url.startsWith("https://") ? httpsClients.clientFor(server) : HTTP;
    // Bounded by sendBounded rather than by the request timeout alone, which on JDK 21-25 stops at the response
    // headers: a peer that stalls inside its body would otherwise park the probe with no bound at all (issue #8472).
    return parse(expectedPeerId, LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(),
        timeoutMs), url);
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
    checkStatus(response.statusCode(), url);
    return parse(expectedPeerId, response.body(), url);
  }

  /**
   * Refuses any status but 200, raising a 404 as {@link RouteMissingException} so the caller can tell a peer whose
   * build has no capability route from one it could not get a usable answer from (issue #8655). The message is the
   * same for both, so nothing an operator reads changes. Package-private and pure for unit testing.
   */
  // @VisibleForTesting
  static void checkStatus(final int statusCode, final String url) throws IOException {
    if (statusCode == 200)
      return;
    final String message = "capability query to " + url + " returned HTTP " + statusCode;
    if (statusCode == 404)
      throw new RouteMissingException(message);
    throw new IOException(message);
  }

  /**
   * The peer answered, and its answer is that the capability route does not exist: a build that predates it
   * (issue #8655). Unlike every other probe failure this is a property of the PEER rather than of the path to it, so
   * every node that asks gets it - which is what lets a follower report it as the leader's verdict too.
   */
  public static final class RouteMissingException extends IOException {
    public RouteMissingException(final String message) {
      super(message);
    }
  }

  /**
   * Reads one advertisement document, refusing one that names a peer other than {@code expectedPeerId}. A
   * {@code null} {@code expectedPeerId} accepts whatever peer answered - the shared-endpoint route, whose caller
   * binds the answer to the peer the document names (issue #7256).
   * Package-private and pure for unit testing.
   */
  // @VisibleForTesting
  static Advertisement parse(final String expectedPeerId, final String body, final String url) throws IOException {
    final JSONObject json = new JSONObject(body);
    final String peerId = json.getString("peerId", "");
    if (peerId.isEmpty())
      throw new IOException("capability query to " + url + " was answered by a document that names no peer");
    if (expectedPeerId != null && !peerId.equals(expectedPeerId))
      throw new IOException("capability query to " + url + " for peer '" + expectedPeerId
          + "' was answered by peer '" + peerId + "'; the address does not identify the peer it was meant for "
          + "(declare each node's 'http' port explicitly in " + GlobalConfiguration.HA_SERVER_LIST.getKey() + ")");

    final Set<String> capabilities = new LinkedHashSet<>();
    final JSONArray array = json.has("capabilities") ? json.getJSONArray("capabilities") : new JSONArray();
    for (int i = 0; i < array.length(); i++)
      capabilities.add(array.getString(i));

    final JSONArray quarantinedArray = json.has(PostCapabilitiesHandler.QUARANTINED) ?
        json.getJSONArray(PostCapabilitiesHandler.QUARANTINED) : null;
    Set<String> quarantined = Set.of();
    if (quarantinedArray != null && quarantinedArray.length() > 0) {
      quarantined = new LinkedHashSet<>();
      for (int i = 0; i < quarantinedArray.length(); i++)
        quarantined.add(quarantinedArray.getString(i));
    }

    return new Advertisement(peerId, json.getString("version", ""), capabilities,
        json.getBoolean(PostCapabilitiesHandler.SERVICE_GAP, false), quarantined, readPeerHttpAddresses(json, url));
  }

  /**
   * The {@link PostCapabilitiesHandler#PEER_HTTP_ADDRESSES} a peer relayed, keeping only well-formed {@code host:port}
   * values (issue #9255). A malformed entry is dropped rather than failing the whole answer: the capabilities it came
   * with are still the peer's, and the relay is only ever a hint.
   */
  private static Map<String, String> readPeerHttpAddresses(final JSONObject json, final String url) {
    final JSONObject relayed = json.has(PostCapabilitiesHandler.PEER_HTTP_ADDRESSES) ?
        json.getJSONObject(PostCapabilitiesHandler.PEER_HTTP_ADDRESSES, null) : null;
    if (relayed == null || relayed.length() == 0)
      return Map.of();
    final Map<String, String> addresses = new LinkedHashMap<>();
    for (final String peerId : relayed.keySet()) {
      final String address = relayed.getString(peerId, "");
      if (peerId.isEmpty() || !isPeerAddress(address)) {
        LogManager.instance().log(PeerCapabilityQuery.class, Level.FINE,
            "Ignoring the HTTP address relayed for peer '%s' by %s: '%s' is not a host:port", peerId, url, address);
        continue;
      }
      addresses.put(peerId, address);
    }
    return addresses;
  }

  /** Whether {@code address} passes the {@code host:port} rules every other peer address of this module is held to. */
  static boolean isPeerAddress(final String address) {
    if (address == null || address.isEmpty())
      return false;
    try {
      RaftPeerAddressResolver.validatePeerAddress(address);
      return true;
    } catch (final RuntimeException e) {
      return false;
    }
  }
}
