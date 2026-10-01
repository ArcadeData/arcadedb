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
import org.apache.ratis.server.protocol.TermIndex;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

  /**
   * The peer's database list plus its own latest Raft snapshot {@link TermIndex} (issue #8360), i.e. the boundary
   * {@link ArcadeStateMachine#takeSnapshot()} last checkpointed - the same value {@code LogAppender.getPreviousLog()}
   * falls back to on the leader when it answers an {@code AppendEntries} whose {@code previousIndex} is no longer in
   * its own retained Raft log. {@code snapshotTermIndex} is {@code null} when the peer has not taken a Raft snapshot
   * yet (a young cluster before its first compaction).
   */
  public record BootstrapState(List<DatabaseInfo> databases, TermIndex snapshotTermIndex) {
  }

  /**
   * The address a query dialled was answered by a node other than the peer it was meant for (issue #8658), or by one
   * that names no peer. Its own type so a caller that would otherwise degrade to a lesser read on a failure (the
   * reconciler falling back from the full listing to the marker alone) can tell that the next read would be
   * answered by the same stranger.
   */
  public static final class WrongPeerAnsweredException extends IOException {
    public WrongPeerAnsweredException(final String message) {
      super(message);
    }
  }

  /** The chosen endpoint and scheme for a query; package-private so scheme selection is unit-testable. */
  record Endpoint(String url, boolean https) {
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private LeaderDatabaseQuery() {
  }

  /**
   * Synchronously queries a peer for the list of databases it holds.
   *
   * @param expectedPeerId the id of the peer the address is meant to reach. The answer names the node that wrote it,
   *                     and one written by any other node is refused with {@link WrongPeerAnsweredException} (issue
   *                     #8658): an address that resolves to another node would otherwise hand that node's databases
   *                     or snapshot marker back as this peer's. {@code null} accepts any answer.
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
  public static BootstrapState fetch(final String expectedPeerId, final String httpAddr, final String httpsAddr, final String clusterToken,
      final long timeoutMs, final ArcadeDBServer server) throws IOException, InterruptedException {
    return send(expectedPeerId, httpAddr, httpsAddr, clusterToken, timeoutMs, server, "{}");
  }

  /**
   * Asks a peer for its latest Raft snapshot {@link TermIndex} only (issue #8374), with {@code markerOnly} set so the
   * peer skips fingerprinting every database. The returned {@link BootstrapState#databases()} is empty from a peer
   * that honours the flag; a peer that predates it ignores the flag and answers in full, which is still correct.
   */
  public static BootstrapState fetchSnapshotMarker(final String expectedPeerId, final String httpAddr,
      final String httpsAddr, final String clusterToken, final long timeoutMs, final ArcadeDBServer server)
      throws IOException, InterruptedException {
    return send(expectedPeerId, httpAddr, httpsAddr, clusterToken, timeoutMs, server,
        new JSONObject().put(PostBootstrapStateHandler.MARKER_ONLY, true).toString());
  }

  private static BootstrapState send(final String expectedPeerId, final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server, final String body) throws IOException, InterruptedException {

    final boolean useSSL = server != null && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final Endpoint endpoint = chooseEndpoint(httpAddr, httpsAddr, useSSL);
    if (endpoint == null)
      throw new IOException("no peer address available for bootstrap-state query");
    if (useSSL && !endpoint.https())
      // One latch across every dial that can fall back this way, rather than one per dial: they say the same
      // thing and are fixed by the same setting, so three copies would only bury the first (issue #7546).
      PlainHttpFallbackNotice.sayOnce(LeaderDatabaseQuery.class, "querying its database list");

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(endpoint.url()))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body));
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
        return parse(LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), timeoutMs),
            endpoint.url(), expectedPeerId);
      }
    }
    // Bounded over the whole exchange, body included (issue #8325): on JDK 21-25 the request timeout stops at the
    // response headers, so a peer that stalled inside its body parked the caller unbounded. The request timeout stays:
    // on JDK 26+ it covers the same span with the same value, and either one firing is the same HttpTimeoutException.
    return parse(LeaderDial.sendBounded(HTTP, request, HttpResponse.BodyHandlers.ofString(), timeoutMs), endpoint.url(),
        expectedPeerId);
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

  private static BootstrapState parse(final HttpResponse<String> resp, final String url, final String expectedPeerId)
      throws IOException {
    if (resp.statusCode() != 200)
      throw new IOException("bootstrap-state query to " + url + " returned HTTP " + resp.statusCode());
    return parseBody(resp.body(), expectedPeerId, url);
  }

  /**
   * Refuses an answer that was not written by {@code expectedPeerId} (issue #8658), the check
   * {@link PeerCapabilityQuery#parse} makes for capabilities. Every build that serves the endpoint names itself in
   * {@code peerId}, so an answer that names nobody is refused too. A {@code null} {@code expectedPeerId} accepts any.
   * Also used by {@link BootstrapElection#fetchBootstrapState}, which reads the same endpoint.
   */
  static void requireAnsweredBy(final JSONObject json, final String expectedPeerId, final String url)
      throws WrongPeerAnsweredException {
    if (expectedPeerId == null)
      return;
    final String answeredBy = json.getString("peerId", "");
    if (!answeredBy.equals(expectedPeerId))
      throw new WrongPeerAnsweredException("bootstrap-state query to " + url + " for peer '" + expectedPeerId + "' was answered by "
          + (answeredBy.isEmpty() ? "a node that names no peer" : "peer '" + answeredBy + "'")
          + "; the address does not identify the peer it was meant for (declare each node's 'http' port explicitly in "
          + GlobalConfiguration.HA_SERVER_LIST.getKey() + ")");
  }

  /** The JSON-decoding half of {@link #parse}, split out so the wire format is unit-testable without HTTP. */
  static BootstrapState parseBody(final String body, final String expectedPeerId, final String url) throws IOException {
    final JSONObject json = new JSONObject(body);
    requireAnsweredBy(json, expectedPeerId, url);
    final JSONArray dbs = json.getJSONArray("databases");
    final List<DatabaseInfo> out = new ArrayList<>(dbs.length());
    for (int i = 0; i < dbs.length(); i++) {
      final JSONObject db = dbs.getJSONObject(i);
      out.add(new DatabaseInfo(db.getString("name"), db.getLong("lastTxId", -1L)));
    }

    // snapshotIndex is absent/-1 when the peer has not taken a Raft snapshot yet (issue #8360).
    final long snapshotIndex = json.getLong("snapshotIndex", -1L);
    final TermIndex snapshotTermIndex = snapshotIndex >= 0
        ? TermIndex.valueOf(json.getLong("snapshotTerm", 0L), snapshotIndex)
        : null;

    return new BootstrapState(out, snapshotTermIndex);
  }
}
