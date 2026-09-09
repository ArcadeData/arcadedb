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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashSet;
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

  /** What a peer says about itself. {@code version} is operator-facing only; nothing decides on it. */
  public record Advertisement(String peerId, String version, Set<String> capabilities) {
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  /** One-time warning that SSL is enabled but the probe fell back to plain HTTP for lack of an HTTPS address. */
  private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = new AtomicBoolean(false);

  /** The HTTPS client and the truststore it was built from; guarded by this class's monitor. */
  private static TrustedClient trustedClient;

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
    return ask(Objects.requireNonNull(expectedPeerId, "expectedPeerId"), httpAddr, httpsAddr, clusterToken, timeoutMs,
        server);
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
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server)
      throws IOException, InterruptedException {
    return ask(null, httpAddr, httpsAddr, clusterToken, timeoutMs, server);
  }

  private static Advertisement ask(final String expectedPeerId, final String httpAddr, final String httpsAddr,
      final String clusterToken, final long timeoutMs, final ArcadeDBServer server)
      throws IOException, InterruptedException {

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
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    builder.header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER);
    final HttpRequest request = builder.build();

    if (url.startsWith("https://"))
      return parse(expectedPeerId, trustedHttpClient(server).send(request, HttpResponse.BodyHandlers.ofString()), url);
    return parse(expectedPeerId, HTTP.send(request, HttpResponse.BodyHandlers.ofString()), url);
  }

  /**
   * The client carrying the cluster trust context, built once and reused until the truststore changes
   * (issue #7301).
   * <p>
   * This used to build a fresh {@code SSLContext} - a file read, a certificate-chain parse and an
   * {@code SSLContext.init} - and a fresh {@link HttpClient} for every probe: once per peer, every
   * {@link PeerCapabilityRegistry#REFRESH_PERIOD_MS}, for the whole life of the leadership. That is a steady,
   * avoidable cost on every TLS-enabled cluster, and it also threw away the connection pool between rounds, so
   * each probe paid a fresh TLS handshake as well.
   * <p>
   * Rebuilt when the truststore this node reads has changed: its path, its password, or the file's modification
   * time or size. That is the condition an operator rotating a certificate actually produces, and it is checked
   * per probe - the cost of the check is one {@code stat}, against a probe that is about to open a socket.
   * <p>
   * The old client is closed on a rebuild. {@link HttpClient#close()} is an orderly shutdown that waits for
   * in-flight operations, and the one production caller - the capability fan-out - is sequential on a single
   * scheduled thread, so nothing of its own is ever in flight here.
   */
  // @VisibleForTesting
  static synchronized HttpClient trustedHttpClient(final ArcadeDBServer server) throws IOException {
    final String storePath = server != null
        ? server.getConfiguration().getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE) : null;
    final String storePassword = server != null
        ? server.getConfiguration().getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD) : null;

    long lastModified = -1L;
    long length = -1L;
    if (storePath != null && !storePath.isBlank()) {
      final File store = new File(storePath);
      lastModified = store.lastModified();
      length = store.length();
    }

    // The password is fingerprinted rather than kept: this cache only ever has to answer "did it change".
    final TrustedClient current = new TrustedClient(storePath, storePassword == null ? 0 : storePassword.hashCode(),
        lastModified, length, null);

    final TrustedClient cached = trustedClient;
    if (cached != null && cached.sameTrustMaterialAs(current))
      return cached.client();

    final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(server))
        .build();
    trustedClient = current.withClient(client);
    if (cached != null) {
      LogManager.instance().log(PeerCapabilityQuery.class, Level.FINE,
          "The truststore backing the cluster capability probe changed; its HTTPS client was rebuilt");
      cached.client().close();
    }
    return client;
  }

  /** The cached client and the trust material it was built from, so a rotation is noticed and nothing else is. */
  private record TrustedClient(String storePath, int passwordFingerprint, long lastModified, long length,
                               HttpClient client) {
    private boolean sameTrustMaterialAs(final TrustedClient other) {
      return Objects.equals(storePath, other.storePath) && passwordFingerprint == other.passwordFingerprint
          && lastModified == other.lastModified && length == other.length;
    }

    private TrustedClient withClient(final HttpClient built) {
      return new TrustedClient(storePath, passwordFingerprint, lastModified, length, built);
    }
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

    return new Advertisement(peerId, json.getString("version", ""), capabilities);
  }
}
