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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.handler.LeaderDial;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.logging.Level;

/**
 * One node's HTTPS client for the peer-capability probe, built from its truststore and rebuilt only when that
 * truststore changes (issue #7301).
 * <p>
 * The probe used to build a fresh {@code SSLContext} - a file read, a certificate-chain parse and an
 * {@code SSLContext.init} - and a fresh {@link HttpClient} for every peer, every
 * {@link PeerCapabilityRegistry#REFRESH_PERIOD_MS}, for the whole life of a leadership; the client was then
 * closed, throwing away its connection pool, so each probe also paid a fresh TLS handshake.
 * <p>
 * <b>Owned per server, not statically.</b> {@code BaseGraphServerTest} and the HA integration suites start
 * several {@link ArcadeDBServer} instances in ONE JVM, each with its own truststore. A single static cache would
 * see every other server's trust material as a change, rebuild on each probe, and - because the request is sent
 * outside this object's monitor - could close a client another server was still sending on. Keyed to the server
 * that owns it, "has the truststore changed" is the question it reads as (issue #7314 review).
 * <p>
 * The change is detected from the truststore's path, its password and the file's modification time and size:
 * what an operator rotating a certificate actually produces. It costs one {@code stat} per probe, against a
 * probe that is about to open a socket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TrustedHttpClientCache {

  /** The trust material a cached client was built from; two equal ones mean the client is still good. */
  private record TrustMaterial(String storePath, String passwordDigest, long lastModified, long length) {
  }

  private       TrustMaterial    material;
  private       HttpClient       client;
  /**
   * Clients replaced by a rotation that were told to shut down but may still be draining an exchange another
   * thread sent before the rotation (issue #8025). Kept so {@link #close()} can release them too; pruned of the
   * ones that have terminated on every rebuild, so repeated rotations do not accumulate them. It is usually empty
   * or holds one client, but it is bounded by the number of retired clients still draining a straggler, not by
   * one: rotations in quick succession while a long exchange is in flight each retire another. Guarded by this.
   */
  private final List<HttpClient> retired = new ArrayList<>(1);
  // Latched by close(), so a probe that outlived stopCapabilityMonitor()'s shutdownNow() cannot have a client
  // built for it that nothing will ever close - the leak this cache exists to prevent (PR #7314 review).
  private       boolean          closed;

  /**
   * The client for {@code server}, building one when the truststore behind it has changed since the last call.
   * <p>
   * <b>The previous client is retired, not closed, on a rebuild</b> (issue #8025). This method is entered
   * concurrently: {@code RaftHAServer} holds the class twice, the leader-forward instance is asked by every HTTP
   * worker thread forwarding a request to the leader ({@code LeaderDial.resolve}), and the peer-RPC instance by
   * {@code PeerAuthSessionQuery} on request threads as well as by the capability probe. So when the truststore
   * rotates, other threads can have exchanges in flight on the client being replaced.
   * <p>
   * {@link HttpClient#close()} used to be called here, and it waits for every one of those exchanges to complete
   * - bounded only by each one's own deadline, {@code arcadedb.ha.proxyCommandTimeout} (one hour at its default)
   * for a forward - while this method holds the monitor every other caller queues on. The shutdown path's
   * {@code LeaderDial.releaseBounded} is not the answer either: it cancels, and these are live forwards whose
   * callers are still waiting on the answer. {@link HttpClient#shutdown()} is the call that fits: it returns at
   * once, refuses new requests, lets the ones in flight finish, and the client terminates on its own when the
   * last one does. The retired client is remembered so {@link #close()} can still release it if a straggler
   * outlives the server.
   * <p>
   * A caller that took the previous client just before the rotation and sends on it just after gets an
   * {@code IOException}, as it would have from {@code close()}: the forward fails exactly as it does when the
   * leader is unreachable, and the window is the few instructions between being handed the client and sending.
   */
  synchronized HttpClient clientFor(final ArcadeDBServer server) throws IOException {
    if (closed)
      // Refused rather than built: the only caller is a refresh round the server has already told to stand down,
      // and it handles this exactly as it handles an unreachable peer - the peer is recorded unanswered, which on
      // a node that is shutting down is both true and harmless. Building one here would hand back a client whose
      // owner has already made its single close() call.
      throw new IOException("the capability probe's HTTPS client cache is closed: this server is shutting down");

    final TrustMaterial current = trustMaterialOf(server);
    if (client != null && current.equals(material))
      return client;

    final HttpClient previous = client;
    // The same connect budget the plain-HTTP forwards get, rather than a hardcoded 5s (issue #7741). An
    // operator tuning arcadedb.ha.proxyConnectTimeout on a TLS cluster changed nothing before this and had no
    // way to tell, and the setting's own description named this path among the ones it governs. Clamped like
    // LeaderDial.newConnectTimeoutBoundedClient does, and read once for the same reason: a built HttpClient's
    // connect timeout cannot change afterwards, so a change needs a restart (or a truststore rotation, which
    // rebuilds the client here).
    client = HttpClient.newBuilder()
        .connectTimeout(connectTimeoutOf(server.getConfiguration()))
        .sslContext(SnapshotInstaller.buildSSLContext(server))
        .build();
    material = current;

    if (previous != null) {
      LogManager.instance().log(this, Level.FINE,
          "The truststore backing the cluster's peer HTTPS client changed; the client was rebuilt");
      // Non-blocking: in-flight exchanges on it finish on their own, and it terminates after the last one.
      previous.shutdown();
      retired.removeIf(HttpClient::isTerminated);
      if (!previous.isTerminated())
        retired.add(previous);
    }
    return client;
  }

  /**
   * The connect budget an HTTPS peer dial gets: {@link GlobalConfiguration#HA_PROXY_CONNECT_TIMEOUT}, the same
   * setting the plain-HTTP forwards read (issue #7741).
   * <p>
   * It used to be a hardcoded 5s here, so an operator tuning the setting on a TLS cluster changed nothing and had
   * no way to tell - the setting's own description named this path among the ones it governs, and the one place
   * that knew better was a comment in the error message that had to avoid naming it. Clamped the way
   * {@link LeaderDial#newConnectTimeoutBoundedClient} clamps it, because a zero or negative connect timeout is
   * not something {@link HttpClient.Builder#connectTimeout} accepts and an outgoing dial must not be unbounded.
   * <p>
   * Read when the client is BUILT, not per request: a {@code java.net.http.HttpClient}'s connect timeout is fixed
   * at build time, so a change takes effect on the next rebuild - a truststore rotation, or a restart.
   * <p>
   * <b>Both users of this cache get it</b>, the leader forward and the peer-capability probe, because the cache is
   * one class held twice by {@code RaftHAServer} and the budget is the same question either way: how long to wait
   * for a peer that is not accepting connections. The probe's PLAIN-HTTP sibling, {@code PeerCapabilityQuery.HTTP},
   * is a JVM-wide static built before any server exists and keeps its own 5s - so an operator who lowers the
   * setting speeds up the probe on a TLS cluster and not on a plaintext one. The setting's description says so
   * rather than this being left for a reader to discover (code review on PR #7747).
   */
  static Duration connectTimeoutOf(final ContextConfiguration configuration) {
    return Duration.ofMillis(Math.max(configuration.getValueAsLong(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT),
        LeaderDial.MIN_FORWARD_TIMEOUT_MS));
  }

  /**
   * Releases the cached client, if one was ever built. Called from {@code RaftHAServer.stop()}: the client holds
   * a connection pool and a selector thread, and a JVM that starts and stops many servers - which is what the HA
   * suites do - would otherwise keep one per server that ever probed an HTTPS peer (PR #7314 review).
   * <p>
   * Safe to call more than once, and safe to call on a cache that never built anything. One-way: a closed cache
   * refuses to build again, since whoever asked after this has no one left to close what it would get.
   * <p>
   * Like the rebuild path above, this one can run while a peer dial is in flight: {@code stopCapabilityMonitor()}
   * ends the refresh with {@code shutdownNow()} and does not wait for the round to unwind, and the request is
   * sent outside this object's monitor. So a straggler either delays this close or fails on the closed client.
   * Both are benign and both are on a server that is shutting down: the failure lands in
   * {@code refreshPeerCapabilities}' own catch and is recorded as unanswered, and the registry's generation stamp
   * drops that write anyway.
   * <p>
   * <b>The delay is bounded here rather than by the straggler</b> (issue #7985). {@code HttpClient.close()} used
   * to do this, and it waits for every submitted operation to complete. That reads as harmless for the
   * capability probe, whose round is bounded by {@link PeerCapabilityRegistry#PROBE_TIMEOUT_MS} - but
   * {@code RaftHAServer} holds this class TWICE, and the second instance carries the HTTPS half of the leader
   * forwards, whose bound is {@code arcadedb.ha.proxyCommandTimeout}: one hour at its default. This method is
   * {@code synchronized}, so that wait was held under the monitor as well. {@link LeaderDial#releaseBounded}
   * cancels what is in flight and waits seconds, not deadlines, for the termination.
   */
  synchronized void close() {
    closed = true;
    // A client retired by a rotation may still be draining a straggler; it is this cache's to release too, or it
    // holds its selector thread until that straggler's own deadline (issue #8025).
    for (final HttpClient r : retired)
      LeaderDial.releaseBounded(r);
    retired.clear();
    if (client == null)
      return;
    LeaderDial.releaseBounded(client);
    client = null;
    material = null;
  }

  /** How many retired clients are still being tracked; for tests (issue #8025). */
  synchronized int retiredCount() {
    return retired.size();
  }

  private static TrustMaterial trustMaterialOf(final ArcadeDBServer server) {
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
    return new TrustMaterial(storePath, digestOf(storePassword), lastModified, length);
  }

  /**
   * A digest of the truststore password, never the password itself: this only ever has to answer "did it
   * change", and a 32-bit {@code hashCode} answering that would - however unlikely - keep serving a client built
   * from the previous password on a collision. Nothing is derived from or published from this value.
   */
  private static String digestOf(final String password) {
    if (password == null)
      return "";
    try {
      return HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256").digest(password.getBytes(StandardCharsets.UTF_8)));
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is mandated by every Java SE implementation; a JVM without it cannot have loaded the TLS stack
      // this client is for.
      throw new IllegalStateException("SHA-256 is not available in this JVM", e);
    }
  }
}
