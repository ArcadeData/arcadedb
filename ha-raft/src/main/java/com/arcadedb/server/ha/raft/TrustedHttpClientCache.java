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
import com.arcadedb.server.ArcadeDBServer;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
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

  private TrustMaterial material;
  private HttpClient   client;

  /**
   * The client for {@code server}, building one when the truststore behind it has changed since the last call.
   * <p>
   * The previous client is closed on a rebuild. {@link HttpClient#close()} is an orderly shutdown that waits for
   * in-flight operations, and the one production caller - {@code RaftHAServer.refreshPeerCapabilities} - is
   * sequential on a single scheduled thread, so on THIS path nothing of the server's own is ever in flight: the
   * thread that would be sending is the thread asking for the client. {@link #close()} carries no such guarantee;
   * see its own note.
   */
  synchronized HttpClient clientFor(final ArcadeDBServer server) throws IOException {
    final TrustMaterial current = trustMaterialOf(server);
    if (client != null && current.equals(material))
      return client;

    final HttpClient previous = client;
    client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(server))
        .build();
    material = current;

    if (previous != null) {
      LogManager.instance().log(this, Level.FINE,
          "The truststore backing the cluster capability probe changed; its HTTPS client was rebuilt");
      previous.close();
    }
    return client;
  }

  /**
   * Releases the cached client, if one was ever built. Called from {@code RaftHAServer.stop()}: the client holds
   * a connection pool and a selector thread, and a JVM that starts and stops many servers - which is what the HA
   * suites do - would otherwise keep one per server that ever probed an HTTPS peer (PR #7314 review).
   * <p>
   * Safe to call more than once, and safe to call on a cache that never built anything.
   * <p>
   * Unlike the rebuild path above, this one can run while a probe is in flight: {@code stopCapabilityMonitor()}
   * ends the refresh with {@code shutdownNow()} and does not wait for the round to unwind, and the request is
   * sent outside this object's monitor. So a straggling probe either delays this close until it finishes - bounded
   * by {@link PeerCapabilityRegistry#PROBE_TIMEOUT_MS} - or fails on the closed client. Both are benign and both
   * are on a server that is shutting down: the failure lands in {@code refreshPeerCapabilities}' own catch and is
   * recorded as unanswered, and the registry's generation stamp drops that write anyway.
   */
  synchronized void close() {
    if (client == null)
      return;
    client.close();
    client = null;
    material = null;
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
