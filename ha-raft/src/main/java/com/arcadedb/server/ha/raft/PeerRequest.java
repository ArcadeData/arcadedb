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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * The one way a peer-to-peer dial in this module starts its request (issue #9999): the credential it presents and the
 * notice it owes the operator when it goes out in clear text, in one place.
 * <p>
 * <b>The credential.</b> Two headers that only mean anything together. {@code X-ArcadeDB-Cluster-Token} proves the HOP
 * - it is the shared secret only cluster members hold - and that is all it proves; {@code X-ArcadeDB-Forwarded-User}
 * names the principal, and {@code AbstractServerHttpHandler} reads it only inside the branch a valid token opens. Sent
 * apart, the first is a hop with no identity and the second is a claim with no proof. A blank or absent token attaches
 * NEITHER: the correct request for a cluster configured without one carries no credentials at all, which a server that
 * does not require authentication accepts and one that does refuses.
 * <p>
 * <b>The notice.</b> When {@code arcadedb.ssl.enabled} is set and a dial still has to go out over the plain listener,
 * because no {@code https} endpoint resolves for the peer it is reaching, the cluster token goes on the wire in clear
 * text. Falling back rather than refusing is deliberate and is the package-wide rule: the plain listener is the one
 * that is always bound, and an SSL cluster that never declared the optional 5th field of {@code arcadedb.ha.serverList}
 * would otherwise be unable to bootstrap, resync or be administered at all. But silence is not part of that rule: an
 * operator who believes the cluster is encrypted end to end has no other way to discover the gap (issues #7546,
 * #7563). One latch for the whole family, not one per dial: every dial says the same thing and is fixed by the same
 * setting, so several copies would only push the first out of the operator's scrollback.
 * <p>
 * <b>Why one builder rather than two helpers.</b> Issue #7837 was one dial spelling the credential by hand and spelling
 * it wrong. The two helpers extracted after it - one for the headers, one for the notice - were adopted by a minority
 * of the dials each, and the sites left alone drifted (a dial with the token and no principal, one with the principal
 * spelled as a literal, most of them silent on the fallback). Every dial now builds its request here, and
 * {@code Issue9999PeerDialsGoThroughOneBuilderTest} fails the build when a request is built or either header is
 * spelled anywhere else in this module.
 */
final class PeerRequest {

  /** Proves the hop: the shared secret only cluster members hold. */
  static final String CLUSTER_TOKEN_HEADER   = "X-ArcadeDB-Cluster-Token";
  /** Names the principal the hop acts for; read by the receiving node only under a valid {@link #CLUSTER_TOKEN_HEADER}. */
  static final String FORWARDED_USER_HEADER  = "X-ArcadeDB-Forwarded-User";

  private static final AtomicBoolean WARNED = new AtomicBoolean(false);

  private PeerRequest() {
  }

  /**
   * A request to {@code url} presenting the cluster credential as the root principal every cluster-internal RPC acts
   * as, and saying the fallback notice if this dial is the first to go out in clear text on an SSL cluster.
   *
   * @param url          the full URL the dial chose, scheme included
   * @param sslEnabled   whether this node has {@code arcadedb.ssl.enabled} set
   * @param dial         the class of the dial, so the notice is attributed where it happened
   * @param what         what the dial is doing, phrased to follow "no HTTPS address is known for a peer; " - e.g.
   *                     {@code "probing its bootstrap-state"}
   * @param clusterToken this node's cluster token, or {@code null}/blank when the cluster has none
   */
  static HttpRequest.Builder newRequest(final String url, final boolean sslEnabled, final Class<?> dial,
      final String what, final String clusterToken) {
    return newRequest(url, sslEnabled, dial, what, clusterToken, RaftHAServer.FORWARDED_ROOT_USER);
  }

  /**
   * {@link #newRequest(String, boolean, Class, String, String)} naming a principal other than root, for a dial that
   * relays the caller's own identity rather than acting for the cluster.
   */
  static HttpRequest.Builder newRequest(final String url, final boolean sslEnabled, final Class<?> dial,
      final String what, final String clusterToken, final String forwardedUser) {
    final URI uri = URI.create(url);
    final boolean credentials = carriesCredentials(clusterToken, forwardedUser, url);
    noticePlainFallback(sslEnabled, credentials, uri, dial, what);
    final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri);
    if (credentials) {
      builder.header(CLUSTER_TOKEN_HEADER, clusterToken);
      builder.header(FORWARDED_USER_HEADER, forwardedUser);
    }
    return builder;
  }

  /**
   * The {@link HttpURLConnection} form of {@link #newRequest(String, boolean, Class, String, String, String)}, for the
   * dials that stream a body ({@code SnapshotInstaller}) or predate {@code HttpClient}: opened, not connected, with the
   * credential attached and the notice said. The caller still installs the SSL socket factory and the method.
   */
  static HttpURLConnection openConnection(final String url, final boolean sslEnabled, final Class<?> dial,
      final String what, final String clusterToken, final String forwardedUser) throws IOException {
    final URI uri;
    try {
      uri = new URI(url);
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid peer URL: " + url, e);
    }
    final boolean credentials = carriesCredentials(clusterToken, forwardedUser, url);
    noticePlainFallback(sslEnabled, credentials, uri, dial, what);
    final HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
    if (credentials) {
      connection.setRequestProperty(CLUSTER_TOKEN_HEADER, clusterToken);
      connection.setRequestProperty(FORWARDED_USER_HEADER, forwardedUser);
    }
    return connection;
  }

  /** Whether a request built here carries the credential: what gates any header a peer honors only under the token. */
  static boolean hasToken(final String clusterToken) {
    return clusterToken != null && !clusterToken.isBlank();
  }

  /**
   * Whether the request carries the pair. A token with no principal is refused rather than sent as half a pair or
   * patched up as root: a dial that relays a caller's identity and lost it must fail, never act with more authority.
   */
  private static boolean carriesCredentials(final String clusterToken, final String forwardedUser, final String url) {
    if (!hasToken(clusterToken))
      return false;
    if (forwardedUser == null || forwardedUser.isBlank())
      throw new IllegalArgumentException(
          "A peer request carrying the cluster token must name the principal it acts for (" + url + ")");
    return true;
  }

  /** Said only when there is a token to leak: a plain dial with no credentials puts no shared secret on the wire. */
  private static void noticePlainFallback(final boolean sslEnabled, final boolean credentials, final URI uri,
      final Class<?> dial, final String what) {
    if (sslEnabled && credentials && "http".equalsIgnoreCase(uri.getScheme()))
      // Host and port only: never the authority, which would carry any user-info a URL held
      sayOnce(dial, what + " (peer at " + uri.getHost() + ":" + uri.getPort() + ")");
  }

  /**
   * Says the fallback notice, if it has not been said yet in this JVM.
   *
   * @return {@code true} when this call was the one that logged it, {@code false} when it had already been said
   */
  static boolean sayOnce(final Class<?> dial, final String what) {
    if (!WARNED.compareAndSet(false, true))
      return false;
    LogManager.instance().log(dial, Level.WARNING,
        "SSL is enabled but no HTTPS address is known for a peer; %s over plain HTTP, which puts the cluster token "
            + "on the wire in clear text. Declare each node's 'https' port in %s to avoid it. This notice is logged "
            + "once per JVM, and covers every peer-to-peer dial that falls back the same way.",
        what, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return true;
  }

  /** Whether the notice has been said in this JVM. Test-only. */
  // @VisibleForTesting
  static boolean saidForTests() {
    return WARNED.get();
  }

  /** Re-arms the latch. Test-only: a static latch outlives the test that tripped it. */
  // @VisibleForTesting
  static void rearmForTests() {
    WARNED.set(false);
  }
}
