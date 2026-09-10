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
package com.arcadedb.server.http;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

/**
 * Resolves, renews and revokes authentication sessions across the nodes of a cluster (issue #7424).
 * <p>
 * A login token lives on the node that answered {@code /api/v1/login}. Behind a load balancer the next request
 * lands on another node, which has never seen the token and used to answer 401. The token now names its issuer
 * ({@code AU-<server name>-<uuid>}), so that node can ask exactly one peer - through the same cluster-token channel
 * every peer-to-peer RPC uses - and install a local copy of the session. The copy is a lease: it is confirmed with
 * the issuer every {@link HttpAuthSessionManager#getRemoteRenewalIntervalMs()}, which also counts as activity on
 * the issuer, so a session in use anywhere never idles out at its source. An issuer that no longer holds the
 * session (logout, restart) revokes the copy at the next renewal; an issuer that cannot be reached keeps the copy
 * alive for at most the idle timeout, after which the copy is dropped rather than served on trust.
 * <p>
 * Bounded on purpose: an unauthenticated caller can make this node dial a peer by presenting a made-up token, so
 * at most {@link #MAX_CONCURRENT_LOOKUPS} lookups are in flight at once (past that a miss is refused without
 * dialling), and a token a peer refused is remembered for {@link #REFUSAL_CACHE_TTL_MS} so a repeat costs nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ClusterAuthSessionResolver {
  static final int  MAX_CONCURRENT_LOOKUPS = 16;
  static final long REFUSAL_CACHE_TTL_MS   = 5_000L;
  static final int  REFUSAL_CACHE_MAX      = 4_096;

  private final ArcadeDBServer                server;
  private final HttpAuthSessionManager        sessions;
  private final Semaphore                     inFlight = new Semaphore(MAX_CONCURRENT_LOOKUPS);
  private final ConcurrentHashMap<String, Long> refused  = new ConcurrentHashMap<>();

  public ClusterAuthSessionResolver(final ArcadeDBServer server, final HttpAuthSessionManager sessions) {
    this.server = server;
    this.sessions = sessions;
  }

  /**
   * Called on a token this node does not hold. Asks the issuer the token names and, when it vouches for the
   * session, installs a local copy.
   *
   * @return the local copy, or {@code null} when the token is not honoured here: it names no issuer, names this
   * node (which does not hold it, so it expired), names a node that is not a member, the issuer refused it or
   * could not be asked, or this node has no room for a copy
   */
  public HttpAuthSession resolve(final String token) {
    final String issuer = HttpAuthSessionManager.issuerOf(token);
    if (issuer == null || issuer.equals(sessions.getIssuerName()))
      return null;
    final HAServerPlugin ha = server.getHA();
    if (ha == null)
      return null;

    final long now = System.currentTimeMillis();
    final Long refusedUntil = refused.get(token);
    if (refusedUntil != null && refusedUntil > now) {
      count("refused_cached");
      return null;
    }

    if (!inFlight.tryAcquire()) {
      count("saturated");
      LogManager.instance().log(this, Level.FINE,
          "Refused to resolve authentication token issued by '%s': %d lookups already in flight", issuer,
          MAX_CONCURRENT_LOOKUPS);
      return null;
    }
    final HAServerPlugin.PeerAuthSession peerSession;
    try {
      peerSession = ha.lookupAuthSession(issuer, token);
    } catch (final IOException e) {
      count("unreachable");
      recordRefusal(token, now);
      LogManager.instance().log(this, Level.FINE, "Cannot ask node '%s' about an authentication token: %s", issuer,
          e.getMessage());
      return null;
    } finally {
      inFlight.release();
    }

    if (peerSession == null) {
      count("refused");
      recordRefusal(token, now);
      return null;
    }
    // Re-resolved from the live users map, like the bearer branch does for a local session: the issuer vouches
    // for the principal's name, this node decides what that principal may do today.
    final ServerSecurityUser user = server.getSecurity().getUser(peerSession.userName());
    if (user == null) {
      count("refused");
      recordRefusal(token, now);
      return null;
    }
    count("resolved");
    return sessions.addRemoteSession(token, user, peerSession.createdAt(), issuer);
  }

  /**
   * Called on every hit. A local session is always kept. A remote copy is kept while its lease holds; once the
   * renewal interval has passed the issuer is asked again, and the copy is dropped when the issuer no longer
   * holds the session or has been unreachable for a whole idle timeout.
   *
   * @return {@code true} to serve the request with this session, {@code false} after the copy has been dropped
   */
  public boolean renew(final HttpAuthSession session) {
    if (!session.isRemote() || session.elapsedFromConfirmation() < sessions.getRemoteRenewalIntervalMs())
      return true;
    final HAServerPlugin ha = server.getHA();
    if (ha == null)
      return true;
    if (!inFlight.tryAcquire())
      // Saturated: the lease is served as it stands and the next request tries again. Renewal is a background
      // concern of a session already vouched for, unlike a first lookup, which is the abuse surface.
      return true;
    final HAServerPlugin.PeerAuthSession peerSession;
    try {
      peerSession = ha.lookupAuthSession(session.getIssuer(), session.token);
    } catch (final IOException e) {
      if (session.elapsedFromConfirmation() <= sessions.getSessionTimeoutInMs())
        return true;
      count("lease_expired");
      LogManager.instance().log(this, Level.FINE,
          "Dropping the copy of authentication session %s: node '%s' has not confirmed it for %d ms (%s)",
          session.token, session.getIssuer(), session.elapsedFromConfirmation(), e.getMessage());
      sessions.removeSession(session.token);
      return false;
    } finally {
      inFlight.release();
    }
    if (peerSession == null) {
      count("revoked");
      sessions.removeSession(session.token);
      return false;
    }
    session.confirm();
    return true;
  }

  /**
   * Called on logout, after the local session is gone: tells every other node to drop its copy.
   */
  public void revoke(final String token) {
    final HAServerPlugin ha = server.getHA();
    if (ha != null && HttpAuthSessionManager.issuerOf(token) != null)
      ha.revokeAuthSession(token);
  }

  private void recordRefusal(final String token, final long now) {
    if (refused.size() >= REFUSAL_CACHE_MAX)
      // Flooded with distinct made-up tokens: forget them all rather than grow. The in-flight cap, not this
      // cache, is what bounds the cost of a flood; the cache only makes a REPEATED token free.
      refused.clear();
    refused.put(token, now + REFUSAL_CACHE_TTL_MS);
  }

  private static void count(final String result) {
    Metrics.counter("http.authSession.peerLookup", "result", result).increment();
  }
}
