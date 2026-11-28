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
  /** The issuer is asked once more when a session was removed while its answer was on the way. */
  static final int  MAX_LOOKUP_ATTEMPTS    = 2;

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
   * session, installs a local copy. A copy is installed only if no session of the principal, and not the token itself,
   * was removed on this node while the issuer was being asked: the answer was given before that removal. The issuer is
   * then asked once more, and the token is refused (without being remembered as refused) when that happens again.
   *
   * @return the local copy, or {@code null} when the token is not honoured here: it names no issuer, names this
   * node (which does not hold it, so it expired), names a node that is not a member, the issuer refused it or
   * could not be asked, or this node has no room for a copy
   */
  public HttpAuthSession resolve(final String token) {
    return resolve(token, true);
  }

  /**
   * As {@link #resolve(String)}, for a caller that may be on a thread which must not wait on a peer (issue #9864).
   *
   * @param mayDial {@code false} when the calling thread must not block on the network - an Undertow IO thread. A
   *                token the issuer would have to be asked about is then refused without asking and without being
   *                remembered as refused, exactly as when the lookups are saturated. The HTTP layer hands such a
   *                request to a worker thread before it gets here (see {@link #wouldDial(String)}), so this only
   *                decides the request whose answer changed between that check and this call: a refusal that
   *                expired from the cache, or a session removed in that instant
   */
  public HttpAuthSession resolve(final String token, final boolean mayDial) {
    final String issuer = peerIssuerOf(token);
    if (issuer == null)
      return null;
    final HAServerPlugin ha = server.getHA();
    if (ha == null)
      return null;

    if (isRefusalCached(token)) {
      count("refused_cached");
      return null;
    }
    if (!mayDial)
      return null;

    // The issuer's answer is given before whatever this node does while waiting for it. When a session was removed in the
    // meantime (the principal's, or this token) the answer is not installed, and the issuer is asked once more: its second
    // answer reflects the removal, or says the session is still valid.
    for (int attempt = 1; attempt <= MAX_LOOKUP_ATTEMPTS; attempt++) {
      if (!inFlight.tryAcquire()) {
        count("saturated");
        LogManager.instance().log(this, Level.FINE,
            "Refused to resolve authentication token issued by '%s': %d lookups already in flight", issuer,
            MAX_CONCURRENT_LOOKUPS);
        return null;
      }
      // Started before the issuer is asked, closed once the answer is used
      try (final HttpAuthSessionManager.PeerLookup lookup = sessions.beginPeerLookup(token)) {
        final HAServerPlugin.PeerAuthSession peerSession;
        try {
          peerSession = ha.lookupAuthSession(issuer, token);
        } catch (final IOException e) {
          count("unreachable");
          recordRefusal(token);
          LogManager.instance().log(this, Level.FINE, "Cannot ask node '%s' about an authentication token: %s", issuer,
              e.getMessage());
          return null;
        } finally {
          inFlight.release();
        }

        if (peerSession == null) {
          count("refused");
          recordRefusal(token);
          return null;
        }
        // Re-resolved from the live users map, like the bearer branch does for a local session: the issuer vouches
        // for the principal's name, this node decides what that principal may do today.
        final ServerSecurityUser user = server.getSecurity().getUser(peerSession.userName());
        if (user == null) {
          count("refused");
          recordRefusal(token);
          return null;
        }
        final HttpAuthSession copy = sessions.addRemoteSession(token, user, peerSession.createdAt(), issuer, lookup);
        if (copy != null || !lookup.isRevoked()) {
          count("resolved");
          return copy;
        }
        count("changed_while_asking");
      }
    }
    // Not remembered as refused: the token may be valid, and the next request asks again
    return null;
  }

  /**
   * Called on every hit. A local session is always kept. A remote copy is kept while its lease holds; once the
   * renewal interval has passed the issuer is asked again, and the copy is dropped when the issuer no longer
   * holds the session or has been unreachable for a whole idle timeout.
   *
   * @return {@code true} to serve the request with this session, {@code false} after the copy has been dropped
   */
  public boolean renew(final HttpAuthSession session) {
    return renew(session, true);
  }

  /**
   * As {@link #renew(HttpAuthSession)}, for a caller that may be on a thread which must not wait on a peer (issue
   * #9864).
   *
   * @param mayDial {@code false} when the calling thread must not block on the network - an Undertow IO thread. A
   *                lease that is due is then served as it stands, exactly as when the lookups are saturated, and the
   *                next request renews it. The HTTP layer hands a request whose lease is due to a worker thread
   *                before it gets here (see {@link #wouldDial(String)}), so this only decides the request whose
   *                lease fell due between that check and this call
   */
  public boolean renew(final HttpAuthSession session, final boolean mayDial) {
    if (!isRenewalDue(session) || !mayDial)
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
    // Confirmed only while this very copy is still held: a removal that ran while the issuer was being asked took it out
    // of the map, and the issuer's answer, given before that, must not serve the request with it.
    return sessions.confirmIfHeld(session);
  }

  /**
   * Whether authenticating {@code token} right now would call a peer: either this node holds no session for it and
   * {@link #resolve(String)} would ask the issuer it names, or it holds a copy whose lease is due and
   * {@link #renew(HttpAuthSession)} would (issue #9864). That call is a synchronous HTTP request bounded only by the
   * peer connect timeout, so a caller on a thread that must not block - an Undertow IO thread - asks this first and
   * moves to a worker thread on {@code true}.
   * <p>
   * It never dials and never counts: a hash-map read under the session manager's read lock, and nothing at all on
   * a server that is not clustered. It is built from the very guards {@code resolve} and {@code renew} open with,
   * so the two cannot drift apart. It is a question and not a use of the session: the session is read without being
   * touched, so asking does not extend it, and one past its idle limit counts as not held. It errs towards
   * {@code true} in one case it cannot see: with {@link #MAX_CONCURRENT_LOOKUPS} lookups already in flight the call
   * would be skipped, which only costs the hand-off.
   */
  public boolean wouldDial(final String token) {
    if (server.getHA() == null)
      return false;
    final HttpAuthSession session = sessions.peekSessionByToken(token);
    return session != null ? isRenewalDue(session) : peerIssuerOf(token) != null && !isRefusalCached(token);
  }

  /**
   * The issuer {@code token} names when that is another node, {@code null} when it names none (a legacy token) or
   * this node (which would hold the session if it were still valid).
   */
  private String peerIssuerOf(final String token) {
    final String issuer = HttpAuthSessionManager.issuerOf(token);
    return issuer == null || issuer.equals(sessions.getIssuerName()) ? null : issuer;
  }

  private boolean isRefusalCached(final String token) {
    final Long refusedUntil = refused.get(token);
    return refusedUntil != null && refusedUntil > System.currentTimeMillis();
  }

  /** Only a copy of another node's session has a lease, and it is due once the renewal interval has passed. */
  private boolean isRenewalDue(final HttpAuthSession session) {
    return session.isRemote() && session.elapsedFromConfirmation() >= sessions.getRemoteRenewalIntervalMs();
  }

  /**
   * Called on logout, after the local session is gone: tells every other node to drop its copy.
   */
  public void revoke(final String token) {
    final HAServerPlugin ha = server.getHA();
    if (ha != null && HttpAuthSessionManager.issuerOf(token) != null)
      ha.revokeAuthSession(token);
  }

  /**
   * Stamped when the refusal is recorded, not when the lookup began: a lookup that ran to its timeout would
   * otherwise insert an entry that is already expired, and the next request would dial the peer again.
   */
  private void recordRefusal(final String token) {
    if (refused.size() >= REFUSAL_CACHE_MAX)
      // Flooded with distinct made-up tokens: forget them all rather than grow. The in-flight cap, not this
      // cache, is what bounds the cost of a flood; the cache only makes a REPEATED token free.
      refused.clear();
    refused.put(token, System.currentTimeMillis() + REFUSAL_CACHE_TTL_MS);
  }

  private static void count(final String result) {
    Metrics.counter("http.authSession.peerLookup", "result", result).increment();
  }
}
