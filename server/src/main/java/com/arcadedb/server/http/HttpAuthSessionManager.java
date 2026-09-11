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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.RWLockContext;

import java.util.*;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Manages authenticated HTTP sessions. These sessions allow users to authenticate once
 * and receive a token that can be used for subsequent requests instead of sending
 * credentials with every request.
 * <p>
 * This is different from {@link HttpSessionManager} which manages transaction sessions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
public class HttpAuthSessionManager extends RWLockContext {
  public static final  String TOKEN_PREFIX            = "AU-";
  /** Length of the random part of a token: a {@link UUID} in its canonical text form. */
  static final         int    TOKEN_RANDOM_LENGTH     = 36;
  /** Upper bound on the issuer segment of a token, see {@link #sanitizeIssuerName(String)}. */
  static final         int    MAX_ISSUER_NAME_LENGTH  = 64;
  /** Cap on how often a remote copy is confirmed with its issuer, whatever the idle timeout says. */
  static final         long   MAX_RENEWAL_INTERVAL_MS = 60_000L;

  private final Map<String, HttpAuthSession> sessions = new HashMap<>();
  // Per-principal index of the tokens above, in insertion order. A LinkedHashSet (not a Deque) so both the
  // "which is the oldest session of this user" lookup the per-user cap needs and the arbitrary removal an
  // expiry/logout needs are O(1). Kept in exact lock-step with `sessions`: an entry here always names a live
  // session, and an empty set is dropped rather than left behind, so the index cannot outgrow the map.
  private final Map<String, LinkedHashSet<String>> sessionsByUser = new HashMap<>();
  private final       long                         sessionTimeoutInMs;
  private final       long                         absoluteTimeoutInMs;
  // <= 0 means unlimited. See SERVER_HTTP_AUTH_SESSION_MAX / SERVER_HTTP_AUTH_SESSION_MAX_PER_USER.
  private final       int                          maxSessions;
  private final       int                          maxSessionsPerUser;
  private final       LongSupplier                 clock;
  private final       Timer                        timer;
  /**
   * Written into every token this node mints, between the prefix and the random part, so a peer that has never
   * seen the token can tell which node to ask about it (issue #7424). {@code null} keeps the legacy
   * {@code AU-<uuid>} form, which no peer can resolve.
   */
  private final       String                       issuerName;
  /**
   * Stands in for a server that was not supplied - the test-only constructors below; reads through to each
   * setting's process-wide value. Shared rather than built per read: it holds nothing per instance.
   */
  private static final ContextConfiguration        NO_SERVER_CONFIGURATION = new ContextConfiguration();

  public HttpAuthSessionManager(final long sessionTimeoutInMs) {
    this(sessionTimeoutInMs, 0);
  }

  /**
   * Convenience form for a caller with no server configuration in reach - the tests; {@code HttpServer} passes both
   * caps explicitly, read from the server's own configuration. The empty {@link ContextConfiguration} says exactly
   * that: both settings are SCOPE.SERVER, so reading them off the {@link GlobalConfiguration} enum would have been
   * reading a value only a system property or an environment variable can have written (issue #7233).
   */
  public HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs) {
    this(sessionTimeoutInMs, absoluteTimeoutInMs, System::currentTimeMillis);
  }

  public HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs, final int maxSessions,
      final int maxSessionsPerUser) {
    this(sessionTimeoutInMs, absoluteTimeoutInMs, maxSessions, maxSessionsPerUser, System::currentTimeMillis);
  }

  /**
   * Package-private constructor that allows tests to inject a deterministic clock instead of the
   * wall clock, so idle/absolute timeout behavior can be asserted without sleeping (see #6398).
   */
  HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs, final LongSupplier clock) {
    this(sessionTimeoutInMs, absoluteTimeoutInMs,
        NO_SERVER_CONFIGURATION.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_AUTH_SESSION_MAX),
        NO_SERVER_CONFIGURATION.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_AUTH_SESSION_MAX_PER_USER), clock);
  }

  HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs, final int maxSessions,
      final int maxSessionsPerUser, final LongSupplier clock) {
    this(sessionTimeoutInMs, absoluteTimeoutInMs, maxSessions, maxSessionsPerUser, null, clock);
  }

  /**
   * The form {@code HttpServer} uses: {@code issuerName} is this node's {@code arcadedb.server.name}, written into
   * every token so the other nodes of a cluster can resolve it (issue #7424).
   */
  public HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs, final int maxSessions,
      final int maxSessionsPerUser, final String issuerName) {
    this(sessionTimeoutInMs, absoluteTimeoutInMs, maxSessions, maxSessionsPerUser, issuerName, System::currentTimeMillis);
  }

  HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs, final int maxSessions,
      final int maxSessionsPerUser, final String issuerName, final LongSupplier clock) {
    this.sessionTimeoutInMs = sessionTimeoutInMs;
    this.absoluteTimeoutInMs = absoluteTimeoutInMs;
    this.maxSessions = maxSessions;
    this.maxSessionsPerUser = maxSessionsPerUser;
    this.issuerName = sanitizeIssuerName(issuerName);
    this.clock = clock;

    timer = new Timer("HttpAuthSessionManager-Cleanup", true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          final int expired = checkSessionsValidity();
          if (expired > 0)
            LogManager.instance().log(this, Level.FINE, "Removed %d expired authentication sessions", null, expired);
        } catch (Exception e) {
          // IGNORE IT
        }
      }
    }, sessionTimeoutInMs, sessionTimeoutInMs);
  }

  public void close() {
    timer.cancel();
    executeInWriteLock(() -> {
      sessions.clear();
      sessionsByUser.clear();
      return null;
    });
  }

  public int checkSessionsValidity() {
    if (executeInReadLock(sessions::isEmpty))
      return 0;

    return executeInWriteLock(() -> {
      int expired = 0;
      for (final Iterator<Map.Entry<String, HttpAuthSession>> it = sessions.entrySet().iterator(); it.hasNext(); ) {
        final HttpAuthSession session = it.next().getValue();

        final boolean idleExpired = session.elapsedFromLastUpdate() > sessionTimeoutInMs;
        final boolean absoluteExpired = absoluteTimeoutInMs > 0 && session.elapsedFromCreation() > absoluteTimeoutInMs;

        if (idleExpired || absoluteExpired) {
          LogManager.instance().log(this, Level.FINE, "Removing expired authentication session %s for user %s (idle=%b, absolute=%b)",
              session.token, session.user.getName(), idleExpired, absoluteExpired);
          it.remove();
          unindex(session);
          expired++;
        }
      }
      return expired;
    });
  }

  /**
   * Get an authenticated session by token.
   * Returns null if the session doesn't exist or if it has expired (either by idle or absolute timeout).
   *
   * @param token the authentication token
   * @return the session if found and valid, null otherwise
   */
  public HttpAuthSession getSessionByToken(final String token) {
    return executeInReadLock(() -> {
      final HttpAuthSession session = sessions.get(token);
      if (session != null) {
        // Check if session is expired by absolute timeout (from creation)
        if (absoluteTimeoutInMs > 0 && session.elapsedFromCreation() > absoluteTimeoutInMs) {
          return null;
        }
        session.touch();
      }

      return session;
    });
  }

  /**
   * Create a new authenticated session for a user.
   *
   * @param user the authenticated user
   * @return the new session with a unique token
   */
  public HttpAuthSession createSession(final ServerSecurityUser user) {
    return createSession(user, null, null, null, null);
  }

  /**
   * Create a new authenticated session for a user with additional metadata.
   * <p>
   * The session map is bounded on both axes (issue #6809). The <b>per-principal</b> cap
   * ({@code arcadedb.server.httpAuthSessionMaxPerUser}) is enforced by evicting that principal's oldest
   * session, so a login loop churns only its own sessions - it cannot grow the map and it cannot evict
   * anybody else's session. The <b>global</b> cap ({@code arcadedb.server.httpAuthSessionMax})
   * is enforced by refusing, because at that point the only honest answer is that the server is out of
   * session capacity; evicting globally would let one principal push other principals out. An idle sweep is
   * attempted first, so a full map made of expired sessions is reclaimed instead of refused.
   *
   * @param user      the authenticated user
   * @param sourceIp  the source IP address of the client
   * @param userAgent the user agent string of the client
   * @param country   the country from Cloudflare headers (if available)
   * @param city      the city from Cloudflare headers (if available)
   *
   * @return the new session with a unique token, or {@code null} when the global cap is reached and no
   * session could be reclaimed (the caller answers 503)
   */
  public HttpAuthSession createSession(final ServerSecurityUser user, final String sourceIp,
      final String userAgent, final String country, final String city) {
    final String token = issuerName != null ? TOKEN_PREFIX + issuerName + "-" + UUID.randomUUID()
        : TOKEN_PREFIX + UUID.randomUUID();
    return insert(user, () -> new HttpAuthSession(user, token, sourceIp, userAgent, country, city, clock));
  }

  /**
   * Installs the local copy of a session another node of the cluster issued (issue #7424), once that node has
   * confirmed the token. The copy is subject to the same caps and timeouts as a local session: it counts against
   * the principal's and the server's limits, idles out when unused HERE, and inherits the issuer's creation time
   * so the absolute timeout is measured from the original login.
   *
   * @return the installed copy, or {@code null} when the global cap refused it (the caller answers 401: the token
   * is valid, but this node has no room to honour it)
   */
  public HttpAuthSession addRemoteSession(final String token, final ServerSecurityUser user, final long createdAt,
      final String issuer) {
    return insert(user, () -> new HttpAuthSession(user, token, issuer, createdAt, clock));
  }

  /**
   * Which node minted {@code token}, read from the {@code AU-<issuer>-<uuid>} form; {@code null} for the legacy
   * {@code AU-<uuid>} form, for anything that is not a session token, or for an issuer segment that is not a
   * sanitized name. Parsed from the END, because the issuer is a server name and server names carry dashes
   * ({@code arcadedb-0}), while the random part has a fixed length.
   */
  public static String issuerOf(final String token) {
    if (token == null || !token.startsWith(TOKEN_PREFIX))
      return null;
    final int issuerEnd = token.length() - TOKEN_RANDOM_LENGTH - 1;
    if (issuerEnd <= TOKEN_PREFIX.length() || token.charAt(issuerEnd) != '-')
      return null;
    final String issuer = token.substring(TOKEN_PREFIX.length(), issuerEnd);
    return issuer.equals(sanitizeIssuerName(issuer)) ? issuer : null;
  }

  /**
   * The issuer segment a server name becomes inside a token: any character outside {@code [A-Za-z0-9._-]} is
   * replaced with {@code _} and the result is capped at {@link #MAX_ISSUER_NAME_LENGTH}, so the token stays a
   * plain header value whatever the operator called the node. Applied on BOTH sides - when minting and when a
   * peer maps the segment back to a node - so the mapping is stable. {@code null} and blank stay {@code null}.
   */
  public static String sanitizeIssuerName(final String name) {
    if (name == null || name.isBlank())
      return null;
    final StringBuilder out = new StringBuilder(Math.min(name.length(), MAX_ISSUER_NAME_LENGTH));
    for (int i = 0; i < name.length() && out.length() < MAX_ISSUER_NAME_LENGTH; i++) {
      final char c = name.charAt(i);
      final boolean allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
          || c == '.' || c == '_' || c == '-';
      out.append(allowed ? c : '_');
    }
    return out.toString();
  }

  /**
   * How long a remote copy is served before it is confirmed with its issuer again: a third of the idle timeout,
   * capped at {@link #MAX_RENEWAL_INTERVAL_MS}. The confirmation touches the session on the issuer, so a token in
   * use anywhere in the cluster never idles out at its source; a third leaves two attempts before it would.
   */
  public long getRemoteRenewalIntervalMs() {
    return Math.max(1, Math.min(sessionTimeoutInMs / 3, MAX_RENEWAL_INTERVAL_MS));
  }

  public long getSessionTimeoutInMs() {
    return sessionTimeoutInMs;
  }

  public String getIssuerName() {
    return issuerName;
  }

  private HttpAuthSession insert(final ServerSecurityUser user, final Supplier<HttpAuthSession> factory) {
    if (maxSessions > 0 && getActiveSessionCount() >= maxSessions)
      // Reclaim first: a map full of idle-expired sessions must not refuse a legitimate login just because
      // the background sweep has not fired yet. Done outside the write lock below (it takes its own).
      checkSessionsValidity();

    return executeInWriteLock(() -> {
      final String userName = user.getName();
      final LinkedHashSet<String> existingTokens = sessionsByUser.get(userName);

      // A principal already at its own cap will free exactly one slot by evicting, so it is admitted even
      // when the map is globally full. Decided BEFORE anything is evicted: refusing after the eviction would
      // destroy a live session of the very principal being refused.
      final boolean willEvict = maxSessionsPerUser > 0 && existingTokens != null
          && existingTokens.size() >= maxSessionsPerUser;

      if (maxSessions > 0 && sessions.size() >= maxSessions && !willEvict) {
        LogManager.instance().log(this, Level.WARNING,
            "Refused authentication session for user %s: the server reached the maximum of %d concurrent sessions "
                + "(see '%s')", userName, maxSessions, GlobalConfiguration.SERVER_HTTP_AUTH_SESSION_MAX.getKey());
        return null;
      }

      final LinkedHashSet<String> userTokens = existingTokens != null ? existingTokens
          : sessionsByUser.computeIfAbsent(userName, k -> new LinkedHashSet<>());

      // Per-principal cap: evict this principal's oldest session(s) to make room. `while`, not `if`, so a
      // lowered configuration takes effect on the next login instead of leaving the surplus in place forever.
      while (maxSessionsPerUser > 0 && userTokens.size() >= maxSessionsPerUser) {
        final Iterator<String> oldest = userTokens.iterator();
        final String evicted = oldest.next();
        oldest.remove();
        sessions.remove(evicted);
        LogManager.instance().log(this, Level.FINE,
            "Evicted authentication session %s: user %s reached the maximum of %d concurrent sessions", evicted,
            userName, maxSessionsPerUser);
      }

      final HttpAuthSession session = factory.get();
      final String token = session.token;
      sessions.put(token, session);
      userTokens.add(token);
      LogManager.instance().log(this, Level.FINE, "Created authentication session %s for user %s from %s", token,
          user.getName(), session.isRemote() ? "peer " + session.getIssuer() : session.getSourceIp());
      return session;
    });
  }

  /**
   * Remove an authenticated session (logout).
   *
   * @param token the authentication token to invalidate
   * @return true if the session was found and removed, false otherwise
   */
  public boolean removeSession(final String token) {
    return executeInWriteLock(() -> {
      final HttpAuthSession removed = sessions.remove(token);
      if (removed != null) {
        unindex(removed);
        LogManager.instance().log(this, Level.FINE, "Removed authentication session %s for user %s",
            token, removed.user.getName());
        return true;
      }
      return false;
    });
  }

  /**
   * Invalidates every live authentication session owned by the named principal. Called when a user is
   * dropped or its password is changed, so a token minted before that keeps no authority - mirroring what
   * {@link HttpSessionManager#removeSessionsForUser} already does for transaction sessions.
   * <p>
   * Unlike its transaction-session counterpart this does no blocking work at all: an authentication session
   * owns no transaction and has nothing to cancel, so it is safe to call from anywhere, including the Raft
   * state-machine apply thread that installs a replicated user list on a peer.
   *
   * @return the number of sessions removed
   */
  public int removeSessionsForUser(final String userName) {
    if (userName == null)
      return 0;

    return executeInWriteLock(() -> {
      final LinkedHashSet<String> userTokens = sessionsByUser.remove(userName);
      if (userTokens == null)
        return 0;
      for (final String token : userTokens)
        sessions.remove(token);
      LogManager.instance().log(this, Level.FINE, "Removed %d authentication session(s) of user %s",
          userTokens.size(), userName);
      return userTokens.size();
    });
  }

  /**
   * Drops a session from the per-principal index, and the principal's (now empty) index entry with it, so
   * {@link #sessionsByUser} can never retain a key for a user with no live session. Must be called under the
   * write lock, right after the session has been removed from {@link #sessions}.
   */
  private void unindex(final HttpAuthSession session) {
    final String userName = session.user != null ? session.user.getName() : null;
    if (userName == null)
      return;
    final LinkedHashSet<String> userTokens = sessionsByUser.get(userName);
    if (userTokens != null) {
      userTokens.remove(session.token);
      if (userTokens.isEmpty())
        sessionsByUser.remove(userName);
    }
  }

  /**
   * Returns the number of active sessions held by the named principal. Package-private: used by the tests
   * that pin the per-principal cap.
   */
  int getActiveSessionCount(final String userName) {
    return executeInReadLock(() -> {
      final LinkedHashSet<String> userTokens = sessionsByUser.get(userName);
      return userTokens != null ? userTokens.size() : 0;
    });
  }

  /**
   * Returns the number of active sessions.
   *
   * @return the count of active sessions
   */
  public int getActiveSessionCount() {
    return executeInReadLock(sessions::size);
  }

  /**
   * Returns a list of all active sessions.
   * This method is intended for administrative purposes.
   *
   * @return a list of active sessions
   */
  public List<HttpAuthSession> getActiveSessions() {
    return executeInReadLock(() -> new ArrayList<>(sessions.values()));
  }
}
