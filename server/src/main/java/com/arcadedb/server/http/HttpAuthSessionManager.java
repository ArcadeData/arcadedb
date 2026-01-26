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
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.RWLockContext;

import java.util.*;
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
  private final Map<String, HttpAuthSession> sessions = new HashMap<>();
  private final       long                         sessionTimeoutInMs;
  private final       long                         absoluteTimeoutInMs;
  private final       Timer                        timer;

  public HttpAuthSessionManager(final long sessionTimeoutInMs) {
    this(sessionTimeoutInMs, 0);
  }

  public HttpAuthSessionManager(final long sessionTimeoutInMs, final long absoluteTimeoutInMs) {
    this.sessionTimeoutInMs = sessionTimeoutInMs;
    this.absoluteTimeoutInMs = absoluteTimeoutInMs;

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
    sessions.clear();
  }

  public int checkSessionsValidity() {
    if (sessions.isEmpty())
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
   *
   * @param user      the authenticated user
   * @param sourceIp  the source IP address of the client
   * @param userAgent the user agent string of the client
   * @param country   the country from Cloudflare headers (if available)
   * @param city      the city from Cloudflare headers (if available)
   * @return the new session with a unique token
   */
  public HttpAuthSession createSession(final ServerSecurityUser user, final String sourceIp,
      final String userAgent, final String country, final String city) {
    return executeInWriteLock(() -> {
      final String token = "AU-" + UUID.randomUUID();
      final HttpAuthSession session = new HttpAuthSession(user, token, sourceIp, userAgent, country, city);
      sessions.put(token, session);
      LogManager.instance().log(this, Level.FINE, "Created authentication session %s for user %s from %s", token,
          user.getName(), sourceIp);
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
        LogManager.instance().log(this, Level.FINE, "Removed authentication session %s for user %s",
            token, removed.user.getName());
        return true;
      }
      return false;
    });
  }

  /**
   * Returns the number of active sessions.
   *
   * @return the count of active sessions
   */
  public int getActiveSessionCount() {
    return sessions.size();
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
