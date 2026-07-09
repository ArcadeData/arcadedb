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

import com.arcadedb.database.TransactionContext;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.RWLockContext;

import java.util.*;
import java.util.logging.Level;

/**
 * Handles the stateful transactions in HTTP protocol as sessions. A HTTP transaction starts with the `/begin` command and is committed with `/commit` and
 * rolled back with `/rollback`.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSessionManager extends RWLockContext {
  public static final String                   ARCADEDB_SESSION_ID = "arcadedb-session-id";
  private final       Map<String, HttpSession> sessions            = new HashMap<>();
  private final       long                     transactionTimeoutInMs;
  private final       Timer                    timer;

  public HttpSessionManager(final long transactionTimeoutInMs) {
    this.transactionTimeoutInMs = transactionTimeoutInMs;

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          final int expired = checkSessionsValidity();
          if (expired > 0)
            LogManager.instance().log(this, Level.FINE, "Removed %d expired sessions", null, expired);
        } catch (Exception e) {
          // IGNORE IT
        }
      }
    }, transactionTimeoutInMs, transactionTimeoutInMs);
  }

  public void close() {
    timer.cancel();

    // SNAPSHOT UNDER THE WRITE LOCK BEFORE ITERATING: session.cancel() BELOW CAN NOW BLOCK FOR A WHILE ON AN
    // IN-FLIGHT COMMAND, WIDENING THE WINDOW FOR A CONCURRENT checkSessionsValidity() TICK (WHICH MUTATES
    // `sessions` UNDER THE WRITE LOCK) TO RACE A LIVE ITERATOR OVER THE SAME MAP
    final List<HttpSession> snapshot = executeInWriteLock(() -> new ArrayList<>(sessions.values()));

    // CANCEL ALL THE SESSIONS
    for (final HttpSession session : snapshot)
      session.cancel();

    executeInWriteLock(() -> {
      sessions.clear();
      return null;
    });
  }

  public int checkSessionsValidity() {
    if (executeInReadLock(sessions::isEmpty))
      return 0;

    return executeInWriteLock(() -> {
      int expired = 0;
      Map.Entry<String, HttpSession> s;
      for (final Iterator<Map.Entry<String, HttpSession>> it = sessions.entrySet().iterator(); it.hasNext(); ) {
        final HttpSession session = it.next().getValue();

        if (session.elapsedFromLastUpdate() > transactionTimeoutInMs) {
          // ONLY CANCEL AND REMOVE THE SESSION IF IT'S ACTUALLY IDLE (NO COMMAND CURRENTLY EXECUTING ON IT).
          // A BUSY SESSION IS LEFT ALONE FOR THIS SWEEP AND RE-EVALUATED ON THE NEXT TICK (SEE HttpSession#cancelIfIdle)
          final HttpSession.IdleCancelOutcome outcome = session.cancelIfIdle();
          if (outcome == HttpSession.IdleCancelOutcome.ROLLED_BACK)
            LogManager.instance().log(this, Level.FINE, "Canceling session %s because of timeout (%dms)", session.id,
                transactionTimeoutInMs);

          if (outcome != HttpSession.IdleCancelOutcome.BUSY) {
            it.remove();
            expired++;
          }
        }
      }
      return expired;
    });
  }

  public HttpSession getSessionById(final ServerSecurityUser user, final String txId) {
    return executeInReadLock(() -> {
      final HttpSession session = sessions.get(txId);
      if (session == null)
        return null;
      // Enforce ownership BEFORE the caller attaches the session to the thread context: a session must only be
      // resolvable by the principal that opened it, so another user cannot adopt (and commit) its transaction.
      // A session with no recorded owner is never resolvable by an authenticated principal (null-safe).
      if (user != null && (session.user == null || !session.user.equals(user)))
        return null;
      return session;
    });
  }

  /**
   * Returns true if a session with the given id is still tracked. Used by {@link HttpSession#execute} to
   * re-validate, under the session lock, that the idle sweep did not remove the session between the manager
   * lookup and lock acquisition.
   */
  public boolean isSessionRegistered(final String id) {
    return executeInReadLock(() -> sessions.containsKey(id));
  }

  public HttpSession createSession(final ServerSecurityUser user, final TransactionContext dbTx) {
    return executeInWriteLock(() -> {
      final String id = "AS-" + UUID.randomUUID();
      final HttpSession session = new HttpSession(user, id, dbTx, this);
      sessions.put(id, session);
      return session;
    });
  }

  public HttpSession removeSession(final String id) {
    return executeInWriteLock(() -> sessions.remove(id));
  }

  /**
   * Invalidates every live session owned by the named principal: the sessions are unregistered and their
   * open transactions rolled back. Called when a user is dropped or its password is changed so a stale (or
   * recreated same-name) principal cannot adopt a session opened by the previous principal.
   *
   * @return the number of sessions removed
   */
  public int removeSessionsForUser(final String userName) {
    if (userName == null)
      return 0;

    // Unregister under the write lock first, then cancel() (rollback) outside it: cancel() waits on the
    // per-session lock for any in-flight command, which must never be done while holding the manager lock.
    final List<HttpSession> removed = executeInWriteLock(() -> {
      final List<HttpSession> owned = new ArrayList<>();
      for (final Iterator<Map.Entry<String, HttpSession>> it = sessions.entrySet().iterator(); it.hasNext(); ) {
        final HttpSession session = it.next().getValue();
        if (session.user != null && userName.equals(session.user.getName())) {
          owned.add(session);
          it.remove();
        }
      }
      return owned;
    });

    for (final HttpSession session : removed)
      session.cancel();

    return removed.size();
  }

  public int getActiveSessions() {
    return executeInReadLock(sessions::size);
  }
}
