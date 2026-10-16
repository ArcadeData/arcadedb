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
  public static final String                                  ARCADEDB_SESSION_ID    = "arcadedb-session-id";
  /**
   * How many committed sessions {@link #recordCommitted} remembers at most: a few hundred KB at worst, whatever the commit
   * rate, while a retry normally arrives within seconds of the answer it replaces (issue #10164).
   */
  static final        int                                     MAX_REMEMBERED_COMMITS = 10_000;
  private final       Map<String, HttpSession>                sessions               = new HashMap<>();
  /**
   * The sessions a /commit ended, by id, with their owner's name and when they committed, oldest first: session ids are
   * unique, so every put appends. Lets a retried commit of one of them still answer 204 while a commit naming any other
   * unresolvable session answers 404 (issue #10164). Kept for {@link #transactionTimeoutInMs}, and never more than
   * {@link #MAX_REMEMBERED_COMMITS} of them. Per server and in memory: a retry sent to another server, or after a restart,
   * is not recognised.
   */
  private final       LinkedHashMap<String, CommittedSession> committedSessions      = new LinkedHashMap<>();
  private final       long                                    transactionTimeoutInMs;
  private final       Timer                                   timer;

  private record CommittedSession(String userName, long committedAt) {
  }

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
      committedSessions.clear();
      return null;
    });
  }

  public int checkSessionsValidity() {
    pruneCommittedSessions(System.currentTimeMillis());

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
      // NOTE: ownership compares by ServerSecurityUser identity/name, so protection against a dropped-then-
      // recreated same-name principal relies on removeSessionsForUser() being invoked from every user
      // removal/mutation path (ServerSecurity.dropUser/setUserPassword). Any future path that replaces the
      // users map without routing through those hooks must also invalidate the affected sessions here.
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
   * Remembers that a /commit ended the session {@code id} of {@code user}, so a retry of that commit - whose first answer
   * may never have reached the client - can still be told it committed (issue #10164).
   */
  public void recordCommitted(final String id, final ServerSecurityUser user) {
    if (id == null || user == null)
      return;
    final long now = System.currentTimeMillis();
    executeInWriteLock(() -> {
      committedSessions.put(id, new CommittedSession(user.getName(), now));
      if (committedSessions.size() > MAX_REMEMBERED_COMMITS) {
        // Oldest first, so the overflow is always the head
        final Iterator<CommittedSession> it = committedSessions.values().iterator();
        while (committedSessions.size() > MAX_REMEMBERED_COMMITS && it.hasNext()) {
          it.next();
          it.remove();
        }
      }
      return null;
    });
  }

  /**
   * True when a /commit by the principal {@code user} ended the session {@code id} within the session timeout. Gated on the
   * owner like {@link #getSessionById}, so nobody else learns that the session existed.
   */
  public boolean wasCommittedBy(final ServerSecurityUser user, final String id) {
    if (user == null || id == null)
      return false;
    final long oldest = System.currentTimeMillis() - transactionTimeoutInMs;
    return executeInReadLock(() -> {
      final CommittedSession committed = committedSessions.get(id);
      return committed != null && committed.committedAt >= oldest && committed.userName.equals(user.getName());
    });
  }

  /** How many committed sessions are remembered right now. For tests. */
  int getRememberedCommits() {
    return executeInReadLock(committedSessions::size);
  }

  /** Forgets the commits older than the session timeout at {@code now}. Called by the idle sweep. */
  void pruneCommittedSessions(final long now) {
    if (executeInReadLock(committedSessions::isEmpty))
      return;
    final long oldest = now - transactionTimeoutInMs;
    executeInWriteLock(() -> {
      // Oldest first: stop at the first one still young enough
      for (final Iterator<CommittedSession> it = committedSessions.values().iterator(); it.hasNext(); ) {
        if (it.next().committedAt >= oldest)
          break;
        it.remove();
      }
      return null;
    });
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
      // A recreated same-name principal must not be told it committed the prior principal's sessions (issue #10164)
      committedSessions.values().removeIf(committed -> userName.equals(committed.userName));
      return owned;
    });

    for (final HttpSession session : removed)
      session.cancel();

    return removed.size();
  }

  /** A snapshot of the open transaction sessions (issue #9680). */
  public List<HttpSession> getSessions() {
    return executeInReadLock(() -> new ArrayList<>(sessions.values()));
  }

  public int getActiveSessions() {
    return executeInReadLock(sessions::size);
  }
}
