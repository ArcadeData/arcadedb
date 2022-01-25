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
  private final       long                     expirationTimeInMs;
  private final       Timer                    timer;

  public HttpSessionManager(final long expirationTimeInMs) {
    this.expirationTimeInMs = expirationTimeInMs;

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        final int expired = checkSessionsValidity();
        if (expired > 0)
          LogManager.instance().log(this, Level.FINE, "Removed %d expired sessions", null, expired);
      }
    }, expirationTimeInMs, expirationTimeInMs);
  }

  public void close() {
    timer.cancel();
    sessions.clear();
  }

  public int checkSessionsValidity() {
    return executeInWriteLock(() -> {
      int expired = 0;
      Map.Entry<String, HttpSession> s;
      for (Iterator<Map.Entry<String, HttpSession>> it = sessions.entrySet().iterator(); it.hasNext(); ) {
        s = it.next();

        if (s.getValue().elapsedFromLastUpdate() > expirationTimeInMs) {
          // REMOVE THE SESSION
          it.remove();
          expired++;
        }
      }
      return expired;
    });
  }

  public HttpSession getSessionById(final ServerSecurityUser user, final String txId) {
    return executeInReadLock(() -> sessions.get(txId));
  }

  public HttpSession createSession(final ServerSecurityUser user, final TransactionContext dbTx) {
    return executeInWriteLock(() -> {
      final String id = "AS-" + UUID.randomUUID();
      final HttpSession session = new HttpSession(user, id, dbTx);
      sessions.put(id, session);
      return session;
    });
  }

  public HttpSession removeSession(final String iSessionId) {
    return executeInWriteLock(() -> sessions.remove(iSessionId));
  }

  public int getActiveSessions() {
    return sessions.size();
  }
}
