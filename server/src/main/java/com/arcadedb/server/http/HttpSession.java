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
import com.arcadedb.query.QuerySession;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * Manage a transaction on the HTTP protocol. Also acts as the {@link QuerySession} that ISO GQL Session
 * Management statements ({@code SESSION SET/RESET/CLOSE}) operate on: it carries session parameters that
 * subsequent commands in the same session see as query parameters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSession implements QuerySession {
  private static final long                DEFAULT_TIMEOUT = 5_000;
  public final         String              id;
  public final         TransactionContext  transaction;
  public final         ServerSecurityUser  user;
  private final        HttpSessionManager  manager;
  // Accessed only under the session lock (see execute()), so a plain HashMap is sufficient.
  private final        Map<String, Object> parameters      = new HashMap<>();
  // Cached live read-only view over 'parameters' (reflects mutations), so getParameters() allocates nothing.
  private final        Map<String, Object> parametersView  = Collections.unmodifiableMap(parameters);
  private final        ReentrantLock       lock            = new ReentrantLock();
  private volatile     long                lastUpdate      = System.currentTimeMillis();

  public HttpSession(final ServerSecurityUser user, final String id, final TransactionContext dbTx,
      final HttpSessionManager manager) {
    this.user = user;
    this.id = id;
    this.transaction = dbTx;
    this.manager = manager;
  }

  @Override
  public void setParameter(final String name, final Object value) {
    parameters.put(name, value);
  }

  @Override
  public Map<String, Object> getParameters() {
    return parametersView;
  }

  @Override
  public void reset() {
    parameters.clear();
  }

  @Override
  public void close() {
    // Invalidate the session so later references to its id fail, then roll back its open transaction.
    manager.removeSession(id);
    cancel();
  }

  public long elapsedFromLastUpdate() {
    return System.currentTimeMillis() - lastUpdate;
  }

  public boolean cancel() {
    try {
      if (transaction != null && transaction.isActive()) {
        transaction.rollback();
        return true;
      }
    } catch (Exception e) {
      // IGNORE IT
    }
    return false;
  }

  public HttpSession execute(final ServerSecurityUser user, final Callable callback) throws Exception {
    if (!this.user.equals(user))
      throw new SecurityException("Cannot use the requested transaction because in use by a different user");

    lastUpdate = System.currentTimeMillis();

    if (lock.tryLock(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS)) {
      try {
        LogManager.instance().log(this, Level.FINE, "Executing session %s for user %s", id, user.getName());
        callback.call();
      } catch (Exception e) {
        // ROLLBACK SERVER-SIDE TRANSACTION
        cancel();
        throw e;
      } finally {
        lock.unlock();
      }
    } else {
      throw new TimeoutException("Timeout on locking http session");
    }

    lastUpdate = System.currentTimeMillis();
    return this;
  }
}
