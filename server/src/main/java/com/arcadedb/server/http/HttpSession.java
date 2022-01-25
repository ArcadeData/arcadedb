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
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manage a transaction on the HTTP protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSession {
  private static final long               DEFAULT_TIMEOUT = 5_000;
  public final         String             id;
  public final         TransactionContext transaction;
  public final         ServerSecurityUser user;
  private final        ReentrantLock      lock            = new ReentrantLock();
  private volatile     long               lastUpdate      = 0L;

  public HttpSession(final ServerSecurityUser user, final String id, final TransactionContext dbTx) {
    this.user = user;
    this.id = id;
    this.transaction = dbTx;
  }

  public long elapsedFromLastUpdate() {
    return System.currentTimeMillis() - lastUpdate;
  }

  public HttpSession execute(final ServerSecurityUser user, final Callable callback) throws Exception {
    if (!this.user.equals(user))
      throw new SecurityException("Cannot use the requested transaction because in use by a different user");

    lastUpdate = System.currentTimeMillis();

    if (lock.tryLock(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS)) {
      try {
        callback.call();
      } finally {
        lock.unlock();
      }
    } else
      throw new TimeoutException("Timeout on locking http session");

    lastUpdate = System.currentTimeMillis();
    return this;
  }
}
