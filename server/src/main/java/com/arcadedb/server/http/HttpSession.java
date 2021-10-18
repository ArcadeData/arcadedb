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
 */
package com.arcadedb.server.http;

import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.concurrent.atomic.*;

/**
 * Manage a transaction on the HTTP protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSession {
  public final     String                  id;
  public final     TransactionContext      transaction;
  public final     ServerSecurityUser      user;
  public final     AtomicReference<Thread> currentThreadUsing = new AtomicReference<>();
  private volatile long                    lastUpdate         = 0L;

  public HttpSession(final ServerSecurityUser user, final String id, final TransactionContext dbTx) {
    this.user = user;
    this.id = id;
    this.transaction = dbTx;
    use(user);
  }

  public long elapsedFromLastUpdate() {
    return System.currentTimeMillis() - lastUpdate;
  }

  public HttpSession use(final ServerSecurityUser user) {
    if (!this.user.equals(user))
      throw new SecurityException("Cannot use the requested transaction because in use by a different user");

    if (!currentThreadUsing.compareAndSet(null, Thread.currentThread()))
      throw new TransactionException("Cannot use the requested transaction because in use by a different thread");

    lastUpdate = System.currentTimeMillis();
    return this;
  }

  public HttpSession endUsage() {
    currentThreadUsing.set(null);
    return this;
  }
}
