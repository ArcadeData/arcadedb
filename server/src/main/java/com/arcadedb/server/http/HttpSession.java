/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.http;

import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.security.ServerSecurityUser;

/**
 * Manage a transaction on the HTTP protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSession {
  public final     String             id;
  public final     TransactionContext transaction;
  public final     ServerSecurityUser user;
  public volatile  Thread             currentThreadUsing;
  private volatile long               lastUpdate = 0L;

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
    if (currentThreadUsing != null)
      throw new TransactionException("Cannot use the requested transaction because in use by a different thread");

    if (!this.user.equals(user))
      throw new SecurityException("Cannot use the requested transaction because in use by a different user");

    lastUpdate = System.currentTimeMillis();
    currentThreadUsing = Thread.currentThread();
    return this;
  }

  public HttpSession endUsage() {
    currentThreadUsing = null;
    return this;
  }
}
