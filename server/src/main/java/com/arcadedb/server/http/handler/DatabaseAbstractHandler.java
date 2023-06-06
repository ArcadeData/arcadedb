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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public abstract class DatabaseAbstractHandler extends AbstractHandler {
  private static final HttpString SESSION_ID_HEADER = new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID);

  protected DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, Database database) throws Exception;

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user) throws Exception {
    final Database database;
    HttpSession activeSession = null;
    boolean atomicTransaction = false;

    DatabaseContext.DatabaseContextTL current = null;

    if (requiresDatabase()) {
      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty())
        return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");

      database = httpServer.getServer().getDatabase(databaseName.getFirst(), false, false);

      current = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
      if (current != null && !current.transactions.isEmpty() && current.transactions.get(0).isActive()) {
        LogManager.instance().log(this, Level.WARNING, "Found a pending transaction from a previous operation. Rolling it back...");
        cleanTL(database, current);
      }

      activeSession = setTransactionInThreadLocal(exchange, database, user);

      current = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
      if (current == null)
        // INITIALIZE THE DATABASE CONTEXT
        current = DatabaseContext.INSTANCE.init((DatabaseInternal) database);

      final SecurityDatabaseUser currentUser = current.getCurrentUser();
      if (currentUser == null || !currentUser.equals(user.getDatabaseUser(database)))
        current.setCurrentUser(user != null ? user.getDatabaseUser(database) : null);

      if (requiresTransaction() && activeSession == null) {
        atomicTransaction = true;
        database.begin();
      }

    } else
      database = null;

    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();
    try {
      if (activeSession != null)
        // EXECUTE THE CODE LOCKING THE CURRENT SESSION. THIS AVOIDS USING THE SAME SESSION FROM MULTIPLE THREADS AT THE SAME TIME
        activeSession.execute(user, () -> {
          response.set(execute(exchange, user, database));
          return null;
        });
      else
        response.set(execute(exchange, user, database));

      if (database != null && atomicTransaction && database.isTransactionActive())
        // STARTED ATOMIC TRANSACTION, COMMIT
        database.commit();

    } finally {

      if (activeSession != null)
        // DETACH CURRENT CONTEXT/TRANSACTIONS FROM CURRENT THREAD
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      else if (database != null) {
        try {
          if (!atomicTransaction)
            // NO TRANSACTION, ROLLBACK TO MAKE SURE ANY PENDING OPERATION IS REMOVED
            database.rollbackAllNested();
        } finally {
          // DO NOT CLEAN THE CURRENT SESSION BECAUSE IT COULD HAVE AN OPEN TX
          cleanTL(database, null);
        }
      }
    }

    return response.get();
  }

  private void cleanTL(final Database database, DatabaseContext.DatabaseContextTL current) {
    if (current == null)
      current = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());

    if (current != null) {
      TransactionContext tx;
      while ((tx = current.popIfNotLastTransaction()) != null) {
        if (tx.isActive())
          tx.rollback();
        else
          break;
      }

      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
    }
  }

  protected boolean requiresDatabase() {
    return true;
  }

  protected boolean requiresTransaction() {
    return true;
  }

  protected HttpSession setTransactionInThreadLocal(final HttpServerExchange exchange, final Database database, final ServerSecurityUser user) {
    final HeaderValues sessionId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (sessionId != null && !sessionId.isEmpty()) {
      // LOOK UP FOR THE SESSION ID
      final HttpSession session = httpServer.getSessionManager().getSessionById(user, sessionId.getFirst());
      if (session == null) {
        exchange.setStatusCode(401);
        throw new TransactionException("Remote transaction not found or expired");
      }

      // FORCE THE RESET OF TL
      DatabaseContext.INSTANCE.init((DatabaseInternal) database, session.transaction);
      exchange.getResponseHeaders().put(SESSION_ID_HEADER, session.id);

      return session;
    }
    return null;
  }
}
