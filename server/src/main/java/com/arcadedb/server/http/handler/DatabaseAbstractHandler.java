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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public abstract class DatabaseAbstractHandler extends AbstractServerHttpHandler {
  private static final HttpString SESSION_ID_HEADER = new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID);

  protected DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, Database database,
      JSONObject payload) throws Exception;

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload)
      throws Exception {
    final DatabaseInternal database;
    HttpSession activeSession = null;
    boolean atomicTransaction = false;

    DatabaseContext.DatabaseContextTL current;

    if (requiresDatabase()) {
      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty())
        return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");

      database = httpServer.getServer().getDatabase(databaseName.getFirst(), false, false);

      current = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
      if (current != null && !current.transactions.isEmpty() && current.transactions.get(0).isActive()) {
        LogManager.instance().log(this, Level.WARNING, "Found a pending transaction from a previous operation. Rolling it back...");
        cleanTL(database, current);
      }

      activeSession = setTransactionInThreadLocal(exchange, database, user);

      current = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
      if (current == null)
        // INITIALIZE THE DATABASE CONTEXT
        current = DatabaseContext.INSTANCE.init(database);

      final SecurityDatabaseUser currentUser = current.getCurrentUser();
      if (currentUser == null || !currentUser.equals(user.getDatabaseUser(database)))
        current.setCurrentUser(user != null ? user.getDatabaseUser(database) : null);

      // Warn if autoCommit parameter conflicts with session
      if (activeSession != null && payload != null && payload.has("autoCommit")) {
        final boolean explicitAutoCommit = payload.getBoolean("autoCommit");
        if (explicitAutoCommit) {
          LogManager.instance().log(this, Level.WARNING,
            "autoCommit parameter 'true' ignored: session transaction is already active (session ID: %s)",
            activeSession.id);
        }
      }

      final Boolean explicitAutoCommit = extractAutoCommitParameter(payload);
      atomicTransaction = determineAtomicTransaction(explicitAutoCommit, activeSession);

    } else
      database = null;

    final int retries = payload != null && !payload.isNull("retries") ? payload.getInt("retries") : 1;

    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();
    try {
      boolean finalAtomicTransaction = atomicTransaction;
      if (activeSession != null) {
        // EXECUTE THE CODE LOCKING THE CURRENT SESSION. THIS AVOIDS USING THE SAME SESSION FROM MULTIPLE THREADS AT THE SAME TIME
        activeSession.execute(user, () -> {
          if (finalAtomicTransaction) {
            database.transaction(() -> {
              try {
                response.set(execute(exchange, user, database, payload));
              } catch (Exception e) {
                throw new TransactionException("Error on executing command", e);
              }
            }, false, retries);
          } else
            response.set(execute(exchange, user, database, payload));
          return null;
        });
      } else {
        if (finalAtomicTransaction) {
          database.transaction(() -> {
            try {
              response.set(execute(exchange, user, database, payload));
            } catch (Exception e) {
              throw new TransactionException("Error on executing command", e);
            }
          }, false, retries);
        } else
          response.set(execute(exchange, user, database, payload));
      }

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
      current = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());

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

  /**
   * Extracts the autoCommit parameter from the request payload.
   *
   * @param payload The request payload (may be null)
   * @return null if not specified, true to force atomic transaction, false to disable transaction
   */
  protected Boolean extractAutoCommitParameter(final JSONObject payload) {
    if (payload == null)
      return null;

    if (payload.has("autoCommit"))
      return payload.getBoolean("autoCommit");

    return null;
  }

  /**
   * Determines whether to use an atomic transaction based on the explicit parameter,
   * session state, and handler's default transaction requirement.
   *
   * @param explicitAutoCommit null (auto), true (force), or false (disable)
   * @param activeSession The active session if any
   * @return true if atomic transaction should be used
   */
  protected boolean determineAtomicTransaction(final Boolean explicitAutoCommit, final HttpSession activeSession) {
    // If there's an active session, never use atomic transaction (session manages the transaction)
    if (activeSession != null)
      return false;

    // Explicit parameter takes precedence
    if (explicitAutoCommit != null)
      return explicitAutoCommit;

    // Fall back to handler's default behavior
    return requiresTransaction();
  }

  protected HttpSession setTransactionInThreadLocal(final HttpServerExchange exchange, final Database database,
      final ServerSecurityUser user) {
    final HeaderValues sessionId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (sessionId != null && !sessionId.isEmpty()) {
      // LOOK UP FOR THE SESSION ID
      final HttpSession session = httpServer.getSessionManager().getSessionById(user, sessionId.get(0));
      if (session == null) {
        exchange.setStatusCode(StatusCodes.UNAUTHORIZED);
        throw new TransactionException("Remote transaction '" + sessionId.getFirst() + "' not found or expired");
      }

      // FORCE THE RESET OF TL
      DatabaseContext.INSTANCE.init((DatabaseInternal) database, session.transaction);
      exchange.getResponseHeaders().put(SESSION_ID_HEADER, session.id);

      return session;
    }
    return null;
  }
}
