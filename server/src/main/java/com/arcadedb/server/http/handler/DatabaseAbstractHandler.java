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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.util.*;

public abstract class DatabaseAbstractHandler extends AbstractHandler {
  private static final HttpString SESSION_ID_HEADER = new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID);

  protected DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract void execute(HttpServerExchange exchange, ServerSecurityUser user, Database database) throws Exception;

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user) throws Exception {
    final Database database;
    HttpSession activeSession = null;
    boolean atomicTransaction = false;
    if (requiresDatabase()) {
      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("{ \"error\" : \"Database parameter is null\"}");
        return;
      }

      database = httpServer.getServer().getDatabase(databaseName.getFirst());

      activeSession = setTransactionInThreadLocal(exchange, database, user, false);

      if (requiresTransaction() && activeSession == null) {
        atomicTransaction = true;
        database.begin();
      }

    } else
      database = null;

    try {
      execute(exchange, user, database);
    } finally {

      if (activeSession != null) {
        // TRANSACTION FOUND, REMOVE THE TRANSACTION TO BE REUSED IN ANOTHER REQUEST
        activeSession.endUsage();
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      } else if (database != null) {
        if (atomicTransaction)
          // STARTED ATOMIC TRANSACTION, COMMIT
          database.commit();
        else
          // NO TRANSACTION, ROLLBACK TO MAKE SURE ANY PENDING OPERATION IS REMOVED
          database.rollbackAllNested();
      }
    }
  }

  protected boolean requiresDatabase() {
    return true;
  }

  protected boolean requiresTransaction() {
    return true;
  }

  protected HttpSession setTransactionInThreadLocal(final HttpServerExchange exchange, final Database database, ServerSecurityUser user,
      final boolean mandatory) {
    final HeaderValues sessionId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (sessionId == null || sessionId.isEmpty()) {
      if (mandatory) {
        exchange.setStatusCode(401);
        exchange.getResponseSender().send("{ \"error\" : \"Transaction id not found in request headers\" }");
      }
      return null;
    }

    final HttpSession session = httpServer.getTransactionManager().getSessionById(user, sessionId.getFirst());
    if (session == null) {
      if (mandatory) {
        exchange.setStatusCode(401);
        exchange.getResponseSender().send("{ \"error\" : \"Transaction not found or expired\" }");
        return null;
      }
    }

    if (session != null) {
      // FORCE THE RESET OF TL
      session.use(user);
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.init((DatabaseInternal) database, session.transaction);
      current.setCurrentUser(user != null ? user.getDatabaseUser(database) : null);
      exchange.getResponseHeaders().put(SESSION_ID_HEADER, session.id);
    }

    return session;
  }
}
