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
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.*;

public abstract class DatabaseAbstractHandler extends AbstractHandler {
  public DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract void execute(HttpServerExchange exchange, ServerSecurityUser user, Database database) throws Exception;

  @Override
  public void execute(final HttpServerExchange exchange, ServerSecurityUser user) throws Exception {
    final Database database;
    if (openDatabase()) {
      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("{ \"error\" : \"Database parameter is null\"}");
        return;
      }

      database = httpServer.getServer().getDatabase(databaseName.getFirst());

      DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user.getDatabaseUser(database));

    } else
      database = null;

    try {

      execute(exchange, user, database);

    } finally {
      if (database != null)
        database.rollbackAllNested();
    }
  }

  protected boolean openDatabase() {
    return true;
  }
}
