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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.io.IOException;

public class PostBeginHandler extends DatabaseAbstractHandler {

  public PostBeginHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, ServerSecurityUser user, final Database database) throws IOException {
    final HeaderValues txId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (txId != null && !txId.isEmpty()) {
      final HttpSession tx = httpServer.getSessionManager().getSessionById(user, txId.getFirst());
      if (tx != null) {
        exchange.setStatusCode(401);
        exchange.getResponseSender().send("{ \"error\" : \"Transaction already started\" }");
        return;
      }
    }

    DatabaseContext.INSTANCE.init((DatabaseInternal) database);

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    final HttpSession session = httpServer.getSessionManager().createSession(user, tx);

    DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());

    exchange.getResponseHeaders().put(new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID), session.id);
    exchange.setStatusCode(204);
    exchange.getResponseSender().send("");
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
