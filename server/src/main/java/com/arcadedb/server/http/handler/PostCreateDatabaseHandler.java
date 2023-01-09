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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.*;

public class PostCreateDatabaseHandler extends DatabaseAbstractHandler {
  public PostCreateDatabaseHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean requiresDatabase() {
    return false;
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database) {
    final Deque<String> databaseNamePar = exchange.getQueryParameters().get("database");
    String databaseName = databaseNamePar.isEmpty() ? null : databaseNamePar.getFirst().trim();
    if (databaseName.isEmpty())
      databaseName = null;

    if (databaseName == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Database parameter is null\"}");
      return;
    }

    final ArcadeDBServer server = httpServer.getServer();
    if (!server.getHA().isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", server.getHA().getLeaderName());

    server.getServerMetrics().meter("http.create-database").mark();

    final DatabaseInternal db = server.createDatabase(databaseName, PaginatedFile.MODE.READ_WRITE);

    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      ((ReplicatedDatabase) db).createInReplicas();

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : \"ok\"}");
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
