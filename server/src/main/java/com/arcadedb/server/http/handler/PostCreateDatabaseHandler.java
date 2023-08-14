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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.*;

/**
 * Creates a new database on a server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @Deprecated Use the generic @see PostServerCommandHandler
 */
@Deprecated
public class PostCreateDatabaseHandler extends AbstractHandler {
  public PostCreateDatabaseHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    checkRootUser(user);

    final Deque<String> databaseNamePar = exchange.getQueryParameters().get("database");
    String databaseName = databaseNamePar.isEmpty() ? null : databaseNamePar.getFirst().trim();
    if (databaseName.isEmpty())
      databaseName = null;

    if (databaseName == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");

    final ArcadeDBServer server = httpServer.getServer();
    if (!server.getHA().isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", server.getHA().getLeaderName());

    server.getServerMetrics().meter("http.create-database").hit();

    final DatabaseInternal db = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);

    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      final ReplicatedDatabase replicatedDatabase = (ReplicatedDatabase) db.getEmbedded();
      replicatedDatabase.createInReplicas();
    }

    return new ExecutionResponse(200, "{ \"result\" : \"ok\"}");
  }
}
