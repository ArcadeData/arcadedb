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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;
import com.arcadedb.server.ha.message.ServerShutdownRequest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.*;

public class PostServerCommandHandler extends DatabaseAbstractHandler {
  public PostServerCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean requiresDatabase() {
    return false;
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database) throws IOException {
    final JSONObject payload = new JSONObject(parseRequestPayload(exchange));

    final String command = payload.has("command") ? payload.getString("command") : null;
    if (command == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Server command is null\"}");
      return;
    }

    httpServer.getServer().getServerMetrics().meter("http.server-command").mark();

    if (command.startsWith("shutdown"))
      shutdownServer(command);
    else if (command.startsWith("disconnect "))
      disconnectServer(command);
    else {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Server command not valid\"}");
      return;
    }

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : \"ok\"}");
  }

  private void shutdownServer(final String command) throws IOException {
    if (command.equals("shutdown")) {
      // SHUTDOWN CURRENT SERVER
      httpServer.getServer().stop();
    } else if (command.startsWith("shutdown ")) {
      final String serverName = command.substring("shutdown ".length());
      final Leader2ReplicaNetworkExecutor replica = httpServer.getServer().getHA().getReplica(serverName);

      final Binary buffer = new Binary();
      httpServer.getServer().getHA().getMessageFactory().serializeCommand(new ServerShutdownRequest(), buffer, -1);
      replica.sendMessage(buffer);
    }
  }

  private void disconnectServer(final String command) {
    final String serverName = command.substring("disconnect ".length());
    final Leader2ReplicaNetworkExecutor replica = httpServer.getServer().getHA().getReplica(serverName);
    replica.close();
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
