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
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;
import com.arcadedb.server.ha.Replica2LeaderNetworkExecutor;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.ha.message.ServerShutdownRequest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.*;

public class PostServerCommandHandler extends AbstractHandler {
  public PostServerCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user) throws IOException {
    checkRootUser(user);

    final JSONObject payload = new JSONObject(parseRequestPayload(exchange));

    final String command = payload.has("command") ? payload.getString("command") : null;
    if (command == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Server command is null\"}");
      return;
    }

    if (command.startsWith("shutdown"))
      shutdownServer(command);
    else if (command.startsWith("create database "))
      createDatabase(command);
    else if (command.startsWith("drop database "))
      dropDatabase(command);
    else if (command.startsWith("close database "))
      closeDatabase(command);
    else if (command.startsWith("create user "))
      createUser(command);
    else if (command.startsWith("drop user "))
      dropUser(command);
    else if (command.startsWith("connect cluster "))
      connectCluster(command);
    else if (command.equals("disconnect cluster"))
      disconnectCluster();
    else {
      httpServer.getServer().getServerMetrics().meter("http.server-command.invalid").mark();
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Server command not valid\"}");
      return;
    }

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : \"ok\"}");
  }

  private void shutdownServer(final String command) throws IOException {
    httpServer.getServer().getServerMetrics().meter("http.server-shutdown").mark();

    if (command.equals("shutdown")) {
      // SHUTDOWN CURRENT SERVER
      httpServer.getServer().stop();
    } else if (command.startsWith("shutdown ")) {
      final String serverName = command.substring("shutdown ".length()).trim();
      final Leader2ReplicaNetworkExecutor replica = httpServer.getServer().getHA().getReplica(serverName);

      final Binary buffer = new Binary();
      httpServer.getServer().getHA().getMessageFactory().serializeCommand(new ServerShutdownRequest(), buffer, -1);
      replica.sendMessage(buffer);
    }
  }

  private void disconnectCluster() {
    httpServer.getServer().getServerMetrics().meter("http.server-disconnect").mark();
    final Replica2LeaderNetworkExecutor leader = httpServer.getServer().getHA().getLeader();
    if (leader != null)
      leader.close();
    else
      httpServer.getServer().getHA().disconnectAllReplicas();
  }

  private void connectCluster(final String command) {
    httpServer.getServer().getServerMetrics().meter("http.connect-cluster").mark();

    final String serverAddress = command.substring("connect cluster ".length());
    httpServer.getServer().getHA().connectToLeader(serverAddress);
  }

  private void createDatabase(final String command) {
    final String databaseName = command.substring("create database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    server.getServerMetrics().meter("http.create-database").mark();

    final DatabaseInternal db = server.createDatabase(databaseName, PaginatedFile.MODE.READ_WRITE);

    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      ((ReplicatedDatabase) db).createInReplicas();
  }

  private void dropDatabase(final String command) {
    final String databaseName = command.substring("drop database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final Database database = httpServer.getServer().getDatabase(databaseName);

    httpServer.getServer().getServerMetrics().meter("http.drop-database").mark();

    ((DatabaseInternal) database).getEmbedded().drop();
    httpServer.getServer().removeDatabase(database.getName());
  }

  private void closeDatabase(final String command) {
    final String databaseName = command.substring("close database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final Database database = httpServer.getServer().getDatabase(databaseName);
    ((DatabaseInternal) database).getEmbedded().close();

    httpServer.getServer().getServerMetrics().meter("http.close-database").mark();
    httpServer.getServer().removeDatabase(database.getName());
  }

  private void createUser(final String command) {
    final String payload = command.substring("create user ".length()).trim();
    final JSONObject json = new JSONObject(payload);

    if (!json.has("name"))
      throw new IllegalArgumentException("User name is null");

    final String userPassword = json.getString("password");
    if (userPassword.length() < 4)
      throw new ServerSecurityException("User password must be 5 minimum characters");
    if (userPassword.length() > 256)
      throw new ServerSecurityException("User password cannot be longer than 256 characters");

    json.put("password", httpServer.getServer().getSecurity().encodePassword(userPassword));

    httpServer.getServer().getServerMetrics().meter("http.create-user").mark();

    httpServer.getServer().getSecurity().createUser(json);
  }

  private void dropUser(final String command) {
    final String userName = command.substring("drop user ".length()).trim();
    if (userName.isEmpty())
      throw new IllegalArgumentException("User name was missing");

    httpServer.getServer().getServerMetrics().meter("http.drop-user").mark();

    final boolean result = httpServer.getServer().getSecurity().dropUser(userName);
    if (!result)
      throw new IllegalArgumentException("User '" + userName + "' not found on server");
  }

  private void checkServerIsLeaderIfInHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());

  }
}
