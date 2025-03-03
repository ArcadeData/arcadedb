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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;
import com.arcadedb.server.ha.Replica2LeaderNetworkExecutor;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.ha.message.ServerShutdownRequest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;

import java.io.IOException;
import java.rmi.ServerException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class PostServerCommandHandler extends AbstractServerHttpHandler {
  final String LIST_DATABASES       = "list databases";
  final String SHUTDOWN             = "shutdown";
  final String CREATE_DATABASE      = "create database";
  final String DROP_DATABASE        = "drop database";
  final String CLOSE_DATABASE       = "close database";
  final String OPEN_DATABASE        = "open database";
  final String CREATE_USER          = "create user";
  final String DROP_USER            = "drop user";
  final String CONNECT_CLUSTER      = "connect cluster";
  final String DISCONNECT_CLUSTER   = "disconnect cluster";
  final String SET_DATABASE_SETTING = "set database setting";
  final String SET_SERVER_SETTING   = "set server setting";
  final String GET_SERVER_EVENTS    = "get server events";
  final String ALIGN_DATABASE       = "align database";

  public PostServerCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user) throws IOException {

    final JSONObject payload = new JSONObject(parseRequestPayload(exchange));

    final String command = payload.has("command") ? payload.getString("command").trim() : null;
    if (command == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Server command is null\"}");

    final JSONObject response = createResult(user, null).put("result", "ok");

    final String command_lc = command.toLowerCase(Locale.ENGLISH).trim();

    if (command_lc.equals(LIST_DATABASES))
      return listDatabases(user);
    else
      checkRootUser(user);

    if (command_lc.startsWith(SHUTDOWN))
      shutdownServer(extractTarget(command, SHUTDOWN));
    else if (command_lc.startsWith(CREATE_DATABASE))
      createDatabase(extractTarget(command, CREATE_DATABASE));
    else if (command_lc.startsWith(DROP_DATABASE))
      dropDatabase(extractTarget(command, DROP_DATABASE));
    else if (command_lc.startsWith(CLOSE_DATABASE))
      closeDatabase(extractTarget(command, CLOSE_DATABASE));
    else if (command_lc.startsWith(OPEN_DATABASE))
      openDatabase(extractTarget(command, OPEN_DATABASE));
    else if (command_lc.startsWith(CREATE_USER))
      createUser(extractTarget(command, CREATE_USER));
    else if (command_lc.startsWith(DROP_USER))
      dropUser(extractTarget(command, DROP_USER));
    else if (command_lc.startsWith(CONNECT_CLUSTER)) {
      if (!connectCluster(extractTarget(command, CONNECT_CLUSTER), exchange))
        return null;
    } else if (command_lc.equals(DISCONNECT_CLUSTER))
      disconnectCluster();
    else if (command_lc.startsWith(SET_DATABASE_SETTING))
      setDatabaseSetting(extractTarget(command, SET_DATABASE_SETTING));
    else if (command_lc.startsWith(SET_SERVER_SETTING))
      setServerSetting(extractTarget(command, SET_SERVER_SETTING));
    else if (command_lc.startsWith(GET_SERVER_EVENTS))
      response.put("result", getServerEvents(extractTarget(command, GET_SERVER_EVENTS)));
    else if (command_lc.startsWith(ALIGN_DATABASE))
      alignDatabase(extractTarget(command, ALIGN_DATABASE));
    else {
      Metrics.counter("http.server-command.invalid").increment();

      return new ExecutionResponse(400, "{ \"error\" : \"Server command not valid\"}");
    }

    return new ExecutionResponse(200, response.toString());
  }

  private String extractTarget(String command, String keyword) {
    final int pos = command.indexOf(keyword);
    if (pos == -1)
      return "";

    return command.substring(pos + keyword.length()).trim();
  }

  private ExecutionResponse listDatabases(final ServerSecurityUser user) {
    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.list-databases").increment();
    ;

    final Set<String> installedDatabases = new HashSet<>(server.getDatabaseNames());
    final Set<String> allowedDatabases = user.getAuthorizedDatabases();

    if (!allowedDatabases.contains("*"))
      installedDatabases.retainAll(allowedDatabases);

    final JSONObject response = createResult(user, null).put("result", new JSONArray(installedDatabases));

    return new ExecutionResponse(200, response.toString());
  }

  private void shutdownServer(final String serverName) throws IOException {
    Metrics.counter("http.server-shutdown").increment();
    ;

    if (serverName.isEmpty()) {
      // SHUTDOWN CURRENT SERVER
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          httpServer.getServer().stop();
          System.exit(0);
        }
      }, 1000);
    } else {
      final HAServer ha = getHA();
      final Leader2ReplicaNetworkExecutor replica = ha.getReplica(serverName);
      if (replica == null)
        throw new ServerException("Cannot contact server '" + serverName + "' from the current server");

      final Binary buffer = new Binary();
      ha.getMessageFactory().serializeCommand(new ServerShutdownRequest(), buffer, -1);
      replica.sendMessage(buffer);
    }
  }

  private void createDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.create-database").increment();
    ;

    final ServerDatabase db = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);

    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      final ReplicatedDatabase replicatedDatabase = (ReplicatedDatabase) db.getWrappedDatabaseInstance();
      replicatedDatabase.createInReplicas();
    }
  }

  private void dropDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final ServerDatabase database = httpServer.getServer().getDatabase(databaseName);

    Metrics.counter("http.drop-database").increment();
    ;

    database.getEmbedded().drop();
    httpServer.getServer().removeDatabase(database.getName());
  }

  private void closeDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final ServerDatabase database = httpServer.getServer().getDatabase(databaseName);
    database.getEmbedded().close();

    Metrics.counter("http.close-database").increment();
    ;
    httpServer.getServer().removeDatabase(database.getName());
  }

  private void openDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    httpServer.getServer().getDatabase(databaseName);
    Metrics.counter("http.open-database").increment();
    ;
  }

  private void createUser(final String payload) {
    final JSONObject json = new JSONObject(payload);

    if (!json.has("name"))
      throw new IllegalArgumentException("User name is null");

    final String userPassword = json.getString("password");
    if (userPassword.length() < 4)
      throw new ServerSecurityException("User password must be 5 minimum characters");
    if (userPassword.length() > 256)
      throw new ServerSecurityException("User password cannot be longer than 256 characters");

    json.put("password", httpServer.getServer().getSecurity().encodePassword(userPassword));

    Metrics.counter("http.create-user").increment();
    ;

    httpServer.getServer().getSecurity().createUser(json);
  }

  private void dropUser(final String userName) {
    if (userName.isEmpty())
      throw new IllegalArgumentException("User name was missing");

    Metrics.counter("http.drop-user").increment();
    ;

    final boolean result = httpServer.getServer().getSecurity().dropUser(userName);
    if (!result)
      throw new IllegalArgumentException("User '" + userName + "' not found on server");
  }

  private boolean connectCluster(final String serverAddress, final HttpServerExchange exchange) {
    final HAServer ha = getHA();

    Metrics.counter("http.connect-cluster").increment();
    ;

    return ha.connectToLeader(serverAddress, exception -> {
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send("{ \"error\" : \"" + exception.getMessage() + "\"}");
      return null;
    });
  }

  private void disconnectCluster() {
    Metrics.counter("http.server-disconnect").increment();
    ;
    final HAServer ha = getHA();

    final Replica2LeaderNetworkExecutor leader = ha.getLeader();
    if (leader != null)
      leader.close();
    else
      ha.disconnectAllReplicas();
  }

  private void setDatabaseSetting(final String triple) throws IOException {

    final String tripleTrimmed = triple.trim();
    final Integer firstSpace = tripleTrimmed.indexOf(" ");
    if (firstSpace == -1)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    final String pairTrimmed = tripleTrimmed.substring(firstSpace).trim();
    final Integer secondSpace = pairTrimmed.indexOf(" ");
    if (secondSpace == -1)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    final String db = tripleTrimmed.substring(0, firstSpace);
    final String key = pairTrimmed.substring(0, secondSpace);
    final String value = pairTrimmed.substring(secondSpace);

    final DatabaseInternal database = httpServer.getServer().getDatabase(db);
    database.getConfiguration().setValue(key, value);
    database.saveConfiguration();
  }

  private void setServerSetting(final String pair) {

    final String pairTrimmed = pair.trim();

    final Integer firstSpace = pairTrimmed.indexOf(" ");
    if (firstSpace == -1)
      throw new IllegalArgumentException("Expected <key> <value>");

    final String key = pairTrimmed.substring(0, firstSpace);
    final String value = pairTrimmed.substring(firstSpace);

    httpServer.getServer().getConfiguration().setValue(key, value);
  }

  private String getServerEvents(final String fileName) {
    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.get-server-events").increment();
    ;

    final JSONArray events = fileName.isEmpty() ?
        server.getEventLog().getCurrentEvents() :
        server.getEventLog().getEvents(fileName);
    final JSONArray files = server.getEventLog().getFiles();

    return "{ \"events\": " + events + ", \"files\": " + files + " }";
  }

  private void alignDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final Database database = httpServer.getServer().getDatabase(databaseName);

    Metrics.counter("http.align-database").increment();
    ;

    database.command("sql", "align database");
  }

  private void checkServerIsLeaderIfInHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());
  }

  private HAServer getHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha == null)
      throw new CommandExecutionException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
  }
}
