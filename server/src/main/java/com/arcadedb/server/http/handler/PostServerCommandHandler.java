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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.security.AuthorizationUtils;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;
import com.arcadedb.server.ha.Replica2LeaderNetworkExecutor;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.ha.message.ServerShutdownRequest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.security.oidc.ArcadeRole;
import com.arcadedb.server.security.oidc.KeycloakClient;
import com.arcadedb.server.security.oidc.role.CRUDPermission;
import com.arcadedb.server.security.oidc.role.DatabaseAdminRole;
import com.arcadedb.server.security.oidc.role.RoleType;
import com.arcadedb.server.security.oidc.role.ServerAdminRole;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nimbusds.oauth2.sdk.util.StringUtils;

import io.undertow.server.HttpServerExchange;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.rmi.*;
import java.util.*;

public class PostServerCommandHandler extends AbstractHandler {
  public PostServerCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user)
      throws IOException {
    String payloadString = parseRequestPayload(exchange);

    if (payloadString != null && StringUtils.isNotBlank(payloadString)) {
      final var payload = JsonParser.parseString(payloadString).getAsJsonObject();

      final String command = payload.has("command") ? payload.get("command").getAsString() : null;
      if (command == null)
        return new ExecutionResponse(400, "{ \"error\" : \"Server command is null\"}");

      // List of server commands that will check their own user permissions
      var excludedTopLevelCheckComamnds = List.of("list databases", "create database", "drop database");

      var isExcludedCommand = excludedTopLevelCheckComamnds.stream()
          .anyMatch(excludedCommand -> command.startsWith(excludedCommand));

      // If not a command that manages its own permissions, check if user has sa role
      if (!isExcludedCommand
          && !httpServer.getServer().getSecurity().checkUserHasAnyServerAdminRole(user, List.of(ServerAdminRole.ALL))) {
        throw new ServerSecurityException(
            String.format("User '%s' is not authorized to execute server command '%s'", user.getName(), command));
      }

      if (command.startsWith("shutdown"))
        shutdownServer(command);
      else if (command.startsWith("create database "))
        createDatabase(payload, user);
      else if (command.equals("list databases")) {
        return listDatabases(user);
      } else if (command.startsWith("drop database "))
        dropDatabase(command, user);
      else if (command.startsWith("close database "))
        closeDatabase(command);
      else if (command.startsWith("open database "))
        openDatabase(command);
      else if (command.startsWith("create user ")) {
        // createUser(command);
        return new ExecutionResponse(400, "{ \"error\" : \"Please create new users through keycloak.\"}");
      } else if (command.startsWith("drop user "))
        dropUser(command);
      else if (command.startsWith("connect cluster ")) {
        if (!connectCluster(command, exchange))
          return null;
      } else if (command.equals("disconnect cluster"))
        disconnectCluster();
      else if (command.startsWith("set database setting "))
        setDatabaseSetting(command);
      else if (command.startsWith("set server setting "))
        setServerSetting(command);
      else if (command.startsWith("get server events"))
        return getServerEvents(command);
      else if (command.startsWith("align database "))
        alignDatabase(command);
      else {
        httpServer.getServer().getServerMetrics().meter("http.server-command.invalid").hit();
        return new ExecutionResponse(400, "{ \"error\" : \"Server command not valid\"}");
      }
    }

    return new ExecutionResponse(200, "{ \"result\" : \"ok\"}");
  }

  private void setDatabaseSetting(final String command) throws IOException {
    final String pair = command.substring("set database setting ".length());
    final String[] dbKeyValue = pair.split(" ");
    if (dbKeyValue.length != 3)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    final DatabaseInternal database = (DatabaseInternal) httpServer.getServer().getDatabase(dbKeyValue[0]);
    database.getConfiguration().setValue(dbKeyValue[1], dbKeyValue[2]);
    database.saveConfiguration();
  }

  private void setServerSetting(final String command) {
    // TODO check if user has sa role, or is root

    final String pair = command.substring("set server setting ".length());
    final String[] keyValue = pair.split(" ");
    if (keyValue.length != 2)
      throw new IllegalArgumentException("Expected <key> <value>");

    httpServer.getServer().getConfiguration().setValue(keyValue[0], keyValue[1]);
  }

  private void shutdownServer(final String command) throws IOException {
    httpServer.getServer().getServerMetrics().meter("http.server-shutdown").hit();

    if (command.equals("shutdown")) {
      // SHUTDOWN CURRENT SERVER
      httpServer.getServer().stop();
    } else if (command.startsWith("shutdown ")) {
      final String serverName = command.substring("shutdown ".length()).trim();
      final HAServer ha = getHA();
      final Leader2ReplicaNetworkExecutor replica = ha.getReplica(serverName);
      if (replica == null)
        throw new ServerException("Cannot contact server '" + serverName + "' from the current server");

      final Binary buffer = new Binary();
      ha.getMessageFactory().serializeCommand(new ServerShutdownRequest(), buffer, -1);
      replica.sendMessage(buffer);
    }
  }

  private void disconnectCluster() {
    httpServer.getServer().getServerMetrics().meter("http.server-disconnect").hit();
    final HAServer ha = getHA();

    final Replica2LeaderNetworkExecutor leader = ha.getLeader();
    if (leader != null)
      leader.close();
    else
      ha.disconnectAllReplicas();
  }

  private boolean connectCluster(final String command, final HttpServerExchange exchange) {
    final HAServer ha = getHA();

    httpServer.getServer().getServerMetrics().meter("http.connect-cluster").hit();

    final String serverAddress = command.substring("connect cluster ".length());
    return ha.connectToLeader(serverAddress, exception -> {
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"" + exception.getMessage() + "\"}");
      return null;
    });
  }

  private boolean isNotNullOrEmpty(String toCheck) {
    return toCheck != null && !toCheck.isEmpty();
  }

  private void createDatabase(final JsonObject command, final ServerSecurityUser user) {

    // check if user has create database role, or is root
    var anyRequiredRoles = List.of(ServerAdminRole.CREATE_DATABASE, ServerAdminRole.ALL);
    if (httpServer.getServer().getSecurity().checkUserHasAnyServerAdminRole(user, anyRequiredRoles)) {
      checkServerIsLeaderIfInHA();

      // TODO check if required database info is present in the JSON options payload, including owner, classificaiton, public/private
      final String databaseName = command.get("command").getAsString().substring("create database ".length()).trim();

      final String OPTIONS = "options";
      final String CLASSIFICATION = "classification";
      final String OWNER = "owner";
      final String VISIBILITY = "visibility";

      if (databaseName.isEmpty())
        throw new IllegalArgumentException("Database name empty");
      // validate json payload, needs command and options object
      if (!command.has("command"))
        throw new IllegalArgumentException("Missing command");
      if (!command.has(OPTIONS))
        throw new IllegalArgumentException(String.format("Missing %s object", OPTIONS));

      var options = command.get(OPTIONS).getAsJsonObject();

      if (!options.has(CLASSIFICATION) && !isNotNullOrEmpty(options.get(CLASSIFICATION).getAsString())) {
        throw new IllegalArgumentException(String.format("Missing %s.%s", OPTIONS, CLASSIFICATION));
      }
      if (!options.has(OWNER) && !isNotNullOrEmpty(options.get(OWNER).getAsString())) {
        throw new IllegalArgumentException(String.format("Missing %s.%s", OPTIONS, OWNER));
      }
      if (options.has(VISIBILITY) && !isNotNullOrEmpty(options.get(VISIBILITY).getAsString())) {
        throw new IllegalArgumentException(String.format("Missing %s.%s", OPTIONS, VISIBILITY));
      }

      // Handle operational database metadata
      String classification = options.get(CLASSIFICATION).getAsString();

      // TODO cap acceptable classifications at the deployment level.
      // make static util method for this
      
      if (!AuthorizationUtils.isClassificationValidForDeployment(classification)) {
        throw new IllegalArgumentException(String.format("Invalid classification %s. Acceptable values are %s", classification));
      }
      
      String owner = options.has(OWNER) ? options.get(OWNER).getAsString() : null;
      boolean isPublic = options.has(VISIBILITY) ? options.get(VISIBILITY).getAsString().equalsIgnoreCase("public") : false;

      final ArcadeDBServer server = httpServer.getServer();
      server.getServerMetrics().meter("http.create-database").hit();

      final DatabaseInternal db = server.createDatabase(databaseName, PaginatedFile.MODE.READ_WRITE, classification, owner, isPublic);

      if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        ((ReplicatedDatabase) db).createInReplicas();

      // Create and assign new role granting all data, schema, and settings on the new
      // database to the user who created it.
      ArcadeRole dataRole = new ArcadeRole(RoleType.USER, databaseName, "*", CRUDPermission.getAll());
      ArcadeRole schemaRole = new ArcadeRole(RoleType.DATABASE_ADMIN, databaseName, DatabaseAdminRole.ALL);
      createAndAssignRoleToUser(dataRole, user.getName());
      createAndAssignRoleToUser(schemaRole, user.getName());

      if (options.has("importOntology") && options.get("importOntology").getAsBoolean()) {
        // get the ontology file from project resources
        try {
          InputStream inputStream = PostServerCommandHandler.class.getResourceAsStream("/ontology");
          String data = readFromInputStream(inputStream);

          // get JSONArray from data
          JSONArray jsonArray = new JSONArray(data);

          // iterate over jsonArray and insert into database
          for (int i = 0; i < jsonArray.length(); i++) {
            db.command("sql", jsonArray.getString(i));
          }
        } catch (Exception e) {
          throw new ServerException("Error importing ontology: " + e.getMessage());
        }
      }
    } else {
      throw new ServerSecurityException("Create database operation not allowed for user " + user.getName());
    }
  }

  private String readFromInputStream(InputStream inputStream) throws IOException {
    StringBuilder resultStringBuilder = new StringBuilder();
    try (BufferedReader br
      = new BufferedReader(new InputStreamReader(inputStream))) {
        String line;
        while ((line = br.readLine()) != null) {
            resultStringBuilder.append(line).append("\n");
        }
    }
  return resultStringBuilder.toString();
}

  private void createAndAssignRoleToUser(ArcadeRole arcadeRole, String username) {
    String newRole = arcadeRole.getKeycloakRoleName();
    KeycloakClient.createRole(newRole);
    KeycloakClient.assignRoleToUser(newRole, username);
    httpServer.getServer().getSecurity().appendArcadeRoleToUserCache(username, newRole);
  }

  private ExecutionResponse getServerEvents(final String command) {
    final String fileName = command.substring("get server events".length()).trim();

    final ArcadeDBServer server = httpServer.getServer();
    server.getServerMetrics().meter("http.get-server-events").hit();

    final JSONArray events = fileName.isEmpty() ? server.getEventLog().getCurrentEvents()
        : server.getEventLog().getEvents(fileName);
    final JSONArray files = server.getEventLog().getFiles();

    return new ExecutionResponse(200, "{ \"result\" : { \"events\": " + events + ", \"files\": " + files + " } }");
  }

  private ExecutionResponse listDatabases(final ServerSecurityUser user) {
    // All users can list databases, no check is needed
    final ArcadeDBServer server = httpServer.getServer();
    server.getServerMetrics().meter("http.list-databases").hit();

    final Set<String> installedDatabases = new HashSet<>(server.getDatabaseNames());
    final Set<String> allowedDatabases = user.getAuthorizedDatabases();

    if (!allowedDatabases.contains("*"))
      installedDatabases.retainAll(allowedDatabases);

    return new ExecutionResponse(200, "{ \"result\" : " + new JSONArray(installedDatabases) + "}");
  }

  private void alignDatabase(final String command) {
    final String databaseName = command.substring("align database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final Database database = httpServer.getServer().getDatabase(databaseName);

    httpServer.getServer().getServerMetrics().meter("http.align-database").hit();

    database.command("sql", "align database");
  }

  private void dropDatabase(final String command, final ServerSecurityUser user) {
    final String databaseName = command.substring("drop database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    // check if user has create database role, or is root
    var anyRequiredRoles = List.of(ServerAdminRole.DROP_DATABASE, ServerAdminRole.ALL);
    if (httpServer.getServer().getSecurity().checkUserHasAnyServerAdminRole(user, anyRequiredRoles)) {
      final Database database = httpServer.getServer().getDatabase(databaseName);

      httpServer.getServer().getServerMetrics().meter("http.drop-database").hit();

      ((DatabaseInternal) database).getEmbedded().drop();
      httpServer.getServer().removeDatabase(database.getName());

      /**
       * TODO in keycloak:
       * 1. delete all roles for the removed database
       */
    }
  }

  private void closeDatabase(final String command) {
    final String databaseName = command.substring("close database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final Database database = httpServer.getServer().getDatabase(databaseName);
    ((DatabaseInternal) database).getEmbedded().close();

    httpServer.getServer().getServerMetrics().meter("http.close-database").hit();
    httpServer.getServer().removeDatabase(database.getName());
  }

  private void openDatabase(final String command) {
    final String databaseName = command.substring("open database ".length()).trim();
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    httpServer.getServer().getDatabase(databaseName);

    httpServer.getServer().getServerMetrics().meter("http.open-database").hit();
  }

  private void createUser(final String command) {
    // final String payload = command.substring("create user ".length()).trim();
    // final JSONObject json = new JSONObject(payload);

    // if (!json.has("name"))
    // throw new IllegalArgumentException("User name is null");

    // final String userPassword = json.getString("password");
    // if (userPassword.length() < 4)
    // throw new ServerSecurityException("User password must be 5 minimum
    // characters");
    // if (userPassword.length() > 256)
    // throw new ServerSecurityException("User password cannot be longer than 256
    // characters");

    // json.put("password",
    // httpServer.getServer().getSecurity().encodePassword(userPassword));

    // httpServer.getServer().getServerMetrics().meter("http.create-user").hit();

    // httpServer.getServer().getSecurity().createUser(json);
    throw new RuntimeException("Please create new user through keycloak");
  }

  private void dropUser(final String command) {
    final String userName = command.substring("drop user ".length()).trim();
    if (userName.isEmpty())
      throw new IllegalArgumentException("User name was missing");

    httpServer.getServer().getServerMetrics().meter("http.drop-user").hit();

    final boolean result = httpServer.getServer().getSecurity().dropUser(userName);
    if (!result)
      throw new IllegalArgumentException("User '" + userName + "' not found on server");
  }

  private void checkServerIsLeaderIfInHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server",
          ha.getLeaderName());
  }

  private HAServer getHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha == null)
      throw new CommandExecutionException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
  }
}
