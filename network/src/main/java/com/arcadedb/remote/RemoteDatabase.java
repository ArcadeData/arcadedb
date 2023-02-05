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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RWLockContext;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

public class RemoteDatabase extends RWLockContext implements BasicDatabase {
  public static final String ARCADEDB_SESSION_ID = "arcadedb-session-id";

  public static final  int                         DEFAULT_PORT              = 2480;
  private final        String                      originalServer;
  private final        int                         originalPort;
  private              int                         apiVersion                = 1;
  private final        ContextConfiguration        configuration;
  private final        String                      databaseName;
  private final        String                      userName;
  private final        String                      userPassword;
  private final        List<Pair<String, Integer>> replicaServerList         = new ArrayList<>();
  private              String                      currentServer;
  private              int                         currentPort;
  private              CONNECTION_STRATEGY         connectionStrategy        = CONNECTION_STRATEGY.ROUND_ROBIN;
  private              Pair<String, Integer>       leaderServer;
  private              int                         currentReplicaServerIndex = -1;
  private              int                         timeout;
  private static final String                      protocol                  = "http";
  private static final String                      charset                   = "UTF-8";
  private              String                      sessionId;

  public List<String> getReplicaAddresses() {
    return replicaServerList.stream().map((e) -> e.getFirst() + ":" + e.getSecond()).collect(Collectors.toList());
  }

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN
  }

  public interface Callback {
    Object call(HttpURLConnection iArgument, JSONObject response) throws Exception;
  }

  public RemoteDatabase(final String server, final int port, final String databaseName, final String userName, final String userPassword) {
    this(server, port, databaseName, userName, userPassword, new ContextConfiguration());
  }

  public RemoteDatabase(final String server, final int port, final String databaseName, final String userName, final String userPassword,
      final ContextConfiguration configuration) {
    this.originalServer = server;
    this.originalPort = port;

    this.currentServer = originalServer;
    this.currentPort = originalPort;

    this.databaseName = databaseName;
    this.userName = userName;
    this.userPassword = userPassword;

    this.configuration = configuration;
    this.timeout = this.configuration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

    requestClusterConfiguration();
  }

  @Override
  public String getName() {
    return databaseName;
  }

  public void create() {
    serverCommand("POST", "create database " + databaseName, true, true, null);
  }

  public List<String> databases() {
    return (List<String>) serverCommand("POST", "list databases", true, true, (connection, response) -> response.getJSONArray("result").toList());
  }

  public boolean exists() {
    return (boolean) httpCommand("GET", databaseName, "exists", "SQL", null, null, false, true, (connection, response) -> response.getBoolean("result"));
  }

  @Override
  public void close() {
    sessionId = null;
  }

  @Override
  public void drop() {
    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("server"));
      setRequestPayload(connection, new JSONObject().put("command", "drop database " + databaseName));
      connection.connect();
      if (connection.getResponseCode() != 200) {
        final Exception detail = manageException(connection, "drop database");
        throw new RuntimeException("Error on deleting database: " + connection.getResponseMessage(), detail);
      }

    } catch (final Exception e) {
      throw new RuntimeException("Error on deleting database", e);
    }
    close();
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    return new RemoteMutableDocument(this, typeName);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    return new RemoteMutableVertex(this, typeName);
  }

  @Override
  public void transaction(final BasicDatabase.TransactionScope txBlock) {
    transaction(txBlock, true, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES));
  }

  @Override
  public boolean transaction(final BasicDatabase.TransactionScope txBlock, final boolean joinCurrentTransaction) {
    return transaction(txBlock, joinCurrentTransaction, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES));
  }

  @Override
  public boolean transaction(final BasicDatabase.TransactionScope txBlock, final boolean joinCurrentTransaction, int attempts) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ArcadeDBException lastException = null;

    if (attempts < 1)
      attempts = 1;

    for (int retry = 0; retry < attempts; ++retry) {
      boolean createdNewTx = true;
      try {
        if (joinCurrentTransaction && isTransactionActive())
          createdNewTx = false;
        else
          begin();

        txBlock.execute();

        if (createdNewTx)
          commit();

        return createdNewTx;

      } catch (final NeedRetryException | DuplicatedKeyException e) {
        // RETRY
        lastException = e;
      } catch (final Exception e) {
        try {
          rollback();
        } catch (final Throwable t) {
          // IGNORE IT
        }
        throw e;
      }
    }

    throw lastException;
  }

  public boolean isTransactionActive() {
    return sessionId != null;
  }

  public void begin() {
    if (sessionId != null)
      throw new TransactionException("Transaction already begun");

    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("begin", databaseName));
      connection.connect();
      if (connection.getResponseCode() != 204) {
        final Exception detail = manageException(connection, "begin transaction");
        throw new TransactionException("Error on transaction begin", detail);
      }
      sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID);
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction begin", e);
    }
  }

  public void commit() {
    if (sessionId == null)
      throw new TransactionException("Transaction not begun");
    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("commit", databaseName));
      connection.connect();
      if (connection.getResponseCode() != 204) {
        final Exception detail = manageException(connection, "commit transaction");
        throw new TransactionException("Error on transaction commit", detail);
      }
      sessionId = null;
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction commit", e);
    }
  }

  public void rollback() {
    if (sessionId == null)
      throw new TransactionException("Transaction not begun");

    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("rollback", databaseName));
      connection.connect();
      if (connection.getResponseCode() != 204) {
        final Exception detail = manageException(connection, "rollback transaction");
        throw new TransactionException("Error on transaction rollback", detail);
      }
      sessionId = null;
    } catch (final Exception e) {
      throw new TransactionException("Error on transaction rollback", e);
    }
  }

  public Record lookupByRID(final RID rid) {
    return lookupByRID(rid, true);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    try {
      final HttpURLConnection connection = createConnection("GET", getUrl("document", databaseName + "/" + rid.getBucketId() + ":" + rid.getPosition()));
      connection.connect();
      if (connection.getResponseCode() == 404)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));
      if (response.has("result"))
        return json2Record(response.getJSONObject("result"));
      return null;

    } catch (final RecordNotFoundException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on loading record " + rid, e);
    }
  }

  @Override
  public void deleteRecord(final Record record) {
    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot delete a non persistent record");

    command("SQL", "delete from " + record.getIdentity());
  }

  @Override
  public ResultSet command(final String language, final String command, final Object... args) {
    final Map<String, Object> params = mapArgs(args);
    return (ResultSet) databaseCommand("command", language, command, params, true, (connection, response) -> createResultSet(response));
  }

  @Override
  public ResultSet query(final String language, final String command, final Object... args) {
    final Map<String, Object> params = mapArgs(args);
    return (ResultSet) databaseCommand("query", language, command, params, false, (connection, response) -> createResultSet(response));
  }

  /**
   * @deprecated use {@link #command() command} instead
   */
  @Deprecated
  @Override
  public ResultSet execute(final String language, final String command, final Object... args) {
    final Map<String, Object> params = mapArgs(args);
    return (ResultSet) databaseCommand("command", language, command, params, false, (connection, response) -> createResultSet(response));
  }

  public CONNECTION_STRATEGY getConnectionStrategy() {
    return connectionStrategy;
  }

  public void setConnectionStrategy(final CONNECTION_STRATEGY connectionStrategy) {
    this.connectionStrategy = connectionStrategy;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  List<Pair<String, Integer>> getReplicaServerList() {
    return replicaServerList;
  }

  @Override
  public String toString() {
    return databaseName;
  }

  public void createUser(final String userName, final String password, final List<String> databases) {
    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("server"));

      final JSONObject jsonUser = new JSONObject();
      jsonUser.put("name", userName);
      jsonUser.put("password", password);
      if (databases != null && !databases.isEmpty()) {
        final JSONObject databasesJson = new JSONObject();
        for (final String dbName : databases)
          databasesJson.put(dbName, new String[] { "admin" });
        jsonUser.put("databases", databasesJson);
      }

      setRequestPayload(connection, new JSONObject().put("command", "create user " + jsonUser));

      connection.connect();
      if (connection.getResponseCode() != 200) {
        final Exception detail = manageException(connection, "create user");
        throw new SecurityException("Error on creating user: " + connection.getResponseMessage(), detail);
      }

    } catch (final Exception e) {
      throw new RuntimeException("Error on creating user", e);
    }
  }

  public void dropUser(final String userName) {
    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("server"));
      setRequestPayload(connection, new JSONObject().put("command", "drop user " + userName));
      connection.connect();
      if (connection.getResponseCode() != 200) {
        final Exception detail = manageException(connection, "drop user");
        throw new RuntimeException("Error on deleting user: " + connection.getResponseMessage(), detail);
      }

    } catch (final Exception e) {
      throw new RuntimeException("Error on deleting user", e);
    }
  }

  private Object serverCommand(final String method, final String command, final boolean leaderIsPreferable, final boolean autoReconnect,
      final Callback callback) {
    return httpCommand(method, null, "server", null, command, null, leaderIsPreferable, autoReconnect, callback);
  }

  private Object databaseCommand(final String operation, final String language, final String payloadCommand, final Map<String, Object> params,
      final boolean requiresLeader, final Callback callback) {
    return httpCommand("POST", databaseName, operation, language, payloadCommand, params, requiresLeader, true, callback);
  }

   Object httpCommand(final String method, final String extendedURL, final String operation, final String language, final String payloadCommand,
      final Map<String, Object> params, final boolean leaderIsPreferable, final boolean autoReconnect, final Callback callback) {

    Exception lastException = null;

    final int maxRetry = leaderIsPreferable ? 3 : getReplicaServerList().size() + 1;

    Pair<String, Integer> connectToServer = leaderIsPreferable && leaderServer != null ? leaderServer : new Pair<>(currentServer, currentPort);

    String server = null;

    for (int retry = 0; retry < maxRetry && connectToServer != null; ++retry) {
      server = connectToServer.getFirst() + ":" + connectToServer.getSecond();
      String url = protocol + "://" + server + "/api/v" + apiVersion + "/" + operation;

      if (extendedURL != null)
        url += "/" + extendedURL;

      try {
        final HttpURLConnection connection = createConnection(method, url);
        connection.setDoOutput(true);
        try {

          if (payloadCommand != null) {
            if ("GET".equalsIgnoreCase(method))
              throw new IllegalArgumentException("Cannot execute a HTTP GET request with a payload");

            final JSONObject jsonRequest = new JSONObject();
            if (language != null)
              jsonRequest.put("language", language);
            if (payloadCommand != null)
              jsonRequest.put("command", payloadCommand);
            jsonRequest.put("serializer", "record");

            if (params != null)
              jsonRequest.put("params", new JSONObject(params));

            setRequestPayload(connection, jsonRequest);
          }

          connection.connect();

          if (connection.getResponseCode() != 200) {
            lastException = manageException(connection, payloadCommand != null ? payloadCommand : operation);
            if (lastException instanceof RuntimeException && lastException.getMessage().equals("Empty payload received"))
              LogManager.instance().log(this, Level.FINE, "Empty payload received, retrying (retry=%d/%d)...", null, retry, maxRetry);
            continue;
          }

          final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));

          if (callback == null)
            return null;

          return callback.call(connection, response);

        } finally {
          connection.disconnect();
        }

      } catch (final IOException | ServerIsNotTheLeaderException e) {
        lastException = e;

        if (!autoReconnect)
          break;

        if (!reloadClusterConfiguration())
          throw new RemoteException("Error on executing remote operation " + operation + ", no server available", e);

        final Pair<String, Integer> currentConnectToServer = connectToServer;

        if (leaderIsPreferable && !currentConnectToServer.equals(leaderServer)) {
          connectToServer = leaderServer;
        } else
          connectToServer = getNextReplicaAddress();

        if (connectToServer != null)
          LogManager.instance()
              .log(this, Level.WARNING, "Remote server (%s:%d) seems unreachable, switching to server %s:%d...", null, currentConnectToServer.getFirst(),
                  currentConnectToServer.getSecond(), connectToServer.getFirst(), connectToServer.getSecond());

      } catch (final RemoteException | NeedRetryException | DuplicatedKeyException | TransactionException | TimeoutException | SecurityException e) {
        throw e;
      } catch (final Exception e) {
        throw new RemoteException("Error on executing remote operation " + operation + " (cause: " + e.getMessage() + ")", e);
      }
    }

    if (lastException instanceof RuntimeException)
      throw (RuntimeException) lastException;

    throw new RemoteException("Error on executing remote operation '" + operation + "' (server=" + server + " retry=" + maxRetry + ")", lastException);
  }

  public int getApiVersion() {
    return apiVersion;
  }

  public void setApiVersion(final int apiVersion) {
    this.apiVersion = apiVersion;
  }

  public String getLeaderAddress() {
    return leaderServer.getFirst() + ":" + leaderServer.getSecond();
  }

  HttpURLConnection createConnection(final String httpMethod, final String url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestProperty("charset", "utf-8");
    connection.setRequestMethod(httpMethod);

    final String authorization = userName + ":" + userPassword;
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(authorization.getBytes(DatabaseFactory.getDefaultCharset())));

    connection.setConnectTimeout(timeout);
    connection.setReadTimeout(timeout);

    if (sessionId != null)
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);

    return connection;
  }

  void requestClusterConfiguration() {
    try {
      final HttpURLConnection connection = createConnection("GET", getUrl("server?mode=cluster"));
      connection.connect();
      if (connection.getResponseCode() != 200) {
        final Exception detail = manageException(connection, "cluster configuration");
        if (detail instanceof SecurityException)
          throw detail;
        throw new SecurityException("Error on requesting cluster configuration: " + connection.getResponseMessage(), detail);
      }

      final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));

      LogManager.instance().log(this, Level.FINE, "Configuring remote database: %s", null, response);

      if (!response.has("ha")) {
        leaderServer = new Pair<>(originalServer, originalPort);
        replicaServerList.clear();
        return;
      }

      final JSONObject ha = response.getJSONObject("ha");

      final String cfgLeaderServer = (String) ha.get("leaderAddress");
      final String[] leaderServerParts = cfgLeaderServer.split(":");
      leaderServer = new Pair<>(leaderServerParts[0], Integer.parseInt(leaderServerParts[1]));

      final String cfgReplicaServers = (String) ha.get("replicaAddresses");

      // PARSE SERVER LISTS
      replicaServerList.clear();

      if (cfgReplicaServers != null && !cfgReplicaServers.isEmpty()) {
        final String[] serverEntries = cfgReplicaServers.split(",");
        for (final String serverEntry : serverEntries) {
          try {
            final String[] serverParts = serverEntry.split(":");
            if (serverParts.length != 2)
              LogManager.instance().log(this, Level.WARNING, "No port specified on remote server URL '%s'", null, serverEntry);

            final String sHost = serverParts[0];
            final int sPort = Integer.parseInt(serverParts[1]);

            replicaServerList.add(new Pair(sHost, sPort));
          } catch (Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Invalid replica server address '%s'", null, serverEntry);
          }
        }
      }

      LogManager.instance().log(this, Level.FINE, "Remote Database configured with leader=%s and replicas=%s", null, leaderServer, replicaServerList);

    } catch (final SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on requesting cluster configuration", e);
    }
  }

  private Pair<String, Integer> getNextReplicaAddress() {
    if (replicaServerList.isEmpty())
      return leaderServer;

    ++currentReplicaServerIndex;
    if (currentReplicaServerIndex > replicaServerList.size() - 1)
      currentReplicaServerIndex = 0;

    return replicaServerList.get(currentReplicaServerIndex);
  }

  boolean reloadClusterConfiguration() {
    final Pair<String, Integer> oldLeader = leaderServer;

    // ASK REPLICA FIRST
    for (int replicaIdx = 0; replicaIdx < replicaServerList.size(); ++replicaIdx) {
      final Pair<String, Integer> connectToServer = replicaServerList.get(replicaIdx);

      currentServer = connectToServer.getFirst();
      currentPort = connectToServer.getSecond();

      try {
        requestClusterConfiguration();
      } catch (final Exception e) {
        // IGNORE< TRY NEXT
        continue;
      }

      if (leaderServer != null)
        return true;
    }

    if (oldLeader != null) {
      // RESET LEADER SERVER TO AVOID LOOP
      leaderServer = null;

      // ASK TO THE OLD LEADER
      currentServer = oldLeader.getFirst();
      currentPort = oldLeader.getSecond();
      requestClusterConfiguration();
    }

    return leaderServer != null;
  }

  private Map<String, Object> mapArgs(final Object[] args) {
    Map<String, Object> params = null;
    if (args != null && args.length > 0) {
      if (args.length == 1 && args[0] instanceof Map)
        params = (Map<String, Object>) args[0];
      else {
        params = new HashMap<>();
        for (final Object o : args) {
          params.put("" + params.size(), o);
        }
      }
    }
    return params;
  }

  private String getUrl(final String command) {
    return protocol + "://" + currentServer + ":" + currentPort + "/api/v" + apiVersion + "/" + command;
  }

  private String getUrl(final String command, final String databaseName) {
    return getUrl(command) + "/" + databaseName;
  }

  protected ResultSet createResultSet(final JSONObject response) {
    final ResultSet resultSet = new InternalResultSet();

    final JSONArray resultArray = response.getJSONArray("result");
    for (int i = 0; i < resultArray.length(); ++i) {
      final JSONObject result = resultArray.getJSONObject(i);
      ((InternalResultSet) resultSet).add(json2Result(result));
    }
    return resultSet;
  }

  protected Result json2Result(final JSONObject result) {
    final Record record = json2Record(result);
    if (record == null)
      return new ResultInternal(result.toMap());

    return new ResultInternal(record);
  }

  protected Record json2Record(final JSONObject result) {
    final Map<String, Object> map = result.toMap();

    if (map.containsKey("@cat")) {
      final String cat = result.getString("@cat");
      switch (cat) {
      case "d":
        return new RemoteImmutableDocument(this, map);

      case "v":
        return new RemoteImmutableVertex(this, map);

      case "e":
        return new RemoteImmutableEdge(this, map);
      }
    }
    return null;
  }

  void setRequestPayload(final HttpURLConnection connection, final JSONObject jsonRequest) throws IOException {
    connection.setDoOutput(true);
    final byte[] postData = jsonRequest.toString().getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(postData);
    }
  }

  private Exception manageException(final HttpURLConnection connection, final String operation) throws IOException {
    String detail = null;
    String reason = null;
    String exception = null;
    String exceptionArgs = null;
    String responsePayload = null;

    if (connection.getErrorStream() != null) {
      try {
        responsePayload = FileUtils.readStreamAsString(connection.getErrorStream(), charset);
        final JSONObject response = new JSONObject(responsePayload);
        reason = response.getString("error");
        detail = response.has("detail") ? response.getString("detail") : null;
        exception = response.has("exception") ? response.getString("exception") : null;
        exceptionArgs = response.has("exceptionArgs") ? response.getString("exceptionArgs") : null;
      } catch (final Exception e) {
        // TODO CHECK IF THE COMMAND NEEDS TO BE RE-EXECUTED OR NOT
        LogManager.instance().log(this, Level.WARNING, "Error on executing command, retrying... (payload=%s, error=%s)", null, responsePayload, e.toString());
        return e;
      }
    }

    if (exception != null) {
      if (detail == null)
        detail = "Unknown";

      if (exception.equals(ServerIsNotTheLeaderException.class.getName())) {
        final int sep = detail.lastIndexOf('.');
        return new ServerIsNotTheLeaderException(sep > -1 ? detail.substring(0, sep) : detail, exceptionArgs);
      } else if (exception.equals(QuorumNotReachedException.class.getName())) {
        return new QuorumNotReachedException(detail);
      } else if (exception.equals(DuplicatedKeyException.class.getName()) && exceptionArgs != null) {
        final String[] exceptionArgsParts = exceptionArgs.split("\\|");
        return new DuplicatedKeyException(exceptionArgsParts[0], exceptionArgsParts[1], new RID(this, exceptionArgsParts[2]));
      } else if (exception.equals(ConcurrentModificationException.class.getName())) {
        return new ConcurrentModificationException(detail);
      } else if (exception.equals(TransactionException.class.getName())) {
        return new TransactionException(detail);
      } else if (exception.equals(TimeoutException.class.getName())) {
        return new TimeoutException(detail);
      } else if (exception.equals(SchemaException.class.getName())) {
        return new SchemaException(detail);
      } else if (exception.equals(NoSuchElementException.class.getName())) {
        return new NoSuchElementException(detail);
      } else if (exception.equals(SecurityException.class.getName())) {
        return new SecurityException(detail);
      } else if (exception.equals("com.arcadedb.server.security.ServerSecurityException")) {
        return new SecurityException(detail);
      } else
        // ELSE
        return new RemoteException("Error on executing remote operation " + operation + " (cause:" + exception + " detail:" + detail + ")");
    }

    final String httpErrorDescription = connection.getResponseMessage();

    // TEMPORARY FIX FOR AN ISSUE WITH THE CLIENT/SERVER COMMUNICATION WHERE THE PAYLOAD ARRIVES AS EMPTY
    if (connection.getResponseCode() == 400 && "Bad Request".equals(httpErrorDescription) && "Command text is null".equals(reason)) {
      // RETRY
      return new RuntimeException("Empty payload received");
    }

    return new RemoteException(
        "Error on executing remote command '" + operation + "' (httpErrorCode=" + connection.getResponseCode() + " httpErrorDescription=" + httpErrorDescription
            + " reason=" + reason + " detail=" + detail + " exception=" + exception + ")");
  }
}
