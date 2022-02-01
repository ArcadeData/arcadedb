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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.network.http.HttpUtils;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RWLockContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.logging.*;

public class RemoteDatabase extends RWLockContext {

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

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN
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

  public String getName() {
    return databaseName;
  }

  public void create() {
    databaseCommand("create", "SQL", null, null, true, null);
  }

  public boolean exists() {
    return (boolean) databaseCommand("exists", "SQL", null, null, true, (connection, response) -> {

      final boolean exists = response.getBoolean("result");

      return exists;
    });
  }

  public void close() {
  }

  public void drop() {
    databaseCommand("drop", "SQL", null, null, true, null);
    close();
  }

  public void transaction(final Database.TransactionScope txBlock) {
    transaction(txBlock, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES));
  }

  public void transaction(final Database.TransactionScope txBlock, int attempts) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ArcadeDBException lastException = null;

    if (attempts < 1)
      attempts = 1;

    for (int retry = 0; retry < attempts; ++retry) {
      try {
        begin();
        txBlock.execute();
        commit();

        return;
      } catch (NeedRetryException | DuplicatedKeyException e) {
        // RETRY
        lastException = e;
      } catch (Exception e) {
        rollback();
        throw e;
      }
    }

    throw lastException;
  }

  public void begin() {
    if (sessionId != null)
      throw new TransactionException("Transaction already begun");

    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("begin", databaseName));
      connection.connect();
      if (connection.getResponseCode() != 204)
        throw new TransactionException("Error on transaction begin");
      sessionId = connection.getHeaderField(HttpUtils.ARCADEDB_SESSION_ID);
    } catch (Exception e) {
      throw new TransactionException("Error on transaction begin", e);
    }
  }

  public void commit() {
    if (sessionId == null)
      throw new TransactionException("Transaction not begun");
    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("commit", databaseName));
      connection.connect();
      if (connection.getResponseCode() != 204)
        throw new TransactionException("Error on transaction commit");
      sessionId = null;
    } catch (Exception e) {
      throw new TransactionException("Error on transaction commit", e);
    }
  }

  public void rollback() {
    if (sessionId == null)
      throw new TransactionException("Transaction not begun");
    try {
      final HttpURLConnection connection = createConnection("POST", getUrl("rollback", databaseName));
      connection.connect();
      if (connection.getResponseCode() != 204)
        throw new TransactionException("Error on transaction rollback");

      sessionId = null;
    } catch (Exception e) {
      throw new TransactionException("Error on transaction rollback", e);
    }
  }

  public JSONObject lookupByRID(final String rid) {
    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    try {
      final HttpURLConnection connection = createConnection("GET", getUrl("document", databaseName + "/" + rid.substring(1)));
      connection.connect();
      if (connection.getResponseCode() == 404)
        throw new RecordNotFoundException("Record " + rid + " not found", new RID(null, rid));

      final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));
      if (response.has("result"))
        return response.getJSONObject("result");
      return null;

    } catch (Exception e) {
      throw new DatabaseOperationException("Error on loading record " + rid, e);
    }
  }

  public ResultSet command(final String language, final String command, final Object... args) {
    Map<String, Object> params = mapArgs(args);

    return (ResultSet) databaseCommand("command", language, command, params, true, (connection, response) -> {
      final ResultSet resultSet = new InternalResultSet();

      final JSONArray resultArray = response.getJSONArray("result");
      for (int i = 0; i < resultArray.length(); ++i) {
        final JSONObject result = resultArray.getJSONObject(i);
        ((InternalResultSet) resultSet).add(new ResultInternal(result.toMap()));
      }

      return resultSet;
    });
  }

  public ResultSet query(final String language, final String command, final Object... args) {
    Map<String, Object> params = mapArgs(args);

    return (ResultSet) databaseCommand("query", language, command, params, false, (connection, response) -> {
      final ResultSet resultSet = new InternalResultSet();

      final JSONArray resultArray = response.getJSONArray("result");
      for (int i = 0; i < resultArray.length(); ++i) {
        final JSONObject result = resultArray.getJSONObject(i);
        ((InternalResultSet) resultSet).add(new ResultInternal(result.toMap()));
      }

      return resultSet;
    });
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

  @Override
  public String toString() {
    return databaseName;
  }

  private Object serverCommand(final String operation, final String language, final String payloadCommand, final Map<String, Object> params,
      final boolean leaderIsPreferable, final boolean autoReconnect, final Callback callback) {
    return httpCommand(null, operation, language, payloadCommand, params, leaderIsPreferable, autoReconnect, callback);
  }

  private Object databaseCommand(final String operation, final String language, final String payloadCommand, final Map<String, Object> params,
      final boolean requiresLeader, final Callback callback) {
    return httpCommand(databaseName, operation, language, payloadCommand, params, requiresLeader, true, callback);
  }

  private Object httpCommand(final String extendedURL, final String operation, final String language, final String payloadCommand,
      final Map<String, Object> params, final boolean leaderIsPreferable, final boolean autoReconnect, final Callback callback) {

    Exception lastException = null;

    final int maxRetry = leaderIsPreferable ? 3 : replicaServerList.size() + 1;

    Pair<String, Integer> connectToServer = leaderIsPreferable ? leaderServer : new Pair<>(currentServer, currentPort);

    for (int retry = 0; retry < maxRetry && connectToServer != null; ++retry) {
      String url = protocol + "://" + connectToServer.getFirst() + ":" + connectToServer.getSecond() + "/api/v" + apiVersion + "/" + operation;

      if (extendedURL != null)
        url += "/" + extendedURL;

      try {
        final HttpURLConnection connection = createConnection("POST", url);
        connection.setDoOutput(true);
        try {

          if (payloadCommand != null) {
            final JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("language", language);
            jsonRequest.put("command", payloadCommand);
            jsonRequest.put("serializer", "record");

            if (params != null) {
              final JSONObject jsonParams = new JSONObject(params);
              jsonRequest.put("params", jsonParams);
            }

            final byte[] postData = jsonRequest.toString().getBytes(StandardCharsets.UTF_8);
            connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
            try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
              wr.write(postData);
            }
          }

          connection.connect();

          if (connection.getResponseCode() != 200) {
            String detail;
            String reason;
            String exception;
            String exceptionArgs;
            String responsePayload = null;
            try {
              responsePayload = FileUtils.readStreamAsString(connection.getErrorStream(), charset);
              final JSONObject response = new JSONObject(responsePayload);
              reason = response.getString("error");
              detail = response.has("detail") ? response.getString("detail") : null;
              exception = response.has("exception") ? response.getString("exception") : null;
              exceptionArgs = response.has("exceptionArgs") ? response.getString("exceptionArgs") : null;
            } catch (Exception e) {
              lastException = e;
              // TODO CHECK IF THE COMMAND NEEDS TO BE RE-EXECUTED OR NOT
              LogManager.instance()
                  .log(this, Level.WARNING, "Error on executing command, retrying... (payload=%s, error=%s)", null, responsePayload, e.toString());
              continue;
            }

            String cmd = payloadCommand;
            if (cmd == null)
              cmd = "-";

            if (exception != null) {
              if (detail == null)
                detail = "Unknown";

              if (exception.equals(ServerIsNotTheLeaderException.class.getName())) {
                final int sep = detail.lastIndexOf('.');
                throw new ServerIsNotTheLeaderException(sep > -1 ? detail.substring(0, sep) : detail, exceptionArgs);
              } else if (exception.equals(QuorumNotReachedException.class.getName())) {
                lastException = new QuorumNotReachedException(detail);
                continue;
              } else if (exception.equals(DuplicatedKeyException.class.getName()) && exceptionArgs != null) {
                final String[] exceptionArgsParts = exceptionArgs.split("\\|");
                throw new DuplicatedKeyException(exceptionArgsParts[0], exceptionArgsParts[1], new RID(null, exceptionArgsParts[2]));
              } else if (exception.equals(ConcurrentModificationException.class.getName())) {
                throw new ConcurrentModificationException(detail);
              } else if (exception.equals(TransactionException.class.getName())) {
                throw new TransactionException(detail);
              } else if (exception.equals(TimeoutException.class.getName())) {
                throw new TimeoutException(detail);
              } else if (exception.equals(SchemaException.class.getName())) {
                throw new SchemaException(detail);
              } else
                // ELSE
                throw new RemoteException("Error on executing remote operation " + operation + " (cause:" + exception + ")");
            }

            final String httpErrorDescription = connection.getResponseMessage();

            // TEMPORARY FIX FOR AN ISSUE WITH THE CLIENT/SERVER COMMUNICATION WHERE THE PAYLOAD ARRIVES AS EMPTY
            if (connection.getResponseCode() == 400 && "Bad Request".equals(httpErrorDescription) && "Command text is null".equals(reason)) {
              // RETRY
              LogManager.instance().log(this, Level.FINE, "Empty payload received, retrying (retry=%d/%d)...", null, retry, maxRetry);
              continue;
            }

            throw new RemoteException(
                "Error on executing remote command '" + cmd + "' (httpErrorCode=" + connection.getResponseCode() + " httpErrorDescription="
                    + httpErrorDescription + " reason=" + reason + " detail=" + detail + " exception=" + exception + ")");
          }

          final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));

          if (callback == null)
            return null;

          return callback.call(connection, response);

        } finally {
          connection.disconnect();
        }

      } catch (IOException | ServerIsNotTheLeaderException e) {
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

      } catch (RemoteException | NeedRetryException | DuplicatedKeyException | TransactionException | TimeoutException e) {
        throw e;
      } catch (Exception e) {
        throw new RemoteException("Error on executing remote operation " + operation + " (cause: " + e.getMessage() + ")", e);
      }
    }

    if (lastException instanceof RuntimeException)
      throw (RuntimeException) lastException;

    throw new RemoteException("Error on executing remote operation " + operation + " (retry=" + maxRetry + ")", lastException);
  }

  public int getApiVersion() {
    return apiVersion;
  }

  public void setApiVersion(final int apiVersion) {
    this.apiVersion = apiVersion;
  }

  public interface Callback {
    Object call(HttpURLConnection iArgument, JSONObject response) throws Exception;
  }

  protected HttpURLConnection createConnection(final String httpMethod, final String url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestProperty("charset", "utf-8");
    connection.setRequestMethod(httpMethod);

    final String authorization = userName + ":" + userPassword;
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(authorization.getBytes(DatabaseFactory.getDefaultCharset())));

    connection.setConnectTimeout(timeout);
    connection.setReadTimeout(timeout);

    if (sessionId != null)
      connection.setRequestProperty(HttpUtils.ARCADEDB_SESSION_ID, sessionId);

    return connection;
  }

  private void requestClusterConfiguration() {
    serverCommand("server", "SQL", null, null, false, false, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) {
        LogManager.instance().log(this, Level.FINE, "Configuring remote database: %s", null, response);

        if (!response.has("leaderServer")) {
          leaderServer = new Pair<>(originalServer, originalPort);
          replicaServerList.clear();
          return null;
        }

        final String cfgLeaderServer = (String) response.get("leaderServer");
        final String[] leaderServerParts = cfgLeaderServer.split(":");
        leaderServer = new Pair<>(leaderServerParts[0], Integer.parseInt(leaderServerParts[1]));

        final String cfgReplicaServers = (String) response.get("replicaServers");

        // PARSE SERVER LISTS
        replicaServerList.clear();

        if (cfgReplicaServers != null && !cfgReplicaServers.isEmpty()) {
          final String[] serverEntries = cfgReplicaServers.split(",");
          for (String serverEntry : serverEntries) {
            final String[] serverParts = serverEntry.split(":");
            if (serverParts.length != 2)
              LogManager.instance().log(this, Level.WARNING, "No port specified on remote server URL '%s'", null, serverEntry);

            final String sHost = serverParts[0];
            final int sPort = Integer.parseInt(serverParts[1]);

            replicaServerList.add(new Pair(sHost, sPort));
          }
        }

        LogManager.instance().log(this, Level.INFO, "Remote Database configured with leader=%s and replicas=%s", null, leaderServer, replicaServerList);

        return null;
      }
    });
  }

  private Pair<String, Integer> getNextReplicaAddress() {
    if (replicaServerList.isEmpty())
      return leaderServer;

    ++currentReplicaServerIndex;
    if (currentReplicaServerIndex > replicaServerList.size() - 1)
      currentReplicaServerIndex = 0;

    return replicaServerList.get(currentReplicaServerIndex);
  }

  private boolean reloadClusterConfiguration() {
    final Pair<String, Integer> oldLeader = leaderServer;

    // ASK REPLICA FIRST
    for (int replicaIdx = 0; replicaIdx < replicaServerList.size(); ++replicaIdx) {
      Pair<String, Integer> connectToServer = replicaServerList.get(replicaIdx);

      currentServer = connectToServer.getFirst();
      currentPort = connectToServer.getSecond();

      try {
        requestClusterConfiguration();
      } catch (Exception e) {
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

  private Map<String, Object> mapArgs(Object[] args) {
    Map<String, Object> params = null;
    if (args != null && args.length > 0) {
      if (args.length == 1 && args[0] instanceof Map)
        params = (Map<String, Object>) args[0];
      else {
        params = new HashMap<>();
        for (Object o : args) {
          params.put("" + params.size(), o);
        }
      }
    }
    return params;
  }

  private String getUrl(final String command, final String databaseName) {
    return protocol + "://" + currentServer + ":" + currentPort + "/api/v" + apiVersion + "/" + command + "/" + databaseName;
  }
}
