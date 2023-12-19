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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseStats;
import com.arcadedb.database.RID;
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

/**
 * Remote Database implementation. It's not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteHttpComponent extends RWLockContext {
  public static final    int                         DEFAULT_PORT              = 2480;
  private final          String                      originalServer;
  private final          int                         originalPort;
  private                int                         apiVersion                = 1;
  protected final        ContextConfiguration        configuration;
  private final          String                      userName;
  private final          String                      userPassword;
  private final          List<Pair<String, Integer>> replicaServerList         = new ArrayList<>();
  protected              String                      currentServer;
  protected              int                         currentPort;
  private                CONNECTION_STRATEGY         connectionStrategy        = CONNECTION_STRATEGY.ROUND_ROBIN;
  private                Pair<String, Integer>       leaderServer;
  private                int                         currentReplicaServerIndex = -1;
  private                int                         timeout;
  protected static final String                      protocol                  = "http";
  private static final   String                      charset                   = "UTF-8";
  protected final        DatabaseStats               stats                     = new DatabaseStats();

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN
  }

  public interface Callback {
    Object call(HttpURLConnection iArgument, JSONObject response) throws Exception;
  }

  public RemoteHttpComponent(final String server, final int port, final String userName, final String userPassword) {
    this(server, port, userName, userPassword, new ContextConfiguration());
  }

  public RemoteHttpComponent(final String server, final int port, final String userName, final String userPassword,
      final ContextConfiguration configuration) {
    this.originalServer = server;
    this.originalPort = port;

    this.currentServer = originalServer;
    this.currentPort = originalPort;

    this.userName = userName;
    this.userPassword = userPassword;

    this.configuration = configuration;
    this.timeout = this.configuration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

    requestClusterConfiguration();
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  public String getUserName() {
    return userName;
  }

  public String getUserPassword() {
    return userPassword;
  }

  List<Pair<String, Integer>> getReplicaServerList() {
    return replicaServerList;
  }

  public Map<String, Object> getStats() {
    return stats.toMap();
  }

  Object httpCommand(final String method, final String extendedURL, final String operation, final String language,
      final String payloadCommand, final Map<String, Object> params, final boolean leaderIsPreferable, final boolean autoReconnect,
      final Callback callback) {

    Exception lastException = null;

    final int maxRetry = leaderIsPreferable ? 3 : getReplicaServerList().size() + 1;

    Pair<String, Integer> connectToServer =
        leaderIsPreferable && leaderServer != null ? leaderServer : new Pair<>(currentServer, currentPort);

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
              LogManager.instance()
                  .log(this, Level.FINE, "Empty payload received, retrying (retry=%d/%d)...", null, retry, maxRetry);
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
              .log(this, Level.WARNING, "Remote server (%s:%d) seems unreachable, switching to server %s:%d...", null,
                  currentConnectToServer.getFirst(), currentConnectToServer.getSecond(), connectToServer.getFirst(),
                  connectToServer.getSecond());

      } catch (final RemoteException | NeedRetryException | DuplicatedKeyException | TransactionException | TimeoutException |
                     SecurityException e) {
        throw e;
      } catch (final Exception e) {
        throw new RemoteException("Error on executing remote operation " + operation + " (cause: " + e.getMessage() + ")", e);
      }
    }

    if (lastException instanceof RuntimeException exception)
      throw exception;

    throw new RemoteException(
        "Error on executing remote operation '" + operation + "' (server=" + server + " retry=" + maxRetry + ")", lastException);
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

  public List<String> getReplicaAddresses() {
    return replicaServerList.stream().map((e) -> e.getFirst() + ":" + e.getSecond()).collect(Collectors.toList());
  }

  HttpURLConnection createConnection(final String httpMethod, final String url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
    connection.setRequestProperty("charset", "utf-8");
    connection.setRequestMethod(httpMethod);

    final String authorization = userName + ":" + userPassword;
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(authorization.getBytes(DatabaseFactory.getDefaultCharset())));

    connection.setConnectTimeout(timeout);
    connection.setReadTimeout(timeout);

    return connection;
  }

  void requestClusterConfiguration() {
    final JSONObject response;
    try {
      final HttpURLConnection connection = createConnection("GET", getUrl("server?mode=cluster"));
      connection.connect();
      if (connection.getResponseCode() != 200) {
        final Exception detail = manageException(connection, "cluster configuration");
        if (detail instanceof SecurityException)
          throw detail;
        throw new RemoteException("Error on requesting cluster configuration: " + connection.getResponseMessage(), detail);
      }

      response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));

      LogManager.instance().log(this, Level.FINE, "Configuring remote database: %s", null, response);

    } catch (final SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on connecting to the server", e);
    }

    try {
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

      LogManager.instance().log(this, Level.FINE, "Remote Database configured with leader=%s and replicas=%s", null, leaderServer,
          replicaServerList);

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
        // IGNORE: TRY NEXT
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
      try {
        requestClusterConfiguration();
      } catch (final Exception e) {
        // IGNORE
      }
    }

    return leaderServer != null;
  }

  protected String getUrl(final String command) {
    return protocol + "://" + currentServer + ":" + currentPort + "/api/v" + apiVersion + "/" + command;
  }

  void setRequestPayload(final HttpURLConnection connection, final JSONObject jsonRequest) throws IOException {
    connection.setDoOutput(true);
    final byte[] postData = jsonRequest.toString().getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(postData);
    }
  }

  protected Exception manageException(final HttpURLConnection connection, final String operation) throws IOException {
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
        LogManager.instance()
            .log(this, Level.WARNING, "Error on executing command, retrying... (payload=%s, error=%s)", null, responsePayload,
                e.toString());
        return e;
      }
    }

    if (exception != null) {
      if (detail == null)
        detail = "Unknown";

      if (exception.equals(ServerIsNotTheLeaderException.class.getName())) {
        final int sep = detail.lastIndexOf('.');
        return new ServerIsNotTheLeaderException(sep > -1 ? detail.substring(0, sep) : detail, exceptionArgs);
      } else if (exception.equals(RecordNotFoundException.class.getName())) {
        final int begin = detail.indexOf("#");
        final int end = detail.indexOf(" ", begin);
        return new RecordNotFoundException(detail, new RID( detail.substring(begin, end)));
      } else if (exception.equals(QuorumNotReachedException.class.getName())) {
        return new QuorumNotReachedException(detail);
      } else if (exception.equals(DuplicatedKeyException.class.getName()) && exceptionArgs != null) {
        final String[] exceptionArgsParts = exceptionArgs.split("\\|");
        return new DuplicatedKeyException(exceptionArgsParts[0], exceptionArgsParts[1], new RID( exceptionArgsParts[2]));
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
      } else if (exception.equals(ConnectException.class.getName())) {
        return new NeedRetryException(detail);
      } else if (exception.equals("com.arcadedb.server.ha.ReplicationException")) {
        return new NeedRetryException(detail);
      } else
        // ELSE
        return new RemoteException(
            "Error on executing remote operation " + operation + " (cause:" + exception + " detail:" + detail + ")");
    }

    final String httpErrorDescription = connection.getResponseMessage();

    // TEMPORARY FIX FOR AN ISSUE WITH THE CLIENT/SERVER COMMUNICATION WHERE THE PAYLOAD ARRIVES AS EMPTY
    if (connection.getResponseCode() == 400 && "Bad Request".equals(httpErrorDescription) && "Command text is null".equals(
        reason)) {
      // RETRY
      return new RemoteException("Empty payload received");
    }

    return new RemoteException(
        "Error on executing remote command '" + operation + "' (httpErrorCode=" + connection.getResponseCode()
            + " httpErrorDescription=" + httpErrorDescription + " reason=" + reason + " detail=" + detail + " exception="
            + exception + ")");
  }
}
