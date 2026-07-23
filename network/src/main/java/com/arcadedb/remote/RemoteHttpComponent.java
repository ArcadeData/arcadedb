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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RWLockContext;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Remote Database implementation. It's not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteHttpComponent extends RWLockContext {
  public static final  int    DEFAULT_PORT = 2480;
  private static final String charset      = "UTF-8";

  protected       String                      protocol                  = "http";
  private final   String                      originalServer;
  private final   int                         originalPort;
  private final   String                      userName;
  private final   String                      userPassword;
  private volatile List<Pair<String, Integer>> replicaServerList        = new ArrayList<>();
  protected final HttpClient                  httpClient;
  protected final DatabaseStats               stats                     = new DatabaseStats();
  protected final ContextConfiguration        configuration;
  private         int                         sameServerErrorRetries;
  private         int                         haServerErrorRetries;
  private final   Integer                     txRetries;
  private         int                         apiVersion                = 1;
  private         CONNECTION_STRATEGY         connectionStrategy        = CONNECTION_STRATEGY.ROUND_ROBIN;
  private volatile Pair<String, Integer>       leaderServer;
  private volatile int                         currentReplicaServerIndex = -1;
  private          int                         timeout;
  protected        String                      currentServer;
  protected        int                         currentPort;
  private         Pair<String, Integer>       stickyTransactionServer;

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN, FIXED
  }

  public interface Callback {
    Object call(HttpResponse<String> iArgument, JSONObject response) throws Exception;
  }

  public RemoteHttpComponent(final String server, final int port, final String userName, final String userPassword) {
    this(server, port, userName, userPassword, new ContextConfiguration());
  }

  public RemoteHttpComponent(String server, final int port, final String userName, final String userPassword,
      final ContextConfiguration configuration) {
    if (server.startsWith("https://")) {
      protocol = "https";
      server = server.substring("https://".length());
    } else if (server.startsWith("http://")) {
      protocol = "http";
      server = server.substring("http://".length());
    }

    this.originalServer = server;
    this.originalPort = port;

    this.currentServer = originalServer;
    this.currentPort = originalPort;

    this.userName = userName;
    this.userPassword = userPassword;

    this.configuration = configuration;
    this.timeout = this.configuration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

    setSameServerErrorRetries(this.configuration.getValueAsInteger(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES));
    haServerErrorRetries = this.configuration.getValueAsInteger(GlobalConfiguration.HA_ERROR_RETRIES);

    this.txRetries = this.configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES);

    httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(60))
        .version(HttpClient.Version.HTTP_2)
        .build();

    requestClusterConfiguration();
  }

  public void close() {
    // httpClient is final and always assigned in the constructor, so no null check is needed. close()
    // performs an orderly shutdown that lets in-flight requests drain (each is already bounded by the
    // per-request watchdog timeout); shutdownNow() is intentionally not used so requests are not interrupted.
    httpClient.close();
  }

  private HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
    final long watchdogMs = Math.max(timeout * 1000L, 30_000L);
    try {
      return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .get(watchdogMs, TimeUnit.MILLISECONDS);
    } catch (final java.util.concurrent.TimeoutException e) {
      throw new IOException("HTTP request watchdog timeout after " + watchdogMs + "ms: " + request.uri(), e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException ioe)
        throw ioe;
      if (cause instanceof InterruptedException ie)
        throw ie;
      throw new IOException("HTTP request failed: " + request.uri(), cause);
    }
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  public void setSameServerErrorRetries(Integer maxRetries) {
    if (maxRetries == null || maxRetries < 0)
      maxRetries = 0;
    this.sameServerErrorRetries = maxRetries;
  }

  public String getUserName() {
    return userName;
  }

  public String getUserPassword() {
    return userPassword;
  }

  public CONNECTION_STRATEGY getConnectionStrategy() {
    return connectionStrategy;
  }

  public void setConnectionStrategy(final CONNECTION_STRATEGY connectionStrategy) {
    this.connectionStrategy = connectionStrategy;
  }

  protected void setStickyTransactionServer(final Pair<String, Integer> server) {
    this.stickyTransactionServer = server;
  }

  private Pair<String, Integer> getStickyPin() {
    return connectionStrategy == CONNECTION_STRATEGY.STICKY ? stickyTransactionServer : null;
  }

  Pair<String, Integer> getLeaderServer() {
    return leaderServer;
  }

  List<Pair<String, Integer>> getReplicaServerList() {
    return replicaServerList;
  }

  public Map<String, Object> getStats() {
    return stats.toMap();
  }

  // Routing for query/command traffic. RemoteDatabase.begin/commit/rollback build
  // their URLs through getUrl() instead - any STICKY routing change must update both paths.
  Object httpCommand(final String method,
      final String extendedURL,
      final String operation,
      final String language,
      final String payloadCommand,
      final Map<String, Object> params,
      final boolean leaderIsPreferable,
      final boolean autoReconnect,
      final Callback callback) {

    Exception lastException = null;

    final Pair<String, Integer> stickyPin = getStickyPin();
    final boolean stickyPinned = stickyPin != null;

    int maxRetry =
        leaderIsPreferable || connectionStrategy == CONNECTION_STRATEGY.FIXED || stickyPinned ?
            sameServerErrorRetries :
            haServerErrorRetries == 0 ? getReplicaServerList().size() + 1 : haServerErrorRetries;
    if (maxRetry < 1)
      maxRetry = 1;

    Pair<String, Integer> connectToServer;
    if (connectionStrategy == CONNECTION_STRATEGY.FIXED)
      connectToServer = new Pair<>(originalServer, originalPort);
    else if (stickyPinned)
      connectToServer = stickyPin;
    else
      connectToServer = leaderIsPreferable && leaderServer != null ? leaderServer : new Pair<>(currentServer, currentPort);

    String server = null;

    for (int retry = 0; retry < maxRetry && connectToServer != null; ++retry) {
      server = connectToServer.getFirst() + ":" + connectToServer.getSecond();
      String url = protocol + "://" + server + "/api/v" + apiVersion + "/" + operation;

      if (extendedURL != null)
        url += "/" + extendedURL;

      try {
        HttpRequest.Builder requestBuilder = createRequestBuilder(method, url);

        // Inject HA read-consistency headers when used from a RemoteDatabase.
        // Semantics:
        //   X-ArcadeDB-Read-Consistency : consistency level requested by the client.
        //   X-ArcadeDB-Read-After       : client-supplied bookmark - the commit index
        //                                 the follower must have applied before serving
        //                                 the read (read-your-writes barrier).
        //   X-ArcadeDB-Commit-Index     : response-only - the server echoes its current
        //                                 last-applied commit index so the client can
        //                                 use it as the next Read-After bookmark.
        if (this instanceof RemoteDatabase remoteDb) {
          final ReadConsistency rc = remoteDb.getReadConsistency();
          if (rc != ReadConsistency.EVENTUAL)
            requestBuilder = requestBuilder.header("X-ArcadeDB-Read-Consistency", rc.name().toLowerCase());
          if (rc == ReadConsistency.READ_YOUR_WRITES) {
            final long last = remoteDb.getLastCommitIndex();
            if (last >= 0)
              requestBuilder = requestBuilder.header("X-ArcadeDB-Read-After", String.valueOf(last));
          }
        }

        HttpRequest request;

        if (payloadCommand != null) {
          if ("GET".equalsIgnoreCase(method))
            throw new IllegalArgumentException("Cannot execute a HTTP GET request with a payload");

          final JSONObject jsonRequest = new JSONObject();
          if (language != null)
            jsonRequest.put("language", language);
          jsonRequest.put("command", payloadCommand);
          jsonRequest.put("serializer", "record");
          jsonRequest.put("retries", txRetries);

          if (params != null)
            jsonRequest.put("params", new JSONObject(params));

          String payload = getRequestPayload(jsonRequest);
          request = requestBuilder
              .method(method, HttpRequest.BodyPublishers.ofString(payload))
              .header("Content-Type", "application/json")
              .build();
        } else {
          if ("GET".equalsIgnoreCase(method)) {
            request = requestBuilder.GET().build();
          } else {
            request = requestBuilder
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
          }
        }

        HttpResponse<String> response = sendWithWatchdog(request);

        // Capture commit-index from response for read-your-writes consistency.
        if (this instanceof RemoteDatabase remoteDb) {
          response.headers().firstValue("X-ArcadeDB-Commit-Index").ifPresent(val -> {
            try {
              remoteDb.updateLastCommitIndex(Long.parseLong(val));
            } catch (final NumberFormatException ignored) {
              // server sent an invalid header; ignore
            }
          });
        }

        if (response.statusCode() != 200) {
          lastException = manageException(response, payloadCommand != null ? payloadCommand : operation);
          if (lastException instanceof RuntimeException && "Empty payload received".equals(lastException.getMessage())) {
            LogManager.instance()
                .log(this, Level.FINE, "Empty payload received, retrying (retry=%d/%d)...", null, retry, maxRetry);
            continue;
          }

          throw lastException;
        }

        // The server returned HTTP 200 but the body is not valid JSON: this is a protocol/transport
        // problem, not a callback bug. Surface it as a clearly-labelled RemoteException (issue #4580)
        // so it is not confused with a network failure or buried as a generic error.
        final JSONObject jsonResponse;
        try {
          jsonResponse = new JSONObject(response.body());
        } catch (final JSONException e) {
          throw new RemoteException("Malformed server response for operation '" + operation + "'", e);
        }

        if (callback == null)
          return null;

        return callback.call(response, jsonResponse);

      } catch (final IOException | ServerIsNotTheLeaderException e) {
        lastException = e;

        if (!autoReconnect || retry + 1 >= maxRetry)
          break;

        if (connectionStrategy == CONNECTION_STRATEGY.FIXED || stickyPinned) {
          LogManager.instance()
              .log(this, Level.WARNING, "Remote server (%s:%d) seems unreachable, retrying...",
                  connectToServer.getFirst(), connectToServer.getSecond());
        } else {
          if (this instanceof RemoteDatabase remoteDb && remoteDb.getSessionId() != null) {
            remoteDb.setSessionId(null);
            throw new TransactionException("Server failover during active transaction", e);
          }

          if (!reloadClusterConfiguration())
            throw new RemoteException("Error on executing remote operation " + operation + ", no server available", e);

          final Pair<String, Integer> currentConnectToServer = connectToServer;
          final Pair<String, Integer> snapshotLeader = leaderServer;

          if (leaderIsPreferable && snapshotLeader != null && !currentConnectToServer.equals(snapshotLeader)) {
            connectToServer = snapshotLeader;
          } else
            connectToServer = getNextReplicaAddress();

          if (connectToServer != null)
            LogManager.instance()
                .log(this, Level.WARNING, "Remote server (%s:%d) seems unreachable, switching to server %s:%d...", null,
                    currentConnectToServer.getFirst(), currentConnectToServer.getSecond(), connectToServer.getFirst(),
                    connectToServer.getSecond());
        }

      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RemoteException("Request interrupted", e);
      } catch (final NeedRetryException e) {
        // Election in progress - retry with delay.
        final int maxElectionRetries = this instanceof RemoteDatabase db ? db.getElectionRetryCount() : 3;
        final long delayMs = this instanceof RemoteDatabase db ? db.getElectionRetryDelayMs() : 2000L;
        if (retry + 1 >= maxRetry || retry >= maxElectionRetries) {
          lastException = e;
          break;
        }
        try {
          Thread.sleep(delayMs);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RemoteException("Request interrupted during election retry", ie);
        }
        LogManager.instance().log(this, Level.WARNING,
            "Election in progress, retrying after %dms (retry=%d/%d)...", null, delayMs, retry, maxRetry);
      } catch (final RuntimeException e) {
        // Propagate any RuntimeException unchanged (issue #4580): a callback-side bug (e.g. NPE), a
        // malformed-response RemoteException, or a typed ArcadeDB exception (DuplicatedKeyException,
        // TransactionException, TimeoutException, SecurityException, RecordNotFoundException, ...) must
        // keep its original type and stack trace instead of being buried as a generic RemoteException.
        throw e;
      } catch (final Exception e) {
        // Only checked exceptions thrown by the callback reach here: wrap them as a RemoteException.
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
    final Pair<String, Integer> snapshot = leaderServer;
    return snapshot != null ? snapshot.getFirst() + ":" + snapshot.getSecond() : null;
  }

  public List<String> getReplicaAddresses() {
    return replicaServerList.stream().map(e -> e.getFirst() + ":" + e.getSecond()).collect(Collectors.toList());
  }

  HttpRequest.Builder createRequestBuilder(final String httpMethod, final String url) {
    final String authorization = userName + ":" + userPassword;
    String authHeader = "Basic " + Base64.getEncoder().encodeToString(authorization.getBytes(DatabaseFactory.getDefaultCharset()));

    return HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(timeout))
        .header("charset", "utf-8")
        .header("Authorization", authHeader);
  }

  void requestClusterConfiguration() {
    final JSONObject response;
    try {
      HttpRequest request = createRequestBuilder("GET", getUrl("server?mode=cluster"))
          .GET()
          .build();

      HttpResponse<String> httpResponse = sendWithWatchdog(request);

      if (httpResponse.statusCode() != 200) {
        final Exception detail = manageException(httpResponse, "cluster configuration");
        if (detail instanceof SecurityException)
          throw detail;
        throw new RemoteException("Error on requesting cluster configuration", detail);
      }

      response = new JSONObject(httpResponse.body());

      LogManager.instance().log(this, Level.FINE, "Configuring remote database: %s", null, response);

    } catch (final SecurityException e) {
      throw e;
    } catch (final Exception e) {
      // Fall back to the original server when the cluster configuration is unavailable.
      // This allows the client to work against any reachable node without requiring
      // the full cluster topology (which may contain internal addresses unreachable from the client).
      LogManager.instance()
          .log(this, Level.WARNING, "Unable to fetch cluster configuration from %s:%d, using direct connection (%s)",
              null, currentServer, currentPort, e.getMessage());
      leaderServer = new Pair<>(originalServer, originalPort);
      publishReplicaServerList(new ArrayList<>());
      return;
    }

    try {
      if (!response.has("ha")) {
        leaderServer = new Pair<>(originalServer, originalPort);
        publishReplicaServerList(new ArrayList<>());
        return;
      }

      final JSONObject ha = response.getJSONObject("ha");

      final String cfgLeaderServer = (String) ha.get("leaderAddress");

      final String[] leaderServerParts = HostUtil.parseHostAddress(cfgLeaderServer, HostUtil.HA_DEFAULT_PORT);

      leaderServer = new Pair<>(leaderServerParts[0], Integer.parseInt(leaderServerParts[1]));

      final String cfgReplicaServers = (String) ha.get("replicaAddresses");

      // PARSE SERVER LISTS INTO A FRESH LIST, THEN PUBLISH IT ATOMICALLY (SEE publishReplicaServerList).
      // Never mutate the live list in place: a concurrent getNextReplicaAddress() could observe a
      // non-empty size and then read past the end after a clear() (issue #4579).
      final List<Pair<String, Integer>> newReplicaServerList = new ArrayList<>();

      if (cfgReplicaServers != null && !cfgReplicaServers.isEmpty()) {
        final String[] serverEntries = cfgReplicaServers.split(",");
        for (final String serverEntry : serverEntries) {
          try {
            final String[] serverParts = HostUtil.parseHostAddress(serverEntry, HostUtil.CLIENT_DEFAULT_PORT);
            final String sHost = serverParts[0];
            final int sPort = Integer.parseInt(serverParts[1]);

            newReplicaServerList.add(new Pair(sHost, sPort));
          } catch (Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Invalid replica server address '%s'", null, serverEntry);
          }
        }
      }

      publishReplicaServerList(newReplicaServerList);

      LogManager.instance()
          .log(this, Level.FINE, "Remote Database configured with leader=%s and replicas=%s strategy=%s",
              leaderServer, replicaServerList, connectionStrategy);

    } catch (final SecurityException e) {
      throw e;
    } catch (final Exception e) {
      // Cluster configuration response was malformed or contained unresolvable addresses.
      // Fall back to direct connection.
      LogManager.instance()
          .log(this, Level.WARNING, "Unable to parse cluster configuration, using direct connection (%s)",
              null, e.getMessage());
      leaderServer = new Pair<>(originalServer, originalPort);
      publishReplicaServerList(new ArrayList<>());
    }
  }

  /**
   * Atomically replaces the replica server list with a freshly built one and resets the round-robin cursor.
   * The list reference is volatile, so readers (e.g. {@link #getNextReplicaAddress}) that snapshot it observe
   * either the complete old list or the complete new one, never an intermediate cleared state (issue #4579).
   */
  private void publishReplicaServerList(final List<Pair<String, Integer>> newList) {
    this.replicaServerList = newList;
    this.currentReplicaServerIndex = -1;
  }

  private Pair<String, Integer> getNextReplicaAddress() {
    // Snapshot the reference once: a concurrent publishReplicaServerList() may swap it at any time, but the
    // local snapshot is stable for the size check and the get() below, so it can never go out of bounds.
    final List<Pair<String, Integer>> snapshot = replicaServerList;
    final int size = snapshot.size();
    if (size == 0)
      return leaderServer;

    int index = currentReplicaServerIndex + 1;
    if (index < 0 || index >= size)
      index = 0;
    currentReplicaServerIndex = index;

    return snapshot.get(index);
  }

  boolean reloadClusterConfiguration() {
    final Pair<String, Integer> oldLeader = leaderServer;

    // ASK REPLICA FIRST. Snapshot the reference: requestClusterConfiguration() below may swap the
    // live list mid-loop, so iterate over the stable snapshot taken here (issue #4579).
    final List<Pair<String, Integer>> snapshot = replicaServerList;
    for (final Pair<String, Integer> connectToServer : snapshot) {
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

  // Routing for raw httpClient.send() callers (RemoteDatabase.begin/commit/rollback).
  // Regular query/command traffic flows through httpCommand() - any STICKY routing
  // change must update both paths.
  protected String getUrl(final String command) {
    final Pair<String, Integer> pin = getStickyPin();
    final String host = pin != null ? pin.getFirst() : currentServer;
    final int port = pin != null ? pin.getSecond() : currentPort;
    return protocol + "://" + host + ":" + port + "/api/v" + apiVersion + "/" + command;
  }

  String getRequestPayload(final JSONObject jsonRequest) {
    return jsonRequest.toString();
  }

  protected Exception manageException(final HttpResponse<String> response, final String operation) {
    String detail = null;
    String reason = null;
    String exception = null;
    String exceptionArgs = null;
    String responsePayload = response.body();

    try {
      if (responsePayload != null && !responsePayload.isEmpty()) {
        final JSONObject jsonResponse = new JSONObject(responsePayload);
        reason = jsonResponse.has("error") ? jsonResponse.getString("error") : null;
        detail = jsonResponse.has("detail") ? jsonResponse.getString("detail") : null;
        exception = jsonResponse.has("exception") ? jsonResponse.getString("exception") : null;
        exceptionArgs = jsonResponse.has("exceptionArgs") ? jsonResponse.getString("exceptionArgs") : null;
      }
    } catch (final Exception e) {
      // TODO CHECK IF THE COMMAND NEEDS TO BE RE-EXECUTED OR NOT
      LogManager.instance()
          .log(this, Level.WARNING, "Error on executing command, retrying... (payload=%s, error=%s)", null, responsePayload,
              e.toString());
      return e;
    }

    if (exception != null) {
      if (detail == null)
        detail = "Unknown";

      if (exception.equals(ServerIsNotTheLeaderException.class.getName())) {
        // The leader-address hint travels in its own field (exceptionArgs); it is not parsed out of the
        // human-readable detail. Keep the detail message intact so a period in it no longer truncates the
        // text, and preserve the leader address so the client retains the hint to the current leader.
        return new ServerIsNotTheLeaderException(detail, exceptionArgs);
      } else if (exception.equals(RecordNotFoundException.class.getName())) {
        // PARSE THE RID OUT OF THE DETAIL MESSAGE (e.g. "Record #12:7 not found"). BE ROBUST: THE MESSAGE MAY NOT CONTAIN A '#',
        // MAY NOT HAVE A TRAILING SPACE AFTER THE RID, OR MAY CARRY A MALFORMED TOKEN (see issue #4551). FALL BACK TO A null RID.
        RID rid = null;
        final int begin = detail.indexOf("#");
        if (begin > -1) {
          int end = detail.indexOf(" ", begin);
          if (end < 0)
            end = detail.length();
          try {
            rid = new RID(detail.substring(begin, end));
          } catch (final Exception e) {
            // INVALID RID FORMAT: KEEP rid null SO THE TYPED EXCEPTION IS STILL RETURNED
            rid = null;
          }
        }
        return new RecordNotFoundException(detail, rid);
      } else if (exception.equals(QuorumNotReachedException.class.getName())) {
        return new QuorumNotReachedException(detail);
      } else if (exception.equals(DuplicatedKeyException.class.getName()) && exceptionArgs != null) {
        final String[] exceptionArgsParts = exceptionArgs.split("\\|");
        return new DuplicatedKeyException(exceptionArgsParts[0], exceptionArgsParts[1], new RID(exceptionArgsParts[2]));
      } else if (exception.equals(ConcurrentModificationException.class.getName())) {
        return new ConcurrentModificationException(detail);
      } else if (exception.equals(TransactionException.class.getName())) {
        return new TransactionException(detail);
      } else if ("com.arcadedb.server.http.HttpSessionException".equals(exception)) {
        // SERVER-SIDE SUBCLASS OF TransactionException: THE WIRE CARRIES ONLY THE CLASS NAME, SO THE
        // SUBCLASS RELATIONSHIP MUST BE RESTORED EXPLICITLY HERE
        return new TransactionException(detail);
      } else if (exception.equals(TimeoutException.class.getName())) {
        return new TimeoutException(detail);
      } else if (exception.equals(SchemaException.class.getName())) {
        return new SchemaException(detail);
      } else if (exception.equals(NoSuchElementException.class.getName())) {
        return new NoSuchElementException(detail);
      } else if (exception.equals(SecurityException.class.getName())) {
        return new SecurityException(detail);
      } else if ("com.arcadedb.server.security.ServerSecurityException".equals(exception)) {
        return new SecurityException(detail);
      } else if (exception.equals(ConnectException.class.getName())) {
        return new NeedRetryException(detail);
      } else if ("com.arcadedb.server.ha.ReplicationException".equals(exception)) {
        return new NeedRetryException(detail);
      } else if (exception.equals(NeedRetryException.class.getName())) {
        return new NeedRetryException(detail);
      } else
        // ELSE
        return new RemoteException(
            "Error on executing remote operation " + operation + " (cause:" + exception + " detail:" + detail + ")");
    }

    final String httpErrorDescription = response.statusCode() == 400 ? "Bad Request" :
        response.statusCode() == 404 ? "Not Found" :
            response.statusCode() == 500 ? "Internal Server Error" :
                "HTTP Error";

    // TEMPORARY FIX FOR AN ISSUE WITH THE CLIENT/SERVER COMMUNICATION WHERE THE PAYLOAD ARRIVES AS EMPTY
    if (response.statusCode() == 400 && "Bad Request".equals(httpErrorDescription) && "Command text is null".equals(reason)) {
      // RETRY
      return new RemoteException("Empty payload received");
    }

    return new RemoteException(
        "Error on executing remote command '" + operation + "' (httpErrorCode=" + response.statusCode()
            + " httpErrorDescription=" + httpErrorDescription + " reason=" + reason + " detail=" + detail + " exception="
            + exception + ")");
  }
}
