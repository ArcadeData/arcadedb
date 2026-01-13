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
package com.arcadedb.server;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.HATestTimeouts;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class has been copied under Console project to avoid complex dependencies.
 */
public abstract class BaseGraphServerTest extends StaticBaseServerTest {
  private static final   int              PARALLEL_LEVEL      = 4;
  protected static final String           VERTEX1_TYPE_NAME   = "V1";
  protected static final String           VERTEX2_TYPE_NAME   = "V2";
  protected static final String           EDGE1_TYPE_NAME     = "E1";
  protected static final String           EDGE2_TYPE_NAME     = "E2";
  protected static       RID              root;
  private                ArcadeDBServer[] servers;
  private                Database[]       databases;
  protected volatile     boolean          serversSynchronized = false;

  protected interface Callback {
    void call(int serverIndex) throws Exception;
  }

  protected BaseGraphServerTest() {
    LogManager.instance().setContext("TEST");
  }

  @BeforeEach
  public void beginTest() {

    System.out.println("-------------------- BEING TEST---------------");
    checkForActiveDatabases();

    setTestConfiguration();

    checkArcadeIsTotallyDown();

    LogManager.instance().log(this, Level.FINE, "Starting test %s...", getClass().getName());

    deleteDatabaseFolders();

    prepareDatabase();

    startServers();

    System.out.println("-------------------- BEING TEST - started---------------");


  }

  private void prepareDatabase() {
    if (isCreateDatabases()) {
      databases = new Database[getServerCount()];

      //create database for server 0
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
      databases[0] = new DatabaseFactory(getDatabasePath(0)).create();
      databases[0].async().setParallelLevel(PARALLEL_LEVEL);
      populateDatabase();
      databases[0].close();

      //copy the database under other servers
      for (int i = 1; i < getServerCount(); ++i) {
        try {
          FileUtils.copyDirectory(new File(getDatabasePath(0)), new File(getDatabasePath(i)));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    } else
      databases = new Database[0];

    if (databases != null)
      for (Database db : databases)
        if (db != null && db.isOpen())
          databases[0].close();
  }

  protected void populateDatabase() {
    final Database database = databases[0];
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      assertThat(schema.existsType(VERTEX1_TYPE_NAME)).isFalse();

      final VertexType v = schema.buildVertexType().withName(VERTEX1_TYPE_NAME).withTotalBuckets(3).create();
      v.createProperty("id", Long.class);

      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, VERTEX1_TYPE_NAME, "id");

      assertThat(schema.existsType(VERTEX2_TYPE_NAME)).isFalse();
      schema.buildVertexType().withName(VERTEX2_TYPE_NAME).withTotalBuckets(3).create();

      schema.createEdgeType(EDGE1_TYPE_NAME);
      schema.createEdgeType(EDGE2_TYPE_NAME);

      schema.createDocumentType("Person");
    });

    final Database db = databases[0];
    db.begin();

    final MutableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
    v1.set("id", 0);
    v1.set("name", VERTEX1_TYPE_NAME);
    v1.save();

    final MutableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
    v2.set("name", VERTEX2_TYPE_NAME);
    v2.save();

    // CREATION OF EDGE PASSING PARAMS AS VARARGS
    final MutableEdge e1 = v1.newEdge(EDGE1_TYPE_NAME, v2, "name", "E1");
    assertThat(v1).isEqualTo(e1.getOut());
    assertThat(v2).isEqualTo(e1.getIn());

    final MutableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
    v3.set("name", "V3");
    v3.save();

    final Map<String, Object> params = new HashMap<>();
    params.put("name", "E2");

    // CREATION OF EDGE PASSING PARAMS AS MAP
    final MutableEdge e2 = v2.newEdge(EDGE2_TYPE_NAME, v3, params);
    assertThat(v2).isEqualTo(e2.getOut());
    assertThat(v3).isEqualTo(e2.getIn());

    final MutableEdge e3 = v1.newEdge(EDGE2_TYPE_NAME, v3);
    assertThat(v1).isEqualTo(e3.getOut());
    assertThat(v3).isEqualTo(e3.getIn());

    db.commit();

    root = v1.getIdentity();
  }

  protected void waitForReplicationIsCompleted(final int serverNumber) {
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> getServer(serverNumber).getHA().getMessagesInQueue() == 0);
  }

  @AfterEach
  public void endTest() {

    System.out.println("-------------------- END TEST---------------");

    boolean anyServerRestarted = false;
    try {
      if (servers != null) {
        // RESTART ANY SERVER IS DOWN TO CHECK INTEGRITY AFTER THE REALIGNMENT
        for (int i = servers.length - 1; i > -1; --i) {
          if (servers[i] != null && !servers[i].isStarted()) {
            testLog(" Restarting server %d to force re-alignment", i);
            if (servers[i].getHttpServer() != null) {
              final int oldPort = servers[i].getHttpServer().getPort();
              servers[i].getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, oldPort);
              servers[i].start();
              anyServerRestarted = true;
            }
          }
        }
      }

      if (anyServerRestarted) {
        // WAIT A BIT FOR THE SERVER TO BE SYNCHRONIZED
        testLog("Wait a bit until realignment is completed");
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .ignoreExceptions()
            .until(() -> {
              // Check if all servers are synchronized
              for (int i = 0; i < servers.length; i++) {
                if (servers[i] != null && servers[i].isStarted()) {
                  if (servers[i].getHA() != null && !servers[i].getHA().isLeader()) {
                    // For replicas, check if they're aligned
                    if (servers[i].getHA().getMessagesInQueue() > 0) {
                      return false;
                    }
                  }
                }
              }
              return true;
            });
      }
    } finally {
      try {
        LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Check DBS are identical...");
        checkDatabasesAreIdentical();
      } catch (Exception e) {
        //ignore
      } finally {
        GlobalConfiguration.resetAll();

        LogManager.instance().log(this, Level.INFO, "TEST: Stopping servers...");
        stopServers();

        LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Cleaning test %s...", getClass().getName());
        if (dropDatabasesAtTheEnd())
          deleteDatabaseFolders();

        checkArcadeIsTotallyDown();

        GlobalConfiguration.TEST.setValue(false);
        GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
        TestServerHelper.checkActiveDatabases(dropDatabasesAtTheEnd());
        TestServerHelper.deleteDatabaseFolders(getServerCount());
      }
    }
  }

  protected Database getDatabase(final int serverId) {
    return databases[serverId];
  }

  protected void checkArcadeIsTotallyDown() {
    if (servers != null)
      for (final ArcadeDBServer server : servers) {
        if (server != null) {
          assertThat(server.isStarted()).isFalse();
          assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.Status.OFFLINE);
          assertThat(server.getHttpServer().getSessionManager().getActiveSessions()).isEqualTo(0);
        }
      }

    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final PrintWriter output = new PrintWriter(new BufferedOutputStream(os));
    new Exception().printStackTrace(output);
    output.flush();
    final String out = os.toString();
    assertThat(out.contains("ArcadeDB")).as("Some thread is still up & running: \n" + out).isFalse();
  }

  protected String getServerAddresses() {
    int port = 2424;
    StringBuilder serverURLs = new StringBuilder();
    for (int i = 0; i < getServerCount(); ++i) {
      if (i > 0)
        serverURLs.append(",");

      serverURLs.append("localhost:").append(port++);
    }
    return serverURLs.toString();
  }

  protected void startServers() {
    final int totalServers = getServerCount();
    servers = new ArcadeDBServer[totalServers];

    for (int i = 0; i < totalServers; ++i) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + i);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, getServerAddresses());
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_ENABLED, getServerCount() > 1);
      config.setValue(GlobalConfiguration.HA_SERVER_ROLE, getServerRole(i));
      //config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, 2000);

      onServerConfiguration(config);

      servers[i] = new ArcadeDBServer(config);
      onBeforeStarting(servers[i]);
      servers[i].start();

      LogManager.instance().log(this, Level.FINE, "Server %d database directory: %s", i,
          servers[i].getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    }

    waitAllReplicasAreConnected();
  }

  protected HAServer.ServerRole getServerRole(final int serverIndex) {
    return serverIndex == 0 ? HAServer.ServerRole.ANY : HAServer.ServerRole.REPLICA;
  }

  protected void waitAllReplicasAreConnected() {
    final int serverCount = getServerCount();
    if (serverCount == 1)
      return;

    try {
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> {
            // Safely find the leader without NPE during election phase
            ArcadeDBServer leader = null;
            for (int i = 0; i < serverCount; i++) {
              if (servers[i] != null && servers[i].getHA() != null && servers[i].getHA().isLeader()) {
                leader = servers[i];
                break;
              }
            }

            // If no leader elected yet, continue waiting
            if (leader == null) {
              LogManager.instance().log(this, Level.FINER, "Waiting for leader election...");
              return false;
            }

            // Leader elected, check if all replicas are connected
            final int onlineReplicas = leader.getHA().getOnlineReplicas();
            if (onlineReplicas >= serverCount - 1) {
              // ALL CONNECTED
              serversSynchronized = true;
              LogManager.instance().log(this, Level.INFO, "All %d replicas are online (leader: %s)", onlineReplicas, leader.getServerName());
              return true;
            } else {
              LogManager.instance().log(this, Level.FINER, "Waiting for replicas: %d/%d online", onlineReplicas, serverCount - 1);
              return false;
            }
          });
    } catch (ConditionTimeoutException e) {
      int lastTotalConnectedReplica = 0;
      ArcadeDBServer leaderAtTimeout = null;
      for (int i = 0; i < serverCount; ++i) {
        if (servers[i] != null && servers[i].getHA() != null && servers[i].getHA().isLeader()) {
          leaderAtTimeout = servers[i];
          lastTotalConnectedReplica = servers[i].getHA().getOnlineReplicas();
          break;
        }
      }
      LogManager.instance()
          .log(this, Level.SEVERE, "Timeout waiting for cluster to stabilize. Leader: %s, Online replicas: %d/%d",
              leaderAtTimeout != null ? leaderAtTimeout.getServerName() : "NONE",
              lastTotalConnectedReplica,
              serverCount - 1);
      throw new RuntimeException("Cluster failed to stabilize: expected " + serverCount + " servers, only " +
          (lastTotalConnectedReplica + 1) + " connected", e);
    }
  }

  protected boolean areAllReplicasAreConnected() {
    final int serverCount = getServerCount();

    int lastTotalConnectedReplica;

    for (int i = 0; i < serverCount; ++i) {
      if (getServerRole(i) == HAServer.ServerRole.ANY) {
        // ONLY FOR CANDIDATE LEADERS
        if (servers[i].getHA() != null) {
          if (servers[i].getHA().isLeader()) {
            lastTotalConnectedReplica = servers[i].getHA().getOnlineReplicas();
            if (lastTotalConnectedReplica >= serverCount - 1)
              return true;
          }
        }
      }
    }
    return false;
  }

  protected void stopServers() {
    if (servers != null) {
      // RESTART ANY SERVER IS DOWN TO CHECK INTEGRITY AFTER THE REALIGNMENT
      for (int i = servers.length - 1; i > -1; --i) {
        if (servers[i] != null)
          servers[i].stop();
      }
    }
  }

  protected void formatPayload(final HttpURLConnection connection, final String language, final String payloadCommand,
      final String serializer, final Map<String, Object> params) throws Exception {
    if (payloadCommand != null) {
      final JSONObject jsonRequest = new JSONObject();
      jsonRequest.put("language", language);
      jsonRequest.put("command", payloadCommand);
      if (serializer != null)
        jsonRequest.put("serializer", serializer);

      if (params != null) {
        final JSONObject jsonParams = new JSONObject(params);
        jsonRequest.put("params", jsonParams);
      }

      formatPayload(connection, jsonRequest);
    } else
      connection.setDoOutput(true);
  }

  protected void formatPayload(final HttpURLConnection connection, final JSONObject payload) throws Exception {
    connection.setDoOutput(true);
    final byte[] data = payload.toString().getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(data);
    }
  }

  protected void onServerConfiguration(final ContextConfiguration config) {
  }

  protected void onBeforeStarting(final ArcadeDBServer server) {
  }

  protected boolean isCreateDatabases() {
    return true;
  }

  protected ArcadeDBServer getServer(final int i) {
    return servers[i];
  }

  protected ArcadeDBServer[] getServers() {
    return servers;
  }

  protected Database[] getDatabases() {
    return databases;
  }

  protected Database getServerDatabase(final int i, final String name) {
    return servers[i] != null ? servers[i].getDatabase(name) : null;
  }

  protected ArcadeDBServer getServer(final String name) {
    for (final ArcadeDBServer s : servers) {
      if (s.getServerName().equals(name))
        return s;
    }
    return null;
  }

  protected int getServerNumber(final String name) {
    for (int i = 0; i < servers.length; i++) {
      if (servers[i].getServerName().equals(name))
        return i;
    }
    return -1;
  }

  protected int getServerCount() {
    return 1;
  }

  protected boolean dropDatabasesAtTheEnd() {
    return true;
  }

  protected String getDatabaseName() {
    return "graph";
  }

  protected String getDatabasePath(final int serverId) {
    return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + serverId + File.separator + getDatabaseName();
  }

  protected static String readResponse(final HttpURLConnection connection) throws IOException {
    final InputStream in = connection.getInputStream();
    final String buffer = FileUtils.readStreamAsString(in, "utf8");
    return buffer.replace('\n', ' ');
  }

  protected String readError(final HttpURLConnection connection) throws IOException {
    final InputStream in = connection.getErrorStream();
    final String buffer = FileUtils.readStreamAsString(in, "utf8");
    return buffer.replace('\n', ' ');
  }

  protected void executeAsynchronously(final Callable callback) {
    final Timer task = new Timer();
    task.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          callback.call();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }, 1);
  }

  protected ArcadeDBServer getLeaderServer() {
    for (int i = 0; i < getServerCount(); ++i)
      if (getServer(i).isStarted()) {
        final ArcadeDBServer onlineServer = getServer(i);
        final String leaderName = onlineServer.getHA().getLeaderName();
        return getServer(leaderName);
      }
    return null;
  }

  protected int[] getServerToCheck() {
    final int[] result = new int[getServerCount()];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }

  protected void deleteDatabaseFolders() {
    if (databases != null)
      for (Database database : databases) {
        if (database != null && database.isOpen())
          ((DatabaseInternal) database).getEmbedded().drop();
      }

    if (servers != null)
      for (int i = 0; i < getServerCount(); ++i)
        if (getServer(i) != null)
          for (final String dbName : getServer(i).getDatabaseNames())
            if (getServer(i).existsDatabase(dbName))
              ((DatabaseInternal) getServer(i).getDatabase(dbName)).getEmbedded().drop();

    TestServerHelper.checkActiveDatabases(dropDatabasesAtTheEnd());
    TestServerHelper.deleteDatabaseFolders(getServerCount());
  }

  protected void checkDatabasesAreIdentical() {
    final int[] servers2Check = getServerToCheck();

    for (int i = 1; i < servers2Check.length; ++i) {
      final Database db1 = getServerDatabase(servers2Check[0], getDatabaseName());
      final Database db2 = getServerDatabase(servers2Check[i], getDatabaseName());

      if (db1 == null || db2 == null)
        continue;

      LogManager.instance().log(this, Level.FINE, "TEST: Comparing databases '%s' and '%s' are identical...", db1.getDatabasePath(),
          db2.getDatabasePath());
      try {
        new DatabaseComparator().compare(db1, db2);
        LogManager.instance()
            .log(this, Level.FINE, "TEST: OK databases '%s' and '%s' are identical", db1.getDatabasePath(), db2.getDatabasePath());
      } catch (final RuntimeException e) {
        LogManager.instance()
            .log(this, Level.FINE, "ERROR on comparing databases '%s' and '%s': %s", db1.getDatabasePath(), db2.getDatabasePath(),
                e.getMessage());
        throw e;
      }
    }
  }

  protected void testEachServer(final Callback callback) throws Exception {
    for (int i = 0; i < getServerCount(); i++) {
      LogManager.instance()
          .log(this, Level.FINE, "***********************************************************************************");
      LogManager.instance().log(this, Level.FINE, "EXECUTING TEST ON SERVER %d/%d...", i, getServerCount());
      LogManager.instance()
          .log(this, Level.FINE, "***********************************************************************************");
      try {
        callback.call(i);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on executing test %s on server %d/%d", e, getClass().getName(), i + 1,
            getServerCount());
        throw e;
      }
    }
  }

  private void checkForActiveDatabases() {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();
    for (final Database db : activeDatabases)
      db.close();

    if (!activeDatabases.isEmpty())
      LogManager.instance()
          .log(this, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced close before starting a new test");

    assertThat(activeDatabases.isEmpty()).isTrue().as("Found active databases: " + activeDatabases);
  }

  protected String command(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection initialConnection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph")
        .toURL()
        .openConnection();
    try {

      initialConnection.setRequestMethod("POST");
      initialConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(initialConnection, "sql", command, null, new HashMap<>());
      initialConnection.connect();

      final String response = readResponse(initialConnection);

      LogManager.instance().log(this, Level.FINE, "Response: %s", response);
      assertThat(initialConnection.getResponseCode()).isEqualTo(200);
      assertThat(initialConnection.getResponseMessage()).isEqualTo("OK");
      return response;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on connecting to server %s", e, "http://127.0.0.1:248" + serverIndex);
      throw e;
    } finally {
      initialConnection.disconnect();
    }
  }

  protected JSONObject executeCommand(final int serverIndex, final String language, final String payloadCommand) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection, language, payloadCommand, "studio", Collections.emptyMap());
    connection.connect();

    try {
      final String response = readResponse(connection);
      LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(connection.getResponseMessage()).isEqualTo("OK");

      return new JSONObject(response);

    } catch (Exception e) {
      if (connection.getErrorStream() != null) {
        String responsePayload = FileUtils.readStreamAsString(connection.getErrorStream(), "UTF8");
        LogManager.instance().log(this, Level.SEVERE, "Error: " + responsePayload);
      }
      return null;
    } finally {
      connection.disconnect();
    }
  }

  protected ArcadeDBServer getLeader() {
    for (int i = 0; i < getServerCount(); ++i) {
      ArcadeDBServer server = getServer(i);
      if (server != null && server.getHA() != null && server.getHA().isLeader())
        return server;
    }
    return null;
  }

  /**
   * Get leader with retry logic during election phase.
   * Waits up to 30 seconds for a leader to be elected.
   */
  protected ArcadeDBServer getLeaderWithRetry() {
    try {
      return Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> getLeader(), java.util.Objects::nonNull);
    } catch (ConditionTimeoutException e) {
      throw new RuntimeException("No leader elected after 30 seconds", e);
    }
  }

  /**
   * Waits for the entire cluster to stabilize after server operations.
   *
   * <p>This method performs a 3-phase stabilization check:
   * <ol>
   *   <li>Phase 1: Wait for all servers to be ONLINE
   *   <li>Phase 2: Wait for all replication queues to drain
   *   <li>Phase 3: Wait for all replicas to be connected to leader
   * </ol>
   *
   * <p>Use this after server start/stop/restart operations or after data modifications
   * to ensure the cluster is fully synchronized before making assertions.
   *
   * @param serverCount number of servers in the cluster
   * @throws org.awaitility.core.ConditionTimeoutException if stabilization doesn't complete within timeout
   */
  protected void waitForClusterStable(final int serverCount) {
    LogManager.instance().log(this, Level.FINE, "TEST: Waiting for cluster to stabilize (%d servers)...", serverCount);

    // Phase 1: Wait for all servers to be ONLINE
    Awaitility.await("all servers ONLINE")
        .atMost(HATestTimeouts.CLUSTER_STABILIZATION_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
        .until(() -> {
          for (int i = 0; i < serverCount; i++) {
            final ArcadeDBServer server = getServer(i);
            if (server.getStatus() != ArcadeDBServer.Status.ONLINE) {
              return false;
            }
          }
          return true;
        });

    // Phase 2: Wait for replication queues to drain
    for (int i = 0; i < serverCount; i++) {
      waitForReplicationIsCompleted(i);
    }

    // Phase 3: Wait for all replicas to be connected
    Awaitility.await("all replicas connected")
        .atMost(HATestTimeouts.REPLICA_RECONNECTION_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
        .until(() -> {
          try {
            return areAllReplicasAreConnected();
          } catch (Exception e) {
            return false;
          }
        });

    LogManager.instance().log(this, Level.FINE, "TEST: Cluster stabilization complete");
  }

  /**
   * Waits for a server to complete shutdown.
   *
   * <p>Ensures the server fully completes shutdown before proceeding. This prevents
   * tests from restarting servers that are still shutting down.
   *
   * @param server the server that is shutting down
   * @param serverId server index (for logging)
   * @throws org.awaitility.core.ConditionTimeoutException if shutdown doesn't complete within timeout
   */
  protected void waitForServerShutdown(final ArcadeDBServer server, final int serverId) {
    LogManager.instance().log(this, Level.FINE, "TEST: Waiting for server %d to complete shutdown...", serverId);

    Awaitility.await("server shutdown")
        .atMost(HATestTimeouts.SERVER_SHUTDOWN_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL_LONG)
        .until(() -> server.getStatus() != ArcadeDBServer.Status.SHUTTING_DOWN);

    LogManager.instance().log(this, Level.FINE, "TEST: Server %d shutdown complete", serverId);
  }

  /**
   * Waits for a server to complete startup and join the cluster.
   *
   * <p>Ensures the server fully completes startup and joins the cluster before
   * proceeding. This prevents tests from running operations on servers that
   * are still initializing.
   *
   * @param server the server that is starting
   * @param serverId server index (for logging)
   * @throws org.awaitility.core.ConditionTimeoutException if startup doesn't complete within timeout
   */
  protected void waitForServerStartup(final ArcadeDBServer server, final int serverId) {
    LogManager.instance().log(this, Level.FINE, "TEST: Waiting for server %d to complete startup...", serverId);

    Awaitility.await("server startup")
        .atMost(HATestTimeouts.SERVER_STARTUP_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL_LONG)
        .until(() -> server.getStatus() == ArcadeDBServer.Status.ONLINE);

    LogManager.instance().log(this, Level.FINE, "TEST: Server %d startup complete", serverId);
  }

}
