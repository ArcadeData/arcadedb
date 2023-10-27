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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * This class has been copied under Console project to avoid complex dependencies.
 */
public abstract class BaseGraphServerTest extends StaticBaseServerTest {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";
  private static final   int    PARALLEL_LEVEL    = 4;

  protected static   RID              root;
  private            ArcadeDBServer[] servers;
  private            Database[]       databases;
  protected volatile boolean          serversSynchronized = false;

  protected interface Callback {
    void call(int serverIndex) throws Exception;
  }

  protected BaseGraphServerTest() {
  }

  @BeforeEach
  public void beginTest() {
    checkForActiveDatabases();

    setTestConfiguration();

    checkArcadeIsTotallyDown();

    LogManager.instance().log(this, Level.FINE, "Starting test %s...", getClass().getName());

    deleteDatabaseFolders();

    if (isCreateDatabases()) {
      databases = new Database[getServerCount()];

      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
      databases[0] = new DatabaseFactory(getDatabasePath(0)).create();
      databases[0].async().setParallelLevel(PARALLEL_LEVEL);
      populateDatabase();
      databases[0].close();

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

    startServers();
  }

  protected void populateDatabase() {
    final Database database = databases[0];
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      Assertions.assertFalse(schema.existsType(VERTEX1_TYPE_NAME));

      final VertexType v = schema.buildVertexType().withName(VERTEX1_TYPE_NAME).withTotalBuckets(3).create();
      v.createProperty("id", Long.class);

      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, VERTEX1_TYPE_NAME, "id");

      Assertions.assertFalse(schema.existsType(VERTEX2_TYPE_NAME));
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
    final MutableEdge e1 = v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
    Assertions.assertEquals(e1.getOut(), v1);
    Assertions.assertEquals(e1.getIn(), v2);

    final MutableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
    v3.set("name", "V3");
    v3.save();

    final Map<String, Object> params = new HashMap<>();
    params.put("name", "E2");

    // CREATION OF EDGE PASSING PARAMS AS MAP
    final MutableEdge e2 = v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
    Assertions.assertEquals(e2.getOut(), v2);
    Assertions.assertEquals(e2.getIn(), v3);

    final MutableEdge e3 = v1.newEdge(EDGE2_TYPE_NAME, v3, true);
    Assertions.assertEquals(e3.getOut(), v1);
    Assertions.assertEquals(e3.getIn(), v3);

    db.commit();

    root = v1.getIdentity();
  }

  protected void waitForReplicationIsCompleted(final int serverNumber) {
    while (getServer(serverNumber).getHA().getMessagesInQueue() > 0) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AfterEach
  public void endTest() {
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
        try {
          Thread.sleep(5000);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      }
    } finally {
      try {
        LogManager.instance().log(this, Level.FINE, "END OF THE TEST: Check DBS are identical...");
        checkDatabasesAreIdentical();
      } finally {
        GlobalConfiguration.resetAll();

        LogManager.instance().log(this, Level.FINE, "TEST: Stopping servers...");
        stopServers();

        LogManager.instance().log(this, Level.FINE, "END OF THE TEST: Cleaning test %s...", getClass().getName());
        if (dropDatabasesAtTheEnd())
          deleteDatabaseFolders();

        checkArcadeIsTotallyDown();

        GlobalConfiguration.TEST.setValue(false);
        GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
      }
    }

    TestServerHelper.checkActiveDatabases(dropDatabasesAtTheEnd());
    TestServerHelper.deleteDatabaseFolders(getServerCount());
  }

  protected Database getDatabase(final int serverId) {
    return databases[serverId];
  }

  protected void checkArcadeIsTotallyDown() {
    if (servers != null)
      for (final ArcadeDBServer server : servers) {
        if (server != null) {
          Assertions.assertFalse(server.isStarted());
          Assertions.assertEquals(ArcadeDBServer.STATUS.OFFLINE, server.getStatus());
          Assertions.assertEquals(0, server.getHttpServer().getSessionManager().getActiveSessions());
        }
      }

    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final PrintWriter output = new PrintWriter(new BufferedOutputStream(os));
    new Exception().printStackTrace(output);
    output.flush();
    final String out = os.toString();
    Assertions.assertFalse(out.contains("ArcadeDB"), "Some thread is still up & running: \n" + out);
  }

  protected String getServerAddresses() {
    int port = 2424;
    String serverURLs = "";
    for (int i = 0; i < getServerCount(); ++i) {
      if (i > 0)
        serverURLs += ",";

      serverURLs += "localhost:" + (port++);
    }
    return serverURLs;
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

  protected HAServer.SERVER_ROLE getServerRole(final int serverIndex) {
    return serverIndex == 1 ? HAServer.SERVER_ROLE.REPLICA : HAServer.SERVER_ROLE.ANY;
  }

  protected void waitAllReplicasAreConnected() {
    final int serverCount = getServerCount();

    int lastTotalConnectedReplica = 0;
    final long beginTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - beginTime < 10_000) {
      for (int i = 0; i < serverCount; ++i) {
        if (servers[i].getHA() != null && servers[i].getHA().isLeader()) {
          lastTotalConnectedReplica = servers[i].getHA().getOnlineReplicas();
          if (lastTotalConnectedReplica >= serverCount - 1) {
            // ALL CONNECTED
            serversSynchronized = true;
            return;
          }

          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            break;
          }
        } else {
          serversSynchronized = true;
          return;
        }
      }
    }

    LogManager.instance()
        .log(this, Level.SEVERE, "Timeout on waiting for all servers to get online %d < %d", 1 + lastTotalConnectedReplica,
            serverCount);
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

  protected boolean areAllServersOnline() {
    final ArcadeDBServer leader = getLeaderServer();
    if (leader == null)
      return false;

    final int onlineReplicas = leader.getHA().getOnlineReplicas();
    if (1 + onlineReplicas < getServerCount()) {
      // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
      LogManager.instance().log(this, Level.FINE, "TEST: Not all the servers are ONLINE (%d), skip this crash...", onlineReplicas);
      leader.getHA().printClusterConfiguration();
      return false;
    }
    return true;
  }

  protected int[] getServerToCheck() {
    final int[] result = new int[getServerCount()];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }

  protected void deleteDatabaseFolders() {
    if (databases != null)
      for (int i = 0; i < databases.length; ++i) {
        if (databases[i] != null && databases[i].isOpen())
          ((DatabaseInternal) databases[i]).getEmbedded().drop();
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

    //Assertions.assertTrue(activeDatabases.isEmpty(), "Found active databases: " + activeDatabases);
  }

  protected String command(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection initialConnection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    try {

      initialConnection.setRequestMethod("POST");
      initialConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(initialConnection, "sql", command, null, new HashMap<>());
      initialConnection.connect();

      final String response = readResponse(initialConnection);

      LogManager.instance().log(this, Level.FINE, "Response: %s", response);
      Assertions.assertEquals(200, initialConnection.getResponseCode());
      Assertions.assertEquals("OK", initialConnection.getResponseMessage());
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
      Assertions.assertEquals(200, connection.getResponseCode());
      Assertions.assertEquals("OK", connection.getResponseMessage());

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
}
