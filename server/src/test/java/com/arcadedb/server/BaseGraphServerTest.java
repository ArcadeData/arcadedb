/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * This class has been copied under Console project to avoid complex dependencies.
 */
public abstract class BaseGraphServerTest {
  public static final    String DEFAULT_PASSWORD_FOR_TESTS = "DefaultPasswordForTests";
  protected static final String VERTEX1_TYPE_NAME          = "V1";
  protected static final String VERTEX2_TYPE_NAME          = "V2";
  protected static final String EDGE1_TYPE_NAME            = "E1";
  protected static final String EDGE2_TYPE_NAME            = "E2";

  protected static RID              root;
  private          ArcadeDBServer[] servers;
  private          Database         databases[];

  protected interface Callback {
    void call(int serverIndex) throws Exception;
  }

  protected Database getDatabase(final int serverId) {
    return databases[serverId];
  }

  protected BaseGraphServerTest() {
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeEach
  public void beginTest() {
    checkArcadeIsTotallyDown();

    LogManager.instance().log(this, Level.INFO, "Starting test %s...", null, getClass().getName());

    if (isCreateDatabases()) {
      deleteDatabaseFolders();

      databases = new Database[getServerCount()];
      for (int i = 0; i < getServerCount(); ++i) {
        GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
        databases[i] = new DatabaseFactory(getDatabasePath(i)).create();
      }
    } else
      databases = new Database[0];

    if (isPopulateDatabase()) {
      getDatabase(0).transaction(new Database.TransactionScope() {
        @Override
        public void execute(Database database) {
          final Schema schema = database.getSchema();
          Assertions.assertFalse(schema.existsType(VERTEX1_TYPE_NAME));

          VertexType v = schema.createVertexType(VERTEX1_TYPE_NAME, 3);
          v.createProperty("id", Long.class);

          schema.createTypeIndex(SchemaImpl.INDEX_TYPE.LSM_TREE, true, VERTEX1_TYPE_NAME, new String[] { "id" });

          Assertions.assertFalse(schema.existsType(VERTEX2_TYPE_NAME));
          schema.createVertexType(VERTEX2_TYPE_NAME, 3);

          schema.createEdgeType(EDGE1_TYPE_NAME);
          schema.createEdgeType(EDGE2_TYPE_NAME);

          schema.createDocumentType("Person");
        }
      });

      final Database db = getDatabase(0);
      db.begin();

      final MutableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
      v1.set("id", 0);
      v1.set("name", VERTEX1_TYPE_NAME);
      v1.save();

      final MutableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
      v2.set("name", VERTEX2_TYPE_NAME);
      v2.save();

      // CREATION OF EDGE PASSING PARAMS AS VARARGS
      MutableEdge e1 = (MutableEdge) v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
      Assertions.assertEquals(e1.getOut(), v1);
      Assertions.assertEquals(e1.getIn(), v2);

      final MutableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
      v3.set("name", "V3");
      v3.save();

      Map<String, Object> params = new HashMap<>();
      params.put("name", "E2");

      // CREATION OF EDGE PASSING PARAMS AS MAP
      MutableEdge e2 = (MutableEdge) v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
      Assertions.assertEquals(e2.getOut(), v2);
      Assertions.assertEquals(e2.getIn(), v3);

      MutableEdge e3 = (MutableEdge) v1.newEdge(EDGE2_TYPE_NAME, v3, true);
      Assertions.assertEquals(e3.getOut(), v1);
      Assertions.assertEquals(e3.getIn(), v3);

      db.commit();

      root = v1.getIdentity();
    }

    // CLOSE ALL DATABASES BEFORE TO START THE SERVERS
    LogManager.instance().log(this, Level.INFO, "TEST: Closing databases before starting");
    for (int i = 0; i < databases.length; ++i) {
      databases[i].close();
      databases[i] = null;
    }

    startServers();
  }

  @AfterEach
  public void endTest() {
    boolean anyServerRestarted = false;
    if (servers != null) {
      // RESTART ANY SERVER IS DOWN TO CHECK INTEGRITY AFTER THE REALIGNMENT
      for (int i = servers.length - 1; i > -1; --i) {
        if (servers[i] != null && !servers[i].isStarted()) {
          testLog(" Restarting server %d to force re-alignment", i);
          servers[i].start();
          anyServerRestarted = true;
        }
      }
    }

    if (anyServerRestarted) {
      // WAIT A BIT FOR THE SERVER TO BE SYNCHRONIZED
      testLog("Wait a bit until realignment is completed");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    LogManager.instance().log(this, Level.INFO, "TEST: Stopping servers...");
    stopServers();

    try {
      checkDatabasesAreIdentical();
    } finally {
      LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Cleaning test %s...", null, getClass().getName());
      if (dropDatabasesAtTheEnd())
        deleteDatabaseFolders();

      checkArcadeIsTotallyDown();

      GlobalConfiguration.TEST.setValue(false);
    }
  }

  protected void checkArcadeIsTotallyDown() {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final PrintWriter output = new PrintWriter(new BufferedOutputStream(os));
    new Exception().printStackTrace(output);
    output.flush();
    final String out = os.toString();
    Assertions.assertFalse(out.contains("ArcadeDB"), "Some thread is still up & running: \n" + out);
  }

  protected void startServers() {
    final int totalServers = getServerCount();
    servers = new ArcadeDBServer[totalServers];

    int port = 2424;
    String serverURLs = "";
    for (int i = 0; i < totalServers; ++i) {
      if (i > 0)
        serverURLs += ",";

      try {
        serverURLs += (InetAddress.getLocalHost().getHostName()) + ":" + (port++);
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < totalServers; ++i) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + i);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverURLs);
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "0.0.0.0");
      config.setValue(GlobalConfiguration.HA_ENABLED, getServerCount() > 1);

      onServerConfiguration(config);

      servers[i] = new ArcadeDBServer(config);
      onBeforeStarting(servers[i]);
      servers[i].start();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    }
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

  protected void formatPost(final HttpURLConnection connection, final String language, final String payloadCommand, final Map<String, Object> params)
      throws Exception {
    connection.setDoOutput(true);
    if (payloadCommand != null) {
      final JSONObject jsonRequest = new JSONObject();
      jsonRequest.put("language", language);
      jsonRequest.put("command", payloadCommand);

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
  }

  protected void onServerConfiguration(final ContextConfiguration config) {
  }

  protected void onBeforeStarting(ArcadeDBServer server) {
  }

  protected boolean isCreateDatabases() {
    return true;
  }

  protected boolean isPopulateDatabase() {
    return true;
  }

  protected ArcadeDBServer getServer(final int i) {
    return servers[i];
  }

  protected Database getServerDatabase(final int i, final String name) {
    return servers[i].getDatabase(name);
  }

  protected ArcadeDBServer getServer(final String name) {
    for (ArcadeDBServer s : servers) {
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
    return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + serverId + "/" + getDatabaseName();
  }

  protected String readResponse(final HttpURLConnection connection) throws IOException {
    InputStream in = connection.getInputStream();
    Scanner scanner = new Scanner(in);

    final StringBuilder buffer = new StringBuilder();

    while (scanner.hasNext()) {
      buffer.append(scanner.next().replace('\n', ' '));
    }

    return buffer.toString();
  }

  protected void executeAsynchronously(final Callable callback) {
    final Timer task = new Timer();
    task.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          callback.call();
        } catch (Exception e) {
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
    final int onlineReplicas = getLeaderServer().getHA().getOnlineReplicas();
    if (1 + onlineReplicas < getServerCount()) {
      // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
      LogManager.instance().log(this, Level.INFO, "TEST: Not all the servers are ONLINE (%d), skip this crash...", null, onlineReplicas);
      getLeaderServer().getHA().printClusterConfiguration();
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
    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(getDatabasePath(i)));
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/replication"));
  }

  protected void deleteAllDatabases() {
    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + i + "/"));
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/replication"));
  }

  protected void checkDatabasesAreIdentical() {
    final int[] servers2Check = getServerToCheck();

    if (servers2Check.length > 1) {
      LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Check DBS are identical...");

      for (int i = 1; i < servers2Check.length; ++i) {
        final DatabaseInternal db1 = (DatabaseInternal) getServerDatabase(servers2Check[0], getDatabaseName());
        final DatabaseInternal db2 = (DatabaseInternal) getServerDatabase(servers2Check[i], getDatabaseName());

        // TODO: DISCOVER WHY THIS IS NEEDED. NOW CAN HAPPENS THAT THE TX HAS A DB INSTANCE DIFFERENT FROM THE CURRENT DATABASE. AND IT'S ALWAYS CLOSED
        DatabaseContext.INSTANCE.init(db1);
        DatabaseContext.INSTANCE.init(db2);

        testLog("Comparing databases '%s' and '%s' are identical...", db1.getDatabasePath(), db2.getDatabasePath());
        new DatabaseComparator().compare(db1, db2);
      }
    }
  }

  protected void testLog(final String msg, final Object... args) {
    LogManager.instance()
        .log(this, Level.INFO, "****************************************************************************************************************");
    LogManager.instance().log(this, Level.INFO, "TEST: " + msg, null, args);
    LogManager.instance()
        .log(this, Level.INFO, "****************************************************************************************************************");

  }

  protected void testEachServer(Callback callback) throws Exception {
    for (int i = 0; i < getServerCount(); i++) {
      callback.call(i);
    }
  }

}
