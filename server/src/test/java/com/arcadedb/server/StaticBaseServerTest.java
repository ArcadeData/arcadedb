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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * Executes all the tests while the server is up and running.
 */
public abstract class StaticBaseServerTest {
  public static final    String DEFAULT_PASSWORD_FOR_TESTS = "DefaultPasswordForTests";
  protected static final String VERTEX1_TYPE_NAME          = "V1";
  protected static final String VERTEX2_TYPE_NAME          = "V2";
  protected static final String EDGE1_TYPE_NAME            = "E1";
  protected static final String EDGE2_TYPE_NAME            = "E2";
  private static final   int    PARALLEL_LEVEL             = 4;

  protected static RID              root;
  private static   ArcadeDBServer[] servers;
  private static   Database[]       databases;

  protected interface Callback {
    void call(int serverIndex) throws Exception;
  }

  protected StaticBaseServerTest() {
  }

  public static void setTestConfiguration() {
    GlobalConfiguration.resetAll();
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeAll
  public static void beginTest() {
    checkForActiveDatabases();
    setTestConfiguration();
    checkArcadeIsTotallyDown();

    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "Starting test...");

    deleteDatabaseFolders();

    if (isCreateDatabases()) {
      deleteDatabaseFolders();

      databases = new Database[getServerCount()];
      for (int i = 0; i < getServerCount(); ++i) {
        GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
        databases[i] = new DatabaseFactory(getDatabasePath(i)).create();
        databases[i].async().setParallelLevel(PARALLEL_LEVEL);

      }
    } else
      databases = new Database[0];

    if (isPopulateDatabase()) {
      final Database database = databases[0];
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        Assertions.assertFalse(schema.existsType(VERTEX1_TYPE_NAME));

        VertexType v = schema.createVertexType(VERTEX1_TYPE_NAME, 3);
        v.createProperty("id", Long.class);

        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, VERTEX1_TYPE_NAME, "id");

        Assertions.assertFalse(schema.existsType(VERTEX2_TYPE_NAME));
        schema.createVertexType(VERTEX2_TYPE_NAME, 3);

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
      MutableEdge e1 = v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
      Assertions.assertEquals(e1.getOut(), v1);
      Assertions.assertEquals(e1.getIn(), v2);

      final MutableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
      v3.set("name", "V3");
      v3.save();

      Map<String, Object> params = new HashMap<>();
      params.put("name", "E2");

      // CREATION OF EDGE PASSING PARAMS AS MAP
      MutableEdge e2 = v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
      Assertions.assertEquals(e2.getOut(), v2);
      Assertions.assertEquals(e2.getIn(), v3);

      MutableEdge e3 = v1.newEdge(EDGE2_TYPE_NAME, v3, true);
      Assertions.assertEquals(e3.getOut(), v1);
      Assertions.assertEquals(e3.getIn(), v3);

      db.commit();

      root = v1.getIdentity();
    }

    // CLOSE ALL DATABASES BEFORE STARTING THE SERVERS
    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "TEST: Closing databases before starting");
    for (int i = 0; i < databases.length; ++i) {
      databases[i].close();
      databases[i] = null;
    }
    startServers();
  }

  @AfterAll
  public static void endTest() {
    boolean anyServerRestarted = false;
    try {
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
    } finally {

      try {
        LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "END OF THE TEST: Check DBS are identical...");
        checkDatabasesAreIdentical();
      } finally {

        LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "TEST: Stopping servers...");
        stopServers();

        LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "END OF THE TEST: Cleaning test...");
        if (dropDatabasesAtTheEnd())
          deleteDatabaseFolders();

        checkArcadeIsTotallyDown();

        GlobalConfiguration.TEST.setValue(false);
        GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
      }
    }

    checkForActiveDatabases();
  }

  protected Database getDatabase(final int serverId) {
    return databases[serverId];
  }

  protected static void checkArcadeIsTotallyDown() {
    if (servers != null)
      for (ArcadeDBServer server : servers) {
        Assertions.assertFalse(server.isStarted());
        Assertions.assertEquals(ArcadeDBServer.STATUS.OFFLINE, server.getStatus());
        Assertions.assertEquals(0, server.getHttpServer().getSessionManager().getActiveSessions());
      }

    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final PrintWriter output = new PrintWriter(new BufferedOutputStream(os));
    new Exception().printStackTrace(output);
    output.flush();
    final String out = os.toString();
    Assertions.assertFalse(out.contains("ArcadeDB"), "Some thread is still up & running: \n" + out);
  }

  protected static void startServers() {
    servers = TestServerHelper.startServers(getServerCount(), StaticBaseServerTest::onServerConfiguration, StaticBaseServerTest::onBeforeStarting);
  }

  protected static void stopServers() {
    TestServerHelper.stopServers(servers);
  }

  protected static void onServerConfiguration(final ContextConfiguration config) {
  }

  protected static void onBeforeStarting(ArcadeDBServer server) {
  }

  protected static boolean isCreateDatabases() {
    return true;
  }

  protected static boolean isPopulateDatabase() {
    return true;
  }

  protected static ArcadeDBServer getServer(final int i) {
    return servers[i];
  }

  protected ArcadeDBServer[] getServers() {
    return servers;
  }

  protected Database[] getDatabases() {
    return databases;
  }

  protected static Database getServerDatabase(final int i, final String name) {
    return servers[i].getDatabase(name);
  }

  protected ArcadeDBServer getServer(final String name) {
    for (ArcadeDBServer s : servers) {
      if (s.getServerName().equals(name))
        return s;
    }
    return null;
  }

  protected static int getServerCount() {
    return 1;
  }

  protected static boolean dropDatabasesAtTheEnd() {
    return true;
  }

  protected static String getDatabaseName() {
    return "graph";
  }

  protected static String getDatabasePath(final int serverId) {
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
    final ArcadeDBServer leader = getLeaderServer();
    if (leader == null)
      return false;

    final int onlineReplicas = leader.getHA().getOnlineReplicas();
    if (1 + onlineReplicas < getServerCount()) {
      // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
      LogManager.instance().log(this, Level.FINE, "TEST: Not all the servers are ONLINE (%d), skip this crash...", null, onlineReplicas);
      leader.getHA().printClusterConfiguration();
      return false;
    }
    return true;
  }

  protected static int[] getServerToCheck() {
    final int[] result = new int[getServerCount()];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }

  protected static void deleteDatabaseFolders() {
    if (databases != null)
      for (int i = 0; i < databases.length; ++i) {
        if (databases[i] != null && databases[i].isOpen())
          ((DatabaseInternal) databases[i]).getEmbedded().drop();
      }

    if (servers != null)
      for (int i = 0; i < getServerCount(); ++i)
        for (String dbName : getServer(i).getDatabaseNames())
          if (getServer(i).existsDatabase(dbName))
            ((DatabaseInternal) getServer(i).getDatabase(dbName)).getEmbedded().drop();

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());

    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + i + "/"));
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/replication"));
  }

  protected static void checkDatabasesAreIdentical() {
    final int[] servers2Check = getServerToCheck();

    if (servers2Check.length > 1) {
      LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "END OF THE TEST: Check DBS are identical...");

      for (int i = 1; i < servers2Check.length; ++i) {
        final DatabaseInternal db1 = (DatabaseInternal) getServerDatabase(servers2Check[0], getDatabaseName());
        final DatabaseInternal db2 = (DatabaseInternal) getServerDatabase(servers2Check[i], getDatabaseName());

        // TODO: DISCOVER WHY THIS IS NEEDED. NOW CAN HAPPENS THAT THE TX HAS A DB INSTANCE DIFFERENT FROM THE CURRENT DATABASE. AND IT'S ALWAYS CLOSED
        DatabaseContext.INSTANCE.init(db1);
        DatabaseContext.INSTANCE.init(db2);

        LogManager.instance()
            .log(StaticBaseServerTest.class, Level.FINE, "TEST: Comparing databases '%s' and '%s' are identical...", null, db1.getDatabasePath(),
                db2.getDatabasePath());
        try {
          new DatabaseComparator().compare(db1, db2);
          LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "TEST: OK databases '%s' and '%s' are identical", null, db1.getDatabasePath(),
              db2.getDatabasePath());
        } catch (RuntimeException e) {
          LogManager.instance()
              .log(StaticBaseServerTest.class, Level.FINE, "ERROR on comparing databases '%s' and '%s': %s", null, db1.getDatabasePath(), db2.getDatabasePath(),
                  e.getMessage());
          throw e;
        }
      }
    }
  }

  protected static void testLog(final String msg, final Object... args) {
    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "***********************************************************************************");
    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "TEST: " + msg, args);
    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "***********************************************************************************");
  }

  protected void testEachServer(Callback callback) throws Exception {
    for (int i = 0; i < getServerCount(); i++) {
      callback.call(i);
    }
  }

  private static void checkForActiveDatabases() {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();
    for (Database db : activeDatabases)
      db.close();

    if (!activeDatabases.isEmpty())
      LogManager.instance()
          .log(StaticBaseServerTest.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced close before starting a new test");

    //Assertions.assertTrue(activeDatabases.isEmpty(), "Found active databases: " + activeDatabases);
  }
}
