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
 */
package com.arcadedb.redis;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

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
  private          Database[]       databases;

  protected Database getDatabase(final int serverId) {
    return databases[serverId];
  }

  protected BaseGraphServerTest() {
  }

  public void setTestConfiguration() {
    GlobalConfiguration.resetAll();
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeEach
  public void beginTest() {
    setTestConfiguration();

    checkArcadeIsTotallyDown();

    LogManager.instance().log(this, Level.INFO, "Starting test %s...", null, getClass().getName());

    deleteDatabaseFolders();

    databases = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); ++i) {
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
      databases[i] = new DatabaseFactory(getDatabasePath(i)).create();
    }

    final Database database = getDatabase(0);
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute() {
        if (isPopulateDatabase()) {
          Assertions.assertFalse(database.getSchema().existsType(VERTEX1_TYPE_NAME));

          VertexType v = database.getSchema().createVertexType(VERTEX1_TYPE_NAME, 3);
          v.createProperty("id", Long.class);

          database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, VERTEX1_TYPE_NAME, "id");

          Assertions.assertFalse(database.getSchema().existsType(VERTEX2_TYPE_NAME));
          database.getSchema().createVertexType(VERTEX2_TYPE_NAME, 3);

          database.getSchema().createEdgeType(EDGE1_TYPE_NAME);
          database.getSchema().createEdgeType(EDGE2_TYPE_NAME);

          database.getSchema().createDocumentType("Person");
        }
      }
    });

    if (isPopulateDatabase()) {
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
    try {
      LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Check DBS are identical...");
      checkDatabasesAreIdentical();
    } finally {
      LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Cleaning test %s...", null, getClass().getName());
      if (servers != null)
        for (int i = servers.length - 1; i > -1; --i) {
          if (servers[i] != null)
            servers[i].stop();
        }

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
      serverURLs += "localhost:" + (port++);
    }

    for (int i = 0; i < totalServers; ++i) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + i);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverURLs);
      config.setValue(GlobalConfiguration.HA_ENABLED, getServerCount() > 1);

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

  protected void onBeforeStarting(ArcadeDBServer server) {
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
    for (int i = 0; i < getServerCount(); ++i) {
      if (getServer(i).existsDatabase(getDatabaseName()))
        getServer(i).getDatabase(getDatabaseName()).drop();
    }

    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(getDatabasePath(i)));
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/replication"));
  }

  protected void checkDatabasesAreIdentical() {
    final int[] servers2Check = getServerToCheck();

    for (int i = 1; i < servers2Check.length; ++i) {
      final Database db1 = getServerDatabase(servers2Check[0], getDatabaseName());
      final Database db2 = getServerDatabase(servers2Check[i], getDatabaseName());

      LogManager.instance().log(this, Level.INFO, "TEST: Comparing databases '%s' and '%s' are identical...", null, db1, db2);
      new DatabaseComparator().compare(db1, db2);
    }
  }
}
