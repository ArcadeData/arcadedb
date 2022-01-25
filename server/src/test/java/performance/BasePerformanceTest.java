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
package performance;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;

public abstract class BasePerformanceTest {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";

  protected static RID              root;
  private          ArcadeDBServer[] servers;
  protected        Database[]       databases;

  protected Database getDatabase(final int serverId) {
    return databases[serverId];
  }

  public void setTestConfiguration() {
    GlobalConfiguration.resetAll();
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
  }

  protected void endTest() {
    try {
      LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Check DBS are identical...");
      checkDatabasesAreIdentical();
    } finally {
      LogManager.instance().log(this, Level.INFO, "END OF THE TEST: Cleaning test %s...", null, getClass().getName());
      stopServers();

      if (dropDatabasesAtTheEnd())
        deleteDatabaseFolders();

      checkArcadeIsTotallyDown();

      GlobalConfiguration.TEST.setValue(false);
    }
  }

  protected void stopServers() {
    if (servers != null)
      for (int i = servers.length - 1; i > -1; --i) {
        if (servers[i] != null)
          servers[i].stop();
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
      for (int i = 0; i < databases.length; ++i) {
        if (databases[i] != null && databases[i].isOpen())
          databases[i].drop();
      }

    if (servers != null)
      for (int i = 0; i < getServerCount(); ++i)
        if (getServer(i).existsDatabase(getDatabaseName()))
          getServer(i).getDatabase(getDatabaseName()).drop();

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());

    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + i + "/"));
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/replication"));
  }

  protected void checkDatabasesAreIdentical() {
    final int[] servers2Check = getServerToCheck();

    for (int i = 1; i < servers2Check.length; ++i) {
      final Database db1 = getServerDatabase(servers2Check[0], getDatabaseName());
      final Database db2 = getServerDatabase(servers2Check[i], getDatabaseName());

      LogManager.instance()
          .log(this, Level.INFO, "TEST: Comparing databases '%s' and '%s' are identical...", null, db1.getDatabasePath(), db2.getDatabasePath());
      try {
        new DatabaseComparator().compare(db1, db2);
        LogManager.instance().log(this, Level.INFO, "TEST: OK databases '%s' and '%s' are identical", null, db1.getDatabasePath(), db2.getDatabasePath());
      } catch (RuntimeException e) {
        LogManager.instance()
            .log(this, Level.INFO, "ERROR on comparing databases '%s' and '%s': %s", null, db1.getDatabasePath(), db2.getDatabasePath(), e.getMessage());
        throw e;
      }
    }
  }
}
