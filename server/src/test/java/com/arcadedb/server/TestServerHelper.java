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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.CallableParameterNoReturn;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.util.Collection;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Executes all the tests while the server is up and running.
 */
public abstract class TestServerHelper {

  public static ArcadeDBServer[] startServers(final int totalServers,
      final CallableParameterNoReturn<ContextConfiguration> onServerConfigurationCallback,
      final CallableParameterNoReturn<ArcadeDBServer> onBeforeStartingCallback) {
    final ArcadeDBServer[] servers = new ArcadeDBServer[totalServers];

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
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_ENABLED, totalServers > 1);
      //config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, 2000);

      if (onServerConfigurationCallback != null)
        onServerConfigurationCallback.call(config);

      servers[i] = new ArcadeDBServer(config);

      if (onBeforeStartingCallback != null)
        onBeforeStartingCallback.call(servers[i]);

      servers[i].start();
    }

    return servers;
  }

  public static void stopServers(final ArcadeDBServer[] servers) {
    if (servers != null) {
      for (final ArcadeDBServer server : servers)
        if (server != null)
          server.stop();
    }
  }

  public static ArcadeDBServer getServerByName(final ArcadeDBServer[] servers, final String serverName) {
    for (final ArcadeDBServer s : servers) {
      if (s.getServerName().equals(serverName))
        return s;
    }
    return null;
  }

  public static ArcadeDBServer getLeaderServer(final ArcadeDBServer[] servers) {
    for (final ArcadeDBServer server : servers)
      if (server.isStarted()) {
        final String leaderName = server.getHA().getLeaderName();
        return getServerByName(servers, leaderName);
      }
    return null;
  }

  public static void expectException(final CallableNoReturn callback, final Class<? extends Throwable> expectedException)
      throws Exception {
    try {
      callback.call();
      fail("");
    } catch (final Throwable e) {
      if (e.getClass().equals(expectedException))
        // EXPECTED
        return;

      if (e instanceof Exception exception)
        throw exception;

      throw new Exception(e);
    }
  }

  public static void checkActiveDatabases() {
    checkActiveDatabases(true);
  }

  public static void checkActiveDatabases(final boolean drop) {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();

    if (!activeDatabases.isEmpty())
      LogManager.instance()
          .log(TestServerHelper.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced closing...");

    for (final Database db : activeDatabases)
      if (drop) {
        if (db.isTransactionActive())
          db.commit();

        ((DatabaseInternal) db).getEmbedded().drop();
      } else
        db.close();

    assertThat(activeDatabases.isEmpty()).as("Found active databases: " + activeDatabases).isTrue();
  }

  public static void deleteDatabaseFolders(final int totalServers) {
    FileUtils.deleteRecursively(new File("./target/databases/"));
    FileUtils.deleteRecursively(new File("./target/config/"));
    FileUtils.deleteRecursively(new File("./target/backups/"));
    FileUtils.deleteRecursively(new File("./target/log/"));
    FileUtils.deleteRecursively(new File("./target/replication/"));
    String databaseDirectoryValueAsString = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString();
    FileUtils.deleteRecursively(new File(databaseDirectoryValueAsString + File.separator));
    for (int i = 0; i < totalServers; ++i) {
      LogManager.instance().log("TestServerHelper", Level.INFO, "Deleting:: %s ", databaseDirectoryValueAsString + i + File.separator);

      FileUtils.deleteRecursively(new File(databaseDirectoryValueAsString + i + File.separator));
    }
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + File.separator + "replication"));
  }
}
