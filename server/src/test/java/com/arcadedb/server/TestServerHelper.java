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
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.CallableParameterNoReturn;
import org.junit.jupiter.api.Assertions;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Executes all the tests while the server is up and running.
 */
public abstract class TestServerHelper {

  public static ArcadeDBServer[] startServers(final int totalServers, final CallableParameterNoReturn<ContextConfiguration> onServerConfigurationCallback,
      final CallableParameterNoReturn<ArcadeDBServer> onBeforeStartingCallback) {
    final ArcadeDBServer[] servers = new ArcadeDBServer[totalServers];

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
      config.setValue(GlobalConfiguration.HA_ENABLED, totalServers > 1);

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
      for (ArcadeDBServer server : servers)
        if (server != null)
          server.stop();
    }
  }

  public static ArcadeDBServer getServerByName(final ArcadeDBServer[] servers, final String serverName) {
    for (ArcadeDBServer s : servers) {
      if (s.getServerName().equals(serverName))
        return s;
    }
    return null;
  }

  public static ArcadeDBServer getLeaderServer(final ArcadeDBServer[] servers) {
    for (ArcadeDBServer server : servers)
      if (server.isStarted()) {
        final String leaderName = server.getHA().getLeaderName();
        return getServerByName(servers, leaderName);
      }
    return null;
  }

  public static boolean areAllServersOnline(final ArcadeDBServer[] servers) {
    final ArcadeDBServer leader = getLeaderServer(servers);
    if (leader == null)
      return false;

    final int onlineReplicas = leader.getHA().getOnlineReplicas();
    if (1 + onlineReplicas < servers.length) {
      // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
      leader.getHA().printClusterConfiguration();
      return false;
    }
    return true;
  }

  public static void expectException(final CallableNoReturn callback, final Class<? extends Throwable> expectedException) throws Exception {
    try {
      callback.call();
      Assertions.fail();
    } catch (Throwable e) {
      if (e.getClass().equals(expectedException))
        // EXPECTED
        return;

      if (e instanceof Exception)
        throw (Exception) e;

      throw new Exception(e);
    }
  }
}
