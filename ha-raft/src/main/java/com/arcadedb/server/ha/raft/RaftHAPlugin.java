/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;

import java.io.IOException;
import java.util.logging.Level;

/**
 * ServerPlugin implementation that bootstraps the Raft-based HA subsystem.
 * Discovered via Java ServiceLoader when {@code HA_ENABLED=true} and
 * {@code HA_IMPLEMENTATION=raft}.
 */
public class RaftHAPlugin implements ServerPlugin {

  private ArcadeDBServer       server;
  private ContextConfiguration configuration;
  private RaftHAServer         raftHAServer;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    if (!isRaftEnabled()) {
      LogManager.instance().log(this, Level.FINE, "Raft HA plugin not activated (HA_IMPLEMENTATION != raft or HA not enabled)");
      return;
    }

    validateConfiguration();

    try {
      raftHAServer = new RaftHAServer(server, configuration);
      raftHAServer.start();

      LogManager.instance().log(this, Level.INFO, "Raft HA plugin started successfully");
    } catch (final IOException e) {
      throw new RuntimeException("Failed to start Raft HA server", e);
    }
  }

  @Override
  public void stopService() {
    if (raftHAServer != null) {
      raftHAServer.stop();
      raftHAServer = null;
    }
  }

  public RaftHAServer getRaftHAServer() {
    return raftHAServer;
  }

  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  private boolean isRaftEnabled() {
    return configuration != null
        && configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)
        && "raft".equalsIgnoreCase(configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION));
  }

  private void validateConfiguration() {
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    if (serverList == null || serverList.isEmpty())
      throw new RuntimeException("HA_SERVER_LIST must be configured for Raft HA");

    final String quorum = configuration.getValueAsString(GlobalConfiguration.HA_QUORUM).toUpperCase();
    final int serverCount = serverList.split(",").length;

    if ("MAJORITY".equals(quorum) && serverCount == 2)
      LogManager.instance().log(this, Level.WARNING,
          "HA_QUORUM=MAJORITY with 2 nodes: losing 1 node will prevent writes. Consider NONE for 2-node setups.");

    if ("NONE".equals(quorum) && serverCount > 2)
      LogManager.instance().log(this, Level.WARNING,
          "HA_QUORUM=NONE with %d nodes: replication is asynchronous, data loss possible on leader failure.", serverCount);

    if (!"NONE".equals(quorum) && !"MAJORITY".equals(quorum))
      LogManager.instance().log(this, Level.WARNING,
          "Raft HA only supports NONE and MAJORITY quorum modes. '%s' is not supported, defaulting to MAJORITY.", quorum);
  }
}
