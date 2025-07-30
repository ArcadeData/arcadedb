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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import org.junit.jupiter.api.AfterEach;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulates a split brain on 5 nodes, by isolating nodes 4th and 5th in a separate network. After 10 seconds, allows the 2 networks to see
 * each other and hoping for a rejoin in only one network where the leader is still the original one.
 */
public class HASplitBrainIT extends ReplicationServerIT {
  private final    Timer      timer     = new Timer();
  private final    AtomicLong messages  = new AtomicLong();
  private volatile boolean    split     = false;
  private volatile boolean    rejoining = false;
  private          String     firstLeader;

  public HASplitBrainIT() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.reset();
  }

  @Override
  protected void onAfterTest() {
    timer.cancel();
    assertThat(getLeaderServer().getServerName()).isEqualTo(firstLeader);
  }

  @Override
  protected HAServer.ServerRole getServerRole(int serverIndex) {
    return HAServer.ServerRole.ANY;
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    server.registerTestEventListener(new ReplicationCallback() {
      @Override
      public void onEvent(final Type type, final Object object, final ArcadeDBServer server) throws IOException {
        if (type == Type.LEADER_ELECTED) {
          if (firstLeader == null)
            firstLeader = (String) object;
        } else if (type == Type.NETWORK_CONNECTION && split) {
          final String connectTo = (String) object;

          final String[] parts = HostUtil.parseHostAddress(connectTo, HostUtil.HA_DEFAULT_PORT);
          final int connectToPort = Integer.parseInt(parts[1]);

          if (server.getServerName().equals("ArcadeDB_3") || server.getServerName().equals("ArcadeDB_4")) {
            // SERVERS 3-4
            if (connectToPort == 2424 || connectToPort == 2425 || connectToPort == 2426) {
              if (!rejoining) {
                testLog("SIMULATING CONNECTION ERROR TO CONNECT TO THE LEADER FROM " + server);
                throw new IOException(
                    "Simulating an IO Exception on reconnecting from server '" + server.getServerName() + "' to " + connectTo);
              } else
                testLog("AFTER REJOINING -> ALLOWED CONNECTION TO THE ADDRESS " + connectTo + "  FROM " + server);
            } else
              LogManager.instance()
                  .log(this, Level.FINE, "ALLOWED CONNECTION FROM SERVER %s TO %s...", null, server.getServerName(), connectTo);
          } else {
            // SERVERS 0-1-2
            if (connectToPort == 2427 || connectToPort == 2428) {
              if (!rejoining) {
                testLog("SIMULATING CONNECTION ERROR TO SERVERS " + connectTo + " FROM " + server);
                throw new IOException(
                    "Simulating an IO Exception on reconnecting from server '" + server.getServerName() + "' to " + connectTo);
              } else
                testLog("AFTER REJOINING -> ALLOWED CONNECTION TO THE ADDRESS " + connectTo + "  FROM " + server);
            } else
              LogManager.instance()
                  .log(this, Level.FINE, "ALLOWED CONNECTION FROM SERVER %s TO %s...", null, server.getServerName(), connectTo);
          }
        }
      }
    });

    if (server.getServerName().equals("ArcadeDB_4"))
      server.registerTestEventListener((type, object, server1) -> {
        if (!split) {
          if (type == ReplicationCallback.Type.REPLICA_MSG_RECEIVED) {
            messages.incrementAndGet();
            if (messages.get() > 10) {

              final Leader2ReplicaNetworkExecutor replica3 = getServer(0).getHA().getReplica("ArcadeDB_3");
              final Leader2ReplicaNetworkExecutor replica4 = getServer(0).getHA().getReplica("ArcadeDB_4");

              if (replica3 == null || replica4 == null) {
                testLog("REPLICA 4 and 5 NOT STARTED YET");
                return;
              }

              split = true;

              testLog("SHUTTING DOWN NETWORK CONNECTION BETWEEN SERVER 0 (THE LEADER) and SERVER 4TH and 5TH...");
              getServer(3).getHA().getLeader().closeChannel();
              replica3.closeChannel();

              getServer(4).getHA().getLeader().closeChannel();
              replica4.closeChannel();
              testLog("SHUTTING DOWN NETWORK CONNECTION COMPLETED");

              timer.schedule(new TimerTask() {
                @Override
                public void run() {
                  testLog("ALLOWING THE REJOINING OF SERVERS 4TH AND 5TH");
                  rejoining = true;
                }
              }, 10000);
            }
          }
        }
      });
  }

  @Override
  protected int getServerCount() {
    return 5;
  }

  @Override
  protected boolean isPrintingConfigurationAtEveryStep() {
    return true;
  }

  @Override
  protected int getTxs() {
    return 3000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}
