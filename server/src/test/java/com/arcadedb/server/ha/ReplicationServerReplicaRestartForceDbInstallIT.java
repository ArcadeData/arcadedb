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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 15, unit = TimeUnit.MINUTES)
@Tag("ha")
public class ReplicationServerReplicaRestartForceDbInstallIT extends ReplicationServerIT {
  private final    AtomicLong totalMessages           = new AtomicLong();
  private volatile boolean    firstTimeServerShutdown = true;
  private volatile boolean    slowDown                = true;
  private          boolean    hotResync               = false;
  private          boolean    fullResync              = false;

  public ReplicationServerReplicaRestartForceDbInstallIT() {
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(10);
  }

  @Override
  protected void onAfterTest() {
    assertThat(hotResync).isFalse();
    assertThat(fullResync).isTrue();
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (!serversSynchronized)
            return;

          if (slowDown) {
            // SLOW DOWN A SERVER AFTER 5TH MESSAGE - intentionally inject latency to fill replication queue
            if (totalMessages.incrementAndGet() > 5) {
              LogManager.instance().log(this, getErrorLevel(), "TEST: Slowing down response from replica server 2...");
              // Intentional 10s delay to simulate slow replica and force replication queue overflow
              Awaitility.await("intentional latency injection")
                  .pollDelay(10, TimeUnit.SECONDS)
                  .atMost(11, TimeUnit.SECONDS)
                  .until(() -> true);
            }
          } else {

            if (type == Type.REPLICA_HOT_RESYNC) {
              LogManager.instance().log(this, getErrorLevel(), "TEST: Received hot resync request");
              hotResync = true;
            } else if (type == Type.REPLICA_FULL_RESYNC) {
              LogManager.instance().log(this, getErrorLevel(), "TEST: Received full resync request");
              fullResync = true;
            }
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_0"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (!serversSynchronized)
            return;

          // AS SOON AS SERVER 2 IS OFFLINE, A CLEAN OF REPLICATION LOG AND RESTART IS EXECUTED
          if (object instanceof String serverName &&
              "ArcadeDB_2".equals(serverName) &&
              type == Type.REPLICA_OFFLINE && firstTimeServerShutdown) {
            LogManager.instance().log(this, Level.SEVERE,
                "TEST: Stopping Replica 2, removing latency, delete the replication log file and restart the server...");
            slowDown = false;
            firstTimeServerShutdown = false;

            executeAsynchronously(() -> {
              getServer(2).stop();
              GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.reset();

              assertThat(new File("./target/replication/replication_ArcadeDB_2.rlog.0").exists()).isTrue();
              new File("./target/replication/replication_ArcadeDB_2.rlog.0").delete();

              LogManager.instance().log(this, Level.SEVERE, "TEST: Restarting Replica 2...");

              getServer(2).start();
              return null;
            });
          }
        }
      });
  }
}
