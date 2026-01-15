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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

@Tag("ha")
public class ReplicationServerReplicaHotResyncIT extends ReplicationServerIT {
  private final    CountDownLatch hotResyncLatch    = new CountDownLatch(1);
  private final    CountDownLatch fullResyncLatch   = new CountDownLatch(1);
  private final    AtomicLong     totalMessages     = new AtomicLong();
  private volatile boolean        slowDown          = true;
  private volatile boolean        reconnectTriggered = false;

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @Override
  public void replication() throws Exception {
    super.replication();
  }

  @Override
  protected int getTxs() {
    // Use 10 transactions to test hot resync with moderate load
    return 10;
  }

  @Override
  protected int getMaxRetry() {
    // Increase retries to 100 to allow time for server 2 to reconnect
    // During reconnection (which takes a few seconds), transactions will retry
    // Once reconnection completes, transactions will succeed
    return 100;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(10);
  }

  @Override
  protected void onAfterTest() {
    // Verify hot resync was triggered
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> hotResyncLatch.getCount() == 0);

    // Verify full resync was NOT triggered (count should still be 1)
    if (fullResyncLatch.getCount() == 0) {
      throw new AssertionError("Full resync event was received but only hot resync was expected");
    }

    LogManager.instance().log(this, Level.INFO, "TEST: Hot resync verified successfully");
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2")) {
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (!serversSynchronized)
            return;

          if (slowDown) {
            // SLOW DOWN A SERVER AFTER 5TH MESSAGE
            final long msgCount = totalMessages.incrementAndGet();
            if (msgCount > 5 && msgCount < 10) {
              LogManager.instance()
                  .log(this, Level.INFO, "TEST: Slowing down response from replica server 2... - total messages %d",
                      msgCount);
              try {
                // Still need some delay to trigger the hot resync
                Thread.sleep(1_000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }

            // After slowdown, trigger reconnection to test hot resync
            if (msgCount == 10 && !reconnectTriggered) {
              reconnectTriggered = true;
              LogManager.instance().log(this, Level.INFO, "TEST: Triggering disconnect for hot resync test...");
              slowDown = false;

              executeAsynchronously(() -> {
                try {
                  // Wait a bit for current message to finish processing
                  Thread.sleep(1000);

                  final ArcadeDBServer server2 = getServer(2);
                  if (server2 != null && server2.getHA() != null && server2.getHA().getLeader() != null) {
                    LogManager.instance().log(this, Level.INFO, "TEST: Closing connection to trigger reconnection...");

                    // Close the channel - this will cause the next message receive to fail
                    // and trigger automatic reconnection via the reconnect() method
                    server2.getHA().getLeader().closeChannel();

                    LogManager.instance().log(this, Level.INFO, "TEST: Channel closed, waiting for automatic reconnection...");

                    // Wait for server 2 to reconnect to the leader before continuing
                    // This prevents race condition where transactions commit while server 2 is offline
                    Awaitility.await("server 2 reconnection")
                        .atMost(30, TimeUnit.SECONDS)
                        .pollInterval(500, TimeUnit.MILLISECONDS)
                        .until(() -> {
                          final ArcadeDBServer leader = getServer(0);
                          if (leader == null || leader.getHA() == null)
                            return false;
                          final Leader2ReplicaNetworkExecutor replica = leader.getHA().getReplica("ArcadeDB_2");
                          return replica != null && replica.getStatus() == Leader2ReplicaNetworkExecutor.STATUS.ONLINE;
                        });

                    LogManager.instance().log(this, Level.INFO, "TEST: Server 2 reconnected successfully");
                  }
                } catch (Exception e) {
                  LogManager.instance().log(this, Level.WARNING, "TEST: Failed to close channel: %s", e.getMessage());
                }
                return null;
              });
            }
          } else {
            // Handle hot/full resync events
            if (type == Type.REPLICA_HOT_RESYNC) {
              hotResyncLatch.countDown();
              LogManager.instance().log(this, Level.INFO, "TEST: Received hot resync request %s", hotResyncLatch.getCount());
            } else if (type == Type.REPLICA_FULL_RESYNC) {
              fullResyncLatch.countDown();
              LogManager.instance().log(this, Level.INFO, "TEST: Received full resync request %s", fullResyncLatch.getCount());
            }
          }
        }
      });
    }

  }
}
