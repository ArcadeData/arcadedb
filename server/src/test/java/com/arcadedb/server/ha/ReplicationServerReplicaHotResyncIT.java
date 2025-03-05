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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class ReplicationServerReplicaHotResyncIT extends ReplicationServerIT {
  private final    CountDownLatch hotResyncLatch  = new CountDownLatch(1);
  private final    CountDownLatch fullResyncLatch = new CountDownLatch(1);
  private final    AtomicLong     totalMessages   = new AtomicLong();
  private volatile boolean        slowDown        = true;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(10);
  }

  @Override
  protected void onAfterTest() {

    Awaitility.await().atMost(10, TimeUnit.MINUTES)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          // Wait for the hot resync event to be received

          return hotResyncLatch.getCount() == 0;
        });

    Awaitility.await().atMost(10, TimeUnit.MINUTES)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          // Wait for the full resync event to be received
          return fullResyncLatch.getCount() == 0;
        });
//    try {
//      // Wait for hot resync event with timeout
//      boolean hotResyncReceived = hotResyncLatch.await(30, TimeUnit.SECONDS);
//      // Wait for full resync event with timeout
//      boolean fullResyncReceived = fullResyncLatch.await(1, TimeUnit.SECONDS);
//
//      assertThat(hotResyncReceived).as("Hot resync event should have been received").isTrue();
//      assertThat(fullResyncReceived).as("Full resync event should not have been received").isFalse();
//    } catch (InterruptedException e) {
//      Thread.currentThread().interrupt();
//      fail("Test was interrupted while waiting for resync events");
//    }
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
            if (totalMessages.incrementAndGet() > 5 && totalMessages.get() < 10) {
              LogManager.instance()
                  .log(this, Level.INFO, "TEST: Slowing down response from replica server 2... - total messages %d",
                      totalMessages.get());
              try {
                // Still need some delay to trigger the hot resync
                Thread.sleep(1_000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          } else {
            LogManager.instance().log(this, Level.INFO, "TEST: Slowdown is disabled");
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

    if (server.getServerName().equals("ArcadeDB_0")) {
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (!serversSynchronized)
            return;

          if ("ArcadeDB_2".equals(object) && type == Type.REPLICA_OFFLINE) {
            LogManager.instance().log(this, Level.INFO, "TEST: Replica 2 is offline removing latency...");
            slowDown = false;
          }
        }
      });
    }
  }
}
