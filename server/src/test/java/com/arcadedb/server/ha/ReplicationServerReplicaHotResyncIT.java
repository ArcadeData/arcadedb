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
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class ReplicationServerReplicaHotResyncIT extends ReplicationServerIT {
  private final    AtomicLong totalMessages = new AtomicLong();
  private volatile boolean    slowDown      = true;
  private          boolean    hotResync     = false;
  private          boolean    fullResync    = false;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(10);
  }

  @Override
  protected void onAfterTest() {
    Assertions.assertTrue(hotResync);
    Assertions.assertFalse(fullResync);
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          if( !serversSynchronized)
            return;

          if (slowDown) {
            // SLOW DOWN A SERVER AFTER 5TH MESSAGE
            if (totalMessages.incrementAndGet() > 5) {
              try {
                LogManager.instance().log(this, Level.FINE, "TEST: Slowing down response from replica server 2...");
                Thread.sleep(10000);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                // IGNORE IT
              }
            }
          } else {
            if (type == TYPE.REPLICA_HOT_RESYNC) {
              LogManager.instance().log(this, Level.FINE, "TEST: Received hot resync request");
              hotResync = true;
            } else if (type == TYPE.REPLICA_FULL_RESYNC) {
              LogManager.instance().log(this, Level.FINE, "TEST: Received full resync request");
              fullResync = true;
            }
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_0"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          // SLOW DOWN A SERVER
          if( !serversSynchronized)
            return;

          if ("ArcadeDB_2".equals(object) && type == TYPE.REPLICA_OFFLINE) {
            LogManager.instance().log(this, Level.FINE, "TEST: Replica 2 is offline removing latency...");
            slowDown = false;
          }
        }
      });
  }
}
