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
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestCallback;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ReplicationServerQuorumMajority2ServersOutIT extends ReplicationServerIT {
  private final AtomicInteger messages = new AtomicInteger();

  public ReplicationServerQuorumMajority2ServersOutIT() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_1"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          if (type == TYPE.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 100) {
              LogManager.instance().log(this, Level.FINE, "TEST: Stopping Replica 1...");
              getServer(1).stop();
            }
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          if (type == TYPE.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 200) {
              LogManager.instance().log(this, Level.FINE, "TEST: Stopping Replica 2...");
              getServer(2).stop();
            }
          }
        }
      });
  }

  @Test
  public void testReplication() throws Exception {
    try {
      super.testReplication();
      Assertions.fail("Replication is supposed to fail without enough online servers");
    } catch (QuorumNotReachedException e) {
      // CATCH IT
    }
  }

  protected int[] getServerToCheck() {
    return new int[] {};
  }

  protected void checkEntriesOnServer(final int s) {
    final Database db = getServerDatabase(s, getDatabaseName());
    db.begin();
    try {
      Assertions.assertTrue(1 + getTxs() * getVerticesPerTx() > db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail("Error on checking on server" + s);
    }
  }

  @Override
  protected int getTxs() {
    return 500;
  }

}
