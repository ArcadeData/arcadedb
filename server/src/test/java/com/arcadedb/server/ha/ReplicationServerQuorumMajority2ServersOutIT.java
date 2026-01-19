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
import com.arcadedb.server.ReplicationCallback;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@Tag("ha")
public class ReplicationServerQuorumMajority2ServersOutIT extends ReplicationServerIT {
  private final AtomicInteger messages = new AtomicInteger();

  public ReplicationServerQuorumMajority2ServersOutIT() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_1"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (type == Type.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 100) {
              LogManager.instance().log(this, Level.FINE, "TEST: Stopping Replica 1...");
              getServer(1).stop();
            }
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (type == Type.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 200) {
              LogManager.instance().log(this, Level.FINE, "TEST: Stopping Replica 2...");
              getServer(2).stop();
            }
          }
        }
      });
  }

  @Override
  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  public void replication() throws Exception {
    assertThatThrownBy(super::replication)
        .isInstanceOf(QuorumNotReachedException.class);
  }

  protected int[] getServerToCheck() {
    return new int[] {};
  }

  protected void checkEntriesOnServer(final int server) {
    final Database db = getServerDatabase(server, getDatabaseName());
    db.begin();
    try {
      assertThat(1 + (long) getTxs() * getVerticesPerTx() > db.countType(VERTEX1_TYPE_NAME, true))
          .as("Check for vertex count for server" + server)
          .isTrue();

    } catch (final Exception e) {
      fail("Error on checking on server" + server, e);
    }
  }

  @Override
  protected int getTxs() {
    return 500;
  }

}
