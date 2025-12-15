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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ReplicationCallback;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ReplicationServerLeaderDownIT extends ReplicationServerIT {
  private final AtomicInteger messages = new AtomicInteger();

  public ReplicationServerLeaderDownIT() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected HAServer.ServerRole getServerRole(int serverIndex) {
    return HAServer.ServerRole.ANY;
  }

  @Test
  @Disabled
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  void testReplication() {
    checkDatabases();

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();

    final String[] server1AddressParts = HostUtil.parseHostAddress(server1Address, HostUtil.CLIENT_DEFAULT_PORT);
    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    LogManager.instance()
        .log(this, Level.FINE, "Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      for (int i = 0; i < getVerticesPerTx(); ++i) {
        final long currentId = ++counter;

        // Use Awaitility to handle retry logic with proper timeout
        await().atMost(Duration.ofSeconds(15))
               .pollInterval(Duration.ofMillis(500))
               .ignoreException(RemoteException.class)
               .untilAsserted(() -> {
                 final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?",
                     currentId, "distributed-test");

                 assertThat(resultSet.hasNext()).isTrue();
                 final Result result = resultSet.next();
                 assertThat(result).isNotNull();
                 final Set<String> props = result.getPropertyNames();
                 assertThat(props.size()).as("Found the following properties " + props).isEqualTo(2);
                 assertThat(result.<Long>getProperty("id")).isEqualTo(currentId);
                 assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
               });
      }

      if (counter % 1000 == 0) {
        LogManager.instance().log(this, Level.FINE, "- Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }
    }

    LogManager.instance().log(this, Level.FINE, "Done");

    // Wait for replication to complete instead of fixed sleep
    for (final int s : getServerToCheck())
      waitForReplicationIsCompleted(s);

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (final int s : getServerToCheck())
      checkEntriesOnServer(s);

    onAfterTest();
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener((type, object, server1) -> {
        if (type == ReplicationCallback.Type.REPLICA_MSG_RECEIVED) {
          if (messages.incrementAndGet() > 10 && getServer(0).isStarted()) {
            testLog("TEST: Stopping the Leader...");

            executeAsynchronously(() -> {
              getServer(0).stop();
              return null;
            });
          }
        }
      });
  }

  protected int[] getServerToCheck() {
    return new int[] { 1, 2 };
  }

  @Override
  protected int getTxs() {
    return 1000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}
