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
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class ReplicationServerLeaderDownNoTransactionsToForwardIT extends ReplicationServerIT {
  private final AtomicInteger messages = new AtomicInteger();

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
  void testReplication() {
    checkDatabases();

    final String server2Address = getServer(1).getHttpServer().getListeningAddress();

    final String[] server2AddressParts = HostUtil.parseHostAddress(server2Address, HostUtil.CLIENT_DEFAULT_PORT);

    final RemoteDatabase db = new RemoteDatabase(server2AddressParts[0], Integer.parseInt(server2AddressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    LogManager.instance()
        .log(this, Level.FINE, "Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;

    final int maxRetry = 10;

    for (int tx = 0; tx < getTxs(); ++tx) {
      for (int i = 0; i < getVerticesPerTx(); ++i) {
        for (int retry = 0; retry < maxRetry; ++retry) {
          try {
            final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter,
                "distributed-test");

            assertThat(resultSet.hasNext()).isTrue();
            final Result result = resultSet.next();
            assertThat(result).isNotNull();
            final Set<String> props = result.getPropertyNames();
            assertThat(props.size()).as("Found the following properties " + props).isEqualTo(2);
            assertThat(props.contains("id")).isTrue();
            assertThat(result.<Long>getProperty("id")).isEqualTo(counter);
            assertThat(props.contains("name")).isTrue();
            assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
            break;
          } catch (final RemoteException e) {
            // IGNORE IT
            LogManager.instance()
                .log(this, Level.SEVERE, "Error on creating vertex %d, retrying (retry=%d/%d)...", e, counter, retry, maxRetry);
            CodeUtils.sleep(500);
          }
        }
      }

      if (counter % 1000 == 0) {
        LogManager.instance().log(this, Level.FINE, "- Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }
    }

    LogManager.instance().log(this, Level.FINE, "Done");

    CodeUtils.sleep(1000);

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
    return 1;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}
