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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ReplicationCallback;
import com.arcadedb.server.ha.message.TxRequest;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("ha")
public class ReplicationServerLeaderChanges3TimesIT extends ReplicationServerIT {
  private final AtomicInteger                       messagesInTotal    = new AtomicInteger();
  private final AtomicInteger                       messagesPerRestart = new AtomicInteger();
  private final AtomicInteger                       restarts           = new AtomicInteger();
  private final ConcurrentHashMap<Integer, Boolean> semaphore          = new ConcurrentHashMap<>();

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
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
//  @Disabled
  void testReplication() {
    checkDatabases();

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = HostUtil.parseHostAddress(server1Address, HostUtil.CLIENT_DEFAULT_PORT);

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    LogManager.instance()
        .log(this, Level.FINE, "Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;
    final int maxRetry = 10;
    int timeouts = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      for (int retry = 0; retry < 3; ++retry) {
        try {
          for (int i = 0; i < getVerticesPerTx(); ++i) {
            final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter,
                "distributed-test");

            assertThat(resultSet.hasNext()).isTrue();
            final Result result = resultSet.next();
            assertThat(result).isNotNull();
            final Set<String> props = result.getPropertyNames();
            assertThat(props.size()).as("Found the following properties " + props).isEqualTo(2);
            assertThat(props.contains("id")).isTrue();
            assertThat((int) result.getProperty("id")).isEqualTo(counter);
            assertThat(props.contains("name")).isTrue();
            assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");

            if (counter % 100 == 0) {
              LogManager.instance().log(this, Level.SEVERE, "- Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));
              if (isPrintingConfigurationAtEveryStep())
                getLeaderServer().getHA().printClusterConfiguration();
            }

          }
          break;

        } catch (final NeedRetryException | TimeoutException | TransactionException e) {
          if (e instanceof TimeoutException) {
            if (++timeouts > 3)
              throw e;
          }
          // IGNORE IT
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on creating vertex %d, retrying (retry=%d/%d): %s", counter, retry, maxRetry,
                  e.getMessage());
          CodeUtils.sleep(500);

        } catch (final DuplicatedKeyException e) {
          // THIS MEANS THE ENTRY WAS INSERTED BEFORE THE CRASH
          LogManager.instance().log(this, Level.SEVERE, "Error: %s (IGNORE IT)", e.getMessage());
        } catch (final Exception e) {
          // IGNORE IT
          LogManager.instance().log(this, Level.SEVERE, "Generic Exception: %s", e.getMessage());
        }
      }
    }

    LogManager.instance().log(this, Level.SEVERE, "Done");

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (final int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();

    LogManager.instance().log(this, Level.FINE, "TEST Restart = %d", null, restarts);
    assertThat(restarts.get() >= getServerCount()).as("Restarted " + restarts.get() + " times").isTrue();
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    server.registerTestEventListener(new ReplicationCallback() {
      @Override
      public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
        if (!serversSynchronized)
          return;

        if (type == Type.REPLICA_MSG_RECEIVED) {
          if (!(((Pair) object).getSecond() instanceof TxRequest))
            return;

          final String leaderName = server.getHA().getLeaderName();

          messagesInTotal.incrementAndGet();
          messagesPerRestart.incrementAndGet();

          if (getServer(leaderName).isStarted() && messagesPerRestart.get() > getTxs() / (getServerCount() * 2)
              && restarts.get() < getServerCount()) {
            LogManager.instance()
                .log(this, Level.FINE, "TEST: Found online replicas %d", null, getServer(leaderName).getHA().getOnlineReplicas());

            if (getServer(leaderName).getHA().getOnlineReplicas() < getServerCount() - 1) {
              // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
              LogManager.instance().log(this, Level.FINE,
                  "TEST: Skip restart of the Leader %s because no all replicas are online yet (messages=%d txs=%d) ...", null,
                  leaderName, messagesInTotal.get(), getTxs());
              return;
            }

            if (semaphore.putIfAbsent(restarts.get(), true) != null)
              // ANOTHER REPLICA JUST DID IT
              return;

            testLog("Stopping the Leader %s (messages=%d txs=%d restarts=%d) ...", leaderName, messagesInTotal.get(), getTxs(),
                restarts.get());

            getServer(leaderName).stop();
            restarts.incrementAndGet();
            messagesPerRestart.set(0);

            executeAsynchronously(() -> {
              getServer(leaderName).start();
              return null;
            });
          }
        }
      }
    });
  }

  @Override
  protected int getTxs() {
    return 5_000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}
