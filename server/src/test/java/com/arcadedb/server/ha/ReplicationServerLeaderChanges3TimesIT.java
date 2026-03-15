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

import com.arcadedb.Constants;
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
import com.arcadedb.utility.Pair;
import org.awaitility.Awaitility;
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
  private final Object                              restartLock        = new Object();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected HAServer.ServerRole getServerRole(int serverIndex) {
    return HAServer.ServerRole.ANY;
  }

  @Override
  @Disabled("Skipping parent's replication() test - we use testReplication() with RemoteDatabase instead")
  public void replication() throws Exception {
    // Parent test uses local database, but this test stops leaders multiple times
    // So we override to disable the parent test and use our own testReplication() method
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @Disabled("Test has fundamental design flaw: attempts to restart leader from within replication callback thread, " +
      "causing deadlock. The callback thread is part of the cluster infrastructure, so stopping/restarting the leader " +
      "from that thread blocks the very infrastructure needed for cluster stabilization (waitForClusterStable). " +
      "Multiple restart attempts visible in logs but all hang waiting for cluster to stabilize. " +
      "Test needs complete redesign: either move restart logic to separate control thread, use external chaos tool " +
      "(Toxiproxy), or simplify to test single restart controlled by main test thread.")
  void testReplication() {
    checkDatabases();

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = HostUtil.parseHostAddress(server1Address, HostUtil.CLIENT_DEFAULT_PORT);

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    try {
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
              assertThat(((Number) result.getProperty("id")).longValue()).isEqualTo(counter);
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
            // Retry with backoff - intentional delay before retrying transaction
            LogManager.instance()
                .log(this, Level.SEVERE, "Error on creating vertex %d, retrying (retry=%d/%d): %s", counter, retry, maxRetry,
                    e.getMessage());
            // Intentional delay for retry backoff (500ms) before next retry attempt
            Awaitility.await("retry backoff delay")
                .pollDelay(500, TimeUnit.MILLISECONDS)
                .atMost(550, TimeUnit.MILLISECONDS)
                .until(() -> true);

          } catch (final DuplicatedKeyException e) {
            // THIS MEANS THE ENTRY WAS INSERTED BEFORE THE CRASH
            LogManager.instance().log(this, Level.SEVERE, "Error: %s (IGNORE IT)", e.getMessage());
          } catch (final Exception e) {
            // IGNORE IT
            LogManager.instance().log(this, Level.SEVERE, "Generic Exception", e);
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
    } finally {
      // Close the remote database connection before test cleanup
      try {
        db.close();
      } catch (final Exception e) {
        // Ignore exceptions during close - servers may already be stopped
        LogManager.instance().log(this, Level.FINE, "Exception closing remote database: %s", null, e.getMessage());
      }
    }
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

          // Log every 500 messages to track progress
          if (messagesInTotal.get() % 500 == 0) {
            LogManager.instance().log(this, Level.SEVERE,
                "TEST: Progress - totalMsgs=%d, msgsThisRestart=%d, restarts=%d/%d, threshold=%d, leader=%s",
                messagesInTotal.get(), messagesPerRestart.get(), restarts.get(), getServerCount(),
                getTxs() / (getServerCount() * 2), leaderName);
          }

          // Check if we should trigger a restart
          if (messagesPerRestart.get() > getTxs() / (getServerCount() * 2) && restarts.get() < getServerCount()) {
            // Re-fetch leader server on each check to get current instance after async restarts
            final ArcadeDBServer leaderServer = getServer(leaderName);

            if (leaderServer == null) {
              LogManager.instance().log(this, Level.FINE, "TEST: Leader '%s' not found in server list, skipping restart check",
                  leaderName);
              return;
            }

            if (!leaderServer.isStarted()) {
              LogManager.instance().log(this, Level.FINE,
                  "TEST: Leader '%s' not started yet, skipping restart check (probably restarting)", leaderName);
              return;
            }

            // Synchronize restart logic to prevent concurrent restarts from multiple callback threads
            synchronized (restartLock) {
              // Re-check condition after acquiring lock (another thread might have just completed a restart)
              if (messagesPerRestart.get() <= getTxs() / (getServerCount() * 2) || restarts.get() >= getServerCount()) {
                return;
              }

              // Re-fetch leader server and check status after acquiring lock
              final ArcadeDBServer currentLeader = getServer(leaderName);
              if (currentLeader == null || !currentLeader.isStarted()) {
                return;
              }

              final int onlineReplicas = currentLeader.getHA().getOnlineReplicas();
              if (onlineReplicas < getServerCount() - 1) {
                // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
                testLog("Skip restart of the Leader %s because not all replicas are online yet (online=%d, need=%d, messages=%d)",
                    leaderName, onlineReplicas, getServerCount() - 1, messagesInTotal.get());
                return;
              }

              testLog("Stopping the Leader %s (messages=%d txs=%d restarts=%d onlineReplicas=%d) ...", leaderName,
                  messagesInTotal.get(), getTxs(), restarts.get(), onlineReplicas);

              // Stop and restart leader synchronously to ensure proper cluster reformation
              // before next restart can be triggered
              currentLeader.stop();

              testLog("Restarting %s synchronously...", leaderName);
              currentLeader.start();

              testLog("Waiting for %s to complete startup...", leaderName);
              HATestHelpers.waitForServerStartup(currentLeader);

              testLog("Waiting for cluster to stabilize after %s restart...", leaderName);
              waitForClusterStable(getServerCount());

              // Update counters after successful restart and stabilization
              restarts.incrementAndGet();
              messagesPerRestart.set(0);

              testLog("Cluster stabilized after %s restart (restarts=%d/%d)", leaderName, restarts.get(), getServerCount());
            }
          }
        }
      }
    });
  }

  @Override
  protected String getServerAddresses() {
    // Override to include server aliases that match SERVER_NAME configuration.
    // This ensures getServer(leaderName) can find servers by their alias.
    // Without aliases, all servers get alias="localhost" which doesn't match "ArcadeDB_N" server names.
    int port = 2424;
    final StringBuilder serverURLs = new StringBuilder();
    for (int i = 0; i < getServerCount(); ++i) {
      if (i > 0)
        serverURLs.append(",");

      serverURLs.append("{").append(Constants.PRODUCT).append("_").append(i).append("}");
      serverURLs.append("localhost:").append(port++);
    }
    return serverURLs.toString();
  }

  @Override
  protected int getTxs() {
    // Increased from 5,000 to allow time for 3 leader restarts with cluster stabilization
    // Each restart + stabilization takes ~2 minutes, so we need more transactions
    return 15_000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}
