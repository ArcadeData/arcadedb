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

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for High Availability random crash scenarios (chaos engineering).
 *
 * <p>This test simulates random server crashes during continuous operation to verify cluster
 * resilience and automatic recovery. Uses bounded randomness and exponential backoff to ensure
 * reliable testing without infinite loops or test hangs.
 *
 * <p><b>Test Strategy:</b>
 * <ul>
 *   <li>Continuously inserts vertices while randomly crashing servers
 *   <li>Each server crash triggers immediate restart with validation
 *   <li>Tests continue until all servers have been restarted at least once
 *   <li>Validates cluster consistency at end of test
 * </ul>
 *
 * <p><b>Key Patterns Demonstrated:</b>
 * <ul>
 *   <li><b>Bounded Waits:</b> Awaitility with explicit timeouts (no infinite loops)
 *   <li><b>Exponential Backoff:</b> Adaptive delays: min(1000 * (retry + 1), 5000) ms
 *   <li><b>Daemon Timer:</b> Timer thread runs as daemon to prevent JVM hangs
 *   <li><b>Restart Verification:</b> Server status checked before operations
 *   <li><b>Resource Management:</b> ResultSet wrapped in try-with-resources
 * </ul>
 *
 * <p><b>Timeout Rationale:</b>
 * <ul>
 *   <li>Server shutdown: {@link HATestTimeouts#SERVER_SHUTDOWN_TIMEOUT} - Graceful shutdown with cleanup
 *   <li>Server startup: {@link HATestTimeouts#SERVER_STARTUP_TIMEOUT} - HA cluster joining and sync
 *   <li>Replica reconnection: {@link HATestTimeouts#REPLICA_RECONNECTION_TIMEOUT} - Network availability detection
 *   <li>Transaction execution: {@link HATestTimeouts#CHAOS_TRANSACTION_TIMEOUT} - Extended for recovery
 * </ul>
 *
 * <p><b>Expected Behavior:</b>
 * <ul>
 *   <li>All {link getTxs()} transactions eventually succeed
 *   <li>No data loss despite random crashes
 *   <li>Cluster automatically recovers without manual intervention
 *   <li>No test hangs or timeouts under normal conditions
 * </ul>
 *
 * @see HATestTimeouts for timeout rationale
 * @see ReplicationServerIT for base replication test functionality
 */
public class HARandomCrashIT extends ReplicationServerIT {
  private          int   restarts                = 0;
  private volatile long  delay                   = 0;
  private          Timer timer                   = null;
  private volatile int   consecutiveFailures     = 0;  // Track failures for exponential backoff

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
  }

  @Override
  protected HAServer.ServerRole getServerRole(int serverIndex) {
    return HAServer.ServerRole.ANY;
  }

  @Test
  @Override
  @Timeout(value = 20, unit = TimeUnit.MINUTES)
  public void replication() {
    checkDatabases();

    timer = new Timer("HARandomCrashIT-Timer", true);  // daemon=true to prevent JVM hangs
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        if (!areAllReplicasAreConnected())
          return;

        final int serverId = ThreadLocalRandom.current().nextInt(getServerCount());

        // Validate that the selected server is actually running before attempting operations
        if (getServer(serverId).getStatus() != ArcadeDBServer.Status.ONLINE) {
          LogManager.instance().log(this, getLogLevel(), "TEST: Skip stop of server %d because it's not ONLINE (status=%s)", null,
              serverId, getServer(serverId).getStatus());
          return;
        }

        if (restarts >= getServerCount()) {
          delay = 0;
          return;
        }

        for (int i = 0; i < getServerCount(); ++i)
          if (getServer(i).isStarted()) {
            final Database db = getServer(i).getDatabase(getDatabaseName());
            db.begin();
            try {
              final long count = db.countType(VERTEX1_TYPE_NAME, true);
              if (count > ((long) getTxs() * getVerticesPerTx()) * 9 / 10) {
                LogManager.instance()
                    .log(this, getLogLevel(), "TEST: Skip stop of server because it's close to the end of the test (%d/%d)", null,
                        count, getTxs() * getVerticesPerTx());
                return;
              }
            } catch (final Exception e) {
              // GENERIC ERROR, SKIP STOP
              LogManager.instance().log(this, Level.SEVERE, "TEST: Skip stop of server for generic error", e);
              continue;
            } finally {
              db.rollback();
            }

            // Exponential backoff for client operations based on consecutive failures
            delay = Math.min(1_000 * (consecutiveFailures + 1), 5_000);
            LogManager.instance().log(this, getLogLevel(), "TEST: Stopping the Server %s (delay=%d, failures=%d)...", null, serverId, delay, consecutiveFailures);

            getServer(serverId).stop();

            // Wait for server to finish shutting down using Awaitility with extended timeout
            await("server shutdown")
                .atMost(HATestTimeouts.SERVER_SHUTDOWN_TIMEOUT)
                .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL_LONG)
                .until(() -> getServer(serverId).getStatus() != ArcadeDBServer.Status.SHUTTING_DOWN);

            LogManager.instance().log(this, getLogLevel(), "TEST: Restarting the Server %s (delay=%d)...", null, serverId, delay);

            restarts++;

            for (int restartRetry = 0; restartRetry < 3; restartRetry++) {
              try {
                getServer(serverId).start();
                break;
              } catch (Throwable e) {
                LogManager.instance()
                    .log(this, getLogLevel(), "TEST: Error on restarting the server %s, retrying (%d/%d)", e, restartRetry + 1, 3);
              }
            }

            LogManager.instance().log(this, getLogLevel(), "TEST: Server %s restarted (delay=%d)...", null, serverId, delay);

            // Wait for cluster to be fully connected after restart
            // This prevents cascading failures where multiple servers are offline simultaneously
            try {
              await("cluster reconnected after restart").atMost(HATestTimeouts.REPLICA_RECONNECTION_TIMEOUT)
                     .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL_LONG)
                     .until(() -> {
                       try {
                         // Check if all replicas are connected (includes the restarted server)
                         return areAllReplicasAreConnected();
                       } catch (Exception e) {
                         return false;
                       }
                     });
              LogManager.instance().log(this, getLogLevel(), "TEST: Cluster fully connected after server %d restart", null, serverId);
            } catch (Exception e) {
              LogManager.instance()
                  .log(this, Level.WARNING, "TEST: Timeout waiting for cluster to reconnect after server %d restart", e, serverId);
            }

            new Timer("HARandomCrashIT-DelayReset", true).schedule(new TimerTask() {
              @Override
              public void run() {
                delay = 0;
                consecutiveFailures = 0;  // Reset failures when delay expires
                LogManager.instance().log(this, getLogLevel(), "TEST: Resetting delay and failures (delay=%d, failures=%d)...", null, delay, consecutiveFailures);
              }
            }, 10_000);

            return;
          }

        LogManager.instance().log(this, getLogLevel(), "TEST: Cannot restart server because unable to count vertices");

      }
    }, 15_000, 10_000);

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = HostUtil.parseHostAddress(server1Address, HostUtil.CLIENT_DEFAULT_PORT);

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    LogManager.instance()
        .log(this, getLogLevel(), "TEST: Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      final long lastGoodCounter = counter;

      try {
        // Use Awaitility to handle retry logic with adaptive delay based on failures
        long adaptiveDelay = Math.min(100 + (consecutiveFailures * 50), 500);  // Adaptive delay for polling
        await("transaction execution during chaos").atMost(HATestTimeouts.CHAOS_TRANSACTION_TIMEOUT)
               .pollInterval(Duration.ofSeconds(1))
               .pollDelay(Duration.ofMillis(adaptiveDelay))
               .ignoreExceptionsMatching(e ->
                   e instanceof TransactionException ||
                   e instanceof NeedRetryException ||
                   e instanceof RemoteException ||
                   e instanceof TimeoutException ||
                   e instanceof QuorumNotReachedException ||
                   e instanceof AssertionError)  // Include AssertionError from assertions
               .untilAsserted(() -> {
                 for (int i = 0; i < getVerticesPerTx(); ++i) {
                   final long currentId = lastGoodCounter + i + 1;

                   try (final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?",
                       currentId, "distributed-test")) {
                     final Result result = resultSet.next();
                     final Set<String> props = result.getPropertyNames();
                     assertThat(props).as("Found the following properties " + props).hasSize(2);
                     assertThat(result.<Long>getProperty("id")).isEqualTo(currentId);
                     assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
                   }
                 }
               });

        counter = lastGoodCounter + getVerticesPerTx();
        consecutiveFailures = 0;  // Reset on success

        // Intentional delay to pace writes during chaos scenario
        long pacingDelay = Math.min(100 + delay / 10, 500);  // Adapt pacing to current conditions
        CodeUtils.sleep(pacingDelay);

      } catch (final DuplicatedKeyException e) {
        // THIS MEANS THE ENTRY WAS INSERTED BEFORE THE CRASH
        LogManager.instance().log(this, getLogLevel(), "TEST: - RECEIVED ERROR: %s (IGNORE IT)", null, e.toString());
        counter = lastGoodCounter + getVerticesPerTx();
        consecutiveFailures = Math.max(0, consecutiveFailures - 1);  // Reduce failure count for duplicates
      } catch (final Exception e) {
        consecutiveFailures++;  // Track consecutive failures for exponential backoff
        LogManager.instance().log(this, Level.SEVERE, "TEST: - RECEIVED UNKNOWN ERROR (failures=%d): %s", e, consecutiveFailures, e.toString());
        throw e;
      }

      if (counter % 1000 == 0) {
        LogManager.instance().log(this, getLogLevel(), "TEST: - Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));

        for (int i = 0; i < getServerCount(); ++i) {
          final Database database = getServerDatabase(i, getDatabaseName());
          database.begin();
          try {
            final long tot = database.countType(VERTEX1_TYPE_NAME, false);
            LogManager.instance().log(this, getLogLevel(), "TEST: -- SERVER %d - %d records", null, i, tot);
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "TEST: -- ERROR ON RETRIEVING COUNT FROM DATABASE '%s'", e, database);
          } finally {
            database.rollback();
          }
        }

        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();

        LogManager.instance().flush();
      }
    }

    timer.cancel();

    LogManager.instance().log(this, getLogLevel(), "Done, restarted %d times", null, restarts);

    // Wait for cluster to fully stabilize after chaos
    // This prevents verification from running while servers are still recovering
    LogManager.instance().log(this, getLogLevel(), "TEST: Waiting for cluster to stabilize...");

    // Phase 1: Wait for all servers to be fully ONLINE
    await("all servers online")
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            if (getServer(i).getStatus() != ArcadeDBServer.Status.ONLINE) {
              LogManager.instance().log(this, getLogLevel(),
                  "TEST: Server %d not yet ONLINE (status=%s)", i, getServer(i).getStatus());
              return false;
            }
          }
          return true;
        });

    LogManager.instance().log(this, getLogLevel(), "TEST: All servers are ONLINE");

    // Phase 2: Wait for replication to complete
    // This implicitly ensures cluster connectivity - if replication queues drain,
    // the cluster must be connected with a working leader-replica relationship
    LogManager.instance().log(this, getLogLevel(), "TEST: Waiting for replication to complete...");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (final int s : getServerToCheck())
      checkEntriesOnServer(s);

    onAfterTest();

    assertThat(restarts >= getServerCount()).as("Restarts " + restarts + " times").isTrue();
  }

  @AfterEach
  @Override
  public void endTest() {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
    super.endTest();
  }

  private static Level getLogLevel() {
    return Level.INFO;
  }

  @Override
  protected int getTxs() {
    return 1500;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}
