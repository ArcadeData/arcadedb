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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class HARandomCrashIT extends ReplicationServerIT {
  private          int  restarts = 0;
  private volatile long delay    = 0;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
  }

  @Override
  protected HAServer.SERVER_ROLE getServerRole(int serverIndex) {
    return HAServer.SERVER_ROLE.ANY;
  }

  @Test
  @Override
  public void replication() {
    checkDatabases();

    final Timer timer = new Timer("HARandomCrashIT-Timer", true);
    final Random random = new Random();

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        if (!areAllReplicasAreConnected())
          return;

        if (restarts >= getServerCount()) {
          delay = 0;
          return;
        }

        // Select a random server that is currently started
        int serverId = -1;
        for (int attempt = 0; attempt < getServerCount() * 2; attempt++) {
          int candidate = random.nextInt(getServerCount());
          if (getServer(candidate).isStarted()) {
            serverId = candidate;
            break;
          }
        }

        if (serverId == -1) {
          LogManager.instance().log(this, getLogLevel(), "TEST: No started server found to crash");
          return;
        }

        final Database db = getServer(serverId).getDatabase(getDatabaseName());
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
          return;
        } finally {
          db.rollback();
        }

        delay = 100;
        final int finalServerId = serverId;
        LogManager.instance().log(this, getLogLevel(), "TEST: Stopping the Server %d (delay=%d)...", null, finalServerId, delay);

        getServer(finalServerId).stop();

        // Use Awaitility instead of busy-wait
        try {
          org.awaitility.Awaitility.await()
              .atMost(30, java.util.concurrent.TimeUnit.SECONDS)
              .pollInterval(300, java.util.concurrent.TimeUnit.MILLISECONDS)
              .until(() -> getServer(finalServerId).getStatus() != ArcadeDBServer.STATUS.SHUTTING_DOWN);
        } catch (org.awaitility.core.ConditionTimeoutException e) {
          LogManager.instance().log(this, Level.SEVERE, "TEST: Timeout waiting for server %d to shutdown", e, finalServerId);
          return;
        }

        LogManager.instance().log(this, getLogLevel(), "TEST: Restarting the Server %d (delay=%d)...", null, finalServerId, delay);

        restarts++;

        boolean restarted = false;
        for (int restartRetry = 0; restartRetry < 3; restartRetry++) {
          try {
            getServer(finalServerId).start();
            restarted = true;
            break;
          } catch (Throwable e) {
            LogManager.instance()
                .log(this, getLogLevel(), "TEST: Error on restarting the server %d, retrying (%d/%d)", e, finalServerId, restartRetry + 1, 3);
            CodeUtils.sleep(1_000);
          }
        }

        if (!restarted) {
          LogManager.instance().log(this, Level.SEVERE, "TEST: Failed to restart server %d after 3 attempts", null, finalServerId);
          return;
        }

        LogManager.instance().log(this, getLogLevel(), "TEST: Server %d restarted successfully (delay=%d)...", null, finalServerId, delay);

        // Wait for server to be fully online and replicas to reconnect
        try {
          org.awaitility.Awaitility.await()
              .atMost(30, java.util.concurrent.TimeUnit.SECONDS)
              .pollInterval(500, java.util.concurrent.TimeUnit.MILLISECONDS)
              .until(() -> areAllReplicasAreConnected());
        } catch (org.awaitility.core.ConditionTimeoutException e) {
          LogManager.instance().log(this, Level.WARNING, "TEST: Timeout waiting for replicas to reconnect after server %d restart", e, finalServerId);
        }

        delay = 0;
      }
    }, 20_000, 15_000);

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = HostUtil.parseHostAddress(server1Address, HostUtil.CLIENT_DEFAULT_PORT);

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    LogManager.instance()
        .log(this, getLogLevel(), "TEST: Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      final long lastGoodCounter = counter;

      for (int retry = 0; retry < getMaxRetry(); ++retry) {
        try {

          for (int i = 0; i < getVerticesPerTx(); ++i) {

            final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter,
                "distributed-test");

            final Result result = resultSet.next();
            final Set<String> props = result.getPropertyNames();
            assertThat(props).as("Found the following properties " + props).hasSize(2);
            assertThat(result.<Long>getProperty("id")).isEqualTo(counter);
            assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
          }

          // Small delay to allow replication to process if server crashes are happening
          if (delay > 0)
            CodeUtils.sleep(Math.min(delay, 200));

          break;

        } catch (final TransactionException | NeedRetryException | RemoteException | TimeoutException e) {
          LogManager.instance()
              .log(this, getLogLevel(), "TEST: - RECEIVED ERROR: %s %s (RETRY %d/%d)", null, e.getClass().getName(), e.toString(),
                  retry, getMaxRetry());
          if (retry >= getMaxRetry() - 1)
            throw e;
          counter = lastGoodCounter;

          // Exponential backoff for retries
          CodeUtils.sleep(Math.min(1_000 * (retry + 1), 5_000));

        } catch (final DuplicatedKeyException e) {
          // THIS MEANS THE ENTRY WAS INSERTED BEFORE THE CRASH
          LogManager.instance().log(this, getLogLevel(), "TEST: - RECEIVED ERROR: %s (IGNORE IT)", null, e.toString());
          break;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "TEST: - RECEIVED UNKNOWN ERROR: %s", e, e.toString());
          throw e;
        }
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

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (final int s : getServerToCheck())
      checkEntriesOnServer(s);

    onAfterTest();

    assertThat(restarts >= getServerCount()).as("Restarts " + restarts + " times").isTrue();
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
