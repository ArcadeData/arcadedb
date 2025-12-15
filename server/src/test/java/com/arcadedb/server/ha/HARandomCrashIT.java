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

public class HARandomCrashIT extends ReplicationServerIT {
  private          int   restarts = 0;
  private volatile long  delay    = 0;
  private          Timer timer    = null;

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

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        if (!areAllReplicasAreConnected())
          return;

        final int serverId = ThreadLocalRandom.current().nextInt(getServerCount());

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

            delay = 100;
            LogManager.instance().log(this, getLogLevel(), "TEST: Stopping the Server %s (delay=%d)...", null, serverId, delay);

            getServer(serverId).stop();

            // Wait for server to finish shutting down using Awaitility
            await().atMost(Duration.ofSeconds(60))
                   .pollInterval(Duration.ofMillis(300))
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

            new Timer().schedule(new TimerTask() {
              @Override
              public void run() {
                delay = 0;
                LogManager.instance().log(this, getLogLevel(), "TEST: Resetting delay (delay=%d)...", null, delay);
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
        // Use Awaitility to handle retry logic with proper exception handling
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(1))
               .pollDelay(Duration.ofMillis(100))  // Initial delay between operations
               .ignoreExceptionsMatching(e ->
                   e instanceof TransactionException ||
                   e instanceof NeedRetryException ||
                   e instanceof RemoteException ||
                   e instanceof TimeoutException)
               .untilAsserted(() -> {
                 for (int i = 0; i < getVerticesPerTx(); ++i) {
                   final long currentId = lastGoodCounter + i + 1;

                   final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?",
                       currentId, "distributed-test");

                   final Result result = resultSet.next();
                   final Set<String> props = result.getPropertyNames();
                   assertThat(props).as("Found the following properties " + props).hasSize(2);
                   assertThat(result.<Long>getProperty("id")).isEqualTo(currentId);
                   assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
                 }
               });

        counter = lastGoodCounter + getVerticesPerTx();

        // Intentional delay to pace writes during chaos scenario (not waiting for a condition)
        CodeUtils.sleep(100);

      } catch (final DuplicatedKeyException e) {
        // THIS MEANS THE ENTRY WAS INSERTED BEFORE THE CRASH
        LogManager.instance().log(this, getLogLevel(), "TEST: - RECEIVED ERROR: %s (IGNORE IT)", null, e.toString());
        counter = lastGoodCounter + getVerticesPerTx();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "TEST: - RECEIVED UNKNOWN ERROR: %s", e, e.toString());
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
