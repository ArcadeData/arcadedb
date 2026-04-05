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
package com.arcadedb.server.ha.raft;

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

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.*;

class RaftHARandomCrashIT extends BaseRaftHATest {

  private static final int TXS                   = 1_500;
  private static final int VERTICES_PER_TX       = 10;
  private static final int MAX_RETRY             = 30;
  private static final int CRASH_INITIAL_DELAY_MS = 15_000;
  private static final int CRASH_INTERVAL_MS     = 10_000;
  private static final int RESTART_POLL_MS       = 300;
  private static final int TX_SLEEP_MS           = 100;
  private static final int RETRY_SLEEP_MS        = 1_000;

  private volatile int restarts = 0;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Test
  void replicationWithRandomCrashes() {
    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        // Only crash when the cluster has a leader
        boolean hasLeader = false;
        for (int i = 0; i < getServerCount(); i++) {
          final RaftHAPlugin plugin = getRaftPlugin(i);
          if (plugin != null && plugin.isLeader()) {
            hasLeader = true;
            break;
          }
        }
        if (!hasLeader)
          return;

        if (restarts >= getServerCount())
          return;

        final int serverId = ThreadLocalRandom.current().nextInt(getServerCount());

        for (int i = 0; i < getServerCount(); ++i) {
          if (getServer(i).isStarted()) {
            final Database db = getServer(i).getDatabase(getDatabaseName());
            db.begin();
            try {
              final long count = db.countType(VERTEX1_TYPE_NAME, true);
              if (count > (long) TXS * VERTICES_PER_TX * 9 / 10) {
                LogManager.instance().log(this, Level.INFO,
                    "TEST: Skipping crash — near end of test (%d/%d)", count, TXS * VERTICES_PER_TX);
                return;
              }
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.SEVERE, "TEST: Skipping crash — error counting vertices", e);
              continue;
            } finally {
              db.rollback();
            }

            LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %d", serverId);
            getServer(serverId).stop();

            while (getServer(serverId).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
              CodeUtils.sleep(RESTART_POLL_MS);

            restarts++;
            LogManager.instance().log(this, Level.INFO, "TEST: Restarting server %d", serverId);

            for (int attempt = 0; attempt < 3; attempt++) {
              try {
                getServer(serverId).start();
                break;
              } catch (final Throwable e) {
                LogManager.instance().log(this, Level.INFO, "TEST: Restart attempt %d/3 failed", attempt + 1, e);
              }
            }

            LogManager.instance().log(this, Level.INFO, "TEST: Server %d restarted", serverId);

            return;
          }
        }
      }
    }, CRASH_INITIAL_DELAY_MS, CRASH_INTERVAL_MS);

    final String server0Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] addressParts = HostUtil.parseHostAddress(server0Address, HostUtil.CLIENT_DEFAULT_PORT);
    final RemoteDatabase db = new RemoteDatabase(addressParts[0], Integer.parseInt(addressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    long counter = 0;

    for (int tx = 0; tx < TXS; ++tx) {
      final long lastGoodCounter = counter;

      for (int retry = 0; retry < MAX_RETRY; ++retry) {
        try {
          for (int i = 0; i < VERTICES_PER_TX; ++i) {
            final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?",
                ++counter, "distributed-test");
            final Result result = resultSet.next();
            final Set<String> props = result.getPropertyNames();
            assertThat(props).hasSize(2);
            assertThat(result.<Long>getProperty("id")).isEqualTo(counter);
            assertThat(result.<String>getProperty("name")).isEqualTo("distributed-test");
          }
          CodeUtils.sleep(TX_SLEEP_MS);
          break;
        } catch (final TransactionException | NeedRetryException | RemoteException | TimeoutException e) {
          LogManager.instance().log(this, Level.INFO, "TEST: Error (retry %d/%d): %s", retry, MAX_RETRY, e);
          if (retry >= MAX_RETRY - 1)
            throw e;
          counter = lastGoodCounter;
          CodeUtils.sleep(RETRY_SLEEP_MS);
        } catch (final DuplicatedKeyException e) {
          // Entry inserted before crash — this is expected
          LogManager.instance().log(this, Level.INFO, "TEST: DuplicatedKey (expected after crash): %s", e);
          break;
        }
      }
    }

    timer.cancel();

    LogManager.instance().log(this, Level.INFO, "TEST: Done. Restarts: %d", restarts);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    assertClusterConsistency();
    assertThat(restarts).as("Expected at least %d restarts", getServerCount()).isGreaterThanOrEqualTo(getServerCount());
  }
}
