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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class HARandomCrashIT extends ReplicationServerIT {
  private          int  restarts = 0;
  private volatile long delay    = 0;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Test
  public void testReplication() {
    checkDatabases();

    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        int serverId = new Random().nextInt(getServerCount());

        if (!areAllServersOnline())
          return;

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
              if (count > (getTxs() * getVerticesPerTx()) * 9 / 10) {
                LogManager.instance().log(this, Level.FINE, "TEST: Skip stop of server because it's close to the end of the test (%d/%d)", null, count,
                    getTxs() * getVerticesPerTx());
                return;
              }
            } catch (Exception e) {
              // GENERIC ERROR, SKIP STOP
              LogManager.instance().log(this, Level.SEVERE, "TEST: Skip stop of server for generic error", e);
              continue;
            } finally {
              db.rollback();
            }

            delay = 1000;
            LogManager.instance().log(this, Level.FINE, "TEST: Stopping the Server %s (delay=%d)...", null, serverId, delay);

            getServer(serverId).stop();

            while (getServer(serverId).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }

            LogManager.instance().log(this, Level.FINE, "TEST: Restarting the Server %s (delay=%d)...", null, serverId, delay);

            restarts++;
            getServer(serverId).start();

            LogManager.instance().log(this, Level.FINE, "TEST: Server %s restarted (delay=%d)...", null, serverId, delay);

            new Timer().schedule(new TimerTask() {
              @Override
              public void run() {
                delay = 0;
                LogManager.instance().log(this, Level.FINE, "TEST: Resetting delay (delay=%d)...", null, delay);
              }
            }, 10000);

            return;

          }

        LogManager.instance().log(this, Level.FINE, "TEST: Cannot restart server because unable to count vertices");

      }
    }, 15000, 10000);

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = server1Address.split(":");

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    LogManager.instance().log(this, Level.FINE, "TEST: Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      final long lastGoodCounter = counter;

      for (int retry = 0; retry < getMaxRetry(); ++retry) {
        try {

          for (int i = 0; i < getVerticesPerTx(); ++i) {

            ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter, "distributed-test");

            Assertions.assertTrue(resultSet.hasNext());
            final Result result = resultSet.next();
            Assertions.assertNotNull(result);
            final Set<String> props = result.getPropertyNames();
            Assertions.assertEquals(4, props.size());
            Assertions.assertTrue(props.contains("id"));
            Assertions.assertEquals(counter, (int) result.getProperty("id"));
            Assertions.assertTrue(props.contains("name"));
            Assertions.assertEquals("distributed-test", result.getProperty("name"));
          }

          if (delay > 0) {
            try {
              Thread.sleep(delay);
            } catch (Exception e) {
            }
          }
          break;

        } catch (TransactionException | NeedRetryException | RemoteException e) {
          LogManager.instance().log(this, Level.FINE, "TEST: - RECEIVED ERROR: %s (RETRY %d/%d)", null, e.toString(), retry, getMaxRetry());
          if (retry >= getMaxRetry() - 1)
            throw e;
          counter = lastGoodCounter;
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "TEST: - RECEIVED UNKNOWN ERROR: %s", e, e.toString());
          throw e;
        }
      }

      if (counter % 1000 == 0) {
        LogManager.instance().log(this, Level.FINE, "TEST: - Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));

        for (int i = 0; i < getServerCount(); ++i) {
          final Database database = getServerDatabase(i, getDatabaseName());
          database.begin();
          try {
            final long tot = database.countType(VERTEX1_TYPE_NAME, false);
            LogManager.instance().log(this, Level.FINE, "TEST: -- SERVER %d - %d records", null, i, tot);
          } catch (Exception e) {
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

    LogManager.instance().log(this, Level.FINE, "Done, restarted %d times", null, restarts);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();

    Assertions.assertTrue(restarts >= getServerCount(), "Restarts " + restarts);
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
