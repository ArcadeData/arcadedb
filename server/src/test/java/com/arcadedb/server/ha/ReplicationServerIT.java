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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.fail;

public abstract class ReplicationServerIT extends BaseGraphServerTest {
  private static final int DEFAULT_MAX_RETRIES = 30;

  protected int getServerCount() {
    return 3;
  }

  protected int getTxs() {
    return 1000;
  }

  protected int getVerticesPerTx() {
    return 500;
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  public void replication() throws Exception {
    testReplication(0);
  }

  public void testReplication(final int serverId) {
    checkDatabases();

    final Database db = getServerDatabase(serverId, getDatabaseName());
    db.rollbackAllNested();

    db.begin();

    assertThat(db.countType(VERTEX1_TYPE_NAME, true)).as("TEST: Check for vertex count for server" + 0).isEqualTo(1);

    LogManager.instance()
        .log(this, Level.FINE, "TEST: Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    final long total = getTxs() * getVerticesPerTx();
    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      final long lastGoodCounter = counter;

      for (int retry = 0; retry < getMaxRetry(); ++retry) {
        try {
          for (int i = 0; i < getVerticesPerTx(); ++i) {
            final MutableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
            v1.set("id", ++counter);
            v1.set("name", "distributed-test");
            v1.save();
          }

          db.commit();
          break;

        } catch (final TransactionException | NeedRetryException e) {
          LogManager.instance()
              .log(this, Level.FINE, "TEST: - RECEIVED ERROR: %s (RETRY %d/%d)", null, e.toString(), retry, getMaxRetry());
          if (retry >= getMaxRetry() - 1)
            throw e;
          counter = lastGoodCounter;
        } finally {
          db.begin();
        }
      }

      if (counter % (total / 10) == 0) {
        LogManager.instance().log(this, Level.FINE, "TEST: - Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }
    }

    db.commit();

    testLog("Done");

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    assertThat(db.countType(VERTEX1_TYPE_NAME, true))
        .as("Check for vertex count for server" + 0)
        .isEqualTo(1 + (long) getTxs() * getVerticesPerTx());

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (final int s : getServerToCheck())
      checkEntriesOnServer(s);

    onAfterTest();
  }

  protected int getMaxRetry() {
    return DEFAULT_MAX_RETRIES;
  }

  protected void checkDatabases() {
    for (int s = 0; s < getServerCount(); ++s) {
      final Database db = getServerDatabase(s, getDatabaseName());
      db.begin();
      final ArcadeDBServer server = getServer(s);
      assertThatNoException().isThrownBy(() -> {
        assertThat(db.countType(VERTEX1_TYPE_NAME, true))
            .as("Check for vertex count for server" + server)
            .isEqualTo(1);
        assertThat(db.countType(VERTEX2_TYPE_NAME, true))
            .as("Check for vertex count for server" + server)
            .isEqualTo(2);

        assertThat(db.countType(EDGE1_TYPE_NAME, true))
            .as("Check for edge count for server" + server)
            .isEqualTo(1);
        assertThat(db.countType(EDGE2_TYPE_NAME, true))
            .as("Check for edge count for server" + server)
            .isEqualTo(2);
      });

      db.commit();
    }
  }

  protected void onAfterTest() {
  }

  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  protected void checkEntriesOnServer(final int server) {
    final Database db = getServerDatabase(server, getDatabaseName());

    // RESET ANY PREVIOUS TRANSACTION IN TL. IN CASE OF STOP/CRASH THE TL COULD HAVE AN OLD INSTANCE THAT POINT TO AN OLD SERVER
    DatabaseContext.INSTANCE.init((DatabaseInternal) db);

    db.transaction(() -> {
      try {
        final long recordInDb = db.countType(VERTEX1_TYPE_NAME, true);
        assertThat(recordInDb)
            .withFailMessage(
                "TEST: Check for vertex count for server" + server + " found " + recordInDb + " not less than " + (1
                    + getTxs() * getVerticesPerTx()))
            .isLessThanOrEqualTo(1 + getTxs() * getVerticesPerTx());

        final TypeIndex index = db.getSchema().getType(VERTEX1_TYPE_NAME).getPolymorphicIndexByProperties("id");
        long total = 0;
        for (final IndexCursor it = index.iterator(true); it.hasNext(); ) {
          it.dumpStats();
          it.next();
          ++total;
        }

        LogManager.instance()
            .log(this, Level.FINE, "TEST: Entries in the index (%d) >= records in database (%d)", null, total, recordInDb);

        final Map<RID, Set<String>> ridsFoundInIndex = new HashMap<>();
        long total2 = 0;
        long missingsCount = 0;

        for (final IndexCursor it = index.iterator(true); it.hasNext(); ) {
          final Identifiable rid = it.next();
          ++total2;

          Set<String> rids = ridsFoundInIndex.get(rid);
          if (rids == null) {
            rids = new HashSet<>();
            ridsFoundInIndex.put(rid.getIdentity(), rids);
          }

          rids.add(index.getName());

          Record record = null;
          try {
            record = rid.getRecord(true);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT, CAUGHT BELOW
          }

          if (record == null) {
            LogManager.instance()
                .log(this, Level.FINE, "TEST: - Cannot find record %s in database even if it's present in the index (null)", null,
                    rid);
            missingsCount++;
          }
        }

        assertThat(ridsFoundInIndex.size()).isEqualTo(recordInDb);
        assertThat(missingsCount).isZero();
        assertThat(total).isEqualTo(total2);

      } catch (final Exception e) {
        fail("TEST: Error on checking on server" + server + ": " + e.getMessage());
      }
    });
  }

  protected static Level getErrorLevel() {
    return Level.INFO;
  }
}
