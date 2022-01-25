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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.logging.*;

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

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS.setValue("2424-2500");
  }

  @Test
  public void testReplication() throws Exception {
    testReplication(0);
  }

  public void testReplication(final int serverId) {
    checkDatabases();

    Database db = getServerDatabase(serverId, getDatabaseName());
    db.rollbackAllNested();

    db.begin();

    Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "TEST: Check for vertex count for server" + 0);

    LogManager.instance().log(this, Level.INFO, "TEST: Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

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

        } catch (TransactionException | NeedRetryException e) {
          LogManager.instance().log(this, Level.INFO, "TEST: - RECEIVED ERROR: %s (RETRY %d/%d)", null, e.toString(), retry, getMaxRetry());
          if (retry >= getMaxRetry() - 1)
            throw e;
          counter = lastGoodCounter;
        } finally {
          db.begin();
        }
      }

      if (counter % (total / 10) == 0) {
        LogManager.instance().log(this, Level.INFO, "TEST: - Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }
    }

    db.commit();
    db.begin();

    testLog("Done");

    Assertions.assertEquals(1 + getTxs() * getVerticesPerTx(), db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();
  }

  protected int getMaxRetry() {
    return DEFAULT_MAX_RETRIES;
  }

  protected void checkDatabases() {
    for (int s = 0; s < getServerCount(); ++s) {
      Database db = getServerDatabase(s, getDatabaseName());
      db.begin();
      try {
        Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);
        Assertions.assertEquals(2, db.countType(VERTEX2_TYPE_NAME, true), "Check for vertex count for server" + s);

        Assertions.assertEquals(1, db.countType(EDGE1_TYPE_NAME, true), "Check for edge count for server" + s);
        Assertions.assertEquals(2, db.countType(EDGE2_TYPE_NAME, true), "Check for edge count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      }
    }
  }

  protected void onAfterTest() {
  }

  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  protected void checkEntriesOnServer(final int s) {
    final Database db = getServerDatabase(s, getDatabaseName());

    // RESET ANY PREVIOUS TRANSACTION IN TL. IN CASE OF STOP/CRASH THE TL COULD HAVE AN OLD INSTANCE THAT POINT TO AN OLD SERVER
    DatabaseContext.INSTANCE.init((DatabaseInternal) db);

    db.transaction(() -> {
      try {
        final long recordInDb = db.countType(VERTEX1_TYPE_NAME, true);
        Assertions.assertTrue(recordInDb <= 1 + getTxs() * getVerticesPerTx(),
            "TEST: Check for vertex count for server" + s + " found " + recordInDb + " not less than " + (1 + getTxs() * getVerticesPerTx()));

        final TypeIndex index = db.getSchema().getType(VERTEX1_TYPE_NAME).getPolymorphicIndexByProperties("id");
        long total = 0;
        for (IndexCursor it = index.iterator(true); it.hasNext(); ) {
          it.dumpStats();
          it.next();
          ++total;
        }

        LogManager.instance().log(this, Level.INFO, "TEST: Entries in the index (%d) >= records in database (%d)", null, total, recordInDb);

        final Map<RID, Set<String>> ridsFoundInIndex = new HashMap<>();
        long total2 = 0;
        long missingsCount = 0;

        for (IndexCursor it = index.iterator(true); it.hasNext(); ) {
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
          } catch (RecordNotFoundException e) {
            // IGNORE IT, CAUGHT BELOW
          }

          if (record == null) {
            LogManager.instance().log(this, Level.FINE, "TEST: - Cannot find record %s in database even if it's present in the index (null)", null, rid);
            missingsCount++;
          }

        }

        Assertions.assertEquals(recordInDb, ridsFoundInIndex.size(), "TEST: Found " + missingsCount + " missing records");
        Assertions.assertEquals(0, missingsCount);
        Assertions.assertEquals(total, total2);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("TEST: Error on checking on server" + s);
      }
    });
  }
}
