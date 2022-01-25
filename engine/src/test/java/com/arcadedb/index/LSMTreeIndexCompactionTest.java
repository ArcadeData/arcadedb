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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

/**
 * This test stresses the index compaction by forcing using only 1MB of RAM for compaction causing multiple page compacted index.
 *
 * @author Luca
 */
public class LSMTreeIndexCompactionTest extends TestHelper {
  private static final int    TOT               = 100_000;
  private static final int    INDEX_PAGE_SIZE   = 64 * 1024; // 64K
  private static final int    COMPACTION_RAM_MB = 1; // 1MB
  private static final int    PARALLEL          = 4;
  private static final String TYPE_NAME         = "Device";

  @Test
  public void testCompaction() {
    try {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(COMPACTION_RAM_MB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      // INSERT DATA AND CHECK WITH LOOKUPS (EVERY 100)
      LogManager.instance().log(this, Level.FINE, "TEST: INSERT DATA AND CHECK WITH LOKUPS (EVERY 100)");
      insertData();
      checkLookups(100, 1);

      // THIS TIME LOOK UP FOR KEYS WHILE COMPACTION
      LogManager.instance().log(this, Level.FINE, "TEST: THIS TIME LOOK UP FOR KEYS WHILE COMPACTION");
      final CountDownLatch semaphore1 = new CountDownLatch(1);
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            compaction();
          } finally {
            semaphore1.countDown();
          }
        }
      }, 0);

      checkLookups(1, 1);

      semaphore1.await();

      // INSERT DATA ON TOP OF THE MIXED MUTABLE-COMPACTED INDEX AND CHECK WITH LOOKUPS
      LogManager.instance().log(this, Level.FINE, "TEST: INSERT DATA ON TOP OF THE MIXED MUTABLE-COMPACTED INDEX AND CHECK WITH LOOKUPS");
      insertData();
      checkLookups(1, 2);
      compaction();
      checkLookups(1, 2);

      // INSERT DATA WHILE COMPACTING AND CHECK AGAIN
      LogManager.instance().log(this, Level.FINE, "TEST: INSERT DATA WHILE COMPACTING AND CHECK AGAIN");
      final CountDownLatch semaphore2 = new CountDownLatch(1);
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          compaction();
          semaphore2.countDown();
        }
      }, 0);

      insertData();

      semaphore2.await();

      checkLookups(1, 3);
      compaction();
      checkLookups(1, 3);

    } catch (InterruptedException e) {
      Assertions.fail(e);
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(300);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(10);
    }
  }

  private void compaction() {
    if (database.isOpen())
      for (Index index : database.getSchema().getIndexes()) {
        if (database.isOpen())
          try {
            index.scheduleCompaction();
            ((IndexInternal) index).compact();
          } catch (Exception e) {
            Assertions.fail(e);
          }
      }
  }

  private void insertData() {
    database.transaction(() -> {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        DocumentType v = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL);

        v.createProperty("id", String.class);
        v.createProperty("number", String.class);
        v.createProperty("relativeName", String.class);

        v.createProperty("Name", String.class);

        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "id" }, INDEX_PAGE_SIZE);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "number" }, INDEX_PAGE_SIZE);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "relativeName" }, INDEX_PAGE_SIZE);
      }
    });

    long begin = System.currentTimeMillis();
    try {

      database.setReadYourWrites(false);
      database.async().setCommitEvery(50000);
      database.async().setParallelLevel(PARALLEL);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);

      database.async().onError(new ErrorCallback() {
        @Override
        public void call(Throwable exception) {
          LogManager.instance().log(this, Level.SEVERE, "TEST: ERROR: ", exception);
          exception.printStackTrace();
          Assertions.fail(exception);
        }
      });

      final int totalToInsert = TOT;
      final long startTimer = System.currentTimeMillis();

      database.async().transaction(new Database.TransactionScope() {
        @Override
        public void execute() {
          long lastLap = startTimer;
          long lastLapCounter = 0;

          long counter = 0;
          for (; counter < totalToInsert; ++counter) {
            final MutableDocument v = database.newDocument("Device");

            final String randomString = "" + counter;

            v.set("id", randomString); // INDEXED
            v.set("number", "" + counter); // INDEXED
            v.set("relativeName", "/shelf=" + counter + "/slot=1"); // INDEXED

            v.set("Name", "1" + counter);

            v.save();

            if (counter % 1000 == 0) {
              if (System.currentTimeMillis() - lastLap > 1000) {
                LogManager.instance().log(this, Level.FINE, "TEST: - Progress %d/%d (%d records/sec)", null, counter, totalToInsert, counter - lastLapCounter);
                lastLap = System.currentTimeMillis();
                lastLapCounter = counter;
              }
            }
          }
        }
      });

      LogManager.instance().log(this, Level.FINE, "TEST: Inserted " + totalToInsert + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      LogManager.instance().log(this, Level.FINE, "TEST: Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

    database.async().waitCompletion();
  }

  private void checkLookups(final int step, final int expectedItems) {
    database.transaction(() -> Assertions.assertEquals(TOT * expectedItems, database.countType(TYPE_NAME, false)));

    LogManager.instance().log(this, Level.FINE, "TEST: Lookup all the keys...");

    long begin = System.currentTimeMillis();

    int checked = 0;

    for (long id = 0; id < TOT; id += step) {
      try {
        final IndexCursor records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
        Assertions.assertNotNull(records);
        if (records.size() != expectedItems)
          LogManager.instance().log(this, Level.FINE, "Cannot find key '%s'", null, id);

        Assertions.assertEquals(expectedItems, records.size(), "Wrong result for lookup of key " + id);

        for (Iterator<Identifiable> it = records.iterator(); it.hasNext(); ) {
          final Identifiable rid = it.next();
          final Document record = (Document) rid.getRecord();
          Assertions.assertEquals("" + id, record.get("id"));
        }

        checked++;

        if (checked % 10000 == 0) {
          long delta = System.currentTimeMillis() - begin;
          if (delta < 1)
            delta = 1;
          LogManager.instance().log(this, Level.FINE, "Checked " + checked + " lookups in " + delta + "ms = " + (10000 / delta) + " lookups/msec");
          begin = System.currentTimeMillis();
        }
      } catch (Exception e) {
        Assertions.fail("Error on lookup key " + id, e);
      }
    }
    LogManager.instance().log(this, Level.FINE, "TEST: Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
  }
}
