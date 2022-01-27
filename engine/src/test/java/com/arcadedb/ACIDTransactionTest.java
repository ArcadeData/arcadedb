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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.WALException;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class ACIDTransactionTest extends TestHelper {
  @Override
  protected void beginTest() {
    GlobalConfiguration.TX_RETRIES.setValue(50);

    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    database.transaction(() -> {
      if (!database.getSchema().existsType("V")) {
        final DocumentType v = database.getSchema().createDocumentType("V");

        v.createProperty("id", Integer.class);
        v.createProperty("name", String.class);
        v.createProperty("surname", String.class);
      }
    });
  }

  @Test
  public void testAsyncTX() {
    final Database db = database;

    db.async().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);
    db.async().setTransactionUseWAL(true);
    db.async().setCommitEvery(1);

    final int TOT = 1000;

    final AtomicInteger total = new AtomicInteger(0);

    try {
      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", total.get());
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.async().createRecord(v, null);
      }

      db.async().waitCompletion();

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }

    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> Assertions.assertEquals(TOT, database.countType("V", true)));
  }

  @Test
  public void testDatabaseInternals() {
    Assertions.assertNotNull(((DatabaseInternal) database).getStats());
    Assertions.assertNull(database.getCurrentUserName());
  }

  @Test
  public void testCrashDuringTx() {
    final Database db = database;
    db.begin();
    try {
      final MutableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

    } finally {
      ((DatabaseInternal) db).kill();
    }

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> Assertions.assertEquals(0, database.countType("V", true)));
  }

  @Test
  public void testIOExceptionAfterWALIsWritten() {
    final Database db = database;
    db.begin();

    final Callable<Void> callback = () -> {
      throw new IOException("Test IO Exception");
    };

    try {
      final MutableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, callback);

      db.commit();

      Assertions.fail("Expected commit to fail");

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof WALException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> Assertions.assertEquals(1, database.countType("V", true)));

    ((DatabaseInternal) db).unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, callback);
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenLastRecords() {
    final Database db = database;

    final AtomicInteger errors = new AtomicInteger(0);

    db.async().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);
    db.async().setTransactionUseWAL(true);
    db.async().setCommitEvery(1);
    db.async().onError(exception -> errors.incrementAndGet());

    final int TOT = 1000;

    final AtomicInteger total = new AtomicInteger(0);
    final AtomicInteger commits = new AtomicInteger(0);

    try {
      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<>() {
          @Override
          public Void call() throws IOException {
              if (commits.incrementAndGet() > TOT - 1) {
                  LogManager.instance().log(this, Level.INFO, "TEST: Causing IOException at commit %d...", commits.get());
                  throw new IOException("Test IO Exception");
              }
              return null;
          }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.async().createRecord(v, null);
      }

      db.async().waitCompletion();

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }

      Assertions.assertEquals(1, errors.get());

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> Assertions.assertEquals(TOT, database.countType("V", true)));
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenManyRecords() {
    final Database db = database;

    final int TOT = 100000;

    final AtomicInteger total = new AtomicInteger(0);

    final AtomicInteger errors = new AtomicInteger(0);

    db.async().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);
    db.async().setTransactionUseWAL(true);
    db.async().setCommitEvery(1000000);
    db.async().onError(exception -> errors.incrementAndGet());

    try {
      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, () -> {
        if (total.incrementAndGet() > TOT - 10)
          throw new IOException("Test IO Exception");
        return null;
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.async().createRecord(v, null);
      }

      db.async().waitCompletion();

      Assertions.assertTrue(errors.get() > 0);

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> Assertions.assertEquals(TOT, database.countType("V", true)));
  }

  @Test
  public void multiThreadConcurrentTransactions() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Stock");
      type.createProperty("symbol", Type.STRING);
      type.createProperty("date", Type.DATETIME);
      type.createProperty("history", Type.LIST);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "symbol", "date");

      final DocumentType type2 = database.getSchema().createDocumentType("Aggregate", 1);
      type2.createProperty("volume", Type.LONG);
    });

    final int TOT_STOCKS = 100;
    final int TOT_DAYS = 150;
    final int TOT_MINS = 400;

    final Calendar startingDay = Calendar.getInstance();
    for (int i = 0; i < TOT_DAYS; ++i)
      startingDay.add(Calendar.DAY_OF_YEAR, -1);

    final AtomicInteger errors = new AtomicInteger();

    for (int stockId = 0; stockId < TOT_STOCKS; ++stockId) {
      final int id = stockId;

      database.async().transaction(() -> {
        try {
          final Calendar now = Calendar.getInstance();
          now.setTimeInMillis(startingDay.getTimeInMillis());

          for (int i = 0; i < TOT_DAYS; ++i) {
            final MutableDocument stock = database.newDocument("Stock");

            stock.set("symbol", "" + id);
            stock.set("date", now.getTimeInMillis());

            final List<Document> history = new ArrayList<>();
            for (int e = 0; e < TOT_MINS; ++e) {
              final MutableDocument embedded = ((DatabaseInternal) database).newEmbeddedDocument(null, "Aggregate");
              embedded.set("volume", 1_000_000l);
              history.add(embedded);
            }

            stock.set("history", history);
            stock.save();

            now.add(Calendar.DAY_OF_YEAR, +1);
          }

        } catch (Exception e) {
          errors.incrementAndGet();
          LogManager.instance().log(this, Level.SEVERE, "Error on saving stockId=%d", e, id);
        }
      });
    }

    database.async().waitCompletion();

    Assertions.assertEquals(0, errors.get());

    database.transaction(() -> {
      Assertions.assertEquals(TOT_STOCKS * TOT_DAYS, database.countType("Stock", true));
      Assertions.assertEquals(0, database.countType("Aggregate", true));

      final Calendar now = Calendar.getInstance();
      now.setTimeInMillis(startingDay.getTimeInMillis());

      for (int i = 0; i < TOT_DAYS; ++i) {
        for (int stockId = 0; stockId < TOT_STOCKS; ++stockId) {
          final ResultSet result = database.query("sql", "select from Stock where symbol = ? and date = ?", "" + stockId, now.getTimeInMillis());
          Assertions.assertNotNull(result);
          Assertions.assertTrue(result.hasNext(), "Cannot find stock=" + stockId + " date=" + now.getTimeInMillis());
        }
        now.add(Calendar.DAY_OF_YEAR, +1);
      }
    });
  }

  private void verifyDatabaseWasNotClosedProperly() {
    final AtomicBoolean dbNotClosedCaught = new AtomicBoolean(false);

    database.close();
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, () -> {
      dbNotClosedCaught.set(true);
      return null;
    });

    database = factory.open();
    Assertions.assertTrue(dbNotClosedCaught.get());
  }

  private void verifyWALFilesAreStillPresent() {
    File dbDir = new File(getDatabasePath());
    Assertions.assertTrue(dbDir.exists());
    Assertions.assertTrue(dbDir.isDirectory());
    File[] files = dbDir.listFiles((dir, name) -> name.endsWith("wal"));
    Assertions.assertTrue(files.length > 0);
  }
}
