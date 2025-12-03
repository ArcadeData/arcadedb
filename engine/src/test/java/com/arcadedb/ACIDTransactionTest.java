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
import com.arcadedb.database.RID;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.WALException;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class ACIDTransactionTest extends TestHelper {
  @Test
  void asyncTX() {
    final Database db = database;

    db.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
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

    } catch (final TransactionException e) {
      assertThat(e.getCause() instanceof IOException).isTrue();
    }

    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> assertThat(database.countType("V", true)).isEqualTo(TOT));
  }

  @Test
  void indexCreationWhileAsyncMustFail() {
    final Database db = database;

    final int TOT = 100;

    final AtomicInteger total = new AtomicInteger(0);

    try {
      db.async().setParallelLevel(2);
      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", total.get());
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.async().createRecord(v, null);
      }

      // creating the index should throw exception because there's an aync creation ongoing: sometimes it doesn't happen, the async is finished
      try {
        database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      } catch (NeedRetryException e) {
        //no action
      }

      db.async().waitCompletion();

    } catch (final TransactionException e) {
      assertThat(e.getCause() instanceof IOException).isTrue();
    }

    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> assertThat(database.countType("V", true)).isEqualTo(TOT));
  }

  @Test
  void databaseInternals() {
    assertThat(database.getStats()).isNotNull();
    assertThat(database.getCurrentUserName()).isNull();
  }

  @Test
  void crashDuringTx() {
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

    database.transaction(() -> assertThat(database.countType("V", true)).isEqualTo(0));
  }

  @Test
  void iOExceptionAfterWALIsWritten() {
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

      fail("Expected commit to fail");

    } catch (final TransactionException e) {
      assertThat(e.getCause() instanceof WALException).isTrue();
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> assertThat(database.countType("V", true)).isEqualTo(1));

    ((DatabaseInternal) db).unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, callback);
  }

  @Test
  void asyncIOExceptionAfterWALIsWrittenLastRecords() {
    final Database db = database;

    final AtomicInteger errors = new AtomicInteger(0);

    db.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
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

      assertThat(errors.get()).isEqualTo(1);

    } catch (final TransactionException e) {
      assertThat(e.getCause() instanceof IOException).isTrue();
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> assertThat(database.countType("V", true)).isEqualTo(TOT));
  }

  @Test
  @Tag("slow")
  void asyncIOExceptionAfterWALIsWrittenManyRecords() {
    final Database db = database;

    final int TOT = 100000;

    final AtomicInteger total = new AtomicInteger(0);
    final AtomicInteger errors = new AtomicInteger(0);

    db.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
    assertThat(db.async().getTransactionSync()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);

    db.async().setTransactionUseWAL(true);
    assertThat(db.async().isTransactionUseWAL()).isTrue();

    db.async().setCommitEvery(1000000);
    assertThat(db.async().getCommitEvery()).isEqualTo(1000000);

    db.async().setBackPressure(1);
    assertThat(db.async().getBackPressure()).isEqualTo(1);

    db.async().onError(exception -> errors.incrementAndGet());

    final Callable<Void> callback = () -> {
      if (total.incrementAndGet() > TOT - 10)
        throw new IOException("Test IO Exception");
      return null;
    };

    try {

      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, callback);

      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.async().createRecord(v, null);
      }

      db.async().waitCompletion();

      assertThat(errors.get() > 0).isTrue();

    } catch (final TransactionException e) {
      assertThat(e.getCause() instanceof IOException).isTrue();
    }
    ((DatabaseInternal) db).kill();

    ((DatabaseInternal) db).unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, callback);

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> assertThat(database.countType("V", true)).isEqualTo(TOT));
  }

  @Test
  void multiThreadConcurrentTransactions() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName("Stock").withTotalBuckets(32).create();
      type.createProperty("symbol", Type.STRING);
      type.createProperty("date", Type.DATETIME);
      type.createProperty("history", Type.LIST);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "symbol", "date");

      final DocumentType type2 = database.getSchema().buildDocumentType().withName("Aggregate").withTotalBuckets(1).create();
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
              embedded.set("volume", 1_000_000L);
              history.add(embedded);
            }

            stock.set("history", history);
            stock.save();

            now.add(Calendar.DAY_OF_YEAR, +1);
          }

        } catch (final Exception e) {
          errors.incrementAndGet();
          System.err.printf("\nError on saving stockId=%d", id);
          e.printStackTrace(System.err);
        }
      });
    }

    database.async().waitCompletion();

    assertThat(errors.get()).isEqualTo(0);

    database.transaction(() -> {
      assertThat(database.countType("Stock", true)).isEqualTo(TOT_STOCKS * TOT_DAYS);
      assertThat(database.countType("Aggregate", true)).isEqualTo(0);

      final Calendar now = Calendar.getInstance();
      now.setTimeInMillis(startingDay.getTimeInMillis());

      for (int i = 0; i < TOT_DAYS; ++i) {
        for (int stockId = 0; stockId < TOT_STOCKS; ++stockId) {
          final ResultSet result = database.query("sql", "select from Stock where symbol = ? and date = ?", "" + stockId,
              now.getTimeInMillis());
          assertThat((Iterator<? extends Result>) result).isNotNull();
          assertThat(result.hasNext()).withFailMessage("Cannot find stock=" + stockId + " date=" + now.getTimeInMillis()).isTrue();
        }
        now.add(Calendar.DAY_OF_YEAR, +1);
      }
    });
  }

  @Test
  @Tag("slow")
  void asyncEdges() {
    final Database db = database;

    final int TOT = 10000;

    final AtomicInteger total = new AtomicInteger(0);
    final AtomicInteger errors = new AtomicInteger(0);

    db.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
    assertThat(db.async().getTransactionSync()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);

    db.async().setTransactionUseWAL(true);
    assertThat(db.async().isTransactionUseWAL()).isTrue();

    db.async().setCommitEvery(TOT);
    assertThat(db.async().getCommitEvery()).isEqualTo(TOT);

    db.async().setBackPressure(1);
    assertThat(db.async().getBackPressure()).isEqualTo(1);

    db.async().onError(exception -> errors.incrementAndGet());

    final VertexType type = database.getSchema().getOrCreateVertexType("Node");
    type.getOrCreateProperty("id", Type.STRING).getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    type.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());

    database.getSchema().getOrCreateEdgeType("Arc");

    for (; total.get() < TOT; total.incrementAndGet()) {
      final MutableVertex v = db.newVertex("Node");
      v.set("id", total.get());
      v.set("name", "Crash");
      v.set("surname", "Test");
      db.async().createRecord(v, null);
    }
    db.async().waitCompletion();

    assertThat(errors.get()).isEqualTo(0);

    for (int i = 1; i < TOT; ++i) {
      db.async().newEdgeByKeys("Node", "id", i, "Node", "id", i - 1, false, "Arc", true, false, null, "id", i);
    }
    db.async().waitCompletion();

    assertThat(errors.get()).isEqualTo(0);

    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    database.transaction(() -> assertThat(database.countType("Node", true)).isEqualTo(TOT));
    database.transaction(() -> assertThat(database.countType("Arc", true)).isEqualTo(TOT - 1));
  }

  @Test
  void exceptionInsideTransaction() {
    final Database db = database;

    final int TOT = 100;

    database.getSchema().getOrCreateVertexType("Node");

    final AtomicInteger thrownExceptions = new AtomicInteger(0);
    final AtomicInteger caughtExceptions = new AtomicInteger(0);
    final AtomicInteger committed = new AtomicInteger(0);

    final RID[] rid = new RID[1];

    database.transaction(() -> {
      final MutableVertex v = db.newVertex("Node");
      v.set("id", 0);
      v.set("name", "Exception(al)");
      v.set("surname", "Test");
      v.save();
      rid[0] = v.getIdentity();
    });

    final int CONCURRENT_THREADS = 4;

    // SPAWN ALL THE THREADS USING EXECUTORSERVICE
    final ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_THREADS);
    final List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      Future<?> future = executorService.submit(() -> {
        for (int k = 0; k < TOT; ++k) {
          final int id = k;

          try {
            database.transaction(() -> {
              MutableVertex v = rid[0].asVertex().modify();
              v.set("id", id);
              v.save();

              if ((id + 1) % 100 == 0) {
                thrownExceptions.incrementAndGet();
                throw new RuntimeException("Test exception at " + id);
              }
            });
            committed.incrementAndGet();

          } catch (Exception e) {
            caughtExceptions.incrementAndGet();
          }
        }

      });
      futures.add(future);
    }

    // WAIT FOR ALL THE THREADS
    for (Future<?> future : futures) {
      try {
        future.get(120, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LogManager.instance().log(this, Level.WARNING, "Thread interrupted while waiting for future", e);
      } catch (ExecutionException e) {
        LogManager.instance().log(this, Level.WARNING, "Execution exception in future", e);
      } catch (TimeoutException e) {
        LogManager.instance().log(this, Level.SEVERE, "Future timed out after 120 seconds", e);
        future.cancel(true);
      }
    }

    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    assertThat(database.countType("Node", true)).isEqualTo(1);

    assertThat(committed.get()).isGreaterThan(0);
    assertThat(thrownExceptions.get()).isGreaterThan(0);
    assertThat(caughtExceptions.get()).isGreaterThan(0);
    assertThat(committed.get() + caughtExceptions.get()).isEqualTo(TOT * CONCURRENT_THREADS);
  }

  @Test
  void deleteOverwriteCompositeKeyInTx() {
    database.transaction(() ->
        database.command("sqlscript", """
            CREATE VERTEX TYPE zone;
            CREATE PROPERTY zone.id STRING;
            CREATE VERTEX TYPE device;
            CREATE PROPERTY device.id STRING;
            CREATE EDGE TYPE zone_device;
            CREATE PROPERTY zone_device.from_id STRING;
            CREATE PROPERTY zone_device.to_id STRING;
            CREATE INDEX ON zone_device (from_id, to_id) UNIQUE;
            CREATE VERTEX zone SET id='zone1';
            CREATE VERTEX zone SET id='zone2';
            CREATE VERTEX device SET id='device1';
            CREATE EDGE zone_device FROM (SELECT FROM zone WHERE id='zone1') TO (SELECT FROM device WHERE id='device1') SET from_id='zone1', to_id='device1';
            """));

    database.transaction(() -> {
      database.command("sqlscript", """
          DELETE FROM zone_device WHERE from_id='zone1' and to_id='device1';
          CREATE EDGE zone_device FROM (SELECT FROM zone WHERE id='zone2') TO (SELECT FROM device WHERE id='device1') SET from_id='zone2', to_id='device1';
          CREATE EDGE zone_device FROM (SELECT FROM zone WHERE id='zone1') TO (SELECT FROM device WHERE id='device1') SET from_id='zone1', to_id='device1';
          """);
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
    assertThat(dbNotClosedCaught.get()).isTrue();
  }

  private void verifyWALFilesAreStillPresent() {
    final File dbDir = new File(getDatabasePath());
    assertThat(dbDir.exists()).isTrue();
    assertThat(dbDir.isDirectory()).isTrue();
    final File[] files = dbDir.listFiles((dir, name) -> name.endsWith("wal"));
    assertThat(files.length > 0).isTrue();
  }

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
}
