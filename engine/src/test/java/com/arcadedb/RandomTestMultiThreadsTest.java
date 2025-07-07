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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

public class RandomTestMultiThreadsTest extends TestHelper {
  private static final int CYCLES           = 10000;
  private static final int STARTING_ACCOUNT = 10000;
  private static final int BUCKETS          = 4;
  private static final int WORKERS          = 4 * 8;

  private final AtomicLong                           total                   = new AtomicLong();
  private final AtomicLong                           totalTransactionRecords = new AtomicLong();
  private final AtomicLong                           mvccErrors              = new AtomicLong();
  private final Random                               rnd                     = new Random();
  private final AtomicLong                           uuid                    = new AtomicLong();
  private final List<Pair<Integer, Exception>>       otherErrors             = Collections.synchronizedList(new ArrayList<>());
  private final Database.TRANSACTION_ISOLATION_LEVEL txType                  = Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ;
  private final boolean                              explicitLocks           = true;
  private final boolean                              debug                   = false;

  @Test
  public void testRandom() {
    GlobalConfiguration.COMMIT_LOCK_TIMEOUT.setValue(10_000);

    LogManager.instance().log(this, Level.SEVERE, "Executing " + CYCLES + " transactions with %d workers", WORKERS);

    createSchema();
    populateDatabase();

    LogManager.instance().log(this, Level.SEVERE, "Start MT test with %d workers", WORKERS);

    final long begin = System.currentTimeMillis();

    try {

      final Thread[] threads = new Thread[WORKERS];
      for (int i = 0; i < WORKERS; ++i) {
        final int threadId = i;
        threads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            beginTransaction();

            long totalTransactionInCurrentTx = 0;

            while (true) {
              final long i = total.incrementAndGet();
              if (i >= CYCLES)
                break;

              try {
                final int op = getRandom(100);
                if (i % 5000 == 0)
                  LogManager.instance()
                      .log(this, Level.FINE, "Operations %d/%d totalTransactionInCurrentTx=%d totalTransactions=%d (thread=%d)", i,
                          CYCLES, totalTransactionInCurrentTx, totalTransactionRecords.get(), threadId);

                LogManager.instance().log(this, Level.FINE, "Operation %d %d/%d (thread=%d)", op, i, CYCLES, threadId);

                if (op >= 0 && op <= 19) {
                  final int txOps = getRandom(10);
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Creating %d transactions (thread=%d)...", txOps, threadId);

                  createTransactions(database, txOps);
                  totalTransactionInCurrentTx += txOps;

                } else if (op >= 20 && op <= 39) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Querying Account by index records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  final int randomId = getRandom(10000) + 1;
                  map.put(":id", randomId);

                  final ResultSet result = database.command("SQL", "select from Account where id = :id", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toJSON();
                    assertThat((Long) record.getProperty("id")).isEqualTo(randomId);
                  }

                } else if (op >= 40 && op <= 59) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Querying Transaction by index records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  final int randomUUID = getRandom((int) (totalTransactionRecords.get() + 1)) + 1;
                  map.put(":uuid", randomUUID);

                  final ResultSet result = database.command("SQL", "select from Transaction where uuid = :uuid", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toJSON();
                    if (randomUUID != (Long) record.getProperty("uuid")) {
                      LogManager.instance()
                          .log(this, Level.SEVERE, "Looking for %d but found %d (threadId=%s)", randomUUID,
                              record.getProperty("uuid"), Thread.currentThread().threadId());
                    }
                    assertThat((Long) record.getProperty("uuid")).isEqualTo(randomUUID);
                  }
                } else if (op >= 60 && op <= 64) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Scanning Account records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", getRandom(100) + 1);

                  final ResultSet result = database.command("SQL", "select from Account limit :limit", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toJSON();
                  }

                } else if (op >= 65 && op <= 69) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Scanning Transaction records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", getRandom((int) totalTransactionRecords.get() + 1) + 1);

                  final ResultSet result = database.command("SQL", "select from Transaction limit :limit", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toJSON();
                  }

                } else if (op >= 70 && op <= 74) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Deleting records (thread=%d)...", threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);
                } else if (op >= 75 && op <= 84) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Committing (thread=%d)...", threadId);
                  database.commit();

                  totalTransactionRecords.addAndGet(totalTransactionInCurrentTx);
                  totalTransactionInCurrentTx = 0;

                  beginTransaction();
                } else if (op >= 85 && op <= 94) {
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Updating records (thread=%d)...", threadId);

                  updateRecords(database, threadId);
                } else if (op >= 95 && op <= 95) {
                  // DELETE RECORDS
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Counting Transaction records (thread=%d)...", threadId);

                  final long newCounter = database.countType("Transaction", true);

                  LogManager.instance()
                      .log(this, Level.FINE, "Found %d Transaction records, ram counter=%d (thread=%d)...", newCounter,
                          totalTransactionRecords.get(), threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);

                } else if (op >= 96 && op <= 96) {
                  // DELETE RECORDS
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Counting account records (thread=%d)...", threadId);

                  final long newCounter = database.countType("Account", true);

                  LogManager.instance().log(this, Level.FINE, "Found %d Account records (thread=%d)...", newCounter, threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);
                } else if (op >= 97 && op <= 99) {
                  // JUST WAIT
                  final long ms = getRandom(299) + 1;
                  if (debug)
                    LogManager.instance().log(this, Level.SEVERE, "Sleeping %d ms (thread=%d)...", ms, threadId);
                  Thread.sleep(ms);
                }

              } catch (final Exception e) {
                if (e instanceof ConcurrentModificationException) {
                  mvccErrors.incrementAndGet();
                  total.decrementAndGet();
                  totalTransactionInCurrentTx = 0;
                } else {
                  otherErrors.add(new Pair<>(threadId, e));
                  LogManager.instance().log(this, Level.SEVERE, "UNEXPECTED ERROR: " + e, e);
                }

                if (!database.isTransactionActive()) {
                  beginTransaction();
                }
              }
            }

            try {
              database.commit();
            } catch (final Exception e) {
              mvccErrors.incrementAndGet();
            }

          }
        });
        threads[i].start();
      }

      //LogManager.instance().flush();
      //System.out.flush();
      //System.out.println("----------------");

      for (int i = 0; i < WORKERS; ++i) {
        try {
          threads[i].join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
        }
      }

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();

      //System.out.println(
      //     "Test finished in " + (System.currentTimeMillis() - begin) + "ms, mvccExceptions=" + mvccErrors.get() + " otherExceptions=" + otherErrors.size());

      for (final Pair<Integer, Exception> entry : otherErrors) {
        //System.out.println(" = threadId=" + entry.getFirst() + " exception=" + entry.getSecond());
      }
    }
  }

  private void beginTransaction() {
    database.begin(txType);
    if (explicitLocks && getRandom(2) == 0) // 50% CHANCES
      database.acquireLock().type("Account").type("Transaction").type("PurchasedBy").lock();
  }

  private void createTransactions(final Database database, final int txOps) {
    for (long txId = 0; txId < txOps; ++txId) {
      final MutableDocument tx = database.newVertex("Transaction");
      tx.set("uuid", uuid.getAndIncrement());
      tx.set("date", new Date());
      tx.set("amount", getRandom(STARTING_ACCOUNT));
      tx.save();
    }
  }

  private int updateRecords(final Database database, final int threadId) {
    if (totalTransactionRecords.get() == 0)
      return 0;

    final Iterator<Record> iter = database.iterateType("Transaction", true);

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = getRandom(((int) totalTransactionRecords.get() + 1) / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int updated = 0;

    while (iter.hasNext() && getRandom(10) != 0) {
      final Record next = iter.next();

      if (getRandom(2) == 0) {
        try {
          final MutableDocument doc = ((Document) next).modify();

          Integer val = (Integer) doc.get("updated");
          if (val == null)
            val = 0;
          doc.set("updated", val + 1);

          if (getRandom(2) == 1)
            doc.set("longFieldUpdated", "This is a long field to test the break of pages");

          doc.save();

          updated++;

        } catch (final RecordNotFoundException e) {
          // OK
        }
        LogManager.instance().log(this, Level.FINE, "Updated record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return updated;
  }

  private int deleteRecords(final Database database, final int threadId) {
    if (totalTransactionRecords.get() == 0)
      return 0;

    final Iterator<Record> iter = database.iterateType("Transaction", true);

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = getRandom(((int) totalTransactionRecords.get() + 1) / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int deleted = 0;

    while (iter.hasNext() && getRandom(20) != 0) {
      final Record next = iter.next();

      if (getRandom(6) != 0) {
        try {
          database.deleteRecord(next);
          deleted++;
        } catch (final RecordNotFoundException e) {
          // OK
        }
        //LogManager.instance().log(this, Level.FINE, "Deleted record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return deleted;
  }

  private int getRandom(int bound) {
    if (bound < 1)
      bound = 1;
    return rnd.nextInt(bound);
  }

  private void populateDatabase() {

    final long begin = System.currentTimeMillis();

    beginTransaction();
    try {
      for (long row = 0; row < STARTING_ACCOUNT; ++row) {
        final MutableDocument record = database.newVertex("Account");
        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("registered", new Date());
        record.save();
      }

      database.commit();

    } finally {
      LogManager.instance().log(this, Level.FINE, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {
    if (!database.getSchema().existsType("Account")) {
      database.begin(txType);

      final VertexType accountType = database.getSchema().buildVertexType().withName("Account").withTotalBuckets(BUCKETS).create();
      accountType.createProperty("id", Long.class);
      accountType.createProperty("name", String.class);
      accountType.createProperty("surname", String.class);
      accountType.createProperty("registered", Date.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", new String[] { "id" }, 500000);

      final VertexType txType = database.getSchema().buildVertexType().withName("Transaction").withTotalBuckets(BUCKETS).create();
      txType.createProperty("uuid", Long.class);
      txType.createProperty("date", Date.class);
      txType.createProperty("amount", BigDecimal.class);
      txType.createProperty("updated", Integer.class);
      txType.createProperty("longFieldUpdated", String.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Transaction", new String[] { "uuid" }, 500000);

      final EdgeType edgeType = database.getSchema().buildEdgeType().withName("PurchasedBy").withTotalBuckets(BUCKETS).create();
      edgeType.createProperty("date", Date.class);

      database.commit();
    }
  }
}
