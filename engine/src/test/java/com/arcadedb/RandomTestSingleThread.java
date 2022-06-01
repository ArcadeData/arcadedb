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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;
import performance.PerformanceTest;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class RandomTestSingleThread extends TestHelper {
  private static final int CYCLES           = 1500;
  private static final int STARTING_ACCOUNT = 100;
  private static final int PARALLEL         = 4;

  private final AtomicLong otherErrors = new AtomicLong();
  private final AtomicLong mvccErrors  = new AtomicLong();
  private final Random     rnd         = new Random();

  @Test
  public void testRandom() {
    LogManager.instance().log(this, Level.INFO, "Executing " + CYCLES + " transactions");

    PerformanceTest.clean();
    createSchema();
    populateDatabase();

    long begin = System.currentTimeMillis();

    try {
      database.begin();

      for (int i = 0; i < CYCLES; ++i) {
        try {

          final int op = rnd.nextInt(6);

          LogManager.instance().log(this, Level.INFO, "Operation %d %d/%d", op, i, CYCLES);

          switch (op) {
          case 0:
          case 1:
          case 2:
            createTransactions(database);
            break;
          case 3:
            deleteRecords(database);
            break;
          case 4:
            // RANDOM PAUSE
            Thread.sleep(rnd.nextInt(100));
            break;
          case 5:
            LogManager.instance().log(this, Level.INFO, "Committing...");
            database.commit();
            database.begin();
            break;
          }

        } catch (Exception e) {
          if (e instanceof ConcurrentModificationException) {
            mvccErrors.incrementAndGet();
          } else {
            otherErrors.incrementAndGet();
            LogManager.instance().log(this, Level.SEVERE, "UNEXPECTED ERROR: " + e, e);
          }
        }
      }

      database.commit();

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();

      //System.out.println(
      //    "Test finished in " + (System.currentTimeMillis() - begin) + "ms, mvccExceptions=" + mvccErrors.get() + " otherExceptions=" + otherErrors.get());
    }

    //LogManager.instance().flush();
    //System.out.flush();
    //System.out.println("----------------");
  }

  private void createTransactions(Database database) {
    final int txOps = rnd.nextInt(100);

    //LogManager.instance().log(this, Level.INFO, "Creating %d transactions...", txOps);

    for (long txId = 0; txId < txOps; ++txId) {
      final MutableDocument tx = database.newVertex("Transaction");
      tx.set("uuid", UUID.randomUUID().toString());
      tx.set("date", new Date());
      tx.set("amount", rnd.nextInt(STARTING_ACCOUNT));
      tx.save();
    }
  }

  private void deleteRecords(Database database) {
    LogManager.instance().log(this, Level.INFO, "Deleting records...");

    final Iterator<Record> iter = database.iterateType("Account", true);

    while (iter.hasNext() && rnd.nextInt(10) != 0) {
      final Record next = iter.next();

      if (rnd.nextInt(2) == 0) {
        database.deleteRecord(next);
        LogManager.instance().log(this, Level.INFO, "Deleted record %s", next.getIdentity());
      }
    }
  }

  private void populateDatabase() {

    long begin = System.currentTimeMillis();

    database.begin();

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
      LogManager.instance().log(this, Level.INFO, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {

    if (!database.getSchema().existsType("Account")) {
      database.begin();

      final VertexType accountType = database.getSchema().createVertexType("Account", PARALLEL);
      accountType.createProperty("id", Long.class);
      accountType.createProperty("name", String.class);
      accountType.createProperty("surname", String.class);
      accountType.createProperty("registered", Date.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", "id");

      final VertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
      txType.createProperty("uuid", String.class);
      txType.createProperty("date", Date.class);
      txType.createProperty("amount", BigDecimal.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Transaction", "uuid");

      final EdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
      edgeType.createProperty("date", Date.class);

      database.commit();
    }
  }
}
