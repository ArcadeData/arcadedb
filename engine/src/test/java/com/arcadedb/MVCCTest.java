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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class MVCCTest extends TestHelper {
  private static final int CYCLES      = 3;
  private static final int TOT_ACCOUNT = 100;
  private static final int TOT_TX      = 100;
  private static final int PARALLEL    = 4;

  @Test
  public void testMVCC() {
    for (int i = 0; i < CYCLES; ++i) {
      createSchema();

      populateDatabase();

      LogManager.instance().log(this, Level.FINE, "Executing " + TOT_TX + " transactions between " + TOT_ACCOUNT + " accounts");

      database.async().setParallelLevel(PARALLEL);

      final AtomicLong otherErrors = new AtomicLong();
      final AtomicLong mvccErrors = new AtomicLong();
      database.async().onError((exception) -> {
        if (exception instanceof ConcurrentModificationException) {
          mvccErrors.incrementAndGet();
        } else {
          otherErrors.incrementAndGet();
          LogManager.instance().log(this, Level.SEVERE, "UNEXPECTED ERROR: " + exception, exception);
        }
      });

      long begin = System.currentTimeMillis();

      try {
        final Random rnd = new Random();

        for (long txId = 0; txId < TOT_TX; ++txId) {
          database.async().transaction(() -> {
            final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

            Assertions.assertTrue(tx.getModifiedPages() == 0);
            Assertions.assertNull(tx.getPageCounter(1));

            final MutableDocument doc = database.newVertex("Transaction");
            doc.set("uuid", UUID.randomUUID().toString());
            doc.set("date", new Date());
            doc.set("amount", rnd.nextInt(TOT_ACCOUNT));
            doc.save();

            final IndexCursor accounts = database.lookupByKey("Account", new String[] { "id" }, new Object[] { 0 });

            Assertions.assertTrue(accounts.hasNext());

            Identifiable account = accounts.next();

            ((MutableVertex) doc).newEdge("PurchasedBy", account, true, "date", new Date());
          }, 0);
        }

        database.async().waitCompletion();

      } finally {
        new DatabaseChecker(database).setVerboseLevel(0).check();

        Assertions.assertTrue(mvccErrors.get() > 0);
        Assertions.assertEquals(0, otherErrors.get());

        //System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms, managed mvcc exceptions " + mvccErrors.get());

        database.drop();
        database = factory.create();
      }

      //LogManager.instance().flush();
      //System.out.flush();
      //System.out.println("----------------");
    }
  }

  private void populateDatabase() {

    long begin = System.currentTimeMillis();

    try {
      database.transaction(() -> {
        for (long row = 0; row < TOT_ACCOUNT; ++row) {
          final MutableDocument record = database.newVertex("Account");
          record.set("id", row);
          record.set("name", "Luca" + row);
          record.set("surname", "Skywalker" + row);
          record.set("registered", new Date());
          record.save();
        }
      });

    } finally {
      LogManager.instance().log(this, Level.FINE, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
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

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", new String[] { "id" }, 5000000);

      final VertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
      txType.createProperty("uuid", String.class);
      txType.createProperty("date", Date.class);
      txType.createProperty("amount", BigDecimal.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Transaction", new String[] { "uuid" }, 5000000);

      final EdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
      edgeType.createProperty("date", Date.class);

      database.commit();
    }
  }
}
