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
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

public class MVCCTest extends TestHelper {
  private static final int CYCLES      = 3;
  private static final int TOT_ACCOUNT = 10000;
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
      final AtomicLong txErrors = new AtomicLong();

      database.async().onError((exception) -> {
        if (exception instanceof ConcurrentModificationException) {
          mvccErrors.incrementAndGet();
        } else {
          otherErrors.incrementAndGet();
          LogManager.instance().log(this, Level.SEVERE, "UNEXPECTED ERROR: " + exception, exception);
        }
      });

      try {
        final Random rnd = new Random();

        for (long txId = 0; txId < TOT_TX; ++txId) {
          database.async().transaction(() -> {
            final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

            assertThat(tx.getModifiedPages()).isEqualTo(0);
            assertThat(tx.getPageCounter(1)).isNull();

            final MutableDocument doc = database.newVertex("Transaction");
            doc.set("uuid", UUID.randomUUID().toString());
            doc.set("date", new Date());
            doc.set("amount", rnd.nextInt(TOT_ACCOUNT));
            doc.save();

            final IndexCursor accounts = database.lookupByKey("Account", new String[] { "id" }, new Object[] { 0 });

            assertThat(accounts.hasNext()).isTrue();

            final Identifiable account = accounts.next();

            ((MutableVertex) doc).newEdge("PurchasedBy", account, true, "date", new Date());
          }, 0, null, (err) -> txErrors.incrementAndGet());
        }

        database.async().waitCompletion();

      } finally {
        new DatabaseChecker(database).setVerboseLevel(0).check();

        assertThat(mvccErrors.get() > 0).isTrue();
        assertThat(otherErrors.get()).isEqualTo(0);
        assertThat(txErrors.get()).isEqualTo(0);

        database.drop();
        database = factory.create();
      }
    }
  }

  @Test
  public void testNoConflictOnUpdateTx() {
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

      try {
        for (long accountId = 0; accountId < TOT_ACCOUNT; ++accountId) {
          final long finalAccountId = accountId;

          final IndexCursor accounts = database.lookupByKey("Account", new String[] { "id" }, new Object[] { finalAccountId });
          assertThat(accounts.hasNext()).isTrue();
          final Vertex account = accounts.next().asVertex();

          final int slot = ((DatabaseAsyncExecutorImpl) database.async()).getSlot(account.getIdentity().getBucketId());

          database.async().transaction(() -> {
            final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
            assertThat(tx.getModifiedPages()).isEqualTo(0);

            account.modify().set("updated", true).save();
          }, 0, null, null, slot);
        }

        database.async().waitCompletion();

        assertThat((long) database.query("sql", "select count(*) as count from Account where updated = true").nextIfAvailable().getProperty("count")).isEqualTo(TOT_ACCOUNT);

      } finally {
        new DatabaseChecker(database).setVerboseLevel(0).check();

        assertThat(mvccErrors.get()).isEqualTo(0);
        assertThat(otherErrors.get()).isEqualTo(0);

        database.drop();
        database = factory.create();
      }
    }
  }

  private void populateDatabase() {
    final long begin = System.currentTimeMillis();

    try {
      database.transaction(() -> {
        for (long row = 0; row < TOT_ACCOUNT; ++row) {
          final MutableVertex vertex = database.newVertex("Account");
          vertex.set("id", row);
          vertex.set("name", "Luca" + row);
          vertex.set("surname", "Skywalker" + row);
          vertex.set("registered", new Date());
          vertex.save();
        }
      });

    } finally {
      LogManager.instance().log(this, Level.FINE, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {
    if (!database.getSchema().existsType("Account")) {
      database.begin();

      final VertexType accountType = database.getSchema().buildVertexType().withName("Account").withTotalBuckets(PARALLEL).create();
      accountType.createProperty("id", Long.class);
      accountType.createProperty("name", String.class);
      accountType.createProperty("surname", String.class);
      accountType.createProperty("registered", Date.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", new String[] { "id" }, 5000000);

      final VertexType txType = database.getSchema().buildVertexType().withName("Transaction").withTotalBuckets(PARALLEL).create();
      txType.createProperty("uuid", String.class);
      txType.createProperty("date", Date.class);
      txType.createProperty("amount", BigDecimal.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Transaction", new String[] { "uuid" }, 5000000);

      final EdgeType edgeType = database.getSchema().buildEdgeType().withName("PurchasedBy").withTotalBuckets(PARALLEL).create();
      edgeType.createProperty("date", Date.class);

      database.commit();
    }
  }
}
