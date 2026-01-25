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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for issue #1509: Index iterators could return records that don't match the filtered condition
 * when concurrent modifications are executed.
 */
class IndexIteratorIsolationTest extends TestHelper {
  private static final int     TOTAL_RECORDS = 100;
  private static final int     WORKERS       = 8;
  private static final int     ITERATIONS    = 1000;
  private final        AtomicLong uuid          = new AtomicLong();
  private final        AtomicLong errors        = new AtomicLong();

  @Test
  void testIndexIteratorIsolation() {
    LogManager.instance().log(this, Level.SEVERE, "Creating schema and populating database...");
    createSchema();
    populateDatabase();

    LogManager.instance().log(this, Level.SEVERE, "Starting concurrent test with %d workers...", WORKERS);

    final Thread[] threads = new Thread[WORKERS];
    final AtomicLong totalQueries = new AtomicLong();

    for (int i = 0; i < WORKERS; ++i) {
      final int threadId = i;
      threads[i] = new Thread(() -> {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
          try {
            database.begin();
            try {
              // Insert a new record
              final MutableDocument tx = database.newVertex("Transaction");
              final long newUuid = uuid.incrementAndGet();
              tx.set("uuid", newUuid);
              tx.set("data", "data-" + newUuid);
              tx.save();

              // Query for a random existing UUID
              final Map<String, Object> map = new HashMap<>();
              final long randomUUID = (long) (Math.random() * TOTAL_RECORDS) + 1;
              map.put(":uuid", randomUUID);

              final ResultSet result = database.command("SQL", "select from Transaction where uuid = :uuid", map);
              while (result.hasNext()) {
                final Result record = result.next();
                final Long foundUuid = (Long) record.getProperty("uuid");

                if (!foundUuid.equals(randomUUID)) {
                  errors.incrementAndGet();
                  LogManager.instance()
                      .log(this, Level.SEVERE, "ISOLATION ERROR: Looking for %d but found %d (threadId=%d iteration=%d)",
                          randomUUID, foundUuid, threadId, iteration);
                }
                assertThat(foundUuid).isEqualTo(randomUUID);
              }

              totalQueries.incrementAndGet();
              database.commit();
            } catch (final Exception e) {
              database.rollback();
              LogManager.instance().log(this, Level.SEVERE, "Error in thread " + threadId, e);
            }
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Unexpected error in thread " + threadId, e);
          }
        }
      });
      threads[i].start();
    }

    for (int i = 0; i < WORKERS; ++i) {
      try {
        threads[i].join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    }

    LogManager.instance()
        .log(this, Level.SEVERE, "Test completed: totalQueries=%d errors=%d", totalQueries.get(), errors.get());
    assertThat(errors.get()).isEqualTo(0);
  }

  private void createSchema() {
    database.begin();
    if (!database.getSchema().existsType("Transaction")) {
      final VertexType txType = database.getSchema().buildVertexType().withName("Transaction").withTotalBuckets(4).create();
      txType.createProperty("uuid", Long.class);
      txType.createProperty("data", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Transaction", new String[] { "uuid" }, 500000);
    }
    database.commit();
  }

  private void populateDatabase() {
    database.begin();
    for (long row = 1; row <= TOTAL_RECORDS; ++row) {
      final MutableDocument record = database.newVertex("Transaction");
      record.set("uuid", row);
      record.set("data", "initial-data-" + row);
      record.save();
      uuid.set(row);
    }
    database.commit();
  }
}
