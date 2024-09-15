/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ConcurrentWriteTest {
  private static final int           TOTAL                = 10_000;
  private static final int           BATCH_TX             = 1;
  private static final int           BUCKETS              = 3;
  private static final int           CONCURRENT_THREADS   = BUCKETS;
  private static final int           TX_RETRY             = CONCURRENT_THREADS * 100;
  private static final String        DATABASE_NAME        = "benchmark";
  private              AtomicLong    globalCounter        = new AtomicLong();
  private              AtomicInteger concurrentExceptions = new AtomicInteger();
  private              AtomicInteger errors               = new AtomicInteger();
  private              Database      database;

  @AfterEach
  public void endTest() {
    database.drop();
  }

  @BeforeEach
  public void beginTest() {
    if (new DatabaseFactory(DATABASE_NAME).exists())
      new DatabaseFactory(DATABASE_NAME).open().drop();
    database = new DatabaseFactory(DATABASE_NAME).create();
  }

  @Test
  public void checkConcurrentInsertWithHighConcurrencyOnSamePage() {
    database.command("sql", "create vertex type User buckets " + BUCKETS);
    database.command("sql", "create property User.id long");
    database.command("sql", "alter type User BucketSelectionStrategy `thread`");

    // SPAWN ALL THE THREADS
    final Thread[] threads = new Thread[CONCURRENT_THREADS];
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      int finalI = i;
      threads[i] = new Thread(() -> executeInThread(finalI));
      threads[i].start();
    }

    // WAIT FOR ALL THE THREADS
    for (int i = 0; i < CONCURRENT_THREADS; i++)
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        // IGNORE IT
      }

    long totalRecordsOnClusters = 0L;
    for (int i = 0; i < BUCKETS; i++)
      totalRecordsOnClusters += database.countBucket("User_" + i);

    List<Long> allIds = checkRecordSequence(database);

    assertThat(allIds.size()).isEqualTo(TOTAL * CONCURRENT_THREADS);

    assertThat(totalRecordsOnClusters).isEqualTo(TOTAL * CONCURRENT_THREADS);

    assertThat(database.countType("User", true)).isEqualTo(TOTAL * CONCURRENT_THREADS);
  }

  private List<Long> checkRecordSequence(final Database database) {
    final List<Long> allIds = new ArrayList<>();
    database.iterateType("User", true).forEachRemaining((a) -> allIds.add(a.getRecord().asVertex().getLong("id")));
    Collections.sort(allIds);

    int missing = 0;
    long last = -1;
    for (int i = 0; i < allIds.size(); i++) {
      final Long current = allIds.get(i);
      if (current != last + 1)
        System.out.println((++missing) + " - MISSING ID " + (last + 1) + " FOUND " + current);
      last = current;
    }
    return allIds;
  }

  private void executeInThread(final int threadId) {
    try {

      for (AtomicInteger threadCounter = new AtomicInteger(); threadCounter.get() < TOTAL; ) {
        database.transaction(() -> {
          for (int txCounter = 0; txCounter < BATCH_TX; txCounter++) {
            try {
              final int id = threadId * TOTAL + (threadCounter.get() + txCounter);
              final MutableVertex user = database.newVertex("User").set("id", id);
              user.save();
            } catch (Throwable t) {
              incrementError(t);
            }
          }
        }, false, TX_RETRY, null, (e) -> {
          if (e instanceof ConcurrentModificationException)
            concurrentExceptions.incrementAndGet();
          else {
            incrementError(e);
          }
        });

        threadCounter.addAndGet(BATCH_TX);
        globalCounter.addAndGet(BATCH_TX);
      }
    } catch (Throwable t) {
      incrementError(t);
    }
  }

  private void incrementError(final Throwable t) {
    t.printStackTrace();
    errors.incrementAndGet();
  }
}
