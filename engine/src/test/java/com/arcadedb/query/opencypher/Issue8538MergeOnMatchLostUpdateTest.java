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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8538: {@code MERGE (c:C {id: $id}) ON MATCH SET c.n = c.n + 1} lost an increment under READ_COMMITTED, with
 * both transactions committing. The MERGE action evaluated the right-hand side against the record the MERGE matched,
 * and only then called {@code modify()}, which pins the vertex page and silently reloads the record when that page
 * moved on. A transaction that committed between the match and the write was therefore overwritten with a value
 * computed from the older snapshot, and the reload had already made the record image the commit checks look current.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8538MergeOnMatchLostUpdateTest {
  private static final String MERGE_ONE  = "MERGE (c:C {id: $id}) ON CREATE SET c.n = 1 ON MATCH SET c.n = c.n + 1 RETURN c.n AS n";
  private static final String MERGE_MANY = "UNWIND $ids AS id MERGE (c:C {id: id}) ON CREATE SET c.n = 1 ON MATCH SET c.n = c.n + 1 RETURN c.id AS id, c.n AS n";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-8538-merge-lost-update");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sqlscript", "CREATE VERTEX TYPE C; CREATE PROPERTY C.id STRING; CREATE INDEX ON C (id) UNIQUE;");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Deterministic interleaving: a concurrent increment commits right after the MERGE has matched the record and before
   * its ON MATCH SET runs. The MERGE must build on that commit (or fail with a retryable conflict), never write back
   * the value computed from the record it matched.
   */
  @Test
  void onMatchSetSeesTheIncrementCommittedAfterTheMatch() throws Exception {
    database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

    final Thread mergeThread = Thread.currentThread();
    final AtomicBoolean armed = new AtomicBoolean(true);
    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    final AfterRecordReadListener interleave = record -> {
      if (Thread.currentThread() == mergeThread && armed.compareAndSet(true, false)) {
        final Thread concurrent = new Thread(() -> {
          try {
            database.transaction(() -> database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c.n = c.n + 1"));
          } catch (final Throwable t) {
            concurrentFailure.set(t);
          }
        });
        concurrent.start();
        try {
          concurrent.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return record;
    };
    database.getSchema().getType("C").getEvents().registerListener(interleave);

    final AtomicInteger written = new AtomicInteger();
    boolean committed;
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try (final ResultSet rs = database.command("cypher", MERGE_ONE, Map.of("id", "c0"))) {
        written.set(((Number) rs.next().getProperty("n")).intValue());
      }
      database.commit();
      committed = true;
    } catch (final ConcurrentModificationException e) {
      // A retryable conflict is a correct outcome too: the MERGE did not commit, so it owes no increment.
      committed = false;
      if (database.isTransactionActive())
        database.rollback();
    } finally {
      database.getSchema().getType("C").getEvents().unregisterListener(interleave);
    }

    assertThat(armed.get()).as("the concurrent increment must have been interleaved").isFalse();
    assertThat(concurrentFailure.get()).isNull();

    final int finalValue = readN("c0");
    if (committed) {
      assertThat(written.get()).as("the MERGE must have built on the concurrent increment").isEqualTo(2);
      assertThat(finalValue).isEqualTo(2);
    } else
      assertThat(finalValue).isEqualTo(1);
  }

  /**
   * The reporter's shape: concurrent explicit READ_COMMITTED transactions, each running one MERGE over the same ids and
   * retried on a conflict. Every committed transaction increments every id once, and no two committed transactions
   * may write the same value to the same record.
   */
  @Test
  void concurrentMergeIncrementsAreNeverLost() throws Exception {
    final int writers = 8;
    final int batches = 100;
    final List<String> ids = new ArrayList<>();
    for (int i = 0; i < 1; i++)
      ids.add("c" + i);

    for (int round = 0; round < 3; round++) {
      database.transaction(() -> database.command("sql", "DELETE FROM C"));

      final AtomicInteger committedTx = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Map<String, List<Integer>> writtenValues = new ConcurrentHashMap<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int w = 0; w < writers; w++) {
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int b = 0; b < batches; b++) {
              for (int attempt = 0; ; attempt++) {
                final Map<String, Integer> written = new HashMap<>();
                try {
                  database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
                  try (final ResultSet rs = database.command("cypher", MERGE_MANY, Map.of("ids", ids))) {
                    while (rs.hasNext()) {
                      final Result row = rs.next();
                      written.put(row.getProperty("id"), ((Number) row.getProperty("n")).intValue());
                    }
                  }
                  database.commit();
                  committedTx.incrementAndGet();
                  for (final Map.Entry<String, Integer> e : written.entrySet())
                    writtenValues.computeIfAbsent(e.getKey(), k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(e.getValue());
                  break;
                } catch (final NeedRetryException | DuplicatedKeyException e) {
                  if (database.isTransactionActive())
                    database.rollback();
                  if (attempt > 10_000)
                    throw e;
                }
              }
            }
          } catch (final Throwable t) {
            failure.compareAndSet(null, t);
            if (database.isTransactionActive())
              database.rollback();
          }
        });
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      assertThat(failure.get()).isNull();
      assertThat(committedTx.get()).isEqualTo(writers * batches);
      for (final String id : ids) {
        assertThat(writtenValues.get(id)).as("values committed to %s", id).doesNotHaveDuplicates();
        assertThat(readN(id)).as("final n of %s", id).isEqualTo(writers * batches);
      }
    }
  }

  private int readN(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT n FROM C WHERE id = ?", id)) {
      return ((Number) rs.next().getProperty("n")).intValue();
    }
  }
}
