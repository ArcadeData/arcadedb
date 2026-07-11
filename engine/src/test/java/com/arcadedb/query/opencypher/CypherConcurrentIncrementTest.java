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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5227: concurrent {@code SET a.counter = a.counter + 1} on the same vertex must not
 * silently lose updates.
 * <p>
 * The lost update was introduced by #5190, which moved the SET right-hand-side evaluation ahead of the record's
 * reload-for-write. The right-hand side was then computed from the (possibly stale) snapshot the MATCH loaded, so
 * a concurrent transaction that committed in the read-to-write window was silently overwritten with no conflict
 * reported. The fix re-evaluates the right-hand side against the record's latest committed state (restoring the
 * pre-#5190 read-after-reload behaviour) while keeping #5190's simultaneous-assignment snapshot semantics.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherConcurrentIncrementTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-concurrent-increment-test");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void concurrentIncrementDoesNotLoseUpdates() throws InterruptedException {
    // A few rounds keep the (probabilistic) regression reliably caught: pre-fix a single round already loses
    // updates the large majority of the time, so several independent rounds make a false pass vanishingly unlikely.
    for (int round = 0; round < 5; round++)
      runIncrementRound();
  }

  private void runIncrementRound() throws InterruptedException {
    database.transaction(() -> database.command("cypher", "MATCH (a:Cnt {id:1}) DETACH DELETE a"));
    database.transaction(() -> database.command("cypher", "CREATE (a:Cnt {id:1, counter:0})"));

    final int threads = 8;
    final int iterations = 100;

    final AtomicLong successCount = new AtomicLong();
    final AtomicLong otherErrors = new AtomicLong();

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final Thread worker = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (int i = 0; i < iterations; i++) {
          try {
            database.transaction(() -> {
              try (final ResultSet rs = database.command("cypher",
                  "MATCH (a:Cnt {id:1}) SET a.counter = a.counter + 1 RETURN a.counter AS value")) {
                assertThat(rs.hasNext()).isTrue();
                rs.next();
              }
            });
            // The transaction() helper retries on conflict and commits: reaching here is a real applied increment.
            successCount.incrementAndGet();
          } catch (final NeedRetryException e) {
            // Retries exhausted: not counted as a success, so it must not be reflected in the final value either.
          } catch (final Exception e) {
            otherErrors.incrementAndGet();
          }
        }
        done.countDown();
      }, "increment-worker-" + t);
      worker.start();
    }

    start.countDown();
    done.await();

    final long finalValue;
    try (final ResultSet rs = database.query("cypher", "MATCH (a:Cnt {id:1}) RETURN a.counter AS value")) {
      finalValue = ((Number) rs.next().getProperty("value")).longValue();
    }

    assertThat(otherErrors.get()).as("unexpected non-conflict errors").isEqualTo(0);
    assertThat(finalValue)
        .as("every acknowledged (non-conflict) increment must be reflected in the final counter")
        .isEqualTo(successCount.get());
  }
}
