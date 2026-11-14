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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Cypher shape of issue #11713: {@code MATCH (h)<-[:TRANSFERS]-(s) RETURN s.number} while other transactions add sources and
 * {@code DETACH DELETE} them. A reader may fail with a retryable conflict, never with a {@code RecordNotFoundException}: an entry
 * whose source vertex a concurrent commit deleted is an edge that commit removed as well, so it is skipped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class CypherMatchDuringConcurrentDetachDeleteTest extends TestHelper {
  private static final int  INITIAL = 600;
  private static final long RUN_MS  = 6_000;

  @Test
  void readersNeverSeeARecordNotFound() throws Exception {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Account");
      database.command("sql", "CREATE PROPERTY Account.number INTEGER");
      database.command("sql", "CREATE INDEX ON Account (number) UNIQUE");
      database.command("sql", "CREATE EDGE TYPE TRANSFERS");
    });
    database.transaction(() -> database.command("opencypher", "CREATE (:Account {number: 0})"));
    final List<Integer> numbers = new ArrayList<>();
    for (int i = 1; i <= INITIAL; i++)
      numbers.add(i);
    database.transaction(() -> database.command("opencypher",
        "MATCH (h:Account {number: 0}) UNWIND $ns AS n CREATE (:Account {number: n})-[:TRANSFERS]->(h)", Map.of("ns", numbers)));

    final long stop = System.currentTimeMillis() + RUN_MS;
    final AtomicInteger next = new AtomicInteger(INITIAL + 1);
    final AtomicInteger toDelete = new AtomicInteger(1);
    final AtomicInteger reads = new AtomicInteger();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();

    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < 4; t++) {
      threads.add(new Thread(() -> {
        while (System.currentTimeMillis() < stop)
          run(failures, () -> database.command("opencypher", "MATCH (h:Account {number: 0}) CREATE (:Account {number: $n})-[:TRANSFERS]->(h)",
              Map.of("n", next.getAndIncrement())).close());
      }, "adder-" + t));
      threads.add(new Thread(() -> {
        while (System.currentTimeMillis() < stop)
          run(failures, () -> database.command("opencypher", "MATCH (s:Account {number: $n}) DETACH DELETE s",
              Map.of("n", toDelete.getAndAdd(2))).close());
      }, "remover-" + t));
      threads.add(new Thread(() -> {
        while (System.currentTimeMillis() < stop)
          run(failures, () -> {
            try (final ResultSet rs = database.query("opencypher", "MATCH (h:Account {number: 0})<-[:TRANSFERS]-(s) RETURN s.number AS n")) {
              while (rs.hasNext())
                assertThat(rs.next().<Integer>getProperty("n")).isNotNull();
            }
            reads.incrementAndGet();
          });
      }, "reader-" + t));
    }
    threads.forEach(Thread::start);
    for (final Thread thread : threads)
      thread.join();

    if (!failures.isEmpty())
      throw new AssertionError(failures.size() + " non-retryable failure(s), first: " + failures.getFirst(), failures.getFirst());
    assertThat(reads.get()).as("the readers must have run").isGreaterThan(0);
  }

  private void run(final List<Throwable> failures, final Runnable work) {
    try {
      database.transaction(work::run);
    } catch (final NeedRetryException e) {
      // CONTENTION: THE DOCUMENTED, RETRYABLE OUTCOME
    } catch (final Throwable e) {
      failures.add(e);
    }
  }
}
