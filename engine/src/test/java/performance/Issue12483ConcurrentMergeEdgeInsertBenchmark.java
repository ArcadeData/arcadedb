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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12483: the in-process equivalent of the reporter's load. Each writer MERGEs a batch of relationships with a unique
 * {@code uuid} between random vertices, {@code UNWIND $rows AS r MATCH (a:N {id: r[0]}), (b:N {id: r[1]}) MERGE (a)-[:R {uuid: r[2]}]->(b)},
 * as one transaction, sent again when refused. Prints relationships/s and the refusals by message, for 1, 2, 4 and 8 writers, with
 * and without an index on {@code R(uuid)}: with one the existence check reads no edge list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue12483ConcurrentMergeEdgeInsertBenchmark {
  private static final String PATH     = "target/databases/Issue12483ConcurrentMergeEdgeInsertBenchmark";
  private static final String MERGE    = "UNWIND $rows AS r MATCH (a:N {id: r[0]}), (b:N {id: r[1]}) MERGE (a)-[:R {uuid: r[2]}]->(b)";
  private static final int    VERTICES = 50_000;
  private static final int    BATCH    = 500;
  private static final int    BATCHES  = 80;

  @Test
  void relationshipsPerSecondByWriters() {
    run(4, true); // warm up
    for (int round = 0; round < 2; round++)
      for (final boolean indexed : new boolean[] { true, false })
        for (final int writers : new int[] { 1, 2, 4, 8 })
          run(writers, indexed);
  }

  private void run(final int writers, final boolean indexed) {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();

    final Database database = factory.create();
    try {
      database.command("opencypher", "CREATE INDEX FOR (n:N) ON (n.id)");
      database.getSchema().createEdgeType("R");
      if (indexed)
        database.command("opencypher", "CREATE INDEX FOR ()-[e:R]-() ON (e.uuid)");
      for (int start = 0; start < VERTICES; start += 5000)
        database.command("opencypher", "UNWIND range($a, $b) AS i CREATE (:N {id: i})", "a", start, "b",
            Math.min(start + 4999, VERTICES - 1));

      final AtomicInteger next = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicLong committed = new AtomicLong();
      final AtomicLong refused = new AtomicLong();
      final Map<String, Integer> refusals = new TreeMap<>();

      final List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int seed = w;
        threads.add(new Thread(() -> {
          final Random rnd = new Random(seed);
          int batch;
          while (failure.get() == null && (batch = next.getAndIncrement()) < BATCHES) {
            final List<List<Object>> rows = new ArrayList<>(BATCH);
            for (int i = 0; i < BATCH; i++)
              rows.add(List.of(rnd.nextInt(VERTICES), rnd.nextInt(VERTICES), batch * BATCH + i));

            while (true) {
              try {
                database.transaction(() -> {
                  try (final ResultSet rs = database.command("opencypher", MERGE, Map.of("rows", rows))) {
                    while (rs.hasNext())
                      rs.next();
                  }
                }, false, 1);
                committed.addAndGet(BATCH);
                break;
              } catch (final NeedRetryException e) {
                refused.incrementAndGet();
                final String key = String.valueOf(e.getMessage()).replaceAll("\\d+", "N");
                synchronized (refusals) {
                  refusals.merge(key.substring(0, Math.min(70, key.length())), 1, Integer::sum);
                }
              } catch (final Throwable e) {
                // not a refusal: the measurement is void, reported once every writer has stopped
                failure.compareAndSet(null, e);
                return;
              }
            }
          }
        }));
      }

      final long start = System.nanoTime();
      for (final Thread t : threads)
        t.start();
      for (final Thread t : threads)
        try {
          t.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      final double seconds = (System.nanoTime() - start) / 1_000_000_000D;

      assertThat(failure.get()).as("a writer failed with something other than a retryable refusal").isNull();
      // a scan of the records, not the cached counter countType answers from: what was stored, outside the timed interval
      final long stored = database.query("sql", "SELECT count(@rid) AS c FROM R").next().<Number>getProperty("c").longValue();
      assertThat(committed.get()).isEqualTo((long) BATCHES * BATCH);
      assertThat(stored).isEqualTo(committed.get());
      System.out.printf(Locale.ENGLISH, "%s writers=%d relationships/s=%.0f refused=%d (%.2f per commit) stored=%d/%d %s%n",
          indexed ? "indexed  " : "unindexed", writers, committed.get() / seconds, refused.get(),
          refused.get() / (double) BATCHES, stored, committed.get(), refusals);
    } finally {
      database.drop();
    }
  }
}
