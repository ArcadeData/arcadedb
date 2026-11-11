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

import com.arcadedb.GlobalConfiguration;
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

/**
 * Issue #11498: relationship inserts by concurrent writers that share some vertices. Each writer runs
 * {@code UNWIND $pairs AS p MATCH (a:N {id: p[0]}), (b:N {id: p[1]}) CREATE (a)-[:R]->(b)} as one auto-committed statement with
 * the server's retries ({@link GlobalConfiguration#TX_RETRIES}, as the HTTP handler runs it), and re-sends it when that still fails,
 * as the reporter's client did. Prints edges/s, the conflicts that reached the client and the attempts the engine threw away, for
 * 1, 2, 4 and 8 writers and for 8 writers on disjoint vertices.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue11498ConcurrentEdgeInsertBenchmark {
  private static final String PATH     = "target/databases/Issue11498ConcurrentEdgeInsertBenchmark";
  private static final String INSERT   = "UNWIND $pairs AS p MATCH (a:N {id: p[0]}), (b:N {id: p[1]}) CREATE (a)-[:R]->(b)";
  private static final int    VERTICES = 50_000;
  private static final int    BATCH    = 500;
  private static final int    BATCHES  = 400;

  @Test
  void edgesPerSecondByWriters() {
    run(4, false); // warm up
    for (int round = 0; round < 2; round++) {
      for (final int writers : new int[] { 1, 2, 4, 8 })
        run(writers, false);
      run(8, true);
    }
  }

  private void run(final int writers, final boolean disjoint) {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();

    final Database database = factory.create();
    try {
      database.command("opencypher", "CREATE INDEX FOR (n:N) ON (n.id)");
      for (int start = 0; start < VERTICES; start += 5000)
        database.command("opencypher", "UNWIND range($a, $b) AS i CREATE (:N {id: i})", "a", start, "b",
            Math.min(start + 4999, VERTICES - 1));

      final int retries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
      final AtomicInteger remaining = new AtomicInteger(BATCHES);
      final AtomicLong committed = new AtomicLong();
      final AtomicLong clientRetries = new AtomicLong();
      final AtomicLong engineAttemptsFailed = new AtomicLong();
      final Map<String, Integer> conflicts = new TreeMap<>();

      final List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int seed = w;
        threads.add(new Thread(() -> {
          final Random rnd = new Random(seed);
          while (remaining.getAndDecrement() > 0) {
            final List<List<Integer>> pairs = new ArrayList<>(BATCH);
            final int span = VERTICES / writers;
            for (int i = 0; i < BATCH; i++)
              pairs.add(disjoint ?
                  List.of(rnd.nextInt(span) * writers + seed, rnd.nextInt(span) * writers + seed) :
                  List.of(rnd.nextInt(VERTICES), rnd.nextInt(VERTICES)));

            while (true) {
              try {
                database.transaction(() -> {
                  try (final ResultSet rs = database.command("opencypher", INSERT, Map.of("pairs", pairs))) {
                    while (rs.hasNext())
                      rs.next();
                  }
                }, false, retries, null, e -> {
                  engineAttemptsFailed.incrementAndGet();
                  final String key = String.valueOf(e.getMessage()).replaceAll("\\d+", "N");
                  synchronized (conflicts) {
                    conflicts.merge(key.substring(0, Math.min(70, key.length())), 1, Integer::sum);
                  }
                });
                committed.addAndGet(BATCH);
                break;
              } catch (final NeedRetryException e) {
                clientRetries.incrementAndGet();
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

      final long stored = database.countType("R", false);
      System.out.printf(Locale.ENGLISH, "writers=%d%s edges/s=%.0f failedAttempts=%d clientRetries=%d stored=%d/%d %s%n", writers,
          disjoint ? " (DISJOINT)" : "", committed.get() / seconds, engineAttemptsFailed.get(), clientRetries.get(), stored,
          committed.get(), conflicts);
    } finally {
      database.drop();
    }
  }
}
