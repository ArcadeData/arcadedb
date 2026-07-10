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
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for arcadedb-operations#333. The user reported that OpenCypher MERGE-MERGE-CREATE
 * inserts degraded from ~50 ms to ~1000 ms after the graph grew to ~72k Account vertices and
 * ~685k Transaction edges (BucketSelectionStrategy = {@code thread}, single embedded worker
 * driven by a Gremlin/Groovy script).
 * <p>
 * Reproducing the user's exact schema and workload at engine level shows the OpenCypher
 * MERGE-MERGE-CREATE step chain is in fact stable: ~150 us/op holding flat from the first batch
 * to the last, even at 700k iterations and with a hot-merchant skew where 80% of edges land on
 * 1% of vertices. So the slowdown reported in the ticket is not in the engine path; it lives in
 * the layers above (Gremlin/Groovy bootstrap, per-message logger calls, {@code deferredResources
 * .getResource()}, etc.). This test exists to make sure we notice if the MERGE/CREATE plan
 * itself ever starts dragging.
 * <p>
 * Schema (issue #333, verbatim):
 * <pre>
 *   Account(bank STRING, number STRING)            UNIQUE_HASH index on (number)
 *   Transaction(timestamp DATETIME, amount DECIMAL, currency STRING)
 *     NOTUNIQUE indexes on (timestamp) and (amount)
 * </pre>
 * <p>
 * Cypher executed per iteration:
 * <pre>
 *   MERGE (a:Account {bank: $bank, number: $src})
 *   MERGE (b:Account {bank: $bank, number: $dest})
 *   CREATE (a)-[:Transaction {timestamp:$ts, amount:$amt, currency:$ccy}]->(b)
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class MergeInsertSlowdownTest {

  private static final String DB_PATH       = "./target/databases/test-merge-insert-slowdown";
  private static final int    ACCOUNTS_POOL = 50_000;
  private static final int    ITERATIONS    = 100_000;
  private static final int    BATCH         = 10_000;

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    // Mirror the user's schema (issue #333) verbatim, including the BucketSelectionStrategy.
    database.command("sql", "CREATE VERTEX TYPE Account IF NOT EXISTS");
    database.command("sql", "ALTER TYPE Account BucketSelectionStrategy `thread`");
    database.command("sql", "CREATE PROPERTY Account.bank IF NOT EXISTS STRING");
    database.command("sql", "CREATE PROPERTY Account.number IF NOT EXISTS STRING");

    database.command("sql", "CREATE EDGE TYPE Transaction IF NOT EXISTS");
    database.command("sql", "ALTER TYPE Transaction BucketSelectionStrategy `thread`");
    database.command("sql", "CREATE PROPERTY Transaction.amount IF NOT EXISTS DECIMAL");
    database.command("sql", "CREATE PROPERTY Transaction.timestamp IF NOT EXISTS DATETIME");
    database.command("sql", "CREATE PROPERTY Transaction.currency IF NOT EXISTS STRING");

    database.command("sql", "CREATE INDEX Account_PK IF NOT EXISTS ON Account (number) UNIQUE_HASH");
    database.command("sql", "CREATE INDEX Transaction_CK1 IF NOT EXISTS ON Transaction (timestamp) NOTUNIQUE");
    database.command("sql", "CREATE INDEX Transaction_CK2 IF NOT EXISTS ON Transaction (amount) NOTUNIQUE");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void mergeMergeCreateLatencyStaysStable() {
    final String cypher =
        """
        MERGE (a:Account {bank: $bank, number: $src}) \
        MERGE (b:Account {bank: $bank, number: $dest}) \
        CREATE (a)-[:Transaction {timestamp: $ts, amount: $amt, currency: $ccy}]->(b)""";

    final Random rnd = new Random(42);
    final long baseTs = 1779793007000L;

    final long[] windowNanos = new long[ITERATIONS / BATCH];
    int windowIdx = 0;
    long windowStart = System.nanoTime();

    // The user's workload is heavily skewed: a few merchant accounts receive most edges and
    // ordinary customers send a handful. Zipf-like draw with alpha ~1 on a pool of accounts
    // mimics that shape and forces high-degree hot vertices on the destination side.
    final int hotPool = ACCOUNTS_POOL / 100;

    for (int i = 0; i < ITERATIONS; i++) {
      // Customers (sources): roughly uniform over the full pool.
      final int srcIdx = rnd.nextInt(ACCOUNTS_POOL);
      // Merchants (destinations): 80% land on the hot 1% of accounts, the rest spread out.
      final int destIdx = rnd.nextInt(100) < 80 ? rnd.nextInt(hotPool) : rnd.nextInt(ACCOUNTS_POOL);

      final Map<String, Object> params = new HashMap<>(8);
      params.put("bank", "flash");
      params.put("src", String.format("61%08d", srcIdx));
      params.put("dest", String.format("61%08d", destIdx));
      params.put("ts", baseTs + i);
      params.put("amt", new BigDecimal(rnd.nextInt(10_000)));
      params.put("ccy", "ZAR");

      database.transaction(() -> {
        try (var rs = database.command("opencypher", cypher, params)) {
          while (rs.hasNext())
            rs.next();
        }
      });

      if ((i + 1) % BATCH == 0) {
        final long now = System.nanoTime();
        windowNanos[windowIdx++] = now - windowStart;
        windowStart = now;
      }
    }

    // Counts make sure the workload actually wrote data
    final long vertexCount = database.countType("Account", true);
    final long edgeCount = database.countType("Transaction", true);
    assertThat(edgeCount).isEqualTo(ITERATIONS);
    assertThat(vertexCount).isLessThanOrEqualTo(ACCOUNTS_POOL);

    // The first window includes JIT warmup so it is almost always the slowest. We compare the
    // tail (last quarter) average against the first-windows-after-warmup average to keep the
    // assertion stable across CI machines. A 3x guard is comfortable: in practice the ratio
    // hovers around 1x and small fluctuations from disk/GC do not move it.
    final long warmupNanos = average(windowNanos, 1, Math.min(4, windowNanos.length));
    final long tailNanos = average(windowNanos, windowNanos.length - Math.max(2, windowNanos.length / 4),
        windowNanos.length);
    final double ratio = (double) tailNanos / (double) warmupNanos;

    final StringBuilder dump = new StringBuilder("MERGE-MERGE-CREATE per-batch elapsed (").append(BATCH)
        .append(" iters/batch):\n");
    for (int w = 0; w < windowNanos.length; w++)
      dump.append("  window ").append(w).append(": ")
          .append(String.format("%.1f", windowNanos[w] / 1_000_000.0)).append(" ms\n");
    dump.append("  warmup avg = ").append(String.format("%.1f", warmupNanos / 1_000_000.0)).append(" ms, ")
        .append("tail avg = ").append(String.format("%.1f", tailNanos / 1_000_000.0)).append(" ms, ")
        .append("ratio tail/warmup = ").append(String.format("%.2fx", ratio));

    assertThat(ratio)
        .as("MERGE-MERGE-CREATE latency must not degrade with database size (see issue #333). %s", dump)
        .isLessThan(3.0);
  }

  private static long average(final long[] arr, final int fromInclusive, final int toExclusive) {
    long sum = 0;
    final int from = Math.max(0, fromInclusive);
    final int to = Math.max(from + 1, Math.min(arr.length, toExclusive));
    for (int i = from; i < to; i++)
      sum += arr[i];
    return sum / (to - from);
  }
}
