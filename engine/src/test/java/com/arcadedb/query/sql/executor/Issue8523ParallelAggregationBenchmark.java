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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;

/**
 * Issue #8523: the report's TPC-H-shaped aggregations timed with parallel scans on and off, on a type with one bucket
 * and on one with four. The timings are logged at INFO: run it with a log configuration that shows INFO for
 * {@code com.arcadedb} to read them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue8523ParallelAggregationBenchmark extends TestHelper {
  private static final int ROWS = 1_000_000;
  private static final int REPS = 5;

  private static final String[][] QUERIES = {
      { "q1", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
          + "sum(l_extendedprice * (1 - l_discount)) AS sum_disc, avg(l_quantity) AS avg_qty, count(*) AS n FROM %s "
          + "WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus" },
      { "q6", "SELECT sum(l_extendedprice * l_discount) AS revenue, count(*) AS n FROM %s WHERE l_shipdate >= '1994-01-01' "
          + "AND l_shipdate < '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24" },
      { "top_parts", "SELECT l_partkey, sum(l_extendedprice * (1 - l_discount)) AS rev FROM %s GROUP BY l_partkey "
          + "ORDER BY rev DESC, l_partkey ASC LIMIT 10" },
      { "filter_rows", "SELECT l_partkey FROM %s WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-02-01'" } };

  @Override
  protected void beginTest() {
    load("OneBucket", 1);
    load("FourBuckets", 4);
  }

  private void load(final String type, final int buckets) {
    database.getSchema().createDocumentType(type, buckets);
    final Random rnd = new Random(1);
    for (int b = 0; b < ROWS; b += 50_000) {
      final int from = b;
      database.transaction(() -> {
        for (int i = from; i < from + 50_000; i++)
          database.newDocument(type).set("l_partkey", (long) (1 + rnd.nextInt(ROWS / 30)), "l_quantity", (double) (1 + rnd.nextInt(50)),
              "l_extendedprice", rnd.nextInt(10_000_000) / 100.0, "l_discount", rnd.nextInt(11) / 100.0,
              "l_returnflag", String.valueOf("ARN".charAt(rnd.nextInt(3))), "l_linestatus", String.valueOf("FO".charAt(rnd.nextInt(2))),
              "l_shipdate", String.format("199%d-%02d-%02d", 2 + rnd.nextInt(7), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28))).save();
      });
    }
  }

  @Test
  void compareSequentialAndParallel() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" })
      for (final String[] q : QUERIES) {
        final String query = q[1].formatted(type);
        final double sequential = time(query, false);
        final double parallel = time(query, true);
        LogManager.instance().log(this, Level.INFO, "Issue #8523 %-11s %-11s sequential %8.1f ms  parallel %8.1f ms  speed-up %4.1fx",
            null, type, q[0], sequential, parallel, sequential / parallel);
      }
  }

  private double time(final String query, final boolean parallel) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    try {
      run(query);
      final double[] t = new double[REPS];
      for (int r = 0; r < REPS; r++) {
        final long begin = System.nanoTime();
        run(query);
        t[r] = (System.nanoTime() - begin) / 1e6;
      }
      Arrays.sort(t);
      return t[REPS / 2];
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private void run(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
