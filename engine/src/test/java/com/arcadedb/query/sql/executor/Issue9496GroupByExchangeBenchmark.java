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

import com.arcadedb.TestHelper;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;

/**
 * Issue #9496: a parallel GROUP BY timed with the key exchange and without it, over a range of key counts at a fixed number
 * of rows, so from many rows per key (where a worker's own groups absorb almost every row) to a few (where every worker
 * would otherwise create and merge nearly every key). The timings are logged at INFO: run it with a log configuration that
 * shows INFO for {@code com.arcadedb} to read them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue9496GroupByExchangeBenchmark extends TestHelper {
  private static final int   ROWS = 2_000_000;
  private static final int   REPS = 5;
  private static final int[] KEYS = { 2_000, 8_000, 32_000, 128_000, 512_000 };

  private final int defaultExchangeMinGroups = AggregateProjectionCalculationStep.exchangeMinGroups;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("T", 4);
    final Random rnd = new Random(9496);
    for (int b = 0; b < ROWS; b += 50_000) {
      final int from = b;
      database.transaction(() -> {
        for (int i = from; i < from + 50_000; i++) {
          final var doc = database.newDocument("T").set("id", i, "q", rnd.nextInt(50), "p", rnd.nextInt(10_000_000) / 100.0);
          for (final int keys : KEYS)
            doc.set("k" + keys, (long) rnd.nextInt(keys));
          doc.save();
        }
      });
    }
  }

  @AfterEach
  void restoreThreshold() {
    AggregateProjectionCalculationStep.exchangeMinGroups = defaultExchangeMinGroups;
  }

  @Test
  void exchangeAgainstWorkerOnlyAggregation() {
    for (final int keys : KEYS) {
      final String query = "SELECT k" + keys + ", sum(q) AS sq, sum(p) AS sp, count(*) AS n FROM T GROUP BY k" + keys;
      // ALTERNATED, SO A DRIFT OF THE MACHINE WEIGHS ON BOTH SIDES
      final double[] with = new double[REPS];
      final double[] without = new double[REPS];
      time(query, true, 2);
      time(query, false, 2);
      for (int r = 0; r < REPS; r++) {
        with[r] = time(query, true, 1);
        without[r] = time(query, false, 1);
      }
      final double w = median(with);
      final double wo = median(without);
      LogManager.instance().log(this, Level.INFO, "Issue #9496 keys %7d (%5d rows/key) exchange %8.1f ms  without %8.1f ms  ratio %.3f",
          keys, ROWS / keys, w, wo, w / wo);
    }
  }

  private double time(final String query, final boolean exchange, final int runs) {
    AggregateProjectionCalculationStep.exchangeMinGroups = exchange ? defaultExchangeMinGroups : Integer.MAX_VALUE;
    double last = 0;
    for (int i = 0; i < runs; i++) {
      final long begin = System.nanoTime();
      long rows = 0;
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      last = (System.nanoTime() - begin) / 1_000_000.0;
      if (rows == 0)
        throw new IllegalStateException("no groups");
    }
    return last;
  }

  private static double median(final double[] values) {
    final double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
