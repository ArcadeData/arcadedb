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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Reproduces the measurement of issue #10921: the {@code where} option of {@code vector.neighbors} got slower the more
 * selective it was, because a predicate never took the pre-filter plan of issue #6502. N vectors, M of them matching, for M
 * from 20 to N / 5: an equality on an indexed property, the same on an unindexed one, and a broad equality as the control.
 * <p>
 * Usage: {@code mvn test -pl engine -Dtest=Issue10921WherePrefilterBenchmark -DfailIfNoTests=false -DexcludedGroups=none}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue10921WherePrefilterBenchmark {
  private static final String DB_PATH     = "target/test-databases/Issue10921WherePrefilterBenchmark";
  private static final int    NUM_VECTORS = Integer.getInteger("vector.bench.numVectors", 100_000);
  private static final int    DIMENSIONS  = Integer.getInteger("vector.bench.dimensions", 128);
  private static final int[]  MATCHING    = Arrays.stream(System.getProperty("vector.bench.matching", "20,200,2000,20000").split(","))
      .mapToInt(Integer::parseInt).toArray();
  private static final int    K           = 10;
  private static final int    QUERIES     = 50;
  private static final int    WARMUP      = 5;

  @Test
  void measureWherePreFilter() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random random = new Random(7);
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
        db.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        final boolean[][] matching = new boolean[MATCHING.length][NUM_VECTORS];
        for (int m = 0; m < MATCHING.length; m++) {
          db.command("sql", "CREATE PROPERTY Doc.t" + m + " INTEGER");
          db.command("sql", "CREATE PROPERTY Doc.u" + m + " INTEGER");
          for (int c = 0; c < Math.min(MATCHING[m], NUM_VECTORS); ) {
            final int i = random.nextInt(NUM_VECTORS);
            if (!matching[m][i]) {
              matching[m][i] = true;
              c++;
            }
          }
        }
        for (int start = 0; start < NUM_VECTORS; start += 5_000) {
          final int from = start;
          db.transaction(() -> {
            for (int i = from; i < Math.min(NUM_VECTORS, from + 5_000); i++) {
              final MutableDocument doc = db.newDocument("Doc").set("embedding", vector(random));
              for (int m = 0; m < MATCHING.length; m++)
                doc.set("t" + m, matching[m][i] ? 1 : 0).set("u" + m, matching[m][i] ? 1 : 0);
              doc.save();
            }
          });
        }
        for (int m = 0; m < MATCHING.length; m++)
          db.command("sql", "CREATE INDEX ON Doc (t" + m + ") NOTUNIQUE");
        db.command("sql",
            "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { dimensions: " + DIMENSIONS + ", similarity: 'EUCLIDEAN' }");

        final float[][] queries = new float[QUERIES][];
        for (int q = 0; q < QUERIES; q++)
          queries[q] = vector(random);

        System.out.printf(Locale.ROOT, "=== Issue #10921: where pre-filter, %,d x %d, k = %d, median of %d queries ===%n", NUM_VECTORS,
            DIMENSIONS, K, QUERIES);
        for (int m = 0; m < MATCHING.length; m++) {
          final double indexed = median(db, queries, "{ where: { t" + m + ": 1 } }");
          final double unindexed = median(db, queries, "{ where: 'u" + m + " = 1' }");
          final double broad = median(db, queries, "{ where: { t" + m + ": 0 } }");
          System.out.printf(Locale.ROOT, "M = %,7d  indexed %8.2f ms  unindexed %8.2f ms  broad (indexed) %8.2f ms%n", MATCHING[m],
              indexed, unindexed, broad);
        }
      } finally {
        db.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  private static double median(final Database db, final float[][] queries, final String options) {
    final String sql = "SELECT expand(`vector.neighbors`('Doc[embedding]', :q, " + K + ", " + options + "))";
    final double[] millis = new double[queries.length];
    for (int round = -WARMUP; round < queries.length; round++) {
      final float[] query = queries[Math.max(round, 0)];
      final long start = System.nanoTime();
      try (final ResultSet rs = db.query("sql", sql, Map.of("q", query))) {
        while (rs.hasNext())
          rs.next();
      }
      if (round >= 0)
        millis[round] = (System.nanoTime() - start) / 1e6;
    }
    Arrays.sort(millis);
    return millis[millis.length / 2];
  }

  private static float[] vector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
