/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Times {@code SEARCH_INDEX ... ORDER BY $score DESC LIMIT 50} against the number of OR-ed query words on a Zipf corpus
 * (issue #12458). Run by hand: {@code mvn -o -pl engine test -Dtest=Issue12458BM25TopKBenchmark -Dgroups=benchmark}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue12458BM25TopKBenchmark extends TestHelper {
  private static final int DOCS  = Integer.getInteger("bm25.docs", 50_000);
  private static final int VOCAB = 20_000;

  @Test
  void topKAgainstMatches() {
    final Random rng = new Random(11);
    final String[] syll = { "ka", "lo", "mi", "ren", "tor", "vel", "zen", "dal", "fen", "gri", "por", "sul", "tal", "vas", "cor",
        "dun", "bex", "nor", "qui", "rha" };
    final Set<String> seen = new HashSet<>();
    final List<String> vocab = new ArrayList<>();
    while (vocab.size() < VOCAB) {
      final StringBuilder w = new StringBuilder();
      final int n = 2 + rng.nextInt(3);
      for (int i = 0; i < n; i++)
        w.append(syll[rng.nextInt(syll.length)]);
      if (seen.add(w.toString()))
        vocab.add(w.toString());
    }
    final double[] cum = new double[VOCAB];
    double s = 0;
    for (int r = 1; r <= VOCAB; r++) {
      s += 1.0 / r;
      cum[r - 1] = s;
    }

    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 16");
    database.command("sql", "CREATE PROPERTY Doc.id STRING");
    database.command("sql", "CREATE PROPERTY Doc.body STRING");
    database.command("sql", "CREATE INDEX ON Doc (body) FULL_TEXT");

    final long t0 = System.nanoTime();
    final double total = cum[VOCAB - 1];
    for (int batch = 0; batch < DOCS; batch += 1000) {
      final int from = batch;
      database.transaction(() -> {
        for (int i = from; i < Math.min(DOCS, from + 1000); i++) {
          final int n = (int) Math.max(20, Math.min(1500, Math.exp(Math.log(225) + 0.5 * rng.nextGaussian())));
          final StringBuilder body = new StringBuilder();
          for (int j = 0; j < n; j++) {
            int idx = Arrays.binarySearch(cum, rng.nextDouble() * total);
            if (idx < 0)
              idx = -idx - 1;
            body.append(vocab.get(Math.min(idx, VOCAB - 1))).append(' ');
          }
          final MutableDocument doc = database.newDocument("Doc");
          doc.set("id", "d" + i);
          doc.set("body", body.toString());
          doc.save();
        }
      });
    }
    System.out.printf("loaded %d docs in %d ms%n", DOCS, (System.nanoTime() - t0) / 1_000_000);
    if (Boolean.getBoolean("bm25.compact")) {
      // the steady state of a loaded corpus: its postings in compacted series rather than on the pages the load appended
      database.async().waitCompletion();
      for (final Index bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("Doc[body]")).getIndexesOnBuckets())
        try {
          if (((IndexInternal) bucketIndex).scheduleCompaction())
            ((IndexInternal) bucketIndex).compact();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      System.out.printf("compacted in %d ms%n", (System.nanoTime() - t0) / 1_000_000);
    }

    final Random qr = new Random(12);
    final String top = "SELECT id FROM Doc WHERE SEARCH_INDEX('Doc[body]', ?) = true ORDER BY $score DESC LIMIT 50";
    for (final int words : new int[] { 1, 2, 4, 8, 16, 31 }) {
      final List<String> qs = new ArrayList<>();
      for (int q = 0; q < 10; q++) {
        final List<String> pool = new ArrayList<>(vocab.subList(150, 4000));
        final StringBuilder sb = new StringBuilder();
        for (int w = 0; w < words; w++)
          sb.append(pool.remove(qr.nextInt(pool.size()))).append(' ');
        qs.add(sb.toString().trim());
      }
      for (final String q : qs)
        run(top, q);
      final long[] times = new long[qs.size()];
      for (int q = 0; q < qs.size(); q++) {
        final long[] three = new long[3];
        for (int r = 0; r < 3; r++) {
          final long t = System.nanoTime();
          run(top, qs.get(q));
          three[r] = System.nanoTime() - t;
        }
        Arrays.sort(three);
        times[q] = three[1];
      }
      Arrays.sort(times);
      long matches;
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Doc WHERE SEARCH_INDEX('Doc[body]', ?) = true",
          qs.get(qs.size() / 2))) {
        matches = ((Number) rs.next().getProperty("n")).longValue();
      }
      FullTextTopKSearch.resetExactScoresComputedForTesting();
      run(top, qs.get(qs.size() / 2));
      final long exact = FullTextTopKSearch.getExactScoresComputedForTesting();
      System.out.printf("words=%2d matches~%7d scored exactly=%5d top50 median=%.2f ms%n", words, matches, exact,
          times[times.length / 2] / 1_000_000.0);
      // A profiler attached to the run sees the search rather than the load
      for (int r = 0; r < Integer.getInteger("bm25.profileRepeats", 0); r++)
        for (final String q : qs)
          run(top, q);
    }
    System.out.printf("queries done in %d ms%n", (System.nanoTime() - t0) / 1_000_000);
  }

  private void run(final String sql, final String q) {
    try (final ResultSet rs = database.query("sql", sql, q)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
