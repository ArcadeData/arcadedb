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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Times {@code (SEARCH_INDEX(a) = true OR SEARCH_INDEX(b) = true) AND @rid IN :rids ORDER BY $score DESC LIMIT 300} against the
 * same statement without the RID filter and against the literal-list form, on a Zipf corpus with two FULL_TEXT indexes, with
 * {@code :rids} bound as RIDs and as the RIDs' text (what a remote client binds, issue #13283), and checks the filtered answer
 * against the filtered complete ranking. Run by hand:
 * {@code mvn -o -pl engine test -Dtest=SearchIndexRidFilterBenchmark -Dgroups=benchmark -DexcludedGroups= -Dridfilter.docs=250000}.
 */
@Tag("benchmark")
class SearchIndexRidFilterBenchmark extends TestHelper {
  private static final int DOCS  = Integer.getInteger("ridfilter.docs", 250_000);
  private static final int RIDS  = Integer.getInteger("ridfilter.rids", 5_000);
  private static final int LIMIT = 300;
  private static final int VOCAB = 20_000;
  private static final int TEXT_WORDS = Integer.getInteger("ridfilter.textWords", 30);
  private static final int BUCKETS = Integer.getInteger("ridfilter.buckets", 16);

  private static final String PREDICATE = "(SEARCH_INDEX('Chunk[text]', :q) = true OR SEARCH_INDEX('Chunk[questions_text]', :q) = true)";
  private static final String TAIL      = " ORDER BY $score DESC LIMIT " + LIMIT;

  @Test
  void ridFilterAgainstNoFilter() {
    final Random rng = new Random(11);
    final String[] vocab = new String[VOCAB];
    for (int i = 0; i < VOCAB; i++)
      vocab[i] = "t" + Integer.toString(i, 36);
    final double[] cum = new double[VOCAB];
    double s = 0;
    for (int r = 1; r <= VOCAB; r++) {
      s += 1.0 / r;
      cum[r - 1] = s;
    }

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Chunk BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Chunk.external_id STRING");
      database.command("sql", "CREATE PROPERTY Chunk.text STRING");
      database.command("sql", "CREATE PROPERTY Chunk.questions_text STRING");
      database.command("sql", "CREATE INDEX ON Chunk (text) FULL_TEXT");
      database.command("sql", "CREATE INDEX ON Chunk (questions_text) FULL_TEXT");
    });

    final long t0 = System.nanoTime();
    final double total = cum[VOCAB - 1];
    for (int batch = 0; batch < DOCS; batch += 1000) {
      final int from = batch;
      database.transaction(() -> {
        for (int i = from; i < Math.min(DOCS, from + 1000); i++)
          database.newDocument("Chunk").set("external_id", "c" + i).set("text", words(rng, vocab, cum, total, TEXT_WORDS))
              .set("questions_text", words(rng, vocab, cum, total, 10)).save();
      });
    }
    System.out.printf("loaded %d docs in %d ms%n", DOCS, (System.nanoTime() - t0) / 1_000_000);

    final List<RID> all = new ArrayList<>(DOCS);
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Chunk")) {
      while (rs.hasNext())
        all.add(rs.next().<RID>getProperty("rid"));
    }
    Collections.shuffle(all, new Random(5));
    final List<RID> rids = new ArrayList<>(all.subList(0, Math.min(RIDS, all.size())));
    final Set<RID> ridSet = new HashSet<>(rids);
    // what a remote client binds: JSON and protobuf parameters carry no RID type, so the RIDs arrive as their "#bucket:position" text
    final List<String> ridTexts = new ArrayList<>(rids.size());
    for (final RID rid : rids)
      ridTexts.add(rid.toString());
    final StringBuilder literal = new StringBuilder("[");
    for (final RID rid : rids)
      literal.append(literal.length() > 1 ? "," : "").append(rid);
    literal.append(']');

    final String a = "SELECT external_id FROM Chunk WHERE " + PREDICATE + TAIL;
    final String b = "SELECT external_id FROM Chunk WHERE " + PREDICATE + " AND @rid IN :rids" + TAIL;
    final String c = "SELECT external_id FROM " + literal + " WHERE " + PREDICATE + TAIL;
    // the oracle: every match, best first, with its RID
    final String every = "SELECT external_id, @rid AS rid, $score AS score FROM Chunk WHERE " + PREDICATE + " ORDER BY $score DESC";

    // a common term (rank 3), a mid one and a rare one
    for (final int rank : new int[] { 3, 300, 8000 }) {
      final String term = vocab[rank - 1];
      final Map<String, Object> p = Map.of("q", term);
      final Map<String, Object> pr = Map.of("q", term, "rids", rids);
      final Map<String, Object> pt = Map.of("q", term, "rids", ridTexts);
      long matches;
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Chunk WHERE SEARCH_INDEX('Chunk[text]', :q) = true", p)) {
        matches = ((Number) rs.next().getProperty("n")).longValue();
      }
      System.out.printf("%n== term %s (rank %d): %d of %d documents match in text ==%n", term, rank, matches, DOCS);
      if (rank == 3) {
        System.out.println("EXPLAIN A:\n" + database.query("sql", "EXPLAIN " + a, p).getExecutionPlan().orElseThrow().prettyPrint(0, 2));
        System.out.println("EXPLAIN B:\n" + database.query("sql", "EXPLAIN " + b, pr).getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      }

      System.out.printf("A  no @rid filter         median %9.1f ms%n", median(a, p, 7));
      System.out.printf("C  literal list           median %9.1f ms%n", median(c, p, 7));
      System.out.printf("B  @rid IN :rids (%d)   median %9.1f ms%n", rids.size(), median(b, pr, Integer.getInteger("ridfilter.bRuns", 3)));

      System.out.printf("B' @rid IN :rids as text    median %9.1f ms%n", median(b, pt, Integer.getInteger("ridfilter.bRuns", 3)));
      assertThat(ids(b, pt)).as("B' for %s", term).isEqualTo(ids(b, pr));

      // B must be the best LIMIT matches, by score then RID, among the rows of the rid set
      final List<String> expected = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", every, p)) {
        while (rs.hasNext() && expected.size() < LIMIT) {
          final Result r = rs.next();
          if (ridSet.contains(r.<RID>getProperty("rid")))
            expected.add(r.getProperty("external_id"));
        }
      }
      final List<String> actual = ids(b, pr);
      assertThat(actual).as("B for %s", term).isEqualTo(expected);
      System.out.printf("B equals the filtered complete ranking: %d rows%n", actual.size());
    }
  }

  private static String words(final Random rng, final String[] vocab, final double[] cum, final double total, final int mean) {
    final int n = mean / 2 + rng.nextInt(mean);
    final StringBuilder body = new StringBuilder();
    for (int j = 0; j < n; j++) {
      int idx = Arrays.binarySearch(cum, rng.nextDouble() * total);
      if (idx < 0)
        idx = -idx - 1;
      body.append(vocab[Math.min(idx, vocab.length - 1)]).append(' ');
    }
    return body.toString();
  }

  private double median(final String sql, final Map<String, Object> params, final int runs) {
    ids(sql, params); // warm
    final long[] t = new long[runs];
    for (int i = 0; i < runs; i++) {
      final long t0 = System.nanoTime();
      ids(sql, params);
      t[i] = System.nanoTime() - t0;
    }
    Arrays.sort(t);
    return t[runs / 2] / 1_000_000.0;
  }

  private List<String> ids(final String sql, final Map<String, Object> params) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("external_id"));
    }
    return ids;
  }
}
