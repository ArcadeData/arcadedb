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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.data.DatasetGenerator;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * Full-text index: build time and size, then top-10 BM25 queries (one term, two terms, a phrase) and a hybrid of the one-term
 * query fused with a 10 nearest-neighbors search on a 128-dimension embedding. The query terms are words of documents that
 * exist, so the queries do not time empty results. {@code bm25-phrase} exercises the PhraseQuery parse path of the engine: the
 * engine keeps no term positions, so a phrase currently matches like the multi-term AND, in any order.
 */
public class FullTextWorkload implements Workload {
  public static final String DATABASE = "bench_fulltext";

  static final int QUERIES_PER_REP = 1_000;

  static final String BUILD  = "build";
  static final String SINGLE = "bm25-single-term";
  static final String MULTI  = "bm25-multi-term";
  static final String PHRASE = "bm25-phrase";
  static final String HYBRID = "hybrid";

  private static final String TYPE            = "Doc";
  private static final String TEXT_INDEX      = "Doc[body]";
  private static final String VECTOR_INDEX    = "Doc[embedding]";
  private static final int    WORDS_PER_DOC   = 200;
  private static final int    DIM             = 128;
  private static final int    K               = 10;
  private static final int    BATCH           = 100;
  private static final long   DOC_SEED_OFFSET = 1_000L;

  // Lucene query operators: a drawn term spelled like one would not be a term
  private static final Set<String> OPERATORS = Set.of("and", "or", "not", "to");

  // top-k by score: the ranking is what BM25 is for
  private static final String BM25_SQL =
      "SELECT id, $score AS score FROM " + TYPE + " WHERE SEARCH_INDEX('" + TEXT_INDEX + "', :q) = true ORDER BY score DESC LIMIT " + K;
  private static final String HYBRID_SQL =
      "SELECT id FROM (SELECT expand(`vector.fuse`((SELECT @rid, $score FROM " + TYPE + " WHERE SEARCH_INDEX('" + TEXT_INDEX
          + "', :q) = true ORDER BY $score DESC LIMIT " + K + "), `vector.neighbors`('" + VECTOR_INDEX + "', :v, " + K
          + "), { fusion: 'RRF', limit: " + K + " })))";

  private String[]  queryTexts;
  private float[][] queryVectors;

  static int documents(final Scale scale) {
    return switch (scale) {
      case XS -> 2_000;
      case S -> 50_000;
      case M -> 500_000;
      case L -> 2_000_000;
    };
  }

  @Override
  public String name() {
    return "fulltext";
  }

  @Override
  public List<String> cases() {
    return List.of(BUILD, SINGLE, MULTI, PHRASE, HYBRID);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    RemoteClients.ensureDatabase(ctx.server(), DATABASE);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      // the documents are inserted once; each build repetition only drops and recreates the full-text index
      if (!dataReady(db, ctx.scale())) {
        createType(db);
        insertAll(db, ctx);
      }
      if (BUILD.equals(caseName))
        return;
      // the build case may not have run in this invocation
      if (!indexReady(db, TEXT_INDEX))
        createTextIndex(db);
      if (HYBRID.equals(caseName) && !indexReady(db, VECTOR_INDEX))
        db.command("sql", "CREATE INDEX ON " + TYPE + " (embedding) LSM_VECTOR METADATA {dimensions: " + DIM + ", similarity: 'COSINE'}")
            .close();
    }
    queryTexts = queries(ctx, caseName);
    if (HYBRID.equals(caseName))
      queryVectors = new DatasetGenerator(ctx.seed() + 5).vectors(QUERIES_PER_REP, DIM);
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    queryTexts = null;
    queryVectors = null;
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) {
    return BUILD.equals(caseName) ? build(ctx) : queries(ctx, caseName, queryTexts.length, null);
  }

  /** The number of rows each of the first {@code count} queries of a case returns. */
  int[] hitCounts(final WorkloadContext ctx, final String caseName, final int count) {
    final int[] hits = new int[count];
    queries(ctx, caseName, count, hits);
    return hits;
  }

  private Measurement build(final WorkloadContext ctx) {
    final ServerUnderTest server = ctx.server();
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      if (indexReady(db, TEXT_INDEX))
        db.command("sql", "DROP INDEX `" + TEXT_INDEX + "`").close();
      server.resetPeak();
      final long start = System.nanoTime();
      createTextIndex(db);
      final double seconds = (System.nanoTime() - start) / 1e9;
      return new Measurement(List.of(Metrics.seconds("build_s", seconds), Metrics.bytes("disk_bytes", Metrics.settledDiskBytes(server, DATABASE)),
          Metrics.bytes("peak_rss_bytes", server.peakRssBytes())));
    }
  }

  private Measurement queries(final WorkloadContext ctx, final String caseName, final int count, final int[] hits) {
    final boolean hybrid = HYBRID.equals(caseName);
    final String sql = hybrid ? HYBRID_SQL : BM25_SQL;
    final LatencyRecorder latencies = new LatencyRecorder(count);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      ctx.server().resetPeak();
      for (int i = 0; i < count; i++) {
        final Map<String, Object> params = hybrid ? Map.of("q", queryTexts[i], "v", queryVectors[i]) : Map.of("q", queryTexts[i]);
        int rows = 0;
        final long t0 = System.nanoTime();
        try (final ResultSet rs = db.query("sql", sql, params)) {
          while (rs.hasNext()) {
            rs.next();
            rows++;
          }
        }
        latencies.record(System.nanoTime() - t0);
        if (hits != null)
          hits[i] = rows;
      }
    }
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("queries_per_s", "queries/s", count / (latencies.totalNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    return Measurement.of(metrics, latencies);
  }

  /** Query strings built from the words of documents that exist: the same documents the insert draws. */
  private String[] queries(final WorkloadContext ctx, final String caseName) {
    final int documents = documents(ctx.scale());
    final SplittableRandom random = new SplittableRandom(ctx.seed() + 99);
    final String[] result = new String[QUERIES_PER_REP];
    for (int i = 0; i < result.length; i++) {
      final String[] words = documentText(ctx.seed(), random.nextInt(documents)).split(" ");
      final int first = termIndex(words, random, "");
      result[i] = switch (caseName) {
        // two terms of the same document: both must match
        case MULTI -> words[first] + " AND " + words[termIndex(words, random, words[first])];
        // two adjacent words of the document, quoted: parsed as a phrase, matched as an unordered AND (no position index)
        case PHRASE -> "\"" + words[first] + " " + words[first + 1] + "\"";
        default -> words[first];
      };
    }
    return result;
  }

  // a usable term of the document (and, for a phrase, one with a usable successor), other than {@code not}
  private static int termIndex(final String[] words, final SplittableRandom random, final String not) {
    while (true) {
      final int index = random.nextInt(words.length - 1);
      if (isTerm(words[index]) && isTerm(words[index + 1]) && !words[index].equals(not))
        return index;
    }
  }

  private static boolean isTerm(final String word) {
    return word.length() >= 3 && !OPERATORS.contains(word);
  }

  static String documentText(final long seed, final int document) {
    return new DatasetGenerator(seed + DOC_SEED_OFFSET + document).text(WORDS_PER_DOC);
  }

  private static void createType(final RemoteDatabase db) {
    db.command("sql", "DROP TYPE " + TYPE + " IF EXISTS UNSAFE").close();
    db.command("sql", "CREATE DOCUMENT TYPE " + TYPE).close();
    db.command("sql", "CREATE PROPERTY " + TYPE + ".id INTEGER").close();
    db.command("sql", "CREATE PROPERTY " + TYPE + ".body STRING").close();
    db.command("sql", "CREATE PROPERTY " + TYPE + ".embedding ARRAY_OF_FLOATS").close();
  }

  private static void insertAll(final RemoteDatabase db, final WorkloadContext ctx) {
    final int documents = documents(ctx.scale());
    final StringBuilder script = new StringBuilder(BATCH * (WORDS_PER_DOC * 8 + DIM * 12));
    for (int from = 0; from < documents; from += BATCH) {
      final int to = Math.min(from + BATCH, documents);
      final float[][] embeddings = new DatasetGenerator(ctx.seed() + from).vectors(to - from, DIM);
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int i = from; i < to; i++) {
        script.append("INSERT INTO ").append(TYPE).append(" SET id = ").append(i).append(", body = '")
            .append(documentText(ctx.seed(), i)).append("', embedding = [");
        final float[] v = embeddings[i - from];
        for (int j = 0; j < v.length; j++) {
          if (j > 0)
            script.append(',');
          script.append(v[j]);
        }
        script.append("];\n");
      }
      script.append("COMMIT;");
      db.command("sqlscript", script.toString()).close();
    }
  }

  private static void createTextIndex(final RemoteDatabase db) {
    db.command("sql", "CREATE INDEX ON " + TYPE + " (body) FULL_TEXT").close();
  }

  private static boolean dataReady(final RemoteDatabase db, final Scale scale) {
    try {
      return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + TYPE) == documents(scale);
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private static boolean indexReady(final RemoteDatabase db, final String index) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM schema:indexes WHERE name = '" + index + "'")) {
      return rs.hasNext();
    } catch (final RuntimeException e) {
      return false;
    }
  }
}
