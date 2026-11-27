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
import com.arcadedb.query.sql.executor.Result;
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
  static final String BROAD          = "bm25-broad-or";
  static final String BROAD_FILTERED = "bm25-broad-or-filtered";

  private static final String TYPE            = "Doc";
  private static final String TEXT_INDEX      = "Doc[body]";
  private static final String VECTOR_INDEX    = "Doc[embedding]";
  private static final int    WORDS_PER_DOC   = 200;
  private static final int    DIM             = 128;
  private static final int    K               = 10;
  private static final int    BATCH           = 100;
  private static final long   DOC_SEED_OFFSET = 1_000L;

  // the broad corpus: a separate type, so the history of the Doc cases keeps its baseline
  private static final String POST_TYPE          = "Post";
  private static final String POST_INDEX         = "Post[body]";
  private static final int    WORDS_PER_POST     = 10;
  private static final int    BROAD_LIMIT        = 50;
  private static final long   POST_SEED_OFFSET   = 5_000_000L;
  // one seed for both broad cases: plain and filtered run the identical query set
  private static final long   BROAD_QUERY_SEED   = 201L;
  private static final double MIN_DELETED_SHARE  = 0.08;
  private static final double MAX_DELETED_SHARE  = 0.12;
  private static final double MIN_BROAD_SHARE    = 0.35;
  private static final double MAX_BROAD_SHARE    = 0.55;
  private static final int    POSTS_S            = 100_000;
  private static final int    POST_BATCH         = 500;

  // Lucene query operators: a drawn term spelled like one would not be a term
  private static final Set<String> OPERATORS = Set.of("and", "or", "not", "to");

  // top-k by score: the ranking is what BM25 is for
  private static final String BM25_SQL =
      "SELECT id, $score AS score FROM " + TYPE + " WHERE SEARCH_INDEX('" + TEXT_INDEX + "', :q) = true ORDER BY score DESC LIMIT " + K;
  private static final String HYBRID_SQL =
      "SELECT id FROM (SELECT expand(`vector.fuse`((SELECT @rid, $score FROM " + TYPE + " WHERE SEARCH_INDEX('" + TEXT_INDEX
          + "', :q) = true ORDER BY $score DESC LIMIT " + K + "), `vector.neighbors`('" + VECTOR_INDEX + "', :v, " + K
          + "), { fusion: 'RRF', limit: " + K + " })))";

  private static final String BROAD_SQL =
      "SELECT id, deleted_at, $score AS score FROM " + POST_TYPE + " WHERE SEARCH_INDEX('" + POST_INDEX
          + "', :q) = true ORDER BY score DESC LIMIT " + BROAD_LIMIT;
  private static final String BROAD_FILTERED_SQL =
      "SELECT id, deleted_at, $score AS score FROM " + POST_TYPE + " WHERE SEARCH_INDEX('" + POST_INDEX
          + "', :q) = true AND deleted_at IS NULL ORDER BY score DESC LIMIT " + BROAD_LIMIT;

  private String[]  queryTexts;
  private float[][] queryVectors;
  private long      deletedRowsSeen;

  static int documents(final Scale scale) {
    return switch (scale) {
      case XS -> 2_000;
      case S -> 50_000;
      case M -> 500_000;
      case L -> 2_000_000;
    };
  }

  static int posts(final Scale scale) {
    return switch (scale) {
      case XS -> 2_000;
      case S -> POSTS_S;
      case M -> 300_000;
      case L -> 1_000_000;
    };
  }

  /**
   * Queries per rep of the broad cases. The 1,000-operation rule binds at S only. The filtered case costs about 0.5 ms per
   * 1,000 posts per query (S: 100k posts, 20 qps, 50 s per rep of 1,000 queries), so the counts above S keep a rep to about
   * 75 s at M (300k posts, 500 queries) and 125 s at L (1M posts, 250 queries): 7 steps of at most 2.1 min, a case of about 9 and
   * 15 min, far inside the 30 min step limit and the 120 min case cap.
   */
  static int broadQueries(final Scale scale) {
    return switch (scale) {
      case XS, S -> QUERIES_PER_REP;
      case M -> 500;
      case L -> 250;
    };
  }

  static String postBody(final long seed, final int post) {
    return new DatasetGenerator(seed + POST_SEED_OFFSET + post).broadText(WORDS_PER_POST);
  }

  @Override
  public String name() {
    return "fulltext";
  }

  @Override
  public List<String> cases() {
    return List.of(BUILD, SINGLE, MULTI, PHRASE, HYBRID, BROAD, BROAD_FILTERED);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    RemoteClients.ensureDatabase(ctx.server(), DATABASE);
    if (isBroad(caseName)) {
      prepareBroad(ctx, caseName);
      return;
    }
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

  private static boolean isBroad(final String caseName) {
    return BROAD.equals(caseName) || BROAD_FILTERED.equals(caseName);
  }

  private void prepareBroad(final WorkloadContext ctx, final String caseName) {
    deletedRowsSeen = 0;
    final int posts = posts(ctx.scale());
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      if (!postsReady(db, posts)) {
        createPostType(db);
        insertPosts(db, ctx, posts);
      }
      if (!indexReady(db, POST_INDEX))
        db.command("sql", "CREATE INDEX ON " + POST_TYPE + " (body) FULL_TEXT").close();
    }
    final SplittableRandom random = new SplittableRandom(ctx.seed() + BROAD_QUERY_SEED);
    queryTexts = new String[broadQueries(ctx.scale())];
    for (int i = 0; i < queryTexts.length; i++)
      queryTexts[i] = DatasetGenerator.broadQuery(random);
    checkBroadShare(ctx, queryTexts[0], posts);
    checkDeletedShare(ctx, posts);
  }

  // the filter only proves something if deleted_at is stored: about 10% of the posts must have it, counted on the server
  private static void checkDeletedShare(final WorkloadContext ctx, final int posts) {
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      final long deleted = RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + POST_TYPE + " WHERE deleted_at IS NOT NULL");
      final double share = (double) deleted / posts;
      if (share < MIN_DELETED_SHARE || share > MAX_DELETED_SHARE)
        throw new IllegalStateException(deleted + " of " + posts + " posts have deleted_at (" + share + "), expected about 0.10");
    }
  }

  /** The rows with deleted_at set seen by the last broad run (the plain case must see some, or the filter proves nothing). */
  long deletedRowsSeen() {
    return deletedRowsSeen;
  }

  // what the case claims to measure: a query matching about 45% of the corpus, counted on the server
  private static void checkBroadShare(final WorkloadContext ctx, final String query, final int posts) {
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      final long matches = RemoteClients.count(db, "sql",
          "SELECT count(*) AS n FROM " + POST_TYPE + " WHERE SEARCH_INDEX('" + POST_INDEX + "', '" + query + "') = true");
      final double share = (double) matches / posts;
      if (share < MIN_BROAD_SHARE || share > MAX_BROAD_SHARE)
        throw new IllegalStateException("A broad query matches " + matches + " of " + posts + " posts (" + share
            + "), expected about 0.45: '" + query + "'");
    }
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
    final boolean broad = isBroad(caseName);
    final boolean filtered = BROAD_FILTERED.equals(caseName);
    final String sql = hybrid ? HYBRID_SQL : broad ? (filtered ? BROAD_FILTERED_SQL : BROAD_SQL) : BM25_SQL;
    final LatencyRecorder latencies = new LatencyRecorder(count);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      ctx.server().resetPeak();
      for (int i = 0; i < count; i++) {
        final Map<String, Object> params = hybrid ? Map.of("q", queryTexts[i], "v", queryVectors[i]) : Map.of("q", queryTexts[i]);
        int rows = 0;
        int deletedRows = 0;
        final long t0 = System.nanoTime();
        try (final ResultSet rs = db.query("sql", sql, params)) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            // read only in the broad cases: the timed loop of the baselined cases is unchanged
            if (broad && row.getProperty("deleted_at") != null)
              deletedRows++;
            rows++;
          }
        }
        latencies.record(System.nanoTime() - t0);
        if (broad) {
          deletedRowsSeen += deletedRows;
          // a wrong answer fails the case
          if (rows != BROAD_LIMIT)
            throw new IllegalStateException(caseName + ": query '" + queryTexts[i] + "' returned " + rows + " rows, expected " + BROAD_LIMIT);
          if (filtered && deletedRows > 0)
            throw new IllegalStateException(caseName + ": query '" + queryTexts[i] + "' returned " + deletedRows + " rows with deleted_at set");
        }
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

  private static void createPostType(final RemoteDatabase db) {
    db.command("sql", "DROP TYPE " + POST_TYPE + " IF EXISTS UNSAFE").close();
    db.command("sql", "CREATE DOCUMENT TYPE " + POST_TYPE).close();
    db.command("sql", "CREATE PROPERTY " + POST_TYPE + ".id INTEGER").close();
    db.command("sql", "CREATE PROPERTY " + POST_TYPE + ".body STRING").close();
    db.command("sql", "CREATE PROPERTY " + POST_TYPE + ".deleted_at DATETIME").close();
  }

  private static void insertPosts(final RemoteDatabase db, final WorkloadContext ctx, final int posts) {
    final StringBuilder script = new StringBuilder(POST_BATCH * 120);
    for (int from = 0; from < posts; from += POST_BATCH) {
      final int to = Math.min(from + POST_BATCH, posts);
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int i = from; i < to; i++) {
        script.append("INSERT INTO ").append(POST_TYPE).append(" SET id = ").append(i).append(", body = '")
            .append(postBody(ctx.seed(), i)).append('\'');
        // null (property absent) on 90% of the rows
        if (DatasetGenerator.broadDeleted(ctx.seed(), i))
          script.append(", deleted_at = date('2026-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')");
        script.append(";\n");
      }
      script.append("COMMIT;");
      db.command("sqlscript", script.toString()).close();
    }
  }

  private static boolean postsReady(final RemoteDatabase db, final int posts) {
    try {
      return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + POST_TYPE) == posts;
    } catch (final RuntimeException e) {
      return false;
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
