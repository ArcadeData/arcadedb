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
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Correctness next to the numbers: three cases that change the data and then ask the server a question whose answer the client
 * knows. A wrong answer fails the case, a right one reports how long the check took ({@code check_s}, the whole repetition:
 * the changes and the questions), so a speed-up that costs the answer never looks like an improvement.
 * <ul>
 * <li>{@code vector-recall-after-churn}: a share of the vectors is deleted and inserted again with new embeddings, then the
 * recall@10 of the index is measured against a brute force over the live set, under the gate of {@link VectorWorkload}.</li>
 * <li>{@code index-vs-scan-count}: a share of the rows is deleted and inserted again, then each predicate is counted through
 * an index, through a scan of an unindexed copy of the same column, and by the ledger the client keeps; all three must agree.</li>
 * <li>{@code count-star-vs-iterated}: rows are deleted and inserted, then {@code count(*)}, the rows of a query read to the end
 * and the ledger must agree.</li>
 * </ul>
 * Each case seeds its own data from the run seed plus a constant of its own, so a filtered run measures what a full run does.
 */
public class CorrectnessWorkload implements Workload {
  public static final String DATABASE = "bench_correctness";

  static final String VECTOR_RECALL   = "vector-recall-after-churn";
  static final String INDEX_VS_SCAN   = "index-vs-scan-count";
  static final String COUNT_VS_ITERATED = "count-star-vs-iterated";

  /** The values of {@code v} are drawn from {@code [0, DOMAIN)}, which the predicates assume. */
  static final int DOMAIN             = 1_000;
  /** One row in this many is deleted and inserted again by a repetition of the item cases (5%). */
  static final int ITEM_CHURN_EVERY   = 20;
  /** One vector in this many is deleted and inserted again by a repetition (10%). */
  static final int VECTOR_CHURN_EVERY = 10;

  private static final String ITEM_TYPE   = "Item";
  private static final String VECTOR_TYPE = "ChurnVec";
  private static final int    DIM         = 128;
  private static final int    K           = 10;
  private static final int    QUERIES     = 1_000;
  private static final int    ROW_BATCH   = 500;
  private static final int    VECTOR_BATCH = 200;
  private static final int    DELETE_BATCH = 1_000;
  private static final int    BUCKET_ROWS  = 500_000;

  // seed offsets, one block per case
  private static final long VECTOR_SEED = 100;
  private static final long INDEX_SEED  = 200;
  private static final long COUNT_SEED  = 300;

  private static final Pattern EQUALS  = Pattern.compile("v = (\\d+)");
  private static final Pattern BETWEEN = Pattern.compile("v BETWEEN (\\d+) AND (\\d+)");
  private static final Pattern IN      = Pattern.compile("v IN \\[([\\d, ]+)]");
  private static final Pattern GREATER = Pattern.compile("v > (\\d+)");

  // vector case
  private float[][]                        base;
  private float[][]                        queries;
  private DatasetGenerator.EmbeddingSpace  space;
  // item cases
  private long[]                           ledger;
  private long                             low;
  private long                             high;
  // all cases
  private int                              rep;
  private long                             seed;

  /**
   * Sizes under the S rule (a timed case takes at least 5 s per repetition), see the PR for how they were chosen. Items: the
   * rows of a repetition cost a delete and an insert of 5% of them plus a scan of all of them per predicate and per check.
   */
  static int items(final Scale scale) {
    return switch (scale) {
      case XS -> 20_000;
      case S -> 3_000_000;
      case M -> 20_000_000;
      case L -> 100_000_000;
    };
  }

  /** Vectors: a repetition inserts 10% of them again and runs {@value #QUERIES} queries, each checked by a full brute force. */
  static int vectors(final Scale scale) {
    return switch (scale) {
      case XS -> 2_000;
      case S -> 80_000;
      case M -> 200_000;
      case L -> 1_000_000;
    };
  }

  /** The predicates of the index-vs-scan case, as conditions on {@code v}; each matches some rows and not all. */
  static List<String> predicates() {
    return List.of("v = 500", "v BETWEEN 100 AND 250", "v IN [3, 250, 499, 750, 997]", "v > 900");
  }

  /** The predicate on another field: the conditions all start with {@code v}. */
  static String on(final String predicate, final String field) {
    return field + predicate.substring(1);
  }

  /** The Java mirror of {@link #predicates()}: how many of {@code values} satisfy {@code predicate}. */
  static long expectedMatches(final String predicate, final long[] values) {
    Matcher m = EQUALS.matcher(predicate);
    if (m.matches()) {
      final long k = Long.parseLong(m.group(1));
      return Arrays.stream(values).filter(v -> v == k).count();
    }
    m = BETWEEN.matcher(predicate);
    if (m.matches()) {
      final long a = Long.parseLong(m.group(1));
      final long b = Long.parseLong(m.group(2));
      return Arrays.stream(values).filter(v -> v >= a && v <= b).count();
    }
    m = IN.matcher(predicate);
    if (m.matches()) {
      final long[] set = Arrays.stream(m.group(1).split(",")).mapToLong(s -> Long.parseLong(s.trim())).toArray();
      return Arrays.stream(values).filter(v -> Arrays.stream(set).anyMatch(s -> s == v)).count();
    }
    m = GREATER.matcher(predicate);
    if (m.matches()) {
      final long c = Long.parseLong(m.group(1));
      return Arrays.stream(values).filter(v -> v > c).count();
    }
    throw new IllegalArgumentException("Unknown predicate: " + predicate);
  }

  static void checkCount(final String what, final long expected, final long got) {
    if (expected != got)
      throw new IllegalStateException(what + ": expected " + expected + ", got " + got);
  }

  static void checkRecall(final double recall, final List<Metric> metrics) throws CaseFailedWithMetrics {
    if (recall < VectorWorkload.MIN_RECALL)
      throw new CaseFailedWithMetrics("recall@10 " + recall + " is below the minimum " + VectorWorkload.MIN_RECALL, metrics);
  }

  @Override
  public String name() {
    return "correctness";
  }

  @Override
  public List<String> cases() {
    return List.of(VECTOR_RECALL, INDEX_VS_SCAN, COUNT_VS_ITERATED);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    rep = 0;
    RemoteClients.ensureDatabase(ctx.server(), DATABASE);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      switch (caseName) {
        case VECTOR_RECALL -> prepareVectors(ctx, db);
        case INDEX_VS_SCAN -> prepareItems(ctx, db, INDEX_SEED, true);
        case COUNT_VS_ITERATED -> prepareItems(ctx, db, COUNT_SEED, false);
        default -> throw new IllegalArgumentException("Unknown case: " + caseName);
      }
    }
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    base = null;
    queries = null;
    space = null;
    ledger = null;
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    ctx.server().resetPeak();
    return switch (caseName) {
      case VECTOR_RECALL -> vectorRecall(ctx);
      case INDEX_VS_SCAN -> indexVsScan(ctx);
      case COUNT_VS_ITERATED -> countVsIterated(ctx);
      default -> throw new IllegalArgumentException("Unknown case: " + caseName);
    };
  }

  // ---- vector-recall-after-churn

  private void prepareVectors(final WorkloadContext ctx, final RemoteDatabase db) {
    seed = ctx.seed() + VECTOR_SEED;
    final int n = vectors(ctx.scale());
    // base vectors, new ones of every repetition and queries come from the same space, as a corpus and its queries do
    space = new DatasetGenerator(seed + 2).embeddingSpace(DIM);
    base = new DatasetGenerator(seed).embeddings(n, space);
    queries = new DatasetGenerator(seed + 1).embeddings(QUERIES, space);

    db.command("sql", "DROP TYPE " + VECTOR_TYPE + " IF EXISTS UNSAFE").close();
    db.command("sql", "CREATE DOCUMENT TYPE " + VECTOR_TYPE).close();
    db.command("sql", "CREATE PROPERTY " + VECTOR_TYPE + ".id INTEGER").close();
    db.command("sql", "CREATE PROPERTY " + VECTOR_TYPE + ".v ARRAY_OF_FLOATS").close();
    db.command("sql", "CREATE INDEX ON " + VECTOR_TYPE + " (id) UNIQUE").close();
    final int[] ids = new int[VECTOR_BATCH];
    for (int from = 0; from < n; from += VECTOR_BATCH) {
      final int to = Math.min(from + VECTOR_BATCH, n);
      for (int i = from; i < to; i++)
        ids[i - from] = i;
      insertVectors(db, ids, to - from);
    }
    db.command("sql", "CREATE INDEX ON " + VECTOR_TYPE + " (v) LSM_VECTOR METADATA {dimensions: " + DIM + ", similarity: 'COSINE'}")
        .close();
    checkCount("vectors loaded", n, RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + VECTOR_TYPE));
  }

  private Measurement vectorRecall(final WorkloadContext ctx) throws CaseFailedWithMetrics {
    final int n = base.length;
    final int offset = rep % VECTOR_CHURN_EVERY;
    final int churned = (n - offset + VECTOR_CHURN_EVERY - 1) / VECTOR_CHURN_EVERY;
    final float[][] fresh = new DatasetGenerator(seed + 10 + rep).embeddings(churned, space);
    rep++;

    final String sql = "SELECT id FROM (SELECT expand(vectorNeighbors('" + VECTOR_TYPE + "[v]', :q, " + K + ")))";
    final long start = System.nanoTime();
    final int[][] found = new int[queries.length][K];
    final int[] foundCount = new int[queries.length];
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      final int[] ids = new int[Math.max(DELETE_BATCH, VECTOR_BATCH)];
      int inBatch = 0;
      int next = 0;
      for (int id = offset; id < n; id += VECTOR_CHURN_EVERY) {
        ids[inBatch++] = id;
        base[id] = fresh[next++];
        if (inBatch == DELETE_BATCH) {
          deleteIds(db, VECTOR_TYPE, ids, inBatch);
          insertVectorBatches(db, ids, inBatch);
          inBatch = 0;
        }
      }
      if (inBatch > 0) {
        deleteIds(db, VECTOR_TYPE, ids, inBatch);
        insertVectorBatches(db, ids, inBatch);
      }
      checkCount("vectors after the churn", n, RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + VECTOR_TYPE));

      // only the server's answers are timed: the brute force below is client work
      for (int q = 0; q < queries.length; q++) {
        int count = 0;
        try (final ResultSet rs = db.query("sql", sql, Map.<String, Object>of("q", queries[q]))) {
          while (rs.hasNext()) {
            final Result r = rs.next();
            if (count < K)
              found[q][count++] = ((Number) r.getProperty("id")).intValue();
          }
        }
        foundCount[q] = count;
      }
    }
    final double seconds = (System.nanoTime() - start) / 1e9;

    // the live set is exactly the client's: every deleted id was inserted again with the vector now in base
    final double[] norms = VectorRecall.norms(base);
    double recallSum = 0;
    for (int q = 0; q < queries.length; q++)
      recallSum += VectorRecall.recall(Arrays.copyOf(found[q], foundCount[q]), VectorRecall.exactTopK(base, norms, queries[q], K));
    final double recall = recallSum / queries.length;
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.seconds("check_s", seconds));
    metrics.add(Metrics.of("recall_at_10", "ratio", Metric.Better.HIGHER, recall));
    metrics.add(Metrics.bytes("peak_rss_bytes", ctx.server().peakRssBytes()));
    checkRecall(recall, metrics);
    return new Measurement(metrics);
  }

  private void insertVectorBatches(final RemoteDatabase db, final int[] ids, final int count) {
    final int[] batch = new int[VECTOR_BATCH];
    for (int from = 0; from < count; from += VECTOR_BATCH) {
      final int n = Math.min(VECTOR_BATCH, count - from);
      System.arraycopy(ids, from, batch, 0, n);
      insertVectors(db, batch, n);
    }
  }

  private void insertVectors(final RemoteDatabase db, final int[] ids, final int count) {
    final StringBuilder script = new StringBuilder(count * DIM * 12);
    script.append("BEGIN;\n");
    for (int i = 0; i < count; i++) {
      script.append("INSERT INTO ").append(VECTOR_TYPE).append(" SET id = ").append(ids[i]).append(", v = [");
      final float[] v = base[ids[i]];
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

  // ---- the item cases

  private void prepareItems(final WorkloadContext ctx, final RemoteDatabase db, final long caseSeed, final boolean keepLedger) {
    seed = ctx.seed() + caseSeed;
    final int n = items(ctx.scale());
    db.command("sql", "DROP TYPE " + ITEM_TYPE + " IF EXISTS UNSAFE").close();
    // many buckets, so that no one of them holds more rows than a single streamed response may carry
    db.command("sql", "CREATE DOCUMENT TYPE " + ITEM_TYPE + " BUCKETS " + Math.max(8, n / BUCKET_ROWS + 1)).close();
    db.command("sql", "CREATE PROPERTY " + ITEM_TYPE + ".id LONG").close();
    db.command("sql", "CREATE PROPERTY " + ITEM_TYPE + ".v LONG").close();
    // the same value with no index: the only way to answer through it is a scan
    db.command("sql", "CREATE PROPERTY " + ITEM_TYPE + ".v_copy LONG").close();
    db.command("sql", "CREATE INDEX ON " + ITEM_TYPE + " (id) UNIQUE").close();
    db.command("sql", "CREATE INDEX ON " + ITEM_TYPE + " (v) NOTUNIQUE").close();

    final SplittableRandom random = new SplittableRandom(seed);
    ledger = keepLedger ? new long[n] : null;
    low = 0;
    high = n;
    final long[] ids = new long[ROW_BATCH];
    final long[] values = new long[ROW_BATCH];
    for (long from = 0; from < n; from += ROW_BATCH) {
      final int count = (int) Math.min(ROW_BATCH, n - from);
      for (int i = 0; i < count; i++) {
        ids[i] = from + i;
        values[i] = random.nextInt(DOMAIN);
        if (keepLedger)
          ledger[(int) ids[i]] = values[i];
      }
      insertItems(db, ids, values, count);
    }
    checkCount("items loaded", n, RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + ITEM_TYPE));
    if (keepLedger)
      checkPlans(ctx.server(), DATABASE);
  }

  private Measurement indexVsScan(final WorkloadContext ctx) {
    final int n = ledger.length;
    final int offset = rep % ITEM_CHURN_EVERY;
    final SplittableRandom random = new SplittableRandom(seed + 10 + rep);
    rep++;

    final List<String> predicates = predicates();
    final long[] indexed = new long[predicates.size()];
    final long[] scanned = new long[predicates.size()];
    final long start = System.nanoTime();
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      // delete one row in 20 and insert the same ids again with new values
      final long[] ids = new long[DELETE_BATCH];
      final long[] values = new long[DELETE_BATCH];
      int inBatch = 0;
      for (int id = offset; id < n; id += ITEM_CHURN_EVERY) {
        ids[inBatch] = id;
        values[inBatch++] = random.nextInt(DOMAIN);
        if (inBatch == DELETE_BATCH) {
          replaceItems(db, ids, values, inBatch);
          inBatch = 0;
        }
      }
      if (inBatch > 0)
        replaceItems(db, ids, values, inBatch);

      checkCount("items after the churn", n, RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + ITEM_TYPE));
      for (int p = 0; p < predicates.size(); p++) {
        indexed[p] = RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + ITEM_TYPE + " WHERE " + on(predicates.get(p), "v"));
        scanned[p] = RemoteClients.count(db, "sql",
            "SELECT count(*) AS n FROM " + ITEM_TYPE + " WHERE " + on(predicates.get(p), "v_copy"));
      }
    }
    final double seconds = (System.nanoTime() - start) / 1e9;

    // the ledger is the client's own and is compared once the clock has stopped
    for (int p = 0; p < predicates.size(); p++) {
      final long expected = expectedMatches(predicates.get(p), ledger);
      if (expected == 0)
        throw new IllegalStateException("the predicate matches no row, the check would pass for the wrong reason: " + predicates.get(p));
      checkCount("index " + predicates.get(p), expected, indexed[p]);
      checkCount("scan " + predicates.get(p), expected, scanned[p]);
    }
    return new Measurement(List.of(Metrics.seconds("check_s", seconds), Metrics.bytes("peak_rss_bytes", ctx.server().peakRssBytes())));
  }

  /**
   * The premise of the case: the predicates on {@code v} are answered through its index and the ones on {@code v_copy} are not.
   * A plan that does not say so fails the case with the plan in the message.
   */
  static void checkPlans(final ServerUnderTest server, final String database) {
    for (final String predicate : predicates()) {
      final String viaIndex = plan(server, database, on(predicate, "v"));
      if (!viaIndex.contains("INDEX " + ITEM_TYPE + "[v]"))
        throw new IllegalStateException("the plan of '" + predicate + "' on v does not use its index: " + viaIndex);
      final String viaScan = plan(server, database, on(predicate, "v_copy"));
      if (viaScan.contains("INDEX") || !viaScan.contains("FETCH FROM TYPE " + ITEM_TYPE))
        throw new IllegalStateException("the plan of '" + predicate + "' on v_copy is not a scan: " + viaScan);
    }
  }

  // the plan travels in the "explain" property of the response envelope, which RemoteDatabase's result sets do not carry
  private static String plan(final ServerUnderTest server, final String database, final String condition) {
    final String body = new JSONObject().put("language", "sql")
        .put("command", "EXPLAIN SELECT count(*) AS n FROM " + ITEM_TYPE + " WHERE " + condition).toString();
    final String auth = Base64.getEncoder().encodeToString(("root:" + ServerUnderTest.PASSWORD).getBytes(StandardCharsets.UTF_8));
    try (final HttpClient client = HttpClient.newHttpClient()) {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(server.httpUrl() + "/api/v1/query/" + database))
          .header("Authorization", "Basic " + auth).header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200)
        throw new IllegalStateException("EXPLAIN answered " + response.statusCode() + ": " + response.body());
      final String explain = new JSONObject(response.body()).getString("explain", "");
      if (explain.isEmpty())
        throw new IllegalStateException("the server answered no plan for: " + condition);
      return explain;
    } catch (final IOException e) {
      throw new IllegalStateException("EXPLAIN failed for: " + condition, e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while asking for the plan of: " + condition, e);
    }
  }

  /** Deletes the rows of {@code ids}, checks that each existed, and inserts them again with {@code values}; the ledger follows. */
  private void replaceItems(final RemoteDatabase db, final long[] ids, final long[] values, final int count) {
    deleteIds(db, ITEM_TYPE, ids, count);
    for (int i = 0; i < count; i++)
      ledger[(int) ids[i]] = values[i];
    insertItems(db, ids, values, count);
  }

  private Measurement countVsIterated(final WorkloadContext ctx) {
    final long live = high - low;
    final long deleted = live / ITEM_CHURN_EVERY;
    // fewer rows come back than went away, so the count changes every repetition and a stale one is caught
    final long inserted = deleted * 4 / 5;
    final SplittableRandom random = new SplittableRandom(seed + 10 + rep);
    rep++;

    final long start = System.nanoTime();
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      try (final ResultSet rs = db.command("sql",
          "DELETE FROM " + ITEM_TYPE + " WHERE id >= " + low + " AND id < " + (low + deleted))) {
        checkCount("deleted rows", deleted, ((Number) rs.next().getProperty("count")).longValue());
      }
      low += deleted;
      final long[] ids = new long[ROW_BATCH];
      final long[] values = new long[ROW_BATCH];
      for (long from = 0; from < inserted; from += ROW_BATCH) {
        final int count = (int) Math.min(ROW_BATCH, inserted - from);
        for (int i = 0; i < count; i++) {
          ids[i] = high + from + i;
          values[i] = random.nextInt(DOMAIN);
        }
        insertItems(db, ids, values, count);
      }
      high += inserted;

      final long expected = high - low;
      checkCount("count(*)", expected, RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + ITEM_TYPE));
      // the records are read to the end, bucket by bucket: queryStream sends no limit (a buffered answer stops at 20,000 rows),
      // but the server refuses one response of more than 1,000,000 rows, which a type of millions of rows exceeds
      long iterated = 0;
      for (final String bucket : buckets(db)) {
        try (final ResultSet rs = db.queryStream("sql", "SELECT @rid FROM bucket:" + bucket, Map.<String, Object>of())) {
          while (rs.hasNext()) {
            rs.next();
            iterated++;
          }
        }
      }
      checkCount("iterated", expected, iterated);
    }
    return new Measurement(List.of(Metrics.seconds("check_s", (System.nanoTime() - start) / 1e9),
        Metrics.bytes("peak_rss_bytes", ctx.server().peakRssBytes())));
  }

  private static List<String> buckets(final RemoteDatabase db) {
    try (final ResultSet rs = db.query("sql", "SELECT buckets FROM schema:types WHERE name = '" + ITEM_TYPE + "'")) {
      if (!rs.hasNext())
        throw new IllegalStateException("the server answered no schema for the type " + ITEM_TYPE);
      final List<String> names = new ArrayList<>();
      for (final Object bucket : rs.next().<List<Object>>getProperty("buckets"))
        names.add(String.valueOf(bucket));
      if (names.isEmpty())
        throw new IllegalStateException("the type " + ITEM_TYPE + " has no bucket");
      return names;
    }
  }

  private static void insertItems(final RemoteDatabase db, final long[] ids, final long[] values, final int count) {
    final StringBuilder script = new StringBuilder(count * 70 + 16);
    script.append("BEGIN;\n");
    for (int i = 0; i < count; i++)
      script.append("INSERT INTO ").append(ITEM_TYPE).append(" SET id = ").append(ids[i]).append(", v = ").append(values[i])
          .append(", v_copy = ").append(values[i]).append(";\n");
    script.append("COMMIT;");
    db.command("sqlscript", script.toString()).close();
  }

  /** Deletes {@code count} rows by id and checks that the server found each. */
  private static void deleteIds(final RemoteDatabase db, final String type, final long[] ids, final int count) {
    final StringBuilder sql = new StringBuilder(count * 9 + 64);
    sql.append("DELETE FROM ").append(type).append(" WHERE id IN [");
    for (int i = 0; i < count; i++) {
      if (i > 0)
        sql.append(',');
      sql.append(ids[i]);
    }
    sql.append(']');
    try (final ResultSet rs = db.command("sql", sql.toString())) {
      checkCount("deleted " + type + " rows", count, ((Number) rs.next().getProperty("count")).longValue());
    }
  }

  private static void deleteIds(final RemoteDatabase db, final String type, final int[] ids, final int count) {
    final long[] wide = new long[count];
    for (int i = 0; i < count; i++)
      wide[i] = ids[i];
    deleteIds(db, type, wide, count);
  }
}
