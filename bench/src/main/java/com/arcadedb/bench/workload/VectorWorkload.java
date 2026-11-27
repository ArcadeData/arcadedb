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
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Vector index: build time and size, then k=10 nearest-neighbor queries with their recall against a client-side brute force.
 * The vectors are drawn like embeddings ({@link DatasetGenerator#embeddings}), and the queries run with the engine's default
 * search width: the case measures what a user gets out of the box.
 * A case whose recall falls under {@link #MIN_RECALL} is recorded as failed, with its metrics, so a fast but wrong index can
 * never look like an improvement.
 */
public class VectorWorkload implements Workload {
  public static final String DATABASE = "bench_vector";
  public static final double MIN_RECALL = 0.90;

  static final String BUILD = "build";
  static final String KNN = "knn-k10";

  /** {@code vectors} param: embedding-like vectors, the default. */
  static final String EMBEDDINGS = "embeddings";
  /** {@code vectors} param: uniform directions, the worst case for approximate search. */
  static final String UNIFORM    = "uniform";

  private static final int    DIM    = 128;
  private static final int    K      = 10;
  private static final int    BATCH  = 200;

  // one type per kind of vectors, so a stored data set is never reused for the other kind
  private String    type;
  private boolean   uniform;
  private float[][] base;
  private float[][] queries;
  private int[][]   exact;
  private double[]  baseNorms;

  static int vectors(final Scale scale) {
    return switch (scale) {
      case XS -> 2_000;
      // the smallest round size whose build takes over 5 s on the JVM (20k took 1.0 s, 200k 15.4 s)
      case S -> 80_000;
      case M -> 200_000;
      case L -> 1_000_000;
    };
  }

  static int queryCount(final Scale scale) {
    return switch (scale) {
      case XS -> 50;
      case S, M, L -> 1_000;
    };
  }

  @Override
  public String name() {
    return "vector";
  }

  @Override
  public List<String> cases() {
    return List.of(BUILD, KNN);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    final String kind = ctx.params() == null ? EMBEDDINGS : ctx.params().getString("vectors", EMBEDDINGS);
    if (!EMBEDDINGS.equals(kind) && !UNIFORM.equals(kind))
      throw new IllegalArgumentException("vectors must be '" + EMBEDDINGS + "' or '" + UNIFORM + "', not '" + kind + "'");
    uniform = UNIFORM.equals(kind);
    type = uniform ? "VecUniform" : "Vec";
    RemoteClients.ensureDatabase(ctx.server(), DATABASE);
    loadData(ctx);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      // the vectors are inserted once; each build repetition only drops and recreates the index
      if (!dataReady(db, ctx.scale())) {
        createType(db);
        insertAll(db);
      }
      if (KNN.equals(caseName)) {
        if (queries == null) {
          queries = uniform ?
              new DatasetGenerator(ctx.seed() + 1).vectors(queryCount(ctx.scale()), DIM) :
              new DatasetGenerator(ctx.seed() + 1).embeddings(queryCount(ctx.scale()), space(ctx));
          baseNorms = VectorRecall.norms(base);
          exact = new int[queries.length][];
          for (int i = 0; i < queries.length; i++)
            exact[i] = VectorRecall.exactTopK(base, baseNorms, queries[i], K);
        }
        // the build case may not have run in this invocation
        if (!indexReady(db))
          createIndex(db);
      }
    }
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    base = null;
    queries = null;
    exact = null;
    baseNorms = null;
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    return switch (caseName) {
      case BUILD -> build(ctx);
      case KNN -> knn(ctx);
      default -> throw new IllegalArgumentException("Unknown case: " + caseName);
    };
  }

  private void loadData(final WorkloadContext ctx) {
    if (base == null)
      base = uniform ?
          new DatasetGenerator(ctx.seed()).vectors(vectors(ctx.scale()), DIM) :
          new DatasetGenerator(ctx.seed()).embeddings(vectors(ctx.scale()), space(ctx));
  }

  // base vectors and queries come from the same space, as a corpus and its queries go through the same embedding model
  private static DatasetGenerator.EmbeddingSpace space(final WorkloadContext ctx) {
    return new DatasetGenerator(ctx.seed() + 2).embeddingSpace(DIM);
  }

  private Measurement build(final WorkloadContext ctx) {
    final ServerUnderTest server = ctx.server();
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      if (indexReady(db))
        db.command("sql", "DROP INDEX `" + index() + "`").close();
      server.resetPeak();
      final long start = System.nanoTime();
      createIndex(db);
      final double seconds = (System.nanoTime() - start) / 1e9;
      return new Measurement(List.of(Metrics.seconds("build_s", seconds), Metrics.bytes("disk_bytes", Metrics.settledDiskBytes(server, DATABASE)),
          Metrics.bytes("peak_rss_bytes", server.peakRssBytes())));
    }
  }

  private Measurement knn(final WorkloadContext ctx) throws CaseFailedWithMetrics {
    final int efSearch = ctx.params() == null ? -1 : ctx.params().getInt("efSearch", -1);
    final String sql = "SELECT id FROM (SELECT expand(vectorNeighbors('" + index() + "', :q, " + K
        + (efSearch > 0 ? ", " + efSearch : "") + ")))";
    final LatencyRecorder latencies = new LatencyRecorder(queries.length);
    double recallSum = 0;
    final long start = System.nanoTime();
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      for (int i = 0; i < queries.length; i++) {
        final int[] ids = new int[K];
        int found = 0;
        final long t0 = System.nanoTime();
        try (final ResultSet rs = db.query("sql", sql, Map.<String, Object>of("q", queries[i]))) {
          while (rs.hasNext()) {
            final Result r = rs.next();
            if (found < K)
              ids[found++] = ((Number) r.getProperty("id")).intValue();
          }
        }
        latencies.record(System.nanoTime() - t0);
        recallSum += VectorRecall.recall(found == K ? ids : Arrays.copyOf(ids, found), exact[i]);
      }
    }
    final double seconds = (System.nanoTime() - start) / 1e9;
    final double recall = recallSum / queries.length;

    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("queries_per_s", "queries/s", queries.length / seconds));
    metrics.addAll(Metrics.latencies(latencies));
    metrics.add(Metrics.of("recall_at_10", "ratio", Metric.Better.HIGHER, recall));
    if (recall < MIN_RECALL)
      throw new CaseFailedWithMetrics("recall@10 " + recall + " is below the minimum " + MIN_RECALL, metrics);
    return Measurement.of(metrics, latencies);
  }

  private String index() {
    return type + "[v]";
  }

  private void createType(final RemoteDatabase db) {
    db.command("sql", "DROP TYPE " + type + " IF EXISTS UNSAFE").close();
    db.command("sql", "CREATE DOCUMENT TYPE " + type).close();
    db.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
    db.command("sql", "CREATE PROPERTY " + type + ".v ARRAY_OF_FLOATS").close();
  }

  private void insertAll(final RemoteDatabase db) {
    final StringBuilder script = new StringBuilder(BATCH * DIM * 12);
    for (int from = 0; from < base.length; from += BATCH) {
      script.setLength(0);
      script.append("BEGIN;\n");
      final int to = Math.min(from + BATCH, base.length);
      for (int i = from; i < to; i++) {
        script.append("INSERT INTO ").append(type).append(" SET id = ").append(i).append(", v = [");
        final float[] v = base[i];
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

  private void createIndex(final RemoteDatabase db) {
    db.command("sql", "CREATE INDEX ON " + type + " (v) LSM_VECTOR METADATA {dimensions: " + DIM + ", similarity: 'COSINE'}")
        .close();
  }

  private boolean dataReady(final RemoteDatabase db, final Scale scale) {
    try {
      return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + type) == vectors(scale);
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private boolean indexReady(final RemoteDatabase db) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM schema:indexes WHERE name = '" + index() + "'")) {
      return rs.hasNext();
    } catch (final RuntimeException e) {
      return false;
    }
  }
}
