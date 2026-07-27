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
package com.arcadedb.server.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5413: dense vector query p99 in server mode is amplified far beyond the HTTP transport fee
 * (reported 107 ms vs 17.9 ms embedded, while the p50 fee is only ~1.4 ms).
 * <p>
 * This benchmark runs the very same query stream twice against the same running server JVM - once through the
 * embedded API on the server's own {@code Database} instance, once over HTTP/JSON - so the engine, heap, page
 * cache and index are identical and only the server request path differs. Any p99 amplification measured here
 * is therefore attributable to the HTTP handler path, not to the vector engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue5413DenseVectorServerLatencyIT extends BaseGraphServerTest {
  private static final String TYPE_NAME  = "Doc";
  private static final int    DIMENSIONS = Integer.parseInt(System.getProperty("arcadedb.test.vector.dims", "96"));
  private static final int    VECTORS    = Integer.parseInt(System.getProperty("arcadedb.test.vector.count", "30000"));
  private static final int    THREADS    = Integer.parseInt(System.getProperty("arcadedb.test.vector.threads", "8"));
  private static final int    QUERIES    = Integer.parseInt(System.getProperty("arcadedb.test.vector.queries", "2000"));
  private static final int    WARMUP     = Integer.parseInt(System.getProperty("arcadedb.test.vector.warmup", "300"));
  private static final int    K          = 10;

  private static final String QUERY = "SELECT id, distance FROM ( SELECT expand(`vector.neighbors`('" + TYPE_NAME
      + "[vector]', :q, " + K + ")) )";

  private float[][] queries;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void populateDatabase() {
    final Random rnd = new Random(7);
    final Database database = getDatabase(0);

    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    });

    final long begin = System.currentTimeMillis();
    database.begin();
    for (int i = 0; i < VECTORS; i++) {
      final float[] v = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        v[d] = rnd.nextFloat();
      database.newDocument(TYPE_NAME).set("id", i).set("vector", v).save();
      if (i % 5000 == 4999) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    System.out.println("[#5413] ingested " + VECTORS + " vectors in " + (System.currentTimeMillis() - begin) + "ms");

    assertThat(database.getSchema().getType(TYPE_NAME).getAllIndexes(false)).isNotEmpty();
  }

  @Test
  void compareEmbeddedAndHttpLatency() throws Exception {
    final Random rnd = new Random(11);
    queries = new float[512][DIMENSIONS];
    for (int i = 0; i < queries.length; i++)
      for (int d = 0; d < DIMENSIONS; d++)
        queries[i][d] = rnd.nextFloat();

    final Database serverDb = getServer(0).getDatabase(getDatabaseName());

    // Sanity: the index must really answer with K neighbours, otherwise the numbers below are meaningless.
    final Map<String, Object> probeParams = new HashMap<>();
    probeParams.put("q", queries[0]);
    try (final ResultSet rs = serverDb.query("sql", QUERY, probeParams)) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).as("vector index must return %d neighbours", K).isEqualTo(K);
    }
    printStats(serverDb, "before");

    // ---- warm-up on every lane (graph load, JIT, page cache, vector cache) ----
    runEmbedded(serverDb, 1, WARMUP);
    runWrapped(serverDb, 1, WARMUP);
    runHttp(1, WARMUP);

    final long[] embedded = runEmbedded(serverDb, THREADS, QUERIES);
    final long[] wrapped = runWrapped(serverDb, THREADS, QUERIES);
    final long[] http = runHttp(THREADS, QUERIES);

    printStats(serverDb, "after");
    report("embedded", embedded);
    report("wrapped ", wrapped);
    report("http    ", http);
    serverSideTimers();

    final double embeddedP99 = percentile(embedded, 99) / 1_000_000.0;
    final double httpP99 = percentile(http, 99) / 1_000_000.0;
    final double embeddedP50 = percentile(embedded, 50) / 1_000_000.0;
    final double httpP50 = percentile(http, 50) / 1_000_000.0;

    System.out.printf("[#5413] p50 fee=%.2fms  p99 fee=%.2fms  p99 amplification=%.2fx%n", httpP50 - embeddedP50,
        httpP99 - embeddedP99, httpP99 / embeddedP99);

    // The HTTP tail must not blow up out of proportion to the flat per-request fee. The reported regression was
    // ~6x with a ~90ms absolute gap; the bound below is loose enough to survive CI noise and still catch that.
    assertThat(httpP99).as("HTTP p99 (%.2fms) vs embedded p99 (%.2fms)", httpP99, embeddedP99)
        .isLessThan(embeddedP99 * 3 + 25);
  }

  private long[] runEmbedded(final Database db, final int threads, final int totalQueries) throws Exception {
    return run(threads, totalQueries, (idx) -> {
      final Map<String, Object> params = new HashMap<>();
      params.put("q", queries[idx % queries.length]);
      try (final ResultSet rs = db.query("sql", QUERY, params)) {
        int c = 0;
        while (rs.hasNext()) {
          rs.next();
          c++;
        }
        return c;
      }
    });
  }

  /**
   * Same query, but wrapped exactly the way {@code DatabaseAbstractHandler} wraps every HTTP request:
   * thread-local database context created and destroyed per request, query run inside an atomic transaction.
   */
  private long[] runWrapped(final Database db, final int threads, final int totalQueries) throws Exception {
    final DatabaseInternal dbi = (DatabaseInternal) db;
    final String path = dbi.getDatabasePath();
    return run(threads, totalQueries, (idx) -> {
      DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(path);
      if (ctx == null)
        DatabaseContext.INSTANCE.init(dbi);
      try {
        final int[] counter = new int[1];
        db.transaction(() -> {
          final Map<String, Object> params = new HashMap<>();
          params.put("q", queries[idx % queries.length]);
          try (final ResultSet rs = db.query("sql", QUERY, params)) {
            while (rs.hasNext()) {
              rs.next();
              counter[0]++;
            }
          }
        }, false, 1);
        return counter[0];
      } finally {
        DatabaseContext.INSTANCE.removeContext(path);
      }
    });
  }

  private long[] runHttp(final int threads, final int totalQueries) throws Exception {
    final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
        .executor(Executors.newFixedThreadPool(Math.max(2, threads))).build();
    final String auth = "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final URI uri = URI.create("http://127.0.0.1:2480/api/v1/query/" + getDatabaseName());

    // Pre-serialize the payloads so JSON encoding on the client is not part of the measurement.
    final String[] bodies = new String[queries.length];
    for (int i = 0; i < queries.length; i++) {
      final var params = new JSONObject();
      params.put("q", Arrays.asList(boxed(queries[i])));
      bodies[i] = new JSONObject().put("language", "sql").put("command", QUERY).put("params", params).toString();
    }

    try {
      return run(threads, totalQueries, (idx) -> {
        final HttpRequest request = HttpRequest.newBuilder(uri).header("Authorization", auth)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(bodies[idx % bodies.length])).build();
        final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200)
          throw new IllegalStateException("HTTP " + response.statusCode() + ": " + response.body());
        return response.body().length();
      });
    } finally {
      client.close();
    }
  }

  private interface Lane {
    int execute(int index) throws Exception;
  }

  private long[] run(final int threads, final int totalQueries, final Lane lane) throws Exception {
    final long[] samples = new long[totalQueries];
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(totalQueries);
    final List<Throwable> errors = new ArrayList<>();

    for (int i = 0; i < totalQueries; i++) {
      final int idx = i;
      pool.submit(() -> {
        try {
          start.await();
          final long t0 = System.nanoTime();
          lane.execute(idx);
          samples[idx] = System.nanoTime() - t0;
        } catch (final Throwable e) {
          synchronized (errors) {
            errors.add(e);
          }
          samples[idx] = 0;
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertThat(done.await(20, TimeUnit.MINUTES)).isTrue();
    pool.shutdownNow();

    synchronized (errors) {
      if (!errors.isEmpty())
        throw new IllegalStateException("query lane failed with " + errors.size() + " errors", errors.getFirst());
    }
    return samples;
  }

  private static Float[] boxed(final float[] v) {
    final Float[] out = new Float[v.length];
    for (int i = 0; i < v.length; i++)
      out[i] = v[i];
    return out;
  }

  private static void printStats(final Database db, final String label) {
    final IndexInternal[] subIndexes = ((TypeIndex) db.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next())
        .getIndexesOnBuckets();
    for (final IndexInternal sub : subIndexes) {
      final Map<String, Long> stats = ((LSMVectorIndex) sub).getStats();
      System.out.printf(
          "[#5413] stats(%s) cacheHits=%s cacheMisses=%s fromDocs=%s cacheCapacity=%s pooledSearchers=%s corpus=%s%n", label,
          stats.get("vectorCacheHits"), stats.get("vectorCacheMisses"), stats.get("vectorFetchFromDocuments"),
          stats.get("searchVectorCacheCapacity"), stats.get("pooledGraphSearchers"), stats.get("activeVectors"));
    }
  }

  /**
   * Server-side view of the same requests, straight from the Micrometer timers the handler records. This is what
   * attributes the tail: when the {@code arcadedb.http.requests} max matches the client-observed max, the latency
   * is inside the server, and the engine/serialization split says which part of it.
   */
  private static void serverSideTimers() {
    for (final String name : new String[] { "arcadedb.http.requests", "http.command.deserialization", "http.command.engine",
        "http.command.serialization" }) {
      Metrics.globalRegistry.find(name).timers().forEach(t -> System.out.printf(
          "[#5413] server timer %-30s count=%d mean=%.2fms max=%.2fms%n", name, t.count(), t.mean(TimeUnit.MILLISECONDS),
          t.max(TimeUnit.MILLISECONDS)));
    }
  }

  private static void report(final String label, final long[] samples) {
    System.out.printf("[#5413] %s  p50=%.2fms p90=%.2fms p99=%.2fms p999=%.2fms max=%.2fms%n", label,
        percentile(samples, 50) / 1e6, percentile(samples, 90) / 1e6, percentile(samples, 99) / 1e6,
        percentile(samples, 99.9) / 1e6, percentile(samples, 100) / 1e6);
  }

  private static long percentile(final long[] samples, final double p) {
    final long[] sorted = samples.clone();
    Arrays.sort(sorted);
    final int idx = (int) Math.min(sorted.length - 1L, Math.round(p / 100.0 * (sorted.length - 1)));
    return sorted[idx];
  }
}
