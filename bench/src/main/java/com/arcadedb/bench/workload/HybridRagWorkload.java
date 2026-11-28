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
import com.arcadedb.bench.workload.rag.FilteredExact;
import com.arcadedb.bench.workload.rag.RagDataset;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hybrid retrieval behind multi-tenant visibility rules: a filtered k=100 vector search, a filtered BM25 search, a filtered 2-hop
 * Cypher walk over the entities the chunks mention, and the three fused into one RAG pipeline. A chunk belongs to one of 10
 * tenants and is public, private or restricted ({@link RagDataset}); query {@code i} runs as tenant {@code i % 10} and may only
 * see public chunks and the private chunks of its own tenant. The filter is applied by the server, and every row of every query
 * is checked on the client ({@link #checkVisible}): a single leaked row fails the case, with the metrics measured so far. The
 * vector cases also report their recall@100 against the exact top 100 of the chunks the tenant can see, and fail under
 * {@link #MIN_RECALL_K100}.
 */
public class HybridRagWorkload implements Workload {
  public static final String DATABASE = "hybrid_rag";

  /**
   * The floor of recall@100 of the filtered vector search. The query post-filters the 400 nearest candidates, so a tenant sees
   * fewer than 400 of them and the recall can sit well under 1.0. Calibrated as 0.1 under the lowest per-repetition recall
   * measured, capped at 0.9 (2026-11-27, arcadedata/arcadedb:latest): XS IT 0.9995 in each of 3 reps of both vector cases,
   * laptop S 1.0 in each of 5 reps of both, so the lowest is 0.9995 and the floor 0.8995.
   */
  public static final double MIN_RECALL_K100 = 0.8995;

  static final String VECTOR   = "vector-k100-filtered";
  static final String FULLTEXT = "fulltext-filtered";
  static final String HOP2     = "hop2-filtered";
  static final String PIPELINE = "rag-pipeline";

  /** One returned row: the three fields the visibility rule reads. */
  public record Row(int id, int tenant, String visibility) {
  }

  /** A row the query tenant must not see, or one the rule cannot be checked on: it fails the case. */
  static final class VisibilityLeakException extends IllegalStateException {
    VisibilityLeakException(final String message) {
      super(message);
    }
  }

  /** One timed operation of a case: query {@code query} run as {@code tenant}, returning the rows its result is judged on. */
  @FunctionalInterface
  interface Operation {
    List<Row> run(int query, int tenant);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HybridRagWorkload.class);

  private static final int    DIM              = 128;
  private static final int    K                = 100;
  private static final int    OVERFETCH        = 400;
  private static final int    FULLTEXT_LIMIT   = 50;
  private static final int    HOP_LIMIT        = 100;
  private static final int    FUSED_TOP        = 10;
  private static final int    RRF_K            = 60;
  private static final int    BATCH            = 1_000;
  private static final int    WORDS_PER_CHUNK  = 60;
  private static final int    TENANTS          = 10;
  private static final int    HUBS             = 10;
  private static final int    MENTIONS         = 3;
  private static final int    RELATED          = 4;
  private static final long   BODY_SEED_OFFSET = 1_000L;
  private static final long   TERM_SEED        = 99L;
  private static final long   START_SEED       = 7L;
  private static final String CYPHER           = "opencypher";
  private static final String SQL              = "sql";

  private static final String TEXT_INDEX   = "Chunk[body]";
  private static final String VECTOR_INDEX = "Chunk[embedding]";
  private static final String[] INDEXES    = { "Chunk[id]", "Chunk[tenant]", TEXT_INDEX, VECTOR_INDEX, "Entity[id]" };

  // Lucene query operators: a drawn term spelled like one would not be a term
  private static final Set<String> OPERATORS = Set.of("and", "or", "not", "to");

  private static final String FILTER = "(visibility = 'public' OR (visibility = 'private' AND tenant = :t))";
  private static final String VECTOR_SQL =
      "SELECT id, tenant, visibility FROM (SELECT expand(vectorNeighbors('" + VECTOR_INDEX + "', :q, " + OVERFETCH + "))) WHERE "
          + FILTER + " LIMIT " + K;
  private static final String FULLTEXT_SQL =
      "SELECT id, tenant, visibility, $score AS score FROM Chunk WHERE SEARCH_INDEX('" + TEXT_INDEX + "', :q) = true AND " + FILTER
          + " ORDER BY score DESC LIMIT " + FULLTEXT_LIMIT;
  private static final String HOP2_CYPHER =
      "MATCH (c:Chunk {id: $id})-[:Mentions]->(:Entity)-[:Related]->(:Entity)<-[:Mentions]-(c2:Chunk) "
          + "WHERE c2.visibility = 'public' OR (c2.visibility = 'private' AND c2.tenant = $t) "
          + "RETURN DISTINCT c2.id AS id, c2.tenant AS tenant, c2.visibility AS visibility LIMIT " + HOP_LIMIT;

  // what the queries of a run need, computed once per (scale, seed) and kept across the cases: about 1 MB at any scale, the
  // base vectors are dropped as soon as the exact top 100s are known
  private Truth truth;

  private record Truth(Scale scale, long seed, float[][] queryVectors, int[][] exact, String[] terms, int[] starts) {
  }

  static int chunks(final Scale scale) {
    return switch (scale) {
      case XS -> 5_000;
      case S -> 50_000;
      case M -> 200_000;
      case L -> 1_000_000;
    };
  }

  static int queries(final Scale scale) {
    return switch (scale) {
      case XS -> 100;
      case S, M, L -> 1_000;
    };
  }

  @Override
  public String name() {
    return "hybrid-rag";
  }

  @Override
  public List<String> cases() {
    return List.of(VECTOR, FULLTEXT, HOP2, PIPELINE);
  }

  /**
   * Fails with the case name and the first row a tenant must not see: a restricted row, or a private row of another tenant.
   */
  static void checkVisible(final String caseName, final int queryTenant, final List<Row> rows) {
    for (int i = 0; i < rows.size(); i++) {
      final Row row = rows.get(i);
      if (!RagDataset.visible(row.visibility(), row.tenant(), queryTenant))
        throw new VisibilityLeakException(caseName + ": tenant " + queryTenant + " received chunk " + row.id() + " (tenant " + row.tenant()
            + ", " + row.visibility() + ")");
    }
  }

  /**
   * Reciprocal rank fusion (k = 60) of two ranked id lists, best first: an id scores {@code 1 / (60 + rank)} in each list it is
   * on, ranks from 1. Equal scores keep the order of first appearance, {@code a} before {@code b}. Returns at most {@code n} ids.
   */
  static int[] fuse(final int[] a, final int[] b, final int n) {
    final int[] ids = new int[a.length + b.length];
    final double[] scores = new double[ids.length];
    int size = 0;
    for (int list = 0; list < 2; list++) {
      final int[] ranked = list == 0 ? a : b;
      for (int r = 0; r < ranked.length; r++) {
        int at = 0;
        while (at < size && ids[at] != ranked[r])
          at++;
        if (at == size)
          ids[size++] = ranked[r];
        scores[at] += 1.0 / (RRF_K + r + 1);
      }
    }
    final int[] top = new int[Math.min(n, size)];
    final boolean[] taken = new boolean[size];
    for (int t = 0; t < top.length; t++) {
      int best = -1;
      for (int i = 0; i < size; i++)
        if (!taken[i] && (best < 0 || scores[i] > scores[best]))
          best = i;
      taken[best] = true;
      top[t] = ids[best];
    }
    return top;
  }

  /**
   * The metrics of one repetition: the rate, the latency percentiles and, when {@code recall} is a number, recall@100. A recall
   * under {@link #MIN_RECALL_K100} fails the case with them.
   */
  static Measurement publish(final String caseName, final int operations, final LatencyRecorder latencies, final double recall)
      throws CaseFailedWithMetrics {
    final List<Metric> metrics = metrics(caseName, operations, latencies, recall);
    if (!Double.isNaN(recall) && recall < MIN_RECALL_K100)
      throw new CaseFailedWithMetrics(caseName + ": recall@100 " + recall + " is below the minimum " + MIN_RECALL_K100, metrics);
    return Measurement.of(metrics, latencies);
  }

  private static List<Metric> metrics(final String caseName, final int operations, final LatencyRecorder latencies,
      final double recall) {
    final List<Metric> metrics = new ArrayList<>(5);
    final double seconds = latencies.totalNanos() / 1e9;
    final double rate = seconds > 0 ? operations / seconds : 0;
    metrics.add(PIPELINE.equals(caseName) ? Metrics.rate("pipelines_per_s", "pipelines/s", rate) : Metrics.rate("queries_per_s", "queries/s", rate));
    metrics.addAll(Metrics.latencies(latencies));
    if (!Double.isNaN(recall))
      metrics.add(Metrics.of("recall_at_100", "ratio", Metric.Better.HIGHER, recall));
    return metrics;
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    if (!cases().contains(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    final int chunks = chunks(ctx.scale());
    final RagDataset dataset = new RagDataset(ctx.seed(), chunks);
    final boolean truthReady = truth != null && truth.scale() == ctx.scale() && truth.seed() == ctx.seed();
    float[][] base = null;
    RemoteClients.ensureDatabase(ctx.server(), DATABASE);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      // the data set is loaded once; the four cases and their repetitions only read it
      if (!dataReady(db, dataset, chunks)) {
        base = baseVectors(ctx, chunks);
        load(db, ctx.seed(), dataset, base);
      }
    }
    if (!truthReady) {
      if (base == null)
        base = baseVectors(ctx, chunks);
      truth = computeTruth(ctx, dataset, base);
    }
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    // the truth is kept: it is small, and the next case of the run would compute the same again
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    final boolean withRecall = VECTOR.equals(caseName) || PIPELINE.equals(caseName);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      final Operation operation = switch (caseName) {
        case VECTOR -> (i, tenant) -> vector(db, caseName, i, tenant);
        case FULLTEXT -> (i, tenant) -> fullText(db, caseName, i, tenant);
        case HOP2 -> (i, tenant) -> hop2(db, caseName, truth.starts()[i], tenant);
        case PIPELINE -> (i, tenant) -> pipeline(db, i, tenant);
        default -> throw new IllegalArgumentException("Unknown case: " + caseName);
      };
      return measureLoop(caseName, truth.exact().length, withRecall ? truth.exact() : null, operation);
    }
  }

  /**
   * The timed loop of a case: query {@code i} runs as tenant {@code i % 10}, and every row it returns is checked against the
   * visibility rule. A leak fails the case with the metrics measured until then; so does a repetition that returned no row at
   * all, which checked nothing. With {@code exact}, the recall@100 of each query's rows is measured against it.
   */
  static Measurement measureLoop(final String caseName, final int queries, final int[][] exact, final Operation operation)
      throws CaseFailedWithMetrics {
    final LatencyRecorder latencies = new LatencyRecorder(queries);
    double recallSum = 0;
    long rows = 0;
    int done = 0;
    try {
      for (int i = 0; i < queries; i++) {
        final int tenant = i % TENANTS;
        final long t0 = System.nanoTime();
        final List<Row> result = operation.run(i, tenant);
        checkVisible(caseName, tenant, result);
        latencies.record(System.nanoTime() - t0);
        if (exact != null)
          recallSum += FilteredExact.recall(ids(result), exact[i]);
        rows += result.size();
        done++;
      }
    } catch (final VisibilityLeakException e) {
      throw new CaseFailedWithMetrics(e.getMessage(),
          metrics(caseName, done, latencies, exact != null && done > 0 ? recallSum / done : Double.NaN));
    }
    if (rows == 0)
      throw new CaseFailedWithMetrics(caseName + ": " + queries + " queries returned no row", metrics(caseName, done, latencies,
          exact != null ? recallSum / done : Double.NaN));
    return publish(caseName, queries, latencies, exact != null ? recallSum / queries : Double.NaN);
  }

  private List<Row> vector(final RemoteDatabase db, final String caseName, final int query, final int tenant) {
    return run(db, caseName, tenant, SQL, VECTOR_SQL, Map.of("q", truth.queryVectors()[query], "t", tenant));
  }

  private List<Row> fullText(final RemoteDatabase db, final String caseName, final int query, final int tenant) {
    return run(db, caseName, tenant, SQL, FULLTEXT_SQL, Map.of("q", truth.terms()[query], "t", tenant));
  }

  private static List<Row> hop2(final RemoteDatabase db, final String caseName, final int start, final int tenant) {
    return run(db, caseName, tenant, CYPHER, HOP2_CYPHER, Map.of("id", start, "t", tenant));
  }

  /**
   * One RAG operation: the vector and full-text searches of the query, fused by reciprocal rank on the client, then the 2-hop walk
   * from each of the top 10 chunks. Returns the rows of the vector step, which the recall is measured on.
   */
  private List<Row> pipeline(final RemoteDatabase db, final int query, final int tenant) {
    final List<Row> nearest = vector(db, PIPELINE, query, tenant);
    final List<Row> matches = fullText(db, PIPELINE, query, tenant);
    for (final int start : fuse(ids(nearest), ids(matches), FUSED_TOP))
      hop2(db, PIPELINE, start, tenant);
    return nearest;
  }

  /** Runs a query and checks every row it returns against the visibility rule of {@code tenant}. */
  static List<Row> run(final RemoteDatabase db, final String caseName, final int tenant, final String language,
      final String query, final Map<String, Object> params) {
    final List<Row> rows = new ArrayList<>();
    try (final ResultSet rs = db.query(language, query, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Object id = r.getProperty("id");
        final Object chunkTenant = r.getProperty("tenant");
        final Object visibility = r.getProperty("visibility");
        if (!(id instanceof Number) || !(chunkTenant instanceof Number) || !(visibility instanceof String))
          throw new VisibilityLeakException(caseName + ": a row without id, tenant or visibility: " + r.toJSON());
        rows.add(new Row(((Number) id).intValue(), ((Number) chunkTenant).intValue(), (String) visibility));
      }
    }
    checkVisible(caseName, tenant, rows);
    return rows;
  }

  private static int[] ids(final List<Row> rows) {
    final int[] ids = new int[rows.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = rows.get(i).id();
    return ids;
  }

  // base vectors and queries come from the same space, as a corpus and its queries go through the same embedding model
  private static DatasetGenerator.EmbeddingSpace space(final WorkloadContext ctx) {
    return new DatasetGenerator(ctx.seed() + 2).embeddingSpace(DIM);
  }

  private static float[][] baseVectors(final WorkloadContext ctx, final int chunks) {
    return new DatasetGenerator(ctx.seed()).embeddings(chunks, space(ctx));
  }

  static String body(final long seed, final int chunk) {
    return new DatasetGenerator(seed + BODY_SEED_OFFSET + chunk).text(WORDS_PER_CHUNK);
  }

  /**
   * Untimed. The query vectors, terms and 2-hop starts, and the exact filtered top 100 of every query. One visibility mask per
   * tenant, built once; the exact scans (one per query, each over every chunk) run on a pool of their own sized to the client's
   * cores, not on the common ForkJoinPool, and the pool is shut down before returning.
   */
  private static Truth computeTruth(final WorkloadContext ctx, final RagDataset dataset, final float[][] base) {
    final long start = System.nanoTime();
    final int chunks = base.length;
    final int queries = queries(ctx.scale());
    final double[] norms = VectorRecall.norms(base);
    final boolean[][] masks = new boolean[TENANTS][chunks];
    for (int c = 0; c < chunks; c++) {
      final String visibility = dataset.visibility(c);
      final int tenant = dataset.tenant(c);
      for (int t = 0; t < TENANTS; t++)
        masks[t][c] = RagDataset.visible(visibility, tenant, t);
    }
    final float[][] queryVectors = new DatasetGenerator(ctx.seed() + 1).embeddings(queries, space(ctx));
    final int[][] exact = new int[queries][];
    final int threads = Runtime.getRuntime().availableProcessors();
    final AtomicInteger threadNumber = new AtomicInteger();
    final ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
      final Thread thread = new Thread(r, "hybrid-rag-exact-" + threadNumber.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    });
    try {
      final List<Future<?>> futures = new ArrayList<>(threads);
      for (int w = 0; w < threads; w++) {
        final int first = w;
        futures.add(pool.submit(() -> {
          for (int q = first; q < queries; q += threads)
            exact[q] = FilteredExact.topK(base, norms, masks[q % TENANTS], queryVectors[q], K);
        }));
      }
      for (final Future<?> future : futures)
        future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while computing the exact top " + K, e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("computing the exact top " + K + " failed", e.getCause());
    } finally {
      pool.shutdownNow();
    }
    LOGGER.info("hybrid-rag exact top {} of {} queries over {} chunks: {} s on {} threads", K, queries, chunks,
        (System.nanoTime() - start) / 1e9, threads);
    return new Truth(ctx.scale(), ctx.seed(), queryVectors, exact, terms(ctx.seed(), chunks, queries), starts(ctx.seed(), dataset,
        chunks, queries));
  }

  /** One BM25 term per query, a word of a chunk that exists, so the queries do not time empty results. */
  private static String[] terms(final long seed, final int chunks, final int queries) {
    final SplittableRandom random = new SplittableRandom(seed + TERM_SEED);
    final String[] terms = new String[queries];
    for (int i = 0; i < queries; i++) {
      final String[] words = body(seed, random.nextInt(chunks)).split(" ");
      String word;
      do
        word = words[random.nextInt(words.length)];
      while (word.length() < 3 || OPERATORS.contains(word));
      terms[i] = word;
    }
    return terms;
  }

  /**
   * The 2-hop start chunks. The walk is chunk, a mentioned entity, a related entity, then the chunks that mention it, so its
   * fan-out is the mention count of the entity at the second hop. The even queries start from a chunk that mentions an entity
   * related to a hub (ids 0-9, the most mentioned), so their second hop lands on a hub and the walk meets its thousands of
   * mentioners; the odd ones start uniformly. The even starts are drawn uniformly from every qualifying chunk, found by one scan;
   * if none qualifies (a tiny data set), from the chunks that mention a hub.
   */
  static int[] starts(final long seed, final RagDataset dataset, final int chunks, final int queries) {
    final boolean[] nextToHub = new boolean[dataset.entities()];
    for (int e = 0; e < nextToHub.length; e++)
      nextToHub[e] = anyHub(dataset.related(e));
    int[] qualifying = chunksMentioning(dataset, chunks, nextToHub);
    if (qualifying.length == 0) {
      final boolean[] hub = new boolean[dataset.entities()];
      for (int e = 0; e < Math.min(HUBS, hub.length); e++)
        hub[e] = true;
      qualifying = chunksMentioning(dataset, chunks, hub);
    }
    final SplittableRandom random = new SplittableRandom(seed + START_SEED);
    final int[] starts = new int[queries];
    for (int i = 0; i < queries; i++)
      starts[i] = i % 2 == 0 && qualifying.length > 0 ? qualifying[random.nextInt(qualifying.length)] : random.nextInt(chunks);
    return starts;
  }

  private static int[] chunksMentioning(final RagDataset dataset, final int chunks, final boolean[] entities) {
    int[] found = new int[1024];
    int n = 0;
    for (int c = 0; c < chunks; c++)
      for (final int e : dataset.mentions(c))
        if (entities[e]) {
          if (n == found.length)
            found = Arrays.copyOf(found, n * 2);
          found[n++] = c;
          break;
        }
    return Arrays.copyOf(found, n);
  }

  private static boolean anyHub(final int[] entities) {
    for (final int e : entities)
      if (e < HUBS)
        return true;
    return false;
  }

  /**
   * Untimed. Vertices first (chunks and entities, in scripts of {@link #BATCH} statements, one transaction each), the id indexes,
   * then the edges by the RIDs read back once, then the tenant, full-text and vector indexes, built over the loaded rows.
   */
  private static void load(final RemoteDatabase db, final long seed, final RagDataset dataset, final float[][] base) {
    final long start = System.nanoTime();
    final int chunks = base.length;
    final int entities = dataset.entities();
    LOGGER.info("Loading hybrid-rag: {} chunks, {} entities", chunks, entities);
    createSchema(db);
    final StringBuilder script = new StringBuilder(BATCH * (WORDS_PER_CHUNK * 9 + DIM * 12 + 80));
    for (int from = 0; from < chunks; from += BATCH) {
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int i = from; i < Math.min(from + BATCH, chunks); i++) {
        script.append("CREATE VERTEX Chunk SET id = ").append(i).append(", tenant = ").append(dataset.tenant(i))
            .append(", visibility = '").append(dataset.visibility(i)).append("', body = '").append(body(seed, i))
            .append("', embedding = [");
        final float[] v = base[i];
        for (int j = 0; j < v.length; j++) {
          if (j > 0)
            script.append(',');
          script.append(v[j]);
        }
        script.append("];\n");
      }
      GraphOltpWorkload.flush(db, script);
    }
    for (int from = 0; from < entities; from += BATCH) {
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int i = from; i < Math.min(from + BATCH, entities); i++)
        script.append("CREATE VERTEX Entity SET id = ").append(i).append(";\n");
      GraphOltpWorkload.flush(db, script);
    }
    db.command(SQL, "CREATE INDEX ON Chunk (id) UNIQUE").close();
    db.command(SQL, "CREATE INDEX ON Entity (id) UNIQUE").close();

    final String[] chunkRids = GraphOltpWorkload.loadRids(db, "Chunk", chunks);
    final String[] entityRids = GraphOltpWorkload.loadRids(db, "Entity", entities);
    final int mentions = chunks * MENTIONS;
    for (int from = 0; from < mentions; from += BATCH) {
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int m = from; m < Math.min(from + BATCH, mentions); m++) {
        final int chunk = m / MENTIONS;
        script.append("CREATE EDGE Mentions FROM ").append(chunkRids[chunk]).append(" TO ")
            .append(entityRids[dataset.mentions(chunk)[m % MENTIONS]]).append(";\n");
      }
      GraphOltpWorkload.flush(db, script);
    }
    final int related = entities * RELATED;
    for (int from = 0; from < related; from += BATCH) {
      script.setLength(0);
      script.append("BEGIN;\n");
      for (int r = from; r < Math.min(from + BATCH, related); r++) {
        final int entity = r / RELATED;
        script.append("CREATE EDGE Related FROM ").append(entityRids[entity]).append(" TO ")
            .append(entityRids[dataset.related(entity)[r % RELATED]]).append(";\n");
      }
      GraphOltpWorkload.flush(db, script);
    }
    db.command(SQL, "CREATE INDEX ON Chunk (tenant) NOTUNIQUE").close();
    db.command(SQL, "CREATE INDEX ON Chunk (body) FULL_TEXT").close();
    db.command(SQL, "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA {dimensions: " + DIM + ", similarity: 'COSINE'}").close();
    LOGGER.info("hybrid-rag loaded: {} chunks, {} entities, {} mentions, {} related in {} s", chunks, entities, mentions, related,
        (System.nanoTime() - start) / 1e9);
  }

  private static void createSchema(final RemoteDatabase db) {
    for (final String type : new String[] { "Mentions", "Related", "Chunk", "Entity" })
      db.command(SQL, "DROP TYPE " + type + " IF EXISTS UNSAFE").close();
    db.command(SQL, "CREATE VERTEX TYPE Chunk").close();
    db.command(SQL, "CREATE PROPERTY Chunk.id INTEGER").close();
    db.command(SQL, "CREATE PROPERTY Chunk.tenant INTEGER").close();
    db.command(SQL, "CREATE PROPERTY Chunk.visibility STRING").close();
    db.command(SQL, "CREATE PROPERTY Chunk.body STRING").close();
    db.command(SQL, "CREATE PROPERTY Chunk.embedding ARRAY_OF_FLOATS").close();
    db.command(SQL, "CREATE VERTEX TYPE Entity").close();
    db.command(SQL, "CREATE PROPERTY Entity.id INTEGER").close();
    db.command(SQL, "CREATE EDGE TYPE Mentions").close();
    db.command(SQL, "CREATE EDGE TYPE Related").close();
  }

  private static boolean dataReady(final RemoteDatabase db, final RagDataset dataset, final int chunks) {
    try {
      if (RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM Chunk") != chunks
          || RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM Entity") != dataset.entities()
          || RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM Mentions") != (long) chunks * MENTIONS
          || RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM Related") != (long) dataset.entities() * RELATED)
        return false;
      for (final String index : INDEXES)
        try (final ResultSet rs = db.query(SQL, "SELECT FROM schema:indexes WHERE name = '" + index + "'")) {
          if (!rs.hasNext())
            return false;
        }
      return true;
    } catch (final RuntimeException e) {
      return false;
    }
  }
}
