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

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.test.support.AssetGenerator;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.FacetDatabaseWrapper;
import com.arcadedb.test.support.FacetSchema;
import com.arcadedb.test.support.FacetSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;

/**
 * Faceting over the {@code FacetAsset} type of the load tests ({@link FacetSchema}: columnar indexes on brand, tags and fileSize),
 * queried with the exact {@code FACETS} statement of the load tests ({@link FacetSchema#facetQuery}) through
 * {@link FacetDatabaseWrapper#queryFacets}. The baseline is {@link #records} assets with keys {@code aid = 1..records}, loaded once
 * per database by batched scripts and never inside a timed region.
 *
 * <p>{@code facet-under-ingest} adds assets through writer clients whose keys are {@code (writerId << 32) | seq}, so always at or
 * above {@link #INGEST_KEY_FLOOR}. Every {@link #prepare}, and the untimed tail of every ingest repetition, deletes whatever is at or above the floor and waits until the facets are
 * served from the index again, so each case and each repetition starts from the same baseline.
 */
public class FacetWorkload implements Workload {
  public static final String FACET_DATABASE = DATABASE;

  static final int QUERIES_PER_REP = 200;

  private static final Logger LOGGER = LoggerFactory.getLogger(FacetWorkload.class);

  /** Writer ids start here, and the first key of a writer is {@code id << 32}: above every baseline key. */
  private static final int  FIRST_WRITER_ID   = 1_000;
  private static final long INGEST_KEY_FLOOR  = 1L << 32;
  private static final int  WRITERS           = 2;
  private static final int  BATCH             = 1_000;
  private static final int  SETTLE_TIMEOUT_S  = 1_800;
  private static final long JOIN_TIMEOUT_MS   = 60_000L;
  private static final String BODY            = "x".repeat(200);

  private static final String AFTER_LOAD    = "facet-query-after-load";
  private static final String UNDER_INGEST  = "facet-under-ingest";

  private String buildFailure;
  /** Distinct writer ids across the repetitions of this run, so a writer's keys never repeat. */
  private int    runs;

  static int records(final Scale scale) {
    return switch (scale) {
      case XS -> 10_000;
      case S -> 200_000;
      case M -> 2_000_000;
      case L -> 10_000_000;
    };
  }

  @Override
  public String name() {
    return "facet";
  }

  @Override
  public List<String> cases() {
    return List.of(AFTER_LOAD, UNDER_INGEST);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) throws InterruptedException {
    if (!cases().contains(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    if (buildFailure != null)
      throw new IllegalStateException("facet build failed earlier: " + buildFailure);
    final int records = records(ctx.scale());
    try {
      if (!baselineReady(ctx, records))
        build(ctx, records);
      resetAndSettle(ctx, records);
    } catch (final RuntimeException | Error e) {
      // never drop and rebuild under a writer that may still be running: every later case fails fast
      buildFailure = e.toString();
      throw e;
    }
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws InterruptedException {
    return AFTER_LOAD.equals(caseName) ? queryAfterLoad(ctx) : queryUnderIngest(ctx);
  }

  private Measurement queryAfterLoad(final WorkloadContext ctx) {
    final LatencyRecorder latencies = new LatencyRecorder(QUERIES_PER_REP);
    try (final FacetDatabaseWrapper client = new FacetDatabaseWrapper(ctx.server().wrapper(), DatabaseWrapper.Protocol.HTTP, 2)) {
      ctx.server().resetPeak();
      for (int i = 0; i < QUERIES_PER_REP; i++) {
        final String where = FacetSchema.CHECKED_FILTERS.get(i % FacetSchema.CHECKED_FILTERS.size());
        final long t0 = System.nanoTime();
        client.queryFacets(where);
        latencies.record(System.nanoTime() - t0);
      }
    }
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("queries_per_s", "queries/s", QUERIES_PER_REP / (latencies.totalNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    return new Measurement(metrics);
  }

  /**
   * The same query while {@value #WRITERS} writer threads insert assets. {@link FacetDatabaseWrapper#ingestConcurrently} cannot be
   * stopped once started (it inserts a fixed count), so the writers here use the same client, {@link FacetDatabaseWrapper#ingest},
   * one asset at a time until the queries end: the ingest therefore cannot outlive the measurement.
   */
  private Measurement queryUnderIngest(final WorkloadContext ctx) throws InterruptedException {
    final int firstWriterId = FIRST_WRITER_ID + runs++ * WRITERS;
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong acked = new AtomicLong();
    final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    final List<Thread> writers = new ArrayList<>(WRITERS);
    final LatencyRecorder latencies = new LatencyRecorder(QUERIES_PER_REP);
    long ackedAtEnd;
    double ingestSeconds;
    long lagMs;
    // all clients are opened before the clock starts
    try (final FacetDatabaseWrapper client = new FacetDatabaseWrapper(ctx.server().wrapper(), DatabaseWrapper.Protocol.HTTP, 2)) {
      final List<FacetDatabaseWrapper> writerClients = new ArrayList<>(WRITERS);
      try {
        for (int w = 0; w < WRITERS; w++)
          writerClients.add(new FacetDatabaseWrapper(ctx.server().wrapper(), DatabaseWrapper.Protocol.HTTP, firstWriterId + w));
        ctx.server().resetPeak();
        final long start = System.nanoTime();
        for (final FacetDatabaseWrapper writerClient : writerClients) {
          final Thread thread = new Thread(() -> {
            try {
              while (!stop.get())
                acked.addAndGet(writerClient.ingest(1));
            } catch (final Throwable t) {
              writerFailure.compareAndSet(null, t);
            }
          }, "bench-facet-writer");
          thread.setDaemon(true);
          thread.start();
          writers.add(thread);
        }
        for (int i = 0; i < QUERIES_PER_REP; i++) {
          final String where = FacetSchema.CHECKED_FILTERS.get(i % FacetSchema.CHECKED_FILTERS.size());
          final long t0 = System.nanoTime();
          client.queryFacets(where);
          latencies.record(System.nanoTime() - t0);
        }
        // the lag of the last answer, with the writers still running
        final FacetSnapshot last = client.queryFacets("");
        lagMs = last.lagMs();
        ackedAtEnd = acked.get();
        ingestSeconds = (System.nanoTime() - start) / 1e9;
      } finally {
        stop.set(true);
        boolean interrupted = false;
        for (final Thread thread : writers) {
          try {
            thread.join(JOIN_TIMEOUT_MS);
          } catch (final InterruptedException e) {
            interrupted = true;
          }
          if (thread.isAlive()) {
            LOGGER.warn("A facet writer did not stop within {} ms, interrupting it", JOIN_TIMEOUT_MS);
            thread.interrupt();
          }
        }
        for (final FacetDatabaseWrapper writerClient : writerClients)
          writerClient.close();
        if (interrupted)
          Thread.currentThread().interrupt();
      }
    }
    // untimed tail: back to the baseline, so the next repetition starts from the same data
    resetAndSettle(ctx, records(ctx.scale()));
    if (writerFailure.get() != null)
      throw new IllegalStateException("A facet writer failed during the window, the ingest rate is not valid", writerFailure.get());
    if (ackedAtEnd == 0L)
      throw new IllegalStateException("No asset was ingested while the facets were queried", writerFailure.get());
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("queries_per_s", "queries/s", QUERIES_PER_REP / (latencies.totalNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    metrics.add(Metrics.rate("ingest_records_per_s", "records/s", ackedAtEnd / ingestSeconds));
    metrics.add(Metrics.of("facet_lag_ms", "ms", Metric.Better.LOWER, lagMs));
    return new Measurement(metrics);
  }

  /** True when the database holds exactly the baseline, after deleting what an earlier ingest case added. */
  private boolean baselineReady(final WorkloadContext ctx, final int records) {
    RemoteClients.ensureDatabase(ctx.server(), FACET_DATABASE);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), FACET_DATABASE)) {
      return count(db, "") >= records && count(db, " WHERE aid < " + INGEST_KEY_FLOOR) == records;
    } catch (final RuntimeException e) {
      // only a missing type (another workload's database, or an empty one) means "build"; anything else is a real failure
      final String message = String.valueOf(e.getMessage()).toLowerCase();
      if (message.contains("not found") || message.contains("does not exist"))
        return false;
      throw e;
    }
  }

  /** Deletes what an ingest added, checks the exact baseline count and waits until the facets are served from the index again. */
  private void resetAndSettle(final WorkloadContext ctx, final int records) {
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), FACET_DATABASE)) {
      if (count(db, "") != records) {
        db.command("sql", "DELETE FROM " + FacetSchema.TYPE + " WHERE aid >= " + INGEST_KEY_FLOOR).close();
        final long left = count(db, "");
        if (left != records)
          throw new IllegalStateException("Expected " + records + " assets after the reset, found " + left);
      }
    }
    try (final FacetDatabaseWrapper client = new FacetDatabaseWrapper(ctx.server().wrapper(), DatabaseWrapper.Protocol.HTTP, 1)) {
      client.awaitSchema(SETTLE_TIMEOUT_S);
    }
  }

  /** The number of assets in the database, for checking the baseline. */
  long assetCount(final WorkloadContext ctx) {
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), FACET_DATABASE)) {
      return count(db, "");
    }
  }

  private static long count(final RemoteDatabase db, final String where) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM " + FacetSchema.TYPE + where)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  /**
   * Untimed. The database and the schema are the load tests' ({@link FacetDatabaseWrapper#createDatabase} drops an existing one),
   * and the columnar indexes are created while the type is empty, so they are filled incrementally by the inserts. The assets are
   * inserted by batched scripts, one transaction per {@link #BATCH} statements, with the values of {@link AssetGenerator}.
   */
  private void build(final WorkloadContext ctx, final int records) throws InterruptedException {
    LOGGER.info("Building the facet database: {} assets", records);
    final long start = System.nanoTime();
    try (final FacetDatabaseWrapper admin = new FacetDatabaseWrapper(ctx.server().wrapper(), DatabaseWrapper.Protocol.HTTP, 0)) {
      admin.createDatabase();
      admin.createSchema();
      admin.awaitSchema(SETTLE_TIMEOUT_S);
    }
    final AssetGenerator generator = new AssetGenerator(ctx.seed());
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), FACET_DATABASE)) {
      final StringBuilder script = new StringBuilder(BATCH * 160);
      for (int from = 0; from < records; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = from; i < Math.min(from + BATCH, records); i++)
          appendInsert(script, generator, i + 1L);
        script.append("COMMIT;");
        db.command("sqlscript", script.toString()).close();
      }
      final long loaded = count(db, "");
      if (loaded != records)
        throw new IllegalStateException("Expected " + records + " assets, found " + loaded);
      final double seconds = (System.nanoTime() - start) / 1e9;
      LOGGER.info("Facet database built: {} assets in {} s ({} records/s)", loaded, seconds, (long) (loaded / seconds));
    }
  }

  private static void appendInsert(final StringBuilder script, final AssetGenerator generator, final long aid) {
    final boolean noBrand = generator.omitBrand();
    final Map<String, Object> p = generator.valuesParams(aid);
    script.append("INSERT INTO ").append(FacetSchema.TYPE).append(" SET aid = ").append(aid);
    if (!noBrand)
      script.append(", brand = '").append(p.get("brand")).append('\'');
    script.append(", tags = [");
    boolean first = true;
    for (final Object tag : (List<?>) p.get("tags")) {
      if (!first)
        script.append(',');
      first = false;
      script.append('\'').append(tag).append('\'');
    }
    script.append("], fileSize = ").append(p.get("fileSize")).append(", body = '").append(BODY).append("';\n");
  }
}
