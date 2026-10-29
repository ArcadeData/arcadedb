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
package com.arcadedb.test.load;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.FacetDatabaseWrapper;
import com.arcadedb.test.support.FacetSchema;
import com.arcadedb.test.support.FacetSnapshot;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.test.support.FacetDatabaseWrapper.REBUILDS;
import static com.arcadedb.test.support.FacetDatabaseWrapper.SEGMENTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Facet load on one server: four writers run the mixed facet operations while two threads keep querying the facets, then the facets
 * must equal {@code GROUP BY} once the projection settles, with no error, no fallback scan and no rebuild. Latency, lag, patched counts
 * and throughput are recorded, never asserted: they are logged and written to {@code target/facet-load/<class>-<protocol>.json}, the
 * baseline of the columnar tuning phase.
 */
class SingleServerFacetLoadTestIT extends ContainersTestTemplate {

  private static final int WRITERS        = 4;
  private static final int OPS_PER_WRITER = 20_000;
  private static final int QUERY_THREADS  = 2;

  private static final int    ADMIN_ID          = 1_000;
  private static final int    QUERY_ID_BASE     = 100;
  private static final String QUERY_ERRORS      = "arcadedb.test.facets.query.error";
  private static final String WRITE_ERRORS      = "arcadedb.test.facets.write.error";
  private static final String META_INCONSISTENT = "arcadedb.test.facets.meta.inconsistent";
  private static final String QUERY_RETRIES     = "arcadedb.test.facets.query.retry";
  private static final String BUDGET_WAITS      = "arcadedb_columnar_query_budget_waits_total";
  private static final String PATCH_OVERFLOWS   = "arcadedb_columnar_query_patch_overflows_total";
  private static final String PATCHED_TOTAL     = "arcadedb_columnar_query_patched_total";
  private static final String FALLBACK_SCANS    = "arcadedb_columnar_query_fallback_scans_total";
  private static final String OVERLAY_ROWS      = "arcadedb_columnar_overlay_rows";
  private static final String APPLY_LAG         = "arcadedb_columnar_apply_lag_ms";

  /** A growable array of primitive samples, confined to one thread until it is merged after the join. */
  private static final class Samples {
    private long[] values = new long[1024];
    private int    size;

    void add(final long value) {
      if (size == values.length)
        values = Arrays.copyOf(values, size * 2);
      values[size++] = value;
    }

    void addAll(final Samples other) {
      for (int i = 0; i < other.size; i++)
        add(other.values[i]);
    }

    /** @return {@code p50, p95, p99, max} by nearest rank; all 0 when empty */
    long[] percentiles() {
      if (size == 0)
        return new long[4];
      final long[] sorted = Arrays.copyOf(values, size);
      Arrays.sort(sorted);
      return new long[] { rank(sorted, 0.50), rank(sorted, 0.95), rank(sorted, 0.99), sorted[size - 1] };
    }

    private static long rank(final long[] sorted, final double p) {
      return sorted[Math.max(0, (int) Math.ceil(p * sorted.length) - 1)];
    }
  }

  /** What one query thread saw while the writers ran. */
  private static final class QueryStats {
    final Samples latencyMicros = new Samples();
    final Samples lagMs         = new Samples();
    final Samples patched       = new Samples();
    long nonExact;
    long failed;
  }

  /**
   * Holds every client meter of this run cumulatively. {@link #sumCounter} reads the per-test logging registry, a step registry whose
   * counters and timers report only the last completed 10 s step, so a total over the whole run must come from here.
   */
  private SimpleMeterRegistry cumulative;

  @DisplayName("Single server facet load test")
  @ParameterizedTest(name = "Facet load test with {0}")
  @EnumSource(DatabaseWrapper.Protocol.class)
  @Timeout(value = 30, unit = TimeUnit.MINUTES) // hang detector, not a latency bound
  void singleServerFacetLoadTest(final DatabaseWrapper.Protocol protocol) throws Exception {
    createArcadeContainer("arcade", network);
    final ServerWrapper server = startContainers().getFirst();

    cumulative = new SimpleMeterRegistry();
    Metrics.addRegistry(cumulative);
    try (final FacetDatabaseWrapper admin = new FacetDatabaseWrapper(server, protocol, ADMIN_ID)) {
      runLoad(server, protocol, admin);
    } finally {
      Metrics.removeRegistry(cumulative);
      // before the teardown removes the container: the logs are the only record of what the server saw
      logger.info("Container log markers: {}", countInContainerLogs("Unreadable record", "Possible corrupted record", "SEVER"));
      dumpContainerLogs("singleServerFacetLoad-" + protocol.name());
    }
  }

  private void runLoad(final ServerWrapper server, final DatabaseWrapper.Protocol protocol, final FacetDatabaseWrapper admin)
      throws Exception {
    admin.createDatabase();
    admin.createSchema();
    final long schemaMillis = admin.awaitSchema(60);
    logger.info("Facet load test on protocol {}: the projection served the facets from the index after {} ms", protocol, schemaMillis);
    logger.info("Running {} writers x {} mixed ops and {} query threads", WRITERS, OPS_PER_WRITER, QUERY_THREADS);

    final long startNanos = System.nanoTime();
    final ExecutorService writers = Executors.newFixedThreadPool(WRITERS);
    final List<Future<FacetDatabaseWrapper.OpCounts>> writes = new ArrayList<>();
    for (int t = 0; t < WRITERS; t++) {
      final int writerId = t;
      writes.add(writers.submit(() -> {
        try (final FacetDatabaseWrapper w = new FacetDatabaseWrapper(server, protocol, writerId)) {
          return w.runMixedOps(OPS_PER_WRITER);
        }
      }));
    }
    writers.shutdown();

    final AtomicReference<FacetSnapshot> lastSample = new AtomicReference<>();
    final ExecutorService queriers = Executors.newFixedThreadPool(QUERY_THREADS);
    final List<Future<QueryStats>> queries = new ArrayList<>();
    for (int q = 0; q < QUERY_THREADS; q++) {
      final int queryId = QUERY_ID_BASE + q;
      queries.add(queriers.submit(() -> runQueries(server, protocol, queryId, writers, lastSample)));
    }
    queriers.shutdown();

    long maxOverlayRows = 0L;
    long maxApplyLagMs = 0L;
    while (!writers.isTerminated()) {
      try {
        final FacetSnapshot last = lastSample.get();
        maxOverlayRows = Math.max(maxOverlayRows, (long) admin.scrapeMetric(OVERLAY_ROWS));
        maxApplyLagMs = Math.max(maxApplyLagMs, (long) admin.scrapeMetric(APPLY_LAG));
        logger.info("Current assets: {}, last sample lagMs={} patched={}", total(admin.groupBy("")),
            last == null ? "-" : last.lagMs(), last == null ? "-" : last.patched());
      } catch (final Exception e) {
        logger.error(e.getMessage(), e);
      }
      writers.awaitTermination(5, TimeUnit.SECONDS);
    }
    final double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;

    long acked = 0L;
    long failed = 0L;
    long writeRetries = 0L;
    for (final Future<FacetDatabaseWrapper.OpCounts> write : writes) {
      final FacetDatabaseWrapper.OpCounts counts = write.get();
      acked += counts.acked();
      failed += counts.failed();
      writeRetries += counts.retries();
    }
    assertThat(queriers.awaitTermination(2, TimeUnit.MINUTES)).as("the query threads stop once the writers end").isTrue();
    final QueryStats merged = new QueryStats();
    for (final Future<QueryStats> query : queries) {
      final QueryStats stats = query.get();
      merged.latencyMicros.addAll(stats.latencyMicros);
      merged.lagMs.addAll(stats.lagMs);
      merged.patched.addAll(stats.patched);
      merged.nonExact += stats.nonExact;
      merged.failed += stats.failed;
    }
    final long queryCount = merged.latencyMicros.size;
    logger.info("Writers ended after {} s: {} acked, {} failed; {} queries ({} failed)", String.format("%.1f", elapsedSeconds), acked,
        failed, queryCount, merged.failed);

    // written in any case, so a red run leaves its numbers too, marked as not a baseline
    final JSONObject baseline = baseline(protocol, elapsedSeconds, acked, failed, merged).put("writeRetries", writeRetries)
        .put("overlayRows", new JSONObject().put("max", maxOverlayRows)).put("applyLagMs", new JSONObject().put("max", maxApplyLagMs));
    boolean passed = false;
    try {
      // preconditions: the run did write and query, so the checks below compare real data and real samples
      assertThat(acked).as("acknowledged writes").isGreaterThan(0L);
      assertThat(queryCount).as("facet queries answered while the writers ran").isGreaterThan(0L);

      admin.assertFacetsMatchGroupBy(120);
      final FacetSnapshot settled = admin.awaitSettledFacets("", 120);
      assertThat(settled.exact()).isTrue();
      assertThat(settled.lagMs()).isZero();
      baseline.put("assets", total(settled.counts()));
      assertThat(total(settled.counts())).as("assets counted by the settled facets").isGreaterThan(0L);

      Metrics.globalRegistry.getMeters().forEach(meter -> logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure()));

      final double segments = admin.scrapeMetric(SEGMENTS);
      final long fallbackScans = (long) admin.scrapeMetric(FALLBACK_SCANS);
      final String rebuilds = admin.rebuildsByReason();
      // overlay rows and apply lag after the settle are about 0 by construction: "max" (sampled every 5 s) is the informative one
      baseline.put("budgetWaits", (long) admin.scrapeMetric(BUDGET_WAITS)).put("patchOverflows", (long) admin.scrapeMetric(PATCH_OVERFLOWS))
          .put("patchedTotal", (long) admin.scrapeMetric(PATCHED_TOTAL)).put("fallbackScans", fallbackScans)
          .put("segments", (long) segments).put("rebuilds", rebuilds);
      baseline.getJSONObject("overlayRows").put("final", (long) admin.scrapeMetric(OVERLAY_ROWS));
      baseline.getJSONObject("applyLagMs").put("final", (long) admin.scrapeMetric(APPLY_LAG));

      // totals of the whole run, which sumCounter (the last 10 s step) is not; the admin's own queries count too
      assertThat(failed).as("writes failed (after " + FacetDatabaseWrapper.RETRIES + " retries of a conflict)").isZero();
      assertThat(merged.failed).as("facet queries failed while the writers ran").isZero();
      assertThat(total(QUERY_ERRORS)).isZero();
      assertThat(total(WRITE_ERRORS)).isZero();
      assertThat(total(META_INCONSISTENT)).isZero();
      // precondition of the two checks below: the columnar series are bound, and every series of the database is bound with them
      assertThat(segments).as("columnar segments on disk, so the columnar series are exported").isGreaterThan(0.0);
      assertThat(fallbackScans).as("projection present the whole run").isZero();
      assertThat(admin.scrapeMetric(REBUILDS)).as("indexes created on an empty type never rebuild: " + rebuilds).isZero();
      passed = true;
    } finally {
      baseline.put("passed", passed);
      final Path file = Path.of("target", "facet-load", getClass().getSimpleName() + "-" + protocol.name() + ".json");
      Files.createDirectories(file.getParent());
      Files.writeString(file, baseline.toString(2));
      logger.info("Facet load summary {}: {} (written to {})", protocol, baseline, file.toAbsolutePath());
    }
  }

  /** Queries a random checked filter until the writers end; a failed query is counted (and asserted by the caller), not fatal. */
  private QueryStats runQueries(final ServerWrapper server, final DatabaseWrapper.Protocol protocol, final int queryId,
      final ExecutorService writers, final AtomicReference<FacetSnapshot> lastSample) {
    final QueryStats stats = new QueryStats();
    final Random random = new Random(queryId);
    try (final FacetDatabaseWrapper reader = new FacetDatabaseWrapper(server, protocol, queryId)) {
      while (!writers.isTerminated()) {
        final String where = FacetSchema.CHECKED_FILTERS.get(random.nextInt(FacetSchema.CHECKED_FILTERS.size()));
        final long start = System.nanoTime();
        try {
          final FacetSnapshot sample = reader.queryFacets(where);
          stats.latencyMicros.add((System.nanoTime() - start) / 1_000L);
          stats.lagMs.add(sample.lagMs());
          stats.patched.add(sample.patched());
          if (!sample.exact())
            stats.nonExact++;
          lastSample.set(sample);
        } catch (final RuntimeException e) {
          stats.failed++;
          logger.warn("Facet query '{}' failed: {}", where, e.getMessage());
        }
      }
    }
    return stats;
  }

  private JSONObject baseline(final DatabaseWrapper.Protocol protocol, final double elapsedSeconds, final long acked, final long failed,
      final QueryStats queries) {
    final long queryCount = queries.latencyMicros.size;
    return new JSONObject()//
        .put("test", getClass().getSimpleName()).put("protocol", protocol.name()).put("image", IMAGE).put("native", NATIVE)
        .put("finishedAt", Instant.now().toString()).put("writers", WRITERS).put("opsPerWriter", OPS_PER_WRITER)
        .put("queryThreads", QUERY_THREADS).put("elapsedSeconds", round(elapsedSeconds)).put("ackedOps", acked).put("failedOps", failed)
        .put("throughputOpsPerSec", round(acked / elapsedSeconds)).put("queries", queryCount).put("failedQueries", queries.failed)
        .put("queriesPerSec", round(queryCount / elapsedSeconds)).put("nonExact", queries.nonExact)
        .put("queryLatencyMs", percentiles(queries.latencyMicros, 1_000.0)).put("lagMs", percentiles(queries.lagMs, 1.0))
        .put("patched", percentiles(queries.patched, 1.0)).put("writeMeanMs", writeMeans())
        .put("queryRetries", (long) total(QUERY_RETRIES));
  }

  /** @return the mean client latency of each write operation over the whole run */
  private JSONObject writeMeans() {
    final JSONObject means = new JSONObject();
    for (final Timer timer : cumulative.find("arcadedb.test.facets.write").timers())
      if (timer.count() > 0)
        means.put(timer.getId().getTag("op"), round(timer.mean(TimeUnit.MICROSECONDS) / 1_000.0));
    return means;
  }

  private static JSONObject percentiles(final Samples samples, final double divisor) {
    final long[] p = samples.percentiles();
    return new JSONObject().put("p50", round(p[0] / divisor)).put("p95", round(p[1] / divisor)).put("p99", round(p[2] / divisor))
        .put("max", round(p[3] / divisor));
  }

  /** @return the count of the client counter {@code name} over the whole run */
  private double total(final String name) {
    return cumulative.find(name).counters().stream().mapToDouble(Counter::count).sum();
  }

  private static double round(final double value) {
    return Math.round(value * 100.0) / 100.0;
  }

  /** @return the number of assets in a count map: every asset falls in one size range or in {@code size.missing} */
  private static long total(final SortedMap<String, Long> counts) {
    long total = 0L;
    for (final Map.Entry<String, Long> entry : counts.entrySet())
      if (entry.getKey().startsWith("size=") || entry.getKey().equals("size.missing"))
        total += entry.getValue();
    return total;
  }
}
