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
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.FacetDatabaseWrapper;
import com.arcadedb.test.support.FacetSnapshot;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.test.support.FacetDatabaseWrapper.REBUILDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Facet load on one server: four writers run the mixed facet operations while two threads keep querying the facets, then the facets
 * must equal {@code GROUP BY} once the projection settles, with no error, no fallback scan and no rebuild. Latency, lag, patched counts
 * and throughput are recorded, never asserted: they are logged and written to {@code target/facet-load/<class>-<protocol>.json}, the
 * baseline of the columnar tuning phase.
 */
class SingleServerFacetLoadTestIT extends FacetLoadTestTemplate {

  private static final int WRITERS        = 4;
  private static final int OPS_PER_WRITER = 20_000;
  private static final int QUERY_THREADS  = 2;
  private static final int ADMIN_ID       = 1_000;

  @DisplayName("Single server facet load test")
  @ParameterizedTest(name = "Facet load test with {0}")
  @EnumSource(DatabaseWrapper.Protocol.class)
  @Timeout(value = 30, unit = TimeUnit.MINUTES) // hang detector, not a latency bound
  void singleServerFacetLoadTest(final DatabaseWrapper.Protocol protocol) throws Exception {
    createArcadeContainer("arcade", network);
    final ServerWrapper server = startContainers().getFirst();

    startCumulativeMeters();
    try (final FacetDatabaseWrapper admin = new FacetDatabaseWrapper(server, protocol, ADMIN_ID)) {
      runLoad(server, protocol, admin);
    } finally {
      stopCumulativeMeters();
      logAndDumpContainerLogs("singleServerFacetLoad-" + protocol.name());
    }
  }

  private void runLoad(final ServerWrapper server, final DatabaseWrapper.Protocol protocol, final FacetDatabaseWrapper admin)
      throws Exception {
    admin.createDatabase();
    admin.createSchema();
    final long schemaMillis = admin.awaitSchema(60);
    // the setup poll may scan, by design and exactly: each new column is published by an async create rebuild one bucket at a time, so
    // the bound below applies to the change over the measured run, from a count read once it is stable past the metrics refresh
    final long setupFallbackScans = stableScrape(admin, FALLBACK_SCANS, 60);
    logger.info("Fallback scans during the setup: {}", setupFallbackScans);
    logger.info("Facet load test on protocol {}: the projection served the facets from the index after {} ms", protocol, schemaMillis);
    logger.info("Running {} writers x {} mixed ops and {} query threads", WRITERS, OPS_PER_WRITER, QUERY_THREADS);

    final long startNanos = System.nanoTime();
    final ExecutorService writers = Executors.newFixedThreadPool(WRITERS);
    final List<Future<FacetDatabaseWrapper.OpCounts>> writes = startWriters(writers, server, protocol, WRITERS, OPS_PER_WRITER);

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

    final FacetDatabaseWrapper.OpCounts written = sum(writes);
    assertThat(queriers.awaitTermination(2, TimeUnit.MINUTES)).as("the query threads stop once the writers end").isTrue();
    final QueryStats merged = new QueryStats();
    for (final Future<QueryStats> query : queries)
      merged.addAll(query.get());
    logger.info("Writers ended after {} s: {} acked, {} failed; {} queries ({} failed)", String.format("%.1f", elapsedSeconds),
        written.acked(), written.failed(), merged.count(), merged.failed);

    // written in any case, so a red run leaves its numbers too, marked as not a baseline
    final JSONObject baseline = merged.putInto(runSummary(protocol, elapsedSeconds, WRITERS, OPS_PER_WRITER, QUERY_THREADS, written),
            elapsedSeconds)//
        .put("overlayRows", new JSONObject().put("max", maxOverlayRows)).put("applyLagMs", new JSONObject().put("max", maxApplyLagMs));
    boolean passed = false;
    try {
      // preconditions: the run did write and query, so the checks below compare real data and real samples
      assertThat(written.acked()).as("acknowledged writes").isGreaterThan(0L);
      assertThat(merged.count()).as("facet queries answered while the writers ran").isGreaterThan(0L);

      admin.assertFacetsMatchGroupBy(120);
      final FacetSnapshot settled = admin.awaitSettledFacets("", 120);
      assertThat(settled.exact()).isTrue();
      assertThat(settled.lagMs()).isZero();
      baseline.put("assets", total(settled.counts()));
      assertThat(total(settled.counts())).as("assets counted by the settled facets").isGreaterThan(0L);

      Metrics.globalRegistry.getMeters().forEach(meter -> logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure()));

      putServerMetrics(baseline, admin, setupFallbackScans);
      // overlay rows and apply lag after the settle are about 0 by construction: "max" (sampled every 5 s) is the informative one
      baseline.getJSONObject("overlayRows").put("final", (long) admin.scrapeMetric(OVERLAY_ROWS));
      baseline.getJSONObject("applyLagMs").put("final", (long) admin.scrapeMetric(APPLY_LAG));

      // totals of the whole run, which sumCounter (the last 10 s step) is not; the admin's own queries count too
      assertThat(written.failed()).as("writes failed (after " + FacetDatabaseWrapper.RETRIES + " retries of a conflict)").isZero();
      assertThat(merged.failed).as("facet queries failed while the writers ran").isZero();
      assertThat(total(QUERY_ERRORS)).isZero();
      assertThat(total(WRITE_ERRORS)).isZero();
      // ruling B-M1: inexact answers without lag are logged per sample and counted in the baseline (metaInconsistent), not a failure
      if (total(META_INCONSISTENT) > 0)
        logger.warn("{} facet samples were inexact without lag (each logged above)", (long) total(META_INCONSISTENT));
      // precondition of the two checks below: the columnar series are bound, and every series of the database is bound with them
      assertThat(baseline.getLong("segments", 0L)).as("columnar segments on disk, so the columnar series are exported").isGreaterThan(0L);
      assertThat(baseline.getLong("fallbackScans", -1L)).as("fallback scans during the measured run (after " + setupFallbackScans
          + " during the setup): projection present the whole run").isZero();
      assertThat(admin.scrapeMetric(REBUILDS)).as("indexes created on an empty type never rebuild: " + baseline.getString("rebuilds", ""))
          .isZero();
      assertNoCorruptedReads(baseline);
      passed = true;
    } finally {
      writeBaseline(protocol, baseline, passed);
    }
  }
}
