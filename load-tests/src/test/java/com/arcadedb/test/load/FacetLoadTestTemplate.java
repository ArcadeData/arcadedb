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
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.test.support.FacetDatabaseWrapper.REBUILDS;
import static com.arcadedb.test.support.FacetDatabaseWrapper.SEGMENTS;
import static com.arcadedb.test.support.FacetDatabaseWrapper.SNAPSHOT_INSTALL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the facet load tests share: the writers and query threads, the whole-run client totals, the server metrics and the
 * corrupted-record log check, and the baseline JSON each test writes to {@code target/facet-load/<class>-<protocol>.json} for the
 * columnar tuning phase. Latency, lag, patched counts and throughput are recorded, never asserted.
 */
abstract class FacetLoadTestTemplate extends ContainersTestTemplate {

  static final int    QUERY_ID_BASE     = 100;
  static final String QUERY_ERRORS      = "arcadedb.test.facets.query.error";
  static final String WRITE_ERRORS      = "arcadedb.test.facets.write.error";
  static final String META_INCONSISTENT = "arcadedb.test.facets.meta.inconsistent";
  static final String QUERY_RETRIES     = "arcadedb.test.facets.query.retry";
  static final String BUDGET_WAITS      = "arcadedb_columnar_query_budget_waits_total";
  static final String PATCH_OVERFLOWS   = "arcadedb_columnar_query_patch_overflows_total";
  static final String PATCHED_TOTAL     = "arcadedb_columnar_query_patched_total";
  static final String FALLBACK_SCANS    = "arcadedb_columnar_query_fallback_scans_total";
  static final String OVERLAY_ROWS      = "arcadedb_columnar_overlay_rows";
  static final String APPLY_LAG         = "arcadedb_columnar_apply_lag_ms";

  /** What the serializer logs when a record's bytes do not decode: never expected, whatever read path met it (#10701). */
  private static final String[] CORRUPTED_READS       = { "Possible corrupted record", "Skipping corrupted property" };
  private static final int      FIRST_LINES           = 5;
  /** Longer than the 5 s refresh of the server's columnar metrics. */
  private static final int      STABLE_SCRAPE_SECONDS = 6;

  /** A growable array of primitive samples, confined to one thread until it is merged after the join. */
  static final class Samples {
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

    int size() {
      return size;
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

  /** What one or more query threads saw while the writers ran. */
  static final class QueryStats {
    final Samples latencyMicros = new Samples();
    final Samples lagMs         = new Samples();
    final Samples patched       = new Samples();
    long nonExact;
    long metaInconsistent;
    long notFromIndex;
    long failed;
    /** Queries run again after a retryable error, by the query threads (the setup and check clients excluded). */
    long retries;

    void addAll(final QueryStats other) {
      latencyMicros.addAll(other.latencyMicros);
      lagMs.addAll(other.lagMs);
      patched.addAll(other.patched);
      nonExact += other.nonExact;
      metaInconsistent += other.metaInconsistent;
      notFromIndex += other.notFromIndex;
      failed += other.failed;
      retries += other.retries;
    }

    long count() {
      return latencyMicros.size();
    }

    /** Puts the query numbers into {@code target}, rates over {@code elapsedSeconds}. */
    JSONObject putInto(final JSONObject target, final double elapsedSeconds) {
      return target.put("queries", count()).put("failedQueries", failed).put("queriesPerSec", round(count() / elapsedSeconds))
          .put("nonExact", nonExact).put("metaInconsistent", metaInconsistent).put("samplesNotFromIndex", notFromIndex).put("queryThreadRetries", retries)
          .put("queryLatencyMs", percentiles(latencyMicros, 1_000.0)).put("lagMs", percentiles(lagMs, 1.0))
          .put("patched", percentiles(patched, 1.0));
    }
  }

  /**
   * Holds every client meter of this run cumulatively. {@link #sumCounter} reads the per-test logging registry, a step registry whose
   * counters and timers report only the last completed 10 s step (#10945), so a total over the whole run must come from here.
   */
  private SimpleMeterRegistry cumulative;

  /** Starts counting every client meter from zero; call {@link #stopCumulativeMeters()} in a {@code finally}. */
  protected void startCumulativeMeters() {
    cumulative = new SimpleMeterRegistry();
    Metrics.addRegistry(cumulative);
  }

  protected void stopCumulativeMeters() {
    if (cumulative != null)
      Metrics.removeRegistry(cumulative);
  }

  /**
   * Logs the marker counts and dumps every container log: before the teardown removes the containers, they are the only record. The
   * apply pass logs the first failed read of a page at FINE (a commit is likely still publishing it), so "Unreadable record" counts
   * only the pages that failed again on a later visit (WARNING), not every transient miss.
   */
  protected void logAndDumpContainerLogs(final String label) {
    logger.info("Container log markers: {}", countInContainerLogs("Unreadable record", CORRUPTED_READS[0], CORRUPTED_READS[1], "SEVER"));
    dumpContainerLogs(label);
  }

  /** Submits {@code count} writers, each running {@code ops} mixed operations through {@code server} on its own keys. */
  protected static List<Future<FacetDatabaseWrapper.OpCounts>> startWriters(final ExecutorService writers, final ServerWrapper server,
      final DatabaseWrapper.Protocol protocol, final int count, final int ops) {
    final List<Future<FacetDatabaseWrapper.OpCounts>> writes = new ArrayList<>();
    for (int t = 0; t < count; t++) {
      final int writerId = t;
      writes.add(writers.submit(() -> {
        try (final FacetDatabaseWrapper w = new FacetDatabaseWrapper(server, protocol, writerId)) {
          return w.runMixedOps(ops);
        }
      }));
    }
    writers.shutdown();
    return writes;
  }

  /** @return the sum of the writers' counts */
  protected static FacetDatabaseWrapper.OpCounts sum(final List<Future<FacetDatabaseWrapper.OpCounts>> writes)
      throws ExecutionException, InterruptedException {
    long acked = 0L;
    long failed = 0L;
    long retries = 0L;
    for (final Future<FacetDatabaseWrapper.OpCounts> write : writes) {
      final FacetDatabaseWrapper.OpCounts counts = write.get();
      acked += counts.acked();
      failed += counts.failed();
      retries += counts.retries();
    }
    return new FacetDatabaseWrapper.OpCounts(acked, failed, retries);
  }

  /**
   * Queries a random checked filter on {@code server} until the writers end. A failed query and a sample whose meta is inconsistent are
   * counted (and asserted by the caller), not fatal, so one bad sample does not stop the load.
   */
  protected QueryStats runQueries(final ServerWrapper server, final DatabaseWrapper.Protocol protocol, final int queryId,
      final ExecutorService writers, final AtomicReference<FacetSnapshot> lastSample) {
    final QueryStats stats = new QueryStats();
    final Random random = new Random(queryId);
    try (final FacetDatabaseWrapper reader = new FacetDatabaseWrapper(server, protocol, queryId)) {
      while (!writers.isTerminated()) {
        final String where = FacetSchema.CHECKED_FILTERS.get(random.nextInt(FacetSchema.CHECKED_FILTERS.size()));
        final long start = System.nanoTime();
        final long startMillis = System.currentTimeMillis();
        try {
          final FacetSnapshot sample = reader.queryFacets(where);
          stats.latencyMicros.add((System.nanoTime() - start) / 1_000L);
          stats.lagMs.add(sample.lagMs());
          stats.patched.add(sample.patched());
          if (!sample.exact())
            stats.nonExact++;
          if (!sample.metaConsistent()) {
            stats.metaInconsistent++;
            logger.warn("Facet sample on {} with inconsistent meta at {}: where='{}' exact={} lagMs={}", reader.describe(),
                Instant.ofEpochMilli(startMillis), where, sample.exact(), sample.lagMs());
          }
          if (!sample.sources().values().stream().allMatch("index"::equals)) {
            // the response names one source per facet (no per-bucket detail): enough to time the sample against the server log
            stats.notFromIndex++;
            logger.warn("Facet sample on {} not served from the index at {}: where='{}' sources by facet {} lagMs={} exact={} patched={}",
                reader.describe(), Instant.ofEpochMilli(startMillis), where, sample.sources(), sample.lagMs(), sample.exact(),
                sample.patched());
          }
          lastSample.set(sample);
        } catch (final RuntimeException e) {
          stats.failed++;
          logger.warn("Facet query '{}' on {} failed: {}", where, reader.describe(), e.getMessage());
        }
      }
      stats.retries = reader.queryRetries();
    }
    return stats;
  }

  /** @return the run-wide numbers of the baseline: the test, the image, the writers and their totals */
  protected JSONObject runSummary(final DatabaseWrapper.Protocol protocol, final double elapsedSeconds, final int writers,
      final int opsPerWriter, final int queryThreads, final FacetDatabaseWrapper.OpCounts writes) {
    return new JSONObject()//
        .put("test", getClass().getSimpleName()).put("protocol", protocol.name()).put("image", IMAGE).put("native", NATIVE)
        .put("finishedAt", Instant.now().toString()).put("writers", writers).put("opsPerWriter", opsPerWriter)
        .put("queryThreads", queryThreads).put("elapsedSeconds", round(elapsedSeconds)).put("ackedOps", writes.acked())
        .put("failedOps", writes.failed()).put("writeRetries", writes.retries())
        .put("throughputOpsPerSec", round(writes.acked() / elapsedSeconds)).put("writeMeanMs", writeMeans())
        .put("queryRetries", (long) total(QUERY_RETRIES));
  }

  /**
   * Puts the columnar server metrics of {@code node} into {@code target}: {@code fallbackScans} is the change since
   * {@code setupFallbackScans}, read once stable past the metrics refresh so a scan of the last seconds is counted.
   */
  protected static void putServerMetrics(final JSONObject target, final FacetDatabaseWrapper node, final long setupFallbackScans)
      throws InterruptedException {
    final long fallbackScans = stableScrape(node, FALLBACK_SCANS, 60) - setupFallbackScans;
    target.put("budgetWaits", (long) node.scrapeMetric(BUDGET_WAITS)).put("patchOverflows", (long) node.scrapeMetric(PATCH_OVERFLOWS))
        .put("patchedTotal", (long) node.scrapeMetric(PATCHED_TOTAL)).put("fallbackScans", fallbackScans)
        .put("fallbackScansSetup", setupFallbackScans).put("segments", (long) node.scrapeMetric(SEGMENTS))
        .put("rebuilds", node.rebuildsByReason()).put("rebuildsTotal", (long) node.scrapeMetric(REBUILDS))
        .put("rebuildsSnapshotInstall", (long) node.scrapeMetric(REBUILDS, "reason", SNAPSHOT_INSTALL));
  }

  /**
   * Fails when a container log holds a corrupted-record line. The facet answers can still equal {@code GROUP BY} then (a patch path that
   * drops one record goes unseen once the answer settles), so the log is the only witness. Counted per container, with the first lines.
   */
  protected void assertNoCorruptedReads(final JSONObject baseline) {
    final StringBuilder found = new StringBuilder();
    final JSONObject byContainer = new JSONObject();
    long total = 0L;
    for (final GenericContainer<?> container : containers) {
      long count = 0L;
      final List<String> first = new ArrayList<>();
      for (final String line : container.getLogs().split("\n"))
        for (final String marker : CORRUPTED_READS)
          if (line.contains(marker)) {
            if (count++ < FIRST_LINES)
              first.add(line);
            break;
          }
      total += count;
      byContainer.put(container.getContainerName(), count);
      if (count > 0)
        found.append(container.getContainerName()).append(": ").append(count).append(" line(s), first ").append(first).append("; ");
    }
    baseline.put("corruptedReads", total).put("corruptedReadsByContainer", byContainer);
    assertThat(total).as("corrupted-record lines in the container logs: " + found).isZero();
  }

  /** Writes the baseline, marked with {@code passed} so a red run leaves its numbers too, as not a baseline. */
  protected void writeBaseline(final DatabaseWrapper.Protocol protocol, final JSONObject baseline, final boolean passed)
      throws IOException {
    baseline.put("passed", passed);
    final Path file = Path.of("target", "facet-load", getClass().getSimpleName() + "-" + protocol.name() + ".json");
    Files.createDirectories(file.getParent());
    Files.writeString(file, baseline.toString(2));
    logger.info("Facet load summary {}: {} (written to {})", protocol, baseline, file.toAbsolutePath());
  }

  /** @return the mean client latency of each write operation over the whole run */
  private JSONObject writeMeans() {
    final JSONObject means = new JSONObject();
    for (final Timer timer : cumulative.find("arcadedb.test.facets.write").timers())
      if (timer.count() > 0)
        means.put(timer.getId().getTag("op"), round(timer.mean(TimeUnit.MICROSECONDS) / 1_000.0));
    return means;
  }

  /** @return the count of the client counter {@code name} over the whole run */
  protected double total(final String name) {
    return cumulative.find(name).counters().stream().mapToDouble(Counter::count).sum();
  }

  /**
   * @return the value of the counter {@code name} once it reads the same across more than one refresh of the server's columnar metrics
   * (every 5 s), so a count still being published is not taken as final
   */
  protected static long stableScrape(final FacetDatabaseWrapper node, final String name, final int timeoutSeconds)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    long last = (long) node.scrapeMetric(name);
    long stableSince = System.nanoTime();
    while (System.nanoTime() < deadline) {
      Thread.sleep(1_000L);
      final long current = (long) node.scrapeMetric(name);
      if (current != last) {
        last = current;
        stableSince = System.nanoTime();
      } else if (System.nanoTime() - stableSince > TimeUnit.SECONDS.toNanos(STABLE_SCRAPE_SECONDS))
        return current;
    }
    throw new AssertionError(name + " on " + node.describe() + " did not settle within " + timeoutSeconds + "s, last " + last);
  }

  protected static JSONObject percentiles(final Samples samples, final double divisor) {
    final long[] p = samples.percentiles();
    return new JSONObject().put("p50", round(p[0] / divisor)).put("p95", round(p[1] / divisor)).put("p99", round(p[2] / divisor))
        .put("max", round(p[3] / divisor));
  }

  protected static double round(final double value) {
    return Math.round(value * 100.0) / 100.0;
  }

  /** @return the number of assets in a count map: every asset falls in one size range or in {@code size.missing} */
  protected static long total(final SortedMap<String, Long> counts) {
    long total = 0L;
    for (final Map.Entry<String, Long> entry : counts.entrySet())
      if (entry.getKey().startsWith("size=") || entry.getKey().equals("size.missing"))
        total += entry.getValue();
    return total;
  }
}
