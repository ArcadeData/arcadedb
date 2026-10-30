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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.FacetDatabaseWrapper;
import com.arcadedb.test.support.FacetSnapshot;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Facet load on a three-node Raft cluster: three writers on node 0 (writes forward to the leader) run the mixed facet operations while
 * one query thread per node keeps querying that node's facets, so two of them read followers that lag. Once the writers end, every node
 * must hold the same data and its facets must equal its own {@code GROUP BY}, settled and exact, with no error, no fallback scan and no
 * snapshot install. Lag, latency, patched counts, throughput and each node's WAL files are recorded, never asserted: they are logged and
 * written to {@code target/facet-load/<class>-<protocol>.json}, one object per node, the baseline of the columnar tuning phase.
 */
class ThreeNodesFacetLoadTestIT extends FacetLoadTestTemplate {

  private static final String SERVER_LIST    = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";
  private static final int    NODES          = 3;
  private static final int    NUM_WRITERS    = 3;
  /** 45000 writes stay below the default snapshot threshold of 100000, so no node installs a snapshot. */
  private static final int    OPS_PER_WRITER = 15_000;
  private static final int    ADMIN_ID       = 1_000;

  @AfterEach
  @Override
  public void tearDown() {
    // Skip compareAllDatabases(): with non-persistent containers, database files are not on the
    // host after stop. The test body already verifies convergence through assertSameFacetsOnEveryNode.
    super.tearDown();
  }

  @DisplayName("Three-node Raft HA facet load test")
  @ParameterizedTest(name = "Three-node facet load test with {0}")
  @EnumSource(DatabaseWrapper.Protocol.class)
  @Timeout(value = 40, unit = TimeUnit.MINUTES) // hang detector, not a latency bound
  void threeNodeFacetLoad(final DatabaseWrapper.Protocol protocol) throws Exception {
    for (int i = 0; i < NODES; i++)
      createArcadeContainer("arcadedb-" + i, SERVER_LIST, "majority", network);
    final List<ServerWrapper> servers = startCluster();

    startCumulativeMeters();
    // one client per node for the setup and the checks; node 0's also creates the database and the schema
    try (final FacetDatabaseWrapper r0 = new FacetDatabaseWrapper(servers.get(0), protocol, ADMIN_ID);
        final FacetDatabaseWrapper r1 = new FacetDatabaseWrapper(servers.get(1), protocol, ADMIN_ID + 1);
        final FacetDatabaseWrapper r2 = new FacetDatabaseWrapper(servers.get(2), protocol, ADMIN_ID + 2)) {
      runLoad(servers, protocol, List.of(r0, r1, r2));
    } finally {
      stopCumulativeMeters();
      logAndDumpContainerLogs("threeNodesFacetLoad-" + protocol.name());
    }
  }

  private void runLoad(final List<ServerWrapper> servers, final DatabaseWrapper.Protocol protocol, final List<FacetDatabaseWrapper> nodes)
      throws Exception {
    final FacetDatabaseWrapper admin = nodes.getFirst();
    admin.createDatabase();
    admin.createSchema();
    // every node serves the facets from the index before the first write (a write racing an async create would count a rebuild)
    final long[] schemaMillis = new long[NODES];
    final long[] setupFallbackScans = new long[NODES];
    for (int i = 0; i < NODES; i++)
      schemaMillis[i] = nodes.get(i).awaitSchema(120);
    // the setup poll may scan, by design and exactly (an added column publishes by an async create rebuild): the bound below applies to
    // the change over the measured run, from a count read once it is stable past the metrics refresh
    for (int i = 0; i < NODES; i++)
      setupFallbackScans[i] = stableScrape(nodes.get(i), FALLBACK_SCANS, 60);
    // the setup poll of a follower can query before the database reached it ("does not exist"), which the wrapper counts as an error
    final long setupQueryErrors = (long) total(QUERY_ERRORS);
    final int leaderAtStart = findLeaderIndex(servers);
    logger.info("Facet load test on protocol {}: index served after {} ms per node, setup fallback scans {}, setup query errors {}, "
        + "leader node {}", protocol, Arrays.toString(schemaMillis), Arrays.toString(setupFallbackScans), setupQueryErrors, leaderAtStart);
    logger.info("Running {} writers x {} mixed ops on node 0 and one query thread per node", NUM_WRITERS, OPS_PER_WRITER);

    final long startNanos = System.nanoTime();
    final ExecutorService writers = Executors.newFixedThreadPool(NUM_WRITERS);
    final List<Future<FacetDatabaseWrapper.OpCounts>> writes = startWriters(writers, servers.getFirst(), protocol, NUM_WRITERS,
        OPS_PER_WRITER);

    final List<AtomicReference<FacetSnapshot>> lastSamples = new ArrayList<>();
    final ExecutorService queriers = Executors.newFixedThreadPool(NODES);
    final List<Future<QueryStats>> queries = new ArrayList<>();
    for (int q = 0; q < NODES; q++) {
      final ServerWrapper server = servers.get(q);
      final int queryId = QUERY_ID_BASE + q;
      final AtomicReference<FacetSnapshot> lastSample = new AtomicReference<>();
      lastSamples.add(lastSample);
      queries.add(queriers.submit(() -> runQueries(server, protocol, queryId, writers, lastSample)));
    }
    queriers.shutdown();

    final long[] maxOverlayRows = new long[NODES];
    final long[] maxApplyLagMs = new long[NODES];
    while (!writers.isTerminated()) {
      final StringBuilder progress = new StringBuilder();
      for (int i = 0; i < NODES; i++) {
        try {
          final FacetDatabaseWrapper node = nodes.get(i);
          final FacetSnapshot last = lastSamples.get(i).get();
          maxOverlayRows[i] = Math.max(maxOverlayRows[i], (long) node.scrapeMetric(OVERLAY_ROWS));
          maxApplyLagMs[i] = Math.max(maxApplyLagMs[i], (long) node.scrapeMetric(APPLY_LAG));
          progress.append(" n").append(i).append(": assets=").append(total(node.groupBy(""))).append(" lagMs=")
              .append(last == null ? "-" : last.lagMs()).append(" patched=").append(last == null ? "-" : last.patched());
        } catch (final Exception e) {
          progress.append(" n").append(i).append(": ").append(e.getMessage());
        }
      }
      logger.info("Progress:{}", progress);
      writers.awaitTermination(5, TimeUnit.SECONDS);
    }
    final double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;

    final FacetDatabaseWrapper.OpCounts written = sum(writes);
    assertThat(queriers.awaitTermination(2, TimeUnit.MINUTES)).as("the query threads stop once the writers end").isTrue();
    final QueryStats[] perNode = new QueryStats[NODES];
    for (int q = 0; q < NODES; q++)
      perNode[q] = queries.get(q).get();
    logger.info("Writers ended after {} s: {} acked, {} failed, {} retries; queries per node {} / {} / {}",
        String.format("%.1f", elapsedSeconds), written.acked(), written.failed(), written.retries(), perNode[0].count(),
        perNode[1].count(), perNode[2].count());

    // written in any case, so a red run leaves its numbers too, marked as not a baseline
    final JSONObject baseline = runSummary(protocol, elapsedSeconds, NUM_WRITERS, OPS_PER_WRITER, NODES, written).put("leaderAtStart",
        leaderAtStart).put("queryErrorsSetup", setupQueryErrors);
    final JSONObject[] nodeBaselines = new JSONObject[NODES];
    final JSONArray nodeArray = new JSONArray();
    for (int i = 0; i < NODES; i++) {
      nodeBaselines[i] = perNode[i].putInto(new JSONObject().put("node", i).put("address", nodes.get(i).describe()), elapsedSeconds)
          .put("schemaMillis", schemaMillis[i]).put("overlayRows", new JSONObject().put("max", maxOverlayRows[i]))
          .put("applyLagMs", new JSONObject().put("max", maxApplyLagMs[i]));
      nodeArray.put(nodeBaselines[i]);
    }
    baseline.put("nodes", nodeArray);
    boolean passed = false;
    try {
      // preconditions: the run did write and every node was queried, so the checks below compare real data and real samples
      assertThat(written.acked()).as("acknowledged writes").isGreaterThan(0L);
      for (int i = 0; i < NODES; i++)
        assertThat(perNode[i].count()).as("facet queries answered by node " + i + " while the writers ran").isGreaterThan(0L);

      FacetDatabaseWrapper.assertSameFacetsOnEveryNode(nodes, 180);
      for (int i = 0; i < NODES; i++) {
        final FacetDatabaseWrapper r = nodes.get(i);
        final FacetSnapshot settled = r.awaitSettledFacets("", 120);
        assertThat(settled.exact()).as(r.describe()).isTrue();
        assertThat(total(settled.counts())).as("assets counted by the settled facets of " + r.describe()).isGreaterThan(0L);
        nodeBaselines[i].put("assets", total(settled.counts()));
      }

      Metrics.globalRegistry.getMeters().forEach(meter -> logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure()));

      // every node's numbers first, so a failed check below, or a node whose counter never settles, still leaves all of them
      AssertionError unstable = null;
      for (int i = 0; i < NODES; i++) {
        final FacetDatabaseWrapper r = nodes.get(i);
        try {
          putServerMetrics(nodeBaselines[i], r, setupFallbackScans[i]);
        } catch (final AssertionError e) {
          nodeBaselines[i].put("metricsError", e.getMessage());
          if (unstable == null)
            unstable = e;
        }
        // overlay rows and apply lag after the settle are about 0 by construction: "max" (sampled every 5 s) is the informative one
        nodeBaselines[i].getJSONObject("overlayRows").put("final", (long) r.scrapeMetric(OVERLAY_ROWS));
        nodeBaselines[i].getJSONObject("applyLagMs").put("final", (long) r.scrapeMetric(APPLY_LAG));
      }
      if (unstable != null)
        throw unstable;
      for (int i = 0; i < NODES; i++) {
        final String node = nodes.get(i).describe() + " (node " + i + ")";
        final JSONObject n = nodeBaselines[i];
        // precondition of the two checks below: the columnar series are bound, and every series of the database is bound with them
        assertThat(n.getLong("segments", 0L)).as(node + ": columnar segments on disk, so the columnar series are exported")
            .isGreaterThan(0L);
        assertThat(n.getLong("fallbackScans", -1L)).as(node + ": fallback scans during the measured run (after " + setupFallbackScans[i]
            + " during the setup): projection present the whole run").isZero();
        assertThat(n.getLong("rebuildsSnapshotInstall", -1L)).as(node + ": 45000 writes stay below the snapshot threshold, so no "
            + "snapshot install; rebuilds " + n.getString("rebuilds", "")).isZero();
        // no restart and the type is empty when its indexes are created: no create, missing or corrupt rebuild on any node either
        assertThat(n.getLong("rebuildsTotal", -1L)).as(node + ": rebuilds of any reason " + n.getString("rebuilds", "")).isZero();
      }

      // totals of the whole run, which sumCounter (the last 10 s step) is not; the setup and check clients' queries count too
      assertThat(written.failed()).as("writes failed (after " + FacetDatabaseWrapper.RETRIES + " retries of a conflict)").isZero();
      for (int i = 0; i < NODES; i++)
        assertThat(perNode[i].failed).as("facet queries failed on node " + i + " while the writers ran").isZero();
      assertThat((long) total(QUERY_ERRORS) - setupQueryErrors).as("facet query errors after the setup (" + setupQueryErrors
          + " during it), every client").isZero();
      // every client's samples, the query threads' and the setup and check clients': per node in the baseline as metaInconsistent.
      // Ruling B-M1: logged and counted, not a failure
      if (total(META_INCONSISTENT) > 0)
        logger.warn("{} facet samples were inexact without lag (each logged above)", (long) total(META_INCONSISTENT));
      assertThat(total(WRITE_ERRORS)).isZero();
      assertNoCorruptedReads(baseline);
      passed = true;
    } finally {
      baseline.put("leaderAtEnd", findLeaderIndex(servers));
      for (int i = 0; i < NODES; i++)
        nodeBaselines[i].put("wal", walFiles(containers.get(i))).put("scanReads", scanReads(containers.get(i)));
      writeBaseline(protocol, baseline, passed);
    }
  }

  /**
   * @return how often a record scan of this node met a chunk chain it could not read: {@code skipped} counts the scans that went on
   * without the record ("Scan of bucket N skipped M record(s) that could not be read"), {@code brokenChain} the reads that found the
   * chain broken. On a follower these are reads of a chain a replicated apply is still writing (#10941), logged as evidence
   */
  private JSONObject scanReads(final GenericContainer<?> container) {
    final JSONObject reads = new JSONObject();
    try {
      final String logs = container.getLogs();
      reads.put("skipped", occurrences(logs, "record(s) that could not be read")).put("brokenChain", occurrences(logs, "broken chunk chain"));
    } catch (final RuntimeException e) {
      reads.put("error", e.toString());
    }
    logger.info("Scan reads of {}: {}", container.getContainerName(), reads);
    return reads;
  }

  private static long occurrences(final String text, final String marker) {
    long count = 0L;
    for (int from = text.indexOf(marker); from >= 0; from = text.indexOf(marker, from + marker.length()))
      count++;
    return count;
  }

  /**
   * @return the count and total size of the database's WAL files on one node, logged with the listing: one database-wide counter of
   * queued columnar marks may delay the WAL drops under sustained load, which these numbers show
   */
  private JSONObject walFiles(final GenericContainer<?> container) {
    final JSONObject wal = new JSONObject();
    try {
      // the directory, not a glob: a database with no WAL file at the moment lists 0 files rather than an error
      final Container.ExecResult result = container.execInContainer("ls", "-ln", "/home/arcadedb/databases/" + DATABASE);
      final StringBuilder listing = new StringBuilder();
      long files = 0L;
      long bytes = 0L;
      for (final String line : result.getStdout().split("\n")) {
        final String[] fields = line.trim().split("\\s+");
        if (line.startsWith("-") && fields.length >= 9 && fields[fields.length - 1].endsWith(".wal")) {
          files++;
          bytes += Long.parseLong(fields[4]);
          listing.append(line).append('\n');
        }
      }
      wal.put("files", files).put("bytes", bytes);
      if (result.getExitCode() != 0)
        wal.put("error", result.getStderr().trim());
      logger.info("WAL files of {}: {} files, {} bytes\n{}", container.getContainerName(), files, bytes, listing);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      wal.put("error", e.toString());
    } catch (final IOException | RuntimeException e) {
      wal.put("error", e.toString());
      logger.warn("Could not list the WAL files of {}: {}", container.getContainerName(), e.toString());
    }
    return wal;
  }
}
