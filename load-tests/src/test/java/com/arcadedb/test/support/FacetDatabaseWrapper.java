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
package com.arcadedb.test.support;

import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.serializer.json.JSONArray;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;
import static com.arcadedb.test.support.ContainersTestTemplate.PASSWORD;

/**
 * Client of one node for the facet tests: schema, mixed writes on the keys this wrapper owns, facet queries with client metrics, stable
 * comparisons against {@code GROUP BY} and a scrape of the node's {@code /prometheus}. Writes and queries go through the remote
 * clients ({@code RemoteDatabase} or {@code RemoteGrpcDatabase}), so plan 4's client path is exercised too. Not thread-safe: one
 * instance per writer thread. Every caller runs {@link #awaitSchema(int)} on every node before its first write.
 */
public class FacetDatabaseWrapper implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(FacetDatabaseWrapper.class);

  private static final long POLL_MILLIS = 500L;

  private enum Op {
    INSERT, INSERT_NO_BRAND, UPDATE_VALUES, REMOVE_BRAND, GROW, UPDATE_GROWN, DELETE
  }

  /** @param acked operations the server acknowledged @param failed operations that raised an error */
  public record OpCounts(long acked, long failed) {
  }

  private record Stable(FacetSnapshot facets, SortedMap<String, Long> groupBy) {
  }

  /** A growable array of primitive keys with swap-remove. */
  private static final class KeyList {
    private long[] keys = new long[64];
    private int    size;

    void add(final long key) {
      if (size == keys.length)
        keys = Arrays.copyOf(keys, size * 2);
      keys[size++] = key;
    }

    long swapRemove(final int index) {
      final long key = keys[index];
      keys[index] = keys[--size];
      return key;
    }

    long get(final int index) {
      return keys[index];
    }
  }

  private final ServerWrapper  server;
  private final int            writerId;
  private final RemoteDatabase db;
  private final AssetGenerator generator;
  private final Random         opRandom;
  private final KeyList        live  = new KeyList();
  private final KeyList        grown = new KeyList();
  private       long           nextSeq;

  private final Timer[]             writeTimers = new Timer[Op.values().length];
  private final Counter             writeErrors;
  private final Timer               queryTimer;
  private final DistributionSummary lagSummary;
  private final DistributionSummary patchedSummary;
  private final Counter             exactCounter;
  private final Counter             nonExactCounter;
  private final Counter             queryErrors;
  private final Counter             metaInconsistent;

  /**
   * @param writerId owns the keys {@code (writerId << 32) | seq}; also the seed of the value generator
   */
  public FacetDatabaseWrapper(final ServerWrapper server, final DatabaseWrapper.Protocol protocol, final int writerId) {
    this.server = server;
    this.writerId = writerId;
    this.db = connect(protocol);
    this.generator = new AssetGenerator(writerId);
    this.opRandom = new Random(writerId ^ 0x5DEECE66DL);

    for (final Op op : Op.values())
      writeTimers[op.ordinal()] = Metrics.timer("arcadedb.test.facets.write", "op", op.name().toLowerCase());
    writeErrors = Metrics.counter("arcadedb.test.facets.write.error");
    queryTimer = Timer.builder("arcadedb.test.facets.query").publishPercentiles(0.5, 0.95, 0.99).register(Metrics.globalRegistry);
    lagSummary = DistributionSummary.builder("arcadedb.test.facets.lagMs").publishPercentiles(0.5, 0.95, 0.99)
        .register(Metrics.globalRegistry);
    patchedSummary = DistributionSummary.builder("arcadedb.test.facets.patched").publishPercentiles(0.5, 0.95, 0.99)
        .register(Metrics.globalRegistry);
    exactCounter = Metrics.counter("arcadedb.test.facets.exact");
    nonExactCounter = Metrics.counter("arcadedb.test.facets.nonexact");
    queryErrors = Metrics.counter("arcadedb.test.facets.query.error");
    metaInconsistent = Metrics.counter("arcadedb.test.facets.meta.inconsistent");
  }

  private RemoteDatabase connect(final DatabaseWrapper.Protocol protocol) {
    return switch (protocol) {
      case HTTP -> {
        final RemoteDatabase database = new RemoteDatabase(server.host(), server.httpPort(), DATABASE, "root", PASSWORD);
        database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        database.setTimeout(30000);
        yield database;
      }
      case GRPC -> {
        final RemoteGrpcServer grpcServer = new RemoteGrpcServer(server.host(), server.grpcPort(), "root", PASSWORD, true, List.of());
        yield new RemoteGrpcDatabase(grpcServer, server.host(), server.grpcPort(), server.httpPort(), DATABASE, "root", PASSWORD);
      }
    };
  }

  @Override
  public void close() {
    db.close();
  }

  /** @return {@code host:port} of this node, for messages */
  public String describe() {
    return server.host() + ":" + server.httpPort();
  }

  public void createDatabase() {
    DatabaseWrapper.createDatabase(server);
  }

  public void createSchema() {
    for (final String ddl : FacetSchema.DDL)
      try (final ResultSet ignored = db.command("sql", ddl)) {
        // the statement ran
      }
  }

  /**
   * Waits until every facet of the unfiltered query answers from the index at lag 0 on this node: a COLUMNAR index created on an empty
   * type publishes asynchronously, and a write racing it would count a rebuild and read through fallback scans.
   *
   * @return milliseconds waited
   */
  public long awaitSchema(final int timeoutSeconds) {
    final long start = System.nanoTime();
    final long deadline = start + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    Throwable last = null;
    while (System.nanoTime() < deadline) {
      try {
        if (queryFacets("").settled())
          return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      } catch (final RuntimeException e) {
        last = e;
      }
      sleep();
    }
    throw new AssertionError("Node " + describe() + " did not serve the facets from the index within " + timeoutSeconds + "s"
        + (last != null ? ": " + last.getMessage() : ""), last);
  }

  /** @return the number of acknowledged inserts */
  public int ingest(final int count) {
    int acked = 0;
    for (int i = 0; i < count; i++)
      if (insert())
        acked++;
    return acked;
  }

  public OpCounts runMixedOps(final int ops) {
    long acked = 0L;
    long failed = 0L;
    for (int i = 0; i < ops; i++) {
      if (mixedOp())
        acked++;
      else
        failed++;
    }
    return new OpCounts(acked, failed);
  }

  private boolean mixedOp() {
    final int roll = opRandom.nextInt(100);
    // INSERT 45, UPDATE_VALUES 25, REMOVE_BRAND 2, GROW 8, UPDATE_GROWN 8, DELETE 12
    if (roll < 45 || live.size == 0)
      return insert();
    if (roll < 70) {
      final long aid = live.get(opRandom.nextInt(live.size));
      return write(Op.UPDATE_VALUES, FacetSchema.UPDATE_VALUES, generator.valuesParams(aid));
    }
    if (roll < 72) {
      final long aid = live.get(opRandom.nextInt(live.size));
      return write(Op.REMOVE_BRAND, FacetSchema.REMOVE_BRAND, generator.keyParams(aid));
    }
    if (roll < 80) {
      final long aid = live.get(opRandom.nextInt(live.size));
      grown.add(aid);
      return write(Op.GROW, FacetSchema.GROW, generator.growParams(aid));
    }
    if (roll < 88) {
      if (grown.size == 0)
        return insert();
      // the key may have been deleted since: the UPDATE then matches nothing, which is harmless
      final long aid = grown.get(opRandom.nextInt(grown.size));
      return write(Op.UPDATE_GROWN, FacetSchema.UPDATE_GROWN, generator.valuesParams(aid));
    }
    // removed from the live keys on any outcome: a failed delete may still have committed
    final long aid = live.swapRemove(opRandom.nextInt(live.size));
    return write(Op.DELETE, FacetSchema.DELETE, generator.keyParams(aid));
  }

  private boolean insert() {
    final long aid = ((long) writerId << 32) | nextSeq++;
    final boolean noBrand = generator.omitBrand();
    final boolean acked = write(noBrand ? Op.INSERT_NO_BRAND : Op.INSERT, noBrand ? FacetSchema.INSERT_NO_BRAND : FacetSchema.INSERT,
        generator.insertParams(aid));
    if (acked)
      live.add(aid);
    return acked;
  }

  private boolean write(final Op op, final String sql, final Map<String, Object> params) {
    final long start = System.nanoTime();
    try (final ResultSet rs = db.command("sql", sql, params)) {
      while (rs.hasNext())
        rs.next();
    } catch (final RuntimeException e) {
      writeErrors.increment();
      logger.debug("{} on {} failed: {}", op, describe(), e.getMessage());
      return false;
    }
    writeTimers[op.ordinal()].record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    return true;
  }

  /** Runs the {@code FACETS} query matching {@code where} and records the client metrics. */
  public FacetSnapshot queryFacets(final String where) {
    final long start = System.nanoTime();
    final FacetSnapshot snapshot;
    try (final ResultSet rs = db.query("sql", FacetSchema.facetQuery(where))) {
      // the facets ride after the last row: read the rows first
      while (rs.hasNext())
        rs.next();
      final FacetResults results = rs.getFacets();
      if (results == null)
        throw new IllegalStateException("No facets in the response of " + describe());
      // exact for string/long keys only (FacetAsset): read the HTTP JSON instead if a temporal facet is added
      snapshot = FacetSnapshot.of(results);
    } catch (final RuntimeException e) {
      queryErrors.increment();
      throw e;
    }
    queryTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    lagSummary.record(snapshot.lagMs());
    patchedSummary.record(snapshot.patched());
    (snapshot.exact() ? exactCounter : nonExactCounter).increment();
    if (!snapshot.metaConsistent())
      metaInconsistent.increment();
    return snapshot;
  }

  /** @return the counts of the same facets computed with {@code GROUP BY} over the records */
  public SortedMap<String, Long> groupBy(final String where) {
    try {
      return FacetSchema.expectedCounts(this::rows, where);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private JSONArray rows(final String sql) {
    final JSONArray rows = new JSONArray();
    try (final ResultSet rs = db.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.put(row.toJSON());
      }
    }
    return rows;
  }

  /**
   * @return the first settled facet sample equal to a stable {@code GROUP BY}
   *
   * @throws AssertionError when no stable settled sample appears in time, or when it differs from the {@code GROUP BY}
   */
  public FacetSnapshot awaitSettledFacets(final String where, final int timeoutSeconds) {
    final Stable stable = stableRead(where, timeoutSeconds, true);
    assertEqual(where, stable);
    return stable.facets;
  }

  /** Asserts, for every checked filter, that the facets equal the {@code GROUP BY} on this node, first when comparable, then settled. */
  public void assertFacetsMatchGroupBy(final int timeoutSeconds) {
    for (final String where : FacetSchema.CHECKED_FILTERS) {
      assertEqual(where, stableRead(where, timeoutSeconds, false));
      assertEqual(where, stableRead(where, timeoutSeconds, true));
    }
  }

  private void assertEqual(final String where, final Stable stable) {
    final List<String> diff = FacetSnapshot.diff(stable.groupBy, stable.facets.counts(), 20);
    if (!diff.isEmpty())
      throw new AssertionError(describe() + " where='" + where + "' " + diff);
  }

  /**
   * Reads {@code GROUP BY}, polls the facets until one is comparable (and settled when asked), reads {@code GROUP BY} again and retries
   * while the two reads differ: writes still being applied move the truth between them.
   */
  private Stable stableRead(final String where, final int timeoutSeconds, final boolean settled) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    String last = "no sample";
    while (System.nanoTime() < deadline) {
      try {
        final SortedMap<String, Long> before = groupBy(where);
        FacetSnapshot sample = null;
        while (sample == null && System.nanoTime() < deadline) {
          final FacetSnapshot candidate = queryFacets(where);
          if (candidate.comparable() && (!settled || candidate.settled()))
            sample = candidate;
          else {
            last = "not " + (settled ? "settled" : "comparable") + ": exact=" + candidate.exact() + " lagMs=" + candidate.lagMs()
                + " sources=" + candidate.sources();
            sleep();
          }
        }
        if (sample == null)
          break; // the deadline passed while polling: `last` holds the latest rejected sample
        if (before.equals(groupBy(where)))
          return new Stable(sample, before);
        last = "the GROUP BY moved between the reads";
      } catch (final RuntimeException e) {
        // the node may be catching up
        last = e.toString();
        sleep();
      }
    }
    throw new AssertionError(
        "Node " + describe() + " where='" + where + "' gave no stable " + (settled ? "settled" : "comparable") + " facets within "
            + timeoutSeconds + "s: " + last);
  }

  /**
   * Waits until every facet of the unfiltered query is served by the index at lag 0 again, checking every comparable sample against a
   * stable {@code GROUP BY} on the way.
   *
   * @return the distinct sequence of per-facet source tuples seen
   */
  public List<String> awaitRebuildToIndex(final int timeoutSeconds) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    final List<String> sequence = new ArrayList<>();
    boolean usable = false;
    String last = "no sample";
    while (System.nanoTime() < deadline) {
      try {
        final FacetSnapshot sample = queryFacets("");
        final String tuple = String.join(",", sample.sources().values());
        if (sequence.isEmpty() || !sequence.get(sequence.size() - 1).equals(tuple))
          sequence.add(tuple);
        final boolean allIndex = sample.sources().values().stream().allMatch("index"::equals);
        if (usable && !allIndex)
          throw new AssertionError(describe() + ": projection fell back after it was usable: " + sequence);
        if (sample.comparable())
          checkAgainstGroupBy(sequence);
        usable |= allIndex;
        if (sample.settled())
          return sequence;
        last = "sources=" + sample.sources() + " lagMs=" + sample.lagMs();
      } catch (final RuntimeException e) {
        last = e.toString();
      }
      sleep();
    }
    throw new AssertionError("Node " + describe() + " did not rebuild to the index within " + timeoutSeconds + "s: " + last
        + " sequence=" + sequence);
  }

  /** Takes a facet sample bracketed by two GROUP BY reads (the truth scans run only for a comparable probe) and compares them if stable. */
  private void checkAgainstGroupBy(final List<String> sequence) {
    final SortedMap<String, Long> before = groupBy("");
    final FacetSnapshot bracketed = queryFacets("");
    if (bracketed.comparable() && before.equals(groupBy(""))) {
      final List<String> diff = FacetSnapshot.diff(before, bracketed.counts(), 20);
      if (!diff.isEmpty())
        throw new AssertionError(describe() + " comparable sample differs from GROUP BY: " + diff + " sources=" + sequence);
    }
  }

  /** @return the sum of the series named {@code name} on this node's {@code /prometheus}, 0 when absent */
  public double scrapeMetric(final String name) {
    return PrometheusScrape.sum(scrape(), name);
  }

  /** @return the sum of the series named {@code name} carrying {@code label="value"}, 0 when absent */
  public double scrapeMetric(final String name, final String label, final String value) {
    return PrometheusScrape.sum(scrape(), name, label, value);
  }

  private String scrape() {
    final String auth = "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    try {
      final HttpURLConnection conn = (HttpURLConnection) URI.create("http://" + server.host() + ":" + server.httpPort() + "/prometheus")
          .toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Authorization", auth);
      conn.setConnectTimeout(5_000);
      conn.setReadTimeout(10_000);
      try {
        if (conn.getResponseCode() != 200)
          throw new IOException("Prometheus scrape of " + describe() + " failed: HTTP " + conn.getResponseCode() + " " + errorBody(conn));
        try (final InputStream is = conn.getInputStream()) {
          return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
      } finally {
        conn.disconnect();
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String errorBody(final HttpURLConnection conn) {
    try (final InputStream is = conn.getErrorStream()) {
      return is == null ? "" : new String(is.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      return "(no error body: " + e.getMessage() + ")";
    }
  }

  /**
   * Waits until the {@code GROUP BY} is the same on every node, for every checked filter, in two consecutive rounds, then asserts the
   * facets of each node against its own {@code GROUP BY}.
   */
  public static void assertSameFacetsOnEveryNode(final List<FacetDatabaseWrapper> nodes, final int timeoutSeconds) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    int equalRounds = 0;
    String last = "no round";
    while (System.nanoTime() < deadline && equalRounds < 2) {
      try {
        final StringBuilder differences = new StringBuilder();
        for (final String where : FacetSchema.CHECKED_FILTERS) {
          final SortedMap<String, Long> reference = nodes.get(0).groupBy(where);
          for (int i = 1; i < nodes.size(); i++) {
            final List<String> diff = FacetSnapshot.diff(reference, nodes.get(i).groupBy(where), 10);
            if (!diff.isEmpty())
              differences.append(nodes.get(i).describe()).append(" vs ").append(nodes.get(0).describe()).append(" where='")
                  .append(where).append("' ").append(diff).append("; ");
          }
        }
        if (differences.isEmpty())
          equalRounds++;
        else {
          equalRounds = 0;
          last = differences.toString();
        }
      } catch (final RuntimeException e) {
        equalRounds = 0;
        last = e.toString();
      }
      if (equalRounds < 2)
        sleep();
    }
    if (equalRounds < 2)
      throw new AssertionError("The nodes did not converge to the same data within " + timeoutSeconds + "s: " + last);

    for (final FacetDatabaseWrapper node : nodes)
      node.assertFacetsMatchGroupBy(timeoutSeconds);
  }

  private static void sleep() {
    try {
      Thread.sleep(POLL_MILLIS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting", e);
    }
  }
}
