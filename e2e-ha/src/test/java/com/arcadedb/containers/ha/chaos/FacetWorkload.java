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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.AssetGenerator;
import com.arcadedb.test.support.FacetSchema;
import com.arcadedb.test.support.FacetSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * K writer threads, each a closed loop over the {@code FacetAsset} type: pick a node, send one mixed write (insert, update,
 * brand removal, growth, delete, link) or one facet query, think briefly. Each operation is attempted exactly once, to any node
 * whatever its state. Each writer owns its keys, {@code (writer << 32) | sequence}, so writers never conflict, and has its own
 * random and its own {@link AssetGenerator}, so the statement sequence does not depend on thread interleaving. Outcomes are not
 * ledgered: {@link FacetCheck} compares every node with itself and with its peers.
 */
public final class FacetWorkload implements LoadGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(FacetWorkload.class);

  private static final int  DEFAULT_CONNECT_TIMEOUT_MS = 2_000;
  private static final int  DEFAULT_READ_TIMEOUT_MS    = 15_000;
  private static final int  DEFAULT_THINK_MS           = 10;
  private static final long WRITER_SEED_MIX            = 0x5DEECE66DL;

  // cumulative upper bounds, in percent, of the operation mix
  private static final int INSERT_UPTO        = 35;
  private static final int UPDATE_VALUES_UPTO = 55;
  private static final int REMOVE_BRAND_UPTO  = 58;
  private static final int GROW_UPTO          = 68;
  private static final int UPDATE_GROWN_UPTO  = 78;
  private static final int DELETE_UPTO        = 90;
  private static final int LINK_UPTO          = 95;

  /**
   * Counters of the whole workload. A 5xx on a query or an unreachable node is expected while faults are in progress and
   * never a violation; {@code queryFailures} counts the answers that no fault explains (a non-200, non-5xx status or a body
   * that does not parse), so a broken facet statement shows in the run summary instead of vanishing. {@code metaInconsistent}
   * counts the answers that are inexact yet report no lag, sampled under faults (the checkpoint only samples a quiesced
   * cluster); like the checkpoint's {@code FACET_META}, they are a warning, never a violation.
   */
  public record Stats(long writesOk, long writesFailed, long queries, long queryServerErrors, long queryFailures,
                      long nonExact, long metaInconsistent) {
    public JSONObject toJSON() {
      return new JSONObject().put("writesOk", writesOk).put("writesFailed", writesFailed).put("queries", queries)
          .put("queryServerErrors", queryServerErrors).put("queryFailures", queryFailures).put("nonExact", nonExact)
          .put("metaInconsistent", metaInconsistent);
    }

    /** @return the run-report warnings these counters raise, empty when nothing needs a look */
    public List<String> warnings() {
      final List<String> warnings = new ArrayList<>(2);
      if (queryFailures > 0)
        warnings.add("FACET_QUERY_FAILURES " + queryFailures
            + " facet queries answered with neither a 5xx nor a parsable result (first cause in the log)");
      if (metaInconsistent > 0)
        warnings.add("FACET_META workload " + metaInconsistent + " samples inexact without lag (first sample in the log)");
      return warnings;
    }
  }

  private final Endpoints              endpoints;
  private final long                   seed;
  private final int                    writers;
  private final String                 commandPath;
  private final String                 queryPath;
  private final int                    connectTimeoutMs;
  private final int                    readTimeoutMs;
  private final int                    thinkMs;
  private final ReentrantReadWriteLock gate              = new ReentrantReadWriteLock(true);
  private final AtomicBoolean          running           = new AtomicBoolean();
  private final List<Thread>           threads           = new ArrayList<>();
  private final AtomicLong             writesOk          = new AtomicLong();
  private final AtomicLong             writesFailed      = new AtomicLong();
  private final AtomicLong             queries           = new AtomicLong();
  private final AtomicLong             queryServerErrors = new AtomicLong();
  private final AtomicLong             queryFailures     = new AtomicLong();
  private final AtomicLong             nonExact          = new AtomicLong();
  private final AtomicLong             metaInconsistent  = new AtomicLong();
  private final AtomicBoolean          failureLogged     = new AtomicBoolean();
  private final AtomicBoolean          metaLogged        = new AtomicBoolean();

  public FacetWorkload(final long seed, final int writers, final Endpoints endpoints, final String database) {
    this(seed, writers, endpoints, database, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, DEFAULT_THINK_MS);
  }

  public FacetWorkload(final long seed, final int writers, final Endpoints endpoints, final String database,
      final int connectTimeoutMs, final int readTimeoutMs, final int thinkMs) {
    this.seed = seed;
    this.writers = writers;
    this.endpoints = endpoints;
    this.commandPath = "/api/v1/command/" + database;
    this.queryPath = "/api/v1/query/" + database;
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.thinkMs = thinkMs;
  }

  public Stats stats() {
    return new Stats(writesOk.get(), writesFailed.get(), queries.get(), queryServerErrors.get(), queryFailures.get(),
        nonExact.get(), metaInconsistent.get());
  }

  @Override
  public void start() {
    running.set(true);
    for (int w = 0; w < writers; w++) {
      final int writer = w;
      final long writerSeed = seed ^ (WRITER_SEED_MIX * (writer + 1));
      final Random random = new Random(writerSeed);
      final AssetGenerator generator = new AssetGenerator(writerSeed);
      final Thread thread = new Thread(() -> loop(writer, random, generator), "chaos-facet-writer-" + writer);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  private void loop(final int writer, final Random random, final AssetGenerator generator) {
    long nextSeq = 0;
    while (running.get()) {
      gate.readLock().lock();
      try {
        if (!running.get())
          return;
        if (operation(writer, nextSeq, random, generator))
          ++nextSeq;
      } finally {
        gate.readLock().unlock();
      }
      try {
        Thread.sleep(thinkMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** @return true when the operation consumed a new key, which is then owned by this writer whatever the outcome */
  private boolean operation(final int writer, final long nextSeq, final Random random, final AssetGenerator generator) {
    final int node = random.nextInt(endpoints.size());
    final int pick = random.nextInt(100);
    if (pick >= LINK_UPTO) {
      query(node, FacetSchema.CHECKED_FILTERS.get(random.nextInt(FacetSchema.CHECKED_FILTERS.size())));
      return false;
    }
    final long base = (long) writer << 32;
    if (pick < INSERT_UPTO || nextSeq == 0) {
      final long aid = base | nextSeq;
      final boolean noBrand = generator.omitBrand();
      write(node, noBrand ? FacetSchema.INSERT_NO_BRAND : FacetSchema.INSERT, generator.insertParams(aid));
      return true;
    }
    final long aid = base | (random.nextLong() & Long.MAX_VALUE) % nextSeq;
    if (pick < UPDATE_VALUES_UPTO)
      write(node, FacetSchema.UPDATE_VALUES, generator.valuesParams(aid));
    else if (pick < REMOVE_BRAND_UPTO)
      write(node, FacetSchema.REMOVE_BRAND, generator.keyParams(aid));
    else if (pick < GROW_UPTO)
      write(node, FacetSchema.GROW, generator.growParams(aid));
    else if (pick < UPDATE_GROWN_UPTO)
      write(node, FacetSchema.UPDATE_GROWN, generator.valuesParams(aid));
    else if (pick < DELETE_UPTO)
      write(node, FacetSchema.DELETE, generator.keyParams(aid));
    else
      write(node, FacetSchema.LINK, generator.linkParams(aid, base | (random.nextLong() & Long.MAX_VALUE) % nextSeq));
    return false;
  }

  private void write(final int node, final String statement, final Map<String, Object> params) {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", statement).put("params", new JSONObject(params));
    try {
      final Endpoint endpoint = endpoints.endpoint(node);
      final ChaosHttp.Response response = ChaosHttp.post(endpoint.host(), endpoint.port(), commandPath, payload.toString(),
          connectTimeoutMs, readTimeoutMs);
      if (response.status() >= 200 && response.status() < 300)
        writesOk.incrementAndGet();
      else
        writesFailed.incrementAndGet();
    } catch (final IOException | RuntimeException e) {
      writesFailed.incrementAndGet();
    }
  }

  private void query(final int node, final String where) {
    queries.incrementAndGet();
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", FacetSchema.facetQuery(where))
        .put("params", new JSONObject());
    try {
      final Endpoint endpoint = endpoints.endpoint(node);
      final ChaosHttp.Response response = ChaosHttp.post(endpoint.host(), endpoint.port(), queryPath, payload.toString(),
          connectTimeoutMs, readTimeoutMs);
      if (response.status() >= 500)
        queryServerErrors.incrementAndGet();
      else if (response.status() != 200)
        queryFailed(node, "HTTP " + response.status() + ": " + response.body());
      else
        sampled(node, where, FacetSnapshot.parse(new JSONObject(response.body())));
    } catch (final IOException e) {
      // faults are in progress: an unreachable node is not a finding of the workload
    } catch (final RuntimeException e) {
      queryFailed(node, e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  private void sampled(final int node, final String where, final FacetSnapshot sample) {
    if (!sample.exact())
      nonExact.incrementAndGet();
    if (!sample.metaConsistent()) {
      metaInconsistent.incrementAndGet();
      if (metaLogged.compareAndSet(false, true))
        LOGGER.warn("FACET_META node {} answered an inexact result with no lag where='{}' (first of the run, further ones are only "
            + "counted): patched={} sources={}", node, where, sample.patched(), sample.sources());
    }
  }

  /** Counts a query answer no fault explains; the first one is logged so the summary's count comes with its cause. */
  private void queryFailed(final int node, final String cause) {
    queryFailures.incrementAndGet();
    if (failureLogged.compareAndSet(false, true))
      LOGGER.warn("Facet query on node {} failed (first of the run, further ones are only counted): {}", node,
          cause.length() > 500 ? cause.substring(0, 500) : cause);
  }

  @Override
  public void quiesce() throws InterruptedException {
    gate.writeLock().lockInterruptibly();
  }

  @Override
  public void resume() {
    if (gate.isWriteLockedByCurrentThread())
      gate.writeLock().unlock();
  }

  @Override
  public long acked() {
    return writesOk.get();
  }

  @Override
  public void close() {
    running.set(false);
    resume();
    for (final Thread thread : threads)
      try {
        thread.join(readTimeoutMs + 5_000L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    threads.clear();
  }
}
