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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5665: every {@link GraphBatch#flush()} with {@code parallelFlush=true}
 * relaxed and then restored the shared async executor's WAL policy around the parallel connect phase,
 * and {@code DatabaseAsyncExecutorImpl.setTransactionUseWAL()}/{@code setTransactionSync()}
 * unconditionally tear down and respawn the executor's ENTIRE worker pool - so a multi-flush parallel
 * bulk load recreated every async worker thread on every single flush (4 pool teardowns per flush: two
 * to relax the policy, two to restore it). On a large import (hundreds of thousands of flushes) this
 * was pure overhead, and worse, it force-exited the in-flight and queued tasks of any OTHER concurrent
 * user of {@code database.async()} on the same database, surfacing as
 * "Async executor has been shut down" for completely unrelated callers.
 * <p>
 * {@link GraphBatch} now relaxes/restores the async executor's WAL policy once for the whole batch
 * (constructor / close-or-abandon) instead of once per flush.
 */
class Issue5665GraphBatchAsyncPoolChurnTest extends TestHelper {

  private static final String VERTEX_TYPE = "ChurnPerson";
  private static final String EDGE_TYPE   = "CHURN_KNOWS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });
  }

  @Test
  void asyncWorkerPoolSurvivesMultipleParallelFlushesUnchanged() {
    final int vertices = 40;
    final int edgesPerFlush = 20;
    final int additionalFlushes = 7;

    try (final GraphBatch batch = GraphBatch.builder(database)
        .withBatchSize(edgesPerFlush) // forces one flush per edgesPerFlush edges
        .withParallelFlush(true)
        .build()) {

      final RID[] vertexRIDs = batch.createVertices(VERTEX_TYPE, vertices);

      // First flush: constructing the batch (useWAL differs from the executor's default) is allowed
      // to churn the pool once. Capture the worker identities right after this settled state.
      addEdges(batch, vertexRIDs, edgesPerFlush);
      batch.flush();

      final Set<Long> threadIdsAfterFirstFlush = asyncWorkerThreadIds();
      assertThat(threadIdsAfterFirstFlush).as("async worker pool must exist after the first flush").isNotEmpty();

      // Several more flushes: before the fix, each one tore down and respawned every worker thread.
      for (int f = 0; f < additionalFlushes; f++) {
        addEdges(batch, vertexRIDs, edgesPerFlush);
        batch.flush();
      }

      final Set<Long> threadIdsAfterMoreFlushes = asyncWorkerThreadIds();
      assertThat(threadIdsAfterMoreFlushes)
          .as("async worker threads must survive %d additional flushes unchanged", additionalFlushes)
          .isEqualTo(threadIdsAfterFirstFlush);
    }
  }

  @Test
  void unrelatedAsyncCallerSurvivesConcurrentParallelFlushes() throws Exception {
    final int vertices = 60;
    final int edgesPerFlush = 30;
    final int flushes = 30;

    final GraphBatch batch = GraphBatch.builder(database)
        .withBatchSize(edgesPerFlush)
        .withParallelFlush(true)
        .build();
    try {
      final RID[] vertexRIDs = batch.createVertices(VERTEX_TYPE, vertices);

      final AtomicBoolean stop = new AtomicBoolean(false);
      final AtomicInteger unrelatedCompleted = new AtomicInteger();
      final List<Throwable> unrelatedErrors = new CopyOnWriteArrayList<>();
      final DatabaseAsyncExecutor unrelatedAsync = database.async();

      // Simulates a second, unrelated user of database.async() on the same database (e.g. another
      // ingest worker, an async HTTP handler) hammering it while this GraphBatch is flushing. Started
      // AFTER the batch is built and stopped BEFORE close(), isolating exactly the "in the middle of
      // several flushes" window that issue #5665 broke: no async-executor setting changes are expected
      // there anymore, only at the (already excluded) construction/close boundaries.
      final Thread unrelatedCaller = new Thread(() -> {
        while (!stop.get()) {
          try {
            unrelatedAsync.transaction(() -> {
              /* no-op: only the scheduling/execution survival is under test */
            }, 0, unrelatedCompleted::incrementAndGet, unrelatedErrors::add);
          } catch (final Throwable t) {
            unrelatedErrors.add(t);
          }
        }
      });
      unrelatedCaller.setDaemon(true);
      unrelatedCaller.start();
      try {
        // Let the unrelated caller get some tasks in flight before the flushes start.
        Thread.sleep(20);

        for (int f = 0; f < flushes; f++) {
          addEdges(batch, vertexRIDs, edgesPerFlush);
          batch.flush();
        }
      } finally {
        stop.set(true);
        unrelatedCaller.join(5_000);
      }

      unrelatedAsync.waitCompletion();

      assertThat(unrelatedErrors)
          .as("an unrelated async caller must not be torn down by a concurrent GraphBatch flush")
          .isEmpty();
      assertThat(unrelatedCompleted.get()).as("unrelated caller must have made progress").isGreaterThan(0);
    } finally {
      batch.close();
    }
  }

  private static void addEdges(final GraphBatch batch, final RID[] vertexRIDs, final int count) {
    for (int i = 0; i < count; i++)
      batch.newEdge(vertexRIDs[i % vertexRIDs.length], EDGE_TYPE, vertexRIDs[(i + 1) % vertexRIDs.length]);
  }

  private Set<Long> asyncWorkerThreadIds() {
    final String prefix = "AsyncExecutor-" + database.getName() + "-";
    final Set<Long> ids = new HashSet<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.getName().startsWith(prefix))
        ids.add(t.threadId());
    return ids;
  }
}
