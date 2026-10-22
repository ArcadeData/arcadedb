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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10547: the parallel flush of a {@link GraphBatch} scheduled one task per vertex bucket, so a vertex type created
 * with the default single bucket connected every edge on one async thread while the other slots sat idle. The connect
 * pass now cuts a bucket's sorted range into vertex-aligned sub-ranges when there are fewer non-empty buckets than the
 * async parallel level, in both directions. The graph must come out identical whatever the bucket count, including the
 * edge list of a hub vertex whose edges are a large share of one bucket's range (a vertex's edges are never split across
 * two tasks).
 */
class Issue10547GraphBatchOneBucketParallelismTest extends TestHelper {
  private static final int PARALLEL_LEVEL = 4;
  private static final int VERTICES       = 2_000;
  private static final int EDGES          = 40_000;
  private static final int HUB            = 7;

  @AfterEach
  void clearHooks() {
    GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = null;
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
  }

  @ParameterizedTest(name = "buckets={0}, lightEdges={1}")
  @CsvSource({ "1,false", "1,true", "4,false" })
  void edgesAreConnectedOnSeveralThreadsWhateverTheBucketCount(final int buckets, final boolean lightEdges) {
    database.command("sql", "CREATE VERTEX TYPE Node BUCKETS " + buckets);
    database.command("sql", "CREATE EDGE TYPE E " + (lightEdges ? "LIGHTWEIGHT " : "") + "BUCKETS 1");
    database.async().setParallelLevel(PARALLEL_LEVEL);

    final Set<Thread> outThreads = ConcurrentHashMap.newKeySet();
    final Set<Thread> inThreads = ConcurrentHashMap.newKeySet();
    GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = attempt -> outThreads.add(Thread.currentThread());
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> inThreads.add(Thread.currentThread());

    final int[][] edges = edgeList();
    final RID[] rids;
    try (final GraphBatch batch = database.batch().withBatchSize(EDGES).withBidirectional(true).withWAL(false)
        .withLightEdges(lightEdges).withParallelFlush(true).build()) {
      final Object[][] props = new Object[VERTICES][];
      for (int i = 0; i < VERTICES; i++)
        props[i] = new Object[] { "i", i };
      rids = batch.createVertices("Node", props);

      for (final int[] e : edges)
        batch.newEdge(rids[e[0]], "E", rids[e[1]]);
    }

    assertThat(outThreads).as("async threads that connected outgoing edges, %d bucket(s)", buckets).hasSizeGreaterThan(1);
    assertThat(inThreads).as("async threads that connected incoming edges, %d bucket(s)", buckets).hasSizeGreaterThan(1);

    assertAdjacency(rids, edges);
    assertDatabaseIsConsistent();
  }

  /**
   * The incoming drain of one bucket runs as several tasks, one of which fails its commit. The retry at close() must run that
   * task alone, against the plan the failed attempt used (pinned with the drain prefix): a committed task run again would
   * duplicate its edges, a task the retry plans differently could skip or repeat vertices.
   */
  @Test
  void aSplitIncomingDrainThatFailsPartwayIsResumedTaskByTask() {
    database.command("sql", "CREATE VERTEX TYPE Node BUCKETS 1");
    database.command("sql", "CREATE EDGE TYPE E BUCKETS 1");
    database.async().setParallelLevel(PARALLEL_LEVEL);

    final int edgeCount = PARALLEL_LEVEL * GraphBatch.MIN_EDGES_PER_CONNECT_TASK * 2;
    final int[][] edges = new int[edgeCount][];
    final Random rnd = new Random(105470);
    for (int e = 0; e < edgeCount; e++)
      edges[e] = new int[] { rnd.nextInt(VERTICES), rnd.nextInt(VERTICES) };

    final AtomicInteger calls = new AtomicInteger();
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      calls.incrementAndGet();
      if (attempt == 2)
        throw new RuntimeException("simulated commit failure of one incoming task");
    };

    final RID[] rids;
    final GraphBatch batch = database.batch().withBatchSize(edgeCount).withBidirectional(true).withWAL(false)
        .withParallelFlush(true).withMaxDeferredIncomingEdges(edgeCount).build();
    try {
      final Object[][] props = new Object[VERTICES][];
      for (int i = 0; i < VERTICES; i++)
        props[i] = new Object[] { "i", i };
      rids = batch.createVertices("Node", props);

      // the flush - the one the last newEdge() triggers, or the explicit one - drains the deferred incoming edges early (the
      // buffer reached its cap), and the drain fails
      assertThatThrownBy(() -> {
        for (final int[] e : edges)
          batch.newEdge(rids[e[0]], "E", rids[e[1]]);
        batch.flush();
      }).hasMessageContaining("simulated commit failure");
    } finally {
      batch.close();
    }

    assertThat(calls.get()).as("%d incoming tasks, one failed and was run again alone", PARALLEL_LEVEL)
        .isEqualTo(PARALLEL_LEVEL + 1);
    assertAdjacency(rids, edges);
    assertDatabaseIsConsistent();
  }

  /** Random edges, a quarter of them out of (and another quarter into) one hub vertex. */
  private static int[][] edgeList() {
    final Random rnd = new Random(10547);
    final int[][] edges = new int[EDGES][];
    for (int e = 0; e < EDGES; e++) {
      final int kind = e % 4;
      final int s = kind == 0 ? HUB : rnd.nextInt(VERTICES);
      final int d = kind == 1 ? HUB : rnd.nextInt(VERTICES);
      edges[e] = new int[] { s, d };
    }
    return edges;
  }

  private void assertAdjacency(final RID[] rids, final int[][] edges) {
    final List<List<Integer>> expectedOut = new ArrayList<>();
    final List<List<Integer>> expectedIn = new ArrayList<>();
    for (int i = 0; i < VERTICES; i++) {
      expectedOut.add(new ArrayList<>());
      expectedIn.add(new ArrayList<>());
    }
    for (final int[] e : edges) {
      expectedOut.get(e[0]).add(e[1]);
      expectedIn.get(e[1]).add(e[0]);
    }

    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++) {
        final Vertex v = rids[i].asVertex();
        assertThat(neighbours(v, Vertex.DIRECTION.OUT)).as("out neighbours of vertex %d", i).isEqualTo(sorted(expectedOut.get(i)));
        assertThat(neighbours(v, Vertex.DIRECTION.IN)).as("in neighbours of vertex %d", i).isEqualTo(sorted(expectedIn.get(i)));
      }
    });
  }

  private static List<Integer> neighbours(final Vertex v, final Vertex.DIRECTION direction) {
    final List<Integer> result = new ArrayList<>();
    for (final Vertex n : v.getVertices(direction, "E"))
      result.add(n.getInteger("i"));
    Collections.sort(result);
    return result;
  }

  private static List<Integer> sorted(final List<Integer> list) {
    Collections.sort(list);
    return list;
  }

  private void assertDatabaseIsConsistent() {
    try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
      final Result row = result.next();
      assertThat(row.<Collection<?>>getProperty("corruptedRecords")).as("corruptedRecords: %s", row.toJSON()).isEmpty();
      assertThat(((Number) row.getProperty("invalidLinks")).longValue()).as("invalidLinks: %s", row.toJSON()).isZero();
    }
  }
}
