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
package com.arcadedb.index.vector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5648: {@link GraphSearcherPool} could hand out a {@link GraphSearcher} bound to a
 * graph that had already been replaced.
 * <p>
 * The interleaving that produces it needs a {@code release} running under the outgoing identity to land its
 * searcher in the idle queue after the incoming {@code borrow} has drained but before it has published, which is
 * a two-instruction window that no amount of thread scheduling reproduces reliably. These tests therefore assert
 * the state that window leaves behind - the queue holding an entry pooled under one identity while the pool
 * advertises another - and require the pool to never hand that entry out. That state is unreachable through the
 * public API once the fix is in, so it is planted directly on the private field.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphSearcherPoolStaleGraphTest {
  private static final VectorTypeSupport VTS        = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int               DIMENSIONS = 8;

  private ImmutableGraphIndex graphOne;
  private ImmutableGraphIndex graphTwo;

  @BeforeEach
  void buildGraphs() throws Exception {
    graphOne = buildGraph(11);
    graphTwo = buildGraph(22);
  }

  @AfterEach
  void closeGraphs() throws Exception {
    graphOne.close();
    graphTwo.close();
  }

  @Test
  void borrowNeverHandsOutASearcherPooledUnderAReplacedGraph() throws Exception {
    final GraphSearcherPool pool = new GraphSearcherPool(4);

    // The pool starts serving graphOne, and a searcher built for it goes back into the idle queue.
    final GraphSearcher stale = pool.borrow(graphOne, 1L);
    pool.release(stale, graphOne, 1L);
    assertThat(pool.size()).isOne();

    // A rebuild swapped the graph. This is the state the #5648 window leaves: the identity moved on, but the
    // searcher a concurrent release put back is still sitting in the queue, still bound to graphOne.
    plantIdentity(pool, graphTwo, 2L);

    final GraphSearcher borrowed = pool.borrow(graphTwo, 2L);
    try {
      assertThat(borrowed).as("a searcher built for the replaced graph must never be handed out").isNotSameAs(stale);
    } finally {
      closeQuietly(borrowed);
    }

    assertThat(pool.size()).as("the stale entry must be reclaimed, not left occupying the pool").isZero();
  }

  @Test
  void borrowNeverHandsOutASearcherPooledUnderAStaleEpoch() throws Exception {
    final GraphSearcherPool pool = new GraphSearcherPool(4);

    // Same graph instance, but the index mutated underneath it: the live-builder path keeps serving searches from
    // one graph object while it grows, so identity alone cannot tell the pooled view its contents moved.
    final GraphSearcher stale = pool.borrow(graphOne, 1L);
    pool.release(stale, graphOne, 1L);

    plantIdentity(pool, graphOne, 2L);

    final GraphSearcher borrowed = pool.borrow(graphOne, 2L);
    try {
      assertThat(borrowed).as("a searcher pooled under an older epoch must never be handed out").isNotSameAs(stale);
    } finally {
      closeQuietly(borrowed);
    }

    assertThat(pool.size()).isZero();
  }

  @Test
  void everyStaleEntryIsReclaimedBeforeAFreshSearcherIsBuilt() throws Exception {
    final GraphSearcherPool pool = new GraphSearcherPool(4);

    // Borrow before releasing anything, so three distinct searchers end up queued rather than one recycled.
    final List<GraphSearcher> staleSearchers = new ArrayList<>();
    for (int i = 0; i < 3; i++)
      staleSearchers.add(pool.borrow(graphOne, 1L));
    for (final GraphSearcher searcher : staleSearchers)
      pool.release(searcher, graphOne, 1L);
    assertThat(pool.size()).isEqualTo(3);

    plantIdentity(pool, graphTwo, 2L);

    final GraphSearcher borrowed = pool.borrow(graphTwo, 2L);
    try {
      assertThat(staleSearchers).as("no queued entry bound to the old graph may escape").doesNotContain(borrowed);
    } finally {
      closeQuietly(borrowed);
    }

    assertThat(pool.size()).as("a borrow must drop every stale entry it walks past").isZero();
  }

  @Test
  void aReleaseUnderASupersededIdentityIsNotPooled() {
    final GraphSearcherPool pool = new GraphSearcherPool(4);

    final GraphSearcher onOldGraph = pool.borrow(graphOne, 1L);

    // A borrow for the new graph publishes the new identity, so the in-flight search on the old one must find its
    // searcher rejected when it finally returns it.
    closeQuietly(pool.borrow(graphTwo, 2L));
    pool.release(onOldGraph, graphOne, 1L);

    assertThat(pool.size()).as("a searcher returned under a superseded identity must be closed, not pooled").isZero();
  }

  @Test
  void anUnchangedIdentityStillReusesThePooledSearcher() {
    final GraphSearcherPool pool = new GraphSearcherPool(4);

    final GraphSearcher first = pool.borrow(graphOne, 1L);
    pool.release(first, graphOne, 1L);

    final GraphSearcher second = pool.borrow(graphOne, 1L);
    try {
      assertThat(second).as("the identity check must not become so strict that pooling stops working").isSameAs(first);
    } finally {
      closeQuietly(second);
    }
  }

  /**
   * Writes the pool's published identity without going through {@code borrow}, so the idle queue keeps the entries
   * pooled under the previous one. That is exactly what a {@code release} racing an identity change leaves behind,
   * and it is deliberately not reachable through the public API.
   */
  private static void plantIdentity(final GraphSearcherPool pool, final ImmutableGraphIndex graph, final long epoch)
      throws Exception {
    final Field field = GraphSearcherPool.class.getDeclaredField("pooled");
    field.setAccessible(true);
    final Constructor<?> constructor = field.getType().getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    field.set(pool, constructor.newInstance(graph, epoch));
  }

  private static ImmutableGraphIndex buildGraph(final long seed) throws Exception {
    final Random random = new Random(seed);
    final List<VectorFloat<?>> vectors = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      final float[] values = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        values[d] = random.nextFloat();
      vectors.add(VTS.createFloatVector(values));
    }

    final ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, DIMENSIONS);
    try (final GraphIndexBuilder builder = new GraphIndexBuilder(ravv, VectorSimilarityFunction.COSINE, 8, 16, 1.2f, 1.2f,
        false)) {
      return builder.build(ravv);
    }
  }

  private static void closeQuietly(final GraphSearcher searcher) {
    try {
      searcher.close();
    } catch (final Exception ignored) {
      // Nothing this test asserts depends on the view releasing cleanly.
    }
  }
}
