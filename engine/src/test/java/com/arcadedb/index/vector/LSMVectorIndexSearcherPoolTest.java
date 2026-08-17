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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5413: dense searches allocate a fresh JVector {@code GraphSearcher} (and the graph
 * view it owns) per query. The searcher is now borrowed from an index-scoped pool, which must not change any
 * result and must never hand out a searcher whose view predates a mutation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexSearcherPoolTest {
  private static final String DB_PATH     = "target/test-databases/LSMVectorIndexSearcherPoolTest";
  private static final int    DIMENSIONS  = 32;
  private static final int    NUM_VECTORS = 2_000;
  private static final int    K           = 10;
  /**
   * {@code maxIdle} handed to {@link GraphSearcherPool}, i.e. the number of searchers it aims to keep alive.
   */
  private static final int    POOL_SIZE   = 4;
  /**
   * Threads releasing into that pool concurrently. Deliberately larger than {@link #POOL_SIZE}: the point is to
   * exercise the pool while it is over-subscribed.
   */
  private static final int    CONCURRENCY = 8;

  @BeforeEach
  @AfterEach
  void cleanup() {
    GlobalConfiguration.VECTOR_INDEX_SEARCHER_POOL_SIZE.reset();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void pooledSearchersReturnTheSameResultsAsFreshOnes() {
    final float[][] vectors = randomVectors(NUM_VECTORS, 3);
    final float[] query = randomVectors(1, 99)[0];

    final List<RID> withoutPool;

    createDatabase(vectors);

    GlobalConfiguration.VECTOR_INDEX_SEARCHER_POOL_SIZE.setValue(-1);
    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      withoutPool = searchRids(index(db), query);
      assertThat(stat(index(db), "pooledGraphSearchers")).as("pooling disabled must keep no searcher alive").isZero();
    }

    GlobalConfiguration.VECTOR_INDEX_SEARCHER_POOL_SIZE.setValue(POOL_SIZE);
    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      final LSMVectorIndex index = index(db);
      // Repeat: only the second search onwards can actually reuse a pooled instance.
      for (int i = 0; i < 20; i++)
        searchRids(index, query);

      assertThat(stat(index, "pooledGraphSearchers")).as("a searcher must be retained for reuse").isPositive();
      assertThat(searchRids(index, query)).isEqualTo(withoutPool);
    }

    assertThat(withoutPool).hasSize(K);
  }

  @Test
  void insertingAVectorInvalidatesPooledSearchers() {
    final float[][] vectors = randomVectors(NUM_VECTORS, 5);
    createDatabase(vectors);

    GlobalConfiguration.VECTOR_INDEX_SEARCHER_POOL_SIZE.setValue(POOL_SIZE);
    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      final LSMVectorIndex index = index(db);

      // A vector far from the corpus (all values well above the [0,1) corpus range) so it must come back first.
      final float[] outlier = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        outlier[d] = d % 2 == 0 ? 100f : -100f;

      // Warm the pool up.
      for (int i = 0; i < 10; i++)
        searchRids(index, outlier);
      assertThat(stat(index, "pooledGraphSearchers")).isPositive();

      db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", outlier).save());

      // The new vector must be visible: a pooled searcher holding a pre-insert view would miss it.
      final List<RID> after = searchRids(index(db), outlier);
      assertThat(after).isNotEmpty();
      final var top = db.lookupByRID(after.get(0), true).asDocument();
      assertThat(top.getInteger("id")).as("the just-inserted outlier must be the nearest neighbour").isEqualTo(NUM_VECTORS);
    }
  }

  @Test
  void concurrentSearchesNeverShareASearcher() throws Exception {
    final float[][] vectors = randomVectors(NUM_VECTORS, 7);
    createDatabase(vectors);

    GlobalConfiguration.VECTOR_INDEX_SEARCHER_POOL_SIZE.setValue(POOL_SIZE);
    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      final LSMVectorIndex index = index(db);
      final float[] query = randomVectors(1, 42)[0];
      final List<RID> expected = searchRids(index, query);

      final ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
      try {
        final List<Callable<List<RID>>> tasks = new ArrayList<>();
        for (int i = 0; i < 200; i++)
          tasks.add(() -> searchRids(index, query));

        for (final Future<List<RID>> f : pool.invokeAll(tasks))
          assertThat(f.get()).as("a shared searcher would corrupt the beam-search scratch state").isEqualTo(expected);
      } finally {
        pool.shutdownNow();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }

      // What the pool promises is that it does not grow WITHOUT BOUND, not that it never exceeds maxIdle:
      // GraphSearcherPool.release() reads idleCount and increments it without a lock ("racy by design"), so all
      // CONCURRENCY releasers in flight can read a count below the cap and then all increment. The worst case is
      // therefore maxIdle - 1 observed by every one of them, i.e. maxIdle + CONCURRENCY - 1, and asserting
      // maxIdle alone contradicts the contract of the code under test - it failed as "expected 5L to be less
      // than or equal to 4L" under full-suite CPU contention (issue #6314). "Never share a searcher", the
      // property this test is named for, is what the per-task result comparison above checks.
      assertThat(stat(index, "pooledGraphSearchers"))
          .as("GraphSearcherPool.release() allows a transient overshoot of up to one searcher per concurrent releaser")
          .isLessThanOrEqualTo(POOL_SIZE + CONCURRENCY - 1L);
    }
  }

  private static float[][] randomVectors(final int count, final long seed) {
    final Random rnd = new Random(seed);
    final float[][] vectors = new float[count][DIMENSIONS];
    for (int i = 0; i < count; i++)
      for (int d = 0; d < DIMENSIONS; d++)
        vectors[i][d] = rnd.nextFloat();
    return vectors;
  }

  private static void createDatabase(final float[][] vectors) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH); final Database db = factory.create()) {
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\" }");
      });

      db.begin();
      for (int i = 0; i < vectors.length; i++) {
        db.newDocument("Doc").set("id", i).set("vector", vectors[i]).save();
        if (i % 500 == 499) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }
  }

  private static LSMVectorIndex index(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
  }

  private static long stat(final LSMVectorIndex index, final String name) {
    final Map<String, Long> stats = index.getStats();
    assertThat(stats).containsKey(name);
    return stats.get(name);
  }

  private static List<RID> searchRids(final LSMVectorIndex index, final float[] query) {
    final List<Pair<RID, Float>> found = index.findNeighborsFromVector(query, K, null);
    final List<RID> rids = new ArrayList<>(found.size());
    for (final Pair<RID, Float> p : found)
      rids.add(p.getFirst());
    return rids;
  }
}
