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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6503: a from-scratch graph rebuild keeps the OLD graph (and the shared search cache)
 * resident for the whole build, on top of the new build's own working set (build cache, JVector builder, ordinal
 * map). On the close path that doubling is pure waste - {@code flush()} has already CAS-ed the index to
 * {@code UNAVAILABLE} and a closing database does not hand it any more requests, so nothing can still be reading
 * either the old graph or the old cache by the time the rebuild starts. Measured in the issue at 50,000 x 128:
 * peak heap 1,700 MB for a rebuild against 959 MB for a first build of the same corpus - a factor of 1.73x that
 * raising {@code -Xmx} does not fix, because both caches size themselves off total heap, not free heap.
 * <p>
 * {@link LSMVectorIndex#buildGraphFromScratchReleasingResidentGraph(LSMVectorIndex.GraphBuildCallback)} is the
 * fix: it releases the resident graph, the search cache AND the pooled searchers before starting, and
 * {@code flush()} now calls it instead of the ordinary {@code buildGraphFromScratch()}. All three matter - a
 * pooled searcher holds the graph it was pooled under, and {@code LocalDatabase.closeDurableParts()} runs
 * {@code flush()} BEFORE {@code releaseBackgroundResources()}, so on a real close the pool is still populated
 * when the rebuild starts and would pin the old graph on its own.
 * <p>
 * It must not be used anywhere else - an online rebuild ({@code rebuildGraphBeforeSearch()},
 * {@code startAsyncGraphRebuild()}, an explicit {@code buildVectorGraphNow()}, or {@code compact()}) has to keep
 * serving the old graph to concurrent searches, since none of those gate searches on {@code status}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6503VectorRebuildResidentGraphReleaseTest {
  private static final String DB_ROOT     = "target/test-databases/Issue6503VectorRebuildResidentGraphReleaseTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 200;

  private String dbPath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @Test
  void closeStyleRebuildReleasesTheResidentGraphAndSearchCacheBeforeBuilding() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);

        // A search makes both the graph and the shared search cache resident, same as any index that has
        // actually answered a query - which is the state a close-triggered rebuild finds them in.
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        assertThat(index.getGraphIndex()).as("graph must be resident before the rebuild under test").isNotNull();
        assertThat(searchVectorCacheField(index))
            .as("search cache must be resident before the rebuild under test").isNotNull();

        final AtomicReference<Boolean> graphReleasedOnFirstSample = new AtomicReference<>();
        final AtomicReference<Object>  searchCacheOnFirstSample   = new AtomicReference<>();
        final AtomicInteger            pooledSearchersOnFirstSample = new AtomicInteger(-1);

        index.buildGraphFromScratchReleasingResidentGraph((phase, processedNodes, totalNodes, vectorAccesses) -> {
          if (graphReleasedOnFirstSample.get() != null)
            return; // only the first sample matters: it is chronologically the earliest observation point
          graphReleasedOnFirstSample.set(index.getGraphIndex() == null);
          pooledSearchersOnFirstSample.set(index.getStats().get("pooledGraphSearchers").intValue());
          try {
            searchCacheOnFirstSample.set(searchVectorCacheField(index));
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        });

        assertThat(graphReleasedOnFirstSample.get())
            .as("the resident graph must already be released by the time the rebuild reports its first progress "
                + "sample - otherwise the old graph and the new build's working set are both resident at once")
            .isTrue();
        assertThat(searchCacheOnFirstSample.get())
            .as("the resident search cache must already be released by the time the rebuild reports its first "
                + "progress sample")
            .isNull();
        // Nulling graphIndex alone releases nothing while a pooled searcher still holds the graph it was pooled
        // under. This is not hypothetical on the path under test: LocalDatabase.closeDurableParts() runs
        // index.flush() BEFORE index.releaseBackgroundResources(), so on a real database close the pool is still
        // populated when the rebuild starts, and the old graph would stay reachable through it.
        assertThat(pooledSearchersOnFirstSample.get())
            .as("the searcher pool must be emptied too, or every pooled searcher keeps the old graph alive and "
                + "the release frees nothing")
            .isZero();

        // ...and the rebuild still produces a correct, searchable graph.
        assertThat(index.getGraphIndex()).as("the rebuild must have published a new graph").isNotNull();
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void onlineRebuildDoesNotReleaseTheResidentGraphBeforeBuilding() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);

        final Object residentGraphBeforeRebuild = index.getGraphIndex();
        assertThat(residentGraphBeforeRebuild).isNotNull();

        final AtomicReference<Object> graphOnFirstSample = new AtomicReference<>();
        // buildVectorGraphNow() is what an explicit REBUILD INDEX drives while the database may still be
        // serving concurrent traffic: nothing here gates searches on status, so the old graph must stay put.
        index.buildVectorGraphNow((phase, processedNodes, totalNodes, vectorAccesses) -> {
          if (graphOnFirstSample.get() == null)
            graphOnFirstSample.set(index.getGraphIndex());
        });

        assertThat(graphOnFirstSample.get())
            .as("an explicit/online rebuild must keep serving the OLD graph until the new one is ready - it must "
                + "not take the close-path shortcut that releases it up front")
            .isSameAs(residentGraphBeforeRebuild);

        // The rebuild still completes correctly; only the timing of the release differs from the close path.
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void closingWithPendingMutationsRebuildsAndReopensToACorrectGraph() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      populate(db);

      // One more mutation than the initial build absorbed, so graphState is MUTABLE and flush() takes the
      // rebuild branch under test instead of the "nothing to do" one.
      db.transaction(() -> {
        final float[] vector = randomVector(new Random(123));
        db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", vector).save();
      });

      assertThat(vectorIndex(db).getStats().get("graphState"))
          .as("an extra insert past the initial build must leave the graph MUTABLE, otherwise close() below "
              + "would skip the rebuild this test exercises")
          .isEqualTo(2L); // GraphState.MUTABLE ordinal, see getStats()

      db.close();
    }

    // Reopen and confirm the close-triggered rebuild persisted a correct, searchable graph over all
    // NUM_VECTORS + 1 vectors - including the one added right before close().
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        assertThat(index.getStats().get("activeVectors")).isEqualTo((long) NUM_VECTORS + 1);
      }
    } finally {
      FileUtils.deleteRecursively(new File(dbPath));
    }
  }

  private static VectorCache searchVectorCacheField(final LSMVectorIndex index) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("searchVectorCache");
    field.setAccessible(true);
    return (VectorCache) field.get(index);
  }

  private static void populate(final Database db) {
    final Random rnd = new Random(7);

    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    });

    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++) {
      db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
      if (i % 1000 == 999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static float[] randomVector(final Random rnd) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = rnd.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private static float[] queryVector() {
    return randomVector(new Random(99));
  }
}
