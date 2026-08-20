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
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6518: {@code LSMVectorIndex.close()} released its background resources BEFORE
 * flushing, the opposite of the order {@code LocalDatabase.closeDurableParts()} uses and documents ("an index
 * whose graceful shutdown is a graph build needs its build pool alive for it").
 * <p>
 * The inversion was not cosmetic. {@code releaseBackgroundResources()} shuts the graph-build pool down, and
 * {@code getOrCreateGraphBuildPool()} treats a shut-down pool as one to REPLACE - so the {@code flush()} that
 * followed built a brand new pool, ran the rebuild on it, and left it running, because the releasing step had
 * already happened and nothing shuts a pool down twice. Measured before the fix on this fixture: the graph was
 * persisted correctly and 17 worker threads were still alive on a closed, invalidated index. Nothing failed and
 * nothing was logged, which is what makes it the same failure issue #5418 was filed for, reached another way.
 * <p>
 * Two things are pinned here, because either alone would pass against a half-fix: the close still PERSISTS the
 * graph (so the reorder did not simply trade a leak for a lost flush), and it leaves NO live build pool.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6518CloseDoesNotLeakBuildPoolTest {
  private static final String DB_ROOT     = "target/test-databases/Issue6518CloseDoesNotLeakBuildPoolTest";
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
  void closingADirtyIndexPersistsTheGraphAndLeavesNoRunningBuildPool() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);

        // A search makes the graph resident, then one more insert leaves pending work - so close() has a real
        // rebuild to do. Without both, flush() short-circuits and the leak never gets a chance to happen.
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", randomVector(new Random(5)))
            .save());

        assertThat(index.getStats().get("graphState"))
            .as("precondition: the index must be MUTABLE, otherwise close() skips the build under test")
            .isEqualTo(2L); // GraphState.MUTABLE
        final long rebuildsBefore = index.getStats().get("graphRebuildCount");

        index.close();

        // The close still does its job: the pending vectors were absorbed into a persisted graph.
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("close() must still flush the pending vectors into the graph - the fix must not trade a leaked "
                + "pool for a lost flush")
            .isGreaterThan(rebuildsBefore);

        // ...and it leaves nothing running. Before the fix this pool was a fresh one, built by the flush AFTER
        // the release had already shut its predecessor down, with 17 live workers and nobody left to stop them.
        final ForkJoinPool pool = graphBuildPool(index);
        if (pool != null) {
          assertThat(pool.isShutdown())
              .as("close() must not leave a running graph-build pool behind: %d worker threads outliving a "
                  + "closed index is what issue #5418 was filed for", pool.getPoolSize())
              .isTrue();
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Defence in depth for the same trap arriving from a future caller: if {@code flush()} is reached after the
   * background resources are gone, it must decline the build rather than quietly construct a pool that nothing
   * will ever shut down. Skipping costs a rebuild on the next open; building would cost the pool for the life of
   * the process.
   */
  @Test
  void flushAfterReleaseDeclinesTheBuildInsteadOfLeakingAPool() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", randomVector(new Random(5)))
            .save());
        assertThat(index.getStats().get("graphState")).isEqualTo(2L); // MUTABLE

        // The inverted order, driven explicitly - this is what close() used to do.
        index.releaseBackgroundResources();
        final long rebuildsBefore = index.getStats().get("graphRebuildCount");

        index.flush();

        assertThat(index.getStats().get("graphRebuildCount"))
            .as("a flush that arrives after the release must decline the build, not run one on a pool it had to "
                + "resurrect")
            .isEqualTo(rebuildsBefore);

        final ForkJoinPool pool = graphBuildPool(index);
        if (pool != null) {
          assertThat(pool.isShutdown())
              .as("the pool the release shut down must stay shut down: a declined build must not replace it")
              .isTrue();
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The sibling retention, from the same root cause: nulling {@code graphIndex} frees nothing while pooled
   * searchers still hold the graph they were pooled under. {@code borrow()} normally drains the pool the next
   * time it notices the identity moved, but the emptied-index path leaves no graph to search and
   * {@code findNeighborsFromVector()} returns on {@code graphIndex == null} BEFORE it borrows - so on that path
   * nothing ever drains it, and the last full graph would stay resident for the life of the index object.
   */
  @Test
  void emptyingTheIndexReleasesThePooledSearchersHoldingTheOldGraph() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);

        // Searching populates the pool; releasing the searcher is what leaves one queued holding the graph.
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        assertThat(index.getStats().get("pooledGraphSearchers"))
            .as("precondition: a search must leave a searcher pooled, or this test proves nothing")
            .isGreaterThan(0L);

        // Delete every vector, then rebuild: the build finds nothing live and takes the graphIndex = null path.
        db.transaction(() -> db.command("sql", "DELETE FROM Doc").close());
        index.buildVectorGraphNow(null);

        assertThat(index.getGraphIndex()).as("precondition: the emptied index must have dropped its graph")
            .isNull();
        assertThat(index.getStats().get("pooledGraphSearchers"))
            .as("an index emptied of vectors must not keep its last graph alive through the searcher pool: no "
                + "later search can drain it, because the search path returns before it borrows")
            .isZero();
      } finally {
        db.drop();
      }
    }
  }

  private static ForkJoinPool graphBuildPool(final LSMVectorIndex index) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("graphBuildPool");
    field.setAccessible(true);
    return (ForkJoinPool) field.get(index);
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
    for (int i = 0; i < NUM_VECTORS; i++)
      db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
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
