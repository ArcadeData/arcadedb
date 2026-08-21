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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A {@code COMPACT INDEX} whose graph persist fails partway through must not lose the pre-compaction graph.
 * <p>
 * {@code buildGraphFromScratchExclusively()} drops the stale, pre-compaction graph file only after the
 * replacement is certified (committed pages plus a manifest that vouches for them), and restores it as the
 * active {@code graphFile} if the persist fails first - mirroring the same {@code Throwable} guard issue #6503
 * added around this block. This test injects a failure at the same point that one does (right after the graph
 * pages are written, before the transaction commits), but through a {@code compactDataFile=true} rebuild, which
 * is the path {@code COMPACT INDEX} takes and the one where a stale reference exists to restore.
 * <p>
 * Two things must hold after the injected failure: the index keeps exactly one usable {@code .vecgraph} file
 * (the stale one, restored) rather than either losing the graph outright or leaking the broken replacement
 * alongside it, and the failure itself must not escape - the persist block already swallows it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class CompactIndexGraphPersistFailureTest {
  private static final int    DIMENSIONS  = 24;
  private static final int    NUM_VECTORS = 300;
  private static final String DB_PATH     = "./target/databases/CompactIndexGraphPersistFailureTest";

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void failedPersistAfterCompactionRestoresTheStaleGraphInsteadOfLosingIt() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(11);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final DocumentType t = db.getSchema().createDocumentType("Doc");
          t.createProperty("id", Type.INTEGER);
          t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          for (int i = 0; i < NUM_VECTORS; i++)
            db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
            + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", \"maxConnections\": 32 }");
        final LSMVectorIndex lsm = vectorIndex(db);
        waitForGraph(lsm);

        final LSMVectorIndexGraphFile graphFileBeforeCompaction = graphFileField(lsm);
        assertThat(graphFileBeforeCompaction)
            .as("the index must have a persisted graph before compaction, or this test proves nothing")
            .isNotNull();

        // Drive the exact rebuild COMPACT INDEX triggers (buildGraphFromScratch(null, true), private and always
        // callback-less on the public compact() path), but with a callback that fails persistence right after
        // the graph pages are written - the same injection point Issue6503GraphPersistOutOfMemoryTest uses -
        // so the data file rename has already happened but the replacement graph never gets certified.
        assertThatCode(() -> buildGraphFromScratchWithRetry(lsm,
            (phase, processedNodes, totalNodes, vectorAccesses) -> {
              if ("persisting".equals(phase) && processedNodes > 0 && processedNodes == totalNodes)
                throw new RuntimeException("simulated graph persist failure after compaction");
            }, true))
            .as("the persist block must swallow this failure the same way it swallows an OOM there (issue #6503)")
            .doesNotThrowAnyException();

        final LSMVectorIndexGraphFile graphFileAfterFailure = graphFileField(lsm);
        assertThat(graphFileAfterFailure)
            .as("a persist failure must restore the stale, pre-compaction graph as the active one rather than "
                + "leaving the index with no usable persisted graph at all")
            .isNotNull();
        assertThat(graphFileAfterFailure.hasPersistedGraph())
            .as("the restored graph must still be the valid, already-persisted one from before compaction")
            .isTrue();

        assertThat(countGraphFiles())
            .as("the broken, uncertified replacement must be dropped, not leaked alongside the restored stale "
                + "file - exactly the class of orphan this whole fix exists to prevent")
            .isEqualTo(1);

        // The index must still be able to search and to accept further writes: nothing is left wedged by the
        // aborted persist.
        assertThat(lsm.findNeighborsFromVector(randomVector(new Random(99)), 5, 64)).hasSize(5);
        db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("embedding", randomVector(rng)).save());
      } finally {
        db.drop();
      }
    }
  }

  /** Invokes the private {@code buildGraphFromScratchWithRetry(GraphBuildCallback, boolean, boolean)}. */
  private static void buildGraphFromScratchWithRetry(final LSMVectorIndex index,
      final LSMVectorIndex.GraphBuildCallback callback, final boolean compactDataFile) throws Exception {
    final Method method = LSMVectorIndex.class.getDeclaredMethod("buildGraphFromScratchWithRetry",
        LSMVectorIndex.GraphBuildCallback.class, boolean.class, boolean.class);
    method.setAccessible(true);
    try {
      method.invoke(index, callback, compactDataFile, false);
    } catch (final java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException re)
        throw re;
      if (e.getCause() instanceof Error err)
        throw err;
      throw e;
    }
  }

  private static LSMVectorIndexGraphFile graphFileField(final LSMVectorIndex index) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("graphFile");
    field.setAccessible(true);
    return (LSMVectorIndexGraphFile) field.get(index);
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
  }

  private static void waitForGraph(final LSMVectorIndex lsm) {
    for (int i = 0; i < 600 && lsm.getStats().get("graphNodeCount") == 0L; i++)
      try { Thread.sleep(50); } catch (final InterruptedException ignored) { break; }
  }

  private static int countGraphFiles() {
    final File[] files = new File(DB_PATH).listFiles((d, n) -> n.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT));
    return files == null ? 0 : files.length;
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
