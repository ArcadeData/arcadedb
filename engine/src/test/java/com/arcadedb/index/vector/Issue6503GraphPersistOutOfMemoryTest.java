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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #6503: an {@link OutOfMemoryError} raised while a graph rebuild persists is an
 * {@link Error}, not an {@link Exception}, so it used to escape the {@code catch (final Exception e)} guard around
 * the persist block in {@code LSMVectorIndex.buildGraphFromScratchExclusively()}. That guard is the only place that
 * calls {@code manifest.markUnusable()} on a failed persist - skip it and the manifest is left vouching for
 * whatever the pages happen to hold after a write that died partway through, so the next database open trusts a
 * possibly half-rewritten graph on node-count agreement alone (issue #6106 is exactly the check this exists to
 * satisfy). The transaction rollback in the same block is skipped too, leaving a dangling transaction.
 * <p>
 * The fix widens that one catch to {@code Throwable}. This test does not need an actual heap exhaustion to prove
 * it: it injects a real {@link OutOfMemoryError} through a {@link LSMVectorIndex.GraphBuildCallback}, at the same
 * point in the persist block ("persisting" phase completion, right after the graph pages are written but before
 * the transaction commits or the manifest is certified) where a genuine allocation failure would strike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6503GraphPersistOutOfMemoryTest {
  private static final String DB_ROOT     = "target/test-databases/Issue6503GraphPersistOutOfMemoryTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 50;

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
  void oomDuringPersistStillMarksTheManifestUnusableAndRollsBack() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);

        // Simulates an allocation failure at the exact point in the persist block a real one would strike: the
        // graph pages are already written, but the transaction has not committed and the manifest has not been
        // certified yet.
        assertThatCode(() -> index.buildVectorGraphNow((phase, processedNodes, totalNodes, vectorAccesses) -> {
          if ("persisting".equals(phase) && processedNodes > 0 && processedNodes == totalNodes)
            throw new OutOfMemoryError("simulated for issue #6503 test");
        }))
            .as("the persist block must swallow the OOM the same way it already swallows an ordinary Exception "
                + "there, not let it escape as an unhandled Error")
            .doesNotThrowAnyException();

        final LSMVectorIndexGraphManifest.Content manifest = graphManifestField(index).read();
        assertThat(manifest)
            .as("the manifest must exist and refuse the pages, not be silently absent (issue #6106) or left "
                + "vouching for a persist that died mid-write")
            .isNotNull();
        assertThat(manifest.vectorCount())
            .as("an unusable manifest is marked with a negative vector count - no live set can match it")
            .isNegative();

        // The rollback in the same catch block must have run too: no transaction left dangling on the thread.
        assertThat(db.isTransactionActive())
            .as("the failed persist must have rolled back its own transaction, not left one open")
            .isFalse();

        // The in-memory graph itself is unaffected - only the disk persist failed, so the index built the new
        // graph successfully before the injected failure. It must keep answering correctly, and normal writes
        // must keep working (proves nothing is left wedged by the aborted persist transaction).
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", randomVector(new Random(1))).save());
      } finally {
        db.drop();
      }
    }
  }

  private static LSMVectorIndexGraphManifest graphManifestField(final LSMVectorIndex index) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("graphFile");
    field.setAccessible(true);
    final LSMVectorIndexGraphFile graphFile = (LSMVectorIndexGraphFile) field.get(index);
    return graphFile.getManifest();
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
