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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6527: {@code LSMVectorIndex.graphFile} is written under {@code graphBuildLock} (builders are already
 * serialized against each other by that mutex - see {@link #buildGraphFromScratchWithRetry} usage in the
 * production class) but read by several call sites - {@link Index#getFileIds()} in particular - that take
 * neither that lock, nor {@code lock.readLock()}, nor any other synchronization. A plain (non-{@code volatile})
 * field gives the Java Memory Model no happens-before edge between a builder's write and a reader on another
 * thread, so a reader is not guaranteed to ever observe a reassignment - exactly the class of bug issue #4937
 * fixed once already: {@code getFileIds()} is documented there as needing the graph file's id in the commit-lock
 * set "because the companion graph file receives page writes during the transaction too."
 * <p>
 * The fix makes {@code graphFile} {@code volatile}, matching the pattern already used by every other field in
 * this class that is written under one lock (or under a dedicated build mutex) and read without any lock at all
 * ({@code graphState}, {@code graphIndex}, {@code ordinalToVectorId}, {@code vectorIndex}). That gives every read
 * site a happens-before edge against the writer for free, without adding lock contention to {@code getFileIds()},
 * which runs on every transaction's commit-lock computation.
 * <p>
 * The first test below pins the fix itself, so a future edit cannot silently drop the modifier. The second is a
 * functional regression for the concrete scenario the issue traces its concern to: {@code COMPACT INDEX}
 * reassigns {@code graphFile} to a freshly registered file (see {@code CompactIndexKeepsGraphReusableTest}), and
 * {@link Index#getFileIds()} must report that new file's id afterward, not a stale one.
 */
@Tag("vector")
class LSMVectorIndexGraphFileVisibilityTest {
  private static final int    DIMENSIONS  = 32;
  private static final int    NUM_VECTORS = 500;
  private static final String DB_PATH     = "./target/databases/LSMVectorIndexGraphFileVisibilityTest";

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void graphFileFieldMustBeVolatileForCrossThreadReadersLikeGetFileIds() throws NoSuchFieldException {
    final Field field = LSMVectorIndex.class.getDeclaredField("graphFile");
    assertThat(Modifier.isVolatile(field.getModifiers()))
        .as("graphFile is written under graphBuildLock/no lock at all and read by getFileIds() and others under "
            + "no lock - it must be volatile so those readers have a happens-before edge against the writer "
            + "(issue #6527)")
        .isTrue();
  }

  @Test
  void getFileIdsMustReportTheGraphFileCompactionRegisteredNotAStaleOne() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(11);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.transaction(() -> {
        final DocumentType t = db.getSchema().createDocumentType("Doc");
        t.createProperty("id", Type.INTEGER);
        t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        for (int i = 0; i < NUM_VECTORS; i++)
          db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
      });
      db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
          + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", \"maxConnections\": 32 }");
      waitForGraph(db);

      final TypeIndex idxBefore = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsmBefore = (LSMVectorIndex) idxBefore.getIndexesOnBuckets()[0];
      final int mutableFileIdBefore = lsmBefore.getFileId();
      final List<Integer> fileIdsBefore = lsmBefore.getFileIds();
      assertThat(fileIdsBefore)
          .as("before compaction, getFileIds() must already include the lazily-created graph file")
          .hasSize(2)
          .contains(mutableFileIdBefore);

      // Compaction renames the mutable component and, per issue #6495/#6524, reassigns graphFile to a freshly
      // registered file under the new name once the rebuilt graph is certified - all of it under graphBuildLock,
      // on the compaction thread.
      db.command("sql", "COMPACT INDEX `Doc[embedding]`");

      final TypeIndex idxAfter = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsmAfter = (LSMVectorIndex) idxAfter.getIndexesOnBuckets()[0];
      final int mutableFileIdAfter = lsmAfter.getFileId();
      final List<Integer> fileIdsAfter = lsmAfter.getFileIds();

      assertThat(fileIdsAfter)
          .as("getFileIds(), read on the calling thread with no lock, must observe the graph file compaction "
              + "just registered - not be missing it, and not still point at the pre-compaction one")
          .hasSize(2)
          .contains(mutableFileIdAfter)
          .doesNotContainAnyElementsOf(fileIdsBefore);

      db.close();
    }
  }

  private static void waitForGraph(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    for (int i = 0; i < 600 && lsm.getStats().get("graphNodeCount") == 0L; i++)
      try { Thread.sleep(50); } catch (final InterruptedException ignored) { break; }
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
