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
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A persisted vector graph must survive {@code COMPACT INDEX} and stay reusable across reopens, with no
 * rebuild and no orphaned file left behind by the compaction itself.
 * <p>
 * Two defects compounded here (issue #6495). First, {@code getOrCreateGraphFile()} built the graph file's
 * path from {@code mutable.getFilePath()}, which already carries the
 * {@code .<fileId>.<pageSize>.v<version>.lsmvecidx} suffix. {@code ComponentFile} derives a component's
 * registered name from its path, so the graph registered as
 * {@code <index>.<fileId>.<pageSize>.v<version>.lsmvecidx_vecgraph} while
 * {@code discoverAndLoadGraphFile()} looks for {@code <mutable.getName()>_vecgraph}. The two could never
 * match, so every open failed to find a graph, rebuilt from scratch, and wrote one more file that would
 * never be found either - unbounded growth on every reopen.
 * <p>
 * Second, even with that path fixed, compaction itself ({@code rewriteDataFileWithLiveEntries()}) renames
 * the index but leaves the in-memory {@code graphFile} reference pointing at the file registered under the
 * pre-compaction name, so the graph the compaction rebuilds gets persisted back into that same
 * now-unreachable file: correct content, unreachable name. The persist step now drops that stale reference
 * before it is read, letting {@code getOrCreateGraphFile()} register a fresh file under the CURRENT name -
 * the one {@code discoverAndLoadGraphFile()} will actually look for - and drops the superseded file the same
 * way compaction already drops the mutable/compacted components it replaces.
 * <p>
 * This test pins the fully closed loop: after {@code COMPACT INDEX}, not even the first reopen has to
 * rebuild, and the {@code .vecgraph} file count never grows past the one file compaction leaves behind.
 */
@Tag("vector")
class CompactIndexKeepsGraphReusableTest {
  private static final int    DIMENSIONS  = 32;
  private static final int    NUM_VECTORS = 2_000;
  private static final int    REOPENS     = 3;
  private static final String DB_PATH     = "./target/databases/CompactIndexKeepsGraphReusableTest";

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void compactionMustNotOrphanTheGraphOrForceARebuildOnEveryOpen() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7);

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
      db.command("sql", "COMPACT INDEX `Doc[embedding]`");
      db.close();
    }

    // Compaction itself already persists the graph under a name the next open can find: nothing between here
    // and the first reopen should add or remove a single .vecgraph file.
    final int settledFileCount = countGraphFiles();
    assertThat(settledFileCount)
        .as("COMPACT INDEX must leave exactly one .vecgraph file behind, not one orphan plus a replacement")
        .isEqualTo(1);

    for (int open = 1; open <= REOPENS; open++) {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
        final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
        waitForGraph(db, lsm);

        assertThat(lsm.getStats().get("graphRebuildCount"))
            .as("open %d rebuilt the graph; the one compaction persisted should have been found and reused",
                open)
            .isEqualTo(0L);
        db.close();
      }

      assertThat(countGraphFiles())
          .as("open %d left another .vecgraph behind; the count must never grow past the settled one", open)
          .isEqualTo(settledFileCount);
    }
  }

  private static void waitForGraph(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    waitForGraph(db, (LSMVectorIndex) idx.getIndexesOnBuckets()[0]);
  }

  private static void waitForGraph(final Database db, final LSMVectorIndex lsm) {
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
