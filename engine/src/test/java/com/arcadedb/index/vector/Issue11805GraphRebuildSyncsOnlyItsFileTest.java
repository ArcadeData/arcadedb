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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A vector search that rebuilds a small graph makes the graph file durable, and only the graph file (issue #11805): it
 * used to wait for the flush backlog of the whole database and fsync every file written since its last fsync, on the
 * search thread and with the build lock held, so its cost grew with what OTHER types were writing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11805GraphRebuildSyncsOnlyItsFileTest extends TestHelper {
  private static final int DIMENSIONS               = 16;
  // The graph holds 100 vectors, so the ratio-scaled term of the threshold (20% of the graph) stays below this absolute one
  private static final int MUTATIONS_BEFORE_REBUILD = 50;

  @Test
  void aGraphRebuildOnSearchLeavesTheOtherFilesUnsynced() {
    final DatabaseInternal db = (DatabaseInternal) database;
    // Set on the database rather than read from the global default: the index decides from its own configuration, so the
    // number of vectors the test writes to reach the threshold must be the one the index will use
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, MUTATIONS_BEFORE_REBUILD);
    database.command("sql", "CREATE VERTEX TYPE E BUCKETS 1");
    database.command("sql", "CREATE PROPERTY E.emb ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON E (emb) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'COSINE'}");
    database.command("sql", "CREATE DOCUMENT TYPE W BUCKETS 1");
    database.command("sql", "CREATE PROPERTY W.name STRING");
    database.command("sql", "CREATE INDEX ON W (name) NOTUNIQUE");

    final Random random = new Random(11805);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newVertex("E").set("emb", vector(random)).save();
    });
    assertThat(search(vector(random))).isEqualTo(10);

    // SETTLE EVERY FILE, THEN WRITE THE OTHER TYPE AND ENOUGH VECTORS TO REACH THE MUTATION THRESHOLD: THE NEXT SEARCH REBUILDS
    // THE SMALL GRAPH (below the threshold it searches the new vectors through the delta buffer and does not rebuild)
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
    assertThat(db.getFileManager().syncFiles()).isTrue();

    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("W").set("name", "w" + i).save();
      for (int i = 0; i < MUTATIONS_BEFORE_REBUILD; i++)
        database.newVertex("E").set("emb", vector(random)).save();
    });
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final PaginatedComponentFile otherBucket = (PaginatedComponentFile) db.getFileManager()
        .getFile(db.getSchema().getType("W").getBuckets(false).getFirst().getFileId());
    assertThat(otherBucket.isModifiedSinceLastSync()).as("the other type's bucket was written and not forced yet").isTrue();

    final long syncedBefore = db.getFileManager().getStats().syncedFiles;
    assertThat(search(vector(random))).isEqualTo(10);

    final PaginatedComponentFile graphFile = graphFile(db);
    assertThat(db.getFileManager().getStats().syncedFiles).as("the rebuild on the search thread forces the graph file")
        .isGreaterThan(syncedBefore);
    assertThat(graphFile.isModifiedSinceLastSync()).as("the rebuilt graph file is durable").isFalse();
    assertThat(otherBucket.isModifiedSinceLastSync())
        .as("the rebuild forces its own graph file, not the files other types wrote (issue #11805)").isTrue();
  }

  private int search(final float[] query) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.neighbors`('E[emb]', ?, 10))", (Object) query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private static PaginatedComponentFile graphFile(final DatabaseInternal db) {
    for (final ComponentFile file : db.getFileManager().getFiles())
      if (file instanceof PaginatedComponentFile paginated && LSMVectorIndexGraphFile.FILE_EXT.equals(file.getFileExtension()))
        return paginated;
    throw new AssertionError("The scan no longer reaches the graph file of the vector index");
  }

  private static float[] vector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) random.nextGaussian();
    return v;
  }
}
