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
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the mislabeling gap review round 4 found in {@code releaseBackgroundResources()}'s
 * post-cancellation recheck (added addressing round 2): the recheck used to key off {@code needsGraphBuild()}
 * alone, which is {@code true} whenever {@code graphState == MUTABLE} for ANY reason - not only "flush() skipped
 * an owed rebuild". {@code flush()}'s OTHER branch, the one that actually ATTEMPTS a synchronous build (below
 * {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}), can also leave {@code graphState} at {@code MUTABLE} - a failed persist
 * ({@code markGraphManifestUnusable()} correctly writes {@code closeDeferredRebuild = false}) or new mutations
 * arriving mid-build both do. An ungated recheck would overwrite that correct {@code false} with a misleading
 * {@code true}, reporting "close chose to skip the rebuild" when what actually happened is unrelated to a skip
 * at all.
 * <p>
 * Pinned here at the level of the actual fix - the {@code flushDeferredRebuild} gate - rather than by
 * reproducing a real failed persist (already covered from a different angle by
 * {@code CompactIndexGraphPersistFailureTest}/{@code Issue6503GraphPersistOutOfMemoryTest}): the recheck must do
 * nothing at all when {@code flush()} never ran, regardless of what {@code needsGraphBuild()} says, because
 * {@code flush()} not having run means it cannot possibly have chosen to skip anything.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6657RecheckDoesNotMislabelUnattemptedFlushTest {
  private static final String DB_ROOT    = "target/test-databases/Issue6657RecheckDoesNotMislabelUnattemptedFlushTest";
  private static final int    DIMENSIONS = 8;

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
  void releaseBackgroundResourcesDoesNotMarkDeferredWhenFlushNeverRan() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        final Random rnd = new Random(17);
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.transaction(() -> db.newDocument("Doc").set("id", 0).set("vector", randomVector(rnd)).save());

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("graphState"))
            .as("precondition: MUTABLE - needsGraphBuild() must be true here, or this test does not exercise the "
                + "case the old, ungated recheck would have mislabeled")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(index.getStats().get("closeTimeRebuildPending")).isEqualTo(0L);

        // flush() is deliberately never called - releaseBackgroundResources() alone, standing in for a build
        // attempt that failed (or any other reason graphState stayed MUTABLE without flush() choosing to skip).
        index.releaseBackgroundResources();

        assertThat(index.getStats().get("closeTimeRebuildPending"))
            .as("the recheck must not fire when flush() never ran - it cannot have skipped a build it never "
                + "considered, so needsGraphBuild() alone must not be enough to mark a deferral")
            .isEqualTo(0L);
      } finally {
        if (db.isOpen())
          db.drop();
      }
    }
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
}
