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
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Closing a database rebuilds a vector graph that is already complete and already on disk.
 * <p>
 * A Vamana build leaves a few nodes unreachable from the entry point; jvector ships no connectivity
 * repair pass, so the engine records them and serves them from the delta scan. But
 * {@code buildGraphFromScratchExclusively()} folds them into the pending-mutation list before it
 * derives the graph state:
 * <pre>
 *   remaining.addAll(unreachableEntries);                          // LSMVectorIndex:2424
 *   this.graphState = remaining.isEmpty() ? IMMUTABLE : MUTABLE;   // LSMVectorIndex:2436
 * </pre>
 * so "this build orphaned a few nodes" and "this index has unflushed writes" collapse into one bit.
 * After a bulk load the index therefore reports {@code graphState=MUTABLE} with
 * {@code mutationsSinceRebuild=0} and a complete graph - a state this test asserts directly.
 * <p>
 * The two consumers of that bit then disagree. {@code rebuildGraphBeforeSearch()} requires
 * {@code mutationsSinceSerialize > 0} before it will rebuild (:2902), so searches correctly do
 * nothing. {@code flush()}, which {@code close()} calls via {@code LocalDatabase.closeDurableParts},
 * tests {@code graphState} alone (:5518) and rebuilds the whole graph. The rebuild orphans nodes
 * again, so it cannot clear the condition that triggered it.
 * <p>
 * The state does not outlive the process - a quiesced index reopens {@code IMMUTABLE} off the
 * persisted graph and closes in milliseconds - so the cost falls on every session that builds or
 * writes, not on every close. At DEEP-10M that is a ~5,500 s rebuild appended to a ~5,500 s build,
 * and it is what pushes the tier past a heap ceiling the build alone fits comfortably.
 * <p>
 * The mutation counter was already guarded against exactly this loop ("would rebuild itself
 * forever", :2372-2377). The state flag was not.
 */
@Tag("vector")
class VectorIndexRebuildsOnEveryCloseTest {
  private static final int    DIMENSIONS      = 128;
  private static final int    NUM_VECTORS     = 50_000;
  private static final int    MAX_CONNECTIONS = 32;   // the engine default
  private static final int    BEAM_WIDTH      = 100;
  private static final String DB_PATH         = "./target/databases/VectorIndexRebuildsOnEveryCloseTest";
  /**
   * Tripwire, not a latency budget. Close doing nothing measured 0.07 s; the rebuild it must not do measured
   * 10.8 s at this size. 5 s sits well clear of both, and per CLAUDE.md a wider bound cannot turn a passing
   * run red.
   */
  private static final long   CLOSE_TRIPWIRE_MS = 5_000;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void closingMustNotRebuildAGraphThatIsAlreadyCompleteAndPersisted() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();

      // Bulk-load first, then index. This is the shape a vector bulk load takes, and it leaves the
      // index with no pending writes of its own - which is what makes the rebuild below indefensible.
      final Random rng = new Random(7);
      db.transaction(() -> {
        final DocumentType docType = db.getSchema().createDocumentType("Doc");
        docType.createProperty("id", Type.INTEGER);
        docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        for (int i = 0; i < NUM_VECTORS; i++)
          db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
      });

      db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
          + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", "
          + "\"maxConnections\": " + MAX_CONNECTIONS + ", \"beamWidth\": " + BEAM_WIDTH + ", "
          + "\"storeVectorsInGraph\": false, \"addHierarchy\": true }");

      final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      for (int i = 0; i < 900 && lsm.getStats().get("graphNodeCount") == 0L; i++)
        try { Thread.sleep(100); } catch (final InterruptedException ignored) { break; }

      final Map<String, Long> afterBuild = lsm.getStats();

      assertThat(afterBuild.get("graphNodeCount"))
          .as("the graph was built over every loaded vector").isEqualTo((long) NUM_VECTORS);
      assertThat(afterBuild.get("mutationsSinceRebuild"))
          .as("a completed build leaves nothing pending to flush").isEqualTo(0L);
      assertThat(afterBuild.get("graphState"))
          .as("THE DEFECT: a complete, persisted graph with zero pending mutations is still marked "
              + "MUTABLE(2), because the build's own orphaned nodes were added to the pending list")
          .isEqualTo(1L); // IMMUTABLE

      // StallAwareStopwatch, not System.currentTimeMillis(): a full-suite run shares one JVM and a
      // stop-the-world pause inside the measured window would otherwise flake this independently of any
      // regression (CLAUDE.md, issue #6260).
      final StallAwareStopwatch closing = StallAwareStopwatch.start();
      db.close();
      closing.assertGaveUpWithin(CLOSE_TRIPWIRE_MS,
          "a close that persists an already-persisted graph from one that rebuilds it from scratch");
    }
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
