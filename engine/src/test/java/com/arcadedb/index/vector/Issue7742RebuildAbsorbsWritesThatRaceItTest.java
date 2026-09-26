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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7742: a graph rebuild running alongside ordinary ingestion stopped absorbing the
 * vectors that landed while it ran, and the delta buffer they sit in was then never drained.
 * <p>
 * Two independent defects on {@code buildGraphFromScratchExclusively}, both of which need a writer racing the
 * build to show up at all - which is why the symptom was machine-dependent and looked like a flaky wait:
 * <ol>
 *   <li>the build indexed the delta buffer by walking it with an iterator and no lock, while {@code put()} appends
 *       to that very list under the write lock. A concurrent insert therefore threw
 *       {@code ConcurrentModificationException} out of the build. The async rebuild catches it, logs it, and
 *       leaves {@code completed} false - so it neither chains into the next rebuild nor pays off the mutations it
 *       had snapshotted, and an index under sustained ingestion stops absorbing anything at all;</li>
 *   <li>the build read its two snapshots - the next vector id, which decides what the buffer keeps, and the
 *       mutation counter, which decides what the rebuild pays off - as two separate unlocked reads. A writer
 *       landing between them makes the pair describe different instants: the entry is kept in the buffer AND
 *       counted as paid for. The chained rebuild compares that counter with its threshold and declines, and
 *       {@code cancelInactivityRebuildTimer()} cancels the one mechanism that would still have picked it up. The
 *       vector stays buffered for the rest of the session and every query scans it.</li>
 * </ol>
 * The invariant both fixes restore is the one asserted below: <b>a pending vector is always accounted for</b> -
 * anything still in the buffer that the build did not put in the graph is matched by a mutation the counter still
 * owes, so something is always coming for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue7742RebuildAbsorbsWritesThatRaceItTest {
  private static final String DB_PATH    = "./target/databases/Issue7742RebuildAbsorbsWritesThatRaceItTest";
  private static final int    DIMENSIONS = 16;
  /**
   * Rows seeded before the first build, none of which the graph has absorbed yet - so they are all still in the
   * delta buffer when the build below walks it. That walk is the window a concurrent insert has to land in, and
   * its width IS the buffer size: with a few hundred rows the race is won maybe one run in three, which is a test
   * that reports the defect as a flake. With this many it is won every time.
   */
  private static final int    SEED_ROWS   = 15_000;
  private static final int    ROUNDS      = 3;
  /**
   * Total rows the racing writer inserts across the whole test.
   * <p>
   * Capped, but NOT throttled. What has to happen for either defect to show is a write landing inside a build's
   * own window, and that window is milliseconds on an index this size - a writer that pauses between batches
   * mostly misses it and the test passes on the broken engine. The cap is what keeps the run bounded instead: the
   * rebuild is O(index size), so a writer left running for the whole test would make each round dearer than the
   * last.
   */
  private static final int    WRITER_ROWS = 4_000;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aRebuildRacingAnIngestNeitherDiesNorStrandsAPendingVector() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      Thread writer = null;
      try {
        // The inactivity timer is off so the ONLY thing that can absorb a pending vector is a rebuild. That is
        // what makes the accounting invariant below observable: with the timer on, a stranded vector is picked up
        // eventually and the defect hides.
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100);
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

        db.transaction(() -> {
          final DocumentType t = db.getSchema().createDocumentType("Doc");
          t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
            + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");

        final LSMVectorIndex idx = (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("Doc[embedding]"))
            .getIndexesOnBuckets()[0];

        final Random seedRandom = new Random(7742);
        db.transaction(() -> {
          for (int i = 0; i < SEED_ROWS; i++)
            db.newDocument("Doc").set("embedding", randomVector(seedRandom)).save();
        });

        writer = new Thread(() -> {
          final Random random = new Random(42);
          try {
            for (int written = 0; written < WRITER_ROWS && !stop.get(); written += 20)
              db.transaction(() -> {
                for (int i = 0; i < 20; i++)
                  db.newDocument("Doc").set("embedding", randomVector(random)).save();
              });
          } catch (final Throwable t) {
            writerFailure.set(t);
          }
        }, "issue-7742-writer");
        writer.setDaemon(true);
        writer.start();

        for (int round = 0; round < ROUNDS; round++) {
          // Before the fix this threw ConcurrentModificationException out of the build, usually on the first round.
          idx.buildVectorGraphNow();

          final Map<String, Long> stats = idx.getStats();
          // Vectors this build orphaned are re-queued into the buffer on purpose (issue #7190) and are already in
          // the graph, so they are not pending work and no counter owes anything for them.
          final long pending = PendingDeltaVectors.of(stats);
          if (pending > 0)
            assertThat(stats.get("mutationsSinceRebuild"))
                .as("round %d left %d vector(s) buffered: the mutation counter must still owe them, or nothing "
                    + "will ever come to absorb them", round, pending)
                .isGreaterThan(0L);
        }

        stop.set(true);
        writer.join(30_000);
        assertThat(writer.isAlive())
            .as("the writer must have stopped, or the final build below still races it and its assertion would "
                + "fail for a reason its message denies")
            .isFalse();
        assertThat(writerFailure.get()).as("the ingest must survive every rebuild that ran alongside it").isNull();

        // One last build with nothing writing: it must absorb everything that is genuinely pending.
        idx.buildVectorGraphNow();
        final Map<String, Long> stats = idx.getStats();
        assertThat(PendingDeltaVectors.of(stats))
            .as("a build with no writer racing it must leave nothing pending behind")
            .isZero();
      } finally {
        stop.set(true);
        if (writer != null)
          writer.join(30_000);
        db.drop();
      }
    }
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat() * 2 - 1;
    return v;
  }
}
