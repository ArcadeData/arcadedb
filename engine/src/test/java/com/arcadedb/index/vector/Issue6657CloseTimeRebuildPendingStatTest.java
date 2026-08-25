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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6657: {@code getStats()} did not say whether the last {@code close()} deferred a
 * graph rebuild (issue #6067/#6653) - an operator debugging "why is my first query after reopen slow" had to go
 * looking for the {@code FINE} log line {@code LSMVectorIndex.flush()} emits, rather than checking a stat
 * alongside the existing {@code graphState}/{@code graphRebuildCount}/{@code mutationsSinceRebuild} entries.
 * <p>
 * Three things are pinned, because a half-fix could satisfy any two on its own: the {@code closeTimeRebuildPending}
 * stat must turn on exactly when {@code close()} defers a rebuild, it must still read {@code 1} on a <b>freshly
 * reopened</b> index - i.e. a brand-new {@link LSMVectorIndex} instance, not the one that set it - before that
 * index has been searched (the whole point: answering the question BEFORE the next search silently pays for it),
 * and it must clear back to {@code 0} once that search actually runs the deferred rebuild.
 * <p>
 * A normal close that never deferred anything (below the async-rebuild threshold) is also pinned at {@code 0}, to
 * catch a stat that reads "true" unconditionally.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue6657CloseTimeRebuildPendingStatTest {
  private static final String DB_ROOT         = "target/test-databases/Issue6657CloseTimeRebuildPendingStatTest";
  private static final int    DIMENSIONS      = 16;
  private static final int    NUM_VECTORS     = 1_005; // just above ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)
  private static final int    MAX_CONNECTIONS = 8;
  private static final int    BEAM_WIDTH      = 32;

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
  void closeTimeRebuildPendingTurnsOnAtDeferralSurvivesReopenAndClearsOnceTheDeferredRebuildRuns() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);

        // Force one complete, persisted build so the index starts IMMUTABLE - isolates the deferral this test is
        // about (a write after an already-built graph) from the separate first-ever-build arm.
        index.buildVectorGraphNow();
        assertThat(index.getStats().get("closeTimeRebuildPending"))
            .as("a graph that has just been built cleanly must not report a pending close-time deferral")
            .isEqualTo(0L);
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // One more write on top of the already-built graph - the exact shape flush()'s ASYNC_REBUILD_MIN_GRAPH_SIZE
    // branch defers instead of rebuilding synchronously.
    final LSMVectorIndex indexAtDeferralTime;
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        indexAtDeferralTime = vectorIndex(db);
        db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", extraVector()).save());

        assertThat(indexAtDeferralTime.getStats().get("graphState"))
            .as("precondition: MUTABLE, otherwise flush()'s deferral branch is never reached")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(indexAtDeferralTime.getStats().get("totalVectors"))
            .as("precondition: at or above the async-rebuild threshold, otherwise close() rebuilds synchronously "
                + "and never sets the stat at all")
            .isGreaterThanOrEqualTo(1000L);
        assertThat(indexAtDeferralTime.getStats().get("closeTimeRebuildPending"))
            .as("precondition: not yet deferred - nothing has closed yet this session")
            .isEqualTo(0L);

        db.close();

        // Same object reference the deferring close() ran on: confirms flush() itself set the stat, before even
        // checking whether it is observable on a later, different instance (asserted below).
        assertThat(indexAtDeferralTime.getStats().get("closeTimeRebuildPending"))
            .as("close() deferred a rebuild above the threshold - the stat must say so immediately")
            .isEqualTo(1L);
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Reopen: a brand-new LSMVectorIndex instance is constructed here (LSMVectorIndex.open() factory), with none
    // of indexAtDeferralTime's in-memory state. If the stat were a plain instance field it would read 0 here no
    // matter what the previous session did - this is the assertion that rules that design out.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex reopenedIndex = vectorIndex(db);
        assertThat(reopenedIndex).as("reopen must hand back a different Java instance").isNotSameAs(indexAtDeferralTime);

        assertThat(reopenedIndex.getStats().get("graphState"))
            .as("precondition: LOADING - nothing in this session has searched or written yet")
            .isEqualTo(0L); // GraphState.LOADING
        assertThat(reopenedIndex.getStats().get("closeTimeRebuildPending"))
            .as("the deferral from the previous session's close() must still read 1 on this fresh instance, "
                + "before this session's first search - this is the actual "
                + "\"why is my first query after reopen slow\" moment the issue is about")
            .isEqualTo(1L);
        assertThat(reopenedIndex.getStats().get("graphRebuildCount"))
            .as("precondition: nothing has rebuilt yet in this fresh instance")
            .isZero();

        // The search that finally pays for the deferred rebuild (ensureGraphAvailable() -> buildGraphFromScratch(),
        // synchronous on this call since the graph is not yet loaded this session).
        final var results = reopenedIndex.findNeighborsFromVector(extraVector(), 5, 64);
        assertThat(results)
            .as("the deferred rebuild must still make the vector written right before the deferring close() "
                + "reachable by search")
            .hasSize(5);

        assertThat(reopenedIndex.getStats().get("graphRebuildCount"))
            .as("the search above must be what finally runs the deferred build")
            .isEqualTo(1L);
        assertThat(reopenedIndex.getStats().get("closeTimeRebuildPending"))
            .as("the deferred rebuild caught up - the stat must clear")
            .isEqualTo(0L);
      } finally {
        db.drop();
      }
    }
  }

  private static float[] extraVector() {
    return randomVector(new Random(5));
  }

  private static void populate(final Database db) {
    final Random rnd = new Random(7);
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", com.arcadedb.schema.Type.INTEGER);
      type.createProperty("vector", com.arcadedb.schema.Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
          + ", \"beamWidth\": " + BEAM_WIDTH + " }");
    });

    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++) {
      db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
      if (i % 500 == 499) {
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
}
