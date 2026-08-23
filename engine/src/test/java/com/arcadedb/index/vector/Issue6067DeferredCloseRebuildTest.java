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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6067: {@code flush()}'s rebuild-before-close condition (triggered whenever
 * {@code graphState == MUTABLE}, i.e. any write since the last graph build) always calls
 * {@code buildGraphFromScratch()} SYNCHRONOUSLY on the closing thread, with no size-based deferral - unlike the
 * search path's {@code rebuildGraphBeforeSearch()}, which already runs an async rebuild above 1000 vectors
 * ({@code ASYNC_REBUILD_MIN_GRAPH_SIZE}).
 * <p>
 * Measured on the issue (comment from 2026-08-23): a single write to a 200,000-vector index made {@code close()}
 * block for ~43 seconds - the same cost whether one vector or a thousand were written, because the rebuild is
 * always a full one. The fix defers the close-time rebuild to the next search on the index once the index is at
 * or above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}, reusing {@code ensureGraphAvailable()}'s existing
 * stale-persisted-graph detection (already exercised on every ordinary reopen) rather than paying the full cost
 * synchronously inside {@code close()}.
 * <p>
 * Two things are pinned here, because either alone would pass against a half-fix: {@code close()} itself must
 * return promptly (not merely "eventually", which a half-fixed async-but-still-blocking-close variant could also
 * satisfy), and no data may be lost - a later reopen and search must still find every vector, including the one
 * written after the last full build, once the deferred rebuild actually runs.
 * <p>
 * Same per-vector build parameters as {@code LSMVectorIndexCloseCancelsInFlightBuildTest} (128 dimensions,
 * {@code maxConnections=64}, {@code beamWidth=400}), scaled up to 20,000 vectors so a full rebuild reliably takes
 * multiple real seconds - long enough that the tripwire bound below cannot pass by accident.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue6067DeferredCloseRebuildTest {
  private static final String DB_ROOT         = "target/test-databases/Issue6067DeferredCloseRebuildTest";
  private static final int    DIMENSIONS      = 128;
  private static final int    NUM_VECTORS     = 20_000;
  private static final int    MAX_CONNECTIONS = 64;
  private static final int    BEAM_WIDTH      = 400;

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
  void closeDefersTheRebuildForALargeMutatedIndexInsteadOfBlockingOnAFullRebuild() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);

        // Force one complete, persisted build so the index starts IMMUTABLE rather than LOADING - isolates the
        // case under test (a single write after an already-built graph) from the separate first-ever-build case.
        index.buildVectorGraphNow();
        assertThat(index.getStats().get("graphState")).as("precondition: graph must be built and IMMUTABLE first")
            .isEqualTo(1L); // GraphState.IMMUTABLE

        // One more write - mirrors the issue's own measurement that cost does not scale with what was written.
        db.transaction(
            () -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", extraVector()).save());

        assertThat(index.getStats().get("graphState"))
            .as("precondition: MUTABLE, otherwise flush()'s rebuild-before-close condition is never reached")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: above the async-rebuild threshold, otherwise close() takes the pre-existing "
                + "small-graph synchronous path unaffected by this fix")
            .isGreaterThanOrEqualTo(1000L);

        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        db.close();
        // Measured on this fixture: ~50ms deferred vs. ~2.4s for a full synchronous rebuild - the bound below
        // is a tripwire between the two (see StallAwareStopwatch), with wide margin on both sides.
        stopwatch.assertGaveUpWithin(1_000,
            "a close() that defers the graph rebuild from one that runs a full synchronous rebuild to completion");
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Reopen and verify nothing was lost: the deferred rebuild must still make every vector - including the one
    // written after the last full build - reachable by search once it actually runs, proving the fix trades
    // promptness for a lazily-paid rebuild, not for correctness.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
          assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo((long) (NUM_VECTORS + 1));
        }

        final LSMVectorIndex index = vectorIndex(db);
        final var results = index.findNeighborsFromVector(extraVector(), 5, 64);
        assertThat(results)
            .as("the deferred rebuild must still make the vector written right before close() reachable by "
                + "search after reopen")
            .hasSize(5);
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
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
          + ", \"beamWidth\": " + BEAM_WIDTH + " }");
    });

    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++) {
      db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
      if (i % 1000 == 999) {
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
