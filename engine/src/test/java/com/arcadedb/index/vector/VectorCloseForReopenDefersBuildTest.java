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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A database closed for reopen (HA snapshot install, idle close) refuses {@code begin()}, so the synchronous graph build
 * {@code flush()} runs for a small dirty vector index can only fail: its CPU is wasted and it logs two SEVERE stack
 * traces. The close must defer the build to the next search instead, exactly as it already does for a large index.
 */
class VectorCloseForReopenDefersBuildTest {
  private static final String DB_PATH     = "target/test-databases/VectorCloseForReopenDefersBuildTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 200;

  private final List<String> severe = new CopyOnWriteArrayList<>();
  private       Logger       previousLogger;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    previousLogger = LogManager.instance().getLogger();
    final Logger delegate = previousLogger;
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        capture(requester, level, message);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
          final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
          final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        capture(requester, level, message);
        // Forwarded to the delegate's fixed-arity form, not re-wrapped into an array: the two forms differ on what makes a message
        // a format string (see Logger), and a re-wrapped call would hand the delegate 17 slots of null padding as arguments
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12,
            arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public boolean isLoggable(final Object requester, final Level level) {
        return delegate.isLoggable(requester, level);
      }

      @Override
      public void flush() {
        delegate.flush();
      }

      private void capture(final Object requester, final Level level, final String message) {
        if (level == Level.SEVERE && requester instanceof LSMVectorIndex)
          severe.add(message);
      }
    });
  }

  @AfterEach
  void tearDown() {
    LogManager.instance().setLogger(previousLogger);
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void closeForReopenDefersTheSmallIndexBuildAndTheReopenedIndexStillAnswers() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      populate(db);
      final LSMVectorIndex index = vectorIndex(db);
      assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
      final float[] dirtyingVector = randomVector(new Random(5));
      final MutableDocument dirtying = db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", dirtyingVector);
      db.transaction(() -> dirtying.save());
      final RID dirtyingRid = dirtying.getIdentity();
      assertThat(index.getStats().get("graphState")).as("precondition: a dirty (MUTABLE) graph").isEqualTo(2L);

      ((DatabaseInternal) db).closeForReopen();

      assertThat(severe).as("a close for reopen must not attempt a build that begin() is bound to refuse").isEmpty();
      assertThat(index.getStats().get("closeTimeRebuildPending")).as("the build must be recorded as deferred").isEqualTo(1L);

      final Database reopened = factory.open();
      try {
        final LSMVectorIndex reopenedIndex = vectorIndex(reopened);
        assertThat(reopenedIndex.getStats().get("closeTimeRebuildPending")).as("the deferral survives the reopen, before the first search")
            .isEqualTo(1L);
        assertThat(reopenedIndex.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        assertThat(reopened.countType("Doc", false)).isEqualTo(NUM_VECTORS + 1);

        // The vector written just before the close is the one the deferred build owes to the graph: it must be searchable
        final List<Pair<RID, Float>> hits = reopenedIndex.findNeighborsFromVector(dirtyingVector, 1, 64, Set.of(dirtyingRid));
        assertThat(hits).extracting(Pair::getFirst).containsExactly(dirtyingRid);
      } finally {
        reopened.drop();
      }
    }
  }

  private static void populate(final Database db) {
    final Random rnd = new Random(7);
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql",
          "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");
    });
    db.transaction(() -> {
      for (int i = 0; i < NUM_VECTORS; i++)
        db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
    });
  }

  private static float[] randomVector(final Random rnd) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = rnd.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private static float[] queryVector() {
    return randomVector(new Random(99));
  }
}
