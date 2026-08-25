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
 * Companion to {@link Issue6657CloseTimeRebuildPendingStatTest}, covering the OTHER call site of
 * {@code markCloseTimeRebuildDeferred()} inside {@code flush()}: the {@code needsBuild && !valid} branch, taken
 * when {@code flush()} runs after {@code releaseBackgroundResources()} instead of before it - its own WARNING log
 * says this "should not be reachable" through the ordinary {@code close()} path, since both close paths flush
 * BEFORE releasing. Reaching it here means calling {@code flush()}/{@code releaseBackgroundResources()} directly,
 * out of {@code close()}'s order, the same inversion the WARNING log guards against.
 * <p>
 * Small and fast on purpose - unlike the size-threshold branch, this one does not need
 * {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} vectors to reach, only {@code graphState == MUTABLE}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6657InvalidPathRebuildPendingStatTest {
  private static final String DB_ROOT    = "target/test-databases/Issue6657InvalidPathRebuildPendingStatTest";
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
  void flushAfterBackgroundResourcesAreReleasedStillRecordsTheDeferral() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        final Random rnd = new Random(11);
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
            .as("precondition: MUTABLE, otherwise needsBuild is false and flush() takes neither deferral branch")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(index.getStats().get("closeTimeRebuildPending")).isEqualTo(0L);

        // The inversion the WARNING log in flush() describes: release BEFORE flush, not after. Exercised directly
        // rather than through db.close(), which always does these two in the safe order.
        index.releaseBackgroundResources();
        index.flush();

        assertThat(index.getStats().get("closeTimeRebuildPending"))
            .as("the !valid branch of flush() must record the deferral exactly like the size-threshold branch does")
            .isEqualTo(1L);
      } finally {
        if (db.isOpen())
          db.close();
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
