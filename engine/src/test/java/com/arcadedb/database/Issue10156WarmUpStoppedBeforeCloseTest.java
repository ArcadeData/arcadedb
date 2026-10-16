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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10156: the background warm-up of the indexes a schema reload re-created (#10021) was stopped by the schema close, the
 * tenth step of the write-locked teardown - after the page flush, the fsync, the "database is closed" mark and the purge of this
 * database's pages from the shared read cache. A warm-up read landing in between re-inserted a page keyed by the closed instance
 * into that cache, and on a drop it read files about to be deleted. #5418's rule is that index background work stops in
 * {@code closeDurableParts}, before any of that; the warm-up stops there now.
 * <p>
 * The invariant asserted is the rule's, not the thread's: at the moment the instance reports itself closed, no warm-up of this
 * database is alive or waiting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue10156WarmUpStoppedBeforeCloseTest {
  private static final String DB_PATH    = "target/test-databases/Issue10156WarmUpStoppedBeforeCloseTest";
  private static final int    DIMENSIONS = 8;
  private static final int    VECTORS    = 200;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.transaction(() -> {
          final Random random = new Random(10156);
          for (int i = 0; i < VECTORS; i++) {
            final float[] vector = new float[DIMENSIONS];
            for (int d = 0; d < DIMENSIONS; d++)
              vector[d] = random.nextFloat();
            db.newDocument("Doc").set("vector", vector).save();
          }
        });
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        db.close();
      }
    }
  }

  @AfterEach
  void tearDown() {
    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;
    GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.reset();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void noWarmUpIsAliveOnceTheDatabaseReportsItselfClosed() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      final List<String> seenAtMarkedClosed = armWarmUpAndWatchTheClose(db);

      db.close();

      assertThat(seenAtMarkedClosed).as("warm-up threads alive when the database already reported itself closed").isEmpty();
    }
  }

  @Test
  void noWarmUpIsAliveOnceADroppedDatabaseReportsItselfClosed() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      final List<String> seenAtMarkedClosed = armWarmUpAndWatchTheClose(db);

      // the files the warm-up would read are deleted right after this close
      db.drop();

      assertThat(seenAtMarkedClosed).as("warm-up threads alive when the dropped database already reported itself closed").isEmpty();
    }
  }

  /**
   * Leaves a warm-up of the reloaded vector index waiting on a delay that does not elapse in the test - its thread alive - and
   * arms the hook that runs right after the close marks the instance closed.
   */
  private static List<String> armWarmUpAndWatchTheClose(final Database db) throws Exception {
    vectorIndex(db).warmUp();
    assertThat(vectorIndex(db).isWarm()).isTrue();

    GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(600_000);
    final LocalSchema schema = ((DatabaseInternal) db).getSchema().getEmbedded();
    schema.load(ComponentFile.MODE.READ_WRITE, true);

    final String threadName = "ArcadeDB-IndexWarmUp-" + db.getName();
    assertThat(warmUpThreads(threadName)).as("the reload queued a warm-up, so the fixture is not vacuous").isNotEmpty();
    assertThat(schema.awaitIndexWarmUp(0)).as("the warm-up is waiting").isFalse();

    final List<String> seen = new ArrayList<>();
    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = () -> seen.addAll(warmUpThreads(threadName));
    return seen;
  }

  private static List<String> warmUpThreads(final String threadName) {
    final List<String> alive = new ArrayList<>();
    for (final Thread thread : Thread.getAllStackTraces().keySet())
      if (thread.isAlive() && thread.getName().equals(threadName))
        alive.add(thread.getName() + " (" + thread.getState() + ")");
    return alive;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
