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
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #13212: a search issued while the first graph build of an index with no resident graph runs must wait for
 * it, as one issued before the build starts does, instead of answering from the delta buffer alone (no rows for a persisted
 * corpus). The build is held at its "building" phase through the progress callback, so the search meets it deterministically.
 */
@Tag("vector")
class Issue13212SearchDuringFirstGraphBuildTest {
  private static final String DB_ROOT    = "target/test-databases/Issue13212SearchDuringFirstGraphBuildTest";
  private static final int    DIMENSIONS = 16;
  private static final int    COUNT      = 1_200;

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
  void aSearchDuringTheFirstBuildOfAReopenedIndexWaitsForTheGraph() {
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        searchWhileTheBuildIsHeldAtItsBuildingPhase(db, vectors);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aSearchDuringTheFirstBuildOfAnIndexWrittenToBeforeItWasSearchedWaitsForTheGraph() {
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        // The first write of the session promotes the state past LOADING with no graph resident
        final float[] extra = randomVector(new Random(77));
        db.transaction(() -> db.newDocument("Doc").set("id", COUNT).set("vector", extra).save());
        vectors.add(extra);
        searchWhileTheBuildIsHeldAtItsBuildingPhase(db, vectors);
      } finally {
        db.drop();
      }
    }
  }

  private static void searchWhileTheBuildIsHeldAtItsBuildingPhase(final Database db, final List<float[]> vectors) {
    final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
    final CountDownLatch building = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread builder = new Thread(() -> index.buildVectorGraphNow((phase, processed, total, accesses) -> {
      if ("building".equals(phase) && building.getCount() > 0) {
        building.countDown();
        awaitQuietly(release);
      }
    }));
    builder.start();
    final AtomicReference<List<Pair<RID, Float>>> answer = new AtomicReference<>();
    final Thread searcher = new Thread(() -> answer.set(index.findNeighborsFromVector(vectors.get(5), 5, 64)));
    try {
      assertThat(building.await(60, TimeUnit.SECONDS)).as("the build reached its building phase").isTrue();
      searcher.start();
      final long deadline = System.currentTimeMillis() + 30_000;
      while (searcher.isAlive() && searcher.getState() != Thread.State.WAITING && searcher.getState() != Thread.State.TIMED_WAITING
          && System.currentTimeMillis() < deadline)
        Thread.sleep(2);
      assertThat(answer.get()).as("the search has no answer of its own mid-build: it waits for the graph").isNull();
      assertThat(searcher.isAlive()).isTrue();

      release.countDown();
      searcher.join(60_000);
      builder.join(60_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      release.countDown();
    }

    assertThat(answer.get()).as("the search answers once the graph is there").hasSize(5);
    try (final var rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = 5")) {
      assertThat(answer.get().getFirst().getFirst()).as("the exact match of the whole corpus").isEqualTo(rs.next().getProperty("@rid"));
    }
  }

  private List<float[]> populateAndCrashWithoutPersistingAGraph() {
    final Random random = new Random(0x13212L);
    final List<float[]> vectors = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.begin();
        for (int i = 0; i < COUNT; i++) {
          final float[] vector = randomVector(random);
          vectors.add(vector);
          db.newDocument("Doc").set("id", i).set("vector", vector).save();
        }
        db.commit();
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
    return vectors;
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat() - 0.5f;
    return vector;
  }

  private static void awaitQuietly(final CountDownLatch latch) {
    try {
      latch.await(60, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
