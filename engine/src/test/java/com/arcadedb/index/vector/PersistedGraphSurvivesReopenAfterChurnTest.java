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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A graph that a full build has just persisted must be accepted by the very next load of the same pages.
 * <p>
 * Production symptom (client cluster, 112,914 vectors built, 112,623 live at the next load, the same 291-vector gap on
 * every cycle): the graph build derives its live set from the pages keyed by RID, while a load replays the same pages
 * keyed by vector id. When the two derivations disagree the persisted graph is rejected by the usability check on the
 * search path, rebuilt synchronously, persisted, and rejected again.
 * <p>
 * Each test drives one churn shape that produces page entries the two derivations could read differently (updates
 * that mint a new id for the same RID, deletes, delete plus re-insert, update then delete, repeated updates), across
 * separate transactions as an ingest does, then checks that (1) the live set a rebuild publishes is the live set a
 * reload reconstructs, and (2) the reload reuses the graph instead of rebuilding it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class PersistedGraphSurvivesReopenAfterChurnTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int LIVE       = 300;

  @Test
  void embeddingUpdatesInSeparateTransactions() {
    createSchemaAndLoad();
    for (int round = 0; round < 3; round++) {
      final int r = round;
      for (int i = 0; i < LIVE; i += 3)
        updateEmbedding(i, 10_000 * (r + 1) + i);
    }
    assertRebuildThenReopenReusesGraph();
  }

  @Test
  void deletes() {
    createSchemaAndLoad();
    for (int i = 0; i < LIVE; i += 7)
      deleteDoc(i);
    assertRebuildThenReopenReusesGraph();
  }

  @Test
  void deleteThenReinsertTheSameLogicalDocument() {
    createSchemaAndLoad();
    for (int i = 0; i < LIVE; i += 5) {
      deleteDoc(i);
      insertDoc(i, 20_000 + i);
    }
    assertRebuildThenReopenReusesGraph();
  }

  @Test
  void updateThenDeleteTheUpdatedRecord() {
    createSchemaAndLoad();
    for (int i = 0; i < LIVE; i += 4) {
      updateEmbedding(i, 30_000 + i);
      deleteDoc(i);
    }
    assertRebuildThenReopenReusesGraph();
  }

  @Test
  void updateAndDeleteInOneTransaction() {
    createSchemaAndLoad();
    database.transaction(() -> {
      for (int i = 0; i < LIVE; i += 6) {
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(40_000 + i), i);
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(50_000 + i), i);
        if (i % 12 == 0)
          database.command("sql", "DELETE FROM Doc WHERE id = ?", i);
      }
    });
    assertRebuildThenReopenReusesGraph();
  }

  @Test
  void churnAfterAnEarlierPersistedGraphAndCompaction() throws Exception {
    createSchemaAndLoad();
    vectorIndex().buildVectorGraphNow();
    for (int i = 0; i < LIVE; i += 3)
      updateEmbedding(i, 60_000 + i);
    vectorIndex().compact();
    for (int i = 1; i < LIVE; i += 4)
      deleteDoc(i);
    assertRebuildThenReopenReusesGraph();
  }

  private void assertRebuildThenReopenReusesGraph() {
    vectorIndex().buildVectorGraphNow();
    final long liveAfterBuild = vectorIndex().residentLocationsForTest().getActiveVectorIds().count();
    final long docs = database.countType("Doc", false);
    assertThat(liveAfterBuild).as("precondition: the build must publish one live vector per live record").isEqualTo(docs);

    reopenDatabase();

    // One search: this is what loads the persisted graph and runs the usability check on the search path.
    assertThat(vectorIndex().findNeighborsFromVector(embedding(0), 5, 64)).isNotEmpty();

    assertThat(vectorIndex().residentLocationsForTest().getActiveVectorIds().count())
        .as("the live set a reload reconstructs from the pages must be the one the build published")
        .isEqualTo(liveAfterBuild);
    assertThat(vectorIndex().checkIntegrity())
        .as("ordinary churn leaves no record sharing a vector id, so CHECK DATABASE FIX must not rebuild the index")
        .isEmpty();
    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("the graph persisted by the build must be accepted by the next load, not rebuilt again")
        .isEqualTo(0L);
  }

  private void createSchemaAndLoad() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < LIVE; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", i, embedding(i));
    });
  }

  private void updateEmbedding(final int id, final int seed) {
    database.transaction(
        () -> database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(seed), id));
  }

  private void deleteDoc(final int id) {
    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id = ?", id));
  }

  private void insertDoc(final int id, final int seed) {
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", id, embedding(seed)));
  }

  private static float[] embedding(final int seed) {
    final Random random = new Random(0x5EEDL * 31 + seed);
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = (float) random.nextGaussian();
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
