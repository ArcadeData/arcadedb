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
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8852: the first search after a reopen must not re-read the document of every vector to
 * rebuild the ordinal map when nothing was deleted and the ordinal map recorded next to the graph still describes the
 * live set. The map is validated against the in-memory location index instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue8852ReopenFirstSearchOrdinalMapTest {
  private static final String DB_PATH     = "target/test-databases/Issue8852ReopenFirstSearchOrdinalMapTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 2_000;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void reopenLoadsTheGraphThroughThePersistedOrdinalMap() {
    build();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final RID rid = db.query("sql", "SELECT FROM Doc WHERE id = 7").next().getIdentity().get();

        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), 5, 64);

        assertThat(results.stream().map(Pair::getFirst)).contains(rid);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
            .as("the ordinal map must replace the per-vector document walk").isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void warmUpLoadsTheGraphBeforeTheFirstSearch() {
    build();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("graphNodeCount")).isZero();

        index.warmUp();

        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        assertThat(index.findNeighborsFromVector(embedding(3), 3, 64)).hasSize(3);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void deletesFallBackToThePreviousPaths() {
    build();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id = 5"));
        // The location index of a fresh session holds no tombstone, so a vector id that is gone is simply absent
        // from the map's point of view: whatever path loads the graph must still answer correctly.
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.findNeighborsFromVector(embedding(9), 3, 64)).hasSize(3);
      } finally {
        db.drop();
      }
    }
  }

  private static void build() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
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
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 1000 == 999) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x8852L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
  }
}
