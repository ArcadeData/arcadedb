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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opening a database used to walk every page of every {@code LSM_VECTOR} index to rebuild the in-memory location
 * map, whether or not the session went on to search it: one page parse and one map insert per indexed vector, paid
 * again on every open (issue #6722). The graph, built at the very same call site, has always been deferred to the
 * first search. These tests pin the location map down to the same contract - materialised on first use, never on
 * open - and check that everything reading it still sees exactly what it saw before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexLazyLocationLoadTest {
  private static final String DB_PATH    = "target/databases/LSMVectorIndexLazyLocationLoadTest";
  private static final int    DIMENSIONS = 8;
  private static final int    VECTORS    = 200;

  @AfterEach
  void cleanUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void openDoesNotMaterialiseTheLocationMap() {
    createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);

        assertThat(index.areLocationsMaterializedForTest())
            .as("opening the database must not rebuild the location map of an index nobody has asked about")
            .isFalse();
        assertThat(index.residentLocationsForTest().size())
            .as("no page of the index should have been parsed yet")
            .isZero();
      } finally {
        db.close();
      }
    }
  }

  @Test
  void firstUseMaterialisesTheLocationMapWithTheSameContent() {
    createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        // countEntries() reads the location map, so it is a "first use" like any other
        assertThat(index.countEntries()).isEqualTo(VECTORS);
        assertThat(index.areLocationsMaterializedForTest())
            .as("the first caller that needs the locations pays for materialising them")
            .isTrue();
        assertThat(index.residentLocationsForTest().size()).isEqualTo(VECTORS);
      } finally {
        db.close();
      }
    }
  }

  @Test
  void statsReportTheWholeCorpusAfterReopen() {
    createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final Map<String, Long> stats = vectorIndexOf(db).getStats();
        assertThat(stats.get("totalVectors")).isEqualTo((long) VECTORS);
        assertThat(stats.get("activeVectors")).isEqualTo((long) VECTORS);
      } finally {
        db.close();
      }
    }
  }

  @Test
  void searchAfterReopenReturnsTheSameNeighbours() {
    final float[] query = createDatabaseWithVectorIndex();

    final List<Pair<RID, Float>> expected;
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        expected = vectorIndexOf(db).findNeighborsFromVector(query, 10);
        assertThat(expected).hasSize(10);
      } finally {
        db.close();
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        final List<Pair<RID, Float>> actual = index.findNeighborsFromVector(query, 10);
        assertThat(index.areLocationsMaterializedForTest())
            .as("a search must materialise the locations it resolves RIDs through")
            .isTrue();
        assertThat(actual.stream().map(Pair::getFirst).toList())
            .as("the deferred load must resolve the same RIDs the eager one did")
            .isEqualTo(expected.stream().map(Pair::getFirst).toList());
      } finally {
        db.close();
      }
    }
  }

  /**
   * The ids handed out to new vectors come from the high-water mark the load computes. An insert on a reopened
   * index that had not been materialised yet must therefore materialise it first, or it would restart the sequence
   * at 0 and overwrite the locations of the vectors already on disk.
   */
  @Test
  void insertAfterReopenDoesNotReuseVectorIds() {
    final float[] query = createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        db.transaction(() -> db.newDocument("Doc").set("id", VECTORS).set("embedding", query).save());

        assertThat(index.countEntries())
            .as("the vector inserted after the reopen must be added to the corpus, not overwrite part of it")
            .isEqualTo(VECTORS + 1);
        assertThat(index.residentLocationsForTest().size()).isEqualTo(VECTORS + 1);

        // The exact query vector was just inserted, so it must come back as the nearest neighbour
        final List<Pair<RID, Float>> neighbours = index.findNeighborsFromVector(query, 5);
        assertThat(neighbours).isNotEmpty();
        assertThat(neighbours.getFirst().getSecond()).isEqualTo(0.0f);
      } finally {
        db.close();
      }
    }

    // ...and it survives one more round trip through the deferred load
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        assertThat(vectorIndexOf(db).countEntries()).isEqualTo(VECTORS + 1);
      } finally {
        db.close();
      }
    }
  }

  /**
   * A session that opens the database and closes it again without ever touching the vector index must leave the
   * index exactly as it found it - and, in particular, must not have paid to rebuild anything.
   */
  @Test
  void openAndCloseWithoutTouchingTheIndexKeepsItIntact() {
    createDatabaseWithVectorIndex();

    for (int i = 0; i < 3; i++) {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          assertThat(vectorIndexOf(db).areLocationsMaterializedForTest()).isFalse();
        } finally {
          db.close();
        }
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        assertThat(vectorIndexOf(db).countEntries()).isEqualTo(VECTORS);
      } finally {
        db.close();
      }
    }
  }

  /**
   * The PQ codebooks are loaded together with the locations, so they are deferred with them. The approximate
   * search entry point checks whether PQ is usable BEFORE it reaches the lazy-load, so it has to ask through
   * {@code isPQSearchAvailable()} rather than reading the fields, or the first approximate search after a reopen
   * silently downgrades itself to an exact one.
   */
  @Test
  void productQuantizationIsStillAvailableAfterReopen() {
    final String pqPath = DB_PATH + "-pq";
    FileUtils.deleteRecursively(new File(pqPath));

    final float[] query;
    try (final DatabaseFactory factory = new DatabaseFactory(pqPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
          db.command("sql", "CREATE PROPERTY Doc.id INTEGER");
          db.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        });
        final Random rnd = new Random(3);
        db.transaction(() -> {
          for (int i = 0; i < 300; i++) {
            final float[] v = new float[DIMENSIONS];
            for (int j = 0; j < DIMENSIONS; j++)
              v[j] = (float) rnd.nextGaussian();
            db.newDocument("Doc").set("id", i).set("embedding", v).save();
          }
        });
        db.command("sql", """
            CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
              "dimensions": 8,
              "similarity": "COSINE",
              "quantization": "PRODUCT"
            }""");
        assertThat(vectorIndexOf(db).isPQSearchAvailable())
            .as("precondition: the fixture must actually train PQ")
            .isTrue();
        query = (float[]) db.query("sql", "SELECT embedding FROM Doc WHERE id = 7").next().getProperty("embedding");
      } finally {
        db.close();
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(pqPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        assertThat(index.isPQSearchAvailable())
            .as("the deferred load has to bring the PQ codebooks back with the locations")
            .isTrue();
        assertThat(index.getPQVectorCount()).isEqualTo(300);
        assertThat(index.findNeighborsFromVectorApproximate(query, 5, null)).hasSize(5);
      } finally {
        db.drop();
      }
    }
    FileUtils.deleteRecursively(new File(pqPath));
  }

  /**
   * The deferred parse is O(index size) page reads. Whichever call triggers it, it must not run inside the
   * index-wide exclusive section, or the first write against a freshly reopened index blocks every other reader
   * and writer of that index for the whole scan - which would trade the stall at open for a worse one later, and
   * is exactly why the materialisation has a lock of its own (PR #6731 review).
   * <p>
   * Asserted rather than timed: the property is "the write lock was not held while the scan ran", and the index
   * records that directly. A timing test for the same thing would be a race with a scan that takes microseconds
   * at test scale.
   */
  @Test
  void materialisationNeverRunsUnderTheIndexWriteLock() {
    final float[] query = createDatabaseWithVectorIndex();

    // The insert path: put() reaches the locations through the id sequence
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        db.transaction(() -> db.newDocument("Doc").set("id", VECTORS).set("embedding", query).save());

        assertThat(index.areLocationsMaterializedForTest()).isTrue();
        assertThat(index.didMaterializeUnderWriteLockForTest())
            .as("put() must materialise the locations before it takes the write lock")
            .isFalse();
      } finally {
        db.close();
      }
    }

    // The delete path
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id = 0"));

        assertThat(index.areLocationsMaterializedForTest()).isTrue();
        assertThat(index.didMaterializeUnderWriteLockForTest())
            .as("remove() must materialise the locations before it takes the write lock")
            .isFalse();
      } finally {
        db.close();
      }
    }

    // The search path, which also reaches the write lock through the graph build it triggers
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).isFalse();

        index.findNeighborsFromVector(query, 5);

        assertThat(index.areLocationsMaterializedForTest()).isTrue();
        assertThat(index.didMaterializeUnderWriteLockForTest())
            .as("a search must materialise the locations before any exclusive section it goes on to take")
            .isFalse();
      } finally {
        db.close();
      }
    }
  }

  private LSMVectorIndex vectorIndexOf(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  /**
   * @return the vector of the first indexed document, usable as a query with a known answer
   */
  private float[] createDatabaseWithVectorIndex() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    final float[][] vectors = new float[VECTORS][];
    final Random rnd = new Random(42);
    for (int i = 0; i < VECTORS; i++) {
      final float[] v = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        v[j] = (float) rnd.nextGaussian();
      vectors[i] = v;
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          for (int i = 0; i < VECTORS; i++)
            db.newDocument("Doc").set("id", i).set("embedding", vectors[i]).save();
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\" }");
      } finally {
        db.close();
      }
    }
    return vectors[0];
  }
}
