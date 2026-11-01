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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full rebuild reads the live set from the index pages without holding the index lock, then publishes a location index
 * built from what it read. A delete or an insert committed in between used to be lost by that publish: the deleted vector
 * came back live, so a search returned its deleted record, and the inserted one lost its location, so a later delete of
 * its record wrote no tombstone and the next rebuild brought it back from the pages (issue #11099).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexRebuildRacingWritesTest {
  private static final String DB_PATH    = "target/test-databases/LSMVectorIndexRebuildRacingWritesTest";
  private static final int    DIMENSIONS = 16;
  private static final int    BASE       = 600;
  private static final int    DELETED    = 25;
  private static final int    INSERTED   = 9;

  private final Random random = new Random(5397);

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void writesCommittedWhileARebuildReadsThePagesSurviveItsPublish() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        });
        final List<RID> base = new ArrayList<>();
        final List<float[]> baseVectors = new ArrayList<>();
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++) {
            final float[] vector = vector();
            base.add(db.newDocument("Doc").set("vector", vector).save().getIdentity());
            baseVectors.add(vector);
          }
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
            .getIndexesOnBuckets()[0];
        index.buildVectorGraphNow();

        // Committed by another thread once the rebuild has read the pages and before it publishes what it read
        final List<RID> inserted = new ArrayList<>();
        final List<float[]> insertedVectors = new ArrayList<>();
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        index.setAfterLiveSetReadForTest(() -> {
          final Thread writer = new Thread(() -> {
            try {
              db.transaction(() -> {
                for (int i = 0; i < DELETED; i++)
                  db.lookupByRID(base.get(i), false).asDocument().delete();
                for (int i = 0; i < INSERTED; i++) {
                  final float[] vector = vector();
                  inserted.add(db.newDocument("Doc").set("vector", vector).save().getIdentity());
                  insertedVectors.add(vector);
                }
              });
            } catch (final Throwable t) {
              writerFailure.set(t);
            }
          });
          writer.start();
          try {
            writer.join();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
        try {
          index.buildVectorGraphNow();
        } finally {
          index.setAfterLiveSetReadForTest(null);
        }
        assertThat(writerFailure.get()).isNull();
        assertThat(inserted).hasSize(INSERTED);

        final VectorLocationIndex locations = index.residentLocationsForTest();
        for (int i = 0; i < DELETED; i++)
          assertThat(locations.getVectorIdsForRid(base.get(i))).as("the vector of deleted %s is not live", base.get(i)).isEmpty();
        for (final RID rid : inserted)
          assertThat(locations.getVectorIdsForRid(rid)).as("the vector of inserted %s is live", rid).hasSize(1);
        assertThat(index.countEntries()).isEqualTo(BASE - DELETED + INSERTED);
        for (int i = 0; i < DELETED; i++)
          assertNoDeletedRecordFound(db, index, baseVectors.get(i));

        // The insert's location is what its delete writes the tombstone from: without it the next rebuild finds the vector
        // still live on the pages
        db.transaction(() -> {
          for (final RID rid : inserted)
            db.lookupByRID(rid, false).asDocument().delete();
        });
        index.buildVectorGraphNow();
        assertThat(index.countEntries()).isEqualTo(BASE - DELETED);
        for (final float[] vector : insertedVectors)
          assertNoDeletedRecordFound(db, index, vector);
      } finally {
        db.drop();
      }
    }
  }

  private static void assertNoDeletedRecordFound(final Database db, final LSMVectorIndex index, final float[] query) {
    db.transaction(() -> {
      final List<Pair<RID, Float>> found = index.findNeighborsFromVector(query, 5);
      assertThat(found).isNotEmpty();
      for (final Pair<RID, Float> hit : found)
        assertThat(db.existsRecord(hit.getFirst())).as("%s returned by the search exists", hit.getFirst()).isTrue();
    });
  }

  private float[] vector() {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat();
    return v;
  }
}
