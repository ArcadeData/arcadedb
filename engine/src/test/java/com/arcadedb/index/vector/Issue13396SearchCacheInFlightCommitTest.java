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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A commit replays its index operations in its first phase, which hands out the new vector id, and publishes its pages in its
 * second, one by one: the vector page can be visible while the record page is not. An index that keeps its vectors in the records (no
 * quantization) scores a graph node by reading the record, so a search in that window read the PREVIOUS version of the record and
 * cached it under the new id: the record was scored by its old vector for as long as the cache lived, after its commit had returned
 * (issue #13396). The window is marked in the index for as long as it lasts ({@link LSMVectorIndex#isPutByCommitInFlight}); a vector
 * read from a record inside it must not be remembered.
 */
class Issue13396SearchCacheInFlightCommitTest {
  private static final String DB_PATH    = "target/test-databases/Issue13396SearchCacheInFlightCommitTest";
  private static final int    DIMENSIONS = 8;
  private static final int    BASE       = 300;

  private final Random    random = new Random(13396);
  private final List<RID> rids   = new ArrayList<>();

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
  void aVectorReadFromARecordPutByACommitInFlightIsNotCached() throws Exception {
    withIndex((db, index) -> {
      final RID inFlight = rids.get(7);
      final RID settled = rids.get(8);
      final int inFlightId = onlyIdOf(index, inFlight);
      final int settledId = onlyIdOf(index, settled);
      final float[] query = vector();

      // A search scores every node of the graph: each one is read from its record and cached, except a record whose commit has not
      // concluded, which may still hold the previous version
      index.putInFlight(inFlight);
      search(db, index, query);
      assertThat(index.getSearchVectorCache().get(settledId)).as("a settled record is cached as before").isNotNull();
      assertThat(index.getSearchVectorCache().get(inFlightId)).as("a record put by a commit in flight is not cached").isNull();

      // Once the commit has concluded, the next search reads the committed record and caches it
      index.putsConcluded(new RID[] { inFlight }, 1, false);
      assertThat(index.isPutByCommitInFlight(inFlight)).isFalse();
      search(db, index, query);
      assertThat(index.getSearchVectorCache().get(inFlightId)).as("a concluded commit's record is cached").isNotNull();
    });
  }

  /**
   * The other source of the same stale entry: a transaction that has written a record holds its own copy of the page, and with it
   * the previous version of every other record on that page that a later commit has rewritten. Searching through that copy must not
   * remember what it reads (the writers of #13396 search inside their own transactions, over records that share pages).
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aVectorReadThroughAPageTheCallingTransactionHoldsIsNotCached() throws Exception {
    withIndex((db, index) -> {
      final RID own = rids.get(7);
      final RID other = rids.get(8);
      final float[] otherVector = vector();
      final ExecutorService otherSession = Executors.newSingleThreadExecutor();
      try {
        db.begin();
        try {
          // the transaction takes its private copy of the page both records lie on
          final MutableDocument mine = db.lookupByRID(own, true).asDocument().modify();
          mine.set("vector", vector());
          mine.save();

          // another session re-embeds the other record and the index rebuilds, so its graph holds the new id
          otherSession.submit(() -> {
            db.transaction(() -> {
              final MutableDocument theirs = db.lookupByRID(other, true).asDocument().modify();
              theirs.set("vector", otherVector);
              theirs.save();
            });
            index.buildVectorGraphNow();
          }).get(1, TimeUnit.MINUTES);
          final int otherId = onlyIdOf(index, other);

          assertThat(index.findNeighborsFromVector(otherVector, BASE + 10)).isNotEmpty();
          assertThat(index.getSearchVectorCache().get(otherId)).as("a vector read through the transaction's own page is not cached")
              .isNull();
        } finally {
          db.rollback();
        }
        // outside any transaction the same record is read as committed and cached
        search(db, index, otherVector);
        assertThat(index.getSearchVectorCache().get(onlyIdOf(index, other))).as("a committed read is cached").isNotNull();
      } finally {
        otherSession.shutdownNow();
      }
    });
  }

  private static int onlyIdOf(final LSMVectorIndex index, final RID rid) {
    final int[] ids = index.residentLocationsForTest().getVectorIdsForRid(rid);
    assertThat(ids).hasSize(1);
    return ids[0];
  }

  private static void search(final Database db, final LSMVectorIndex index, final float[] query) {
    db.transaction(() -> assertThat(index.findNeighborsFromVector(query, BASE + 10)).isNotEmpty());
  }

  private void withIndex(final IndexTest test) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        });
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++)
            rids.add(db.newDocument("Doc").set("vector", vector()).save().getIdentity());
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
            .getIndexesOnBuckets()[0];
        index.buildVectorGraphNow();
        test.run(db, index);
      } finally {
        db.drop();
      }
    }
  }

  @FunctionalInterface
  private interface IndexTest {
    void run(Database db, LSMVectorIndex index) throws Exception;
  }

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }
}
