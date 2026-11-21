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
import com.arcadedb.database.TransactionContext;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A commit replays its index operations in its first phase, which hands out the new vector id and records its location, and
 * writes the pages in its second phase, which is when a reader of the pages can see them. A full rebuild that takes its snapshot
 * of the id sequence in between reads pages without the entry and carries over only the ids from the snapshot on, so the id
 * of the update it never read, and was not handed after the snapshot, got no location: the record held a vector in the pages
 * and none in the location index, and the index counted one record short until the next rebuild (issue #12828).
 */
class Issue12828RebuildSnapshotInFlightCommitTest {
  private static final String DB_PATH    = "target/test-databases/Issue12828RebuildSnapshotInFlightCommitTest";
  private static final int    DIMENSIONS = 8;
  private static final int    BASE       = 120;

  private final Random random = new Random(12828);

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
  void anUpdateReplayedBeforeTheRebuildSnapshotAndFlushedAfterItKeepsItsLocation() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        });
        final List<RID> rids = new ArrayList<>();
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++)
            rids.add(db.newDocument("Doc").set("vector", vector()).save().getIdentity());
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
            .getIndexesOnBuckets()[0];
        index.buildVectorGraphNow();

        final RID updated = rids.get(7);
        final float[] newVector = vector();
        final CountDownLatch replayed = new CountDownLatch(1);
        final CountDownLatch rebuilt = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread committer = new Thread(() -> {
          try {
            db.begin();
            final MutableDocument document = db.lookupByRID(updated, true).asDocument().modify();
            document.set("vector", newVector);
            document.save();
            final TransactionContext transaction = ((DatabaseInternal) db).getTransaction();
            // Phase 1 replays the index operations: the new vector has its id and its location, its page is not visible yet
            final TransactionContext.TransactionPhase1 phase1 = transaction.commit1stPhase(true);
            replayed.countDown();
            if (!rebuilt.await(1, TimeUnit.MINUTES))
              throw new IllegalStateException("the rebuild did not read the pages");
            transaction.commit2ndPhase(phase1);
          } catch (final Throwable t) {
            failure.set(t);
            replayed.countDown();
          }
        });
        committer.start();
        assertThat(replayed.await(1, TimeUnit.MINUTES)).isTrue();
        assertThat(failure.get()).isNull();

        // The rebuild has taken its snapshot and read the pages, which lack the update: let the commit write them now, and publish
        index.setAfterLiveSetReadForTest(() -> {
          rebuilt.countDown();
          try {
            committer.join(TimeUnit.MINUTES.toMillis(1));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
        try {
          index.buildVectorGraphNow();
        } finally {
          index.setAfterLiveSetReadForTest(null);
        }
        assertThat(committer.isAlive()).isFalse();
        assertThat(failure.get()).isNull();

        assertThat(index.residentLocationsForTest().getVectorIdsForRid(updated)).as("the updated record keeps a live vector id").hasSize(1);
        assertThat(index.countEntries()).as("one vector per record").isEqualTo(BASE);
      } finally {
        db.drop();
      }
    }
  }

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }
}
