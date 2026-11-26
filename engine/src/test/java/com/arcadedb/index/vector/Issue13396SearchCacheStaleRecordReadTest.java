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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.types.VectorFloat;
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
import static org.assertj.core.api.Assertions.offset;

/**
 * A search of an index without quantization reads the vector of a graph node from its record, and keeps what it read in the
 * index-scoped search cache, keyed by the vector id. Two reads of a record do not see the version that id stands for, and the
 * cache used to keep them anyway, so every later search scored the record by an older vector until the next rebuild cleared the
 * cache: a search made inside a transaction that holds the record's page (it wrote a record of its own on the same page, and the
 * page it holds hides what other transactions commit there afterwards), and a read made while the commit that put the vector
 * has made the id live and not yet published the record (issue #13396).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13396SearchCacheStaleRecordReadTest {
  private static final String DB_PATH    = "target/test-databases/Issue13396SearchCacheStaleRecordReadTest";
  private static final int    DIMENSIONS = 8;
  private static final int    BASE       = 40;

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

  /**
   * The shape of the issue: two writers whose records share a page. One re-embeds its record and the graph is rebuilt; the other,
   * in a transaction that wrote its own record on that page before the re-embed, searches, and the walk scores the re-embedded
   * record through the page that transaction holds.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aSearchInsideATransactionHoldingTheRecordPageDoesNotCacheTheVersionItHolds() throws Exception {
    withIndex((db, index) -> {
      final RID reEmbedded = rids.get(10);
      final RID neighbour = rids.get(11);
      final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(reEmbedded.getBucketId());
      assertThat(reEmbedded.getPosition() / bucket.getMaxRecordsInPage()).as("both records lie on one page")
          .isEqualTo(neighbour.getPosition() / bucket.getMaxRecordsInPage());

      final float[] newVector = vector();
      final CountDownLatch pageHeld = new CountDownLatch(1);
      final CountDownLatch reEmbeddedAndRebuilt = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread holder = new Thread(() -> {
        try {
          db.begin();
          try {
            // A write of a record of its own: the transaction holds the page, as it was before the re-embed below
            final MutableDocument document = db.lookupByRID(neighbour, true).asDocument().modify();
            document.set("name", "changed");
            document.save();
            pageHeld.countDown();
            if (!reEmbeddedAndRebuilt.await(1, TimeUnit.MINUTES))
              throw new IllegalStateException("the re-embed did not commit");
            // Scores every node of the graph, the re-embedded record among them, inside the transaction
            assertThat(index.findNeighborsFromVector(vector(), BASE)).hasSize(BASE);
          } finally {
            db.rollback();
          }
        } catch (final Throwable t) {
          failure.set(t);
          pageHeld.countDown();
        }
      });
      holder.start();
      assertThat(pageHeld.await(1, TimeUnit.MINUTES)).isTrue();

      db.transaction(() -> {
        final MutableDocument document = db.lookupByRID(reEmbedded, true).asDocument().modify();
        document.set("vector", newVector);
        document.save();
      });
      // The new vector id becomes a node of the graph, and the publish clears the search cache
      index.buildVectorGraphNow();
      reEmbeddedAndRebuilt.countDown();
      holder.join(TimeUnit.MINUTES.toMillis(1));
      assertThat(holder.isAlive()).isFalse();
      assertThat(failure.get()).isNull();

      // Committed and searched for by its own vector: the record is at distance zero, not at the distance of its old vector
      final List<Pair<RID, Float>> found = index.findNeighborsFromVector(newVector, 1);
      assertThat(found).hasSize(1);
      assertThat(found.getFirst().getFirst()).isEqualTo(reEmbedded);
      assertThat(found.getFirst().getSecond()).as("the distance of the record to its current vector").isCloseTo(0f,
          offset(1e-5f));
    });
  }

  /**
   * The other read the cache must not keep: the commit of the re-embed has replayed its index operations (the new id is live)
   * and not yet published its pages, so the record still reads as the version before.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aReadWhileTheCommitOfTheVectorIsInFlightIsNotCached() throws Exception {
    withIndex((db, index) -> {
      final RID reEmbedded = rids.get(7);
      final float[] newVector = vector();
      final CountDownLatch replayed = new CountDownLatch(1);
      final CountDownLatch read = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread committer = new Thread(() -> {
        try {
          db.begin();
          final MutableDocument document = db.lookupByRID(reEmbedded, true).asDocument().modify();
          document.set("vector", newVector);
          document.save();
          final TransactionContext transaction = ((DatabaseInternal) db).getTransaction();
          final TransactionContext.TransactionPhase1 phase1 = transaction.commit1stPhase(true);
          replayed.countDown();
          if (!read.await(1, TimeUnit.MINUTES))
            throw new IllegalStateException("the vector was not read");
          transaction.commit2ndPhase(phase1);
        } catch (final Throwable t) {
          failure.set(t);
          replayed.countDown();
        }
      });
      committer.start();
      assertThat(replayed.await(1, TimeUnit.MINUTES)).isTrue();
      assertThat(failure.get()).isNull();

      final int[] liveIds = index.residentLocationsForTest().getVectorIdsForRid(reEmbedded);
      assertThat(liveIds).as("the replay made the new id live").hasSize(1);
      final int newId = liveIds[0];
      final FlatVectorCache cache = index.getSearchVectorCache();
      assertThat(cache).isNotNull();
      try {
        // A search reader over the new id, as a graph holding it would read it: the record is not published yet
        final VectorFloat<?> inFlight = searchReaderOf(db, index, newId).getVector(0);
        assertThat(inFlight).isNotNull();
        assertThat(cache.get(newId)).as("a read made while the commit is in flight is not cached").isNull();
      } finally {
        read.countDown();
        committer.join(TimeUnit.MINUTES.toMillis(1));
      }
      assertThat(committer.isAlive()).isFalse();
      assertThat(failure.get()).isNull();

      // Concluded: the read is the version the id stands for, and is cached
      final VectorFloat<?> committed = searchReaderOf(db, index, newId).getVector(0);
      for (int i = 0; i < DIMENSIONS; i++)
        assertThat(committed.get(i)).isEqualTo(newVector[i]);
      final VectorFloat<?> cached = cache.get(newId);
      assertThat(cached).as("a read made once the commit concluded is cached").isNotNull();
      for (int i = 0; i < DIMENSIONS; i++)
        assertThat(cached.get(i)).isEqualTo(newVector[i]);
    });
  }

  private static ArcadePageVectorValues searchReaderOf(final Database db, final LSMVectorIndex index, final int vectorId) {
    return ArcadePageVectorValues.forSearch((DatabaseInternal) db, DIMENSIONS, "vector", index.residentLocationsForTest(),
        new int[] { vectorId }, index, index.getSearchVectorCache());
  }

  private void withIndex(final IndexTest test) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc", 1);
          type.createProperty("name", Type.STRING);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++)
            rids.add(db.newDocument("Doc").set("name", "doc" + i).set("vector", vector()).save().getIdentity());
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
