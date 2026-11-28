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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
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
 * A commit replays its index operations in its first phase, which puts the new vector where a search finds it, and publishes the
 * record pages in its second phase. Between the two a search can answer the new record, and a row made without reading its record
 * (issue #10973) came back with every property null, although the record was neither deleted nor missing, only not committed yet
 * (issue #13323). The row of a record whose vector a commit in flight put is made from the record read, so a record that cannot be
 * read yet is no row of the answer, while every other row is still made without reading its record.
 */
class VectorNeighborsInFlightInsertTest {
  private static final String DB_PATH    = "target/test-databases/VectorNeighborsInFlightInsertTest";
  private static final int    DIMENSIONS = 8;
  // Past the 1000 vectors under which a search rebuilds a graph that a write made stale, inside the search (older builds did so after every
  // write, and then waited for the file locks of the commit in flight): the search finds the vector of the commit in the delta buffer
  private static final int    BASE       = 1100;
  private static final int    K          = BASE + 10;
  private static final String INDEX      = "Chunk[embedding]";
  private static final String ROWS       =
      "SELECT @rid AS r, external_id AS eid FROM (SELECT expand(vector.neighbors('" + INDEX + "', ?, ?)))";

  private final Random random = new Random(13323);

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
  void aSearchDuringAnInsertCommitAnswersNoRowWithoutProperties() throws Exception {
    withIndex((db, index) -> {
      final float[] vector = vector();
      final RID[] inserted = new RID[1];
      duringCommitInFlight(db, () -> {
        inserted[0] = db.newVertex("Chunk").set("external_id", "in-flight").set("embedding", vector).save().getIdentity();
      }, () -> {
        assertThat(index.isPutByCommitInFlight(inserted[0])).as("the commit replayed the vector and has not published").isTrue();

        // The search finds the vector of the commit in flight: its record is not readable, which is no property-less row
        final List<Result> during = rows(db, vector);
        assertThat(during).as("every row of the answer has its properties").hasSize(BASE)
            .allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());

        // Only the row of the commit in flight is read: every committed row is still handed on unread (issue #10973)
        final List<ScoredRecordRow> rows = neighbours(db, vector);
        assertThat(rows).hasSize(BASE).allSatisfy(row -> assertThat(isUnread(row)).isTrue())
            .noneSatisfy(row -> assertThat(row.getRecord().getIdentity()).isEqualTo(inserted[0]));
      });
      assertThat(index.isPutByCommitInFlight(inserted[0])).as("a published commit is no longer in flight").isFalse();

      // Once published the record is the nearest neighbour of its own vector, with its properties, and handed on unread again
      final List<Result> after = rows(db, vector);
      assertThat(after).hasSize(BASE + 1);
      assertThat(after.getFirst().<String>getProperty("eid")).isEqualTo("in-flight");
      assertThat(after).allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
      final ScoredRecordRow first = neighbours(db, vector).getFirst();
      assertThat(first.getRecord().getIdentity()).isEqualTo(inserted[0]);
      assertThat(isUnread(first)).isTrue();
    });
  }

  /**
   * The record of an update in flight is there: its row is read, with the properties it has before the commit publishes. The
   * replay holds the new vector back until the commit concludes (issue #11962), so the record is found by the vector it had
   * committed, and by the new one once the commit has published.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aSearchDuringAnUpdateCommitAnswersTheRecordAsItWas() throws Exception {
    withIndex((db, index) -> {
      final float[] vector = vector();
      final float[] committedVector = vector();
      final RID[] created = new RID[1];
      db.transaction(() -> created[0] = db.newVertex("Chunk").set("external_id", "before").set("embedding", committedVector).save().getIdentity());
      final RID updated = created[0];
      duringCommitInFlight(db, () -> {
        final MutableDocument record = db.lookupByRID(updated, true).asVertex().modify();
        record.set("external_id", "after").set("embedding", vector).save();
      }, () -> {
        assertThat(index.isPutByCommitInFlight(updated)).isTrue();

        final List<Result> during = rows(db, committedVector);
        assertThat(during).hasSize(BASE + 1).allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
        assertThat(during.getFirst().<RID>getProperty("r")).isEqualTo(updated);
        assertThat(during.getFirst().<String>getProperty("eid")).isEqualTo("before");

        final List<ScoredRecordRow> rows = neighbours(db, committedVector);
        assertThat(rows).hasSize(BASE + 1);
        assertThat(rows.getFirst().getRecord().getIdentity()).isEqualTo(updated);
        assertThat(isUnread(rows.getFirst())).as("the row of the commit in flight is read").isFalse();
        assertThat(rows.subList(1, rows.size())).allSatisfy(row -> assertThat(isUnread(row)).isTrue());
      });
      assertThat(index.isPutByCommitInFlight(updated)).isFalse();

      final List<Result> after = rows(db, vector);
      assertThat(after.getFirst().<RID>getProperty("r")).isEqualTo(updated);
      assertThat(after.getFirst().<String>getProperty("eid")).isEqualTo("after");
    });
  }

  /**
   * A commit that puts more records than a replay tracks one by one (VectorIndexReplayUndo.MAX_TRACKED_PUTS, 4096) marks the whole
   * index in flight instead: every row is read while it is, and none of its records answers a row without properties.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aBulkCommitInFlightHasEveryRowRead() throws Exception {
    withIndex((db, index) -> {
      final float[] vector = vector();
      final RID committed = neighbours(db, vector).getFirst().getRecord().getIdentity();
      final RID[] first = new RID[1];
      duringCommitInFlight(db, () -> {
        first[0] = db.newVertex("Chunk").set("external_id", "bulk-0").set("embedding", vector).save().getIdentity();
        for (int i = 1; i <= 4096; i++)
          db.newVertex("Chunk").set("external_id", "bulk-" + i).set("embedding", vector()).save();
      }, () -> {
        assertThat(index.isPutByCommitInFlight(first[0])).isTrue();
        assertThat(index.isPutByCommitInFlight(committed)).as("a bulk commit in flight marks every record").isTrue();

        final List<Result> during = rows(db, vector);
        assertThat(during).hasSize(BASE).allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
        assertThat(neighbours(db, vector)).hasSize(BASE).allSatisfy(row -> assertThat(isUnread(row)).isFalse());
      });
      assertThat(index.isPutByCommitInFlight(first[0])).isFalse();
      assertThat(index.isPutByCommitInFlight(committed)).isFalse();

      final List<Result> after = rows(db, vector);
      assertThat(after.getFirst().<String>getProperty("eid")).isEqualTo("bulk-0");
      assertThat(neighbours(db, vector)).allSatisfy(row -> assertThat(isUnread(row)).isTrue());
    });
  }

  /** A rolled back first phase takes its vector back and is no longer in flight. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aRolledBackCommitIsNoLongerInFlight() throws Exception {
    withIndex((db, index) -> {
      final float[] vector = vector();
      db.begin();
      final RID rolledBack = db.newVertex("Chunk").set("external_id", "rolled-back").set("embedding", vector).save().getIdentity();
      final TransactionContext transaction = ((DatabaseInternal) db).getTransaction();
      transaction.commit1stPhase(true);
      assertThat(index.isPutByCommitInFlight(rolledBack)).isTrue();
      transaction.rollback();
      assertThat(index.isPutByCommitInFlight(rolledBack)).isFalse();

      final List<Result> rows = rows(db, vector);
      assertThat(rows).hasSize(BASE).extracting(r -> r.<String>getProperty("eid")).doesNotContainNull().doesNotContain("rolled-back");
    });
  }

  /**
   * Runs {@code write} in a transaction on a thread of its own and holds its commit between the two phases (the index replayed, the
   * pages not published) while {@code during} runs on this thread, then completes the commit.
   */
  private static void duringCommitInFlight(final Database db, final Runnable write, final ThrowingRunnable during) throws Exception {
    final CountDownLatch replayed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread committer = new Thread(() -> {
      try {
        db.begin();
        write.run();
        final TransactionContext transaction = ((DatabaseInternal) db).getTransaction();
        // Phase 1 replays the index operations: the vector is in the index, the record page is not published yet
        final TransactionContext.TransactionPhase1 phase1 = transaction.commit1stPhase(true);
        replayed.countDown();
        if (!release.await(1, TimeUnit.MINUTES))
          throw new IllegalStateException("the search did not finish");
        transaction.commit2ndPhase(phase1);
      } catch (final Throwable t) {
        failure.set(t);
        replayed.countDown();
      }
    });
    committer.start();
    try {
      assertThat(replayed.await(1, TimeUnit.MINUTES)).isTrue();
      assertThat(failure.get()).isNull();
      during.run();
    } finally {
      release.countDown();
      committer.join(TimeUnit.MINUTES.toMillis(1));
    }
    assertThat(committer.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
  }

  private static List<Result> rows(final Database db, final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = db.query("sql", ROWS, query, K)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  /** The rows of the function itself, which tell a row that read its record from one that did not. */
  private static List<ScoredRecordRow> neighbours(final Database db, final float[] query) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    final Object result = new SQLFunctionVectorNeighbors().execute(null, null, null, new Object[] { INDEX, query, K }, context);
    final List<ScoredRecordRow> rows = new ArrayList<>();
    for (final Object row : (List<?>) result)
      rows.add((ScoredRecordRow) row);
    return rows;
  }

  private static boolean isUnread(final ScoredRecordRow row) {
    return ((BaseRecord) row.getRecord()).getBuffer() == null;
  }

  private void withIndex(final IndexTest test) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().buildVertexType().withName("Chunk").withTotalBuckets(1).create().createProperty("embedding", Type.ARRAY_OF_FLOATS);
          db.getSchema().getType("Chunk").createProperty("external_id", Type.STRING);
          db.command("sql", "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        });
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++)
            db.newVertex("Chunk").set("external_id", "base-" + i).set("embedding", vector()).save();
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Chunk").getPolymorphicIndexByProperties("embedding")
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

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }
}
