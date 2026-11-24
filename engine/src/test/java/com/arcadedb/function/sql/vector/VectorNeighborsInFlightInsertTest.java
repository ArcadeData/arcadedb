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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.vector.LSMVectorIndex;
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
 * (issue #13323). The row of an index with such a commit in flight is made from the record read, as it was before the deferral,
 * so a record that cannot be read yet is no row of the answer.
 */
class VectorNeighborsInFlightInsertTest {
  private static final String DB_PATH    = "target/test-databases/VectorNeighborsInFlightInsertTest";
  private static final int    DIMENSIONS = 8;
  // Past the 1000 vectors under which a search rebuilds a graph that a write made stale, inside the search (older builds did so after every
  // write, and then waited for the file locks of the commit in flight): the search finds the vector of the commit in the delta buffer
  private static final int    BASE       = 1100;
  private static final String ROWS       =
      "SELECT @rid AS r, external_id AS eid FROM (SELECT expand(vector.neighbors('Chunk[embedding]', ?, ?)))";

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
      final CountDownLatch replayed = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread committer = new Thread(() -> {
        try {
          db.begin();
          db.newVertex("Chunk").set("external_id", "in-flight").set("embedding", vector).save();
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
        assertThat(index.hasCommitReplayInFlight()).as("the commit replayed this index and has not published").isTrue();

        // The search finds the vector of the commit in flight: its record is not readable, which is no property-less row
        final List<Result> during = rows(db, vector);
        assertThat(during).as("every row of the answer has its properties").isNotEmpty()
            .allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
        assertThat(during).hasSizeLessThanOrEqualTo(BASE + 1);
      } finally {
        release.countDown();
        committer.join(TimeUnit.MINUTES.toMillis(1));
      }
      assertThat(committer.isAlive()).isFalse();
      assertThat(failure.get()).isNull();
      assertThat(index.hasCommitReplayInFlight()).as("a published commit is no longer in flight").isFalse();

      // Once published the record is the nearest neighbour of its own vector, with its properties
      final List<Result> after = rows(db, vector);
      assertThat(after).isNotEmpty();
      assertThat(after.getFirst().<String>getProperty("eid")).isEqualTo("in-flight");
      assertThat(after).allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
    });
  }

  /** A rolled back first phase takes its vector back and is no longer in flight: its rows are deferred again. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aRolledBackCommitIsNoLongerInFlight() throws Exception {
    withIndex((db, index) -> {
      final float[] vector = vector();
      db.begin();
      db.newVertex("Chunk").set("external_id", "rolled-back").set("embedding", vector).save();
      final TransactionContext transaction = ((DatabaseInternal) db).getTransaction();
      transaction.commit1stPhase(true);
      assertThat(index.hasCommitReplayInFlight()).isTrue();
      transaction.rollback();
      assertThat(index.hasCommitReplayInFlight()).isFalse();

      final List<Result> rows = rows(db, vector);
      assertThat(rows).extracting(r -> r.<String>getProperty("eid")).isNotEmpty().doesNotContainNull().doesNotContain("rolled-back");
    });
  }

  private static List<Result> rows(final Database db, final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = db.query("sql", ROWS, query, BASE + 10)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
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

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }
}
