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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.RidScore;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9692 (from #9635): {@code vector.sparseNeighbors} asked each sub-index for exactly {@code k} candidates and
 * skipped the ones whose record no longer resolved, so each record a concurrent commit had just deleted cost the caller
 * a result row. The dense function got the widen-and-retry for the same shape in #9368; this is its sparse twin.
 * <p>
 * Exact rescoring is turned off ({@code rescoreOversample: 0}) so the index hands the stale RIDs over as they are,
 * which isolates the SQL layer from the index-level top-up. The records are removed from the bucket only, leaving
 * their index entries behind, which is the state a search sees between a commit's record delete and its index update.
 */
class Issue9692SparseNeighborsStaleTopUpTest extends TestHelper {
  private static final int COUNT = 60;

  @Override
  protected void beginTest() {
    for (final String type : new String[] { "Raw", "Rescored" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + type + ".pid INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
    }
    database.command("sql", "CREATE INDEX ON Raw (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16, rescoreOversample: 0 }");
    database.command("sql", "CREATE INDEX ON Rescored (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    database.transaction(() -> {
      for (final String type : new String[] { "Raw", "Rescored" })
        for (int i = 0; i < COUNT; i++)
          database.newDocument(type).set("pid", i, "tokens", new int[] { 0 }, "weights", new float[] { 1.0f - i * 0.01f }).save();
    });
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the test leaves index entries pointing at records it removed from the bucket, on purpose
    return false;
  }

  @Test
  void staleIndexEntriesAreToppedUpWithoutRescoring() {
    final List<RID> stale = removeBestRecordsFromBucketOnly("Raw", 4);

    // Precondition: the index still answers with the removed records, otherwise this test proves nothing.
    final List<RID> fromIndex = new ArrayList<>();
    for (final RidScore hit : index("Raw").topK(new int[] { 0 }, new float[] { 1.0f }, 10, null))
      fromIndex.add(hit.rid());
    assertThat(fromIndex).containsAll(stale);

    assertThat(sqlPids("Raw", 10)).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
  }

  @Test
  void aWholeWindowOfStaleEntriesIsToppedUpWithoutRescoring() {
    removeBestRecordsFromBucketOnly("Raw", 10);
    assertThat(sqlPids("Raw", 10)).containsExactly(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
  }

  @Test
  void staleIndexEntriesAreToppedUpWithTheDefaultRescoring() {
    removeBestRecordsFromBucketOnly("Rescored", 20);
    assertThat(sqlPids("Rescored", 10)).containsExactly(20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
  }

  @Test
  void anIndexHoldingFewerThanKStillAnswersWhatItHas() {
    removeBestRecordsFromBucketOnly("Raw", COUNT - 3);
    assertThat(sqlPids("Raw", 10)).containsExactly(57, 58, 59);
  }

  /**
   * The reporter's shape, end to end: every record matches the query and at least 1,900 are live at every instant, while
   * a writer replaces 100 of them per commit. Both the index ({@code topK}, default rescoring) and the SQL function
   * must answer a full {@code k} on every probe.
   */
  @Test
  void concurrentDeletesNeverShortenTheAnswer() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Live BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Live.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Live.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Live.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Live (pid) UNIQUE_HASH");
    database.command("sql", "CREATE INDEX ON Live (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("Live").set("pid", i, "tokens", new int[] { 0, 1 }, "weights", new float[] { 1.0f, 1.0f }).save();
    });
    final LSMSparseVectorIndex index = index("Live");

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong probes = new AtomicLong();
    final AtomicLong shortAnswers = new AtomicLong();
    final AtomicReference<Throwable> readerError = new AtomicReference<>();
    final Thread reader = new Thread(() -> {
      try {
        int step = 0;
        while (!stop.get()) {
          final int size = (step++ & 1) == 0 ?
              index.topK(new int[] { 0 }, new float[] { 1.0f }, 10, null).size() :
              sqlPids("Live", 10).size();
          if (size < 10)
            shortAnswers.incrementAndGet();
          probes.incrementAndGet();
        }
      } catch (final Throwable t) {
        readerError.set(t);
      }
    });
    reader.setDaemon(true);
    reader.start();
    try {
      int nextPid = 2_000;
      for (int round = 0; round < 60; round++) {
        final int first = round * 100;
        database.transaction(() -> {
          database.command("sql", "DELETE FROM Live WHERE pid >= ? AND pid < ?", first, first + 100).close();
        });
        final int from = nextPid;
        database.transaction(() -> {
          for (int i = from; i < from + 100; i++)
            database.newDocument("Live").set("pid", i, "tokens", new int[] { 0, 1 }, "weights", new float[] { 1.0f, 1.0f }).save();
        });
        nextPid += 100;
      }
    } finally {
      stop.set(true);
      reader.join();
    }

    assertThat(readerError.get()).isNull();
    assertThat(probes.get()).isPositive();
    assertThat(shortAnswers.get()).as("probes answering fewer than k out of " + probes.get()).isZero();
  }

  private List<RID> removeBestRecordsFromBucketOnly(final String type, final int count) {
    final List<RID> stale = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + type + " WHERE pid < " + count)) {
      rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
    }
    assertThat(stale).hasSize(count);
    database.transaction(() -> {
      for (final RID rid : stale)
        ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
    });
    return stale;
  }

  private List<Integer> sqlPids(final String type, final int k) {
    final List<Integer> pids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT pid FROM (SELECT expand(`vector.sparseNeighbors`('" + type + "[tokens,weights]', [0], [1.0], " + k + ")))")) {
      rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
    }
    return pids;
  }

  private LSMSparseVectorIndex index(final String type) {
    return (LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[tokens,weights]")).getIndexesOnBuckets()[0];
  }
}
