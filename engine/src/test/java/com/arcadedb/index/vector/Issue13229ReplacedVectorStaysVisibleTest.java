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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A record whose vector a commit replaces stays in the index before and after the commit, so a search that runs while the commit
 * replays must find it with the old vector or the new one, never with neither (issue #13229).
 * <p>
 * A commit replays the REMOVE of the old vector and the ADD of the new one in two separate sections under the index write lock, and
 * the rows of a search are checked with {@link LSMVectorIndex#hasLiveVector} after the search released its read lock, a read that
 * takes no lock. A search landing between the two sections, or that check landing in the middle of the replay, dropped the record.
 * The test updates one record in a loop on one thread and searches for it on another: by a filter that names only that record
 * (k=1, the answer is one row or the record is missing) and by an exhaustive search.
 */
class Issue13229ReplacedVectorStaysVisibleTest {
  private static final String DB_PATH    = "target/test-databases/Issue13229ReplacedVectorStaysVisibleTest";
  private static final int    DIMENSIONS = 16;
  /** Above the 1,000 vectors under which a search rebuilds the graph on its own thread. */
  private static final int    RECORDS    = 1200;
  /** Rounds of the reader: each is a filtered search and an exhaustive one, while the writer commits without pause. */
  private static final int    ROUNDS     = 400;
  /**
   * Commits the writer must have made before the reader stops, whatever the number of rounds: on a loaded host the writer can be
   * scheduled less than the reader, and the rounds have to overlap commits for the test to say anything.
   */
  private static final int    MIN_COMMITS = ROUNDS / 4;

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
  void aRecordWhoseVectorIsReplacedIsNeverMissingFromASearch() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try (final Database db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().buildVertexType().withName("Chunk").withTotalBuckets(1).create().createProperty("embedding", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\", \"maxConnections\": 16, \"beamWidth\": 80"
            // No background rebuild: the test is about the two steps of a commit's replay, and a rebuild publishing a graph while the
            // record is replaced is another interleaving, one a loaded CI host met once in 400 searches
            + ", \"mutationsBeforeRebuild\": 1000000000, \"inactivityRebuildTimeoutMs\": 0}");
      });
      final RID[] target = new RID[1];
      for (int from = 0; from < RECORDS; from += 400) {
        final int first = from;
        db.transaction(() -> {
          for (int i = first; i < Math.min(RECORDS, first + 400); i++) {
            final RID rid = db.newVertex("Chunk").set("embedding", vector(i)).save().getIdentity();
            if (i == 0)
              target[0] = rid;
          }
        });
      }
      final TypeIndex typeIndex = db.getSchema().getType("Chunk").getPolymorphicIndexByProperties("embedding");
      for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
        ((LSMVectorIndex) bucketIndex).buildVectorGraphNow();

      final AtomicBoolean done = new AtomicBoolean();
      final AtomicInteger commits = new AtomicInteger();
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          for (int update = 0; !done.get(); update++) {
            final float[] replacement = vector(1_000_000 + update);
            db.transaction(() -> {
              final MutableDocument document = db.lookupByRID(target[0], true).asDocument().modify();
              document.set("embedding", replacement);
              document.save();
            });
            commits.incrementAndGet();
          }
        } catch (final Throwable t) {
          writerFailure.set(t);
          done.set(true);
        }
      }, "writer");
      writer.start();

      final String rid = target[0].toString();
      final Random random = new Random(13229);
      int filtered = 0, filteredMissing = 0, exhaustive = 0, exhaustiveMissing = 0;
      for (int round = 0; (round < ROUNDS || commits.get() < MIN_COMMITS) && !done.get(); round++) {
        final float[] query = vector(random.nextInt(100_000));
        int rows = 0;
        try (final ResultSet resultSet = db.query("sql", "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('Chunk[embedding]', :query, 1, {filter: :rids})))",
            Map.of("query", query, "rids", List.of(rid)))) {
          while (resultSet.hasNext()) {
            resultSet.next();
            rows++;
          }
        }
        ++filtered;
        if (rows != 1)
          ++filteredMissing;

        boolean found = false;
        try (final ResultSet resultSet = db.query("sql", "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('Chunk[embedding]', :query, :k)))",
            Map.of("query", query, "k", RECORDS + 5))) {
          while (resultSet.hasNext())
            if (rid.equals(resultSet.next().getProperty("rid").toString()))
              found = true;
        }
        ++exhaustive;
        if (!found)
          ++exhaustiveMissing;
      }
      done.set(true);
      writer.join();

      assertThat(writerFailure.get()).as("the writer").isNull();
      assertThat(filtered).as("rounds that ran").isGreaterThanOrEqualTo(ROUNDS);
      assertThat(commits.get()).as("commits that replaced the vector while the rounds ran").isGreaterThanOrEqualTo(MIN_COMMITS);
      assertThat(filteredMissing).as("of " + filtered + " searches by a filter naming only the record, the ones that found no row").isZero();
      assertThat(exhaustiveMissing).as("of " + exhaustive + " exhaustive searches, the ones that did not find the record").isZero();
    }
  }

  private static float[] vector(final long seed) {
    final Random random = new Random(seed);
    final float[] vector = new float[DIMENSIONS];
    double norm = 0;
    for (int i = 0; i < DIMENSIONS; i++) {
      vector[i] = (float) random.nextGaussian();
      norm += (double) vector[i] * vector[i];
    }
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = (float) (vector[i] / Math.sqrt(norm));
    return vector;
  }
}
