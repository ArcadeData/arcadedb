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
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9697 (from #9692): {@code vector.neighbors} (#9368) and {@code vector.sparseNeighbors} (#9692) topped up a
 * short answer when a candidate's record had been deleted by an overlapping commit, but only without {@code groupBy}.
 * The grouped emit skipped the record and returned, so a stale entry could leave a group one member short or cost a
 * whole group its place.
 * <p>
 * Records {@code 2g} and {@code 2g + 1} form group {@code g}, ordered nearest-first, so {@code k = 3, groupSize = 2}
 * answers the first three groups. Every case runs over one bucket and over two. The records are removed from the
 * bucket only, leaving their index entries behind, which is the state a search sees between a commit's record delete
 * and its index update. The sparse index runs with exact rescoring off ({@code rescoreOversample: 0}) so it hands the
 * stale RIDs over as they are, which isolates the SQL layer from the index-level top-up of #9692.
 */
class Issue9697GroupedNeighborsStaleTopUpTest extends TestHelper {
  private static final int COUNT = 60;

  @Override
  protected void beginTest() {
    // One bucket answers from a single index; two route every query through the multi-index merge and the #8002
    // second phase, which the top-up has to widen as well. Insertion alternates buckets, so the two members of a group
    // land in different ones.
    for (final int buckets : new int[] { 1, 2 }) {
      final String dense = "Dense" + buckets;
      database.command("sql", "CREATE DOCUMENT TYPE " + dense + " BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY " + dense + ".pid INTEGER");
      database.command("sql", "CREATE PROPERTY " + dense + ".grp INTEGER");
      database.command("sql", "CREATE PROPERTY " + dense + ".vector ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON " + dense + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");

      final String sparse = "Sparse" + buckets;
      database.command("sql", "CREATE DOCUMENT TYPE " + sparse + " BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY " + sparse + ".pid INTEGER");
      database.command("sql", "CREATE PROPERTY " + sparse + ".grp INTEGER");
      database.command("sql", "CREATE PROPERTY " + sparse + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + sparse + ".weights ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON " + sparse + " (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16, rescoreOversample: 0 }");

      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++) {
          database.newDocument(dense).set("pid", i, "grp", i / 2, "vector", new float[] { i, 0f }).save();
          database.newDocument(sparse).set("pid", i, "grp", i / 2, "tokens", new int[] { 0 }, "weights",
              new float[] { 1.0f - i * 0.01f }).save();
        }
      });

      // Build every dense graph now: a lazy build validates every record and would drop the ones a test removes below.
      for (final var sub : ((TypeIndex) database.getSchema().getIndexByName(dense + "[vector]")).getIndexesOnBuckets())
        assertThat(((LSMVectorIndex) sub).findNeighborsFromVector(new float[] { 0f, 0f }, 10)).isNotEmpty();
    }
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the test leaves index entries pointing at records it removed from the bucket, on purpose
    return false;
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void denseGroupWhoseNearestMemberIsStaleKeepsItsPlace(final int buckets) {
    removeFromBucketOnly("Dense" + buckets, 1);
    assertThat(densePids(buckets, 3, 2, null)).containsExactly(1, 2, 3, 4, 5);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void denseWholeStaleGroupsAreReplacedByTheNextOnes(final int buckets) {
    removeFromBucketOnly("Dense" + buckets, 4);
    assertThat(densePids(buckets, 3, 2, null)).containsExactly(4, 5, 6, 7, 8, 9);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void denseShortCorpusStillAnswersWhatItHas(final int buckets) {
    removeFromBucketOnly("Dense" + buckets, COUNT - 4);
    assertThat(densePids(buckets, 3, 2, null)).containsExactly(56, 57, 58, 59);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void denseAllowListWithStaleEntriesIsToppedUpWithinTheList(final int buckets) {
    final List<RID> allowed = rids("Dense" + buckets, 10);
    removeFromBucketOnly("Dense" + buckets, 1);
    assertThat(densePids(buckets, 3, 2, allowed)).containsExactly(1, 2, 3, 4, 5);
    // An allow-list that cannot fill the groups once its stale entry is gone answers what it has, and terminates.
    assertThat(densePids(buckets, 3, 2, allowed.subList(0, 4))).containsExactly(1, 2, 3);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void sparseGroupWhoseNearestMemberIsStaleKeepsItsPlace(final int buckets) {
    removeFromBucketOnly("Sparse" + buckets, 1);
    assertThat(sparsePids("Sparse" + buckets, 3, 2, null)).containsExactly(1, 2, 3, 4, 5);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void sparseWholeStaleGroupsAreReplacedByTheNextOnes(final int buckets) {
    removeFromBucketOnly("Sparse" + buckets, 4);
    assertThat(sparsePids("Sparse" + buckets, 3, 2, null)).containsExactly(4, 5, 6, 7, 8, 9);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void sparseShortCorpusStillAnswersWhatItHas(final int buckets) {
    removeFromBucketOnly("Sparse" + buckets, COUNT - 4);
    assertThat(sparsePids("Sparse" + buckets, 3, 2, null)).containsExactly(56, 57, 58, 59);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2 })
  void sparseAllowListWithStaleEntriesIsToppedUpWithinTheList(final int buckets) {
    final List<RID> allowed = rids("Sparse" + buckets, 10);
    removeFromBucketOnly("Sparse" + buckets, 1);
    assertThat(sparsePids("Sparse" + buckets, 3, 2, allowed)).containsExactly(1, 2, 3, 4, 5);
    assertThat(sparsePids("Sparse" + buckets, 3, 2, allowed.subList(0, 4))).containsExactly(1, 2, 3);
  }

  /**
   * End to end on the sparse function, whose answer is exact: five groups always hold far more than two live members,
   * while a writer replaces 100 records per commit. Every grouped probe must answer three full groups. Timing dependent:
   * a run can pass without a probe ever landing in the race, so the deterministic cases above carry the guarantee and
   * this one is the reporter's shape as a smoke test (it failed before the fix, 0.5 s).
   */
  @Test
  void sparseConcurrentDeletesNeverShortenTheGroupedAnswer() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Live BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Live.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Live.grp INTEGER");
    database.command("sql", "CREATE PROPERTY Live.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Live.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Live (pid) UNIQUE_HASH");
    database.command("sql", "CREATE INDEX ON Live (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("Live").set("pid", i, "grp", i % 5, "tokens", new int[] { 0, 1 }, "weights", new float[] { 1.0f, 1.0f })
            .save();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong probes = new AtomicLong();
    final AtomicLong shortAnswers = new AtomicLong();
    final AtomicReference<Throwable> readerError = new AtomicReference<>();
    final Thread reader = new Thread(() -> {
      try {
        while (!stop.get()) {
          if (sparsePids("Live", 3, 2, null).size() < 6)
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
            database.newDocument("Live").set("pid", i, "grp", i % 5, "tokens", new int[] { 0, 1 }, "weights",
                new float[] { 1.0f, 1.0f }).save();
        });
        nextPid += 100;
      }
    } finally {
      stop.set(true);
      reader.join();
    }

    assertThat(readerError.get()).isNull();
    assertThat(probes.get()).isPositive();
    assertThat(shortAnswers.get()).as("grouped probes answering fewer than 3 full groups out of " + probes.get()).isZero();
  }

  /** The RIDs of the {@code count} nearest records, ordered by pid. */
  private List<RID> rids(final String type, final int count) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + type + " WHERE pid < " + count + " ORDER BY pid")) {
      rs.forEachRemaining(r -> rids.add(r.getProperty("rid")));
    }
    assertThat(rids).hasSize(count);
    return rids;
  }

  private void removeFromBucketOnly(final String type, final int count) {
    final List<RID> stale = rids(type, count);
    database.transaction(() -> {
      for (final RID rid : stale)
        ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
    });
  }

  private List<Integer> densePids(final int buckets, final int k, final int groupSize, final List<RID> filter) {
    final List<Integer> pids = new ArrayList<>();
    final Map<String, Object> params = new HashMap<>();
    params.put("v", new float[] { 0f, 0f });
    params.put("filter", filter);
    try (final ResultSet rs = database.query("sql",
        "SELECT pid FROM (SELECT expand(`vector.neighbors`('Dense" + buckets + "[vector]', :v, " + k + ", { groupBy: 'grp', groupSize: "
            + groupSize + (filter != null ? ", filter: :filter" : "") + " })))", params)) {
      rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
    }
    return pids;
  }

  private List<Integer> sparsePids(final String type, final int k, final int groupSize, final List<RID> filter) {
    final List<Integer> pids = new ArrayList<>();
    final Map<String, Object> params = new HashMap<>();
    params.put("filter", filter);
    try (final ResultSet rs = database.query("sql",
        "SELECT pid FROM (SELECT expand(`vector.sparseNeighbors`('" + type + "[tokens,weights]', [0], [1.0], " + k
            + ", { groupBy: 'grp', groupSize: " + groupSize + (filter != null ? ", filter: :filter" : "") + " })))", params)) {
      rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
    }
    return pids;
  }
}
