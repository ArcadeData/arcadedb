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
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.RidScore;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the vector SQL functions, one nested class per fixed issue.
 */
class VectorNeighborsStaleTopUpTest {
  /**
   * Issue #9368: vectorNeighbors asked the index for exactly {@code limit} candidates and skipped the ones whose record no longer
   * exists, so each stale index entry (a record a concurrent commit has just deleted) cost the caller a row.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NeighborsStaleTopUp extends TestHelper {
    private static final int COUNT = 60;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          database.newDocument("Doc").set("pid", i, "vector", new float[] { i, 0f }).save();
      });
    }

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // the test leaves index entries pointing at records it removed from the bucket, on purpose
      return false;
    }

    @Test
    void staleIndexEntriesAreToppedUp() {
      // Remove the 4 nearest records from the bucket only, leaving their index entries behind, as a concurrent commit does.
      final List<RID> stale = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE pid < 4")) {
        rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
      }
      assertThat(stale).hasSize(4);

      // Build the graph first: a lazy build validates every record and would drop the ones removed below.
      final LSMVectorIndex lsm = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
      assertThat(lsm.findNeighborsFromVector(new float[] { 0f, 0f }, 10)).hasSize(10);

      database.transaction(() -> {
        for (final RID rid : stale)
          ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
      });

      // Precondition: the index still answers with the removed records, otherwise this test proves nothing.
      final List<RID> fromIndex = new ArrayList<>();
      lsm.findNeighborsFromVector(new float[] { 0f, 0f }, 10).forEach(p -> fromIndex.add(p.getFirst()));
      assertThat(fromIndex).containsAll(stale);

      // Since issue #10973 a row is made without reading its record, and asks the index whether the record is still there: a
      // delete that commits takes the vector out of the index in the same commit, which a removal from the bucket alone does not
      // simulate (VectorNeighborsPrefilterTest.VectorNeighborsDeferredRecordRead pins what such a row answers). A read listener keeps the
      // records read as the rows are made, which is the path whose stale entries are found by the read and topped up
      final AfterRecordReadListener listener = record -> record;
      database.getSchema().getType("Doc").getEvents().registerListener(listener);
      final List<Integer> pids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT pid FROM (SELECT expand(vectorNeighbors('Doc[vector]', :v, 10)))",
          Map.of("v", new float[] { 0f, 0f }))) {
        rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
      } finally {
        database.getSchema().getType("Doc").getEvents().unregisterListener(listener);
      }
      assertThat(pids).hasSize(10).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
    }
  }

  /**
   * Issue #9692 (from #9635): {@code vector.sparseNeighbors} asked each sub-index for exactly {@code k} candidates and
   * skipped the ones whose record no longer resolved, so each record a concurrent commit had just deleted cost the caller
   * a result row. The dense function got the widen-and-retry for the same shape in #9368; this is its sparse twin.
   * <p>
   * Exact rescoring is turned off ({@code rescoreOversample: 0}) so the index hands the stale RIDs over as they are,
   * which isolates the SQL layer from the index-level top-up. The records are removed from the bucket only, leaving
   * their index entries behind, which is the state a search sees between a commit's record delete and its index update.
   */
  @Nested
  class SparseNeighborsStaleTopUp extends TestHelper {
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
  @Nested
  class GroupedNeighborsStaleTopUp extends TestHelper {
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

  /**
   * Issue #9697: the termination rules of {@link SQLFunctionVectorAbstract#searchGroupedWithStaleTopUp}, driven by a
   * scripted pass so each rule is pinned without a database or a concurrent writer. Issue #10002 added the rule that a
   * wider pass which did not grow keeps widening while it meets stale candidates no earlier pass met.
   */
  @Nested
  class GroupedStaleTopUpLoop {

    /** One scripted pass: the rows it answers and the stale candidates it reports, by position in bucket 1. */
    private record Step(int rows, int... stale) {
    }

    /** Replays the steps in order and records the budgets each pass was asked for. */
    private static class ScriptedPass implements SQLFunctionVectorAbstract.GroupedSearchPass {
      private final Step[]      steps;
      final         List<int[]> asked = new ArrayList<>();

      private ScriptedPass(final Step... steps) {
        this.steps = steps;
      }

      @Override
      public ArrayList<Object> run(final int fetchGroups, final int fetchGroupSize, final Set<RID> staleRids) {
        final Step step = steps[asked.size()];
        asked.add(new int[] { fetchGroups, fetchGroupSize });
        for (final int position : step.stale())
          staleRids.add(new RID(1, position));
        final ArrayList<Object> rows = new ArrayList<>(step.rows());
        for (int i = 0; i < step.rows(); i++)
          rows.add("pass" + asked.size() + "-row" + i);
        return rows;
      }
    }

    @Test
    void aFullFirstAnswerRunsOnePass() {
      final ScriptedPass pass = new ScriptedPass(new Step(6, 0));
      assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(6);
      assertThat(pass.asked).hasSize(1);
    }

    @Test
    void aShortAnswerWithoutStaleRowsRunsOnePass() {
      final ScriptedPass pass = new ScriptedPass(new Step(4));
      assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(4);
      assertThat(pass.asked).hasSize(1);
    }

    @Test
    void eachPassWidensBothBudgetsByTheStaleCount() {
      final ScriptedPass pass = new ScriptedPass(new Step(3, 0, 1), new Step(5, 0, 1, 2), new Step(6, 0, 1, 2));
      assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(6);
      assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 8, 7 });
    }

    @Test
    void aPassThatDoesNotGrowAndMeetsNoNewStaleCandidateStopsOnTheFresherAnswer() {
      final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(4, 0, 1));
      final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
      assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass2-"));
      assertThat(pass.asked).hasSize(2);
    }

    @Test
    void aPassThatShrinksAndMeetsNoNewStaleCandidateStopsOnTheLargerEarlierAnswer() {
      final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(3, 0, 1));
      final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
      assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass1-"));
      assertThat(pass.asked).hasSize(2);
    }

    /**
     * Issue #10002: under churn a wider pass meets the records deleted while it ran, and those can leave it no larger than
     * the pass before. That is not a corpus with nothing more to give, so the loop widens again - and since it is racing a
     * writer, it at least doubles both budgets: (5, 4) becomes (5 + 5, 4 + 4).
     */
    @Test
    void aPassThatDoesNotGrowButMeetsNewStaleCandidatesWidensAgain() {
      final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(4, 2, 3, 4), new Step(6));
      final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
      assertThat(answer).hasSize(6).allMatch(row -> row.toString().startsWith("pass3-"));
      assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 10, 8 });
    }

    /** A shrinking pass that met new stale candidates widens again too, and the larger answer wins if nothing better comes. */
    @Test
    void aShrinkingPassThatMeetsNewStaleCandidatesWidensAgainAndKeepsTheLargerAnswer() {
      final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(3, 2), new Step(3, 2));
      final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
      assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass1-"));
      assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 10, 8 });
    }

    /**
     * Issue #10002: a writer that keeps deleting what the searches rank hands every pass stale candidates no earlier pass met,
     * and never a full answer. The loop keeps widening, doubling once a pass stops growing, and the candidate budget is what
     * ends it - after a number of passes that grows with the log of the budget, not with the budget.
     */
    @Test
    void anEndlessStreamOfFreshStaleCandidatesStopsAtTheBudget() {
      final ScriptedPass pass = new ScriptedPass() {
        @Override
        public ArrayList<Object> run(final int fetchGroups, final int fetchGroupSize, final Set<RID> staleRids) {
          final int passNumber = asked.size();
          asked.add(new int[] { fetchGroups, fetchGroupSize });
          for (int i = 0; i < 3; i++)
            staleRids.add(new RID(1, passNumber * 3L + i));
          final ArrayList<Object> rows = new ArrayList<>();
          for (int i = 0; i < 4; i++)
            rows.add("pass" + asked.size() + "-row" + i);
          return rows;
        }
      };
      // (3,2) grows from nothing and widens by 3 to (6,5); from there no pass grows, so each doubles: (12,10) costs 600, the
      // next, (24,20), would cost 2,400 > 1,000
      List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 1_000, pass);
      assertThat(answer).hasSize(4);
      assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 6, 5 }, new int[] { 12, 10 });

      // Against the default budget the race costs a handful of passes, not one per stale candidate
      pass.asked.clear();
      answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(5, 3, 100_000, pass);
      assertThat(answer).hasSize(4);
      assertThat(pass.asked.size()).isLessThanOrEqualTo(8);
    }

    @Test
    void aWiderBudgetPastTheCapIsNeverAsked() {
      // groups=3, groupSize=2 costs 30 candidates; widening by 1 would cost 4 * 3 * 5 = 60, past a cap of 50.
      final ScriptedPass pass = new ScriptedPass(new Step(4, 0));
      assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 50, pass)).hasSize(4);
      assertThat(pass.asked).hasSize(1);
    }
  }
}
