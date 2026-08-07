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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An LSM_VECTOR data file only ever grows: an update appends a new vector plus a tombstone for the one it
 * supersedes, a delete appends a tombstone, and nothing reclaims either. Compaction is a rebuild that also rewrites
 * the file (issue #5516 follow-up), and this is the test that it happens on its own - an index left to run must not
 * need an operator typing COMPACT INDEX to stop growing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class LSMVectorIndexAutoCompactionTest extends TestHelper {

  private static final int DIMENSIONS = 384;
  private static final int VERTICES   = 400;
  private static final int CYCLES     = 12;
  // A small page fills after ~39 vectors, so 400 live vectors already occupy more pages than
  // indexCompactionMinPagesSchedule ignores. Every assertion below then turns on the garbage ratio, which is what
  // this test is about, instead of passing because the file was too small to bother with.
  private static final int PAGE_SIZE  = 16 * 1024;

  @Test
  void anUpdateHeavyIndexCompactsItselfWithoutAnExplicitCommand() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final int fileIdBefore = index.getFileId();

    // Re-embed everything a few times: each cycle appends a vector and a tombstone per vertex, so the file grows
    // well past what the live set needs and the automatic trigger has to fire.
    int pagesPeak = index.getTotalPages();
    for (int cycle = 1; cycle <= CYCLES && index.getFileId() == fileIdBefore; cycle++) {
      updateAllVertices(cycle);
      pagesPeak = Math.max(pagesPeak, index.getTotalPages());
      awaitCompactionIdle();
    }

    assertThat(pagesPeak).as("the workload must first grow the file well past the live set, or nothing is tested")
        .isGreaterThan(3 * index.estimatePagesForLiveSetForTest());
    assertThat(index.getFileId())
        .as("the index must have compacted itself: the data file should have been swapped (peak was %d pages)",
            pagesPeak)
        .isNotEqualTo(fileIdBefore);

    final LSMVectorIndex compacted = vectorIndex();
    assertThat(compacted.getTotalPages()).as("the compacted file holds only the live vectors").isLessThan(pagesPeak);
    assertThat(compacted.countEntries()).as("no vector lost to the automatic compaction").isEqualTo(VERTICES);
    assertNeighborIsSelf(VERTICES / 2);

    reopenDatabase();
    assertThat(vectorIndex().countEntries()).as("and it survives a reopen").isEqualTo(VERTICES);
  }

  @Test
  void anIndexThatIsNotBloatedIsLeftAlone() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final int fileIdBefore = index.getFileId();

    // Inserts alone leave no garbage behind: every entry is live, so there is nothing to reclaim and rewriting the
    // file would be pure cost.
    for (int i = VERTICES; i < VERTICES * 2; i++)
      insertVertex(i);
    awaitCompactionIdle();

    assertThat(index.getFileId()).as("an index with no garbage must not be rewritten").isEqualTo(fileIdBefore);
    assertThat(index.countEntries()).isEqualTo(VERTICES * 2L);
  }

  @Test
  void automaticCompactionCanBeTurnedOff() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    try {
      createSchema();
      insertVertices();

      final LSMVectorIndex index = vectorIndex();
      final int fileIdBefore = index.getFileId();

      for (int cycle = 1; cycle <= CYCLES; cycle++) {
        updateAllVertices(cycle);
        awaitCompactionIdle();
      }

      assertThat(index.getFileId()).as("with the scheduler disabled nothing may compact on its own")
          .isEqualTo(fileIdBefore);
      assertThat(index.countEntries()).isEqualTo(VERTICES);
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    }
  }

  /**
   * The point of the ratio is that a compaction pays for itself, so a steady churn workload must reach a steady
   * state: reclaim, then run for a while on the compacted file before reclaiming again. A trigger that fired on
   * every commit - which is what an under-counted live set would produce - would rewrite the whole file, graph
   * included, on each cycle and cost far more than the space it returns.
   */
  @Test
  void aChurningIndexReachesASteadyStateInsteadOfRewritingEveryCycle() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    int lastFileId = index.getFileId();
    int compactions = 0;

    for (int cycle = 1; cycle <= CYCLES; cycle++) {
      updateAllVertices(cycle);
      awaitCompactionIdle();
      final int fileId = vectorIndex().getFileId();
      if (fileId != lastFileId) {
        compactions++;
        lastFileId = fileId;
      }
    }

    assertThat(compactions).as("the workload must reclaim at least once over %d cycles", CYCLES).isGreaterThan(0);
    assertThat(compactions)
        .as("%d cycles produced %d compactions: a rewrite per cycle means the trigger is not measuring garbage",
            CYCLES, compactions)
        .isLessThan(CYCLES / 2);
    assertThat(vectorIndex().countEntries()).isEqualTo(VERTICES);
  }

  /**
   * The ratio rests on {@code VectorLocationIndex.size()} being the live-vector count, which holds only while the map
   * does not evict. #5568 stopped honouring {@code locationCacheSize} and #5559 deleted the bounded backend outright,
   * because an evicted location cannot be recovered - so a configured cap has nowhere to take effect. This pins that:
   * with a cap set, the map still holds every live location and the index reclaims like any other.
   */
  @Test
  void theRatioOnlyTrustsALocationMapThatDoesNotEvict() {
    GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE.setValue(VERTICES / 10);
    try {
      createSchema();
      insertVertices();

      final LSMVectorIndex index = vectorIndex();
      assertThat(index.getVectorIndex().size())
          .as("#5559: a configured cap has no backend to reach, so the live count the estimate reads is whole")
          .isEqualTo(VERTICES);

      // The index is therefore an ordinary one and reclaims under churn, cap configured or not.
      final int fileIdBefore = index.getFileId();
      for (int cycle = 1; cycle <= CYCLES && vectorIndex().getFileId() == fileIdBefore; cycle++) {
        updateAllVertices(cycle);
        awaitCompactionIdle();
      }
      assertThat(vectorIndex().getFileId()).as("an unbounded map means the ratio is computable, so it compacts")
          .isNotEqualTo(fileIdBefore);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE.reset();
    }
  }

  /**
   * A compaction that gives up before it starts - a backup has suspended page flushing - must hand its scheduling
   * slot back. scheduleCompaction() only moves AVAILABLE -> COMPACTION_SCHEDULED, so a slot left reserved disables
   * every later compaction of that index, the explicit COMPACT INDEX included, until the database is reopened.
   */
  @Test
  void acompactionThatGivesUpBeforeStartingReleasesItsSlot() throws Exception {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();

    // Reserve the slot the way a commit does, then let the attempt land inside a backup's flush suspension.
    assertThat(index.scheduleCompaction()).as("the slot is free to begin with").isTrue();
    ((DatabaseInternal) database).getPageManager()
        .suspendFlushAndExecute(database, () -> assertThat(index.compact())
            .as("a suspended flush postpones the compaction").isFalse());

    assertThat(index.scheduleCompaction())
        .as("the postponed attempt must have released the slot, or nothing can ever compact this index again")
        .isTrue();
  }

  /**
   * The slot is reserved by DatabaseAsyncExecutorImpl.compact() BEFORE the task is handed to a worker, and only
   * the compaction running gives it back - so an attempt whose task never reaches a worker has to release it there
   * instead. A shut-down executor is the reachable version of that in a test; a full worker queue answers false
   * from the same call and takes the same path out.
   */
  @Test
  void anAttemptWhoseTaskIsNeverEnqueuedReleasesItsSlot() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    database.async().waitCompletion(30_000);
    ((DatabaseAsyncExecutorImpl) database.async()).close();

    // Nothing can run the compaction now. The attempt reports the shutdown to its caller - onAfterCommit is what
    // turns that into a log line rather than a failed commit - but it must not walk away still holding the index.
    assertThatThrownBy(() -> ((DatabaseAsyncExecutorImpl) database.async()).compact(index))
        .isInstanceOf(DatabaseOperationException.class);

    assertThat(index.scheduleCompaction())
        .as("a compaction that was never enqueued must leave the index schedulable")
        .isTrue();
  }

  /**
   * NONE quantization keeps the vector in the document, so a page entry is just its header - by far the smallest
   * entry the size estimate has to deal with, and the branch of it with the least obvious arithmetic.
   */
  @Test
  void anIndexWithoutQuantizationAlsoCompactsItself() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(3);
    try {
      createSchema(VectorQuantizationType.NONE);
      insertVertices();

      final LSMVectorIndex index = vectorIndex();
      final int fileIdBefore = index.getFileId();

      for (int cycle = 1; cycle <= 30 && index.getFileId() == fileIdBefore; cycle++) {
        updateAllVertices(cycle);
        awaitCompactionIdle();
      }

      assertThat(index.getFileId()).as("header-only entries must be sized correctly too").isNotEqualTo(fileIdBefore);
      assertThat(vectorIndex().countEntries()).isEqualTo(VERTICES);
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    }
  }

  /** Deletes leave the same tombstones an update does, so a delete-heavy index has to reclaim as well. */
  @Test
  void aDeleteHeavyIndexCompactsItself() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final int fileIdBefore = index.getFileId();

    final int deleted = VERTICES * 3 / 4;
    database.transaction(() -> {
      for (int i = 0; i < deleted; i++)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + i);
    });
    awaitCompactionIdle();

    assertThat(index.getFileId()).as("three quarters of the file is now garbage and must be reclaimed")
        .isNotEqualTo(fileIdBefore);
    assertThat(vectorIndex().countEntries()).as("the survivors are all still there")
        .isEqualTo(VERTICES - deleted);
  }

  /**
   * Compaction is leader-only under HA: a follower gets the rewritten file from the leader, and its own attempt is
   * declined by runWithCompactionReplication - but only after the task has been queued, run and reset. A follower
   * taking writes is precisely the node whose garbage ratio keeps crossing the threshold, so every commit would
   * schedule a task that does nothing. The standalone case is the one every other test in this class exercises: they
   * all compact, which they could not do if this gate answered the wrong way for a database that is not replicated.
   */
  @Test
  void aFollowerDoesNotScheduleCompactionsTheLeaderWillSendItAnyway() {
    final DatabaseInternal db = (DatabaseInternal) database;

    assertThat(LSMVectorIndex.isCompactionAllowedOnThisNode(db)).as("a standalone database compacts its own indexes")
        .isTrue();
    assertThat(LSMVectorIndex.isCompactionAllowedOnThisNode(nodeView(db, true))).as("so does the leader of a cluster")
        .isTrue();
    assertThat(LSMVectorIndex.isCompactionAllowedOnThisNode(nodeView(db, false)))
        .as("a follower must not: the compacted file arrives from the leader").isFalse();
  }

  /**
   * The ratio compares the file against the pages its live vectors need, so both sides have to cover the same files.
   * They would not if a compacted companion were ever loaded: its vectors join the same resident location map (so the
   * live-set estimate counts them) while {@code getTotalPages()} reports the mutable alone, and the file would be
   * allowed to grow well past the configured factor. {@code totalPagesForBloatRatio()} adds the companion in.
   * <p>
   * That guard is defensive, and this test is what says so: a companion cannot actually be loaded. Its extension has
   * never been in {@code LocalDatabase.SUPPORTED_FILE_EXT} - not since the index landed in #2816 - so the FileManager
   * does not open one, and the discovery scan that looks for it finds nothing. Dropping a faithfully named legacy
   * companion next to the mutable file proves it. If that extension is ever registered, this test fails and the ratio
   * accounting is the thing to look at first.
   */
  @Test
  void aLegacyCompactedCompanionOnDiskIsNotLoadedIntoTheRatio() throws Exception {
    createSchema();
    insertVertices();

    final String databasePath = database.getDatabasePath();
    database.close();

    // Named the way the pre-#5521 compactor named it: <mutable component>_<timestamp>, which is exactly the pattern
    // discoverAndLoadCompactedSubIndex() scans for, so a miss here is the FileManager not offering the file at all.
    final File directory = new File(databasePath);
    final String mutableFile = List.of(directory.list()).stream().filter(f -> f.endsWith("." + LSMVectorIndex.FILE_EXT))
        .findFirst().orElseThrow();
    final String companion = mutableFile.substring(0, mutableFile.indexOf('.')) + "_9999999999.1.65536.v0."
        + LSMVectorIndexCompacted.FILE_EXT;
    Files.copy(Path.of(databasePath, mutableFile), Path.of(databasePath, companion));

    database = factory.open();

    assertThat(((DatabaseInternal) database).getFileManager().getFiles().stream()
        .anyMatch(f -> f != null && LSMVectorIndexCompacted.FILE_EXT.equals(f.getFileExtension())))
        .as("the companion extension is not registered, so the file is never opened").isFalse();
    assertThat(vectorIndex().getStats().get("compactedPages"))
        .as("with no companion loaded the ratio sees the mutable file only").isEqualTo(0L);
    assertThat(new File(databasePath, companion)).as("and the file itself is left alone on disk").exists();
  }

  // ------------------------------------------------------------------------------------------------- helpers

  /**
   * A view of the database that only differs in being replicated, and in whether this node leads. Avoids pulling a
   * mocking framework into the engine module just to flip two flags (same approach as Issue5470ReplicatedCommitEveryTest).
   */
  private static DatabaseInternal nodeView(final DatabaseInternal delegate, final boolean leader) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if (args == null || args.length == 0)
            switch (method.getName()) {
            case "isReplicated" -> {
              return Boolean.TRUE;
            }
            case "isLeader" -> {
              return leader;
            }
            }
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  /** Compaction runs on the async executor: let anything already scheduled finish before looking at the file. */
  private void awaitCompactionIdle() {
    database.async().waitCompletion(30_000);
  }

  private void createSchema() {
    createSchema(VectorQuantizationType.INT8);
  }

  private void createSchema(final VectorQuantizationType quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType().withPageSize(PAGE_SIZE);
      builder.withDimensions(DIMENSIONS).withQuantization(quantization).create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
    });
  }

  private void insertVertex(final int i) {
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i,
        embedding(i, 0)));
  }

  private void updateAllVertices(final int cycle) {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(i, cycle), "doc" + i);
    });
  }

  private static float[] embedding(final int vertex, final int cycle) {
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private void assertNeighborIsSelf(final int vertex) {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1",
          (Object) embedding(vertex, 12));
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).isNotEmpty();
      assertThat(neighbors.getFirst().get("id")).isEqualTo("doc" + vertex);
    });
  }
}
