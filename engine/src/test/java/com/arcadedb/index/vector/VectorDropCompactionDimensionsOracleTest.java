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
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Oracle tests for the life cycle of an LSM_VECTOR index: its drop, the compaction that rewrites its data file with the live
 * vectors, the gates that decide when a commit schedules one, and the rebuild in place that reconciles the index with its bucket.
 * <p>
 * The oracle is a model of the live set ({@code Map<RID, float[]>}), the exact cosine ranking of that set, and the state the index
 * leaves on disk: the files of the database directory, the file manager and the schema.
 */
@Tag("vector")
class VectorDropCompactionDimensionsOracleTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int PAGE_SIZE  = 16 * 1024;
  private static final int RECORDS    = 60;

  private boolean checkIntegrity = true;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return checkIntegrity;
  }

  // ---------------------------------------------------------------------------------------------------------- drop

  @ParameterizedTest
  @ValueSource(strings = { "DROP INDEX", "SCHEMA API", "DROP TYPE" })
  void dropRemovesEveryComponentOfTheIndexAndNothingOfItsSiblings(final String how) {
    createType("Doc", "embedding", "NONE");
    createIndex("Doc", "other", "INT8");
    createType("Keep", "embedding", "NONE");
    final Map<RID, float[]> docs = insert("Doc", RECORDS, 0);
    final Map<RID, float[]> kept = insert("Keep", RECORDS, 0);
    for (final String name : new String[] { "Doc[embedding]", "Doc[other]", "Keep[embedding]" })
      database.command("sql", "COMPACT INDEX `" + name + "`");

    final Set<String> droppedFiles = new HashSet<>(filesOf(lsm("Doc", "embedding")));
    final Set<Integer> droppedIds = new HashSet<>(lsm("Doc", "embedding").getFileIds());
    final Set<String> keptFiles = new HashSet<>(filesOf(lsm("Keep", "embedding")));
    final Set<Integer> keptIds = new HashSet<>(lsm("Keep", "embedding").getFileIds());
    final Set<String> siblingFiles = new HashSet<>(filesOf(lsm("Doc", "other")));
    final Set<Integer> siblingIds = new HashSet<>(lsm("Doc", "other").getFileIds());
    assertThat(droppedFiles).as("a mutable file, a graph file and the sidecars of the graph").hasSizeGreaterThan(2);
    for (final Integer id : droppedIds)
      assertThat(database.getSchema().getFileByIdIfExists(id)).isNotNull();
    if (how.equals("DROP TYPE")) {
      droppedFiles.addAll(siblingFiles);
      droppedIds.addAll(siblingIds);
    }

    switch (how) {
    case "DROP INDEX" -> database.command("sql", "DROP INDEX `Doc[embedding]`");
    case "SCHEMA API" -> database.getSchema().dropIndex("Doc[embedding]");
    default -> database.command("sql", "DROP TYPE Doc UNSAFE");
    }

    final Set<String> onDisk = new HashSet<>(Arrays.asList(new File(database.getDatabasePath()).list()));
    for (final String file : droppedFiles)
      assertThat(onDisk).as("%s leaves no file behind: %s", how, file).doesNotContain(file);
    for (final Integer id : droppedIds) {
      assertThat(((DatabaseInternal) database).getFileManager().existsFile(id)).as("file %d in the file manager", id).isFalse();
      assertThat(database.getSchema().getFileByIdIfExists(id)).as("file %d in the schema", id).isNull();
    }
    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
    assertThat(database.getSchema().existsIndex("Doc[other]")).isEqualTo(!how.equals("DROP TYPE"));

    // what is not the index's is untouched
    assertThat(onDisk).containsAll(keptFiles);
    for (final Integer id : keptIds)
      assertThat(((DatabaseInternal) database).getFileManager().existsFile(id)).isTrue();
    if (!how.equals("DROP TYPE")) {
      assertThat(onDisk).containsAll(siblingFiles);
      for (final Integer id : siblingIds)
        assertThat(((DatabaseInternal) database).getFileManager().existsFile(id)).isTrue();
      final Map<RID, float[]> others = new LinkedHashMap<>();
      int i = 0;
      for (final RID rid : docs.keySet())
        others.put(rid, vector(100_000 + i++));
      assertSelfLookups(lsm("Doc", "other"), others, true, 7);
    }
    assertExact(lsm("Keep", "embedding"), kept);

    reopenDatabase();
    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
    assertExact(lsm("Keep", "embedding"), kept);

    if (!how.equals("DROP TYPE")) {
      // an index of the same name starts from the records, with nothing of the dropped one
      createIndex("Doc", "embedding", "NONE");
      final LSMVectorIndex again = lsm("Doc", "embedding");
      assertThat(again.getStats().get("activeVectors")).isEqualTo((long) RECORDS);
      assertThat(again.getStats().get("deletedVectors")).as("no tombstone of the dropped index").isZero();
      assertThat(Arrays.asList(new File(database.getDatabasePath()).list())).as("fresh files")
          .noneMatch(droppedFiles::contains);
      assertExact(again, docs);
    }
  }

  @Test
  void dropOfAnIndexOfAClosedDatabaseDeletesItsFilesFromDisk() {
    createType("Doc", "embedding", "NONE");
    insert("Doc", RECORDS, 0);
    database.command("sql", "COMPACT INDEX `Doc[embedding]`");
    final LSMVectorIndex index = lsm("Doc", "embedding");
    final List<String> files = filesOf(index);
    assertThat(files).hasSizeGreaterThan(2);
    final String path = database.getDatabasePath();

    database.close();
    final Set<String> before = new HashSet<>(Arrays.asList(new File(path).list()));
    assertThat(before).containsAll(files);
    index.drop();

    final Set<String> after = new HashSet<>(Arrays.asList(new File(path).list()));
    for (final String file : files)
      assertThat(after).as("the dropped index leaves no file: %s", file).doesNotContain(file);
    assertThat(after).as("only the files of the index went").containsAll(before.stream().filter(f -> !files.contains(f)).toList());
    // the schema still lists the index it was told nothing about: open the files again for the teardown
    database = factory.open();
  }

  // ------------------------------------------------------------------------------------------------------ compaction

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void compactionRewritesTheDataFileWithTheLiveVectorsOnly(final String quantization) throws Exception {
    createType("Doc", "embedding", quantization);
    final Map<RID, float[]> live = insert("Doc", 300, 0);
    final Set<RID> deleted = new HashSet<>();
    final List<RID> order = new ArrayList<>(live.keySet());
    database.transaction(() -> {
      // re-embedded records: a vector and a tombstone more for each
      for (int i = 0; i < 100; i++) {
        final float[] updated = vector(5000 + i);
        order.get(i).asDocument().modify().set("embedding", updated).save();
        live.put(order.get(i), updated);
      }
      for (int i = 100; i < 160; i++) {
        order.get(i).asDocument().delete();
        live.remove(order.get(i));
        deleted.add(order.get(i));
      }
    });

    final LSMVectorIndex index = lsm("Doc", "embedding");
    final int fileBefore = index.getFileId();
    final String nameBefore = index.getName();
    final int pagesBefore = index.getTotalPages();
    final List<String> filesBefore = filesOf(index);
    assertThat(index.getStats().get("deletedVectors")).as("garbage to reclaim").isEqualTo(160L);
    assertThat(index.getStats().get("activeVectors")).isEqualTo((long) live.size());

    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).as("the file was rewritten").isTrue();

    assertThat(index.getFileId()).isNotEqualTo(fileBefore);
    assertThat(index.getName()).isNotEqualTo(nameBefore);
    assertThat(index.getTotalPages()).isLessThanOrEqualTo(pagesBefore);
    assertThat(index.getStats().get("activeVectors")).isEqualTo((long) live.size());
    assertThat(index.getStats().get("deletedVectors")).as("a compaction drops every tombstone").isZero();
    assertThat(index.countEntries()).isEqualTo(live.size());
    final Set<String> onDisk = new HashSet<>(Arrays.asList(new File(database.getDatabasePath()).list()));
    final String replaced = filesBefore.stream().filter(f -> f.endsWith("." + LSMVectorIndex.FILE_EXT)).findFirst().orElseThrow();
    assertThat(onDisk).as("the replaced data file is dropped from disk").doesNotContain(replaced);
    assertThat(((DatabaseInternal) database).getFileManager().existsFile(fileBefore)).as("and from the file manager").isFalse();
    assertThat(database.getSchema().getFileByIdIfExists(fileBefore)).as("and from the schema").isNull();
    assertThat(onDisk).anyMatch(f -> f.endsWith("." + LSMVectorIndex.FILE_EXT) && !f.equals(replaced));

    verify(index, live, deleted, quantization.equals("NONE"));
    reopenDatabase();
    verify(lsm("Doc", "embedding"), live, deleted, quantization.equals("NONE"));
    assertThat(lsm("Doc", "embedding").countEntries()).isEqualTo(live.size());
  }

  @Test
  void aCompactionThatCannotCopyTheEntriesRewritesNothingAndLosesNothing() throws Exception {
    createType("Doc", "embedding", "NONE");
    final Map<RID, float[]> live = insert("Doc", RECORDS, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    // 40 records lose their entries while they stay in the bucket: the index holds far fewer vectors than the bucket has records
    final List<RID> order = new ArrayList<>(live.keySet());
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        index.remove(new Object[] { live.get(order.get(i)) }, order.get(i));
    });
    assertThat(index.countEntries()).isEqualTo(RECORDS - 40);
    final int fileBefore = index.getFileId();
    final String nameBefore = index.getName();

    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).as("the recovered vectors are on no page, so no file can be rewritten from them").isFalse();

    assertThat(index.getFileId()).isEqualTo(fileBefore);
    assertThat(index.getName()).isEqualTo(nameBefore);
    assertThat(new HashSet<>(Arrays.asList(new File(database.getDatabasePath()).list()))).anyMatch(f -> f.startsWith(nameBefore + "."));
    // the records the build recovered from the bucket answer, with the ones the index held
    assertSelfLookups(index, live, false, RECORDS);
    assertThat(index.scheduleCompaction()).as("the slot is handed back").isTrue();
  }

  @Test
  void aCompactionThatTimesOutLockingTheFileIsRetriedLater() throws Exception {
    createType("Doc", "embedding", "NONE");
    final Map<RID, float[]> live = insert("Doc", RECORDS, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    final int fileBefore = index.getFileId();
    final Long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);
    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 50L);
    final LockHoldingThread holder = new LockHoldingThread((DatabaseInternal) database, List.of(fileBefore), 60_000);
    try {
      holder.start();
      assertThat(holder.lockAcquired.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

      assertThat(index.scheduleCompaction()).isTrue();
      assertThat(index.compact()).as("the commits of the index did not leave the file free").isFalse();
      assertThat(index.getFileId()).isEqualTo(fileBefore);
      assertThat(index.isCompacting()).isFalse();
    } finally {
      holder.release();
      holder.join(10_000);
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, timeout);
    }

    assertThat(index.scheduleCompaction()).as("the attempt handed its slot back").isTrue();
    assertThat(index.compact()).isTrue();
    assertThat(index.getFileId()).isNotEqualTo(fileBefore);
    assertExact(index, live);
  }

  @Test
  void theSchedulingSlotIsHandedOutOnceAndFollowsTheStatusTransitions() throws Exception {
    createType("Doc", "embedding", "NONE");
    final Map<RID, float[]> live = insert("Doc", RECORDS, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");

    assertThat(index.compact()).as("nothing is scheduled").isFalse();
    assertThat(index.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_SCHEDULED },
        IndexInternal.INDEX_STATUS.UNAVAILABLE)).as("the index is not in that status").isFalse();
    assertThat(index.setStatus(
        new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS, IndexInternal.INDEX_STATUS.AVAILABLE },
        IndexInternal.INDEX_STATUS.COMPACTION_SCHEDULED)).as("it is in the second one").isTrue();
    assertThat(index.scheduleCompaction()).as("already scheduled").isFalse();
    assertThat(index.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_SCHEDULED },
        IndexInternal.INDEX_STATUS.AVAILABLE)).isTrue();
    assertThat(index.setStatus(new IndexInternal.INDEX_STATUS[] {}, IndexInternal.INDEX_STATUS.UNAVAILABLE)).isFalse();

    // a backup suspended the page flushing: no slot is handed out, and a compaction that was scheduled gives up and hands it back
    final boolean[] answers = new boolean[3];
    ((DatabaseInternal) database).getPageManager().suspendFlushAndExecute(database, () -> {
      answers[0] = index.scheduleCompaction();
    });
    assertThat(answers[0]).as("no compaction is scheduled during a backup").isFalse();
    assertThat(index.scheduleCompaction()).as("and the refusal reserved nothing").isTrue();
    ((DatabaseInternal) database).getPageManager().suspendFlushAndExecute(database, () -> {
      answers[1] = index.compact();
    });
    assertThat(answers[1]).as("the compaction is postponed").isFalse();
    assertThat(index.scheduleCompaction()).as("and gave its slot back").isTrue();

    assertThat(index.compact()).isTrue();
    assertExact(index, live);
  }

  // ------------------------------------------------------------------------------------------- automatic compaction

  @Test
  void aCommitSchedulesACompactionOnlyWhenTheGatesAreOpen() {
    createType("Doc", "embedding", "INT8");
    final Map<RID, float[]> live = insert("Doc", 400, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    final int fileBefore = index.getFileId();

    withMinPages(0, () -> {
      for (int cycle = 1; cycle <= 6; cycle++)
        reembed(live, cycle);
      assertThat(index.getTotalPages()).as("the file is bloated").isGreaterThan(3 * index.estimatePagesForLiveSetForTest());
      index.onAfterCommit();
      database.async().waitCompletion(30_000);
      assertThat(index.getFileId()).as("a minimum of 0 pages turns the scheduling off, whatever the garbage").isEqualTo(fileBefore);
    });
    assertThat(index.getFileId()).isEqualTo(fileBefore);

    withBloatFactor(0, () -> {
      index.onAfterCommit();
      database.async().waitCompletion(30_000);
      assertThat(index.getFileId()).as("a bloat factor of 0 leaves the compaction to COMPACT INDEX").isEqualTo(fileBefore);
    });

    // the same index and the same garbage, the gates open: the commit hook compacts it
    withMinPages(2, () -> {
      index.onAfterCommit();
      database.async().waitCompletion(30_000);
    });
    assertThat(index.getFileId()).as("a bloated index is compacted").isNotEqualTo(fileBefore);
    assertThat(index.countEntries()).isEqualTo(live.size());
    assertSelfLookups(index, live, true, 11);
  }

  @Test
  void anIndexWhoseCompactionIsBlockedBySharedIdsIsNotScheduledAgain() throws Exception {
    checkIntegrity = false; // the index is damaged on purpose
    createType("Doc", "embedding", "INT8");
    final Map<RID, float[]> live = insert("Doc", 400, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    withMinPages(0, () -> {
      for (int cycle = 1; cycle <= 6; cycle++)
        reembed(live, cycle);
    });
    // two records on one vector id: the build leaves one out and blocks the compaction until the index is rebuilt
    final List<RID> order = new ArrayList<>(live.keySet());
    final int sharedId = index.residentLocationsForTest().getVectorIdsForRid(order.get(0))[0];
    database.transaction(() -> index.persistEntryForTest(sharedId, order.get(1), live.get(order.get(1))));
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).isFalse();
    assertThat(index.compactionBlockedBySharedIdsForTest()).isTrue();
    assertThat(index.getTotalPages()).as("the file is bloated").isGreaterThan(3 * index.estimatePagesForLiveSetForTest());

    final long buildsBefore = index.getStats().get("graphRebuildCount");
    final int fileBefore = index.getFileId();
    withMinPages(2, () -> {
      index.onAfterCommit();
      database.async().waitCompletion(30_000);
    });

    assertThat(index.getStats().get("graphRebuildCount")).as("no full graph build is repeated for nothing").isEqualTo(buildsBefore);
    assertThat(index.getFileId()).isEqualTo(fileBefore);
    assertThat(index.isCompacting()).isFalse();
  }

  @Test
  void anIndexHoldingNoLiveVectorIsNotScheduledForCompaction() {
    createType("Doc", "embedding", "INT8");
    final Map<RID, float[]> live = insert("Doc", 400, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    withMinPages(0, () -> {
      for (int cycle = 1; cycle <= 4; cycle++)
        reembed(live, cycle);
      database.transaction(() -> {
        for (final RID rid : live.keySet())
          rid.asDocument().delete();
      });
    });
    assertThat(index.countEntries()).isZero();
    assertThat(index.getTotalPages()).as("a file full of garbage").isGreaterThan(3);
    final long buildsBefore = index.getStats().get("graphRebuildCount");
    final int fileBefore = index.getFileId();

    withMinPages(2, () -> {
      index.onAfterCommit();
      database.async().waitCompletion(30_000);
    });

    assertThat(index.getFileId()).isEqualTo(fileBefore);
    assertThat(index.getStats().get("graphRebuildCount")).as("there is nothing to compact, so no build runs").isEqualTo(buildsBefore);
  }

  // ------------------------------------------------------------------------------------------- rebuild in place

  @Test
  void anIndexThatCannotBeRebuiltInPlaceSaysWhy() {
    createType("Doc", "embedding", "NONE");
    insert("Doc", RECORDS, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    assertThat(index.inPlaceRebuildRefusal()).as("a sound index is rebuilt in place").isNull();

    index.getMetadata().dimensions = 0;
    assertThat(index.inPlaceRebuildRefusal()).isEqualTo("the vector index '" + index.getName() + "' has not decided its dimensions yet");
    index.getMetadata().dimensions = DIMENSIONS;

    index.simulateCrashForTest();
    assertThat(index.inPlaceRebuildRefusal())
        .isEqualTo("the vector index '" + index.getName() + "' was not built to the end (BUILDING)");
    index.markInvalidForTest();
    assertThat(index.inPlaceRebuildRefusal())
        .isEqualTo("the vector index '" + index.getName() + "' was not built to the end (INVALID)");
  }

  @Test
  void aRecordHeldWithAnotherPayloadIsReplacedAndNotHeldTwice() {
    createType("Doc", "embedding", "INT8");
    final Map<RID, float[]> live = insert("Doc", RECORDS, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    final RID stale = live.keySet().iterator().next();
    final float[] real = live.get(stale);
    final float[] other = vector(9000);
    // the index holds another vector of the record than the one the record has
    database.transaction(() -> index.remove(new Object[] { real }, stale));
    database.transaction(() -> index.put(new Object[] { other }, new RID[] { stale }));
    assertThat(index.findNeighborsFromVector(other, 1).getFirst().getFirst()).isEqualTo(stale);

    assertThat(index.reconcileWithBucket(100, null)).as("one record written again").isEqualTo(1);

    assertThat(index.getStats().get("activeVectors")).as("one live vector per record").isEqualTo((long) RECORDS);
    assertThat(index.countEntries()).isEqualTo(RECORDS);
    final List<Pair<RID, Float>> byReal = index.findNeighborsFromVector(real, 1);
    assertThat(byReal.getFirst().getFirst()).isEqualTo(stale);
    assertThat(byReal.getFirst().getSecond()).as("held as the record has it").isLessThan(0.01f);
    final List<Pair<RID, Float>> byStale = index.findNeighborsFromVector(other, 1);
    assertThat(byStale.getFirst().getSecond()).as("the vector it held before is gone").isGreaterThan(0.01f);
    assertThat(index.reconcileWithBucket(100, null)).as("and the index is in sync now").isZero();
  }

  @Test
  void aRecordThatCannotBeReadStopsTheRebuildWithTheErrorOfTheRecord() {
    checkIntegrity = false; // a record is corrupted on purpose
    createType("Doc", "embedding", "NONE");
    final Map<RID, float[]> live = insert("Doc", RECORDS, 0);
    final LSMVectorIndex index = lsm("Doc", "embedding");
    final RID broken = live.keySet().iterator().next();
    corruptRecordTypeByte((DatabaseInternal) database, broken);

    assertThatThrownBy(() -> index.reconcileWithBucket(100, null)).isNotNull().satisfies(e -> {
      assertThat(e).isNotInstanceOf(com.arcadedb.index.IndexException.class);
      assertThat(e.getMessage()).doesNotContain("while rebuilding index");
    });
    assertThat(database.isTransactionActive()).as("the rebuild's transaction is rolled back").isFalse();
  }

  // ------------------------------------------------------------------------------------------------------- helpers

  private void createType(final String type, final String property, final String quantization) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
    database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY " + type + ".other ARRAY_OF_FLOATS");
    createIndex(type, property, quantization);
  }

  private void createIndex(final String type, final String property, final String quantization) {
    final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
        .buildTypeIndex(type, new String[] { property }).withLSMVectorType().withPageSize(PAGE_SIZE);
    builder.withDimensions(DIMENSIONS).withSimilarity("COSINE").withQuantization(quantization).create();
  }

  /** Records with an {@code embedding} and an {@code other} vector; the model holds the first. */
  private Map<RID, float[]> insert(final String type, final int count, final int firstSeed) {
    final Map<RID, float[]> live = new LinkedHashMap<>();
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] embedding = vector(firstSeed + i);
        final RID rid = database.newDocument(type).set("id", i, "embedding", embedding, "other", vector(100_000 + firstSeed + i)).save()
            .getIdentity();
        live.put(rid, embedding);
      }
    });
    return live;
  }

  private void reembed(final Map<RID, float[]> live, final int cycle) {
    database.transaction(() -> {
      int i = 0;
      for (final Map.Entry<RID, float[]> entry : live.entrySet()) {
        final float[] updated = vector(cycle * 1_000_000 + i++);
        entry.getKey().asDocument().modify().set("embedding", updated).save();
        entry.setValue(updated);
      }
    });
  }

  private void withMinPages(final int pages, final Runnable body) {
    final Integer old = database.getConfiguration().getValueAsInteger(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, pages);
    try {
      body.run();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, old);
    }
  }

  private void withBloatFactor(final int factor, final Runnable body) {
    final Integer old = database.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR, factor);
    try {
      body.run();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR, old);
    }
  }

  private LSMVectorIndex lsm(final String type, final String property) {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[" + property + "]")).getIndexesOnBuckets()[0];
  }

  /** The names in the database directory that belong to the index: its data file, its graph and the sidecars of the graph. */
  private List<String> filesOf(final LSMVectorIndex index) {
    final String prefix = index.getName();
    final List<String> files = new ArrayList<>();
    for (final String name : new File(database.getDatabasePath()).list())
      if (name.startsWith(prefix))
        files.add(name);
    return files;
  }

  private void verify(final LSMVectorIndex index, final Map<RID, float[]> live, final Set<RID> deleted, final boolean exact) {
    if (exact)
      assertExact(index, live);
    assertSelfLookups(index, live, !exact, 13);
    for (final RID rid : deleted)
      assertThat(index.hasLiveVector(rid)).as("deleted %s", rid).isFalse();
    final Set<RID> found = new HashSet<>();
    for (int probe = 0; probe < 5; probe++)
      index.findNeighborsFromVector(vector(777_000 + probe), live.size() + deleted.size()).forEach(p -> found.add(p.getFirst()));
    assertThat(found).as("a deleted record is never an answer").doesNotContainAnyElementsOf(deleted);
  }

  /** The record of each of every {@code step}th vector is its own nearest neighbour. */
  private void assertSelfLookups(final LSMVectorIndex index, final Map<RID, float[]> live, final boolean quantized, final int step) {
    int i = 0;
    for (final Map.Entry<RID, float[]> entry : live.entrySet()) {
      if (i++ % step != 0)
        continue;
      final List<Pair<RID, Float>> found = index.findNeighborsFromVector(entry.getValue(), 1);
      assertThat(found).as("self lookup of %s", entry.getKey()).hasSize(1);
      assertThat(found.getFirst().getFirst()).as("self lookup of %s", entry.getKey()).isEqualTo(entry.getKey());
    }
  }

  /** The k live vectors most similar to the query by cosine: what an exact search answers. */
  private void assertExact(final LSMVectorIndex index, final Map<RID, float[]> live) {
    for (int probe = 0; probe < 4; probe++) {
      final float[] query = vector(900_000 + probe);
      final List<RID> expected = live.entrySet().stream()
          .sorted(Comparator.comparingDouble((Map.Entry<RID, float[]> e) -> -cosine(query, e.getValue()))).limit(5)
          .map(Map.Entry::getKey).toList();
      final List<RID> actual = index.findNeighborsFromVector(query, 5, 100).stream().map(Pair::getFirst).toList();
      assertThat(actual).as("probe %d", probe).containsExactlyElementsOf(expected);
    }
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }

  /** A non-zero vector distinct per seed. */
  private static float[] vector(final int seed) {
    final Random random = new Random(seed * 7919L + 13);
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = (float) random.nextGaussian();
    vector[0] += 0.5f;
    return vector;
  }
}
