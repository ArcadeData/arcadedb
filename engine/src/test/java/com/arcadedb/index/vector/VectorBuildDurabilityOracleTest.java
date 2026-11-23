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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The durability and the shape of a vector index build, judged by the records: every live record finds itself, by its own vector,
 * at rank 1 (the vectors are distinct random ones), and no record that is gone is ever answered.
 */
@Tag("vector")
class VectorBuildDurabilityOracleTest extends TestHelper {
  private static final int DIMENSIONS = 1024;
  private static final int VECTORS    = 1000;
  private static final int PAGE_SIZE  = 65_536;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * A flush suspended by a backup keeps the pages a build committed without the WAL in the pipeline. The build writes them again
   * through the WAL, one commit per bulk-load chunk of pages, before it certifies anything (issue #11238). The pages of this index
   * take 17 pages of 64 KB, and a chunk of 1 MB holds 16: the chunk that reaches the limit exactly is committed at the limit, not
   * one page past it, so the build adds two commits to the ones a build with a working flush makes. A process killed right after
   * the statement returns must reopen with every vector.
   */
  @Test
  void aCreateIndexWhoseFlushIsSuspendedWritesItsPagesThroughTheWalInChunksAndSurvivesAKill() throws Exception {
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);
    final Map<RID, float[]> rowsA = populate("A", VECTORS, 11);
    final Map<RID, float[]> rowsB = populate("B", VECTORS, 12);
    final DatabaseInternal db = (DatabaseInternal) database;

    final long beforeA = db.getTransactionManager().getPublishedCommits();
    createIndex("A", DIMENSIONS, VectorQuantizationType.INT8, PAGE_SIZE);
    final long commitsOfA = db.getTransactionManager().getPublishedCommits() - beforeA;
    assertEveryRowFindsItself(indexOf("A"), rowsA, "A after its build");

    final AtomicLong commitsOfB = new AtomicLong();
    final AtomicLong pagesOfB = new AtomicLong();
    db.getPageManager().suspendFlushAndExecute(db, () -> {
      final long beforeB = db.getTransactionManager().getPublishedCommits();
      createIndex("B", DIMENSIONS, VectorQuantizationType.INT8, PAGE_SIZE);
      commitsOfB.set(db.getTransactionManager().getPublishedCommits() - beforeB);
      pagesOfB.set(indexOf("B").getTotalPages());
      db.kill();
    });

    assertThat(pagesOfB.get()).as("the data file of the index takes one page more than a chunk holds").isEqualTo(17L);
    assertThat(commitsOfB.get() - commitsOfA).as("commits that write 17 pages in chunks of 16").isEqualTo(2L);

    database.close();
    database = factory.open();
    assertThat(indexOf("B").countEntries()).as("entries of B after the kill").isEqualTo(VECTORS);
    assertEveryRowFindsItself(indexOf("B"), rowsB, "B after the kill and the reopen");
    assertEveryRowFindsItself(indexOf("A"), rowsA, "A after the kill and the reopen");
  }

  /**
   * A build that persists a graph (it runs over an index that is loaded and has not been written to since: the graph is built from
   * the pages as they are) must not vouch for graph pages the flush has not written. With the flush suspended none is on the disk,
   * and the manifest refuses them: a kill takes the pages away, and the reopen rebuilds the graph from the vector pages, which the
   * build wrote through the WAL.
   */
  @Test
  void aBuildWhoseFlushIsSuspendedDoesNotVouchForTheGraphPagesTheFlushHasNotWritten() throws Exception {
    final Map<RID, float[]> rows = populate("G", 300, 32, 31);
    createIndex("G", 32, VectorQuantizationType.INT8, 4096);
    reopenDatabase();

    assertThat(buildWithTheFlushSuspendedAndKill("G")).as("graph pages not on the disk: the manifest refuses them")
        .isEqualTo(LSMVectorIndexGraphManifest.UNUSABLE_VECTOR_COUNT);

    // the second open: see theFirstOpenAfterTheKillHoldsEveryEntryOnce
    reopenDatabase();
    assertThat(indexOf("G").countEntries()).isEqualTo(300L);
    assertEveryRowFindsItself(indexOf("G"), rows, "G after the kill and two reopens");
  }

  /**
   * The same build and kill: the location index of the first open holds each record once. It holds 27 of the 300 superseded
   * vectors as live too, until the next open (repro vec-build-suspended-flush-kill-stale-locations).
   */
  @Test
  void theFirstOpenAfterTheKillHoldsEveryEntryOnce() throws Exception {
    // Not @Disabled: JUnit still builds the instance of a disabled method, and TestHelper creates a database in its constructor that
    // no @AfterEach then drops. Fails on main (#13216): run with -Darcadedb.test.issue13216=true
    assumeThat(Boolean.getBoolean("arcadedb.test.issue13216"))
        .as("a build over an index that holds its vectors, killed with the flush suspended, leaves stale locations at the first open (#13216)")
        .isTrue();
    final Map<RID, float[]> rows = populate("G", 300, 32, 31);
    createIndex("G", 32, VectorQuantizationType.INT8, 4096);
    reopenDatabase();
    buildWithTheFlushSuspendedAndKill("G");

    assertThat(indexOf("G").countEntries()).isEqualTo(300L);
    assertEveryRowFindsItself(indexOf("G"), rows, "G after the kill and the first open");
  }

  /**
   * Builds the index again, in place, while the flush of the database is suspended, and kills the database as it stands when the
   * build returns, as a process killed then would leave it. The database is open again when this returns.
   *
   * @return the vector count the manifest of the graph the build persisted carried at that moment
   */
  private int buildWithTheFlushSuspendedAndKill(final String typeName) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final AtomicLong manifest = new AtomicLong(Long.MIN_VALUE);
    db.getPageManager().suspendFlushAndExecute(db, () -> {
      final LSMVectorIndex index = indexOf(typeName);
      assertThat(index.build(null, null)).isEqualTo(300L);
      final LSMVectorIndexGraphManifest.Content content = index.getGraphFile().getManifest().read();
      assertThat(content).as("the manifest of the graph persisted by the build").isNotNull();
      manifest.set(content.vectorCount());
      db.kill();
    });
    database.close();
    database = factory.open();
    return (int) manifest.get();
  }

  /**
   * A build that fails inside the transaction it opened rolls it back, marks the index INVALID and refuses the graph pages, and the
   * exception the caller sees is the one that stopped it. A record callback that throws a runtime exception stops the build with
   * that very exception; one that cannot be told from a failed read of the record (a checked exception) is wrapped with the record.
   * REBUILD INDEX is the way back, and it leaves every record finding itself.
   */
  @Test
  void aBuildThatFailsInItsOwnTransactionRollsBackMarksTheIndexInvalidAndRefusesItsGraph() {
    final Map<RID, float[]> rows = populate("F", 300, 16, 21);
    createIndex("F", 16, VectorQuantizationType.NONE, 4096);
    final LSMVectorIndex first = indexOf("F");
    assertEveryRowFindsItself(first, rows, "F before the failures");
    assertThat(first.getGraphFile().getManifest().read().vectorCount()).as("the search certified its graph").isEqualTo(300);

    final IllegalStateException boom = new IllegalStateException("the callback failed");
    assertThatThrownBy(() -> first.build((document, total) -> {
      if (total == 5)
        throw boom;
    }, null)).as("a runtime exception stops the build as it is").isSameAs(boom);
    assertThat(database.isTransactionActive()).as("the build rolled back the transaction it opened").isFalse();
    assertThat(first.isValid()).as("a failed build leaves the index INVALID").isFalse();
    assertThat(first.getGraphFile().getManifest().read().vectorCount()).as("and its graph pages refused")
        .isEqualTo(LSMVectorIndexGraphManifest.UNUSABLE_VECTOR_COUNT);

    database.command("sql", "REBUILD INDEX `F[vector]`").close();
    final LSMVectorIndex second = indexOf("F");
    assertThat(second.isValid()).isTrue();
    assertEveryRowFindsItself(second, rows, "F after REBUILD INDEX");

    final IOException disk = new IOException("no space left");
    assertThatThrownBy(() -> second.build((document, total) -> {
      if (total == 7)
        VectorBuildDurabilityOracleTest.<RuntimeException>sneakyThrow(disk);
    }, null)).isInstanceOf(IndexException.class).hasMessageContaining("Error on building vector index").hasCause(disk);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(second.isValid()).isFalse();

    database.command("sql", "REBUILD INDEX `F[vector]`").close();
    assertEveryRowFindsItself(indexOf("F"), rows, "F after the second REBUILD INDEX");
  }

  /**
   * The graph of a bulk load is built when something is pending, and not otherwise: a call on an index whose graph already covers
   * every vector builds nothing, a failed build hands the pending count back, and a dropped index refuses.
   */
  @Test
  void theGraphIsBuiltOnlyWhenWorkIsPendingAndAFailedBuildKeepsItPending() {
    final Map<RID, float[]> rows = populate("P", 300, 16, 41);
    createIndex("P", 16, VectorQuantizationType.NONE, 4096);
    final LSMVectorIndex index = indexOf("P");
    assertThat(index.getStats().get("graphNodeCount")).as("a build leaves its graph to the first use").isZero();

    assertThat(index.buildVectorGraphIfPending(null)).as("300 vectors wait for a graph").isTrue();
    assertThat(index.getStats().get("graphNodeCount")).isEqualTo(300L);
    assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L);
    assertEveryRowFindsItself(index, rows, "P after the graph was built");

    assertThat(index.buildVectorGraphIfPending(null)).as("nothing is pending: the graph is current").isFalse();
    assertThat(index.getStats().get("graphRebuildCount")).as("and nothing was rebuilt").isEqualTo(1L);

    final float[] extra = randomVector(new Random(42), 16);
    final RID extraRid = insert("P", extra);
    rows.put(extraRid, extra);
    final IllegalStateException refused = new IllegalStateException("the build is refused");
    assertThatThrownBy(() -> index.buildVectorGraphNow((phase, processed, total, accesses) -> {
      throw refused;
    })).isSameAs(refused);
    assertThat(index.getStats().get("graphNodeCount")).as("the failed build published no graph").isEqualTo(300L);

    // before any search: a search of a graph this small rebuilds it by itself
    assertThat(index.buildVectorGraphIfPending(null)).as("the vector the failed build did not absorb is still pending").isTrue();
    assertThat(index.getStats().get("graphNodeCount")).isEqualTo(301L);
    assertEveryRowFindsItself(index, rows, "P after the retry");

    assertThat(index.buildVectorGraphIfPending(null)).isFalse();
    database.getSchema().dropIndex("P[vector]");
    assertThatThrownBy(() -> index.buildVectorGraphIfPending(null)).as("a dropped index builds nothing, not even a no-op")
        .isInstanceOf(IndexException.class).hasMessageContaining("is not valid");
  }

  /**
   * The pool a graph build runs on follows the configured width: when the width changes between two builds the pool of the first
   * is shut down (nothing leaks a pool of idle workers) and the second builds on a new one of the width asked for.
   */
  @Test
  void aChangedBuildWidthShutsTheOldPoolDownAndBuildsOnANewOne() throws Exception {
    final Map<RID, float[]> rows = populate("W", 300, 16, 51);
    createIndex("W", 16, VectorQuantizationType.NONE, 4096);
    final LSMVectorIndex index = indexOf("W");
    final Field poolField = LSMVectorIndex.class.getDeclaredField("graphBuildPool");
    poolField.setAccessible(true);

    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.setValue(1);
    index.buildVectorGraphNow(null);
    final ForkJoinPool narrow = (ForkJoinPool) poolField.get(index);
    assertThat(narrow.getParallelism()).isEqualTo(1);
    assertThat(narrow.isShutdown()).isFalse();

    index.buildVectorGraphNow(null);
    assertThat(poolField.get(index)).as("the same width keeps its pool").isSameAs(narrow);

    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.setValue(2);
    index.buildVectorGraphNow(null);
    final ForkJoinPool wide = (ForkJoinPool) poolField.get(index);
    assertThat(wide).isNotSameAs(narrow);
    assertThat(wide.getParallelism()).isEqualTo(2);
    assertThat(narrow.isShutdown()).as("the pool of the old width is shut down").isTrue();
    assertThat(wide.isShutdown()).isFalse();
    assertEveryRowFindsItself(index, rows, "W after the builds");
  }

  /**
   * The page the index appends to carries a header no entry can be written after (a page of an older format, or a damaged one): the
   * entry goes to the page after it, whatever the page number was, and every page before is left as it was. The same for the
   * tombstone of a delete. The pages hold 512 bytes, so the index spans several, and the page the entry would have landed on
   * had the header been sound is the one that must stay untouched.
   */
  @Test
  void anEntryWrittenAfterAPageWhoseHeaderIsBrokenGoesToANewPageAndLeavesTheOthersAlone() {
    final Map<RID, float[]> rows = populate("H", 400, 6, 61);
    createIndex("H", 6, VectorQuantizationType.NONE, 512);
    final LSMVectorIndex index = indexOf("H");
    final int pages = index.getTotalPages();
    assertThat(pages).as("pages of the index before the header is damaged").isGreaterThanOrEqualTo(3);
    final Map<Integer, RID> before = entriesBeforePage(index, pages - 1);
    assertThat(before).hasSize(entriesOnPagesBefore(index, pages - 1));

    damageHeaderOfPage(index, pages - 1);
    final float[] added = randomVector(new Random(62), 6);
    final RID addedRid = insert("H", added);
    rows.put(addedRid, added);

    assertThat(index.getTotalPages()).as("the entry went to a new page").isEqualTo(pages + 1);
    assertThat(entriesBeforePage(index, pages - 1)).as("the pages before the damaged one are as they were").isEqualTo(before);
    final List<LSMVectorIndexPageParser.VectorEntry> onNewPage = entriesOnPage(index, pages);
    assertThat(onNewPage).hasSize(1);
    assertThat(onNewPage.get(0).rid).isEqualTo(addedRid);
    assertThat(onNewPage.get(0).deleted).isFalse();
    assertEveryRowFindsItself(index, rows, "H after the insert");

    damageHeaderOfPage(index, pages);
    final RID deleted = rows.keySet().stream().filter(r -> !r.equals(addedRid)).sorted().findFirst().orElseThrow();
    final float[] deletedVector = rows.remove(deleted);
    database.transaction(() -> database.lookupByRID(deleted, true).asDocument(true).delete());

    assertThat(index.getTotalPages()).as("the tombstone went to a new page").isEqualTo(pages + 2);
    assertThat(entriesBeforePage(index, pages - 1)).isEqualTo(before);
    final List<LSMVectorIndexPageParser.VectorEntry> tombstones = entriesOnPage(index, pages + 1);
    assertThat(tombstones).hasSize(1);
    assertThat(tombstones.get(0).rid).isEqualTo(deleted);
    assertThat(tombstones.get(0).deleted).isTrue();
    assertThat(index.findNeighborsFromVector(deletedVector, 3, 100)).extracting(Pair::getFirst).doesNotContain(deleted);
    assertEveryRowFindsItself(index, rows, "H after the delete");
  }

  /**
   * A graph build that reads a live entry it cannot use - the record is gone, its vector is not one, the page holds a vector of
   * another width or of another encoding - leaves that entry out and builds the graph on the others; and says how many it left out.
   * The records here are the damage a failed bulk delete or an older build leaves behind, written straight onto the pages. Run with
   * the float cache of the build large enough for every vector, and so small that the build keeps INT8 rows instead.
   */
  @ParameterizedTest
  @CsvSource({ "NONE,0", "INT8,0", "INT8,16" })
  void aGraphBuildLeavesOutTheLiveEntriesItCannotUseAndCountsThem(final VectorQuantizationType quantization,
      final int graphBuildCacheSize) {
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(graphBuildCacheSize);
    final int dimensions = 16;
    final boolean floats = quantization == VectorQuantizationType.NONE;
    final Random rnd = new Random(71);
    database.getSchema().buildDocumentType().withName("V").withTotalBuckets(1).create().createProperty("vector", Type.LIST);
    final Map<RID, float[]> rows = new HashMap<>();
    final List<RID> gone = new ArrayList<>();
    final List<RID> notVectors = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 300; i++) {
        final float[] v = randomVector(rnd, dimensions);
        rows.put(database.newDocument("V").set("vector", asList(v)).save().getIdentity(), v);
      }
      // an INT8 build trusts the pages and reads no record: only a float index finds out that a record is gone
      for (int i = 0; floats && i < 4; i++)
        gone.add(database.newDocument("V").set("vector", asList(randomVector(rnd, dimensions))).save().getIdentity());
      for (int i = 0; i < 5; i++)
        notVectors.add(database.newDocument("V").set("vector", List.of("not", "a", "vector")).save().getIdentity());
    });
    createIndex("V", dimensions, quantization, 4096);
    final LSMVectorIndex index = indexOf("V");
    assertThat(index.getLastBuildSkippedRecords()).as("records the build refused").isEqualTo(5L);
    // the graph absorbs the buffer of the vectors written since the last one: the build below reads the records, not the buffer
    index.buildVectorGraphNow(null);

    // the records behind the live entries of "gone" are deleted below the index; the five others stand for records that hold no
    // vector: for a float index the page entries say nothing about it, for an INT8 one two hold a vector of another width and
    // three one of another encoding
    database.transaction(() -> {
      for (final RID rid : gone)
        deleteRecordAtLowLevel(database, rid);
      if (floats)
        for (int i = 0; i < notVectors.size(); i++)
          index.persistEntryForTest(900_000 + i, notVectors.get(i), new float[dimensions]);
      else {
        index.persistEntryForTest(900_000, notVectors.get(0), new float[dimensions / 2]);
        index.persistEntryForTest(900_001, notVectors.get(1), new float[dimensions * 2]);
        appendEntryOfAnotherEncoding(index, 900_002, notVectors.get(2));
        appendEntryOfAnotherEncoding(index, 900_003, notVectors.get(3));
        appendEntryOfAnotherEncoding(index, 900_004, notVectors.get(4));
      }
    });

    final List<String> messages = captureLog(() -> index.buildVectorGraphNow(null));

    assertThat(messages).as("what the build says it left out").contains(
        "Filtered out " + (floats ? 9 : 5) + " vectors with deleted/invalid documents during graph build");
    assertThat(index.getStats().get("graphNodeCount")).as("the graph holds the records that are there").isEqualTo(300L);
    assertThat(index.getStats().get("graphBuildQuantizedResident") > 0L).as("the build kept INT8 rows")
        .isEqualTo(!floats && graphBuildCacheSize > 0);
    assertEveryRowFindsItself(index, rows, "V after the build");
    final List<RID> neverAnswered = new ArrayList<>(gone);
    neverAnswered.addAll(notVectors);
    for (final float[] query : rows.values().stream().limit(40).toList())
      assertThat(index.findNeighborsFromVector(query, 10, 400)).extracting(Pair::getFirst).doesNotContainAnyElementsOf(neverAnswered);
  }

  /**
   * Records written below the index (a restore, a loader that bypassed it) are not on its pages. A graph build that finds far fewer
   * vectors on the pages than the bucket has records reads the records to recover them, and recovers every one that holds a vector
   * of the index's width; a record that holds another one stays out. It says so, with the counts.
   */
  @Test
  void aGraphBuildRecoversTheRecordsItsPagesDoNotHold() {
    final int dimensions = 16;
    final Random rnd = new Random(81);
    final Map<RID, float[]> rows = populate("S", 60, dimensions, 82);
    createIndex("S", dimensions, VectorQuantizationType.NONE, 4096);
    final LSMVectorIndex index = indexOf("S");
    index.buildVectorGraphNow(null);
    final List<RID> narrow = new ArrayList<>();
    final List<RID> below = new ArrayList<>();
    final DatabaseInternal db = (DatabaseInternal) database;
    final Bucket bucket = database.getSchema().getType("S").getBuckets(false).get(0);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++) {
        final float[] v = randomVector(rnd, dimensions);
        final MutableDocument document = database.newDocument("S").set("vector", v);
        final RID rid = bucket.createRecord(document, false);
        db.getTransaction().updateBucketRecordDelta(bucket.getFileId(), +1);
        rows.put(rid, v);
        below.add(rid);
      }
      for (int i = 0; i < 3; i++) {
        final MutableDocument document = database.newDocument("S").set("vector", randomVector(rnd, dimensions / 2));
        narrow.add(bucket.createRecord(document, false));
        db.getTransaction().updateBucketRecordDelta(bucket.getFileId(), +1);
      }
    });

    final List<String> messages = captureLog(() -> index.buildVectorGraphNow(null));

    assertThat(messages).anyMatch(m -> m.startsWith("Page-parsed vectors (60) significantly less than document count (363)"));
    assertThat(messages).as("what the scan of the records recovered").anyMatch(
        m -> m.startsWith("After document scan fallback: 360 active vectors for graph build (was 60 from pages only)"));
    assertThat(index.getStats().get("graphNodeCount")).isEqualTo(360L);
    assertEveryRowFindsItself(index, rows, "S after the build");
    for (final float[] query : rows.values().stream().limit(40).toList())
      assertThat(index.findNeighborsFromVector(query, 10, 400)).extracting(Pair::getFirst).doesNotContainAnyElementsOf(narrow);
  }

  /**
   * A chunk size of 0 or less cannot chunk anything: the build falls back to the stock 50 MB, says so, and builds the same index.
   */
  @Test
  void aChunkSizeOfZeroFallsBackToTheStockOneAndSaysSo() {
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(0L);
    final Map<RID, float[]> rows = populate("Z", 300, 16, 91);

    final List<String> messages = captureLog(() -> createIndex("Z", 16, VectorQuantizationType.INT8, 4096));

    assertThat(messages).contains("arcadedb.index.buildChunkSizeMB was 0MB during graph persistence; forcing fallback to 50MB");
    assertThat(indexOf("Z").countEntries()).isEqualTo(300L);
    assertEveryRowFindsItself(indexOf("Z"), rows, "Z after the build");
  }

  /**
   * A corpus several times the size of the page cache: the records, the index pages and the graph are read from the disk again
   * and again while the index is built, searched, rewritten and reopened. Every live record still finds itself by its own vector, a
   * record whose vector was rewritten no longer answers to the old one, and a deleted one answers to nothing.
   */
  @ParameterizedTest
  @EnumSource(value = VectorQuantizationType.class, names = { "NONE", "INT8" })
  void aCorpusLargerThanThePageCacheIsBuiltRewrittenAndReopenedWithoutLosingARecord(final VectorQuantizationType quantization) {
    final int dimensions = 64;
    final int count = 4000;
    database.close();
    GlobalConfiguration.MAX_PAGE_RAM.setValue(1L);
    database = factory.open();
    final Map<RID, float[]> rows = populate("C", count, dimensions, 101);
    createIndex("C", dimensions, quantization, 16_384);
    assertThat(sizeOfTheDatabaseOnDisk()).as("bytes of the files of the database: more than the page cache holds")
        .isGreaterThan(2L * 1024 * 1024);
    assertEveryRowFindsItself(indexOf("C"), sample(rows, 13), count, "C after the build");

    final List<RID> order = new ArrayList<>(rows.keySet());
    order.sort(null);
    final Random rnd = new Random(102);
    final List<RID> deleted = new ArrayList<>(order.subList(0, 150));
    final Map<RID, float[]> rewritten = new HashMap<>();
    final Map<RID, float[]> deletedVectors = new HashMap<>();
    database.transaction(() -> {
      for (final RID rid : deleted) {
        deletedVectors.put(rid, rows.remove(rid));
        database.lookupByRID(rid, true).asDocument(true).delete();
      }
      for (final RID rid : order.subList(150, 300)) {
        final float[] v = randomVector(rnd, dimensions);
        database.lookupByRID(rid, true).asDocument(true).modify().set("vector", v).save();
        rewritten.put(rid, rows.put(rid, v));
      }
    });
    indexOf("C").buildVectorGraphNow(null);
    assertAfterTheRewrite(indexOf("C"), rows, deleted, deletedVectors, rewritten, "C after the rewrite");

    reopenDatabase();
    assertThat(indexOf("C").countEntries()).as("entries of C after the reopen").isEqualTo(count - 150L);
    assertAfterTheRewrite(indexOf("C"), rows, deleted, deletedVectors, rewritten, "C after the reopen");
  }

  private void assertAfterTheRewrite(final LSMVectorIndex index, final Map<RID, float[]> rows, final List<RID> deleted,
      final Map<RID, float[]> deletedVectors, final Map<RID, float[]> rewritten, final String where) {
    final int beam = rows.size() + deleted.size();
    assertEveryRowFindsItself(index, sample(rows, 13), beam, where);
    final Map<RID, float[]> rewrittenNow = new HashMap<>();
    for (final RID rid : rewritten.keySet())
      rewrittenNow.put(rid, rows.get(rid));
    assertEveryRowFindsItself(index, sample(rewrittenNow, 3), beam, where + ", rewritten records");
    for (final Map.Entry<RID, float[]> old : rewritten.entrySet())
      assertThat(index.findNeighborsFromVector(old.getValue(), 1, 100)).as("%s: %s answers to its old vector", where, old.getKey())
          .extracting(Pair::getFirst).doesNotContain(old.getKey());
    for (final RID rid : deleted)
      assertThat(index.findNeighborsFromVector(deletedVectors.get(rid), 5, 100)).as("%s: %s is deleted", where, rid)
          .extracting(Pair::getFirst).doesNotContain(rid);
  }

  /** Every {@code step}th row of {@code rows}, in the order of the records. */
  private static Map<RID, float[]> sample(final Map<RID, float[]> rows, final int step) {
    final Map<RID, float[]> sampled = new HashMap<>();
    final List<RID> order = new ArrayList<>(rows.keySet());
    order.sort(null);
    for (int i = 0; i < order.size(); i += step)
      sampled.put(order.get(i), rows.get(order.get(i)));
    return sampled;
  }

  private long sizeOfTheDatabaseOnDisk() {
    long total = 0;
    final File[] files = new File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final File file : files)
        total += file.length();
    return total;
  }

  // ---------------------------------------------------------------------------------------------------------------------------

  private Map<RID, float[]> populate(final String typeName, final int count, final long seed) {
    return populate(typeName, count, DIMENSIONS, seed);
  }

  private Map<RID, float[]> populate(final String typeName, final int count, final int dimensions, final long seed) {
    final Random rnd = new Random(seed);
    final Map<RID, float[]> rows = new HashMap<>();
    if (!database.getSchema().existsType(typeName))
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create()
          .createProperty("vector", Type.ARRAY_OF_FLOATS);
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] v = randomVector(rnd, dimensions);
        rows.put(database.newDocument(typeName).set("vector", v).save().getIdentity(), v);
      }
    });
    return rows;
  }

  private void createIndex(final String typeName, final int dimensions, final VectorQuantizationType quantization,
      final int pageSize) {
    database.transaction(() -> ((TypeLSMVectorIndexBuilder) database.getSchema().buildTypeIndex(typeName, new String[] { "vector" })
        .withLSMVectorType().withPageSize(pageSize)).withDimensions(dimensions).withQuantization(quantization)
        .withSimilarity("EUCLIDEAN").create());
  }

  private LSMVectorIndex indexOf(final String typeName) {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(typeName + "[vector]")).getIndexesOnBuckets()[0];
  }


  private RID insert(final String typeName, final float[] vector) {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> rid.set(database.newDocument(typeName).set("vector", vector).save().getIdentity()));
    return rid.get();
  }

  private List<LSMVectorIndexPageParser.VectorEntry> entriesOnPage(final LSMVectorIndex index, final int pageNumber) {
    final List<LSMVectorIndexPageParser.VectorEntry> entries = new ArrayList<>();
    LSMVectorIndexPageParser.parsePages((DatabaseInternal) database, index.getFileId(), index.getTotalPages(), index.getPageSize(),
        false, false, entry -> {
          if (entry.absoluteFileOffset / index.getPageSize() == pageNumber)
            entries.add(entry);
        });
    return entries;
  }

  private int entriesOnPagesBefore(final LSMVectorIndex index, final int pageNumber) {
    return entriesBeforePage(index, pageNumber).size();
  }

  /** The vector id and record of every entry written on the pages before {@code pageNumber}. */
  private Map<Integer, RID> entriesBeforePage(final LSMVectorIndex index, final int pageNumber) {
    final Map<Integer, RID> entries = new HashMap<>();
    LSMVectorIndexPageParser.parsePages((DatabaseInternal) database, index.getFileId(), index.getTotalPages(), index.getPageSize(),
        false, false, entry -> {
          if (entry.absoluteFileOffset / index.getPageSize() < pageNumber)
            entries.put(entry.vectorId, entry.rid);
        });
    return entries;
  }

  /** Writes into the header of a page the free offset of a page of another format. */
  private void damageHeaderOfPage(final LSMVectorIndex index, final int pageNumber) {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.transaction(() -> {
      try {
        db.getTransaction().getPageToModify(new PageId(db, index.getFileId(), pageNumber), index.getPageSize(), false)
            .writeInt(LSMVectorIndex.OFFSET_FREE_CONTENT, 0);
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  /**
   * Appends to the last page an entry of a record that is not deleted and holds no quantized vector: what an index written before
   * it was quantized leaves on its pages. The entry is known to the location index, as the entries a load reads are.
   */
  private void appendEntryOfAnotherEncoding(final LSMVectorIndex index, final int vectorId, final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    try {
      final int pageNumber = index.getTotalPages() - 1;
      final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, pageNumber < 0 ? 0 : index.getFileId(), pageNumber),
          index.getPageSize(), false);
      final int free = page.readInt(LSMVectorIndex.OFFSET_FREE_CONTENT);
      int written = 0;
      written += page.writeNumber(free + written, vectorId);
      written += page.writeNumber(free + written, rid.getBucketId());
      written += page.writeNumber(free + written, rid.getPosition());
      written += page.writeByte(free + written, (byte) 0);
      written += page.writeByte(free + written, (byte) VectorQuantizationType.NONE.ordinal());
      assertThat(free + written).as("the entry fits on the last page").isLessThanOrEqualTo(page.getMaxContentSize());
      page.writeInt(LSMVectorIndex.OFFSET_FREE_CONTENT, free + written);
      page.writeInt(LSMVectorIndex.OFFSET_NUM_ENTRIES, page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES) + 1);
      index.residentLocationsForTest().addOrUpdate(vectorId, false,
          (long) pageNumber * index.getPageSize() + BasePage.PAGE_HEADER_SIZE + free, rid, false);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The messages the vector index logs while {@code action} runs. */
  private static List<String> captureLog(final Runnable action) {
    // initializes the logging configuration first, which would otherwise reset the handler installed below
    LogManager.instance().log(VectorBuildDurabilityOracleTest.class, Level.FINE, "Capturing");
    final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(LSMVectorIndex.class.getName());
    final Level previousLevel = logger.getLevel();
    final List<String> messages = new CopyOnWriteArrayList<>();
    final Handler capture = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        messages.add(record.getMessage());
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    logger.setLevel(Level.INFO);
    logger.addHandler(capture);
    try {
      action.run();
    } finally {
      logger.removeHandler(capture);
      logger.setLevel(previousLevel);
    }
    return messages;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(final Throwable t) throws T {
    throw (T) t;
  }

  private static List<Float> asList(final float[] vector) {
    final List<Float> list = new ArrayList<>(vector.length);
    for (final float component : vector)
      list.add(component);
    return list;
  }

  private static float[] randomVector(final Random rnd, final int dimensions) {
    final float[] v = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  /** Every live record is the first answer to its own vector, found by a search that covers the whole corpus. */
  private static void assertEveryRowFindsItself(final LSMVectorIndex index, final Map<RID, float[]> live, final String where) {
    assertEveryRowFindsItself(index, live, Math.max(100, live.size()), where);
  }

  private static void assertEveryRowFindsItself(final LSMVectorIndex index, final Map<RID, float[]> live, final int beam,
      final String where) {
    final List<String> misses = new ArrayList<>();
    // a beam as wide as the corpus visits every vector: the answer is exact, and the first one is the record itself. A narrower beam
    // over random vectors of 1024 dimensions, where every other vector is about as far as any, now and then misses the vector itself
    for (final Map.Entry<RID, float[]> row : live.entrySet()) {
      final List<Pair<RID, Float>> answer = index.findNeighborsFromVector(row.getValue(), 1, beam);
      if (answer.size() != 1 || !answer.get(0).getFirst().equals(row.getKey()))
        misses.add(row.getKey() + " -> " + answer);
      if (misses.size() >= 5)
        break;
    }
    assertThat(misses).as("%s: records that do not find themselves", where).isEmpty();
  }
}
