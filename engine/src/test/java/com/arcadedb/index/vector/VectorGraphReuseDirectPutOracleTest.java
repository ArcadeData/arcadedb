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

import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle tests of what a reopened index does with the graph it persisted: reuse it when it still describes the live set,
 * reuse it as a prefix when the live set only grew, and rebuild it otherwise. Every scenario changes what one of the checks
 * of {@code loadPersistedGraphOrDecidePrefix} compares while the database is closed, reopens it through the real API, and
 * holds the answers to brute-force exact search over a model of the live set; the counters say which way the reopen went.
 */
@Tag("vector")
class VectorGraphReuseDirectPutOracleTest extends VectorReuseOracleFixture {
  private static final int LIVE = 300;
  /** Large enough that the live set stays at or above the async threshold of 1000 after a few deletions. */
  private static final int LARGE = 1020;

  private void persistedGraph(final String quant, final int rows) {
    createIndex(quant);
    insertRows(rows);
    index().buildVectorGraphNow();
    assertThat(stat("persistedGraphNodeCount")).as("the graph the scenario starts from is on disk").isEqualTo(rows);
  }

  /** Closes the database, lets {@code tamper} change its files, and opens it again. */
  private void reopenAfter(final FileTamper tamper) throws IOException {
    database.close();
    tamper.run();
    database = factory.open();
  }

  private interface FileTamper {
    void run() throws IOException;
  }

  private Path manifestPath() {
    return index().getGraphFile().getManifest().getFilePath();
  }

  private Path ordinalMapPath() {
    return index().getGraphFile().getManifest().getOrdinalMapFilePath();
  }

  private void assertReusedAsItWas(final String where, final int rows) {
    assertMatchesModel(where, 5, 4);
    assertThat(stat("graphRebuildCount")).as("%s: the persisted graph describes the live set, no rebuild", where).isZero();
    assertThat(stat("graphNodeCount")).as("%s: the graph in use is the persisted one", where).isEqualTo(rows);
    assertThat(stat("graphState")).as("%s: IMMUTABLE, nothing is pending", where).isEqualTo(1L);
  }

  private void assertRebuilt(final String where) {
    assertMatchesModel(where, 5, 4);
    assertThat(stat("graphRebuildCount")).as("%s: the persisted graph is not usable, so it is rebuilt once", where).isEqualTo(1L);
    assertThat(stat("stalePrefixGraphReuses")).as("%s: and not reused as a prefix", where).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphThatStillDescribesTheLiveSetIsLoadedThroughItsOrdinalMapAndNeverRebuilt(final String quant) throws IOException {
    persistedGraph(quant, LIVE);
    final LSMVectorIndex before = index();
    assertThat(before.getGraphFile().getManifest().read().vectorCount()).isEqualTo(LIVE);

    reopenAfter(() -> {
    });

    assertThat(stat("graphNodeCount")).as("lazy: nothing is loaded before the first search").isZero();
    assertThat(stat("persistedGraphNodeCount")).as("but the manifest already says what is on disk").isEqualTo(LIVE);
    assertThat(index().graphNotYetMaterialisedForTest()).isTrue();
    assertReusedAsItWas("reopened", LIVE);
    assertThat(stat("graphLoadsFromOrdinalMap")).as("the fast path: no vector document read").isEqualTo(1L);
    assertThat(stat("unverifiedGraphReuses")).isZero();
    assertThat(index().graphNotYetMaterialisedForTest()).as("resolved by the first search").isFalse();
    assertThat(stat("graphLoadsFromOrdinalMap")).as("and not resolved again by the searches after it").isEqualTo(1L);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphWhoseOrdinalMapIsGoneIsValidatedAgainstTheLiveVectorsAndStillReused(final String quant) throws IOException {
    persistedGraph(quant, LIVE);
    final Path map = ordinalMapPath();
    assertThat(map).exists();

    reopenAfter(() -> Files.delete(map));

    assertReusedAsItWas("no ordinal map", LIVE);
    assertThat(stat("graphLoadsFromOrdinalMap")).as("the walk, not the ordinal map, vouched for it").isZero();
    assertThat(stat("unverifiedGraphReuses")).as("the manifest was there to vouch for it").isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphWithNeitherManifestNorMapIsJudgedByItsNodeCountAndCounted(final String quant) throws IOException {
    persistedGraph(quant, LIVE);
    final Path manifest = manifestPath();
    final Path map = ordinalMapPath();

    reopenAfter(() -> {
      Files.delete(manifest);
      Files.delete(map);
    });

    assertThat(stat("persistedGraphNodeCount")).as("no manifest, so no statement about the pages").isZero();
    assertReusedAsItWas("no manifest", LIVE);
    assertThat(stat("unverifiedGraphReuses")).as("reused, and said to be on the weaker comparison").isEqualTo(1L);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphWhoseManifestRefusesItsPagesIsRebuilt(final String quant) throws IOException {
    persistedGraph(quant, LIVE);
    final LSMVectorIndexGraphManifest manifest = index().getGraphFile().getManifest();

    reopenAfter(() -> manifest.markUnusable("a persist that failed"));

    assertThat(stat("persistedGraphNodeCount")).isZero();
    assertRebuilt("unusable manifest");
    assertThat(stat("unverifiedGraphReuses")).isZero();
  }

  /**
   * The pages are there and the manifest vouches for them, but JVector cannot read the graph out of them: the load throws,
   * the reopen takes it as "no graph" and the search rebuilds instead of failing.
   */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphWhosePagesCannotBeReadIsRebuiltInsteadOfFailingTheSearch(final String quant) throws IOException {
    persistedGraph(quant, LIVE);
    final File file = index().getGraphFile().getOSFile();
    final int pageSize = index().getGraphFile().getPageSize();

    reopenAfter(() -> {
      assertThat(file.length()).isGreaterThanOrEqualTo(pageSize);
      // Every payload byte of every page, the footer JVector validates the graph by included, and not a page header
      try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        final byte[] garbage = new byte[pageSize - BasePage.PAGE_HEADER_SIZE];
        Arrays.fill(garbage, (byte) 0x5A);
        for (long page = 0; page < file.length() / pageSize; page++) {
          raf.seek(page * pageSize + BasePage.PAGE_HEADER_SIZE);
          raf.write(garbage);
        }
      }
    });

    assertRebuilt("unreadable pages");
  }

  /**
   * NONE only: its walk reads the vector documents. A record the index holds a vector of but whose document has none is a
   * vector the walk does not count, so the graph the manifest certified over it describes a live set the walk does not
   * reproduce.
   */
  @Test
  void aVectorWhoseDocumentHoldsNoVectorMakesTheWalkRefuseTheGraphThatCoversIt() throws IOException {
    createIndex("NONE");
    insertRows(LIVE);
    final RID[] bareHolder = new RID[1];
    database.transaction(() -> bareHolder[0] = database.newDocument("Doc").set("id", -1).save().getIdentity());
    final RID bare = bareHolder[0];
    final float[] bareVector = randomVector();
    database.transaction(() -> index().put(new Object[] { bareVector }, new RID[] { bare }));
    model.put(bare, bareVector);
    index().buildVectorGraphNow();
    assertThat(index().getGraphFile().getManifest().read().vectorCount()).as("the graph covers the vector of the bare record")
        .isEqualTo(LIVE + 1);
    final Path map = ordinalMapPath();

    reopenAfter(() -> Files.delete(map));

    // The walk drops the bare record, so 300 vectors are not the 301 the manifest fingerprints: the graph is rebuilt, and
    // the rebuild does not carry the record either, so the live set the answers are held to is the 300 records with a vector
    assertThat(stat("totalVectors")).isEqualTo(LIVE + 1);
    model.remove(bare);
    assertMatchesModel("bare record", 4, 4);
    assertThat(stat("graphRebuildCount")).isEqualTo(1L);
    assertThat(stat("unverifiedGraphReuses")).isZero();
  }

  /** NONE only, same walk: a record deleted underneath the index (no index maintenance) is not a vector the walk counts. */
  @Test
  void aVectorWhoseDocumentIsGoneMakesTheWalkRefuseTheGraphThatCoversIt() throws IOException {
    persistedGraph("NONE", LIVE);
    final RID ghost = model.keySet().iterator().next();
    database.transaction(() -> deleteRecordAtLowLevel(database, ghost));
    model.remove(ghost);
    final Path map = ordinalMapPath();

    reopenAfter(() -> Files.delete(map));

    assertMatchesModel("ghost", 4, 4);
    assertThat(stat("graphRebuildCount")).as("the graph was built over a record that is not there any more").isEqualTo(1L);
  }

  private List<RID> deleteRows(final int count) {
    final List<RID> gone = new ArrayList<>(count);
    final List<RID> live = new ArrayList<>(model.keySet());
    // spread over the live set, so the dead ordinals are neither a prefix nor a suffix of the graph
    for (int i = 0; i < count; i++)
      gone.add(live.get(i * (live.size() / count)));
    database.transaction(() -> {
      for (final RID rid : gone)
        database.lookupByRID(rid, true).asDocument(true).delete();
    });
    gone.forEach(model::remove);
    return gone;
  }

  private void assertNoneOfThemAnswers(final List<RID> gone, final Map<RID, float[]> vectorsBefore) {
    for (final RID rid : gone)
      assertThat(search(vectorsBefore.get(rid), 5)).as("the vector of the deleted %s", rid)
          .extracting(p -> p.getFirst()).doesNotContain(rid);
  }

  /**
   * More vectors written after the persist than the 16 the gap buffer starts with: the graph is reused as a prefix, and every
   * vector past it is read as the gap.
   */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphBehindTheLiveSetByManyVectorsIsReusedAsAPrefixAndTheGapIsFound(final String quant) throws IOException {
    persistedGraph(quant, LARGE);
    final List<RID> added = insertRows(40);
    assertThat(stat("graphState")).as("the writes are pending").isEqualTo(2L);

    reopenAfter(() -> {
    });

    assertThat(stat("closeTimeRebuildPending")).as("a close over the async threshold defers the build").isEqualTo(1L);
    assertThat(stat("persistedGraphNodeCount")).as("so the graph on disk is the old one").isEqualTo(LARGE);
    for (final RID rid : added)
      assertThat(search(model.get(rid), 1)).as("the vector %s written after the persist", rid).extracting(p -> p.getFirst())
          .containsExactly(rid);
    assertMatchesModel("prefix reuse", 7, 4);
    assertThat(stat("stalePrefixGraphReuses")).as("the old graph was reused as a prefix").isEqualTo(1L);
    assertThat(stat("graphRebuildCount")).as("and not rebuilt before the answer").isZero();
    assertThat(stat("graphNodeCount")).as("the graph in use is the persisted one").isEqualTo(LARGE);
    assertThat(stat("graphState")).as("MUTABLE: it is behind the live set").isEqualTo(2L);
    assertThat(stat("deltaVectorsCount")).as("the gap is served from the delta buffer, with the nodes the old build left unreachable")
        .isEqualTo(40L + orphans());
  }

  /** Deletions and later writes together: the ordinal map says which ordinals are dead, and the gap is read past it. */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphWithDeadNodesAndManyLaterVectorsIsReusedAsAPrefix(final String quant) throws IOException {
    persistedGraph(quant, LARGE);
    final Map<RID, float[]> before = new HashMap<>(model);
    final List<RID> gone = deleteRows(7);
    final List<RID> added = insertRows(40);

    reopenAfter(() -> {
    });

    assertThat(stat("deletedVectors")).isEqualTo(7L);
    for (final RID rid : added)
      assertThat(search(model.get(rid), 1)).as("the vector %s written after the persist", rid).extracting(p -> p.getFirst())
          .containsExactly(rid);
    assertMatchesModel("prefix reuse with tombstones", 7, 4);
    assertNoneOfThemAnswers(gone, before);
    assertThat(stat("stalePrefixGraphReuses")).isEqualTo(1L);
    assertThat(stat("graphRebuildCount")).isZero();
    assertThat(stat("activeVectors")).isEqualTo(LARGE - 7 + 40);
  }

  /** Deletions only: every live vector is in the graph, which keeps its dead nodes until a rebuild folds them out. */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aGraphWithDeadNodesIsReusedAsItIsAndNeverAnswersForThem(final String quant) throws IOException {
    persistedGraph(quant, LARGE);
    final Map<RID, float[]> before = new HashMap<>(model);
    final List<RID> gone = deleteRows(7);

    reopenAfter(() -> {
    });

    assertMatchesModel("tombstoned reuse", 7, 4);
    assertNoneOfThemAnswers(gone, before);
    assertThat(stat("graphReusesWithTombstonedNodes")).isEqualTo(1L);
    assertThat(stat("graphRebuildCount")).isZero();
    assertThat(stat("graphNodeCount")).as("the graph still holds the dead nodes").isEqualTo(LARGE);
    assertThat(stat("mutationsSinceRebuild")).as("and owes them to the next rebuild").isEqualTo(7L);
    assertThat(stat("graphState")).isEqualTo(2L);
  }

  /** With tombstones the ordinal map is what says which node is which; without it the graph cannot be told apart. */
  @Test
  void aGraphWithDeadNodesAndNoOrdinalMapIsRebuilt() throws IOException {
    persistedGraph("NONE", LARGE);
    final Map<RID, float[]> before = new HashMap<>(model);
    final Path map = ordinalMapPath();
    final List<RID> gone = deleteRows(7);

    reopenAfter(() -> Files.delete(map));

    assertMatchesModel("tombstones, no map", 7, 4);
    assertNoneOfThemAnswers(gone, before);
    assertThat(stat("graphRebuildCount")).isEqualTo(1L);
    assertThat(stat("graphReusesWithTombstonedNodes")).isZero();
  }

  private long graphFileCount() {
    final File[] found = new File(database.getDatabasePath())
        .listFiles((dir, name) -> name.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT));
    return found == null ? 0 : found.length;
  }

  /**
   * A session that opens an index whose graph is not on disk and never touches it still has the graph built by its close: the
   * index holds vectors, which its page count says without materialising anything. The graph file is then created, found by the
   * next open, and reused.
   */
  @Test
  void aSessionThatNeverTouchedAnIndexWithoutAGraphOnDiskLeavesOneBehind() throws IOException {
    persistedGraph("NONE", LIVE);
    final LSMVectorIndexGraphFile gf = index().getGraphFile();
    final List<Path> graphFiles = List.of(gf.getOSFile().toPath(), gf.getManifest().getFilePath(),
        gf.getManifest().getOrdinalMapFilePath());

    reopenAfter(() -> {
      for (final Path file : graphFiles)
        Files.delete(file);
    });
    // Nothing is asked of the index in this session, so its locations stay unread: only the page counters answer
    reopenAfter(() -> {
    });

    assertThat(stat("persistedGraphNodeCount")).as("the close of the session that never touched it built the graph")
        .isEqualTo(LIVE);
    assertThat(graphFileCount()).as("into one file, the one the open found").isEqualTo(1L);
    assertReusedAsItWas("built by the close", LIVE);
    assertThat(graphFileCount()).as("the search that reused it created no second file").isEqualTo(1L);
  }

  /** The other half: an index that holds no vector is not given a graph by a close that never touched it. */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aSessionThatNeverTouchedAnEmptyIndexBuildsNothing(final String quant) throws IOException {
    createIndex(quant);
    assertThat(stat("graphRebuildCount")).as("a new index builds nothing at creation").isZero();
    assertThat(stat("graphState")).as("and is not waiting for a graph").isEqualTo(1L);
    assertThat(stat("persistedGraphNodeCount")).isZero();
    final long filesBefore = graphFileCount();

    reopenAfter(() -> {
    });
    reopenAfter(() -> {
    });

    assertThat(stat("persistedGraphNodeCount")).isZero();
    assertThat(index().getGraphFile().hasPersistedGraph()).as("no graph page was written for no vector").isFalse();
    assertThat(index().getGraphFile().getManifest().exists()).as("and no manifest says otherwise").isFalse();
    assertThat(graphFileCount()).isEqualTo(filesBefore);
    assertThat(search(randomVector(), 5)).as("an empty index answers nothing").isEmpty();
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }
}
