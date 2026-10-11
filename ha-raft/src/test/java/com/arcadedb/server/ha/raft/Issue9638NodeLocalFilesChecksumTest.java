/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.ContractionHierarchy;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9638: {@code statistics.json} (and its pre-25.2.1 predecessor {@code cached-count.json}), a Graph Analytical
 * View's persisted CSR ({@code gav-<view>.csr}) and its contraction-hierarchy orders ({@code *.cchorder}) are written
 * per node - the GAV files only on the leader - and none of them is shipped by a snapshot, so the
 * {@code /api/v1/ha/snapshot/{database}/checksums} answer reported them as a data divergence on a healthy cluster.
 * {@code database.lck} was covered only because it happens to be empty.
 */
class Issue9638NodeLocalFilesChecksumTest {

  /** The reporter's probe, against the production method. */
  @Test
  void nodeLocalEngineSideFilesAreNotChecksummed(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("schema.json"), "{\"types\":[]}");
    Files.writeString(dir.resolve("Item_0.1.65536.v0.bucket"), "pages");
    Files.writeString(dir.resolve("statistics.json"), "{\"Item_0\":{\"count\":41},\":v\":2}");
    Files.writeString(dir.resolve("cached-count.json"), "{\"Item_0\":41}");
    Files.writeString(dir.resolve("gav-Routes.csr"), "GAVC-payload");
    Files.writeString(dir.resolve("gav-Routes.csr.cost.Road.cchorder"), "CCHO-payload");
    // NOT EMPTY ON PURPOSE: AN EMPTY FILE CRCS TO 0 ON EVERY NODE, WHICH IS HOW IT HID BEFORE
    Files.writeString(dir.resolve("database.lck"), "x");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dir.toFile());

    assertThat(checksums).containsOnlyKeys("schema.json", "Item_0.1.65536.v0.bucket");
  }

  /**
   * The skip is by shape, not by substring: a bucket whose type happens to be named {@code gav-...} or
   * {@code statistics...} is data, and a short map reads as agreement.
   */
  @Test
  void dataFilesThatMerelyLookLikeNodeLocalOnesAreStillChecksummed(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("gav-Routes_0.1.65536.v0.bucket"), "a bucket of a type named gav-Routes");
    Files.writeString(dir.resolve("statistics_0.2.65536.v0.bucket"), "a bucket of a type named statistics");
    Files.writeString(dir.resolve("Routes.csr"), "a user file without the gav- prefix");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dir.toFile());

    assertThat(checksums).containsOnlyKeys("gav-Routes_0.1.65536.v0.bucket", "statistics_0.2.65536.v0.bucket", "Routes.csr");
  }

  /**
   * The documented trade-off of matching by shape: a stray file that takes the engine's own shape is left out too. Pinned
   * so that narrowing or widening the shape is a decision, not an accident.
   */
  @Test
  void aStrayFileTakingTheGavShapeIsLeftOut(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("schema.json"), "{\"types\":[]}");
    Files.writeString(dir.resolve("gav-notes.csr"), "not written by the engine");

    assertThat(SnapshotManager.computeFileChecksums(dir.toFile())).containsOnlyKeys("schema.json");
  }

  /**
   * The issue's three-node scenario stated as the property: the leader closed cleanly last with a GAV persisted, one
   * follower closed an hour ago with older statistics, the other got its copy by snapshot install and has neither.
   */
  @Test
  void threeHealthyNodesAgreeWhateverTheirNodeLocalState(@TempDir final Path tempDir) throws Exception {
    final Path leader = Files.createDirectory(tempDir.resolve("leader"));
    final Path restarted = Files.createDirectory(tempDir.resolve("restarted"));
    final Path installed = Files.createDirectory(tempDir.resolve("installed"));

    for (final Path node : new Path[] { leader, restarted, installed }) {
      Files.writeString(node.resolve("schema.json"), "{\"types\":[]}");
      Files.writeString(node.resolve("configuration.json"), "{}");
      Files.writeString(node.resolve("Item_0.1.65536.v0.bucket"), "pages");
    }

    Files.writeString(leader.resolve("statistics.json"), "{\"Item_0\":{\"count\":41},\":v\":2}");
    Files.writeString(leader.resolve("gav-Routes.csr"), "GAVC-payload");
    Files.writeString(leader.resolve("gav-Routes.csr.cost.Road.cchorder"), "CCHO-payload");
    Files.writeString(leader.resolve("database.lck"), "x");
    Files.writeString(restarted.resolve("statistics.json"), "{\"Item_0\":{\"count\":12},\":v\":2}");
    Files.writeString(restarted.resolve("gav-Routes.csr"), "GAVC-stale-payload-from-when-it-was-leader");
    Files.writeString(restarted.resolve("cached-count.json"), "{\"Item_0\":3}");

    final Map<String, Long> expected = SnapshotManager.computeFileChecksums(leader.toFile());
    assertThat(SnapshotManager.computeFileChecksums(restarted.toFile())).isEqualTo(expected);
    assertThat(SnapshotManager.computeFileChecksums(installed.toFile())).isEqualTo(expected);
  }

  /**
   * The guard the issue asks for over the engine's side-file writers. Rather than another list of names, it drives a
   * REAL database through the writers - a graph, a Graph Analytical View with a contraction hierarchy, a clean close -
   * and requires every file the directory then holds to be either something a snapshot ships (and so something two
   * nodes must agree on) or something the checksum answer leaves out. A new engine side file that is neither fails
   * here, at the build that introduces it, instead of on an operator's diff of two healthy nodes.
   */
  @Test
  void everyFileARealDatabaseWritesIsEitherShippedOrLeftOutOfTheChecksums(@TempDir final Path tempDir) throws Exception {
    final String path = tempDir.resolve("db").toString();
    final List<String> seenWhileOpen = new ArrayList<>();

    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.getSchema().createVertexType("Junction").createProperty("i", Type.INTEGER);
        database.getSchema().createEdgeType("ROAD").createProperty("distance", Type.DOUBLE);
        database.transaction(() -> {
          final MutableVertex[] junctions = new MutableVertex[16];
          for (int i = 0; i < junctions.length; i++)
            junctions[i] = database.newVertex("Junction").set("i", i).save();
          for (int i = 0; i + 1 < junctions.length; i++) {
            junctions[i].newEdge("ROAD", junctions[i + 1]).set("distance", 1.0 + i).save();
            junctions[i + 1].newEdge("ROAD", junctions[i]).set("distance", 2.0 + i).save();
          }
        });

        final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("Routes").withEdgeTypes("ROAD")
            .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withContractionHierarchy("distance").build();
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
        final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
        assertThat(cch).isNotNull();
        assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();

        // #9751: LSM VECTOR INDEXES, THEIR GRAPHS BUILT, WRITE THE GRAPH COMPONENT AND THREE PLAIN SIDE FILES BESIDE IT -
        // ONE WITHOUT AND ONE WITH PRODUCT QUANTIZATION, SINCE ONLY THE FORMER WRITES AN ORDINAL MAP AND ONLY THE LATTER A PQ
        Issue9751VectorSideFilesChecksumTest.createVectorIndex(database, "Doc", "NONE");
        Issue9751VectorSideFilesChecksumTest.createVectorIndex(database, "PqDoc", "PRODUCT");

        // #9751: AND A TIMESERIES TYPE WITH A SEALED STORE, THE OTHER WRITER OF RAW FILES INTO THE DIRECTORY
        database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1");
        database.transaction(() -> {
          for (int i = 0; i < 100; i++)
            database.command("sql", "INSERT INTO Reading SET ts = ?, id = 's1', value = ?", 1_700_000_000_000L + i * 1000L,
                (double) i);
        });
        ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine().compactAll();

        for (final File file : listFiles(path))
          seenWhileOpen.add(file.getName());
        assertThat(seenWhileOpen).as("the open database holds its lock file, so the guard reaches it").contains("database.lck");
        assertUnshippedFilesAreLeftOut(path);
      } finally {
        database.close();
      }
    }

    final List<String> afterClose = new ArrayList<>();
    for (final File file : listFiles(path))
      afterClose.add(file.getName());
    // REACHABILITY: THE CLOSE MUST HAVE WRITTEN THE FILES THE ISSUE IS ABOUT, OR THIS TEST PROVES NOTHING
    assertThat(afterClose).as("the clean close writes the statistics file").contains(LocalSchema.STATISTICS_FILE_NAME);
    assertThat(afterClose).as("the clean close persists the view's CSR").contains("gav-Routes.csr");
    assertThat(afterClose).as("and the contraction hierarchy's order").anyMatch(n -> n.startsWith("gav-Routes.csr.") && n.endsWith(".cchorder"));
    assertThat(afterClose).as("the vector index persisted its graph").anyMatch(n -> n.endsWith(".vecgraph"));
    assertThat(afterClose).as("and its manifest").anyMatch(n -> n.endsWith(".vecgraphfp"));
    assertThat(afterClose).as("and its ordinal map").anyMatch(n -> n.endsWith(".vecordmap"));
    assertThat(afterClose).as("and its product quantization").anyMatch(n -> n.endsWith(".vecpq"));
    assertThat(afterClose).as("the TimeSeries compaction sealed a store").anyMatch(n -> n.endsWith(TimeSeriesSealedStore.FILE_EXTENSION));
    assertThat(afterClose).as("the clean close wrote the recency marker").contains(TransactionManager.LAST_TX_ID_FILE_NAME);

    assertUnshippedFilesAreLeftOut(path);
  }

  private static void assertUnshippedFilesAreLeftOut(final String path) throws Exception {
    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(new File(path));
    for (final File file : listFiles(path)) {
      final String name = file.getName();
      if (isShippedBySnapshot(name))
        continue;
      assertThat(checksums).as("'%s' is not shipped by a snapshot, so it is node-local and must not be in the checksum answer "
          + "- if it IS replicated content, ship it; if it is not, declare it in NodeLocalFiles", name).doesNotContainKey(name);
    }
    for (final String name : checksums.keySet())
      assertThat(isShippedBySnapshot(name)).as("'%s' is in the checksum answer but no snapshot ships it", name).isTrue();
  }

  /**
   * What {@code SnapshotHttpHandler} puts in the archive: the registered page files ({@code addPageFilesToZip}), the
   * sealed stores ({@code addSealedStoresToZip}), the two files of {@code addConfigurationToZip} and the synthesized
   * recency marker. A mirror of those methods, not a call into them: if the handler starts or stops shipping a file,
   * update this helper with it.
   */
  private static boolean isShippedBySnapshot(final String name) {
    return (LocalDatabase.isComponentFileName(name) && SnapshotHttpHandler.isShippedPageFile(name))
        || name.endsWith(TimeSeriesSealedStore.FILE_EXTENSION)
        || name.equals(LocalDatabase.CONFIGURATION_FILE_NAME)
        || name.equals(LocalSchema.SCHEMA_FILE_NAME)
        // SYNTHESIZED INTO THE ARCHIVE, BUT LEFT OUT OF THE CHECKSUMS ALL THE SAME: A LIVE NODE REWRITES IT ON ITS OWN WAL
        // ROTATIONS AND CLOSES, SO TWO HEALTHY PEERS HOLD DIFFERENT VALUES (#9751)
        || name.equals(TransactionManager.LAST_TX_ID_FILE_NAME);
  }

  private static File[] listFiles(final String path) {
    final File[] files = new File(path).listFiles(File::isFile);
    assertThat(files).isNotNull();
    return files;
  }
}
