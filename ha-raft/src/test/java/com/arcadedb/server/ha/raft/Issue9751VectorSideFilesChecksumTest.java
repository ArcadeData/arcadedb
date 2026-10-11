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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.index.vector.LSMVectorIndexGraphManifest;
import com.arcadedb.index.vector.LSMVectorIndexOrdinalMapFile;
import com.arcadedb.index.vector.LSMVectorIndexPQFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9751, the follow-up #9638's completeness sweep left: an LSM vector index writes three plain side files into
 * the database directory - the graph manifest ({@code .vecgraphfp}), the ordinal map ({@code .vecordmap}) and the
 * product quantization ({@code .vecpq}) - that no snapshot ships, so the {@code /checksums} answer carried keys only
 * the node that built its own graph has. The graph they describe ({@code .vecgraph}) is the deeper half: every node
 * builds it itself, on the inner instance and without the WAL, so its pages are node-local as well, and BOTH checksum
 * collectors - {@code /checksums} and the cluster verify - compared it. And {@code last-tx-id.bin}, the recency marker,
 * is rewritten whenever this node rotates its WAL or closes, so two healthy peers hold different values.
 */
class Issue9751VectorSideFilesChecksumTest {

  private static final int DIMENSIONS = 16;
  private static final int VECTORS    = 400;

  private final PostVerifyDatabaseHandler handler = new PostVerifyDatabaseHandler(null, null);

  @AfterEach
  void reset() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    handler.close();
  }

  /** The reporter's probe, against the production method, with the names the writers actually build. */
  @Test
  void vectorSideFilesAndTheRecencyMarkerAreNotChecksummed(@TempDir final Path dir) throws Exception {
    final String index = "Doc_0_123456789.4.262144.v0.lsmvecidx";
    final String graph = "Doc_0_123456789_vecgraph.5.262144.v0." + LSMVectorIndexGraphFile.FILE_EXT;
    Files.writeString(dir.resolve("schema.json"), "{\"types\":[]}");
    Files.writeString(dir.resolve(index), "vector pages");
    Files.writeString(dir.resolve(graph), "graph pages built by this node");
    Files.writeString(dir.resolve(graph + "." + LSMVectorIndexGraphManifest.FILE_EXT), "manifest");
    Files.writeString(dir.resolve(graph + "." + LSMVectorIndexOrdinalMapFile.FILE_EXT), "ordinal map");
    Files.writeString(dir.resolve(index + "." + LSMVectorIndexPQFile.FILE_EXT), "codebooks");
    // THE LEGACY PQ NAME createPQFileWithFallback STILL MIGRATES FROM: THE BASE NAME UP TO ITS FIRST DOT
    Files.writeString(dir.resolve("Doc_0_123456789." + LSMVectorIndexPQFile.FILE_EXT), "legacy codebooks");
    Files.writeString(dir.resolve(TransactionManager.LAST_TX_ID_FILE_NAME), "12345678");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dir.toFile());

    assertThat(checksums).containsOnlyKeys("schema.json", index);
  }

  /** By extension, not by substring: a type whose name merely contains a vector extension is still data. */
  @Test
  void dataFilesThatMerelyLookLikeVectorSideFilesAreStillChecksummed(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("vecgraph_0.1.65536.v0.bucket"), "a bucket of a type named vecgraph");
    Files.writeString(dir.resolve("vecpq_0.2.65536.v0.bucket"), "a bucket of a type named vecpq");
    Files.writeString(dir.resolve("vecordmap"), "a file named only the extension, with no base name");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dir.toFile());

    assertThat(checksums).containsOnlyKeys("vecgraph_0.1.65536.v0.bucket", "vecpq_0.2.65536.v0.bucket", "vecordmap");
  }

  /**
   * The issue's own scenario, as the property: one node built its graph and closed, the other installed a snapshot
   * and has not searched yet, so it has the shipped graph pages but none of the side files, and no recency marker of
   * its own. Both hold the same data, so they must agree.
   */
  @Test
  void aNodeThatBuiltItsGraphAndOneThatInstalledASnapshotAgree(@TempDir final Path tempDir) throws Exception {
    final Path builder = Files.createDirectory(tempDir.resolve("builder"));
    final Path installed = Files.createDirectory(tempDir.resolve("installed"));
    final String index = "Doc_0_1.4.262144.v0.lsmvecidx";
    final String graph = "Doc_0_1_vecgraph.5.262144.v0.vecgraph";

    for (final Path node : new Path[] { builder, installed }) {
      Files.writeString(node.resolve("schema.json"), "{\"types\":[]}");
      Files.writeString(node.resolve(index), "vector pages");
    }
    Files.writeString(builder.resolve(graph), "the graph this node built");
    Files.writeString(builder.resolve(graph + ".vecgraphfp"), "manifest");
    Files.writeString(builder.resolve(graph + ".vecordmap"), "ordinal map");
    Files.writeString(builder.resolve(index + ".vecpq"), "codebooks");
    Files.writeString(builder.resolve(TransactionManager.LAST_TX_ID_FILE_NAME), "rotated at 41");
    Files.writeString(installed.resolve(graph), "the graph the leader built, shipped and not yet replaced");
    Files.writeString(installed.resolve(TransactionManager.LAST_TX_ID_FILE_NAME), "rotated at 17");

    assertThat(SnapshotManager.computeFileChecksums(installed.toFile()))
        .isEqualTo(SnapshotManager.computeFileChecksums(builder.toFile()));
  }

  /**
   * Against a REAL vector index with product quantization: the build has to have written every file the issue names,
   * or the exclusion is untested, and the index's own pages - where the replicated vectors live - must still be
   * compared, or a node missing vectors would read as healthy.
   */
  @Test
  void aRealVectorIndexWritesItsSideFilesAndOnlyItsPagesAreChecksummed(@TempDir final Path tempDir) throws Exception {
    final String path = tempDir.resolve("db").toString();
    createVectorDatabase(path).close();

    final List<String> names = fileNames(path);
    assertThat(names).as("the build persisted its graph").anyMatch(n -> n.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT));
    assertThat(names).as("and its manifest").anyMatch(n -> n.endsWith("." + LSMVectorIndexGraphManifest.FILE_EXT));
    assertThat(names).as("and its ordinal map").anyMatch(n -> n.endsWith("." + LSMVectorIndexOrdinalMapFile.FILE_EXT));
    assertThat(names).as("and the product quantization").anyMatch(n -> n.endsWith("." + LSMVectorIndexPQFile.FILE_EXT));
    assertThat(names).as("the clean close wrote the recency marker").contains(TransactionManager.LAST_TX_ID_FILE_NAME);

    // THE PRODUCTION PREDICATE AGREES WITH THIS TEST'S INDEPENDENT ORACLE ON EVERY NAME THE WRITERS ACTUALLY PRODUCED
    for (final String name : names)
      assertThat(NodeLocalFiles.isVectorGraphFileName(name)).as("isVectorGraphFileName('%s')", name)
          .isEqualTo(isNodeLocalVectorFile(name));

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(new File(path));
    for (final String name : checksums.keySet())
      assertThat(isNodeLocalVectorFile(name) || name.equals(TransactionManager.LAST_TX_ID_FILE_NAME))
          .as("'%s' is node-local and must not be in the checksum answer", name).isFalse();
    assertThat(checksums.keySet()).as("the vector index's own pages are replicated content and still compared")
        .anyMatch(n -> n.endsWith(".lsmvecidx"));
  }

  /**
   * The cluster verify enumerates REGISTERED files rather than the directory, so the three plain side files never
   * reached it - but the {@code .vecgraph} component is registered, and both of its paths compared it.
   */
  @Test
  void theClusterVerifyLeavesTheVectorGraphOutOnBothPaths(@TempDir final Path tempDir) {
    try (final Database database = createVectorDatabase(tempDir.resolve("db").toString())) {
      final DatabaseInternal db = (DatabaseInternal) database;
      assertThat(db.getFileManager().getFiles().stream()
          .anyMatch(f -> f != null && LSMVectorIndexGraphFile.FILE_EXT.equals(f.getFileExtension())))
          .as("the graph is a registered component, so the verify reaches it").isTrue();

      for (final boolean window : new boolean[] { true, false }) {
        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(window);
        final JSONObject checksums = new JSONObject();
        handler.computeLocalChecksums(db, checksums, new JSONArray());

        assertThat(checksums.keySet()).as("window=%s: the node-locally built graph is not compared", window)
            .noneMatch(Issue9751VectorSideFilesChecksumTest::isNodeLocalVectorFile);
        assertThat(checksums.keySet()).as("window=%s: the vector pages still are", window)
            .anyMatch(n -> n.endsWith(".lsmvecidx"));
      }
    }
  }

  /**
   * Two indexes, because no single one writes every side file: PRODUCT quantization writes the {@code .vecpq} and, by
   * design, no {@code .vecordmap} (see {@code LSMVectorIndex.writeGraphManifest}); every other quantization writes the
   * map and no PQ.
   */
  private static Database createVectorDatabase(final String path) {
    final Database database = new DatabaseFactory(path).create();
    createVectorIndex(database, "Doc", "NONE");
    createVectorIndex(database, "PqDoc", "PRODUCT");
    return database;
  }

  static void createVectorIndex(final Database database, final String type, final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    });
    database.command("sql", "CREATE INDEX ON `" + type + "` (`embedding`) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
        + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quantization + "\"}");
    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = (float) Math.sin(i * 0.37 + j * 1.13) + 1.5f;
        database.newDocument(type).set("embedding", vector).save();
      }
    });
    ((LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[embedding]")).getIndexesOnBuckets()[0])
        .buildVectorGraphNow();
  }

  private static boolean isNodeLocalVectorFile(final String name) {
    return name.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT) || name.endsWith("." + LSMVectorIndexGraphManifest.FILE_EXT)
        || name.endsWith("." + LSMVectorIndexOrdinalMapFile.FILE_EXT) || name.endsWith("." + LSMVectorIndexPQFile.FILE_EXT);
  }

  private static List<String> fileNames(final String path) {
    final File[] files = new File(path).listFiles(File::isFile);
    assertThat(files).isNotNull();
    final List<String> names = new ArrayList<>();
    for (final File file : files)
      names.add(file.getName());
    return names;
  }
}
