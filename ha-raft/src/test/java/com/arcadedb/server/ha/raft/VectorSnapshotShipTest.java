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
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.index.vector.LSMVectorIndexGraphManifest;
import com.arcadedb.index.vector.LSMVectorIndexOrdinalMapFile;
import com.arcadedb.index.vector.LSMVectorIndexPQFile;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vector index files in the HA snapshot ship: side-file checksums and an empty graph.
 */
class VectorSnapshotShipTest {

  /**
   * Issue #9751, the follow-up #9638's completeness sweep left: an LSM vector index writes three plain side files into
   * the database directory - the graph manifest ({@code .vecgraphfp}), the ordinal map ({@code .vecordmap}) and the
   * product quantization ({@code .vecpq}) - that no snapshot ships, so the {@code /checksums} answer carried keys only
   * the node that built its own graph has. The graph they describe ({@code .vecgraph}) is the deeper half: every node
   * builds it itself, on the inner instance and without the WAL, so its pages are node-local as well, and BOTH checksum
   * collectors - {@code /checksums} and the cluster verify - compared it. And {@code last-tx-id.bin}, the recency marker,
   * is rewritten whenever this node rotates its WAL or closes, so two healthy peers hold different values.
   */
  @Nested
  class SideFilesChecksum {

    private static final int DIMENSIONS = 16;
    private static final int VECTORS    = 400;

    private final PostVerifyDatabaseHandler handler = new PostVerifyDatabaseHandler(null, null);

    @AfterEach
    void reset() {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      handler.close();
    }

    /** The reporter's probe, against the production method, with the names the writers actually build. */
    // Regression for #9751: vector side files and the recency marker are not checksummed
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
    // Regression for #9751: data files that merely look like vector side files are still checksummed
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
    // Regression for #9751: a node that built its graph and one that installed a snapshot agree
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
    // Regression for #9751: a real vector index writes its side files and only its pages are checksummed
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
    // Regression for #9751: the cluster verify leaves the vector graph out on both paths
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
              .noneMatch(SideFilesChecksum::isNodeLocalVectorFile);
          assertThat(checksums.keySet()).as("window=%s: the vector pages still are", window)
              .anyMatch(n -> n.endsWith(".lsmvecidx"));
        }
      }
    }

    /**
     * Two indexes, because no single one writes every side file: PRODUCT quantization writes the {@code .vecpq} and, like
     * every other quantization, the {@code .vecordmap}; only it writes a PQ.
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

    static boolean isNodeLocalVectorFile(final String name) {
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

  /**
   * Issue #9758: an HA snapshot shipped an LSM vector index's {@code .vecgraph} pages but none of the plain side files
   * that vouch for them - the {@code .vecgraphfp} manifest, the {@code .vecordmap} ordinal map, the {@code .vecpq} product
   * quantization. A follower installing it held graph pages with no manifest, which the index reads as "persisted by a
   * version older than #6106" and reuses on the node count alone: an UNVERIFIED reuse, reached by a healthy cluster.
   * <p>
   * The graph is node-local (#8292, #8905, #9751): every node builds its own. The ship now carries the component as an
   * EMPTY file - the state a freshly created index has on every node - so the follower keeps the leader's file id
   * registered and builds its own graph, with its own manifest, on first use.
   * <p>
   * Both ship paths are driven, the point-in-time window and the frozen-files fallback, and the archive is installed the
   * way the follower sees it: extracted into a directory and opened as a database.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/9758">issue #9758</a>
   */
  @Nested
  class EmptyGraphShip {

    private static final int DIMENSIONS = 16;

    @AfterEach
    void reset() {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    }

    // Regression for #9758: the window path ships the vector graph empty
    @Test
    void theWindowPathShipsTheVectorGraphEmpty(@TempDir final Path tempDir) throws Exception {
      shipInstallAndSearch(tempDir, true);
    }

    // Regression for #9758: the frozen files path ships the vector graph empty
    @Test
    void theFrozenFilesPathShipsTheVectorGraphEmpty(@TempDir final Path tempDir) throws Exception {
      shipInstallAndSearch(tempDir, false);
    }

    // Regression for #9758: only the vector graph component is shipped empty
    @Test
    void onlyTheVectorGraphComponentIsShippedEmpty() {
      assertThat(SnapshotHttpHandler.isShippedEmpty("Doc_0_1_vecgraph.5.262144.v0." + LSMVectorIndexGraphFile.FILE_EXT))
          .as("the node-locally built graph").isTrue();
      assertThat(SnapshotHttpHandler.isShippedEmpty("Doc_0_1.4.262144.v0.lsmvecidx"))
          .as("the vector index's own pages carry the replicated vectors").isFalse();
      assertThat(SnapshotHttpHandler.isShippedEmpty("vecgraph_0.1.65536.v0.bucket"))
          .as("a bucket of a type merely NAMED like the extension is data").isFalse();
    }

    private static void shipInstallAndSearch(final Path tempDir, final boolean window) throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(window);
      final Path leaderPath = tempDir.resolve("leader");
      final Path followerPath = tempDir.resolve("follower");

      final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
      final Map<String, byte[]> entries;
      final long announced;
      try (final Database leader = new DatabaseFactory(leaderPath.toString()).create()) {
        SideFilesChecksum.createVectorIndex(leader, "Doc", "NONE");
        SideFilesChecksum.createVectorIndex(leader, "PqDoc", "PRODUCT");
        final DatabaseInternal db = (DatabaseInternal) leader;
        db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

        try (final PageSnapshot snapshot = window ? db.getPageManager().openSnapshot(db) : null) {
          if (window)
            assertThat(snapshot.getFiles()).as("the fixture: the window carries the built graph, with pages in it")
                .anyMatch(f -> f.fileName().endsWith("." + LSMVectorIndexGraphFile.FILE_EXT) && f.size() > 0);
          else
            assertThat(leaderPath.toFile().listFiles((dir, name) -> name.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT)))
                .as("the fixture: both graphs are on disk with pages in it").hasSize(2).allMatch(f -> f.length() > 0);

          entries = archive(db, snapshot, manifest);
          announced = SnapshotHttpHandler.estimateUncompressedBytes(db, snapshot, List.of());
        }
      }

      // INSTALL: WHAT THE FOLLOWER'S SWAP LEAVES IN THE LIVE DIRECTORY - THE ARCHIVE'S ENTRIES AND NOTHING ELSE
      Files.createDirectories(followerPath);
      for (final Map.Entry<String, byte[]> entry : entries.entrySet())
        Files.write(followerPath.resolve(entry.getKey()), entry.getValue());

      try (final Database follower = new DatabaseFactory(followerPath.toString()).open()) {
        for (final String type : new String[] { "Doc", "PqDoc" }) {
          assertThat(search(follower, type)).as("window=%s: %s answers a search after the install", window, type).isTrue();
          final Map<String, Long> stats = vectorIndex(follower, type).getStats();
          assertThat(stats.get("unverifiedGraphReuses"))
              .as("window=%s: %s must not reuse a graph nothing on the follower vouches for (#6106)", window, type)
              .isEqualTo(0L);
        }
      }

      final List<String> graphs = entries.keySet().stream().filter(n -> n.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT))
          .toList();
      assertThat(graphs).as("the graph components are still shipped, so the follower keeps their file ids").hasSize(2);
      for (final String graph : graphs)
        assertThat(entries.get(graph)).as("'%s' ships empty: the follower builds its own graph", graph).isEmpty();
      assertThat(entries.keySet()).as("the vector index pages, where the replicated vectors live, are still shipped")
          .anyMatch(n -> n.endsWith(".lsmvecidx"));

      long written = 0L;
      for (final SnapshotManager.ManifestEntry entry : manifest)
        written += entry.size();
      assertThat(announced).as("the size announced to the follower's space check (#7037) describes the archive sent")
          .isEqualTo(written + Long.BYTES);
    }

    private static boolean search(final Database database, final String type) {
      final float[] query = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        query[j] = (float) Math.sin(j * 1.13) + 1.5f;
      final boolean[] found = { false };
      database.transaction(() -> {
        final ResultSet rs = database.query("sql",
            "SELECT `vector.neighbors`('" + type + "[embedding]', ?, 5) AS neighbors FROM schema:types LIMIT 1", (Object) query);
        final List<Map<String, Object>> neighbors = rs.hasNext() ? rs.next().getProperty("neighbors") : null;
        found[0] = neighbors != null && !neighbors.isEmpty();
      });
      return found[0];
    }

    private static LSMVectorIndex vectorIndex(final Database database, final String type) {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[embedding]")).getIndexesOnBuckets()[0];
    }

    /** The configuration and page-file steps of the ship, in the order the handler runs them, into an in-memory ZIP. */
    private static Map<String, byte[]> archive(final DatabaseInternal db, final PageSnapshot snapshot,
        final List<SnapshotManager.ManifestEntry> manifest) throws Exception {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final ZipOutputStream zipOut = new ZipOutputStream(bytes)) {
        SnapshotHttpHandler.addConfigurationToZip(zipOut, db, snapshot, manifest);
        SnapshotHttpHandler.addPageFilesToZip(zipOut, db, snapshot, manifest);
        zipOut.finish();
      }
      final Map<String, byte[]> entries = new HashMap<>();
      try (final ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry())
          entries.put(entry.getName(), zipIn.readAllBytes());
      }
      assertThat(manifest).extracting(SnapshotManager.ManifestEntry::name)
          .as("the manifest the follower verifies describes exactly the entries written (#4831)")
          .containsExactlyInAnyOrderElementsOf(entries.keySet());
      return entries;
    }
  }
}
