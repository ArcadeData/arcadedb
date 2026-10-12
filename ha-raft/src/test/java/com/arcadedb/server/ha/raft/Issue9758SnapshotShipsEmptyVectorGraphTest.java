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
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
class Issue9758SnapshotShipsEmptyVectorGraphTest {

  private static final int DIMENSIONS = 16;

  @AfterEach
  void reset() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
  }

  @Test
  void theWindowPathShipsTheVectorGraphEmpty(@TempDir final Path tempDir) throws Exception {
    shipInstallAndSearch(tempDir, true);
  }

  @Test
  void theFrozenFilesPathShipsTheVectorGraphEmpty(@TempDir final Path tempDir) throws Exception {
    shipInstallAndSearch(tempDir, false);
  }

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
      Issue9751VectorSideFilesChecksumTest.createVectorIndex(leader, "Doc", "NONE");
      Issue9751VectorSideFilesChecksumTest.createVectorIndex(leader, "PqDoc", "PRODUCT");
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
