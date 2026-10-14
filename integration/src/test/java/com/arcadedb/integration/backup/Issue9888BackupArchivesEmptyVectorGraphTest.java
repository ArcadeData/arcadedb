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
package com.arcadedb.integration.backup;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.format.FullBackupFormat;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9888, the backup counterpart of #9758: a full backup archived an LSM vector index's {@code .vecgraph} pages
 * but none of the plain side files that vouch for them - the {@code .vecgraphfp} manifest, the {@code .vecordmap}
 * ordinal map and the {@code .vecpq} product quantization are not registered components, so neither the page snapshot
 * window nor {@code FileManager.getFiles()} lists them. A restored database therefore held graph pages with no manifest
 * beside them, which the index reads as "persisted by a version older than #6106" and reuses on the node count alone:
 * an UNVERIFIED reuse, counted in {@code unverifiedGraphReuses}.
 * <p>
 * The archive now carries the component as an EMPTY entry, the state of a freshly created index, so the restored
 * database keeps it registered under the same file id and builds its own graph, with its own manifest, on first use.
 * <p>
 * Both backup paths are driven, because they enumerate their files separately. Since issue #9917 the snapshot path
 * archives an idle graph with its pages AND its side files, so there the graph is reused - verified, which is what this
 * test pins on both paths: never an unverified reuse.
 */
class Issue9888BackupArchivesEmptyVectorGraphTest {
  private static final String DATABASE_PATH = "target/databases/issue9888-backup-vector-graph";
  private static final String RESTORED_PATH = "target/databases/issue9888-backup-vector-graph-restored";
  private static final String BACKUP_FILE   = "target/issue9888-backup-vector-graph.zip";
  private static final int    DIMENSIONS    = 16;
  private static final int    VECTORS       = 400;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  @ParameterizedTest(name = "pageSnapshot={0}")
  @ValueSource(booleans = { true, false })
  void aRestoredVectorIndexBuildsItsOwnGraphInsteadOfReusingAnUnverifiedOne(final boolean pageSnapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createVectorIndex(database, "Doc", "NONE");
      createVectorIndex(database, "PqDoc", "PRODUCT");
      final DatabaseInternal db = (DatabaseInternal) database;
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      // THE FIXTURE: the path being tested really does enumerate a graph with pages in it
      if (pageSnapshot) {
        try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
          assertThat(snapshot.getFiles()).as("the window carries the built graphs, with pages in them")
              .filteredOn(f -> f.fileName().endsWith("." + LSMVectorIndexGraphFile.FILE_EXT)).hasSize(2)
              .allMatch(f -> f.size() > 0);
        }
      } else
        assertThat(new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT)))
            .as("both graphs are on disk with pages in them").hasSize(2).allMatch(f -> f.length() > 0);

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    final Map<String, Long> entries = entrySizes();
    final List<String> graphs = entries.keySet().stream().filter(n -> n.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT))
        .toList();
    assertThat(graphs).as("the graph components are still archived, so the restore keeps their file ids").hasSize(2);
    for (final String graph : graphs)
      if (pageSnapshot)
        // SINCE #9917 THE SNAPSHOT PATH ARCHIVES AN IDLE GRAPH WITH ITS PAGES AND THE SIDE FILES THAT VOUCH FOR THEM,
        // SO THE REUSE BELOW IS A VERIFIED ONE; Issue9917BackupReusesVectorGraphTest covers that path in full
        assertThat(entries.get(graph + ".vecgraphfp")).as("'%s' is archived with the manifest vouching for it", graph)
            .isNotNull();
      else
        assertThat(entries.get(graph)).as("'%s' is archived empty: the restored database builds its own graph", graph)
            .isZero();
    assertThat(entries.entrySet()).as("the vector index pages, where the vectors live, are still archived with their bytes")
        .anyMatch(e -> e.getKey().endsWith(".lsmvecidx") && e.getValue() > 0);

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      for (final String type : new String[] { "Doc", "PqDoc" }) {
        assertThat(restored.countType(type, true)).isEqualTo(VECTORS);
        assertThat(search(restored, type)).as("pageSnapshot=%s: %s answers a search after the restore", pageSnapshot, type)
            .isTrue();
        assertThat(vectorIndex(restored, type).getStats().get("unverifiedGraphReuses"))
            .as("pageSnapshot=%s: %s must not reuse a graph nothing in the restored database vouches for (#6106)",
                pageSnapshot, type).isEqualTo(0L);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  /** Matched by the final extension, so a bucket of a type merely named like it is data and keeps its bytes. */
  @Test
  void onlyTheVectorGraphComponentIsArchivedEmpty() {
    assertThat(FullBackupFormat.isArchivedEmpty("Doc_0_1_vecgraph.5.262144.v0." + LSMVectorIndexGraphFile.FILE_EXT))
        .as("the graph each database builds for itself").isTrue();
    assertThat(FullBackupFormat.isArchivedEmpty("Doc_0_1.4.262144.v0.lsmvecidx"))
        .as("the vector index's own pages carry the vectors").isFalse();
    assertThat(FullBackupFormat.isArchivedEmpty("vecgraph_0.1.65536.v0.bucket"))
        .as("a bucket of a type merely NAMED like the extension is data").isFalse();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static void createVectorIndex(final Database database, final String type, final String quantization) {
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
    vectorIndex(database, type).buildVectorGraphNow();
  }

  /**
   * The projection is evaluated once per row of the FROM target, so the query needs a target with exactly one row to
   * call {@code vector.neighbors} once: {@code schema:types LIMIT 1} is one that always exists, whatever the data.
   */
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

  private static Map<String, Long> entrySizes() throws Exception {
    final Map<String, Long> sizes = new HashMap<>();
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements()) {
        final ZipEntry entry = it.nextElement();
        sizes.put(entry.getName(), entry.getSize());
      }
    }
    return sizes;
  }
}
