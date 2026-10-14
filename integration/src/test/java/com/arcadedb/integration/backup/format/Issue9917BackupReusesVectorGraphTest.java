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
package com.arcadedb.integration.backup.format;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.index.vector.LSMVectorIndexGraphManifest;
import com.arcadedb.index.vector.LSMVectorIndexOrdinalMapFile;
import com.arcadedb.index.vector.LSMVectorIndexPQFile;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9917, the follow-up to #9888: a full backup archives an LSM vector index's graph WITH its pages and its side
 * files - the {@code .vecgraphfp} manifest, the {@code .vecordmap} ordinal map and the {@code .vecpq} product
 * quantization - whenever those side files are proved unchanged across the snapshot window's t0, so a restored database
 * verifies and reuses the graph instead of rebuilding it on its first search. When the proof fails - a graph persist
 * ran between the observation and the copy - the graph is archived empty, as #9888 does, and the restore rebuilds it.
 */
class Issue9917BackupReusesVectorGraphTest {
  private static final String DATABASE_PATH = "target/databases/issue9917-backup-vector-graph";
  private static final String RESTORED_PATH = "target/databases/issue9917-backup-vector-graph-restored";
  private static final String BACKUP_FILE   = "target/issue9917-backup-vector-graph.zip";
  private static final int    DIMENSIONS    = 16;
  private static final int    VECTORS       = 400;
  private static final String GRAPH_EXT     = "." + LSMVectorIndexGraphFile.FILE_EXT;

  @BeforeEach
  @AfterEach
  void clean() {
    FullBackupFormat.beforeVectorGraphSideFilesOpenForTesting = null;
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  @Test
  void aRestoreOfAnIdleDatabaseReusesTheGraphInsteadOfRebuildingIt() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createVectorIndex(database, "Doc", "NONE");
      createVectorIndex(database, "PqDoc", "PRODUCT");
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      TestHelper.checkActiveDatabases();
    }

    final Map<String, Long> entries = entrySizes();
    final List<String> graphs = entries.keySet().stream().filter(n -> n.endsWith(GRAPH_EXT)).toList();
    assertThat(graphs).hasSize(2);
    for (final String graph : graphs) {
      assertThat(entries.get(graph)).as("'%s' is archived with its pages", graph).isGreaterThan(0L);
      assertThat(entries.get(graph + "." + LSMVectorIndexGraphManifest.FILE_EXT)).as("and with the manifest vouching for them")
          .isNotNull().isGreaterThan(0L);
    }
    assertThat(entries.keySet()).as("the ordinal map of the non-quantized index")
        .anyMatch(n -> n.startsWith("Doc_") && n.endsWith("." + LSMVectorIndexOrdinalMapFile.FILE_EXT));
    assertThat(entries.keySet()).as("the PQ of the PRODUCT-quantized index")
        .anyMatch(n -> n.startsWith("PqDoc_") && n.endsWith("." + LSMVectorIndexPQFile.FILE_EXT));

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      for (final String type : new String[] { "Doc", "PqDoc" }) {
        assertThat(restored.countType(type, true)).isEqualTo(VECTORS);
        assertThat(search(restored, type)).as("%s answers a search after the restore", type).isTrue();
        final Map<String, Long> stats = vectorIndex(restored, type).getStats();
        assertThat(stats.get("unverifiedGraphReuses")).as("%s: the reuse is verified by the restored manifest", type)
            .isEqualTo(0L);
        assertThat(stats.get("graphRebuildCount")).as("%s: the archived graph is reused, not rebuilt", type).isEqualTo(0L);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void aBackupTakenDuringAGraphPersistRestoresToARebuildNeverToAReuse() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createVectorIndex(database, "Doc", "NONE");
      createVectorIndex(database, "PqDoc", "PRODUCT");

      // A PERSIST BETWEEN THE WINDOW'S t0 AND THE COPY: the pages the window holds are the previous generation, and
      // the manifest on disk now describes the new one. Archiving the two together would certify the wrong pages
      final boolean[] persisted = { false };
      FullBackupFormat.beforeVectorGraphSideFilesOpenForTesting = () -> {
        vectorIndex(database, "Doc").buildVectorGraphNow();
        vectorIndex(database, "PqDoc").buildVectorGraphNow();
        persisted[0] = true;
      };
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
      assertThat(persisted[0]).as("the fixture: the persist really ran inside the backup").isTrue();
    } finally {
      FullBackupFormat.beforeVectorGraphSideFilesOpenForTesting = null;
      TestHelper.checkActiveDatabases();
    }

    final Map<String, Long> entries = entrySizes();
    final List<String> graphs = entries.keySet().stream().filter(n -> n.endsWith(GRAPH_EXT)).toList();
    assertThat(graphs).hasSize(2);
    for (final String graph : graphs)
      assertThat(entries.get(graph)).as("'%s' falls back to an empty entry", graph).isZero();
    assertThat(entries.keySet()).as("and no side file is archived to vouch for anything")
        .noneMatch(n -> n.endsWith("." + LSMVectorIndexGraphManifest.FILE_EXT) || n.endsWith(
            "." + LSMVectorIndexOrdinalMapFile.FILE_EXT) || n.endsWith("." + LSMVectorIndexPQFile.FILE_EXT));

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      for (final String type : new String[] { "Doc", "PqDoc" }) {
        assertThat(search(restored, type)).as("%s answers a search after the restore", type).isTrue();
        final Map<String, Long> stats = vectorIndex(restored, type).getStats();
        assertThat(stats.get("unverifiedGraphReuses")).as("%s: nothing reused unverified", type).isEqualTo(0L);
        assertThat(stats.get("graphRebuildCount")).as("%s: the restored database builds its own graph", type)
            .isGreaterThan(0L);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void aSideFileGoneBetweenTheCaptureAndTheCopyTakesOnlyItsOwnGraphBackToARebuild() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createVectorIndex(database, "Doc", "NONE");
      createVectorIndex(database, "PqDoc", "PRODUCT");

      // ONE INDEX'S ORDINAL MAP DISAPPEARS AFTER t0: the open finds it gone and refuses that graph's whole set, and the
      // backup itself carries on
      final File directory = new File(DATABASE_PATH);
      FullBackupFormat.beforeVectorGraphSideFilesOpenForTesting = () -> {
        final File[] maps = directory.listFiles((dir, name) -> name.startsWith("Doc_") && name.endsWith(
            "." + LSMVectorIndexOrdinalMapFile.FILE_EXT));
        assertThat(maps).as("the fixture: the non-quantized index has its ordinal map").hasSize(1);
        assertThat(maps[0].delete()).isTrue();
      };
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      FullBackupFormat.beforeVectorGraphSideFilesOpenForTesting = null;
      TestHelper.checkActiveDatabases();
    }

    final Map<String, Long> entries = entrySizes();
    for (final Map.Entry<String, Long> entry : entries.entrySet())
      if (entry.getKey().endsWith(GRAPH_EXT))
        if (entry.getKey().startsWith("Doc_"))
          assertThat(entry.getValue()).as("'%s' lost a side file, so it falls back to an empty entry", entry.getKey()).isZero();
        else
          assertThat(entry.getValue()).as("'%s' is unaffected and keeps its pages", entry.getKey()).isGreaterThan(0L);
    assertThat(entries.keySet()).as("no part of the refused set is archived")
        .noneMatch(n -> n.startsWith("Doc_") && (n.endsWith("." + LSMVectorIndexGraphManifest.FILE_EXT) || n.endsWith(
            "." + LSMVectorIndexOrdinalMapFile.FILE_EXT)));

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      for (final String type : new String[] { "Doc", "PqDoc" })
        assertThat(search(restored, type)).as("%s answers a search after the restore", type).isTrue();
      assertThat(vectorIndex(restored, "Doc").getStats().get("graphRebuildCount")).isGreaterThan(0L);
      assertThat(vectorIndex(restored, "PqDoc").getStats().get("graphRebuildCount")).isEqualTo(0L);
      for (final String type : new String[] { "Doc", "PqDoc" })
        assertThat(vectorIndex(restored, type).getStats().get("unverifiedGraphReuses")).isEqualTo(0L);
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The frozen-files fallback has no t0 a backup can observe (see {@code backupFromFrozenFiles}), so it never attempts
   * the proof: an idle graph still goes in empty, with nothing beside it.
   */
  @Test
  void theFrozenFilesPathStillArchivesTheGraphEmptyWithNoSideFiles() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createVectorIndex(database, "Doc", "NONE");
      createVectorIndex(database, "PqDoc", "PRODUCT");
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      TestHelper.checkActiveDatabases();
    }

    final Map<String, Long> entries = entrySizes();
    final List<String> graphs = entries.keySet().stream().filter(n -> n.endsWith(GRAPH_EXT)).toList();
    assertThat(graphs).hasSize(2);
    for (final String graph : graphs)
      assertThat(entries.get(graph)).as("'%s' is archived empty on the frozen-files path", graph).isZero();
    assertThat(entries.keySet()).as("and with no side file")
        .noneMatch(n -> n.endsWith("." + LSMVectorIndexGraphManifest.FILE_EXT) || n.endsWith(
            "." + LSMVectorIndexOrdinalMapFile.FILE_EXT) || n.endsWith("." + LSMVectorIndexPQFile.FILE_EXT));
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
