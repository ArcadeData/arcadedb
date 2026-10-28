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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The columnar projection is node-local derived state (plan 3, Task 22): a snapshot never ships it, and a node that
 * installs a snapshot drops the projection of the files it replaced and rebuilds with the reason {@code snapshot_install},
 * on the live install path and on the startup roll-forward of an INSTALLED swap alike.
 */
class ColumnarSnapshotExclusionTest {
  private static final String DB_NAME = "columnarSnapshot";

  @TempDir
  Path tempDir;

  private ArcadeDBServer server;

  @AfterEach
  void tearDown() {
    SnapshotInstaller.snapshotOpensForTesting = null;
    if (server != null) {
      if (server.existsDatabase(DB_NAME))
        ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().drop();
      server.stop();
    }
  }

  @Test
  void snapshotZipHasNoProjectionEntry() throws Exception {
    final Path dbPath = tempDir.resolve(DB_NAME);
    try (final Database db = new DatabaseFactory(dbPath.toString()).create()) {
      final ColumnarTestData data = new ColumnarTestData(7L);
      data.createSchema(db);
      data.insertAssets(db, 400);
      FacetOracle.awaitCaughtUp(db);
      ((LocalDatabase) db).getColumnarIndexManager().flushNow();
      assertThat(dbPath.resolve(ColumnarStore.ROOT_DIR)).as("the fixture: a flushed projection is on disk").isNotEmptyDirectory();

      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
      try (final ZipOutputStream zip = new ZipOutputStream(bytes)) {
        SnapshotHttpHandler.addPageFilesToZip(zip, (DatabaseInternal) db, null, manifest);
        SnapshotHttpHandler.addConfigurationToZip(zip, (DatabaseInternal) db, null, manifest);
      }

      final List<String> names = new ArrayList<>();
      try (final ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry())
          names.add(entry.getName());
      }
      assertThat(names).as("the control: the bucket of the projected type is shipped").anyMatch(n -> n.startsWith(ColumnarTestData.TYPE));
      assertThat(names).noneMatch(ColumnarStore::isProjectionEntry);
      assertThat(manifest).noneMatch(e -> ColumnarStore.isProjectionEntry(e.name()));
    }
  }

  /** The predicate both the ZIP and the size estimate apply: a projection entry is never a shipped page file. */
  @Test
  void aProjectionEntryIsNeverAShippedPageFile() {
    assertThat(SnapshotHttpHandler.isShippedPageFile("columnar/1/manifest.json")).isFalse();
    assertThat(SnapshotHttpHandler.isShippedPageFile(ColumnarStore.ROOT_DIR)).isFalse();
    assertThat(SnapshotHttpHandler.isShippedPageFile("columnar_0.1.65536.v0.bucket")).as("a type named columnar").isTrue();
  }

  /** The live install path: the swapped-in copy carries no projection, so the reopen rebuilds it as a snapshot install. */
  @Test
  void installPreparationMarksSnapshotInstall() throws Exception {
    final Path databasesDir = tempDir.resolve("databases");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    server = new ArcadeDBServer(config);

    final Path dbPath = databasesDir.resolve(DB_NAME);
    final LocalDatabase live = (LocalDatabase) new DatabaseFactory(dbPath.toString()).create();
    final ColumnarTestData liveData = new ColumnarTestData(1L);
    liveData.createSchema(live);
    liveData.insertAssets(live, 300);
    FacetOracle.awaitCaughtUp(live);
    live.getColumnarIndexManager().flushNow();
    server.registerDatabase(DB_NAME, live);

    final Path snapshotNew = dbPath.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    stageLeaderSnapshot(snapshotNew, 2L, 500);
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    final Path pendingMarker = dbPath.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    Files.writeString(pendingMarker, "");

    SnapshotInstaller.swapAndReopen(DB_NAME, dbPath, snapshotNew, dbPath.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR), pendingMarker,
        server);

    final DatabaseInternal installed = (DatabaseInternal) server.getDatabase(DB_NAME);
    assertThat(installed.countType(ColumnarTestData.TYPE, true)).as("the snapshot is the one served").isEqualTo(500);
    FacetOracle.assertEquivalent(installed, ColumnarTestData.TYPE, "brand", null);
    FacetOracle.assertEquivalent(installed, ColumnarTestData.TYPE, "tags", null);
    final LocalDatabase embedded = (LocalDatabase) installed.getEmbedded();
    assertThat(embedded.getColumnarIndexManager().getStats().rebuilds(ColumnarTestData.TYPE, "snapshot_install")).isEqualTo(1);
    assertThat(embedded.getColumnarIndexManager().getStats().rebuilds(ColumnarTestData.TYPE, "missing")).isZero();
  }

  /** Startup recovery rolling an INSTALLED swap forward marks the projection the same way, before anything reopens it. */
  @Test
  void rollForwardOfAnInstalledSwapMarksSnapshotInstall() throws Exception {
    SnapshotInstaller.snapshotOpensForTesting = path -> true;
    final Path databasesDir = tempDir.resolve("databases");
    final Path dbPath = databasesDir.resolve(DB_NAME);
    // the swap moved the previous copy, projection included, into the backup and the leader's files into the live directory
    stageLeaderSnapshot(dbPath, 3L, 200);
    final Path backup = dbPath.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    try (final Database previous = new DatabaseFactory(backup.toString()).create()) {
      final ColumnarTestData data = new ColumnarTestData(4L);
      data.createSchema(previous);
      data.insertAssets(previous, 100);
      FacetOracle.awaitCaughtUp(previous);
      ((LocalDatabase) previous).getColumnarIndexManager().flushNow();
    }
    Files.writeString(dbPath.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbPath.resolve(".snapshot-swap-state"), "INSTALLED");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbPath.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).as("the swap was rolled forward").doesNotExist();
    assertThat(ColumnarStore.readRebuildReason(dbPath)).isEqualTo("snapshot_install");
  }

  /** A database as the leader ships it: its pages and schema, never its projection. */
  private static void stageLeaderSnapshot(final Path dir, final long seed, final int records) throws Exception {
    try (final Database leader = new DatabaseFactory(dir.toString()).create()) {
      final ColumnarTestData data = new ColumnarTestData(seed);
      data.createSchema(leader);
      data.insertAssets(leader, records);
      FacetOracle.awaitCaughtUp(leader);
    }
    FileUtils.deleteRecursively(dir.resolve(ColumnarStore.ROOT_DIR).toFile());
  }
}
