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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8381, a follow-up to #7670.
 * <p>
 * Over a torn database directory (no loadable schema, no retained backup, no swap state) the {@code .snapshot-pending}
 * marker is the only thing that stops the directory from being opened: {@code ArcadeDBServer.loadDatabases} defers
 * it and {@code getDatabase} refuses it on the marker alone. {@code installHoldingMaintenanceSlot} used to delete that
 * marker while cleaning up leftovers and re-create it only after {@code Files.createDirectories(snapshotNew)}, so a
 * failure in between - on the full volume that tore the directory in the first place - left the torn directory with
 * no marker, and the next start opened it.
 * <p>
 * The failure is staged without a test seam: a regular file where the {@code .snapshot-new} staging directory goes
 * makes {@code Files.createDirectories} throw, exactly in the window the marker used to be missing.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8381InstallKeepsMarkerOfTornDirectoryBeforeDownloadTest {

  private static final String DB_NAME       = "install8381";
  private static final String PASSWORD      = "DefaultPasswordForTests";
  private static final String ORIGINAL_TYPE = "OriginalContent8381";

  private ArcadeDBServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  /**
   * The issue's scenario: the install fails between the leftover cleanup and the marker write. The torn directory
   * must still carry its marker, so it stays unopenable at runtime and deferred at the next start.
   */
  @Test
  @Timeout(180)
  void aFailureBeforeTheMarkerRewriteKeepsTheMarkerOfATornDirectory(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);

    // Torn: what a failed rollback leaves once its backup is gone - some files, but no schema.
    try (final Stream<Path> entries = Files.list(dbDir)) {
      for (final Path entry : entries.toList())
        if (entry.getFileName().toString().startsWith("schema"))
          Files.delete(entry);
    }
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    Files.writeString(marker, "");

    // A regular file where the staging directory goes: the leftover cleanup only removes directories, so
    // Files.createDirectories(snapshotNew) fails - the window between the old marker delete and its rewrite.
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    Files.writeString(snapshotNew, "blocks the staging directory");

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, SnapshotInstaller.resolveDatabasePath(server, DB_NAME),
        () -> null, () -> null, null, server))
        .as("the install fails creating its staging directory, before any download")
        .isInstanceOf(IOException.class);

    // loadDatabases defers a directory on this file alone, so its presence is what keeps the next start away too.
    assertThat(marker).as("the torn directory keeps the marker that stops it being opened").exists();
    assertThatThrownBy(() -> server.getDatabase(DB_NAME)).as("the torn directory is still refused")
        .hasMessageContaining(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
  }

  /**
   * The install's own marker write still happens over a directory that carries none: the marker is written before
   * the download, so a download failure over a loadable database clears it again and leaves the database servable.
   */
  @Test
  @Timeout(180)
  void aHealthyDirectoryWithoutAMarkerStillGetsOneWrittenAndClearedOnDownloadFailure(@TempDir final Path root)
      throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = databasesDir.resolve(DB_NAME);
    server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE).getSchema().createDocumentType(ORIGINAL_TYPE);
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);

    final boolean[] markerSeenDuringDownload = new boolean[1];
    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, SnapshotInstaller.resolveDatabasePath(server, DB_NAME),
        () -> {
          markerSeenDuringDownload[0] = Files.exists(marker);
          return null;
        }, () -> null, null, server))
        .as("the download fails: no leader address resolves").isInstanceOf(IOException.class);

    assertThat(markerSeenDuringDownload[0]).as("the marker is on disk while the download runs").isTrue();
    assertThat(marker).as("a failed download over a loadable database clears the marker").doesNotExist();
    final ServerDatabase db = server.getDatabase(DB_NAME);
    assertThat(db.getSchema().existsType(ORIGINAL_TYPE)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------------

  private Path registerThenCloseUnderneathTheRegistry(final Path databasesDir) {
    final ServerDatabase live = server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE);
    live.getSchema().createDocumentType(ORIGINAL_TYPE);
    ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().close();
    assertThat(server.existsDatabase(DB_NAME)).as("precondition: still registered").isTrue();
    return databasesDir.resolve(DB_NAME);
  }

  private Path startServer(final Path root) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8381");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, 60_000);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);

    server = new ArcadeDBServer(config);
    server.start();
    return databasesDir;
  }
}
