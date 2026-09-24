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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
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
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7670, a follow-up to #7530.
 * <p>
 * {@code ArcadeDBServer.getDatabase} throws {@code DatabaseNotAvailableException} for a database that is registered
 * but <i>not open</i> while its {@code .snapshot-pending} marker is on disk. #7530 guarded that lookup on the
 * crash-recovery pass only. The install path resolves the same database three more times with the same marker on
 * disk, and each unguarded lookup failed the install before it had moved a file - leaving the follower unable to
 * resync from the leader until a restart:
 * <ul>
 *   <li>{@code resolveDatabasePath}, which every install driver calls to compute the {@code install()} argument;</li>
 *   <li>{@code reconcileRetainedBackup}, which only runs when the marker exists;</li>
 *   <li>{@code swapAndReopen}, which runs after {@code installHoldingMaintenanceSlot} has written the marker.</li>
 * </ul>
 * The registered-but-closed state is staged the same way the #7530 test stages it: the embedded instance is closed
 * underneath the registry without deregistering it, which is what a {@code close()} that marks the instance closed
 * and then throws leaves behind.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7670InstallResolvesClosedRegisteredDatabaseTest {

  private static final String DB_NAME       = "install7670";
  private static final String PASSWORD      = "DefaultPasswordForTests";
  private static final String ORIGINAL_TYPE = "OriginalContent7670";
  private static final String SNAPSHOT_TYPE = "SnapshotContent7670";

  private ArcadeDBServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  /**
   * Entry point 1. Every install driver computes {@code install()}'s path argument through
   * {@code resolveDatabasePath} before the install starts, so a throwing lookup there fails the install before
   * any of its own reconciliation can run. It must answer with the directory the lookup would have opened.
   */
  @Test
  @Timeout(180)
  void resolveDatabasePathLocatesARegisteredButClosedDatabaseWithTheMarkerOnDisk(@TempDir final Path root)
      throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");

    final String resolved = SnapshotInstaller.resolveDatabasePath(server, DB_NAME);

    assertThat(Path.of(resolved).normalize().toAbsolutePath())
        .isEqualTo(dbDir.normalize().toAbsolutePath());
    assertThat(server.existsDatabase(DB_NAME)).as("the lookup did not deregister the entry").isTrue();
  }

  /**
   * Entry point 2. The previous attempt left the marker and its retained backup behind, so the install starts
   * with {@code reconcileRetainedBackup}, which closes the database before moving the backup back into place.
   * <p>
   * The install is still expected to fail, but at the download - no leader address resolves here - and only
   * after the retained backup was reconciled and the database reopened. Before the fix it failed with
   * {@code DatabaseNotAvailableException} before touching anything.
   */
  @Test
  @Timeout(180)
  void anInstallReconcilesARetainedBackupOfARegisteredButClosedDatabase(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);

    // What a failed rollback leaves: dbPath cleared, every file of the previous copy in the retained backup, the
    // marker still on disk.
    final Path backup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(backup);
    try (final Stream<Path> entries = Files.list(dbDir)) {
      for (final Path entry : entries.filter(p -> !p.equals(backup)).toList())
        Files.move(entry, backup.resolve(entry.getFileName().toString()));
    }
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");

    // The path is passed in rather than resolved, so this pins the reconcile lookup on its own; resolving it is
    // entry point 1, pinned above.
    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, dbDir.toString(), () -> null, () -> null, null, server))
        .as("the install gets as far as its download instead of failing to resolve the database")
        .isInstanceOf(IOException.class);

    assertThat(backup).as("the retained backup was reconciled into the live directory").doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();

    final ServerDatabase reopened = server.getDatabase(DB_NAME);
    assertThat(reopened.isOpen()).as("the reconciled database was reopened").isTrue();
    assertThat(reopened.getSchema().existsType(ORIGINAL_TYPE))
        .as("the reopened database is the previous copy the backup held").isTrue();
    assertThat(server.isSnapshotInstallInProgress()).as("the 503 window was released").isFalse();
  }

  /**
   * Entry point 3. {@code installHoldingMaintenanceSlot} writes the marker before the download and only then calls
   * {@code swapAndReopen}, so the swap resolves the database with the marker on disk. A registered-but-closed entry
   * holds no handles in the directory, so the swap proceeds and the reopen replaces the stale entry with the
   * installed snapshot.
   */
  @Test
  @Timeout(180)
  void theSwapInstallsOverARegisteredButClosedDatabase(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = registerThenCloseUnderneathTheRegistry(databasesDir);

    final Path source = root.resolve("leader-snapshot");
    try (final DatabaseFactory factory = new DatabaseFactory(source.toString())) {
      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType(SNAPSHOT_TYPE);
      }
    }
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    copyDirectory(source, snapshotNew);
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    Files.writeString(marker, "");

    SnapshotInstaller.swapAndReopen(DB_NAME, dbDir, snapshotNew, dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR),
        marker, server);

    assertThat(marker).doesNotExist();
    final ServerDatabase reopened = server.getDatabase(DB_NAME);
    assertThat(reopened.isOpen()).isTrue();
    assertThat(reopened.getSchema().existsType(SNAPSHOT_TYPE)).as("the installed snapshot is live").isTrue();
    assertThat(reopened.getSchema().existsType(ORIGINAL_TYPE)).as("the previous copy was swapped out").isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * Registers a database and closes its embedded instance underneath the registry, without deregistering it: the
   * state a {@code close()} that marks the instance closed and then throws leaves behind.
   */
  private Path registerThenCloseUnderneathTheRegistry(final Path databasesDir) {
    final ServerDatabase live = server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE);
    live.getSchema().createDocumentType(ORIGINAL_TYPE);
    ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().close();
    assertThat(server.existsDatabase(DB_NAME)).as("precondition: still registered").isTrue();
    return databasesDir.resolve(DB_NAME);
  }

  private static void copyDirectory(final Path from, final Path to) throws IOException {
    Files.createDirectories(to);
    try (final Stream<Path> entries = Files.walk(from)) {
      final List<Path> sorted = entries.sorted(Comparator.naturalOrder()).toList();
      for (final Path entry : sorted) {
        final Path target = to.resolve(from.relativize(entry).toString());
        if (Files.isDirectory(entry))
          Files.createDirectories(target);
        else
          Files.copy(entry, target, StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private Path startServer(final Path root) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7670");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, 60_000);
    // The reconcile test's download can never succeed: fail it at once rather than pay the backoff.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 1);

    server = new ArcadeDBServer(config);
    server.start();
    return databasesDir;
  }
}
