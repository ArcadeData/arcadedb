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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.backup.BackupCoordinator;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7444.
 * <p>
 * A restore is leader-only, so restore-versus-restore of one database is already serialised cluster-wide. A backup is
 * not: {@code runOnServer} defaults to {@code "*"}, so every node runs its own scheduled backup. After a restore the
 * leader submits an install-database entry with {@code forceSnapshot=true} and every follower answers it by replacing
 * its own copy of the database directory from the leader's snapshot - while that follower's own scheduled backup may
 * be reading it. The follower's {@link BackupCoordinator} slot was the wrong one to consult only because nothing on
 * the install path ever took it: the restore happened on a different JVM, but the <i>reinstall</i> happens right here.
 * <p>
 * {@link SnapshotInstaller#install} is the single choke point every install driver goes through - the forceSnapshot
 * arm of {@code applyInstallDatabaseEntry}, the bootstrap installs, the reconciler's resync - so the slot is taken
 * there rather than at any one call site. These tests drive the real {@code install}, with a local HTTP server
 * standing in for the leader's snapshot endpoint, and pin both directions of the exclusion.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7444SnapshotInstallExcludesBackupTest {

  private static final String DB_NAME        = "snap7444";
  private static final String PASSWORD       = "DefaultPasswordForTests";
  private static final int    LIVE_COUNT     = 12;
  private static final int    SNAPSHOT_COUNT = 27;

  private HttpServer     leader;
  private int            leaderPort;
  private ArcadeDBServer server;

  @BeforeEach
  void startLeaderEndpoint() throws IOException {
    leader = HttpServer.create(new InetSocketAddress(0), 0);
    leaderPort = leader.getAddress().getPort();
  }

  @AfterEach
  void stopEverything() {
    SnapshotInstaller.swapBarrierForTesting = null;
    if (server != null) {
      try {
        if (server.existsDatabase(DB_NAME))
          ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort cleanup; the @TempDir is removed regardless
      }
      server.stop();
      server = null;
    }
    if (leader != null) {
      leader.stop(0);
      leader = null;
    }
  }

  /**
   * The bug as reported: while this node replaces its copy of the database, a backup of it must not be admitted.
   * The install is paused inside the registry-locked swap - the exact moment the directory is being replaced - and a
   * backup is attempted from the test thread, standing in for the scheduled tick {@code BackupTask.run} would fire.
   * <p>
   * Without the fix the reservation succeeds and the tick goes on to read a directory that is being deleted.
   */
  @Test
  @Timeout(120)
  void aBackupIsRefusedWhileThisNodeIsReinstallingTheDatabase(@TempDir final Path root) throws Exception {
    server = startServer(root, 60_000);
    final Path dbPath = createLiveDatabase();
    serveSnapshot(root);

    final CountDownLatch insideSwap = new CountDownLatch(1);
    final CountDownLatch releaseSwap = new CountDownLatch(1);
    SnapshotInstaller.swapBarrierForTesting = () -> {
      insideSwap.countDown();
      try {
        releaseSwap.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final AtomicBoolean installDone = new AtomicBoolean(false);
    final AtomicReference<Throwable> installError = new AtomicReference<>();
    final Thread installer = new Thread(() -> {
      try {
        SnapshotInstaller.install(DB_NAME, dbPath.toString(), "localhost:" + leaderPort, null, null, server);
        installDone.set(true);
      } catch (final Throwable t) {
        installError.set(t);
      }
    }, "snapshot-installer");
    installer.start();

    try {
      assertThat(insideSwap.await(60, TimeUnit.SECONDS)).as("the install reached the in-swap barrier").isTrue();

      final BackupCoordinator coordinator = server.getBackupCoordinator();
      assertThat(coordinator.isInProgress(DB_NAME, BackupCoordinator.Operation.RESTORE))
          .as("the install holds this node's maintenance slot while it replaces the files").isTrue();
      assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP))
          .as("a scheduled backup of a database being reinstalled from the leader is refused")
          .isEqualTo(BackupCoordinator.Operation.RESTORE);
    } finally {
      releaseSwap.countDown();
    }

    installer.join(60_000);
    assertThat(installError.get()).as("the install completed without error").isNull();
    assertThat(installDone.get()).isTrue();

    // The slot is released when the install ends, whatever the outcome: a leaked reservation would block every later
    // backup of this database until the server restarts.
    assertThat(server.getBackupCoordinator().isInProgress(DB_NAME)).as("the install released the slot").isFalse();
    assertThat(server.getBackupCoordinator().begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();
    server.getBackupCoordinator().end(DB_NAME, BackupCoordinator.Operation.BACKUP);

    assertThat(server.getDatabase(DB_NAME).countType("Node", true)).isEqualTo(SNAPSHOT_COUNT);
  }

  /**
   * The other direction: a backup already running when the entry arrives. The install waits for it rather than
   * pulling the directory out from under it, and proceeds the moment it is released.
   */
  @Test
  @Timeout(120)
  void anInstallWaitsForABackupThatIsAlreadyRunning(@TempDir final Path root) throws Exception {
    server = startServer(root, 60_000);
    final Path dbPath = createLiveDatabase();
    serveSnapshot(root);

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();

    final AtomicBoolean installDone = new AtomicBoolean(false);
    final AtomicReference<Throwable> installError = new AtomicReference<>();
    final Thread installer = new Thread(() -> {
      try {
        SnapshotInstaller.install(DB_NAME, dbPath.toString(), "localhost:" + leaderPort, null, null, server);
        installDone.set(true);
      } catch (final Throwable t) {
        installError.set(t);
      }
    }, "snapshot-installer");
    installer.start();

    // The install must not have replaced anything yet: the live database is still the one the backup is reading.
    installer.join(2_000);
    assertThat(installDone.get()).as("the install waited instead of replacing the files under a running backup").isFalse();
    assertThat(server.getDatabase(DB_NAME).countType("Node", true))
        .as("the database the backup is reading is untouched while the install waits").isEqualTo(LIVE_COUNT);

    coordinator.end(DB_NAME, BackupCoordinator.Operation.BACKUP);

    installer.join(60_000);
    assertThat(installError.get()).as("the install completed without error once the backup released the slot").isNull();
    assertThat(installDone.get()).isTrue();
    assertThat(server.getDatabase(DB_NAME).countType("Node", true)).isEqualTo(SNAPSHOT_COUNT);
  }

  /**
   * The wait is bounded, and that is not a detail: an install applies a committed Raft entry, and a follower that
   * blocks on it forever stops applying <i>every</i> database's entries - one state machine multiplexes them all.
   * With the wait configured short, a backup that never ends does not stop the install; it only makes it loud.
   */
  @Test
  @Timeout(120)
  void aBackupThatNeverEndsDoesNotBlockTheInstallForever(@TempDir final Path root) throws Exception {
    server = startServer(root, 500);
    final Path dbPath = createLiveDatabase();
    serveSnapshot(root);

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();
    try {
      SnapshotInstaller.install(DB_NAME, dbPath.toString(), "localhost:" + leaderPort, null, null, server);

      assertThat(server.getDatabase(DB_NAME).countType("Node", true))
          .as("the committed entry was applied even though the wait expired").isEqualTo(SNAPSHOT_COUNT);
      // The install never took the slot, so it must not have released the backup's reservation either.
      assertThat(coordinator.isInProgress(DB_NAME, BackupCoordinator.Operation.BACKUP))
          .as("a timed-out install leaves the reservation it never took alone").isTrue();
    } finally {
      coordinator.end(DB_NAME, BackupCoordinator.Operation.BACKUP);
    }
  }

  private ArcadeDBServer startServer(final Path root, final long backupWaitMs) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7444");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, backupWaitMs);

    final ArcadeDBServer started = new ArcadeDBServer(config);
    started.start();
    return started;
  }

  private Path createLiveDatabase() {
    final ServerDatabase live = server.getOrCreateDatabase(DB_NAME);
    live.transaction(() -> {
      live.getSchema().createVertexType("Node");
      for (int i = 0; i < LIVE_COUNT; i++)
        live.newVertex("Node").set("v", i).save();
    });
    return Path.of(((DatabaseInternal) live).getDatabasePath());
  }

  /**
   * Publishes a real, loadable database with a DIFFERENT record count on the leader's snapshot endpoint, so a
   * successful install is observable as a count change rather than only as the absence of an exception.
   */
  private void serveSnapshot(final Path root) throws IOException {
    final Path source = root.resolve("leader-copy").resolve(DB_NAME);
    try (final Database snap = new DatabaseFactory(source.toString()).create()) {
      snap.transaction(() -> {
        snap.getSchema().createVertexType("Node");
        for (int i = 0; i < SNAPSHOT_COUNT; i++)
          snap.newVertex("Node").set("v", i).save();
      });
    }

    final byte[] zip = zipDirectory(source);
    leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    leader.start();
  }

  private static byte[] zipDirectory(final Path dir) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zip = new ZipOutputStream(bytes); final Stream<Path> files = Files.walk(dir)) {
      for (final Path file : files.filter(Files::isRegularFile).toList()) {
        zip.putNextEntry(new ZipEntry(dir.relativize(file).toString().replace('\\', '/')));
        zip.write(Files.readAllBytes(file));
        zip.closeEntry();
      }
    }
    return bytes.toByteArray();
  }
}
