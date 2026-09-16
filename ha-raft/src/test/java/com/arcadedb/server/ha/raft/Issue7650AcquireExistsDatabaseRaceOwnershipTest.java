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
import com.arcadedb.server.ArcadeDBServer;
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
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for a review finding on PR #7650 (issue #7622's own fix): {@link SnapshotInstaller#acquireNewDatabase}
 * releases its exclusive {@code INSTALLS_IN_FLIGHT} registration early when the database it is acquiring turns
 * out to have been created concurrently (the {@code existsDatabase} race, delegating the refresh to
 * {@link SnapshotInstaller#install}) - and the outer {@code finally} used to release the SAME key again
 * unconditionally, on the theory that a second release of an absent key is a harmless no-op.
 * <p>
 * That is only true if nothing else registered the key in between. A different install starting in the window
 * between the early release and the outer {@code finally} registers its own entry there, and the outer
 * {@code finally}'s "harmless" second release would decrement THAT install's count instead - clearing the
 * #7128 recovery-pass guard while it is still writing its staging directory.
 * <p>
 * Drives the real {@code acquireNewDatabase}, with a local HTTP server standing in for the leader's snapshot
 * endpoint (same pattern as {@code Issue7444SnapshotInstallExcludesBackupTest}), and
 * {@link SnapshotInstaller#existsDatabaseRaceBarrierForTesting} to register a different, concurrent install on
 * the same key at exactly the moment the bug's window opens.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7650AcquireExistsDatabaseRaceOwnershipTest {

  private static final String DB_NAME = "acquire7650";
  private static final String PASSWORD = "DefaultPasswordForTests";

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
    SnapshotInstaller.existsDatabaseRaceBarrierForTesting = null;
    if (server != null) {
      try {
        if (server.existsDatabase(DB_NAME))
          server.getDatabase(DB_NAME).drop();
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

  @Test
  @Timeout(120)
  void aConcurrentInstallsGuardSurvivesTheEarlyReleaseInTheExistsDatabaseRace(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    server = startServer(databasesDir, root);
    assertThat(server.existsDatabase(DB_NAME))
        .as("the database must be never-seen on this node before the acquisition").isFalse();

    // The leader's snapshot endpoint. Handler creates the database ON THIS SAME SERVER before writing the
    // response, standing in for a concurrent INSTALL_DATABASE_ENTRY replay finishing while this node
    // downloads - which is what makes acquireNewDatabase's post-download existsDatabase() recheck true.
    final Path source = root.resolve("leader-copy").resolve(DB_NAME);
    try (final Database snap = new DatabaseFactory(source.toString()).create()) {
      snap.transaction(() -> {
        snap.getSchema().createVertexType("Node");
        snap.newVertex("Node").set("v", 1).save();
      });
    }
    final byte[] zip = zipDirectory(source);
    leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
      server.getOrCreateDatabase(DB_NAME);
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    leader.start();

    final Path dbPath = databasesDir.resolve(DB_NAME);

    // The other, concurrent install: registered at the exact moment acquireNewDatabase releases its own
    // guard early to delegate to install(). If the outer finally then releases this key a second time
    // (the bug), THIS registration - not acquireNewDatabase's own, already gone - is what gets decremented.
    SnapshotInstaller.existsDatabaseRaceBarrierForTesting =
        () -> SnapshotInstaller.markInstallInFlightForTesting(dbPath);

    SnapshotInstaller.acquireNewDatabase(DB_NAME, () -> "localhost:" + leaderPort, () -> null, null, server);

    try {
      // Prove the "other install"'s guard is still up: seed the on-disk state a pending, still-being-written
      // install leaves (same shape Issue7622InstallsInFlightOwnershipTest uses), and confirm the #7128
      // recovery pass SKIPS it rather than deleting it out from under the install that (as far as the guard
      // can tell) still owns it.
      final Path snapshotNew = dbPath.resolve(".snapshot-new");
      Files.createDirectories(snapshotNew);
      Files.writeString(dbPath.resolve(".snapshot-pending"), "");
      Files.writeString(snapshotNew.resolve("partial.dat"), "still-being-extracted");

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

      assertThat(snapshotNew).as("a still-registered concurrent install's staging directory must not be deleted").exists();
      assertThat(dbPath.resolve(".snapshot-pending")).exists();
    } finally {
      SnapshotInstaller.clearInstallInFlightForTesting(dbPath);
    }
  }

  private ArcadeDBServer startServer(final Path databasesDir, final Path root) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7650");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);

    final ArcadeDBServer started = new ArcadeDBServer(config);
    started.start();
    return started;
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
