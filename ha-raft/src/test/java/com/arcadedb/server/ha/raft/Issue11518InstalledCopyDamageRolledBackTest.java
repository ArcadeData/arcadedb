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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.server.ArcadeDBServer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11518, the snapshot-install entry point: the copy a snapshot install reopens is the one a resync then serves, and
 * every install path lifts the quarantine on success. An installed copy whose dictionary turns up empty while its schema
 * declares types opens without an error, matches the files the leader sent, and used to be installed and served. It is now
 * refused like a copy that does not open: the previous copy is restored and the install fails, so the caller's quarantine
 * stands. The acquisition of a database this node never held takes the same check after its publish: the open hook of a
 * running node leaves a database being acquired to it, and its read-only validation cannot see an emptied dictionary.
 */
class Issue11518InstalledCopyDamageRolledBackTest {
  private static final String DB_NAME = "Universe";

  @TempDir
  Path root;

  private ArcadeDBServer server;
  private HttpServer     leader;

  @AfterEach
  void tearDown() {
    if (server != null && server.isStarted())
      server.stop();
    if (leader != null)
      leader.stop(0);
  }

  @Test
  void anInstalledCopyWithAnEmptiedDictionaryIsRolledBack() throws Exception {
    final Path live = root.resolve("databases").resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    createDatabase(live, "old");
    createDatabase(staged, "new");
    emptyTheDictionaryFile(staged);
    // Taken after the damage: the files are exactly what was sent, so the #11200 comparison passes and only the reopen can tell
    final Map<String, Long> expected = SnapshotInstaller.expectedInstalledFiles(staged);

    server = newServer();
    server.start();
    assertValue(server, "old");
    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    assertThatThrownBy(() -> SnapshotInstaller.swapAndReopen(DB_NAME, live, staged, backup, marker, server, expected))
        .isInstanceOf(IOException.class).hasMessageContaining("rolled back to the previous local copy")
        .rootCause().hasMessageContaining("schema dictionary is empty");

    assertValue(server, "old");
  }

  @Test
  void anIntactInstalledCopyIsStillInstalled() throws Exception {
    final Path live = root.resolve("databases").resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    createDatabase(live, "old");
    createDatabase(staged, "new");
    final Map<String, Long> expected = SnapshotInstaller.expectedInstalledFiles(staged);

    server = newServer();
    server.start();
    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    SnapshotInstaller.swapAndReopen(DB_NAME, live, staged, backup, marker, server, expected);

    assertValue(server, "new");
    assertThat(backup).doesNotExist();
  }

  @Test
  @Timeout(120)
  void anAcquiredCopyWithAnEmptiedDictionaryIsDiscarded() throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    final Path source = root.resolve("leader-copy").resolve(DB_NAME);
    createDatabase(source, "new");
    emptyTheDictionaryFile(source);
    final byte[] zip = zipSnapshot(source);
    leader = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    leader.start();
    final int leaderPort = leader.getAddress().getPort();

    server = newServer();
    server.start();
    assertThat(server.existsDatabase(DB_NAME)).isFalse();

    assertThatThrownBy(() -> SnapshotInstaller.acquireNewDatabase(DB_NAME, () -> "localhost:" + leaderPort, () -> null, null, server))
        .isInstanceOf(IOException.class).hasMessageContaining("failed to open after publish")
        .rootCause().hasMessageContaining("schema dictionary is empty");

    assertThat(server.existsDatabase(DB_NAME)).as("the damaged copy is not served").isFalse();
    assertThat(databasesDir.resolve(DB_NAME)).as("and left absent, for the next reconcile to acquire again").doesNotExist();
  }

  private ArcadeDBServer newServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "TestPassword11518");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "0");
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.SERVER_METRICS, false);
    config.setValue(GlobalConfiguration.SERVER_HEALTH_CHECK_ENABLED, false);
    config.setValue(GlobalConfiguration.SERVER_DATABASE_LOADATSTARTUP, true);
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "");
    return new ArcadeDBServer(config);
  }

  private static void createDatabase(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Item", 1);
        db.newDocument("Item").set("value", value).save();
      });
    }
  }

  private static void emptyTheDictionaryFile(final Path dbPath) throws IOException {
    final File[] dictionaryFiles = dbPath.toFile().listFiles((dir, name) -> name.endsWith("." + Dictionary.DICT_EXT));
    assertThat(dictionaryFiles).as("the fixture has a dictionary file").hasSize(1);
    try (final RandomAccessFile raf = new RandomAccessFile(dictionaryFiles[0], "rw")) {
      raf.setLength(0);
    }
  }

  /** Zips the database the way the leader ships a snapshot: without the node-local projection. */
  private static byte[] zipSnapshot(final Path dir) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zip = new ZipOutputStream(bytes); final Stream<Path> files = Files.walk(dir)) {
      for (final Path file : files.filter(Files::isRegularFile).toList()) {
        final String name = dir.relativize(file).toString().replace('\\', '/');
        if (ColumnarStore.isProjectionEntry(name))
          continue;
        zip.putNextEntry(new ZipEntry(name));
        zip.write(Files.readAllBytes(file));
        zip.closeEntry();
      }
    }
    return bytes.toByteArray();
  }

  private static void assertValue(final ArcadeDBServer server, final String expected) {
    final var db = server.getDatabaseForOperator(DB_NAME);
    assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(expected);
  }
}
