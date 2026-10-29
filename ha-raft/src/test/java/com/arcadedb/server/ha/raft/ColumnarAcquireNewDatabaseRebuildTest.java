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
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.server.ArcadeDBServer;
import com.sun.net.httpserver.HttpServer;
import org.awaitility.Awaitility;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A fresh replica, holding no copy of a database, acquires it from the leader's snapshot through
 * {@link SnapshotInstaller#acquireNewDatabase}. A snapshot never carries the node-local columnar projection, so the acquired database
 * rebuilds it, and the rebuild must carry the reason {@code snapshot_install} like the in-place {@link SnapshotInstaller#install}
 * (plan 3 contract: a snapshot install always leads to a {@code snapshot_install} rebuild), not {@code missing}.
 * <p>
 * Drives the real {@code acquireNewDatabase} with a local HTTP server standing in for the leader's snapshot endpoint, as
 * {@code Issue7650AcquireExistsDatabaseRaceOwnershipTest} does.
 */
class ColumnarAcquireNewDatabaseRebuildTest {
  private static final String DB_NAME  = "acquireColumnar";
  private static final String PASSWORD = "DefaultPasswordForTests";

  private HttpServer     leader;
  private int            leaderPort;
  private ArcadeDBServer server;

  @BeforeEach
  void startLeaderEndpoint() throws IOException {
    leader = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    leaderPort = leader.getAddress().getPort();
  }

  @AfterEach
  void stopEverything() {
    if (server != null) {
      try {
        if (server.existsDatabase(DB_NAME))
          server.getDatabaseForOperator(DB_NAME).drop();
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
  void acquiredDatabaseRebuildsItsProjectionAsSnapshotInstall(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    server = startServer(databasesDir, root);
    assertThat(server.existsDatabase(DB_NAME)).as("the database must be never-seen on this node before the acquisition").isFalse();

    // the leader's copy: COLUMNAR indexes created on the empty type, then records, so its projection holds rows
    final Path source = root.resolve("leader-copy").resolve(DB_NAME);
    final ColumnarTestData data = new ColumnarTestData(4711L);
    try (final Database leaderCopy = new DatabaseFactory(source.toString()).create()) {
      data.createSchema(leaderCopy);
      data.insertAssets(leaderCopy, 500);
    }
    assertThat(source.resolve("columnar")).as("the fixture: the leader's copy has a projection the snapshot leaves out").exists();
    final byte[] zip = zipSnapshot(source);
    leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    leader.start();

    SnapshotInstaller.acquireNewDatabase(DB_NAME, () -> "localhost:" + leaderPort, () -> null, null, server);

    assertThat(server.existsDatabase(DB_NAME)).as("the acquired database is registered").isTrue();
    final Database acquired = server.getDatabase(DB_NAME);
    final ColumnarIndexManager manager = ((LocalDatabase) ((DatabaseInternal) acquired).getEmbedded()).getColumnarIndexManager();
    Awaitility.await("the rebuild of the acquired projection").atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> manager.getStats().rebuildsTotal() > 0);
    assertThat(manager.awaitCaughtUp(30_000L)).as("the rebuilt projection catches up").isTrue();

    assertThat(manager.getStats().rebuilds(ColumnarTestData.TYPE, "missing")).as("a snapshot install is not a missing projection")
        .isZero();
    assertThat(manager.getStats().rebuilds(ColumnarTestData.TYPE, "snapshot_install"))
        .as("the acquired database rebuilds its projection with the snapshot_install reason").isPositive();
    assertThat(manager.getStats().rebuildsTotal()).as("no rebuild with any other reason")
        .isEqualTo(manager.getStats().rebuilds(ColumnarTestData.TYPE, "snapshot_install"));
    for (final String property : ColumnarHATestSupport.PROPERTIES)
      FacetOracle.assertEquivalent(acquired, ColumnarTestData.TYPE, property, null);
  }

  private ArcadeDBServer startServer(final Path databasesDir, final Path root) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_acquireColumnar");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);

    final ArcadeDBServer started = new ArcadeDBServer(config);
    started.start();
    return started;
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
}
