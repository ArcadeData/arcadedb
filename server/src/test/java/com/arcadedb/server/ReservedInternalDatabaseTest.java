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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for arcadedb-operations issue #379: the internal Raft control directory
 * {@code .raft} lives under the server database directory but must not be registered as a user
 * database nor leak into the {@code /api/v1/server?mode=cluster} status response.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReservedInternalDatabaseTest extends StaticBaseServerTest {

  @Test
  void reservedDatabaseDirectoryIsNotLoadedAtStartup() {
    final String databaseDirectory = "./target/databases";
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databaseDirectory);

    // 1. Create a regular user database.
    try (final Database db = new DatabaseFactory(databaseDirectory + File.separator + "UserDB").create()) {
      db.getSchema().createDocumentType("Doc");
    }

    // 2. Create a second fully-valid database directory and rename it to ".raft", simulating the
    //    internal Raft control directory that ends up under the server database directory.
    try (final Database db = new DatabaseFactory(databaseDirectory + File.separator + "raftseed").create()) {
      db.getSchema().createDocumentType("Doc");
    }
    final File seed = new File(databaseDirectory, "raftseed");
    final File raftDir = new File(databaseDirectory, ".raft");
    assertThat(seed.renameTo(raftDir)).as("renamed seed database directory to '.raft'").isTrue();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_0");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");

    final ArcadeDBServer server = new ArcadeDBServer(config);
    server.start();
    try {
      // The user database must be loaded.
      assertThat(server.getDatabaseNames()).contains("UserDB");

      // The reserved internal database must NOT be registered.
      assertThat(server.getDatabaseNames()).doesNotContain(".raft");
      assertThat(server.getDatabaseNames()).noneMatch(ArcadeDBServer::isReservedDatabaseName);

      // The directory must still exist on disk: it is skipped, not deleted.
      assertThat(raftDir.isDirectory()).isTrue();
    } finally {
      server.stop();
    }
  }

  /**
   * Regression test for issue #5027: a temp directory left behind by a restore/import that crashed
   * before its atomic swap must be reclaimed at startup instead of accumulating on disk.
   */
  @Test
  void orphanedRestoreTempDirectoryIsSweptAtStartup() throws Exception {
    final String databaseDirectory = "./target/databases";
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databaseDirectory);

    // A real user database that must survive the sweep.
    try (final Database db = new DatabaseFactory(databaseDirectory + File.separator + "KeepDB").create()) {
      db.getSchema().createDocumentType("Doc");
    }

    // Simulate a restore temp directory orphaned by a crash before the swap completed.
    final File orphan = new File(databaseDirectory, ArcadeDBServer.RESTORE_TEMP_DIRECTORY_PREFIX + "KeepDB-123");
    assertThat(orphan.mkdirs()).isTrue();
    Files.writeString(new File(orphan, "leftover.tmp").toPath(), "x");

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_0");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");

    final ArcadeDBServer server = new ArcadeDBServer(config);
    server.start();
    try {
      // The user database must be loaded and the orphaned temp directory swept from disk.
      assertThat(server.getDatabaseNames()).contains("KeepDB");
      assertThat(orphan.exists()).as("orphaned restore temp dir must be swept at startup").isFalse();
    } finally {
      server.stop();
    }
  }
}
