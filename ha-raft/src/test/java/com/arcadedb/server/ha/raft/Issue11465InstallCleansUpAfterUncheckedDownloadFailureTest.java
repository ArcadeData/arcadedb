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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11465: a follower wedged in a silent snapshot-install loop, with a zero-byte
 * {@code .snapshot-pending} marker and a staged {@code .snapshot-new} directory left on disk for the life of the
 * process; only a restart ran the pending-swap cleanup.
 * <p>
 * {@code installHoldingMaintenanceSlot} cleaned up after a failed download only for an {@link IOException}. Anything
 * unchecked escaping the download left the staging directory and the marker behind, which is exactly the state
 * startup recovery found and cleaned.
 */
class Issue11465InstallCleansUpAfterUncheckedDownloadFailureTest {

  private static final String DB_NAME  = "install11465";
  private static final String PASSWORD = "DefaultPasswordForTests";
  private static final String TYPE     = "OriginalContent11465";

  private ArcadeDBServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  @Test
  @Timeout(180)
  void anUncheckedDownloadFailureLeavesNeitherStagingNorMarkerBehind(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    startServer(root, databasesDir);

    server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE).getSchema().createDocumentType(TYPE);
    final Path dbDir = databasesDir.resolve(DB_NAME);
    final Path marker = dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    final Path staging = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, SnapshotInstaller.resolveDatabasePath(server, DB_NAME),
        () -> {
          throw new IllegalStateException("unchecked failure while resolving the leader");
        }, () -> null, null, server))
        .as("the unchecked failure still reaches the caller")
        .isInstanceOf(IllegalStateException.class);

    assertThat(marker).as("the failed install clears its pending marker over a loadable database").doesNotExist();
    assertThat(staging).as("the failed install drops its staging directory").doesNotExist();
    final ServerDatabase db = server.getDatabaseForOperator(DB_NAME);
    assertThat(db.getSchema().existsType(TYPE)).isTrue();
  }

  @Test
  @Timeout(180)
  void anIoFailureStillCleansUp(@TempDir final Path root) throws Exception {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);
    startServer(root, databasesDir);

    server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE).getSchema().createDocumentType(TYPE);
    final Path dbDir = databasesDir.resolve(DB_NAME);

    assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, SnapshotInstaller.resolveDatabasePath(server, DB_NAME),
        () -> null, () -> null, null, server)).isInstanceOf(IOException.class);

    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
  }

  private void startServer(final Path root, final Path databasesDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_11465");
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
  }
}
