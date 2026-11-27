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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The name a client of a wire protocol (Postgres, Bolt, gRPC, Redis, MongoDB, MCP, Gremlin) gives is resolved like the one an HTTP
 * request gives: a name reserved for the server's own use, and a directory that holds no database, are not databases, and naming
 * them opens nothing and creates nothing on disk.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ClientDatabaseNameResolutionTest extends BaseGraphServerTest {
  private static final String[] NOT_DATABASES = { ".raft", ".raft-storage", "lost+found", "plainFolder" };
  private static final String   RESERVED_COPY = ".reservedCopy";
  private static final String   ON_DISK_NAME  = "OnDiskNotOpen";

  @Test
  void aClientNamingADirectoryThatHoldsNoDatabaseIsRefusedAndLeavesItUntouched() {
    final ArcadeDBServer server = getServer(0);

    for (final String name : NOT_DATABASES) {
      final File directory = new File(databaseDirectory(server), name);
      assertThat(directory.mkdirs()).as("created '%s'", name).isTrue();
      try {
        assertThatThrownBy(() -> server.getDatabaseForClient(name)).as("client, '%s'", name)
            .isInstanceOf(DatabaseNotFoundException.class);
        assertThatThrownBy(() -> server.getDatabaseForRequest(name)).as("HTTP, '%s'", name)
            .isInstanceOf(DatabaseNotFoundException.class);

        assertThat(server.existsDatabase(name)).as("nothing registered under '%s'", name).isFalse();
        assertThat(directory.list()).as("nothing written into '%s'", name).isEmpty();
      } finally {
        FileUtils.deleteRecursively(directory);
      }
    }
  }

  @Test
  void aClientNamingADirectoryThatDoesNotExistGetsTheAnswerOfAMissingDatabase() {
    final ArcadeDBServer server = getServer(0);

    assertThatThrownBy(() -> server.getDatabaseForClient("neverCreated")).isInstanceOf(DatabaseNotFoundException.class);
    assertThat(new File(databaseDirectory(server), "neverCreated")).doesNotExist();
  }

  @Test
  void aReservedNameAnOperatorStepOpenedIsNotServedToAClientOrToHttp() {
    final ArcadeDBServer server = getServer(0);
    final File directory = new File(databaseDirectory(server), RESERVED_COPY);

    new DatabaseFactory(directory.getPath()).create().close();
    try {
      // An operator's own step (the snapshot installer, a restore) opens a reserved name through its entry point
      assertThat(server.getDatabaseForOperator(RESERVED_COPY).isOpen()).isTrue();
      assertThat(server.existsDatabase(RESERVED_COPY)).isTrue();

      assertThatThrownBy(() -> server.getDatabaseForClient(RESERVED_COPY)).as("client")
          .isInstanceOf(DatabaseNotFoundException.class);
      assertThatThrownBy(() -> server.getDatabaseForRequest(RESERVED_COPY)).as("HTTP")
          .isInstanceOf(DatabaseNotFoundException.class);
    } finally {
      if (server.existsDatabase(RESERVED_COPY)) {
        server.getRegisteredDatabase(RESERVED_COPY).getEmbedded().close();
        server.removeDatabase(RESERVED_COPY);
      }
      FileUtils.deleteRecursively(directory);
    }
  }

  @Test
  void aClientStillOpensADatabaseOnDiskThatNothingOpenedYet() {
    final ArcadeDBServer server = getServer(0);

    final ServerDatabase created = server.createDatabase(ON_DISK_NAME, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(ON_DISK_NAME);
    assertThat(server.existsDatabase(ON_DISK_NAME)).isFalse();

    try {
      assertThat(server.getDatabaseForClient(ON_DISK_NAME).isOpen()).isTrue();
      assertThat(server.existsDatabase(ON_DISK_NAME)).isTrue();
    } finally {
      if (server.existsDatabase(ON_DISK_NAME))
        server.dropLocalDatabase(server.getDatabaseForOperator(ON_DISK_NAME).getEmbedded(), ON_DISK_NAME);
      FileUtils.deleteRecursively(new File(databaseDirectory(server), ON_DISK_NAME));
    }
  }

  private static String databaseDirectory(final ArcadeDBServer server) {
    return server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
  }
}
