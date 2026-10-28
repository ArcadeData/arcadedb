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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10707: a stopped server opens no database. A thread that outlived the services of the server - a security reload sweep
 * that was running when they stopped, a request still in flight - used to reach {@link ArcadeDBServer#getDatabase(String)} after
 * the server had closed its databases, and reopened the directory: registered behind a stopped server, holding its file lock, so
 * the next server on that directory failed to start with {@code OverlappingFileLockException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10707StoppedServerOpensNoDatabaseTest extends BaseGraphServerTest {

  @Test
  void aStoppedServerRefusesToOpenOrCreateADatabase() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String path = server.getDatabase(getDatabaseName()).getDatabasePath();
    server.stop();

    // from another thread, as the security watcher or a late request would
    final CompletableFuture<Object> lateOpen = CompletableFuture.supplyAsync(() -> server.getDatabase(getDatabaseName()));
    assertThatThrownBy(() -> lateOpen.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(DatabaseNotAvailableException.class)
        .hasMessageContaining("stopped");
    assertThatThrownBy(() -> server.getDatabaseForClient(getDatabaseName())).isInstanceOf(DatabaseNotAvailableException.class);
    assertThatThrownBy(() -> server.createDatabase("stopped10707", ComponentFile.MODE.READ_WRITE))
        .isInstanceOf(DatabaseNotAvailableException.class);

    assertThat(server.existsDatabase(getDatabaseName())).as("nothing registered behind the stopped server").isFalse();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(path)).as("nothing open").isNull();

    // the directory is free: what the next server on it does first
    try (final Database reopened = new DatabaseFactory(path).open()) {
      assertThat(reopened.isOpen()).isTrue();
    }
  }

  @Test
  void aServerStartedAgainOpensItsDatabases() {
    final ArcadeDBServer server = getServer(0);
    server.stop();
    server.start();
    assertThat(server.getDatabase(getDatabaseName()).isOpen()).as("a new lifecycle lifts the refusal").isTrue();
  }
}
