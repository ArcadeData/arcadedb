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
import com.arcadedb.exception.DatabaseOperationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8316: the slow path of {@link ArcadeDBServer#getDatabase(String)} reused any instance registered in
 * {@link DatabaseFactory}'s active-instance map for the same path without asking whether it was still open. A close that
 * failed half way left its CLOSED instance there, and the lookup - including the HA snapshot installer's reopen after a
 * successful swap - registered that dead instance instead of opening the installed files. The engine now unregisters a
 * closed instance even when its close throws; this pins the server's own guard for the window in which one is still
 * registered, staged by putting a closed instance back in the map.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8316ClosedActiveInstanceNotReusedTest {
  private static final String DB = "issue8316";

  @TempDir
  private Path serverDir;

  private ArcadeDBServer server;
  private Path           dbPath;
  private Database       stale;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);
    dbPath = serverDir.resolve(DB).toAbsolutePath().normalize();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (stale != null)
      activeInstances().remove(dbPath, stale);
    final Database live = DatabaseFactory.getActiveDatabaseInstance(dbPath.toString());
    if (live != null && live.isOpen())
      live.drop();
  }

  @Test
  void anOpenActiveInstanceIsStillReused() {
    final Database opened = new DatabaseFactory(dbPath.toString()).create();

    assertThat(server.getDatabase(DB).getWrappedDatabaseInstance())
        .as("a plugin that opened the database at startup shares its instance with the server")
        .isSameAs(opened);
  }

  @Test
  void aClosedActiveInstanceIsNeverRegistered() throws Exception {
    final Database closed = new DatabaseFactory(dbPath.toString()).create();
    closed.close();
    // What a close that marked the instance closed and then failed used to leave behind.
    activeInstances().put(dbPath, closed);
    stale = closed;

    assertThatThrownBy(() -> server.getDatabase(DB))
        .as("the lookup refuses the path as still in use rather than serving a closed database")
        .isInstanceOf(DatabaseOperationException.class);
    assertThat(server.getDatabaseNames()).doesNotContain(DB);

    // Once the stale entry is gone, the same lookup opens the files.
    activeInstances().remove(dbPath, closed);
    stale = null;
    final ServerDatabase reopened = server.getDatabase(DB);
    assertThat(reopened.isOpen()).isTrue();
    assertThat(reopened.getWrappedDatabaseInstance()).isNotSameAs(closed);
  }

  @SuppressWarnings("unchecked")
  private static Map<Path, Database> activeInstances() throws Exception {
    final Field field = DatabaseFactory.class.getDeclaredField("ACTIVE_INSTANCES");
    field.setAccessible(true);
    return (Map<Path, Database>) field.get(null);
  }
}
