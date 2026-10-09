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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * Real, empty databases served the way a server serves them - wrapped in a {@link ServerDatabase} - for tests that need
 * a database handle without a running server (issue #9464). Every database lives in its own directory under
 * {@code target/served-databases/}, and is closed and deleted after each test.
 * <p>
 * Register it as a field: {@code @RegisterExtension final ServedDatabases served = new ServedDatabases();}, or as a
 * {@code static} one for {@code static} helpers, with the same caveat as {@link UnstartedHttpServers} about concurrent
 * method execution.
 */
public final class ServedDatabases implements AfterEachCallback {
  private final List<DatabaseInternal> databases   = new CopyOnWriteArrayList<>();
  private final List<File>             directories = new CopyOnWriteArrayList<>();

  /**
   * A real, open, empty database named {@code name}, served by no server: the {@link ServerDatabase} gets a
   * {@code null} server, so the database is bound to no backup coordinator or backup directory, and its queries are not
   * profiled. A test of either needs a database served by a real server.
   */
  public ServerDatabase open(final String name) {
    final DatabaseInternal database = create(name);
    return new ServerDatabase(null, database);
  }

  /** A real database named {@code name} that was opened and then closed: a handle a server still holds on to. */
  public ServerDatabase closed(final String name) {
    final DatabaseInternal database = create(name);
    database.close();
    return new ServerDatabase(null, database);
  }

  /** How many databases are waiting to be closed and deleted: zero after every test. */
  int pending() {
    return directories.size();
  }

  private DatabaseInternal create(final String name) {
    final Path directory = Path.of("target", "served-databases", UUID.randomUUID().toString(), name).toAbsolutePath();
    directories.add(directory.getParent().toFile());
    try (final DatabaseFactory factory = new DatabaseFactory(directory.toString())) {
      final DatabaseInternal database = (DatabaseInternal) factory.create();
      databases.add(database);
      return database;
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    // One database failing to close must not leave the others open, nor their directories behind
    for (final DatabaseInternal database : databases)
      if (database.isOpen())
        CodeUtils.executeIgnoringExceptions(database::close, "Error on closing a test database", true);
    for (final File directory : directories) {
      FileUtils.deleteRecursively(directory);
      // A file something still holds open survives the delete: say so, rather than leak it silently
      if (directory.exists())
        LogManager.instance().log(this, Level.WARNING, "Test database directory '%s' could not be deleted", directory);
    }
    databases.clear();
    directories.clear();
  }
}
