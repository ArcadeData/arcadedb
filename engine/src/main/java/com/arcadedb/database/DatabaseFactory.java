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
package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.security.SecurityManager;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseFactory implements AutoCloseable {
  private              SecurityManager                                            security;
  private              boolean                                                    autoTransaction      = false;
  private final static Charset                                                    DEFAULT_CHARSET      = StandardCharsets.UTF_8;
  private static final Map<Path, Database>                                        ACTIVE_INSTANCES     = new ConcurrentHashMap<>();
  private final        ContextConfiguration                                       contextConfiguration = new ContextConfiguration();
  private final        String                                                     databasePath;
  private final        Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks            = new HashMap<>();

  public DatabaseFactory(final String path) {
    if (path == null || path.trim().isEmpty())
      throw new IllegalArgumentException("Missing path");

    if (path.endsWith(File.separator))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  @Override
  public synchronized void close() {
    callbacks.clear();
  }

  public boolean exists() {
    boolean exists = new File(databasePath + File.separator + LocalSchema.SCHEMA_FILE_NAME).exists();
    if (!exists)
      exists = new File(databasePath + File.separator + LocalSchema.SCHEMA_PREV_FILE_NAME).exists();
    return exists;
  }

  public String getDatabasePath() {
    return databasePath;
  }

  public Database open() {
    return open(ComponentFile.MODE.READ_WRITE);
  }

  public synchronized Database open(final ComponentFile.MODE mode) {
    checkForActiveInstance(databasePath);

    // #4927: refcounted acquire under the PageManager's global lifecycle lock - the previous
    // ACTIVE_INSTANCES.isEmpty() check-then-act raced concurrent open/close across factory instances.
    PageManager.INSTANCE.acquire();

    LocalDatabase database = null;
    try {
      database = new LocalDatabase(databasePath, mode, contextConfiguration, security, callbacks);
      database.setAutoTransaction(autoTransaction);
      database.open();

      registerActiveInstance(database);

      return database;
    } catch (final Throwable e) {
      // Balance the acquire above so a failed open does not leak the flush thread (#4991) - but EXACTLY
      // once per instance (#5070): when registerActiveInstance loses a same-path open race it closes
      // the database itself, and that close already consumed this reference; releasing again here would
      // double-decrement and could tear the manager down under the race winner.
      if (database == null || !database.isPageManagerReferenceReleased())
        PageManager.INSTANCE.release();
      throw e;
    }
  }

  public synchronized Database create() {
    checkForActiveInstance(databasePath);

    // #4927: refcounted acquire under the PageManager's global lifecycle lock - the previous
    // ACTIVE_INSTANCES.isEmpty() check-then-act raced concurrent open/close across factory instances.
    PageManager.INSTANCE.acquire();

    LocalDatabase database = null;
    try {
      database = new LocalDatabase(databasePath, ComponentFile.MODE.READ_WRITE, contextConfiguration, security, callbacks);
      database.setAutoTransaction(autoTransaction);
      database.create();

      registerActiveInstance(database);

      return database;
    } catch (final Throwable e) {
      // Balance the acquire above so a failed create does not leak the flush thread (#4991) - but EXACTLY
      // once per instance (#5070): see open().
      if (database == null || !database.isPageManagerReferenceReleased())
        PageManager.INSTANCE.release();
      throw e;
    }
  }

  public synchronized DatabaseFactory setAutoTransaction(final boolean enabled) {
    autoTransaction = enabled;
    return this;
  }

  public ContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  public static Charset getDefaultCharset() {
    return DEFAULT_CHARSET;
  }

  public SecurityManager getSecurity() {
    return security;
  }

  public DatabaseFactory setSecurity(final SecurityManager security) {
    this.security = security;
    return this;
  }

  /**
   * Test only API
   */
  public void registerCallback(final DatabaseInternal.CALLBACK_EVENT event, final Callable<Void> callback) {
    final List<Callable<Void>> callbacks = this.callbacks.computeIfAbsent(event, k -> new ArrayList<>());
    callbacks.add(callback);
  }

  private static Path getNormalizedPath(final String path) {
    return Path.of(path).toAbsolutePath().normalize();
  }

  public static Database getActiveDatabaseInstance(final String databasePath) {
    var normalizedPath = getNormalizedPath(databasePath);
    return ACTIVE_INSTANCES.get(normalizedPath);
  }

  protected static boolean removeActiveDatabaseInstance(final String databasePath, final Database instance) {
    final var normalizedPath = getNormalizedPath(databasePath);
    // Keyed to the instance (#5070): when registerActiveInstance closes a same-path open-race LOSER,
    // the loser's close must not remove the WINNER's still-live mapping - a plain remove(path) did, orphaning
    // the winner from the registry and letting a third open of the same path pass checkForActiveInstance.
    ACTIVE_INSTANCES.remove(normalizedPath, instance);
    return ACTIVE_INSTANCES.isEmpty();
  }

  public static Collection<Database> getActiveDatabaseInstances() {
    return Collections.unmodifiableCollection(ACTIVE_INSTANCES.values());
  }

  private static void checkForActiveInstance(final String databasePath) {
    var normalizedPath = getNormalizedPath(databasePath);
    if (ACTIVE_INSTANCES.get(normalizedPath) != null)
      throw new DatabaseOperationException("Found active instance of database '" + normalizedPath + "' already in use");
  }

  private static void registerActiveInstance(final LocalDatabase database) {
    var normalizedPath = getNormalizedPath(database.databasePath);
    if (ACTIVE_INSTANCES.putIfAbsent(normalizedPath, database) != null) {
      database.close();
      throw new DatabaseOperationException("Found active instance of database '" + normalizedPath + "' already in use");
    }
  }
}
