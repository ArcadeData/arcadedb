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
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.security.SecurityManager;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class DatabaseFactory implements AutoCloseable {
  private              SecurityManager                                            security;
  private              boolean                                                    autoTransaction      = false;
  private final static Charset                                                    DEFAULT_CHARSET      = StandardCharsets.UTF_8;
  private static final Map<Path, Database>                                        ACTIVE_INSTANCES     = new ConcurrentHashMap<>();
  private final        ContextConfiguration                                       contextConfiguration = new ContextConfiguration();
  private final        String                                                     databasePath;
  private final        Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks            = new HashMap<>();

  /**
   * Milliseconds the JVM shutdown hook waits for the graceful close of the databases still open. On expiry the
   * hook returns anyway so the JVM can complete its shutdown: the close it abandons is exactly a crash, which the
   * WAL replay of the next open repairs. Only pathological states can reach it - the flush itself is already
   * bounded by {@code arcadedb.flushAllPagesTimeout} and the only unbounded step is acquiring the database lock
   * from a daemon thread that never releases it.
   */
  private static final long SHUTDOWN_CLOSE_TIMEOUT_MS = 30_000;

  /**
   * #5418: the engine's background threads are daemons, so a leaked (never closed) {@link Database} no longer
   * keeps the embedder's JVM alive. This hook is the other half of that contract: it runs to completion before
   * the JVM stops the daemon threads, closing every database still registered so its dirty pages reach the disk
   * exactly as they would on an explicit close. Registered from a static initializer: the class is loaded by
   * anything that can open a database, and an unstarted {@link Thread} costs nothing when no database ever is.
   */
  static {
    Runtime.getRuntime().addShutdownHook(new Thread(DatabaseFactory::closeActiveDatabaseInstancesOnShutdown,
        "ArcadeDB-DatabaseShutdownHook"));
  }

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

  /**
   * Closes every database instance that is still open, ignoring any error (issue #5418). Exposed as a public API
   * for embedders (language bindings, application containers) that want the same graceful teardown at an earlier,
   * controlled point of their own lifecycle than the JVM shutdown hook.
   * <p>
   * A closing database removes itself from the registry, so the iteration runs over a snapshot: the map is
   * modified while the loop advances. Errors are swallowed on purpose - a database that cannot be closed must
   * not stop the remaining ones from being closed, least of all during a JVM shutdown. Closing a database that
   * another component is closing concurrently (the ArcadeDB server's own shutdown hook, say) is a no-op: the
   * second close finds it already closed.
   */
  public static void closeActiveDatabaseInstances() {
    for (final Database database : new ArrayList<>(ACTIVE_INSTANCES.values())) {
      try {
        if (database.isOpen())
          database.close();
      } catch (final Throwable e) {
        // IGNORE: BEST EFFORT, KEEP CLOSING THE OTHERS
      }
    }
  }

  /**
   * {@link #closeActiveDatabaseInstances()} under a deadline, run from the JVM shutdown hook. The close happens on
   * a separate DAEMON thread the hook joins for at most {@link #SHUTDOWN_CLOSE_TIMEOUT_MS}: a shutdown hook that
   * never returns blocks the JVM exit forever, which is the very failure this whole change removes, so it must not
   * be reintroduced here by a database whose lock some other daemon thread never releases.
   */
  private static void closeActiveDatabaseInstancesOnShutdown() {
    if (ACTIVE_INSTANCES.isEmpty())
      return;

    final Thread closer = new Thread(DatabaseFactory::closeActiveDatabaseInstances, "ArcadeDB-DatabaseShutdownCloser");
    closer.setDaemon(true);
    closer.start();
    try {
      closer.join(SHUTDOWN_CLOSE_TIMEOUT_MS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (closer.isAlive())
      LogManager.instance().log(DatabaseFactory.class, Level.WARNING, """
          Could not close %d database(s) within %d ms during the JVM shutdown: giving up so the shutdown can \
          complete. The unflushed pages are replayed from the WAL on the next open""", null, ACTIVE_INSTANCES.size(),
          SHUTDOWN_CLOSE_TIMEOUT_MS);
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
