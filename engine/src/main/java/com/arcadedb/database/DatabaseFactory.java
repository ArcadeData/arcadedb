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
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class DatabaseFactory implements AutoCloseable {
  private              SecurityManager                                            security;
  private              boolean                                                    autoTransaction      = false;
  private              boolean                                                    replicatedNode       = false;
  private final static Charset                                                    DEFAULT_CHARSET      = StandardCharsets.UTF_8;
  private static final Map<Path, Database>                                        ACTIVE_INSTANCES     = new ConcurrentHashMap<>();
  private static final Map<Path, PathLock>                                        OPEN_LOCKS           = new ConcurrentHashMap<>();
  /**
   * The databases whose committed pages may be in neither a forced data file nor a forced WAL because of how their last
   * instance closed (issue #12025), keyed by path, valued by the {@link LocalDatabase#getInstanceSequence() sequence} of the
   * newest instance that put them there. See {@link #getDatabasesClosedWithPagesLeftToRecovery()}.
   */
  private static final ConcurrentHashMap<Path, Long>                               PAGES_LEFT_TO_RECOVERY = new ConcurrentHashMap<>();
  private final        ContextConfiguration                                       contextConfiguration = new ContextConfiguration();
  private final        String                                                     databasePath;
  private final        Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks            = new ConcurrentHashMap<>();

  /**
   * Milliseconds the JVM shutdown hook waits for the graceful close of the databases still open. On expiry the
   * hook returns anyway so the JVM can complete its shutdown: the close it abandons is exactly a crash, which the
   * WAL replay of the next open repairs. Only pathological states can reach it - the flush itself is already
   * bounded by {@code arcadedb.flushAllPagesTimeout} and the only unbounded steps are acquiring the database lock
   * from a daemon thread that never releases it, and waiting for a snapshot window (a backup) that never closes
   * (#7458).
   */
  private static final long SHUTDOWN_CLOSE_TIMEOUT_MS = 30_000;

  /**
   * Milliseconds the JVM shutdown hook waits for an owning hook (see {@link #registerOwningShutdownHook}) that the JVM
   * has not started yet. The JVM starts every hook at once, so a hook still unstarted past this grace is one nobody is
   * going to start - removed from the runtime without being unregistered here - and waiting longer would only delay the
   * exit.
   */
  private static final long OWNING_HOOK_START_GRACE_MS = 5_000;

  /**
   * The JVM shutdown hooks of the components that close, themselves and in their own order, the databases they opened
   * (issue #9548). The JVM runs every shutdown hook concurrently, so without this the hook below closed a server's
   * databases while that server's own hook was still stopping what writes into them - the Raft apply thread of a leader
   * among them, which then failed to publish a transaction the cluster had already committed and quarantined the
   * database. Kept in a concurrent set: a server registers from its own thread, the hook reads it during the shutdown.
   */
  private static final Set<Thread> OWNING_SHUTDOWN_HOOKS = ConcurrentHashMap.newKeySet();

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

    // EITHER SEPARATOR CONVENTION: A CALLER-SUPPLIED PATH IS ROUTINELY WRITTEN WITH '/' EVEN ON WINDOWS, WHERE A
    // File.separator-ONLY CHECK LEFT THE TRAILING '/' IN PLACE AND EVERY PATH BUILT FROM IT DOUBLY SEPARATED. THIS
    // IS THE SAME STRIP LocalDatabase APPLIES BEFORE IT DERIVES THE DATABASE NAME, AND THE TWO HAVE TO AGREE: THIS
    // CLASS HANDS THAT CONSTRUCTOR THE PATH IT KEEPS HERE (ISSUE #7588)
    databasePath = FileUtils.stripTrailingSeparator(path);
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
    final Path path = getRegistryKey(databasePath);
    final PathLock lock = acquirePathLock(path);
    try {
      synchronized (lock) {
        return openLocked(mode);
      }
    } finally {
      releasePathLock(path);
    }
  }

  /**
   * The monitor of one database PATH. {@code open} and {@code create} are {@code synchronized} on their factory, which is a new
   * object per caller, so threads opening the same path passed {@link #checkForActiveInstance} together and each built a whole
   * {@link LocalDatabase} over the same files. The one refused afterwards still ran its open and its failure clean-up, and a
   * {@code LocalDatabase} equals every other of its path, so that clean-up dropped the winner's cached and queued pages and rolled
   * back its transactions (record counters off, commits failing with NullPointerException). Callers of one path now take turns:
   * the second finds the winner registered and is refused before it builds anything.
   * <p>
   * Counted, and removed with its last user: a monitor kept for every path ever opened would grow for the life of the JVM in an
   * application that creates databases under names it never reuses.
   */
  private static final class PathLock {
    // GUARDED BY THE ConcurrentHashMap BIN LOCK: ONLY EVER TOUCHED INSIDE compute / computeIfPresent
    private int users;
  }

  private static PathLock acquirePathLock(final Path path) {
    return OPEN_LOCKS.compute(path, (key, lock) -> {
      final PathLock result = lock != null ? lock : new PathLock();
      result.users++;
      return result;
    });
  }

  private static void releasePathLock(final Path path) {
    OPEN_LOCKS.computeIfPresent(path, (key, lock) -> --lock.users == 0 ? null : lock);
  }

  /** Whether an {@code open} or {@code create} of {@code databasePath} is in flight or queued: false once none is (test only). */
  // @VisibleForTesting
  static boolean isPathLockInUse(final String databasePath) {
    return OPEN_LOCKS.containsKey(getRegistryKey(databasePath));
  }

  private Database openLocked(final ComponentFile.MODE mode) {
    checkForActiveInstance(databasePath);

    // #4927: refcounted acquire under the PageManager's global lifecycle lock - the previous
    // ACTIVE_INSTANCES.isEmpty() check-then-act raced concurrent open/close across factory instances.
    PageManager.INSTANCE.acquire();

    LocalDatabase database = null;
    try {
      database = new LocalDatabase(databasePath, mode, contextConfiguration, security, callbacks);
      database.setAutoTransaction(autoTransaction);
      database.setReplicatedNode(replicatedNode);
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
    final Path path = getRegistryKey(databasePath);
    final PathLock lock = acquirePathLock(path);
    try {
      synchronized (lock) {
        return createLocked();
      }
    } finally {
      releasePathLock(path);
    }
  }

  private Database createLocked() {
    checkForActiveInstance(databasePath);

    // #4927: refcounted acquire under the PageManager's global lifecycle lock - the previous
    // ACTIVE_INSTANCES.isEmpty() check-then-act raced concurrent open/close across factory instances.
    PageManager.INSTANCE.acquire();

    LocalDatabase database = null;
    try {
      database = new LocalDatabase(databasePath, ComponentFile.MODE.READ_WRITE, contextConfiguration, security, callbacks);
      database.setAutoTransaction(autoTransaction);
      database.setReplicatedNode(replicatedNode);
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

  /**
   * Opens the database as a node of an HA cluster, where the files of a replicated change can arrive before the schema entry
   * that names them (issue #11240). Not a configuration setting on purpose: a database's own {@code configuration.json}
   * replaces the context configuration at open, and with it whatever the opener put there.
   */
  public synchronized DatabaseFactory setReplicatedNode(final boolean replicatedNode) {
    this.replicatedNode = replicatedNode;
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
    final List<Callable<Void>> callbacks = this.callbacks.computeIfAbsent(event, k -> new CopyOnWriteArrayList<>());
    callbacks.add(callback);
  }

  private static Path getNormalizedPath(final String path) {
    return Path.of(path).toAbsolutePath().normalize();
  }

  /**
   * The key of the registry of open instances, and of the per-path monitors of {@code open} and {@code create}: the REAL path of
   * the directory, which sees through a symbolic link (and, where the file system is case insensitive, through a difference of
   * case) that {@link #getNormalizedPath} does not. A second factory reaching an open database by a link used to pass the
   * "already in use" check, build an instance over the same files and be refused by the file lock, whose clean-up closed the lock
   * file: on Linux and macOS that releases every lock the process holds on the file, so the winner stayed registered but no other
   * process was kept out of its database any more. A path that does not exist (yet) has no real path and keeps its normalised one.
   */
  private static Path getRegistryKey(final String path) {
    final Path normalized = getNormalizedPath(path);
    try {
      return normalized.toRealPath();
    } catch (final IOException | SecurityException e) {
      return normalized;
    }
  }

  public static Database getActiveDatabaseInstance(final String databasePath) {
    return ACTIVE_INSTANCES.get(getRegistryKey(databasePath));
  }

  protected static boolean removeActiveDatabaseInstance(final String databasePath, final Database instance) {
    // Keyed to the instance (#5070): when registerActiveInstance closes a same-path open-race LOSER,
    // the loser's close must not remove the WINNER's still-live mapping - a plain remove(path) did, orphaning
    // the winner from the registry and letting a third open of the same path pass checkForActiveInstance.
    // By IDENTITY, not Map.remove(key, value): that compares with equals(), and a LocalDatabase equals every other one of its path,
    // so the loser's removal took the winner's entry anyway
    final Path key = getRegistryKey(databasePath);
    if (ACTIVE_INSTANCES.get(key) == instance)
      ACTIVE_INSTANCES.computeIfPresent(key, (path, registered) -> registered == instance ? null : registered);
    else
      // THE REAL PATH THE INSTANCE WAS REGISTERED UNDER IS GONE WITH ITS DIRECTORY (A DELETE FROM OUTSIDE, A DROP): FIND IT BY IDENTITY
      ACTIVE_INSTANCES.values().removeIf(registered -> registered == instance);
    return ACTIVE_INSTANCES.isEmpty();
  }

  public static Collection<Database> getActiveDatabaseInstances() {
    return Collections.unmodifiableCollection(ACTIVE_INSTANCES.values());
  }

  /**
   * The databases, not open now, whose last instance closed without making its pages durable: the bounded flush wait gave
   * up, an fsync failed, the database was fenced, or a WAL record or a columnar recovery set could not be written (issue
   * #12025). Such a close keeps the WAL and the lock file, so the next open replays it, but the WAL of a replicated commit
   * is not forced (#11746) and a follower writes none, so until a later instance has forced the files again the committed
   * pages of those databases may be in the OS cache only. A caller that is about to declare "everything committed so far
   * is on the disk" - the Raft snapshot checkpoint - must not include them.
   * <p>
   * A path is listed from the moment its close starts (the instance reports itself closed before the close decides how it
   * ended) until a clean close of an instance at least as new, a drop, or a successful
   * {@link com.arcadedb.engine.PageManager#flushAndSyncDatabase} of a NEWER instance, whose open ran the recovery and so
   * forces every file at its first sync. A path whose directory is gone has nothing left to lose and is forgotten here.
   */
  public static List<Path> getDatabasesClosedWithPagesLeftToRecovery() {
    if (PAGES_LEFT_TO_RECOVERY.isEmpty())
      return List.of();
    final List<Path> result = new ArrayList<>();
    for (final Map.Entry<Path, Long> entry : PAGES_LEFT_TO_RECOVERY.entrySet()) {
      final Path path = entry.getKey();
      if (!new File(path.toString()).isDirectory())
        PAGES_LEFT_TO_RECOVERY.remove(path, entry.getValue());
      else
        // Listed even while a newer instance is open: only its successful sync releases the path, and one opened after the
        // caller's own sync pass has not been forced yet
        result.add(path);
    }
    return result;
  }

  /** Lists the path of a closing instance (issue #12025), before it reports itself closed. */
  static void markClosingWithPagesAtRisk(final LocalDatabase closing) {
    PAGES_LEFT_TO_RECOVERY.merge(getNormalizedPath(closing.getDatabasePath()), closing.getInstanceSequence(), Math::max);
  }

  /**
   * Unlists the path of {@code synced} once it forced its files (issue #12025), when an EARLIER instance listed it: this one
   * opened after that close, so its open ran the recovery, which leaves every file to be forced by the first sync.
   */
  public static void releasePagesLeftToRecoveryByEarlierInstances(final LocalDatabase synced) {
    releasePagesLeftToRecovery(synced, false);
  }

  /**
   * Unlists a path once {@code instance} made its pages durable (issue #12025): its clean close or its drop when
   * {@code includingOwn}, otherwise a sync of a newer instance than the one that listed it.
   */
  static void releasePagesLeftToRecovery(final LocalDatabase instance, final boolean includingOwn) {
    final long sequence = instance.getInstanceSequence();
    PAGES_LEFT_TO_RECOVERY.computeIfPresent(getNormalizedPath(instance.getDatabasePath()),
        (path, listedBy) -> listedBy < sequence || (includingOwn && listedBy == sequence) ? null : listedBy);
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
   * {@code isOpen()} check just above is not atomic with the {@code close()} call, so both threads can see it
   * open and both call in - {@link LocalDatabase#close()} is what makes the second call a no-op, by waiting for
   * the first one's teardown to finish rather than repeating (or racing) any part of it (issue #8356).
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
    // FIRST, before even looking at the registry: an owning hook may still be about to close what is in it, in the order
    // its databases need (issue #9548). What it leaves open - it gave up, or it owns only some of them - is closed below.
    awaitOwningShutdownHooks(OWNING_HOOK_START_GRACE_MS);

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

  /**
   * Registers the JVM shutdown hook of a component that closes the databases it opened ITSELF, in an order it controls,
   * so that the engine's own shutdown hook waits for that hook to finish before closing whatever is still open (issue
   * #9548). The ArcadeDB server is the one such component: it stops its plugins - the Raft HA service, whose apply thread
   * writes into the databases - before it closes the databases, and the JVM, which runs every shutdown hook
   * concurrently, used to let the engine's hook close them in between.
   * <p>
   * The wait itself costs the exit nothing, since the JVM waits for every hook anyway, the registered one included; what
   * it changes is that closing the databases the owner left open (ones it did not open, or all of them if it gave up)
   * now runs after that hook rather than alongside it. A registered hook the JVM never starts (removed from the runtime
   * and not {@link #unregisterOwningShutdownHook unregistered} here) is waited for only for a short grace.
   *
   * @param hook the thread passed to {@link Runtime#addShutdownHook(Thread)}
   */
  public static void registerOwningShutdownHook(final Thread hook) {
    OWNING_SHUTDOWN_HOOKS.add(Objects.requireNonNull(hook, "hook"));
  }

  /** The counterpart of {@link #registerOwningShutdownHook}, for a hook removed from the runtime. */
  public static void unregisterOwningShutdownHook(final Thread hook) {
    OWNING_SHUTDOWN_HOOKS.remove(hook);
  }

  /** Whether {@code hook} is registered through {@link #registerOwningShutdownHook}. */
  public static boolean isOwningShutdownHook(final Thread hook) {
    return OWNING_SHUTDOWN_HOOKS.contains(hook);
  }

  /**
   * Waits for every registered owning hook to finish (issue #9548). A started hook is joined without a bound: the JVM is
   * waiting for it in any case, so a bound here would only re-open the race it exists to close. A hook not started yet
   * is waited for up to {@code startGraceMs}, shared by all of them, since the JVM starts its hooks together and in no
   * particular order. The calling thread itself, if registered, is skipped.
   * <p>
   * An owning hook that never returns therefore keeps this one from closing what that owner left open. That costs
   * nothing the JVM was not already paying: it cannot exit while any hook runs, so a hung owner hangs the exit either
   * way. The ArcadeDB server's hook bounds its own wait for the lifecycle lock ({@code arcadedb.server.shutdownTimeout})
   * for exactly that reason.
   */
  // @VisibleForTesting
  static void awaitOwningShutdownHooks(final long startGraceMs) {
    final long startDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(startGraceMs);
    final Thread current = Thread.currentThread();
    try {
      for (final Thread hook : OWNING_SHUTDOWN_HOOKS) {
        if (hook == current)
          continue;
        while (true) {
          final Thread.State state = hook.getState();
          if (state == Thread.State.TERMINATED)
            break;
          if (state == Thread.State.NEW) {
            if (System.nanoTime() - startDeadline >= 0)
              break;
            Thread.sleep(10);
          } else
            // Unbounded on purpose: the JVM cannot exit before this hook returns either, and giving up early would close
            // the owner's databases under it again, which is the race this wait exists for (see the javadoc)
            hook.join();
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void checkForActiveInstance(final String databasePath) {
    final Path key = getRegistryKey(databasePath);
    if (ACTIVE_INSTANCES.get(key) != null)
      throw new DatabaseOperationException("Found active instance of database '" + key + "' already in use");
  }

  private static void registerActiveInstance(final LocalDatabase database) {
    final Path key = getRegistryKey(database.databasePath);
    if (ACTIVE_INSTANCES.putIfAbsent(key, database) != null) {
      database.close();
      throw new DatabaseOperationException("Found active instance of database '" + key + "' already in use");
    }
  }
}
