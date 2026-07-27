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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Removes dropped database directories off the caller's thread.
 * <p>
 * A physical delete costs one unlink per file and is unbounded in the size of the database, so the Raft apply
 * loop - which is sequential and shared by every database multiplexed on the state machine - must not perform
 * it. Instead the caller renames the directory to a sibling whose name carries
 * {@link ArcadeDBServer#RESERVED_DATABASE_PREFIX}, which is O(1), and the recursive delete happens here.
 * <p>
 * A staged directory is invisible to the server: every consumer of the databases directory skips reserved
 * names, so it is neither reloaded on restart nor reported in the cluster view, and the database name is free
 * for an immediate re-create. Whatever survives a crash or a shutdown is picked up by
 * {@link #sweepOrphanedStagingDirectories(Path)} on the next start.
 */
class DeferredDatabaseDeleter implements AutoCloseable {

  /** Prefix of the staged directory a dropped database is renamed to. Reserved, so the server ignores it. */
  static final String STAGING_PREFIX = ArcadeDBServer.RESERVED_DATABASE_PREFIX + "dropped-";

  /**
   * Bound on directories awaiting deletion. Beyond it the submitting thread runs the delete itself, which is
   * the behaviour this class exists to avoid - but degrading to it is preferable to growing the queue without
   * limit, and reaching it needs more concurrently pending drops than a cluster realistically produces. Callers
   * holding a lock across the staging step must still call {@link #deleteInBackground(Path)} outside it, so
   * that a caller-runs fallback never widens the lock hold to the length of a recursive delete.
   */
  private static final int MAX_PENDING_DELETIONS = 1024;

  /** Candidate staging names tried before giving up and deleting inline. */
  private static final int STAGING_NAME_ATTEMPTS = 16;

  /** Throttle window for the queue-saturation warning, matching the engine pools. */
  private static final long SATURATION_WARNING_WINDOW_MS = 60_000;

  /** Makes every staging name minted by this JVM distinct, independently of the clock's granularity. */
  private static final AtomicLong STAGING_SEQUENCE = new AtomicLong();

  private final ExecutorService executor;
  private final AtomicLong     lastSaturationWarningOn = new AtomicLong(Long.MIN_VALUE);

  DeferredDatabaseDeleter() {
    this(new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(MAX_PENDING_DELETIONS), r -> {
      final Thread t = new Thread(r, "arcadedb-sm-database-deleter");
      t.setDaemon(true);
      return t;
    }, new ThreadPoolExecutor.AbortPolicy()));
  }

  // @VisibleForTesting
  DeferredDatabaseDeleter(final ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Renames the given database directory to a reserved sibling and returns the staged path, leaving the caller
   * to hand it to {@link #deleteInBackground(Path)}. Returns {@code null} when the directory does not exist, or
   * when the rename was not possible and the deletion had to be performed inline.
   * <p>
   * The database must already be closed and deregistered from the server. On return its directory is gone under
   * its own name either way, so an immediate re-create of the same name succeeds.
   */
  Path stageForDeletion(final Path databaseDirectory) {
    if (!Files.exists(databaseDirectory))
      return null;

    final Path staged = rename(databaseDirectory);
    if (staged == null) {
      // The rename is not available on this filesystem: fall back to deleting where we stand, which is the
      // behaviour that predates the deferral - slow, but never leaves the directory behind under a name the
      // server would reload.
      LogManager.instance().log(this, Level.WARNING,
          "Cannot stage database directory '%s' for deferred deletion: deleting it inline", databaseDirectory);
      FileUtils.deleteRecursively(databaseDirectory.toFile());
    }
    return staged;
  }

  /**
   * Queues the recursive deletion of an already staged directory. Call it outside any lock held across
   * {@link #stageForDeletion(Path)}: on a saturated queue the deletion runs on the calling thread.
   */
  void deleteInBackground(final Path staged) {
    submitDeletion(staged);
  }

  /**
   * Queues the deletion of every staged directory left in the databases directory by a crash, or by a shutdown
   * that cut a pending deletion short.
   */
  void sweepOrphanedStagingDirectories(final Path databasesDirectory) {
    for (final Path orphan : listStagingDirectories(databasesDirectory)) {
      LogManager.instance().log(this, Level.INFO, "Resuming deferred deletion of dropped database directory '%s'", orphan);
      submitDeletion(orphan);
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

  private static List<Path> listStagingDirectories(final Path databasesDirectory) {
    if (databasesDirectory == null || !Files.isDirectory(databasesDirectory))
      return List.of();

    final List<Path> staged = new ArrayList<>();
    try (final var entries = Files.newDirectoryStream(databasesDirectory, STAGING_PREFIX + "*")) {
      for (final Path entry : entries)
        if (Files.isDirectory(entry))
          staged.add(entry);
    } catch (final IOException e) {
      LogManager.instance().log(DeferredDatabaseDeleter.class, Level.WARNING,
          "Cannot scan '%s' for dropped database directories", e, databasesDirectory);
    }
    return staged;
  }

  /**
   * Moves the directory aside under a name that is unique among its siblings, so a drop-create-drop sequence on
   * the same database name never collides with a deletion still in flight.
   */
  // Package-private and overridable so a test can simulate a filesystem that refuses the rename.
  Path rename(final Path databaseDirectory) {
    final Path parent = databaseDirectory.getParent();
    final String name = databaseDirectory.getFileName().toString();

    for (int attempt = 0; attempt < STAGING_NAME_ATTEMPTS; attempt++) {
      // A JVM-wide sequence rather than the clock alone: on a coarse-grained nanoTime two candidates in this
      // loop could otherwise repeat, making the retry budget spin on the same name.
      final Path candidate = parent.resolve(
          STAGING_PREFIX + name + "-" + System.nanoTime() + "-" + STAGING_SEQUENCE.incrementAndGet());
      try {
        moveDirectory(databaseDirectory, candidate);
        return candidate;
      } catch (final FileAlreadyExistsException | DirectoryNotEmptyException e) {
        // The name is taken - an empty candidate raises FileAlreadyExists, a populated one raises
        // DirectoryNotEmpty because a rename onto a non-empty directory cannot succeed. Try the next name.
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Cannot move database directory '%s' to '%s'", e,
            databaseDirectory, candidate);
        return null;
      }
    }
    LogManager.instance().log(this, Level.WARNING,
        "Cannot find a free staging name for database directory '%s' after %d attempts", databaseDirectory,
        STAGING_NAME_ATTEMPTS);
    return null;
  }

  private static void moveDirectory(final Path from, final Path to) throws IOException {
    try {
      Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(from, to);
    }
  }

  private void submitDeletion(final Path staged) {
    try {
      executor.execute(() -> delete(staged));
      return;
    } catch (final RejectedExecutionException e) {
      if (executor.isShutdown()) {
        // Shutting down: the directory is reserved, so it stays invisible to the server and the startup
        // sweep deletes it on the next start.
        LogManager.instance().log(this, Level.FINE,
            "Deferred deletion of '%s' rejected during shutdown: it will be retried on the next restart", staged);
        return;
      }
      warnSaturated(staged);
    }
    // Caller-runs, outside the catch so a failure in delete() is not mistaken for a rejection.
    delete(staged);
  }

  private static void delete(final Path staged) {
    try {
      FileUtils.deleteRecursively(staged.toFile());
      LogManager.instance().log(DeferredDatabaseDeleter.class, Level.FINE,
          "Deleted dropped database directory '%s'", staged);
    } catch (final Exception e) {
      LogManager.instance().log(DeferredDatabaseDeleter.class, Level.WARNING,
          "Error deleting dropped database directory '%s': it will be retried on the next restart", e, staged);
    }
  }

  /**
   * Warns that the deletion queue is saturated and the caller is about to run the delete itself. Throttled to
   * one message per window, matching the other pools, because a saturated queue produces one rejection per drop.
   */
  private void warnSaturated(final Path staged) {
    final long now = System.currentTimeMillis();
    final long last = lastSaturationWarningOn.get();
    if (now - last >= SATURATION_WARNING_WINDOW_MS && lastSaturationWarningOn.compareAndSet(last, now))
      LogManager.instance().log(this, Level.WARNING, """
          Dropped-database deletion queue is full (%d pending): deleting '%s' on the calling thread. Sustained \
          occurrences mean database drops are stalling the Raft apply loop""", MAX_PENDING_DELETIONS, staged);
  }
}
