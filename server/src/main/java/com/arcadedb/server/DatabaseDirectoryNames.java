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

import com.arcadedb.log.LogManager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * The names of the databases in the database directory, open or not, answered from memory (issue #10567).
 * <p>
 * A protocol that guesses whether a name is a database (Redis takes {@code <database>.<key>} for a database prefix only when it
 * is one, and for part of a plain key otherwise) asks that question on every command whose key has a dot in it, so it cannot cost
 * a look at the disk each time: that is the hot path of every plain dotted key ({@code user.session.1}). The listing is taken once
 * and taken again only when something may have changed it:
 * <ul>
 *   <li>the server changed the directory itself ({@link #invalidate()}: a database deregistered, dropped, restored), which is seen
 *   at once;</li>
 *   <li>something else did (an operator copying a database in, deleting one). That is checked at most once per
 *   {@link #RECHECK_INTERVAL_NANOS}, with one stat of the directory's modification time plus one per sub-directory that held no
 *   database at the last listing (a copy in progress is a directory that gets its schema file last). A listing taken within the
 *   granularity of that time of the last change is not trusted to have seen it, and is taken again at the next check.</li>
 * </ul>
 * A directory counts when {@code isDatabase} accepts it: one that holds a database or that a snapshot install left pending. Any
 * other directory (one the operating system or the storage put there) is not a database whatever its name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DatabaseDirectoryNames {
  static final long RECHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
  /** A listing taken this close to the directory's last change may have missed a change made in the same tick of its clock. */
  private static final long MODIFIED_TIME_GRANULARITY_MS = 2_000L;

  private final File              directory;
  private final Predicate<File>   isDatabase;
  private final Predicate<String> isReserved;
  /** Moved by every invalidation: a listing that began before one is not published as current. */
  private final AtomicLong        generation = new AtomicLong();
  /** Taken by the one caller that runs a due re-check. */
  private final AtomicBoolean     checking   = new AtomicBoolean();
  private volatile Listing        listing;

  /**
   * @param names            the databases found
   * @param candidates       the sub-directories that held no database, re-checked at every check
   * @param modified         the directory's modification time the listing was taken against
   * @param trusted          false when the listing was taken within {@link #MODIFIED_TIME_GRANULARITY_MS} of that time
   * @param generation       the {@link #generation} the listing began under
   * @param nextCheckNanos   when the next check is due
   */
  private record Listing(Set<String> names, List<File> candidates, long modified, boolean trusted, long generation,
                         long nextCheckNanos) {
  }

  DatabaseDirectoryNames(final File directory, final Predicate<File> isDatabase, final Predicate<String> isReserved) {
    this.directory = directory;
    this.isDatabase = isDatabase;
    this.isReserved = isReserved;
  }

  /** Whether the database directory holds a database named {@code name}, open or not. */
  boolean contains(final String name) {
    Listing current = listing;
    final long now = System.nanoTime();
    if (current == null || current.generation != generation.get())
      current = list();
    else if (now - current.nextCheckNanos >= 0 && checking.compareAndSet(false, true))
      // one caller re-checks, the others answer from the listing in force meanwhile: a busy port would otherwise stat the
      // directory once per concurrent command at every due check
      try {
        current = check(current, now);
      } finally {
        checking.set(false);
      }
    return current.names.contains(name);
  }

  /** The server changed the directory: the next question lists it again. */
  void invalidate() {
    generation.incrementAndGet();
    listing = null;
  }

  private Listing check(final Listing current, final long now) {
    if (!current.trusted || directory.lastModified() != current.modified)
      return list();
    for (final File candidate : current.candidates)
      if (isDatabase.test(candidate))
        return list();
    final Listing rechecked = new Listing(current.names, current.candidates, current.modified, true, current.generation,
        now + RECHECK_INTERVAL_NANOS);
    publish(rechecked);
    return rechecked;
  }

  private Listing list() {
    final long startedAt = generation.get();
    // read BEFORE the listing: a change made while it runs moves the time past this value, and the next check lists again
    final long modified = directory.lastModified();
    final Set<String> names = new HashSet<>();
    final List<File> candidates = new ArrayList<>();
    final File[] children = directory.listFiles(File::isDirectory);
    if (children == null && directory.exists())
      // an I/O error: until the next check the databases on disk that are not open look absent, so a prefix that names one is a
      // plain key again. Said where an operator looks
      LogManager.instance().log(this, Level.WARNING, "Cannot list the database directory '%s': the databases that are not open "
          + "are not recognized as such until it can be listed again", directory);
    if (children != null)
      for (final File child : children) {
        if (isReserved.test(child.getName()))
          continue;
        if (isDatabase.test(child))
          names.add(child.getName());
        else
          candidates.add(child);
      }
    final boolean trusted = System.currentTimeMillis() - modified >= MODIFIED_TIME_GRANULARITY_MS;
    final Listing fresh = new Listing(Set.copyOf(names), List.copyOf(candidates), modified, trusted, startedAt,
        System.nanoTime() + RECHECK_INTERVAL_NANOS);
    publish(fresh);
    return fresh;
  }

  /** Published only when no invalidation came in meanwhile: an older listing must not hide one the server asked for. */
  private void publish(final Listing fresh) {
    if (fresh.generation == generation.get())
      listing = fresh;
  }
}
