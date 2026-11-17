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
package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Looks exact keys up through an index and loads their records against ONE committed state (issues #9369 and #9397).
 * <p>
 * A commit puts its pages in the read cache one at a time, and gives a freed record slot to the next record it creates, so
 * the entries of an index read before a commit and the records loaded after it do not belong together: a transaction that
 * deletes and re-creates a key makes it vanish (the entry is read from the old state, the record from the new one), appear
 * twice, or answer another key's record. The same holds across the keys of an {@code IN} list read one after the other: a
 * slot freed by one key's record and taken by another's is served for both. The entries of every key are read and every
 * record loaded between two samples of the page manager's publication sequence that are equal and even, which no commit
 * can have interleaved with; otherwise the lookup starts again, and after {@link #MAX_ATTEMPTS} attempts runs once more with the
 * publications held off ({@link #readAgainstOneCommit}). Costs two volatile reads when no commit overlaps.
 * <p>
 * A publication is waited out with {@link PageManager#awaitPublicationEnd} and kept off with
 * {@link PageManager#callWithPublicationsHeld}, never with the page-manager lock: the replicated apply of a replica publishes
 * without that lock (#10941), so a wait on it returned at once and a read under it could still pair the entries and the records
 * of two commits (#11755).
 * <p>
 * The records are held until all are loaded, so a lookup with more than {@link #MAX_ENTRIES} entries is not served here:
 * {@link #lookup} returns {@code null} and the caller streams the cursors as it did before.
 * <p>
 * The last resort cannot deadlock: a publication held off waits for the hold with the page-manager lock and its file locks
 * taken, and the read under the hold takes neither (it loads pages, which takes no lock); the reader holds no hold while it
 * waits for one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ConsistentKeyLookup {
  /** More entries than this in one lookup are streamed by the caller instead of held. */
  public static final int MAX_ENTRIES  = 256;
  /** Attempts made before the lookup runs once more with the publications held off ({@link #readAgainstOneCommit}). */
  public static final int MAX_ATTEMPTS = 8;
  /**
   * How long an attempt waits for an open publication to end before it is spent, as the readers of {@code LocalBucket} wait
   * (#11505): a replica under write load has one open almost all the time, so an attempt that did not wait would be spent at once.
   */
  public static final long PUBLICATION_WAIT_NANOS      = TimeUnit.MILLISECONDS.toNanos(20);
  /** How long the last attempt waits for the publications in flight to end before it holds the next one off. */
  public static final long PUBLICATION_HOLD_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

  // for tests: called on the thread of a lookup once it has settled on the state it returns, to interleave a commit after it
  private static volatile Runnable afterLookupObserver;

  private ConsistentKeyLookup() {
  }

  /** For tests only: {@code observer} runs after every lookup settled on its state, before it returns; null removes it. */
  public static void setAfterLookupObserver(final Runnable observer) {
    afterLookupObserver = observer;
  }

  /**
   * @param database the database the index belongs to
   * @param index    the index to read
   * @param keys     the whole keys (one value per property of the index) to look up
   *
   * @return the entries of the keys, in the order of the keys, with their records loaded; an entry whose record is gone in
   * the state that was read is dropped as dangling. {@code null} when there are more than {@link #MAX_ENTRIES} entries
   */
  public static List<IndexCursorEntry> lookup(final DatabaseInternal database, final Index index, final List<Object[]> keys) {
    return lookup(database, index, keys, false, false);
  }

  private static List<IndexCursorEntry> lookup(final DatabaseInternal database, final Index index, final List<Object[]> keys,
      final boolean forQuery, final boolean prefix) {
    final List<IndexCursorEntry> entries = lookupConsistently(database, index, keys, forQuery, prefix);
    final Runnable observer = afterLookupObserver;
    if (observer != null && entries != null)
      observer.run();
    return entries;
  }

  private static List<IndexCursorEntry> lookupConsistently(final DatabaseInternal database, final Index index,
      final List<Object[]> keys, final boolean forQuery, final boolean prefix) {
    final PageManager pageManager = database.getPageManager();
    boolean unpinnedForDangling = false;
    Set<Integer> lastBucketIds = Set.of();
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      // a commit or a replicated apply is publishing: wait for it to end instead of spending an attempt inside its window (#11755)
      final long before = pageManager.awaitPublicationEnd(PUBLICATION_WAIT_NANOS);
      if ((before & 1) != 0)
        // still publishing after the wait, or another one started: this attempt did no work, and a sequence odd on every attempt
        // ends in the read against one commit below
        continue;

      final Read read = readAndLoad(database, index, keys, forQuery, prefix);
      if (read == null)
        return null;
      lastBucketIds = read.bucketIds;
      final boolean overlapped = pageManager.getPublicationSequence() != before;
      // an entry without a record, in a REPEATABLE_READ transaction, may be a page this transaction pinned before the
      // record was created: the pins are released and the lookup read once more on the committed state
      final boolean stalePins = !overlapped && read.dangling && !unpinnedForDangling && isRepeatableRead(database);
      if (!overlapped && !stalePins)
        return read.entries;
      if (stalePins)
        unpinnedForDangling = true;
      unpinRepeatableRead(database, index, read.bucketIds);
    }

    LogManager.instance().log(ConsistentKeyLookup.class, Level.FINE,
        "Lookup on index '%s' overlapped a commit %d times, reading it with the publications held off", null, index.getName(),
        MAX_ATTEMPTS);
    // the pages pinned by the attempts that overlapped a commit must not serve the last read either
    unpinRepeatableRead(database, index, lastBucketIds);
    final Read read = readAgainstOneCommit(pageManager, () -> readAndLoad(database, index, keys, forQuery, prefix),
        "Lookup on index '" + index.getName() + "'");
    return read == null ? null : read.entries;
  }

  /**
   * What {@code index.get(key)} answers for each of the keys, read against one committed state: a cursor over their entries
   * with the records loaded, or {@code null} when there are too many entries to be held and the index's own streaming
   * cursors must be used.
   */
  public static IndexCursor lookupCursor(final Database database, final Index index, final List<Object[]> keys) {
    if (!(database instanceof DatabaseInternal internal))
      // a remote database has no page manager to sample: the caller reads the index as it did before
      return null;
    final List<IndexCursorEntry> entries = lookup(internal, index, keys);
    return entries == null ? null : new TempIndexCursor(entries);
  }

  /**
   * What {@code index.get(key)} answers, read against one committed state: the cursor of {@link #lookupCursor}, or the
   * index's own streaming cursor for a key too wide to be held.
   */
  public static IndexCursor get(final Database database, final Index index, final Object[] key) {
    final IndexCursor cursor = lookupCursor(database, index, List.<Object[]>of(key));
    return cursor != null ? cursor : index.get(key);
  }

  /**
   * {@link #get}, reading the index through {@link TypeIndex#getForQuery} (issue #9811): for a reader that answers a client
   * with what it finds, which must never take a lookup a concurrent CREATE, REBUILD or DROP INDEX left short for "the key does
   * not exist". Raises the {@link IndexException} of {@code getForQuery} instead, which every guarded reader treats as "read
   * again" ({@link TypeIndex#isStaleRead}).
   */
  public static IndexCursor getForQuery(final Database database, final TypeIndex index, final Object[] key) {
    if (database instanceof DatabaseInternal internal) {
      final List<IndexCursorEntry> entries = lookup(internal, index, List.<Object[]>of(key), true, false);
      if (entries != null)
        return new TempIndexCursor(entries);
    }
    return index.getForQuery(key);
  }

  /**
   * {@link #getForQuery} for the entries whose leading key columns equal {@code prefix}, a key shorter than the index's (issue
   * #11576), read through {@link TypeIndex#prefixForQuery}: against one committed state when there are at most
   * {@link #MAX_ENTRIES} of them, otherwise whole but without that guarantee, as {@link #getForQuery} answers a key too wide to be
   * held. A reader that must not miss an entry, a MERGE deciding whether to create, reads a prefix this way.
   */
  public static IndexCursor prefixForQuery(final Database database, final TypeIndex index, final Object[] prefix) {
    if (database instanceof DatabaseInternal internal) {
      final List<IndexCursorEntry> entries = lookup(internal, index, List.<Object[]>of(prefix), true, true);
      if (entries != null)
        return new TempIndexCursor(entries);
    }
    return index.prefixForQuery(prefix);
  }

  /**
   * The last attempt of a read that must see one committed state (#11755): runs {@code read} with no publication open and the next
   * one held off ({@link PageManager#callWithPublicationsHeld}), which excludes the local commits and the replicated apply of a
   * replica alike, and takes what it read only when the publication sequence stayed even and did not move under it.
   * <p>
   * When no moment without a publication came within {@link #PUBLICATION_HOLD_WAIT_NANOS} (one commit publishing for longer than
   * that, or a replica applying one entry for longer), the read runs under the page-manager lock, which waits for the local commit
   * and keeps every other local commit out: on a database no replicated entry is applied to that is every publisher, so the read
   * is served as it always was. It is taken only when the sequence stayed even and still under the lock; otherwise a replicated
   * apply overlapped it and the read is refused with a retryable {@link ConcurrentModificationException}, never answered from two
   * commits.
   * <p>
   * {@code read} must be short, must not publish and must not wait for anything a publication holds: it delays every commit and
   * apply of the JVM while it runs. It may run twice, so it must start from scratch every time. A {@code null} it returns is taken
   * as it is: it is the caller's "not held here", which promises no one state.
   *
   * @param what names the read in the message of the conflict
   */
  @SuppressWarnings("unchecked")
  public static <T> T readAgainstOneCommit(final PageManager pageManager, final Supplier<T> read, final String what) {
    final Object[] result = { NOT_READ };
    final Boolean held;
    try {
      held = pageManager.callWithPublicationsHeld(PUBLICATION_HOLD_WAIT_NANOS, () -> readIfOneCommit(pageManager, read, result));
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error reading against one committed state: " + what, e);
    }
    // a null read is the caller's "not held here", which claims no one state: taken whenever the read ran
    if (Boolean.TRUE.equals(held) || (held != null && result[0] == null))
      return (T) result[0];

    final Boolean locked = (Boolean) pageManager.executeInLock(() -> readIfOneCommit(pageManager, read, result));
    if (locked || result[0] == null)
      return (T) result[0];
    throw new ConcurrentModificationException(what + " could not be read against one committed state: a replicated apply kept "
        + "publishing through the whole wait. Please retry the operation");
  }

  /** What {@link #readIfOneCommit} leaves when a publication was open and it did not run the read. */
  private static final Object NOT_READ = new Object();

  /**
   * Runs {@code read} into {@code result}[0], and tells whether no publication was open or started meanwhile; leaves
   * {@link #NOT_READ} there without running it when one was open.
   */
  private static <T> Boolean readIfOneCommit(final PageManager pageManager, final Supplier<T> read, final Object[] result) {
    result[0] = NOT_READ;
    final long sequence = pageManager.getPublicationSequence();
    if ((sequence & 1) != 0)
      return Boolean.FALSE;
    result[0] = read.get();
    return pageManager.getPublicationSequence() == sequence;
  }

  private record Read(List<IndexCursorEntry> entries, Set<Integer> bucketIds, boolean dangling) {
  }

  private static Read readAndLoad(final DatabaseInternal database, final Index index, final List<Object[]> keys,
      final boolean forQuery, final boolean prefix) {
    final List<Object[]> entryKeys = new ArrayList<>(keys.size());
    final List<RID> rids = new ArrayList<>(keys.size());
    for (final Object[] key : keys) {
      final IndexCursor cursor;
      if (prefix)
        cursor = ((TypeIndex) index).prefixForQuery(key);
      else
        cursor = forQuery && index instanceof TypeIndex typeIndex ? typeIndex.getForQuery(key) : index.get(key);
      try {
        while (cursor.hasNext()) {
          if (rids.size() >= MAX_ENTRIES)
            return null;
          rids.add(cursor.next().getIdentity());
          entryKeys.add(key);
        }
      } finally {
        cursor.close();
      }
    }

    final List<IndexCursorEntry> entries = new ArrayList<>(rids.size());
    final Set<Integer> bucketIds = new HashSet<>();
    boolean dangling = false;
    for (int i = 0; i < rids.size(); i++) {
      final RID rid = rids.get(i);
      bucketIds.add(rid.getBucketId());
      try {
        entries.add(new IndexCursorEntry(entryKeys.get(i), database.lookupByRID(rid, true), 1));
      } catch (final RecordNotFoundException e) {
        // in a state no commit overlapped, an entry without a record is dangling and dropped, as the scan of an index does
        dangling = true;
      }
    }
    return new Read(entries, bucketIds, dangling);
  }

  private static boolean isRepeatableRead(final DatabaseInternal database) {
    return database.isTransactionActive()
        && database.getTransactionIsolationLevel() == Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ;
  }

  /**
   * Under REPEATABLE_READ the pages this transaction pinned while a commit overlapped the lookup may be the very two states
   * that disagree: they are released, so the lookup is repeated on the committed state. Only called once an overlap was
   * detected, so a lookup that no commit touched keeps its snapshot.
   */
  private static void unpinRepeatableRead(final DatabaseInternal database, final Index index, final Set<Integer> bucketIds) {
    if (!isRepeatableRead(database))
      return;
    final List<Integer> files = new ArrayList<>(bucketIds);
    if (index instanceof IndexInternal internal)
      files.addAll(internal.getFileIds());
    database.getTransaction().unpinFiles(files);
  }
}
