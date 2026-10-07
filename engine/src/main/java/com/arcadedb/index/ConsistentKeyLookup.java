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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * can have interleaved with; otherwise the lookup starts again, and after {@link #MAX_ATTEMPTS} attempts runs under the
 * publication lock, which keeps every commit out. Costs two volatile reads when no commit overlaps.
 * <p>
 * The records are held until all are loaded, so a lookup with more than {@link #MAX_ENTRIES} entries is not served here:
 * {@link #lookup} returns {@code null} and the caller streams the cursors as it did before.
 * <p>
 * The last-resort lock cannot deadlock: it is the page-manager lock, which is reentrant and which a committer takes last,
 * inside the file locks, only to publish pages (see the same argument in {@code GetValueFromIndexEntryStep}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ConsistentKeyLookup {
  /** More entries than this in one lookup are streamed by the caller instead of held. */
  public static final int MAX_ENTRIES  = 256;
  public static final int MAX_ATTEMPTS = 8;

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
    final List<IndexCursorEntry> entries = lookupConsistently(database, index, keys);
    final Runnable observer = afterLookupObserver;
    if (observer != null && entries != null)
      observer.run();
    return entries;
  }

  private static List<IndexCursorEntry> lookupConsistently(final DatabaseInternal database, final Index index,
      final List<Object[]> keys) {
    final PageManager pageManager = database.getPageManager();
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      if ((pageManager.getPublicationSequence() & 1) != 0)
        // a commit is publishing: wait for it to let go of the lock instead of spending an attempt inside its window
        pageManager.executeInLock(() -> null);

      final long before = pageManager.getPublicationSequence();
      if ((before & 1) != 0)
        // another commit started publishing right after the wait: this attempt did no work, and a sequence odd on every
        // attempt ends in the lock below
        continue;

      final Read read = readAndLoad(database, index, keys);
      if (read == null)
        return null;
      if (pageManager.getPublicationSequence() == before)
        return read.entries;
      unpinRepeatableRead(database, index, read.bucketIds);
    }

    LogManager.instance().log(ConsistentKeyLookup.class, Level.FINE,
        "Lookup on index '%s' overlapped a commit %d times, reading it under the publication lock", null, index.getName(), MAX_ATTEMPTS);
    final Read read = (Read) pageManager.executeInLock(() -> readAndLoad(database, index, keys));
    return read == null ? null : read.entries;
  }

  /**
   * What {@code index.get(key)} answers for each of the keys, read against one committed state: a cursor over their entries
   * with the records loaded, or {@code null} when there are too many entries to be held and the index's own streaming
   * cursors must be used.
   */
  public static IndexCursor lookupCursor(final Database database, final Index index, final List<Object[]> keys) {
    if (!(database instanceof DatabaseInternal internal))
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

  private record Read(List<IndexCursorEntry> entries, Set<Integer> bucketIds) {
  }

  private static Read readAndLoad(final DatabaseInternal database, final Index index, final List<Object[]> keys) {
    final List<Object[]> entryKeys = new ArrayList<>(keys.size());
    final List<RID> rids = new ArrayList<>(keys.size());
    for (final Object[] key : keys) {
      final IndexCursor cursor = index.get(key);
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
    for (int i = 0; i < rids.size(); i++) {
      final RID rid = rids.get(i);
      bucketIds.add(rid.getBucketId());
      try {
        entries.add(new IndexCursorEntry(entryKeys.get(i), database.lookupByRID(rid, true), 1));
      } catch (final RecordNotFoundException e) {
        // in a state no commit overlapped, an entry without a record is dangling and dropped, as the scan of an index does
      }
    }
    return new Read(entries, bucketIds);
  }

  /**
   * Under REPEATABLE_READ the pages this transaction pinned while a commit overlapped the lookup may be the very two states
   * that disagree: they are released, so the lookup is repeated on the committed state. Only called once an overlap was
   * detected, so a lookup that no commit touched keeps its snapshot.
   */
  private static void unpinRepeatableRead(final DatabaseInternal database, final Index index, final Set<Integer> bucketIds) {
    if (!database.isTransactionActive()
        || database.getTransactionIsolationLevel() != Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ)
      return;
    final List<Integer> files = new ArrayList<>(bucketIds);
    if (index instanceof IndexInternal internal)
      files.addAll(internal.getFileIds());
    database.getTransaction().unpinFiles(files);
  }
}
