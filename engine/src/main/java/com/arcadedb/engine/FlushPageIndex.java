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
package com.arcadedb.engine;

import com.arcadedb.database.BasicDatabase;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The index of the pages sitting in the asynchronous flush pipeline: {@code pageId} to the most recent
 * {@link MutablePage} waiting in the flush queue, deferred by a suspension, or currently being written.
 * <p>
 * It answers two questions, and owning both in one place is the whole point of the class (issue #6133):
 * <ul>
 * <li><b>"is this exact page pending?"</b> - O(1) on a flat JVM-wide map, the read path of
 * {@code getCachedPageFromMutablePageInQueue};</li>
 * <li><b>"does this database still have pages in the pipeline?"</b> - O(1) on a per-database counter, instead of the
 * walk of the whole JVM-wide map it used to be. That question is polled every 10 ms for the whole duration of a drain
 * by every close, rename, index compaction, backup suspension and snapshot t0 barrier, and the barrier polls it with
 * the JVM-wide page-manager lock held, so its cost used to scale with the backlog of every OTHER open database
 * exactly when that backlog was at its largest.</li>
 * </ul>
 * <b>The counter cannot drift, because nothing outside this class can touch the map.</b> That is the reason the index
 * became a class rather than staying a bare {@code ConcurrentHashMap} with a counter bolted onto its call sites: a
 * count that drifts HIGH hangs {@code close()} forever on a pipeline that is in fact empty, and one that drifts LOW
 * lets a backup stamp its t0 over a half-written batch. Every mutation - schedule, flush, dropped file, dropped
 * database, replay detach - goes through the methods below.
 * <p>
 * <b>Ordering rule, where the two are not one atomic step:</b> the counter is incremented BEFORE a page becomes
 * visible in the map and decremented AFTER it stops being visible. The count is therefore never lower than the number
 * of indexed pages, so {@code pendingOf() == 0} proves the pipeline is empty (what the barrier and the close path
 * need), while the opposite skew is bounded by a few instructions and costs at worst one extra poll.
 * <p>
 * <b>The one thing that rule does NOT cover, and what covers it instead:</b> {@link #removeAllOfDatabase} drops a
 * database's counter outright, so a {@link #putAll} for that same database running concurrently with it could leave a
 * page indexed under a counter that is no longer there - undercounting to zero, the "a backup stamps its t0 over a
 * half-written batch" failure. Nothing in this class prevents that; the exclusion is external and pre-existing.
 * {@code LocalDatabase.close()/drop()} reaches the purge inside {@code executeInWriteLock}, while every commit that
 * reaches {@code scheduleFlushOfPages} holds the matching read lock, so no page of a database can enter the index
 * while that database is being purged from it. Do not call {@link #removeAllOfDatabase} from anywhere that does not
 * hold that write lock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FlushPageIndex {
  private final ConcurrentHashMap<PageId, MutablePage>          pages   = new ConcurrentHashMap<>();
  /**
   * Number of {@link #pages} entries per database. An entry is created with the database's first pending page and
   * dropped by {@link #removeAllOfDatabase} (close/drop), so it is bounded by the number of open databases.
   */
  private final ConcurrentHashMap<BasicDatabase, AtomicInteger> pending = new ConcurrentHashMap<>();

  /** @return the most recent {@link MutablePage} pending for the page, or {@code null} when none is. */
  MutablePage get(final PageId pageId) {
    return pages.get(pageId);
  }

  boolean containsKey(final PageId pageId) {
    return pages.containsKey(pageId);
  }

  /** @return {@code true} when NO database has a page in the pipeline. */
  boolean isEmpty() {
    return pages.isEmpty();
  }

  /** Indexes a batch about to be enqueued. The counter is resolved once for the batch, which is single-database. */
  void putAll(final List<MutablePage> batch) {
    BasicDatabase lastDatabase = null;
    AtomicInteger counter = null;
    for (int i = 0; i < batch.size(); i++) {
      final MutablePage page = batch.get(i);
      final PageId pageId = page.getPageId();
      final BasicDatabase database = pageId.getDatabase();
      // Reference comparison on purpose: a batch carries the pages of ONE transaction, so this resolves the map
      // once per batch, while still staying correct for a hypothetical mixed batch instead of miscounting it.
      if (database != lastDatabase) {
        counter = counterOf(database);
        lastDatabase = database;
      }
      counter.incrementAndGet();
      if (pages.put(pageId, page) != null)
        // REPLACED A COPY THAT WAS ALREADY COUNTED (A LATER TX SUPERSEDING A PAGE STILL QUEUED, ISSUE #4544)
        counter.decrementAndGet();
    }
  }

  /** Indexes a single page. */
  void put(final MutablePage page) {
    final PageId pageId = page.getPageId();
    final AtomicInteger counter = counterOf(pageId.getDatabase());
    counter.incrementAndGet();
    if (pages.put(pageId, page) != null)
      counter.decrementAndGet();
  }

  /** @return the {@link MutablePage} that was indexed for the page, or {@code null} when none was. */
  MutablePage remove(final PageId pageId) {
    final MutablePage removed = pages.remove(pageId);
    if (removed != null)
      release(pageId.getDatabase());
    return removed;
  }

  /** Removes every page of a batch, used to roll back an indexing whose enqueue failed. */
  void removeAll(final List<MutablePage> batch) {
    for (int i = 0; i < batch.size(); i++)
      remove(batch.get(i).getPageId());
  }

  /**
   * Removes a just-flushed page, but ONLY if the indexed value is still the exact same instance that was flushed. A
   * later transaction may have queued a NEWER {@link MutablePage} for the same {@link PageId}; that newer entry must
   * survive so reads keep seeing the latest version.
   * <p>
   * Reference identity ({@code indexed == page}) is used here on purpose instead of {@code remove(key, value)}:
   * {@link BasePage#equals} keys on the mutable {@code version} field, which is an unreliable discriminator for a
   * hash-map value (issue #4544). The atomic {@code computeIfPresent} guarantees the check-and-remove happens as a
   * single operation under the same concurrency guarantees as {@code remove(key, value)}.
   *
   * @return {@code true} when this instance was the indexed one and was removed.
   */
  boolean removeIfSame(final MutablePage page) {
    final PageId pageId = page.getPageId();
    // computeIfPresent returns null both when the key was absent and when the mapping was removed, and only the
    // second may release a count: the holder distinguishes them from inside the map's own atomic section.
    final boolean[] removed = new boolean[1];
    pages.computeIfPresent(pageId, (id, indexed) -> {
      if (indexed != page)
        return indexed;
      removed[0] = true;
      return null;
    });
    if (removed[0])
      release(pageId.getDatabase());
    return removed[0];
  }

  /**
   * Drops every pending page of a database, and its counter with them (the database is closing or being dropped).
   * <p>
   * Callers must hold the database's exclusive lock - see the class javadoc for why this one method cannot be made
   * safe against a concurrent {@link #putAll} from inside this class.
   */
  void removeAllOfDatabase(final BasicDatabase database) {
    pages.keySet().removeIf(pageId -> database.equals(pageId.getDatabase()));
    // Dropped AFTER the pages, never before: the other order would let a page indexed in between survive with a
    // fresh counter that the purge then empties, leaving a count above zero that hangs the close forever.
    pending.remove(database);
  }

  /**
   * Drops every pending page of a single dropped file.
   * <p>
   * This one still walks the whole JVM-wide index - the shape #6133 removed from the pending count - and that is
   * deliberate: every caller of {@code PageManager.deleteFile} is a one-off structural operation (dropping a bucket,
   * an index, a bloom filter, a time-series shard, or the source files of a finished LSM compaction), never a
   * per-record or per-page path, and the same method goes on to walk the read cache, which is larger than this index
   * by orders of magnitude. Keep it that way: a caller that dropped files frequently would want a per-database view
   * here, not this walk.
   */
  void removeAllOfFile(final BasicDatabase database, final int fileId) {
    // Walked (weakly consistent, never throws) rather than removeIf'd so each removal goes through the accounting.
    for (final PageId pageId : pages.keySet())
      if (pageId.getFileId() == fileId && database.equals(pageId.getDatabase()))
        remove(pageId);
  }

  /**
   * How many pages of the database are in the flush pipeline, in O(1) - the point of the whole class.
   * <p>
   * Deliberately NOT clamped at zero: a negative value would mean the accounting has a bug, and clamping it would
   * hide exactly what the regression test of #6133 is there to catch. Every caller treats {@code <= 0} as drained,
   * so a hypothetical negative behaves like an empty pipeline rather than like a hang.
   */
  int pendingOf(final BasicDatabase database) {
    final AtomicInteger counter = pending.get(database);
    return counter != null ? counter.get() : 0;
  }

  boolean hasPendingOf(final BasicDatabase database) {
    return pendingOf(database) > 0;
  }

  /**
   * Whether a per-database counter is still held for this database: {@code true} between its first pending page and
   * the {@link #removeAllOfDatabase} of its close or drop. Exists so the regression test of issue #6133 can prove a
   * clean close forgets the database instead of pinning it as a map key forever.
   */
  boolean isTracked(final BasicDatabase database) {
    return pending.containsKey(database);
  }

  /**
   * The same question {@link #pendingOf} answers, computed the expensive way by walking the whole index. Exists only
   * so the regression test of issue #6133 can prove the counter agrees with the map after every kind of mutation.
   */
  int scanPendingOf(final BasicDatabase database) {
    int count = 0;
    for (final PageId pageId : pages.keySet())
      if (database.equals(pageId.getDatabase()))
        ++count;
    return count;
  }

  private AtomicInteger counterOf(final BasicDatabase database) {
    final AtomicInteger counter = pending.get(database);
    return counter != null ? counter : pending.computeIfAbsent(database, k -> new AtomicInteger());
  }

  private void release(final BasicDatabase database) {
    final AtomicInteger counter = pending.get(database);
    if (counter != null)
      counter.decrementAndGet();
  }
}
