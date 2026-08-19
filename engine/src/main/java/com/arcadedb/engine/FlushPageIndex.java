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
 * <b>The drain signal (issue #6199).</b> Because {@link #release} is the ONE place a database's count goes down, it
 * is also the one place that can tell a waiter "your pipeline just emptied". {@link #awaitDrain} parks on the
 * database's own counter instead of sleeping, so the two drains ({@code waitAllPagesOfDatabaseAreFlushed} and the
 * snapshot barrier's {@code waitPendingPagesOfDatabaseUntil}) wake the moment the last page lands rather than at the
 * next poll boundary. That matters most for the barrier, which polls with the JVM-wide page-manager lock held: every
 * millisecond it sleeps there is a millisecond in which no committer of ANY database can publish a page.
 * <p>
 * <b>The one thing that rule does NOT cover, and what covers it instead:</b> {@link #removeAllOfDatabase} drops a
 * database's counter outright, so a {@link #putAll} for that same database running concurrently with it could leave a
 * page indexed under a counter that is no longer there - undercounting to zero, the "a backup stamps its t0 over a
 * half-written batch" failure. Nothing in this class prevents that; the exclusion is external and pre-existing.
 * {@code LocalDatabase.close()/drop()} reaches the purge inside {@code executeInWriteLock}, while every commit that
 * reaches {@code scheduleFlushOfPages} holds the matching read lock, so no page of a database can enter the index
 * while that database is being purged from it. Do not call {@link #removeAllOfDatabase} from anywhere that does not
 * hold that write lock.
 * <p>
 * <b>That lock argument only holds within ONE instance (issue #6440).</b> {@code executeInWriteLock}/
 * {@code executeInReadLock} guard a {@code ReentrantReadWriteLock} FIELD of the {@code LocalDatabase} instance, not
 * anything keyed by path - so it says nothing about a DIFFERENT instance reopened at the same path, which is exactly
 * what {@code LocalDatabase.equals()} makes indistinguishable from the one being purged. {@link #pending} is
 * therefore keyed by reference identity ({@link IdentityKey}), not {@code equals()}: a same-path sibling's counter
 * must never be resolved, incremented or zeroed by a DIFFERENT instance, in either direction - {@link #counterOf}
 * creating (or finding) the wrong instance's entry on insertion is exactly as wrong as {@link #removeAllOfDatabase}
 * retiring the wrong instance's entry on removal, and a {@code ConcurrentHashMap} keyed directly on
 * {@code BasicDatabase} cannot tell two {@code equals()}-equal instances apart at either end.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FlushPageIndex {
  private final ConcurrentHashMap<PageId, MutablePage> pages = new ConcurrentHashMap<>();
  /**
   * Number of {@link #pages} entries per database. An entry is created with the database's first pending page and
   * dropped by {@link #removeAllOfDatabase} (close/drop), so it is bounded by the number of open databases.
   * <p>
   * The counter object doubles as the monitor {@link #awaitDrain} parks on and {@link #release} notifies (issue
   * #6199). Reusing it rather than adding a parallel map of lock objects is not a shortcut: it removes the window in
   * which a waiter could resolve a monitor that the signaller has not published yet, and it keeps the signalling
   * path down to the counter the decrement already has in hand. The object never escapes this class - every accessor
   * returns an {@code int} - so no foreign code can hold this monitor.
   * <p>
   * Keyed by {@link IdentityKey}, not {@code BasicDatabase} directly (issue #6440): see the class javadoc.
   */
  private final ConcurrentHashMap<IdentityKey, AtomicInteger> pending = new ConcurrentHashMap<>();

  /**
   * Identity-only key for {@link #pending} (issue #6440). {@code LocalDatabase.equals()}/{@code hashCode()} compare
   * by database PATH, which is exactly wrong here: two different instances open at the same path (a database
   * closed and immediately reopened - a restore, a re-provision, a test hammering one fixed path) must never share
   * a counter entry. A manual identity scan on every {@code put()}/{@code putAll()} would put an O(open databases)
   * walk on the hot commit path just to avoid this; wrapping the key keeps every {@link #pending} access O(1) at
   * the cost of one short-lived, non-escaping wrapper per access - cheap enough to scalar-replace under escape
   * analysis, and strictly less than the {@code AtomicInteger} this same call site already allocates on a miss.
   */
  private static final class IdentityKey {
    private final BasicDatabase database;

    IdentityKey(final BasicDatabase database) {
      this.database = database;
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof IdentityKey other && other.database == database;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(database);
    }
  }

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
      final MutablePage replaced = pages.put(pageId, page);
      if (replaced != null)
        // REPLACED A COPY THAT WAS ALREADY COUNTED (A LATER TX SUPERSEDING A PAGE STILL QUEUED, ISSUE #4544) -
        // decrementing the REPLACED page's OWN counter, not necessarily this one (issue #6440 review): see
        // releaseReplacedCounter.
        releaseReplacedCounter(replaced, database, counter);
    }
  }

  /** Indexes a single page. */
  void put(final MutablePage page) {
    final PageId pageId = page.getPageId();
    final BasicDatabase database = pageId.getDatabase();
    final AtomicInteger counter = counterOf(database);
    counter.incrementAndGet();
    final MutablePage replaced = pages.put(pageId, page);
    if (replaced != null)
      releaseReplacedCounter(replaced, database, counter);
  }

  /**
   * Releases the pending count of a page {@link #put}/{@link #putAll} just replaced in {@link #pages} (issue
   * #4544: a later TX superseding one still queued). {@code Map.put()} on an {@code equals()}-match keeps the
   * ORIGINAL key object and only swaps the value, so a same-path sibling's stale {@link PageId} can retain the
   * map slot while its VALUE becomes the new page - the replaced value can therefore belong to a DIFFERENT
   * instance than the one just inserted, even though {@link PageId#equals} said they were "the same page" (issue
   * #6440 review). Decrementing the INCOMING page's counter in that case would phantom-increment a live,
   * un-flushed sibling's count forever, while the replaced page's own (already fully accounted) counter is left
   * one too high - exactly the aliasing the rest of this class's {@link IdentityKey} keying exists to prevent.
   */
  private void releaseReplacedCounter(final MutablePage replaced, final BasicDatabase incomingDatabase,
      final AtomicInteger incomingCounter) {
    final BasicDatabase replacedDatabase = replaced.getPageId().getDatabase();
    (replacedDatabase == incomingDatabase ? incomingCounter : counterOf(replacedDatabase)).decrementAndGet();
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
    // Reference identity ON PURPOSE (issue #6440), not database.equals(...): LocalDatabase compares by PATH, so a
    // database reopened at the same path as `database` indexes its pages under a key that is merely path-equal,
    // not the same instance. A belated cleanup of the OLD instance must not evict pages a DIFFERENT, still-open
    // instance is waiting to have flushed - that silently drops writes with no error, no WAL replay to fall back
    // on (they were never written), which is strictly worse than the counter merely staying nonzero a little
    // longer.
    //
    // Checked against the ENTRY'S VALUE, not the retained map key, and that distinction matters here specifically
    // (issue #6440 review): Map.put() on an equals()-match keeps the ORIGINAL key object and only replaces the
    // value - textbook Java Map behavior - so a page this OLD instance indexed at PageId(A, fileId, pageNumber)
    // and a page a same-path sibling B later indexes at the SAME (fileId, pageNumber) collide as map keys
    // (PageId.equals()/hashCode() go through path-based Database.equals() too). B's put() then leaves the STORED
    // key as A's original PageId object with B's page as the value. Filtering by the retained key's database
    // (pageId.getDatabase() == database) would still match that stale key and evict B's live, un-flushed page -
    // the exact aliasing this method exists to prevent, just reached one layer down. MutablePage.getPageId() is
    // the value's OWN field, set when that specific page object was created, so it always names whichever
    // instance most recently indexed the CURRENT value - immune to which key object the map happened to retain.
    pages.entrySet().removeIf(entry -> entry.getValue().getPageId().getDatabase() == database);
    // Dropped AFTER the pages, never before: the other order would let a page indexed in between survive with a
    // fresh counter that the purge then empties, leaving a count above zero that hangs the close forever.
    //
    // pending is IdentityKey-keyed (issue #6440), so this can only ever resolve the entry THIS exact instance
    // created (via counterOf()) - never a same-path sibling's, in either direction: a sibling can no longer have
    // its live counter zeroed by this instance's belated cleanup, and this instance can no longer resolve (and
    // thereby retire) a sibling's counter by mistake either.
    final AtomicInteger counter = pending.remove(new IdentityKey(database));
    // A waiter parked on that counter would otherwise sleep out its whole fallback interval for a database that no
    // longer has an entry at all: pendingOf() answers 0 for it from here on, so wake it to observe that (#6199).
    if (counter != null)
      synchronized (counter) {
        // ZEROED, not merely notified, and inside the monitor. A waiter that resolved this counter BEFORE the
        // remove above can enter the monitor AFTER the notifyAll has already fired, and what it re-reads there is
        // the counter object itself - the drained map entry it would otherwise consult is already gone. So the
        // object has to carry the drained state, or that waiter parks through a signal it can never be sent again.
        // Bounded rather than fatal (its fallback interval still expires, and its next call resolves no counter at
        // all), but it is precisely the wake-up this notification exists to deliver.
        counter.set(0);
        counter.notifyAll();
      }
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
    final AtomicInteger counter = pending.get(new IdentityKey(database));
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
    return pending.containsKey(new IdentityKey(database));
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

  /**
   * Parks until the database's pipeline is empty, or until {@code maxWaitMillis} elapses - the poll-free form of the
   * drains of issue #6199.
   * <p>
   * <b>Standard guarded wait, and the guard is the whole point:</b> the count is re-read HERE, with the monitor
   * already held, and not by the caller before the call. A caller that read a positive count and then asked to park
   * would race the last {@link #release}: the decrement and its {@code notifyAll} could both land in the gap, and
   * the wait would then be woken only by its own timeout. Re-reading under the monitor closes that gap in both
   * directions - either this read sees the drained count and returns without parking, or it happens before the
   * decrement, in which case the signaller cannot enter its {@code synchronized} block until this thread is already
   * inside {@code wait()} and has released the monitor.
   * <p>
   * The bounded wait is a safety net, not the mechanism: it keeps the callers' timeout machinery (the no-progress
   * window of {@code waitAllPagesOfDatabaseAreFlushed}, the hard deadline of the snapshot barrier) periodically
   * re-evaluated, and degrades a hypothetical lost notification to the polling this used to be rather than to a
   * hang. Callers therefore must keep their own loop and re-check their own condition on return.
   *
   * @param maxWaitMillis upper bound of the park. {@code <= 0} returns immediately - it never means "park forever",
   *                      which is what a bare {@code Object.wait(0)} would do.
   */
  void awaitDrain(final BasicDatabase database, final long maxWaitMillis) throws InterruptedException {
    if (maxWaitMillis <= 0)
      return;

    final AtomicInteger counter = pending.get(new IdentityKey(database));
    if (counter == null)
      // NEVER HAD A PENDING PAGE, OR THE DATABASE WAS ALREADY FORGOTTEN: pendingOf() ANSWERS 0, NOTHING TO WAIT FOR
      return;

    synchronized (counter) {
      if (counter.get() <= 0)
        return;
      counter.wait(maxWaitMillis);
    }
  }

  private AtomicInteger counterOf(final BasicDatabase database) {
    final IdentityKey key = new IdentityKey(database);
    final AtomicInteger counter = pending.get(key);
    return counter != null ? counter : pending.computeIfAbsent(key, k -> new AtomicInteger());
  }

  private void release(final BasicDatabase database) {
    final AtomicInteger counter = pending.get(new IdentityKey(database));
    // The monitor is taken ONLY on the transition to drained, so the hot flush path pays a volatile decrement and a
    // comparison per page exactly as before, and a lock acquisition only on the page that empties the pipeline.
    if (counter != null && counter.decrementAndGet() <= 0)
      synchronized (counter) {
        counter.notifyAll();
      }
  }
}
