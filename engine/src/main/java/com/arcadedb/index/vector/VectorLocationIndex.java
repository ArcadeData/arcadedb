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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Lightweight index that stores only vector location metadata (absolute file offset, RID)
 * instead of the full vector data. This dramatically reduces memory usage: the payload of one location is ~24
 * bytes, and it retains {@link #APPROX_RETAINED_BYTES_PER_LOCATION} once the map and object overheads around it
 * are counted, against ~3KB for a 768-dimension vector.
 * <p>
 * Uses absolute file offsets for direct random access without loading full pages.
 * <p>
 * Only live vectors are resident: once an id is tombstoned its location is released and the id survives as a single
 * bit (see {@link DeletedIds}), so the ~90 bytes a location costs follow the number of live vectors and not the
 * number of writes (issue #5516). What still follows the writes is one bit per id handed out since the last graph
 * rebuild - 1.2MB for the 9.3M ids that used to cost 970MB.
 * <p>
 * Used by LSMVectorIndex to implement lazy-loading of vectors from disk.
 * <p>
 * <b>This map does not evict, and it has no mode in which it can.</b> It used to carry a second, bounded backend -
 * a {@code Collections.synchronizedMap(LinkedHashMap)} that FIFO-evicted past a {@code maxSize} - selected by
 * {@code arcadedb.vectorIndex.locationCacheSize}. That was never a cache bound: there is no vector id to offset
 * index on disk, so an evicted location could not be recovered, and every reader reads "no location" as "deleted".
 * A cap therefore made {@code countEntries()} under-report and dropped the evicted vectors from every search
 * (issues #5568 and #5559). #5568 stopped asking for that backend and #5559 removed it, so the failure is now
 * structurally impossible rather than prevented by policy - and the removal also takes a {@code maxSize} branch
 * off each of the ten methods that had to pick a backend.
 * <p>
 * Reintroducing eviction requires giving the index file a vector id to offset lookup first, so that a miss is
 * recoverable. Until then, size an index at {@link #APPROX_RETAINED_BYTES_PER_LOCATION} per live vector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorLocationIndex {
  /**
   * Approximate retained heap of one live location, and the single figure every caller and document should quote.
   * Enumerated in issue #5516: the {@link VectorLocation} and the {@link RID} it points at, the boxed
   * {@code Integer} key, the map node and its table slot, and the entry the id owns in the RID reverse index. The
   * 24 bytes of payload it carries are only a fraction of that, which is why sizing an index off the payload
   * under-estimates it several-fold. An estimate, not a measurement: the exact layout depends on the JVM and on
   * whether compressed oops are in play.
   */
  public static final int APPROX_RETAINED_BYTES_PER_LOCATION = 90;

  private final ConcurrentHashMap<Integer, VectorLocation> locations;
  // Reverse index RID -> vector ids, mirroring the keys stored in `locations`. Lets remove(keys, rid) resolve the
  // vector ids for a single RID in O(k) instead of scanning every id in the index (issue #5318). Kept perfectly in
  // sync with `locations`: entries are added on addVector/addOrUpdate, dropped when an id is tombstoned and on
  // clear().
  private final ConcurrentHashMap<RID, int[]>              ridToVectorIds;
  // Ids whose location was dropped because they were tombstoned: 1 bit per id instead of a resident VectorLocation
  // (issue #5516). See DeletedIds.
  private final DeletedIds                                 deletedIds;
  private final AtomicInteger                              nextId;

  /**
   * Compact set of the vector ids that have been tombstoned, one bit per id (issue #5516).
   * <p>
   * A tombstoned id is dead: its vector was superseded by a new id (update) or removed (delete), and the tombstone
   * is persisted on the index pages, so nothing needs its offset ever again - all the readers treat "no location"
   * exactly like "deleted location". Keeping the {@link VectorLocation} (plus its RID, its {@code Integer} key, the
   * map node and its slot in the reverse index: ~90 bytes) resident only to carry a boolean made memory grow with
   * the number of writes instead of with the number of live vectors: re-embedding 4K vectors every few minutes
   * accumulated 9.3M locations, ~970MB of heap, before the process died. One bit per id costs ~1.2MB for the same
   * workload and is enough to know whether a persisted graph carries stale ordinals.
   * <p>
   * The array is sized by the highest id handed out since the last {@link #clear()}, not by the number of live
   * vectors: ids are preserved across a compaction rather than renumbered. Every graph rebuild (and therefore every
   * compaction) releases it, so it grows only across the writes between two rebuilds.
   * <p>
   * Writes are serialized on this object; reads are lock-free over a volatile array snapshot, matching the
   * weakly-consistent semantics the {@link ConcurrentHashMap} backing {@code locations} already has. Every mutation
   * republishes {@code bits} even when it did not resize the array: setting a bit is a plain array store, and a
   * reader's volatile read of {@code bits} only synchronizes-with a write to that field, so without the
   * re-publication the reader has no happens-before edge to the new bit and can keep missing it. A missed bit is
   * not benign - it is a stale "not deleted" answer on the reconstruction path, which then scans the pages and
   * finds the id's first (pre-tombstone) entry, resurrecting the vector this set exists to bury.
   */
  private static final class DeletedIds {
    private volatile long[] bits  = EMPTY_BITS;
    private volatile int    count = 0;

    private static final long[] EMPTY_BITS = new long[0];

    boolean contains(final int id) {
      if (id < 0)
        return false;
      final long[] snapshot = bits;
      // `1L << id` shifts by id & 63 (JLS 15.19), which is the bit within the word `id >>> 6` selects.
      final int word = id >>> 6;
      return word < snapshot.length && (snapshot[word] & (1L << id)) != 0;
    }

    synchronized void add(final int id) {
      if (id < 0)
        return;
      final int word = id >>> 6;
      long[] current = bits;
      if (word >= current.length)
        current = Arrays.copyOf(current, Math.max(word + 1, current.length * 2));

      final long mask = 1L << id;
      if ((current[word] & mask) == 0) {
        current[word] |= mask;
        count++;
        // A grown array always lands here (its words start at zero), so the resize is published too.
        bits = current; // publishes the bit above to every lock-free reader
      }
    }

    synchronized void remove(final int id) {
      if (id < 0)
        return;
      final int word = id >>> 6;
      final long[] current = bits;
      if (word >= current.length)
        return;
      final long mask = 1L << id;
      if ((current[word] & mask) != 0) {
        current[word] &= ~mask;
        count--;
        bits = current; // publishes the cleared bit to every lock-free reader
      }
    }

    synchronized void clear() {
      bits = EMPTY_BITS;
      count = 0;
    }

    int size() {
      return count;
    }
  }

  /**
   * Represents the physical location of a vector on disk.
   * Total size: ~24 bytes (vs ~3KB for actual vector data)
   */
  public static class VectorLocation {
    public final boolean isCompacted;        // 1 byte - true if in compacted file, false if in mutable file
    public final long    absoluteFileOffset; // 8 bytes - direct offset into file for O(1) access
    public final RID     rid;                // 12 bytes - document RID (bucketId + position)
    public final boolean deleted;            // 1 byte - LSM tombstone flag

    public VectorLocation(final boolean isCompacted, final long absoluteFileOffset, final RID rid,
        final boolean deleted) {
      this.isCompacted = isCompacted;
      this.absoluteFileOffset = absoluteFileOffset;
      this.rid = rid;
      this.deleted = deleted;
    }
  }

  /**
   * Create a VectorLocationIndex sized for a small index; it grows as entries are added.
   */
  public VectorLocationIndex() {
    this(16);
  }

  /**
   * Create a VectorLocationIndex with an initial capacity hint. Sizing the map for the set it is about to hold
   * saves the rehashes a rebuild would otherwise pay on the way up.
   * <p>
   * The single-int constructor used to mean {@code maxSize} (issue #5559 removed bounding). A caller that still
   * passes an eviction limit here would now silently get a capacity hint instead, which is why the old
   * {@code -1 = unlimited} sentinel is rejected rather than quietly treated as a size.
   * <p>
   * That rejection is an {@link IllegalArgumentException}, deliberately NOT the {@code IndexException} that
   * {@code LSMVectorIndexMetadata.setLocationCacheSize} raises for the same setting. The two guard different
   * things: that one answers a user who wrote {@code locationCacheSize} in DDL and belongs in the error the
   * statement reports, while this one can only fire on a caller inside the engine that was not updated for the
   * changed parameter meaning - programming error, not user input.
   *
   * @param initialCapacity Initial capacity hint for the underlying maps, at least 0
   */
  public VectorLocationIndex(final int initialCapacity) {
    if (initialCapacity < 0)
      throw new IllegalArgumentException(
          "Invalid initial capacity " + initialCapacity + " for a vector location index: this parameter used to be "
              + "an eviction limit, which issue #5559 removed because an evicted location cannot be recovered");

    this.ridToVectorIds = new ConcurrentHashMap<>(initialCapacity);
    this.locations = new ConcurrentHashMap<>(initialCapacity);
    this.deletedIds = new DeletedIds();
    this.nextId = new AtomicInteger(0);
  }

  /**
   * Add a new vector location with an auto-generated ID.
   *
   * @param isCompacted        True if the vector is in the compacted file, false if in mutable file
   * @param absoluteFileOffset The absolute offset in the file where the vector entry is stored
   * @param rid                The document RID
   *
   * @return The assigned vector ID
   */
  public int addVector(final boolean isCompacted, final long absoluteFileOffset, final RID rid) {
    final int id = nextId.getAndIncrement();
    locations.put(id, new VectorLocation(isCompacted, absoluteFileOffset, rid, false));
    addToRidIndex(rid, id);
    return id;
  }

  /**
   * Add or update a vector location with a specific ID.
   * Used during loading from pages (LSM style: later entries override earlier ones).
   * <p>
   * A tombstone ({@code deleted == true}) does NOT become a resident entry: the id is recorded in the compact
   * deleted-id set and any location it had is released (issue #5516). {@link #getLocation(int)} then answers
   * {@code null} for it, which every reader already treats as "deleted".
   *
   * @param id                 The vector ID
   * @param isCompacted        True if the vector is in the compacted file, false if in mutable file
   * @param absoluteFileOffset The absolute offset in the file where the vector entry is stored
   * @param rid                The document RID
   * @param deleted            Whether this vector is deleted (LSM tombstone)
   */
  public void addOrUpdate(final int id, final boolean isCompacted, final long absoluteFileOffset, final RID rid,
      final boolean deleted) {
    if (deleted) {
      removeLocation(id);
      deletedIds.add(id);
    } else {
      // LSM merge-on-read: a live entry read after a tombstone for the same id wins (it cannot happen through the
      // write path, which never reuses an id, but page order is the authority here).
      if (deletedIds.contains(id))
        deletedIds.remove(id);

      final VectorLocation loc = new VectorLocation(isCompacted, absoluteFileOffset, rid, false);
      // Only register the id in the reverse index the first time it appears: later addOrUpdate calls (LSM
      // overrides) reuse the same id with the same RID, so re-adding would create duplicate reverse entries.
      if (locations.put(id, loc) == null)
        addToRidIndex(rid, id);
    }

    // Update nextId if this ID is higher than current
    int currentNext;
    do {
      currentNext = nextId.get();
      if (id < currentNext)
        break; // ID is lower, no need to update
    } while (!nextId.compareAndSet(currentNext, id + 1));
  }

  /**
   * Get the location metadata for a vector by ID.
   *
   * @param vectorId The vector ID
   *
   * @return The location metadata, or null if not found
   */
  public VectorLocation getLocation(final int vectorId) {
    return locations.get(vectorId);
  }

  /**
   * Return the vector ids currently mapped to the given RID (including tombstoned ones), resolved in O(k) via the
   * reverse index instead of scanning all vector ids. The array reflects every id resident in {@code locations} for
   * that RID; callers must re-check {@link #getLocation(int)} for the deleted flag.
   *
   * @param rid The document RID
   *
   * @return the matching vector ids, or an empty array if none are resident
   */
  public int[] getVectorIdsForRid(final RID rid) {
    final int[] ids = ridToVectorIds.get(rid);
    return ids != null ? ids.clone() : EMPTY_IDS;
  }

  private static final int[] EMPTY_IDS = new int[0];

  /** Append a vector id to the reverse index bucket for a RID. */
  private void addToRidIndex(final RID rid, final int id) {
    // merge() atomically, so concurrent updates for the same RID never lose an id.
    ridToVectorIds.merge(rid, new int[] { id }, (a, b) -> {
      final int[] grown = Arrays.copyOf(a, a.length + b.length);
      System.arraycopy(b, 0, grown, a.length, b.length);
      return grown;
    });
  }

  /** Remove a vector id from the reverse index bucket for a RID. */
  private void removeFromRidIndex(final RID rid, final int id) {
    ridToVectorIds.computeIfPresent(rid, (k, existing) -> withoutValue(existing, id));
  }

  /**
   * Return a copy of {@code ids} without the first occurrence of {@code value}, {@code null} if the result is empty,
   * or the same array instance if {@code value} was not present.
   */
  private static int[] withoutValue(final int[] ids, final int value) {
    int pos = -1;
    for (int i = 0; i < ids.length; i++)
      if (ids[i] == value) {
        pos = i;
        break;
      }
    if (pos == -1)
      return ids;
    if (ids.length == 1)
      return null;
    final int[] shrunk = new int[ids.length - 1];
    System.arraycopy(ids, 0, shrunk, 0, pos);
    System.arraycopy(ids, pos + 1, shrunk, pos, ids.length - pos - 1);
    return shrunk;
  }

  /**
   * Mark a vector as deleted (LSM tombstone). The location is released and the id is remembered in the compact
   * deleted-id set (issue #5516): a tombstoned id is never read back, and the tombstone itself is persisted on the
   * index pages by the caller, so keeping the location resident only costs heap.
   * <p>
   * An id that is not resident is left alone: it was already tombstoned, and the map never drops a location for any
   * other reason (issue #5559 removed eviction), so there is nothing to record.
   *
   * @param vectorId The vector ID to mark as deleted
   */
  public void markDeleted(final int vectorId) {
    if (removeLocation(vectorId) != null)
      deletedIds.add(vectorId);
  }

  /**
   * Return whether the given id has been tombstoned, without touching the pages.
   *
   * @param vectorId The vector ID
   *
   * @return true if the id is tombstoned
   */
  public boolean isDeleted(final int vectorId) {
    return deletedIds.contains(vectorId);
  }

  /**
   * Number of tombstoned ids seen since the index was loaded or cleared. A value greater than zero means the
   * persisted graph's ordinals no longer match the live vector set (issue #3135).
   */
  public int getDeletedCount() {
    return deletedIds.size();
  }

  /**
   * Drop a location, keeping the reverse index in sync.
   *
   * @return the removed location, or null if the id was not resident
   */
  private VectorLocation removeLocation(final int vectorId) {
    final VectorLocation removed = locations.remove(vectorId);
    if (removed != null)
      removeFromRidIndex(removed.rid, vectorId);
    return removed;
  }

  /**
   * Get a stream of all vector IDs in the index.
   *
   * @return Stream of vector IDs
   */
  public IntStream getAllVectorIds() {
    // ConcurrentHashMap: keySet() iteration is thread-safe and weakly-consistent, so stream it lazily without the
    // O(N) snapshot allocation.
    return locations.keySet().stream().mapToInt(Integer::intValue);
  }

  /**
   * Get a stream of active (non-deleted) vector IDs.
   * <p>
   * Since issue #5516 no resident location carries {@code deleted == true} - a tombstoned id keeps no location at
   * all - so this returns the same set as {@link #getAllVectorIds()}. The filter stays as the definition of
   * "active": it is what a location loaded straight from a page (which can carry the flag) has to pass.
   *
   * @return Stream of active vector IDs
   */
  public IntStream getActiveVectorIds() {
    // ConcurrentHashMap: stream entrySet() lazily. Using entrySet() also avoids the extra get() lookup the original
    // keySet()+get() implementation performed.
    return locations.entrySet().stream()
        .filter(e -> !e.getValue().deleted)
        .mapToInt(Map.Entry::getKey);
  }

  /**
   * Number of resident locations, which is the number of LIVE vectors: {@link #markDeleted} drops the location and
   * keeps only the id in the tombstone set, and nothing else ever removes one (issue #5559 took away the bounded
   * backend that could evict). Callers such as {@code LSMVectorIndex.estimatePagesForLiveSet} rely on that equality.
   * Constant time, unlike {@link #getActiveCount()}.
   *
   * @return Number of vectors whose location is currently held
   */
  public int size() {
    return locations.size();
  }

  /**
   * Get the count of active (non-deleted) vectors.
   *
   * @return Number of active vectors
   */
  public long getActiveCount() {
    // ConcurrentHashMap: values() iteration is thread-safe and weakly-consistent.
    return locations.values().stream().filter(loc -> !loc.deleted).count();
  }

  /**
   * Get the next ID that will be assigned.
   *
   * @return The next vector ID
   */
  public int getNextId() {
    return nextId.get();
  }

  public int getMaxVectorId() {
    return nextId.get() - 1;
  }

  /**
   * Set the next ID (used during loading to restore ID sequence).
   *
   * @param id The next ID to use
   */
  public void setNextId(final int id) {
    nextId.set(id);
  }

  /**
   * Clear all vector locations.
   */
  public void clear() {
    locations.clear();
    ridToVectorIds.clear();
    deletedIds.clear();
    nextId.set(0);
  }
}
