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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Lightweight index that stores only vector location metadata (absolute file offset, RID)
 * instead of the full vector data. This dramatically reduces memory usage:
 * ~24 bytes per vector vs ~3KB for a 768-dimension vector.
 * <p>
 * Uses absolute file offsets for direct random access without loading full pages.
 * <p>
 * Used by LSMVectorIndex to implement lazy-loading of vectors from disk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorLocationIndex {
  private final Map<Integer, VectorLocation> locations;
  // Reverse index RID -> vector ids, mirroring the keys stored in `locations`. Lets remove(keys, rid) resolve the
  // vector ids for a single RID in O(k) instead of scanning every id in the index (issue #5318). A RID maps to more
  // than one id when its vector is updated: the update assigns a new id and tombstones the previous one, so both ids
  // stay resident (and in this map) until a compaction rebuild clears the index. Kept perfectly in sync with
  // `locations`: entries are added on addVector/addOrUpdate, dropped on FIFO eviction (bounded mode) and clear().
  private final Map<RID, int[]>              ridToVectorIds;
  private final AtomicInteger                nextId;
  private final int                          maxSize;

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
   * Create an unlimited VectorLocationIndex (backward compatible).
   */
  public VectorLocationIndex() {
    this(-1, 16);
  }

  /**
   * Create a VectorLocationIndex with a specific maximum size.
   *
   * @param maxSize Maximum number of entries to cache. Set to -1 for unlimited (backward compatible).
   *                When the limit is reached, the oldest-inserted entry is evicted automatically (insertion-order, FIFO).
   */
  public VectorLocationIndex(final int maxSize) {
    this(maxSize, 16);
  }

  /**
   * Create a VectorLocationIndex with a specific maximum size and initial capacity.
   *
   * @param maxSize         Maximum number of entries to cache. Set to -1 for unlimited (backward compatible).
   * @param initialCapacity Initial capacity hint for the underlying map
   */
  public VectorLocationIndex(final int maxSize, final int initialCapacity) {
    this.maxSize = maxSize;
    if (maxSize > 0) {
      // Bounded cache with automatic insertion-order (FIFO) eviction.
      // Insertion-order (accessOrder=false): a get() must not be a structural modification, otherwise concurrent reads
      // would corrupt the doubly-linked list while another thread iterates it. Eviction stays least-recently-inserted.
      // The reverse index shares the wrapper monitor: every mutation below runs inside synchronized(locations), and the
      // eviction hook prunes the evicted id from ridToVectorIds so the two maps never diverge.
      this.ridToVectorIds = new HashMap<>(initialCapacity);
      this.locations = Collections.synchronizedMap(
          new LinkedHashMap<Integer, VectorLocation>(initialCapacity, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Integer, VectorLocation> eldest) {
              if (size() <= maxSize)
                return false;
              // Keep the reverse index consistent with the FIFO eviction of the eldest entry.
              removeFromRidIndex(eldest.getValue().rid, eldest.getKey());
              return true;
            }
          }
      );
    } else {
      // Unlimited mode (backward compatible)
      this.ridToVectorIds = new ConcurrentHashMap<>(initialCapacity);
      this.locations = new ConcurrentHashMap<>(initialCapacity);
    }
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
    if (maxSize > 0) {
      synchronized (locations) {
        locations.put(id, new VectorLocation(isCompacted, absoluteFileOffset, rid, false));
        addToRidIndex(rid, id);
      }
    } else {
      locations.put(id, new VectorLocation(isCompacted, absoluteFileOffset, rid, false));
      addToRidIndex(rid, id);
    }
    return id;
  }

  /**
   * Add or update a vector location with a specific ID.
   * Used during loading from pages (LSM style: later entries override earlier ones).
   *
   * @param id                 The vector ID
   * @param isCompacted        True if the vector is in the compacted file, false if in mutable file
   * @param absoluteFileOffset The absolute offset in the file where the vector entry is stored
   * @param rid                The document RID
   * @param deleted            Whether this vector is deleted (LSM tombstone)
   */
  public void addOrUpdate(final int id, final boolean isCompacted, final long absoluteFileOffset, final RID rid,
      final boolean deleted) {
    final VectorLocation loc = new VectorLocation(isCompacted, absoluteFileOffset, rid, deleted);
    if (maxSize > 0) {
      synchronized (locations) {
        // Only register the id in the reverse index the first time it appears: later addOrUpdate calls (LSM overrides,
        // tombstone flips) reuse the same id with the same RID, so re-adding would create duplicate reverse entries.
        if (locations.put(id, loc) == null)
          addToRidIndex(rid, id);
      }
    } else {
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
    if (maxSize > 0) {
      synchronized (locations) {
        final int[] ids = ridToVectorIds.get(rid);
        return ids != null ? ids.clone() : EMPTY_IDS;
      }
    }
    final int[] ids = ridToVectorIds.get(rid);
    return ids != null ? ids.clone() : EMPTY_IDS;
  }

  private static final int[] EMPTY_IDS = new int[0];

  /** Append a vector id to the reverse index bucket for a RID. Callers hold the proper synchronization for the mode. */
  private void addToRidIndex(final RID rid, final int id) {
    if (maxSize > 0) {
      // Bounded mode: single-threaded under synchronized(locations), a plain get/put is enough.
      final int[] existing = ridToVectorIds.get(rid);
      if (existing == null)
        ridToVectorIds.put(rid, new int[] { id });
      else {
        final int[] grown = Arrays.copyOf(existing, existing.length + 1);
        grown[existing.length] = id;
        ridToVectorIds.put(rid, grown);
      }
    } else {
      // Unlimited mode: ConcurrentHashMap, merge atomically so concurrent updates for the same RID never lose an id.
      ridToVectorIds.merge(rid, new int[] { id }, (a, b) -> {
        final int[] grown = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, grown, a.length, b.length);
        return grown;
      });
    }
  }

  /** Remove a vector id from the reverse index bucket for a RID. Callers hold the proper synchronization for the mode. */
  private void removeFromRidIndex(final RID rid, final int id) {
    if (maxSize > 0) {
      final int[] existing = ridToVectorIds.get(rid);
      if (existing == null)
        return;
      final int[] shrunk = withoutValue(existing, id);
      if (shrunk == null)
        ridToVectorIds.remove(rid);
      else if (shrunk != existing)
        ridToVectorIds.put(rid, shrunk);
    } else {
      ridToVectorIds.computeIfPresent(rid, (k, existing) -> withoutValue(existing, id));
    }
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
   * Mark a vector as deleted (LSM tombstone).
   * Does not remove the entry - maintains LSM semantics.
   *
   * @param vectorId The vector ID to mark as deleted
   */
  public void markDeleted(final int vectorId) {
    final VectorLocation loc = locations.get(vectorId);
    if (loc != null)
      locations.put(vectorId, new VectorLocation(loc.isCompacted, loc.absoluteFileOffset, loc.rid, true));
  }

  /**
   * Get a stream of all vector IDs in the index.
   *
   * @return Stream of vector IDs
   */
  public IntStream getAllVectorIds() {
    if (maxSize > 0) {
      // Bounded backend is a Collections.synchronizedMap-wrapped LinkedHashMap: iteration requires holding the
      // wrapper monitor, and the linked list must not be mutated concurrently. Snapshot under the monitor and
      // stream the detached copy.
      final int[] ids;
      synchronized (locations) {
        ids = locations.keySet().stream().mapToInt(Integer::intValue).toArray();
      }
      return IntStream.of(ids);
    }
    // Unlimited backend is a ConcurrentHashMap: keySet() iteration is already thread-safe and weakly-consistent,
    // so stream it lazily without the O(N) snapshot allocation.
    return locations.keySet().stream().mapToInt(Integer::intValue);
  }

  /**
   * Get a stream of active (non-deleted) vector IDs.
   *
   * @return Stream of active vector IDs
   */
  public IntStream getActiveVectorIds() {
    if (maxSize > 0) {
      // Bounded backend: snapshot the active ids while holding the wrapper monitor, evaluating the deleted flag
      // inside the critical section so neither the iteration nor the per-entry read races with concurrent mutation.
      final int[] ids;
      synchronized (locations) {
        ids = locations.entrySet().stream()
            .filter(e -> !e.getValue().deleted)
            .mapToInt(Map.Entry::getKey)
            .toArray();
      }
      return IntStream.of(ids);
    }
    // Unlimited backend (ConcurrentHashMap): stream entrySet() lazily. Using entrySet() also avoids the extra
    // get() lookup the original keySet()+get() implementation performed.
    return locations.entrySet().stream()
        .filter(e -> !e.getValue().deleted)
        .mapToInt(Map.Entry::getKey);
  }

  /**
   * Get the total number of vectors (including deleted).
   *
   * @return Total number of vectors
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
    if (maxSize > 0) {
      // Bounded backend: iterate the values inside the wrapper monitor as required by Collections.synchronizedMap.
      synchronized (locations) {
        return locations.values().stream().filter(loc -> !loc.deleted).count();
      }
    }
    // Unlimited backend (ConcurrentHashMap): values() iteration is already thread-safe and weakly-consistent.
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
    if (maxSize > 0) {
      synchronized (locations) {
        locations.clear();
        ridToVectorIds.clear();
      }
    } else {
      locations.clear();
      ridToVectorIds.clear();
    }
    nextId.set(0);
  }
}
