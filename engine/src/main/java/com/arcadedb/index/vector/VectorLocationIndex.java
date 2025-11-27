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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Lightweight index that stores only vector location metadata (page number, offset, RID)
 * instead of the full vector data. This dramatically reduces memory usage:
 * ~20 bytes per vector vs ~3KB for a 768-dimension vector.
 *
 * Used by LSMVectorIndex to implement lazy-loading of vectors from disk pages.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorLocationIndex {
  private final ConcurrentHashMap<Integer, VectorLocation> locations;
  private final AtomicInteger                               nextId;

  /**
   * Represents the physical location of a vector on disk.
   * Total size: ~20 bytes (vs ~3KB for actual vector data)
   */
  public static class VectorLocation {
    public final boolean isCompacted;  // 1 byte - true if in compacted file, false if in mutable file
    public final int     pageNum;      // 4 bytes - which page contains the vector
    public final int     pageOffset;   // 4 bytes - offset within the page
    public final RID     rid;          // 12 bytes - document RID (bucketId + position)
    public final boolean deleted;      // 1 byte - LSM tombstone flag

    public VectorLocation(final boolean isCompacted, final int pageNum, final int pageOffset, final RID rid, final boolean deleted) {
      this.isCompacted = isCompacted;
      this.pageNum = pageNum;
      this.pageOffset = pageOffset;
      this.rid = rid;
      this.deleted = deleted;
    }
  }

  public VectorLocationIndex() {
    this.locations = new ConcurrentHashMap<>();
    this.nextId = new AtomicInteger(0);
  }

  public VectorLocationIndex(final int initialCapacity) {
    this.locations = new ConcurrentHashMap<>(initialCapacity);
    this.nextId = new AtomicInteger(0);
  }

  /**
   * Add a new vector location with an auto-generated ID.
   *
   * @param isCompacted True if the vector is in the compacted file, false if in mutable file
   * @param pageNum     The page number where the vector is stored
   * @param pageOffset  The offset within the page
   * @param rid         The document RID
   * @return The assigned vector ID
   */
  public int addVector(final boolean isCompacted, final int pageNum, final int pageOffset, final RID rid) {
    final int id = nextId.getAndIncrement();
    locations.put(id, new VectorLocation(isCompacted, pageNum, pageOffset, rid, false));
    return id;
  }

  /**
   * Add or update a vector location with a specific ID.
   * Used during loading from pages (LSM style: later entries override earlier ones).
   *
   * @param id          The vector ID
   * @param isCompacted True if the vector is in the compacted file, false if in mutable file
   * @param pageNum     The page number where the vector is stored
   * @param pageOffset  The offset within the page
   * @param rid         The document RID
   * @param deleted     Whether this vector is deleted (LSM tombstone)
   */
  public void addOrUpdate(final int id, final boolean isCompacted, final int pageNum, final int pageOffset, final RID rid, final boolean deleted) {
    locations.put(id, new VectorLocation(isCompacted, pageNum, pageOffset, rid, deleted));

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
   * @return The location metadata, or null if not found
   */
  public VectorLocation getLocation(final int vectorId) {
    return locations.get(vectorId);
  }

  /**
   * Mark a vector as deleted (LSM tombstone).
   * Does not remove the entry - maintains LSM semantics.
   *
   * @param vectorId The vector ID to mark as deleted
   */
  public void markDeleted(final int vectorId) {
    final VectorLocation loc = locations.get(vectorId);
    if (loc != null) {
      locations.put(vectorId, new VectorLocation(loc.isCompacted, loc.pageNum, loc.pageOffset, loc.rid, true));
    }
  }

  /**
   * Get a stream of all vector IDs in the index.
   *
   * @return Stream of vector IDs
   */
  public IntStream getAllVectorIds() {
    return locations.keySet().stream().mapToInt(Integer::intValue);
  }

  /**
   * Get a stream of active (non-deleted) vector IDs.
   *
   * @return Stream of active vector IDs
   */
  public IntStream getActiveVectorIds() {
    return locations.keySet().stream()
        .filter(id -> !locations.get(id).deleted)
        .mapToInt(Integer::intValue);
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
    nextId.set(0);
  }
}
