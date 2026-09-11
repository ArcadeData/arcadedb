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


import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A growable RandomAccessVectorValues with lazy disk fallback.
 * <p>
 * New vectors inserted via {@link #addVector} are cached in memory (ConcurrentHashMap).
 * Existing vectors not in the cache are loaded lazily from ArcadeDB pages/documents
 * on first access and then cached. This avoids pre-loading all vectors at startup
 * while keeping frequently-accessed vectors fast.
 * <p>
 * Thread-safe for concurrent reads and writes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrowableVectorValues implements RandomAccessVectorValues {
  private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

  private final int dimensions;
  private final ConcurrentHashMap<Integer, VectorFloat<?>> vectors;
  private final AtomicInteger count = new AtomicInteger(0);
  // Upper bound on the number of vectors kept on-heap. When the disk fallback is available the
  // map is a pure cache, so it is capped (issue #3144: an unbounded cache held a second full copy
  // of the whole vector set during bulk ingest). Evicted/never-cached ordinals are re-read lazily.
  private final int maxCacheSize;

  /**
   * The index that persisted these vectors, or {@code null} in simple mode (no disk fallback).
   * <p>
   * The only collaborator left: this used to carry a {@code VectorLocationIndex}, a {@code DatabaseInternal} and
   * the vector property name as well, so that it could resolve an evicted ordinal itself. That resolution now
   * lives on the index, as {@link LSMVectorIndex#readPersistedVectorArray(int)}, which is where the delta scan
   * reaches it too (issue #7357) - and the three fields went with it (PR #7360 review). They were always passed
   * all-or-nothing with this one, so nothing that used to have a fallback has lost it.
   */
  private final LSMVectorIndex lsmIndex;

  /**
   * Simple mode: no disk fallback (used in tests and when all vectors are in memory).
   */
  GrowableVectorValues(final int dimensions) {
    this(dimensions, 1024, null, Integer.MAX_VALUE);
  }

  /**
   * Simple mode with initial capacity.
   */
  GrowableVectorValues(final int dimensions, final int initialCapacity) {
    this(dimensions, initialCapacity, null, Integer.MAX_VALUE);
  }

  /**
   * Full mode with lazy disk fallback for existing vectors and a bounded cache.
   * <p>
   * {@code maxCacheSize <= 0} means unbounded (backward compatible). A positive value caps the
   * number of vectors held on-heap; once the cap is reached new vectors are not cached and are
   * re-read from disk on next access via {@link #getVector}. This only makes sense when a disk
   * fallback is configured - callers using simple mode must leave the cache unbounded.
   *
   * @param dimensions     arity of every vector held here
   * @param initialCapacity initial size of the backing map
   * @param lsmIndex       the index to re-read an evicted ordinal from, or {@code null} for simple mode
   * @param maxCacheSize   upper bound on the vectors kept on-heap; {@code <= 0} means unbounded
   */
  GrowableVectorValues(final int dimensions, final int initialCapacity, final LSMVectorIndex lsmIndex,
      final int maxCacheSize) {
    this.dimensions = dimensions;
    this.vectors = new ConcurrentHashMap<>(Math.max(16, Math.min(initialCapacity, maxCacheSize <= 0 ? initialCapacity : maxCacheSize)));
    this.lsmIndex = lsmIndex;
    this.maxCacheSize = maxCacheSize <= 0 ? Integer.MAX_VALUE : maxCacheSize;
  }

  void addVector(final int ordinal, final VectorFloat<?> vector) {
    // Cache the vector only while under the cap; beyond it we rely on the lazy disk fallback in
    // getVector(). The logical count is advanced regardless so size() reflects every added ordinal.
    if (vector != null && vectors.size() < maxCacheSize)
      vectors.put(ordinal, vector);
    int current;
    while ((current = count.get()) <= ordinal)
      count.compareAndSet(current, ordinal + 1);
  }

  void removeVector(final int ordinal) {
    vectors.remove(ordinal);
  }

  @Override
  public int size() {
    return count.get();
  }

  @Override
  public int dimension() {
    return dimensions;
  }

  @Override
  public VectorFloat<?> getVector(final int ordinal) {
    // Fast path: check in-memory cache
    final VectorFloat<?> cached = vectors.get(ordinal);
    if (cached != null)
      return cached;

    // Slow path: lazy-load from disk and cache. Simple mode has nothing to read from.
    if (lsmIndex == null)
      return null;

    // The read-back itself, and the validation of what comes back, belong to the index that persisted it: see
    // LSMVectorIndex.readPersistedVectorArray(). What stays here is the caching policy, which is this cache's own.
    final float[] vector = lsmIndex.readPersistedVectorArray(ordinal);
    if (vector == null)
      return null;

    final VectorFloat<?> vf = vts.createFloatVector(vector);
    if (vectors.size() < maxCacheSize)
      vectors.put(ordinal, vf); // Cache for next access while under the cap (issue #3144)
    return vf;
  }

  @Override
  public boolean isValueShared() {
    return false;
  }

  @Override
  public RandomAccessVectorValues copy() {
    return this;
  }

  int vectorCount() {
    return vectors.size();
  }
}
