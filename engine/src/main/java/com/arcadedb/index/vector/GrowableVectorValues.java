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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.log.LogManager;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

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

  // Lazy-load support: when a vector is not in the map, read from disk
  private final VectorLocationIndex vectorIndex;
  private final LSMVectorIndex lsmIndex;
  private final DatabaseInternal database;
  private final String vectorPropertyName;

  /**
   * Simple mode: no disk fallback (used in tests and when all vectors are in memory).
   */
  GrowableVectorValues(final int dimensions) {
    this(dimensions, 1024, null, null, null, null);
  }

  /**
   * Simple mode with initial capacity.
   */
  GrowableVectorValues(final int dimensions, final int initialCapacity) {
    this(dimensions, initialCapacity, null, null, null, null);
  }

  /**
   * Full mode with lazy disk fallback for existing vectors.
   */
  GrowableVectorValues(final int dimensions, final int initialCapacity,
      final VectorLocationIndex vectorIndex, final LSMVectorIndex lsmIndex,
      final DatabaseInternal database, final String vectorPropertyName) {
    this.dimensions = dimensions;
    this.vectors = new ConcurrentHashMap<>(Math.max(16, initialCapacity));
    this.vectorIndex = vectorIndex;
    this.lsmIndex = lsmIndex;
    this.database = database;
    this.vectorPropertyName = vectorPropertyName;
  }

  void addVector(final int ordinal, final VectorFloat<?> vector) {
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

    // Slow path: lazy-load from disk and cache
    if (vectorIndex == null || database == null)
      return null;

    final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(ordinal);
    if (loc == null || loc.deleted)
      return null;

    try {
      float[] vector = null;

      // Try quantized pages first (INT8/BINARY)
      if (lsmIndex != null)
        vector = lsmIndex.readVectorFromOffset(loc.absoluteFileOffset, loc.isCompacted);

      // Fall back to document lookup
      if (vector == null && vectorPropertyName != null) {
        final var record = database.lookupByRID(loc.rid, false);
        final Document doc = (Document) record;
        vector = VectorUtils.convertToFloatArray(doc.get(vectorPropertyName));
      }

      if (vector != null && vector.length == dimensions && !VectorUtils.isZeroVector(vector)) {
        final VectorFloat<?> vf = vts.createFloatVector(vector);
        vectors.put(ordinal, vf); // Cache for next access
        return vf;
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE,
          "Could not lazy-load vector ordinal=%d: %s", ordinal, e.getMessage());
    }

    return null;
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
