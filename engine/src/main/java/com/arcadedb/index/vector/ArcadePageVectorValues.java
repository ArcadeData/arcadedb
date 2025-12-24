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
import com.arcadedb.exception.RecordNotFoundException;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

/**
 * Implements JVector's RandomAccessVectorValues interface with lazy-loading from ArcadeDB pages.
 * Vectors are read from disk on-demand rather than being stored in memory, dramatically reducing
 * RAM usage while leveraging ArcadeDB's PageManager cache for performance.
 * <p>
 * Thread-safe for concurrent reads (each thread gets its own page references from PageManager).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageVectorValues implements RandomAccessVectorValues {
  private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

  private final DatabaseInternal                                           database;
  private final int                                                        dimensions;
  private final String                                                     vectorPropertyName;
  private final VectorLocationIndex                                        vectorIndex;      // Used for live reads
  private final java.util.Map<Integer, VectorLocationIndex.VectorLocation> vectorSnapshot;   // Used for graph building
  private final int[]                                                      ordinalToVectorId;
  private final LSMVectorIndex                                             lsmIndex;         // Used for reading quantized vectors

  // Cache for graph building - dramatically speeds up repeated vector access
  private final java.util.concurrent.ConcurrentHashMap<Integer, VectorFloat<?>> vectorCache;

  // Constructor for live reads (uses shared vectorIndex, no cache needed)
  public ArcadePageVectorValues(final DatabaseInternal database, final int dimensions, final String vectorPropertyName,
      final VectorLocationIndex vectorIndex, final int[] ordinalToVectorId) {
    this(database, dimensions, vectorPropertyName, vectorIndex, ordinalToVectorId, null);
  }

  // Constructor for live reads with LSM index reference (for quantization support)
  public ArcadePageVectorValues(final DatabaseInternal database, final int dimensions, final String vectorPropertyName,
      final VectorLocationIndex vectorIndex, final int[] ordinalToVectorId, final LSMVectorIndex lsmIndex) {
    this.database = database;
    this.dimensions = dimensions;
    this.vectorPropertyName = vectorPropertyName;
    this.vectorIndex = vectorIndex;
    this.vectorSnapshot = null;
    this.ordinalToVectorId = ordinalToVectorId;
    this.lsmIndex = lsmIndex;
    this.vectorCache = null; // No cache for live reads (search only reads each vector once)
  }

  // Constructor for graph building (uses immutable snapshot + cache for performance)
  public ArcadePageVectorValues(final DatabaseInternal database, final int dimensions, final String vectorPropertyName,
      final java.util.Map<Integer, VectorLocationIndex.VectorLocation> vectorSnapshot, final int[] ordinalToVectorId) {
    this(database, dimensions, vectorPropertyName, vectorSnapshot, ordinalToVectorId, null);
  }

  // Constructor for graph building with LSM index reference (for quantization support)
  public ArcadePageVectorValues(final DatabaseInternal database, final int dimensions, final String vectorPropertyName,
      final java.util.Map<Integer, VectorLocationIndex.VectorLocation> vectorSnapshot, final int[] ordinalToVectorId,
      final LSMVectorIndex lsmIndex) {
    this.database = database;
    this.dimensions = dimensions;
    this.vectorPropertyName = vectorPropertyName;
    this.vectorIndex = null;
    this.vectorSnapshot = vectorSnapshot;
    this.ordinalToVectorId = ordinalToVectorId;
    this.lsmIndex = lsmIndex;
    this.vectorCache = new java.util.concurrent.ConcurrentHashMap<>(); // Cache for graph building (vectors accessed many times)
  }

  @Override
  public int size() {
    return ordinalToVectorId != null ? ordinalToVectorId.length : 0;
  }

  @Override
  public int dimension() {
    return dimensions;
  }

  @Override
  public VectorFloat<?> getVector(final int ordinal) {
    if (ordinal < 0 || ordinalToVectorId == null || ordinal >= ordinalToVectorId.length)
      return null;

    final int vectorId = ordinalToVectorId[ordinal];

    // Check cache first (for graph building - dramatically speeds up repeated access)
    if (vectorCache != null) {
      final VectorFloat<?> cached = vectorCache.get(vectorId);
      if (cached != null)
        return cached;
    }

    // Use snapshot if available (during graph building), otherwise use live vectorIndex
    final VectorLocationIndex.VectorLocation loc;
    if (vectorSnapshot != null)
      loc = vectorSnapshot.get(vectorId);
    else if (vectorIndex != null)
      loc = vectorIndex.getLocation(vectorId);
    else
      loc = null;

    if (loc == null || loc.deleted)
      return null;

    // If LSM index is available and quantization is enabled, try reading from index pages first
    if (lsmIndex != null) {
      try {
        final float[] vector = lsmIndex.readVectorFromOffset(loc.absoluteFileOffset, loc.isCompacted);
        if (vector != null) {
          // Successfully read quantized vector from index pages
          final VectorFloat<?> result = vts.createFloatVector(vector);

          // Cache the result if caching is enabled
          if (vectorCache != null)
            vectorCache.put(vectorId, result);

          return result;
        }
      } catch (final Exception e) {
        // Fall through to document-based retrieval
        com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
            "Error reading quantized vector from index pages (ordinal=%d), falling back to document: %s",
            ordinal, e.getMessage());
      }
    }

    // Fall back to reading from document (for non-quantized indexes or if quantized read failed)
    try {
      final com.arcadedb.database.Record record = database.lookupByRID(loc.rid, false);

      final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) record;
      final Object vectorObj = doc.get(vectorPropertyName);
      if (vectorObj == null) {
        // Log the first few failures to help debug
        if (ordinal < 5) {
          com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.SEVERE,
              "Vector property '%s' not found in document %s (ordinal=%d). Available properties: %s",
              vectorPropertyName, loc.rid, ordinal, doc.getPropertyNames());
        }
        return null; // Property not found
      }

      if (!(vectorObj instanceof float[] vector)) {
        com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
            "Vector property '%s' is not float[] (type=%s, RID=%s)",
            vectorPropertyName, vectorObj.getClass().getName(), loc.rid);
        return null;
      }

      if (vector.length != dimensions) {
        com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
            "Vector dimension mismatch: expected %d, got %d (RID=%s)",
            dimensions, vector.length, loc.rid);
        return null;
      }

      // Safety check: Validate vector is not all zeros (would cause NaN in cosine similarity)
      boolean hasNonZero = false;
      for (float v : vector) {
        if (v != 0.0f) {
          hasNonZero = true;
          break;
        }
      }

      if (!hasNonZero)
        return null; // Zero vectors cause NaN in cosine similarity

      final VectorFloat<?> result = vts.createFloatVector(vector);

      // Cache the result if caching is enabled (for graph building performance)
      if (vectorCache != null)
        vectorCache.put(vectorId, result);

      return result;

    } catch (final RecordNotFoundException e) {
      // DELETED RECORD
      return null;
    } catch (final Exception e) {
      com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
          "Error reading vector from document (ordinal=%d, RID=%s): %s", ordinal, loc.rid, e.getMessage());
      return null;
    }
  }

  @Override
  public boolean isValueShared() {
    // Each call to getVector() creates a new float array
    return false;
  }

  @Override
  public RandomAccessVectorValues copy() {
    // This implementation is thread-safe for reads (PageManager handles concurrency)
    return this;
  }
}
