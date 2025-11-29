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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.IOException;

/**
 * Implements JVector's RandomAccessVectorValues interface with lazy-loading from ArcadeDB pages.
 * Vectors are read from disk on-demand rather than being stored in memory, dramatically reducing
 * RAM usage while leveraging ArcadeDB's PageManager cache for performance.
 *
 * Thread-safe for concurrent reads (each thread gets its own page references from PageManager).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageVectorValues implements RandomAccessVectorValues {
  private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

  private final DatabaseInternal                                                database;
  private final int                                                             mutableFileId;
  private final int                                                             compactedFileId;
  private final int                                                             pageSize;
  private final int                                                             dimensions;
  private final VectorLocationIndex                                             vectorIndex;      // Used for live reads
  private final java.util.Map<Integer, VectorLocationIndex.VectorLocation>     vectorSnapshot;   // Used for graph building
  private final int[]                                                           ordinalToVectorId;

  // Constructor for live reads (uses shared vectorIndex)
  public ArcadePageVectorValues(final DatabaseInternal database, final int mutableFileId, final int compactedFileId, final int pageSize,
      final int dimensions, final VectorLocationIndex vectorIndex, final int[] ordinalToVectorId) {
    this.database = database;
    this.mutableFileId = mutableFileId;
    this.compactedFileId = compactedFileId;
    this.pageSize = pageSize;
    this.dimensions = dimensions;
    this.vectorIndex = vectorIndex;
    this.vectorSnapshot = null;
    this.ordinalToVectorId = ordinalToVectorId;
  }

  // Constructor for graph building (uses immutable snapshot)
  public ArcadePageVectorValues(final DatabaseInternal database, final int mutableFileId, final int compactedFileId, final int pageSize,
      final int dimensions, final java.util.Map<Integer, VectorLocationIndex.VectorLocation> vectorSnapshot, final int[] ordinalToVectorId) {
    this.database = database;
    this.mutableFileId = mutableFileId;
    this.compactedFileId = compactedFileId;
    this.pageSize = pageSize;
    this.dimensions = dimensions;
    this.vectorIndex = null;
    this.vectorSnapshot = vectorSnapshot;
    this.ordinalToVectorId = ordinalToVectorId;
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
    if (ordinal < 0 || ordinalToVectorId == null || ordinal >= ordinalToVectorId.length) {
      return null;
    }

    final int vectorId = ordinalToVectorId[ordinal];

    // Use snapshot if available (during graph building), otherwise use live vectorIndex
    final VectorLocationIndex.VectorLocation loc;
    if (vectorSnapshot != null) {
      loc = vectorSnapshot.get(vectorId);
    } else {
      loc = vectorIndex.getLocation(vectorId);
    }

    if (loc == null || loc.deleted) {
      return null;
    }

    try {
      // Read vector from page (cached by PageManager) without requiring transaction context
      // This is safe because we're only reading, and PageManager handles its own locking
      // IMPORTANT: Use correct fileId based on whether vector is in mutable or compacted file
      final int actualFileId = loc.isCompacted ? compactedFileId : mutableFileId;
      final BasePage page;
      try {
        page = database.getPageManager().getImmutablePage(
            new PageId(database, actualFileId, loc.pageNum), pageSize, false, false);
      } catch (final IllegalArgumentException e) {
        // Page doesn't exist (may have been compacted or deleted)
        // This should not happen if buildGraphFromScratch() properly validated pages
        com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
            "Vector %d references non-existent page %d (isCompacted=%b, ordinal=%d)", vectorId, loc.pageNum, loc.isCompacted, ordinal);
        throw new RuntimeException("Vector " + vectorId + " references non-existent page " + loc.pageNum + " (isCompacted=" + loc.isCompacted + ")", e);
      }

      if (page == null) {
        com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
            "Vector %d has null page %d (ordinal=%d)", vectorId, loc.pageNum, ordinal);
        throw new RuntimeException("Vector " + vectorId + " has null page " + loc.pageNum);
      }

      final float[] vector = readVectorFromPage(page, loc.pageOffset, dimensions);

      // Safety check: Validate vector is not all zeros (would cause NaN in cosine similarity)
      boolean hasNonZero = false;
      for (int i = 0; i < vector.length; i++) {
        if (vector[i] != 0.0f) {
          hasNonZero = true;
          break;
        }
      }

      if (!hasNonZero) {
        // Zero vectors cause NaN in cosine similarity
        // This should have been filtered during buildGraphFromScratch(), but if we encounter one:
        // 1. It might be due to replication lag or concurrent modifications
        // 2. It might be deleted/tombstone data that wasn't filtered properly
        // Log at WARNING level since this indicates potential data integrity issue
        com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
            "Skipping zero vector %d (ordinal=%d, pageNum=%d, offset=%d) - may be deleted or corrupted data",
            vectorId, ordinal, loc.pageNum, loc.pageOffset);
        // Throw exception to fail fast and identify the root cause
        throw new RuntimeException(
            "Zero vector encountered: vectorId=" + vectorId + ", ordinal=" + ordinal + ", page=" + loc.pageNum + ", offset="
                + loc.pageOffset);
      }

      return vts.createFloatVector(vector);

    } catch (final IOException e) {
      throw new RuntimeException("Error reading vector at ordinal " + ordinal + " (vectorId=" + vectorId + ")", e);
    }
  }

  /**
   * Read a vector from a specific offset in a page.
   * Vector entry format: id(4) + position(8) + bucketId(4) + vector(dimensions*4) + deleted(1)
   */
  private float[] readVectorFromPage(final BasePage page, final int offset, final int dims) {
    int pos = offset;

    // Skip: id(4) + position(8) + bucketId(4)
    pos += Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;

    // Read vector data
    final float[] vector = new float[dims];
    for (int i = 0; i < dims; i++) {
      vector[i] = page.readFloat(pos);
      pos += Binary.FLOAT_SERIALIZED_SIZE;
    }

    return vector;
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
