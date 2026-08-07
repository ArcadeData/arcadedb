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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.util.Arrays;
import java.util.logging.Level;

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
  public static final int DEFAULT_CACHE_SIZE = 100_000;

  private final DatabaseInternal    database;
  private final int                 dimensions;
  private final String              vectorPropertyName;
  // Where locations are resolved from. During a search this is the index's live location map, read at traversal
  // time; during a graph build it is an immutable snapshot of the validated live set, so the build cannot be
  // disturbed by concurrent writes.
  //
  // Both are a VectorLocationIndex since issue #5588. The snapshot used to be a
  // Map<Integer, VectorLocationIndex.VectorLocation>, which cost the same ~90 bytes per vector the location index
  // itself used to cost - allocated in full, for the whole duration of every rebuild, on top of the live index.
  private final VectorLocationIndex locations;
  // True when `locations` is a build snapshot rather than the live map. The only behavioural difference is that a
  // build must not short-circuit to the vectors persisted inline in the graph file: it is building that file.
  private final boolean             graphBuilding;
  private final int[]               ordinalToVectorId;
  private final LSMVectorIndex      lsmIndex;         // Used for reading quantized vectors

  // Sentinel vector returned for deleted/missing ordinals to prevent NPE in JVector's GraphSearcher (issue #3715).
  // It is a placeholder of the right shape, NOT a low score: no vector scores low against every query, so callers
  // that score what getVector() returns must ask isDeletedSentinel() first and substitute their own floor.
  private final VectorFloat<?> deletedSentinelVector;

  // Cache for graph building and search - dramatically speeds up repeated vector access.
  // Lock-free and boxing-free to avoid mutex contention and Integer allocation on the hottest path of the
  // engine (issue #5412). During search this instance is owned by the LSMVectorIndex and shared by every
  // query, so a resident working set is not thrown away when a query ends.
  private final VectorCache vectorCache;

  private ArcadePageVectorValues(final DatabaseInternal database, final int dimensions,
      final String vectorPropertyName, final VectorLocationIndex locations, final boolean graphBuilding,
      final int[] ordinalToVectorId, final LSMVectorIndex lsmIndex, final VectorCache vectorCache) {
    this.database = database;
    this.dimensions = dimensions;
    this.vectorPropertyName = vectorPropertyName;
    this.locations = locations;
    this.graphBuilding = graphBuilding;
    this.ordinalToVectorId = ordinalToVectorId;
    this.lsmIndex = lsmIndex;
    this.vectorCache = vectorCache;
    this.deletedSentinelVector = createDeletedSentinelVector(dimensions);
  }

  /**
   * A reader over the index's LIVE location map, for searching.
   * <p>
   * A factory and not a constructor because since issue #5588 the search and the graph-build reader take the same
   * argument types and differ only in what they may short-circuit to, which is not something an overload set can
   * say out loud.
   */
  public static ArcadePageVectorValues forSearch(final DatabaseInternal database, final int dimensions,
      final String vectorPropertyName, final VectorLocationIndex locations, final int[] ordinalToVectorId) {
    return forSearch(database, dimensions, vectorPropertyName, locations, ordinalToVectorId, null, null);
  }

  /** A search reader that can also read quantized vectors straight from the index pages. */
  public static ArcadePageVectorValues forSearch(final DatabaseInternal database, final int dimensions,
      final String vectorPropertyName, final VectorLocationIndex locations, final int[] ordinalToVectorId,
      final LSMVectorIndex lsmIndex) {
    return forSearch(database, dimensions, vectorPropertyName, locations, ordinalToVectorId, lsmIndex, null);
  }

  /**
   * A search reader sharing the index-scoped vector cache (issue #5412). Before that cache, every query allocated
   * its own 1024-entry one and dropped it on completion, so a graph traversal re-read from disk every vector any
   * previous query had already materialized.
   */
  public static ArcadePageVectorValues forSearch(final DatabaseInternal database, final int dimensions,
      final String vectorPropertyName, final VectorLocationIndex locations, final int[] ordinalToVectorId,
      final LSMVectorIndex lsmIndex, final VectorCache sharedCache) {
    return new ArcadePageVectorValues(database, dimensions, vectorPropertyName, locations, false, ordinalToVectorId,
        lsmIndex, sharedCache);
  }

  /**
   * A reader over an immutable snapshot of the validated live set, for building a graph. It gets its own bounded
   * cache: the build reads every ordinal repeatedly and from many threads, and the cap is what keeps that from
   * becoming a second full copy of the vector set on heap (issue #3144).
   */
  public static ArcadePageVectorValues forGraphBuild(final DatabaseInternal database, final int dimensions,
      final String vectorPropertyName, final VectorLocationIndex snapshot, final int[] ordinalToVectorId,
      final LSMVectorIndex lsmIndex, final int cacheSize) {
    final int effectiveCacheSize = cacheSize <= 0 ? DEFAULT_CACHE_SIZE : cacheSize;
    return new ArcadePageVectorValues(database, dimensions, vectorPropertyName, snapshot, true, ordinalToVectorId,
        lsmIndex, new VectorCache(effectiveCacheSize));
  }

  /**
   * Whether this is the placeholder handed back for a vector that could not be read. {@link #getVector} never
   * returns null - a deleted, missing or unreadable ordinal yields the sentinel so JVector's traversal does not
   * NPE (issue #3715) - so a caller that needs a genuine vector has to ask.
   * <p>
   * The check is by reference, so it only recognises a placeholder that never left this instance. One written to disk
   * and read back - {@code LSMVectorIndexGraphFile} persists {@code getVector(ordinal)} inline when
   * {@code storeVectorsInGraph} is on - comes back as a different object and would be scored as an ordinary vector.
   * A rebuild excludes deleted ordinals from the graph it persists, so there is nothing to write today; what keeps
   * that case right regardless is {@link LiveVectorBitsFilter} and the location map, not this guard.
   */
  boolean isDeletedSentinel(final VectorFloat<?> vector) {
    return vector == deletedSentinelVector;
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
      return deletedSentinelVector;

    final int vectorId = ordinalToVectorId[ordinal];

    // Check cache first: during search this is the index-scoped cache shared by every query
    if (vectorCache != null) {
      final VectorFloat<?> cached = vectorCache.get(vectorId);
      if (cached != null)
        return cached;
    }

    // One lookup, one word: the offset and the compacted flag come out together so they cannot be read from two
    // different generations, and nothing is materialized for an ordinal this method is only going to reject.
    final long offsetAndFlag = locations == null ? VectorLocationIndex.ABSENT : locations.getOffsetAndFlag(vectorId);

    if (offsetAndFlag == VectorLocationIndex.ABSENT)
      // Return sentinel instead of null for deleted/missing entries (issue #3715).
      // JVector's GraphSearcher traverses deleted ordinals in the stale HNSW graph and
      // calls .length() on the vector, causing NPE if null. Results are filtered in post-processing.
      return deletedSentinelVector;

    // Phase 2: Try reading from graph file first if vectors are stored inline
    // Only during search, NOT during graph building: the build is what produces that file.
    if (lsmIndex != null && lsmIndex.metadata.storeVectorsInGraph && !graphBuilding) {
      try {
        final ImmutableGraphIndex graph = lsmIndex.getGraphIndex();
        if (graph instanceof OnDiskGraphIndex) {
          final OnDiskGraphIndex diskGraph =
              (OnDiskGraphIndex) graph;
          // Read vector directly from graph file (no RID lookup needed!)
          final VectorFloat<?> vector = diskGraph.getView().getVector(ordinal);
          if (vector != null) {
            // Track fetch source for metrics
            lsmIndex.metrics.incrementVectorFetchFromGraph();

            if (vectorCache != null)
              vectorCache.put(vectorId, vector);

            return vector;
          }
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error reading vector from graph file (ordinal=%d), falling back: %s",
            ordinal, e.getMessage());
      }
    }

    // If LSM index is available and quantization is enabled, try reading from index pages first
    if (lsmIndex != null) {
      try {
        final float[] vector = lsmIndex.readVectorFromOffset(VectorLocationIndex.offsetOf(offsetAndFlag),
            VectorLocationIndex.isCompactedOf(offsetAndFlag));
        if (vector != null) {
          // Successfully read quantized vector from index pages
          final VectorFloat<?> result = vts.createFloatVector(vector);

          // Track fetch source for metrics
          lsmIndex.metrics.incrementVectorFetchFromQuantized();

          // Cache the result. The cache has a fixed capacity and evicts on collision, so it cannot grow
          // past its budget (issue #3144) and it adapts to the working set instead of freezing on the
          // first vectors loaded (issue #5412).
          if (vectorCache != null)
            vectorCache.put(vectorId, result);

          return result;
        }
      } catch (final Exception e) {
        // Fall through to document-based retrieval
        LogManager.instance().log(this, Level.WARNING,
            "Error reading quantized vector from index pages (ordinal=%d), falling back to document: %s",
            ordinal, e.getMessage());
      }
    }

    // Fall back to reading from document (for non-quantized indexes or if quantized read failed). This is the
    // only branch that needs the RID as an object, so it is the only one that materializes it - and it is about to
    // do a record read, so the allocation is noise next to what it enables.
    //
    // It is also the only branch that resolves the id's chunk twice, having already done so for the offset above,
    // where the single getLocation() this replaces resolved it once. Quantized indexes never reach here, and the
    // second resolution is an array index and a volatile read against the lookupByRID on the next line, so the
    // asymmetry is real but not worth a combined accessor that would exist for one caller.
    final RID rid = locations.getRid(vectorId);
    if (rid == null)
      return deletedSentinelVector;

    try {
      final Record record = database.lookupByRID(rid, false);

      final Document doc = (Document) record;
      final Object vectorObj = doc.get(vectorPropertyName);
      if (vectorObj == null) {
        // Log the first few failures to help debug
        if (ordinal < 5) {
          LogManager.instance().log(this, Level.SEVERE,
              "Vector property '%s' not found in document %s (ordinal=%d). Available properties: %s",
              vectorPropertyName, rid, ordinal, doc.getPropertyNames());
        }
        return deletedSentinelVector;
      }

      // Unsupported vector property types surface as a WARNING for operational triage.
      final float[] vector;
      try {
        vector = VectorUtils.toFloatArray(vectorObj,
            lsmIndex != null ? lsmIndex.getMetadata().encoding : VectorEncoding.FLOAT32);
      } catch (final IllegalArgumentException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Vector property '%s' has unsupported type %s (RID=%s): %s",
            vectorPropertyName, vectorObj.getClass().getName(), rid, e.getMessage());
        return deletedSentinelVector;
      }

      if (vector.length != dimensions) {
        LogManager.instance().log(this, Level.WARNING,
            "Vector dimension mismatch: expected %d, got %d (RID=%s)",
            dimensions, vector.length, rid);
        return deletedSentinelVector;
      }

      if (VectorUtils.isZeroVector(vector))
        return deletedSentinelVector;

      final VectorFloat<?> result = vts.createFloatVector(vector);

      // Track fetch source for metrics
      if (lsmIndex != null)
        lsmIndex.metrics.incrementVectorFetchFromDocuments();

      // Cache the result if caching is enabled
      if (vectorCache != null)
        vectorCache.put(vectorId, result);

      return result;

    } catch (final RecordNotFoundException e) {
      // DELETED RECORD — return sentinel to avoid NPE in JVector (issue #3715)
      return deletedSentinelVector;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error reading vector from document (ordinal=%d, RID=%s): %s", ordinal, rid, e.getMessage());
      return deletedSentinelVector;
    }
  }

  /**
   * Pre-populates the cache with a vector for a given vectorId.
   * Must be called from a thread that has a database context (e.g., the main thread during validation).
   * This allows JVector's parallel ForkJoinPool threads to find vectors in the cache
   * without needing their own database context for lookupByRID.
   */
  public void putInCache(final int vectorId, final VectorFloat<?> vector) {
    // The cache has a fixed capacity (issue #3144), so warming it from the validation phase cannot hold
    // a full second copy of the vector set on heap.
    if (vectorCache != null)
      vectorCache.put(vectorId, vector);
  }

  @Override
  public boolean isValueShared() {
    // Each call to getVector() creates a new float array
    return false;
  }

  @Override
  public RandomAccessVectorValues copy() {
    // This implementation is thread-safe for reads (PageManager handles concurrency)
    //
    // DO NOT make this return a real copy. {@link #isDeletedSentinel} recognises the placeholder by reference, and
    // the sentinel is per-instance, so a copy would carry a different one: every caller that scores through the copy
    // would stop recognising it and would score the placeholder as if it were a vector. That is issue #5558's second
    // cause, and it would come back silently - the placeholder is finite now, so it produces a plausible score rather
    // than the Infinity that used to make it obvious. Sharing one instance is what keeps the guard total.
    return this;
  }

  /**
   * Creates the placeholder handed back for a deleted or unreadable ordinal.
   * <p>
   * It only has to be a well-formed vector of the right dimension: {@link #isDeletedSentinel} is how a caller
   * recognises it, and a caller that scores it anyway must at least get a finite number back. The value used to be
   * {@code Float.MIN_NORMAL} on the theory that it would score very low, which it does not - cosine cancels the
   * magnitude out, and the squared magnitude {@code dimensions * MIN_NORMAL^2} underflows to zero in float, so the
   * similarity came back {@code Infinity} and made every tombstone the best candidate in the beam (issue #5558).
   * <p>
   * <b>There is no safer value to pick.</b> The two differ only in magnitude, so under cosine they point the same way
   * and would score identically if the old one had not underflowed - swapping them changed a broken number into a
   * defined one, not a high rank into a low one. And no constant can do better: for any fixed vector {@code s} there
   * are queries scoring it anywhere in range, so "a placeholder that scores toward the floor" does not exist. That is
   * why the floor is applied by the score function that recognises the placeholder, and why it matters that an
   * unguarded scoring path now fails quietly - it gets a plausible number rather than the {@code Infinity} that used
   * to trip an assertion. {@link #isDeletedSentinel} lists the callers that must ask.
   */
  private static VectorFloat<?> createDeletedSentinelVector(final int dimensions) {
    final float[] sentinel = new float[dimensions];
    Arrays.fill(sentinel, 1.0f);
    return vts.createFloatVector(sentinel);
  }
}
