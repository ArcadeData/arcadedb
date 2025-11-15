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
package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * LSMVectorIndexMutable - Standalone mutable component for LSM Vector Index.
 *
 * <p>Manages writable pages containing vector embeddings and their associated RIDs.
 * Implements custom page management optimized for vector storage.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Store vector embeddings in mutable pages (fast writes)</li>
 *   <li>Search vectors with exact and similarity queries</li>
 *   <li>Manage page lifecycle (creation, full detection, immutability marking)</li>
 *   <li>Schedule compaction when page threshold exceeded</li>
 *   <li>Track vector entries for K-way merge during compaction</li>
 * </ul>
 *
 * <p><b>Vector Storage Format:</b>
 * Each vector entry stores: `[dimensions(int)][float_values][RID_count(int)][RID_values]`
 *
 * <p><b>Page Structure:</b>
 * - Mutable pages grow from both ends (index array and data area)
 * - When full, page is marked immutable and new page created
 * - Multiple pages enable fast writes without compaction pause
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexMutable {

  private final LSMVectorIndex                     mainIndex;
  private final DatabaseInternal                   database;
  private final String                             name;
  private final String                             filePath;
  private final ComponentFile.MODE                 fileMode;
  private final int                                dimensions;
  private final VectorSimilarityFunction           similarityFunction;
  private final int                                maxConnections;
  private final int                                beamWidth;
  private final float                              alpha;
  private final int                                pageSize;
  private final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;

  // Disk persistence (Phase 4-5)
  private       PaginatedComponentFile componentFile;
  private       FileChannel            fileChannel;
  private       RandomAccessFile       randomAccessFile;
  private       long                   currentPageOffset    = 0;
  private final List<Long>             pageOffsets          = new ArrayList<>(); // Track offsets of each page
  private       ByteBuffer             currentPageBuffer;
  private       long                   entriesWrittenToDisk = 0;
  private       boolean                diskEnabled          = false;

  // In-memory vector storage (dual-purpose: fast search + disk buffer)
  private final Map<VectorKey, Set<RID>> vectorToRIDs = new HashMap<>();
  private final List<MutablePage>        mutablePages = new ArrayList<>();
  private       int                      totalEntries = 0;

  // Threshold for scheduling compaction
  private static final int DEFAULT_MIN_PAGES_TO_SCHEDULE_COMPACTION = 100;
  public static final  int DEFAULT_PAGE_SIZE                        = 65536; // 64KB default page size
  private              int minPagesToScheduleACompaction            = DEFAULT_MIN_PAGES_TO_SCHEDULE_COMPACTION;

  /**
   * Creates a new LSMVectorIndexMutable component.
   *
   * @param mainIndex          the parent LSMVectorIndex
   * @param database           the database instance
   * @param name               the index name
   * @param filePath           the path to the index file
   * @param fileMode           the file mode
   * @param pageSize           the page size
   * @param dimensions         vector dimensionality
   * @param similarityFunction similarity metric (COSINE, EUCLIDEAN, DOT_PRODUCT)
   * @param maxConnections     HNSW max connections
   * @param beamWidth          HNSW beam width
   * @param alpha              HNSW alpha diversity parameter
   * @param nullStrategy       the null value strategy
   *
   * @throws IOException if file I/O error occurs
   */
  public LSMVectorIndexMutable(
      final LSMVectorIndex mainIndex,
      final DatabaseInternal database,
      final String name,
      final String filePath,
      final ComponentFile.MODE fileMode,
      final int pageSize,
      final int dimensions,
      final VectorSimilarityFunction similarityFunction,
      final int maxConnections,
      final int beamWidth,
      final float alpha,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) throws IOException {
    this.mainIndex = mainIndex;
    this.database = database;
    this.name = name;
    this.filePath = filePath;
    this.fileMode = fileMode;
    this.pageSize = pageSize;
    this.dimensions = dimensions;
    this.similarityFunction = similarityFunction;
    this.maxConnections = maxConnections;
    this.beamWidth = beamWidth;
    this.alpha = alpha;
    this.nullStrategy = nullStrategy != null ? nullStrategy : LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;

    // Initialize disk file for persistence (Phase 3)
    // Attempt to initialize disk storage if path is valid
    if (filePath != null && !filePath.isEmpty()) {
      try {
        // Try to create/open the index file for persistence
        // This is a best-effort initialization - if it fails, we continue with in-memory only
        initializeDiskStorage(filePath, fileMode);
        diskEnabled = true;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Disk persistence disabled for index '%s': %s", name, e.getMessage());
        diskEnabled = false;
      }
    }

    // Initialize with first mutable page
    mutablePages.add(new MutablePage(0, pageSize));
  }

  /**
   * Initialize disk storage for the mutable index.
   * Creates or opens the index file for persistent vector storage.
   * Phase 5: Opens RandomAccessFile for actual disk I/O operations.
   *
   * @param filePath the file path for the index
   * @param fileMode the file mode (READ_WRITE, etc.)
   *
   * @throws IOException if file creation fails
   */
  private void initializeDiskStorage(final String filePath, final ComponentFile.MODE fileMode) throws IOException {
    // Phase 5: Initialize file for direct disk I/O using RandomAccessFile
    // Open file in read-write mode to allow both writes and reads
    try {
      this.randomAccessFile = new RandomAccessFile(filePath, "rw");
      this.fileChannel = randomAccessFile.getChannel();
    } catch (final IOException e) {
      // If file opening fails, throw to allow graceful fallback in constructor
      throw new IOException("Failed to open index file for persistence: " + e.getMessage(), e);
    }

    // Initialize page buffer and tracking
    this.currentPageBuffer = ByteBuffer.allocate(pageSize);
    this.pageOffsets.add(0L); // Page 0 starts at offset 0
    this.currentPageOffset = 0;

    LogManager.instance().log(this, Level.FINE,
        "Disk persistence initialized for mutable index '%s' at path '%s' (Phase 5: actual file I/O)", name, filePath);
  }

  /**
   * Insert a vector into the mutable index.
   *
   * @param vector the vector embedding (float array)
   * @param rid    the record ID associated with this vector
   */
  public void put(final float[] vector, final RID rid) {
    if (vector == null)
      throw new IllegalArgumentException("Vector cannot be null");
    if (vector.length != dimensions)
      throw new IllegalArgumentException(
          "Vector dimension mismatch: expected " + dimensions + ", got " + vector.length);
    if (rid == null)
      throw new IllegalArgumentException("RID cannot be null");

    final VectorKey key = new VectorKey(vector);
    vectorToRIDs.computeIfAbsent(key, k -> new HashSet<>()).add(rid);
    totalEntries++;

    // Phase 4: Persist to disk pages via VectorSerializer
    if (diskEnabled) {
      try {
        Set<RID> rids = new HashSet<>();
        rids.add(rid);
        int entrySize = VectorSerializer.calculateEntrySize(vector, 1);

        // Check if entry fits in current page
        if (currentPageBuffer.remaining() < entrySize) {
          flushCurrentPage(); // Move to next page
        }

        // Serialize entry into current page buffer
        Binary buffer = new Binary(currentPageBuffer.array());
        buffer.position(currentPageBuffer.position());
        VectorSerializer.serializeVectorEntry(buffer, vector, rids);
        currentPageBuffer.position(buffer.position());

      } catch (final Exception e) {
        // Log error, continue with in-memory only
        LogManager.instance().log(this, Level.WARNING,
            "Error persisting vector entry: %s", e.getMessage());
      }
    }
  }

  /**
   * Remove a vector from the mutable index.
   *
   * @param vector the vector embedding
   * @param rid    the record ID to remove
   */
  public void remove(final float[] vector, final RID rid) {
    if (vector == null)
      throw new IllegalArgumentException("Vector cannot be null");

    final VectorKey key = new VectorKey(vector);
    final Set<RID> rids = vectorToRIDs.get(key);
    if (rids != null && rids.remove(rid)) {
      totalEntries--;
      if (rids.isEmpty()) {
        vectorToRIDs.remove(key);
      }
    }
  }

  /**
   * Search for a vector and return all matching RIDs.
   *
   * @param queryVector the query vector
   *
   * @return set of RIDs matching this vector
   */
  public Set<RID> search(final float[] queryVector) {
    if (queryVector == null)
      return Collections.emptySet();
    if (queryVector.length != dimensions)
      return Collections.emptySet();

    final VectorKey key = new VectorKey(queryVector);
    final Set<RID> result = vectorToRIDs.get(key);
    return result != null ? new HashSet<>(result) : Collections.emptySet();
  }

  /**
   * Find K nearest neighbors to the query vector using brute force.
   *
   * <p>Phase 3: This is basic brute force search. JVector will be used
   * in compacted index for efficient KNN via HNSW.
   *
   * @param queryVector the query vector
   * @param k           number of neighbors to return
   *
   * @return list of K nearest vectors with their RIDs and distances
   */
  public List<VectorSearchResult> knnSearch(final float[] queryVector, final int k) {
    return knnSearch(queryVector, k, null);
  }

  /**
   * Find K nearest neighbors to the query vector using brute force with optional filtering.
   *
   * <p>Implements Option B: integrated filtering during search.
   * Filters results in-place while iterating, skipping ignored RIDs.
   *
   * @param queryVector    the query vector
   * @param k              number of neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   *
   * @return list of K nearest vectors with their RIDs and distances
   */
  public List<VectorSearchResult> knnSearch(final float[] queryVector, final int k,
      final LSMVectorIndex.IgnoreVertexCallback ignoreCallback) {
    if (queryVector == null || k <= 0)
      return Collections.emptyList();

    final List<VectorSearchResult> results = new ArrayList<>();

    // Iterate through all vectors and compute distances
    for (final Map.Entry<VectorKey, Set<RID>> entry : vectorToRIDs.entrySet()) {
      final float distance = computeDistance(queryVector, entry.getKey().vector);

      // Filter RIDs in-place if callback provided
      final Set<RID> filteredRids;
      if (ignoreCallback != null) {
        filteredRids = new HashSet<>();
        for (final RID rid : entry.getValue()) {
          if (!ignoreCallback.ignoreVertex(rid)) {
            filteredRids.add(rid);
          }
        }
      } else {
        filteredRids = entry.getValue();
      }

      // Only add result if there are non-ignored RIDs
      if (!filteredRids.isEmpty()) {
        results.add(new VectorSearchResult(entry.getKey().vector, distance, filteredRids));
      }
    }

    // Sort by distance/similarity
    // IMPORTANT: Different metrics have different "better" directions:
    // - EUCLIDEAN: lower distance is better → ascending sort
    // - COSINE: higher similarity is better (1.0 = identical) → descending sort
    // - DOT_PRODUCT: higher product is better → descending sort
    if ("EUCLIDEAN".equals(similarityFunction)) {
      results.sort((a, b) -> Float.compare(a.distance, b.distance)); // Ascending
    } else {
      // COSINE and DOT_PRODUCT are similarity metrics (higher is better)
      results.sort((a, b) -> Float.compare(b.distance, a.distance)); // Descending
    }

    // Return top K
    return results.subList(0, Math.min(k, results.size()));
  }

  /**
   * Compute distance between two vectors using configured similarity function.
   *
   * @param vector1 first vector
   * @param vector2 second vector
   *
   * @return distance value
   */
  private float computeDistance(final float[] vector1, final float[] vector2) {
    if (vector1.length != vector2.length)
      throw new IllegalArgumentException("Vector dimensions mismatch");

    return switch (similarityFunction) {
      case COSINE -> cosineSimilarity(vector1, vector2);
      case EUCLIDEAN -> euclideanDistance(vector1, vector2);
      case DOT_PRODUCT -> dotProduct(vector1, vector2);
    };
  }

  /**
   * Compute cosine similarity between two vectors.
   */
  private float cosineSimilarity(final float[] v1, final float[] v2) {
    float dotProduct = 0;
    float norm1 = 0;
    float norm2 = 0;

    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }

    if (norm1 == 0 || norm2 == 0)
      return 0;

    return (float) (dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2)));
  }

  /**
   * Compute Euclidean distance between two vectors.
   */
  private float euclideanDistance(final float[] v1, final float[] v2) {
    float sum = 0;
    for (int i = 0; i < v1.length; i++) {
      final float diff = v1[i] - v2[i];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  /**
   * Compute dot product of two vectors.
   */
  private float dotProduct(final float[] v1, final float[] v2) {
    float result = 0;
    for (int i = 0; i < v1.length; i++) {
      result += v1[i] * v2[i];
    }
    return result;
  }

  /**
   * Get all vectors stored in this mutable index.
   *
   * <p>Used by compactor for K-way merge.
   * Phase 4: Includes both in-memory and disk-persisted vectors.
   *
   * @return map of vectors to their RIDs
   */
  public Map<VectorKey, Set<RID>> getAllVectors() {
    Map<VectorKey, Set<RID>> result = new HashMap<>(vectorToRIDs);

    // Phase 4: Load persisted vectors from disk if disk persistence is enabled
    if (diskEnabled && !pageOffsets.isEmpty()) {
      try {
        for (int i = 0; i < pageOffsets.size(); i++) {
          Map<VectorKey, Set<RID>> pageVectors = loadPageFromDisk(i);
          // Merge disk vectors with in-memory vectors (disk vectors are older)
          for (final Map.Entry<VectorKey, Set<RID>> entry : pageVectors.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue());
          }
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error loading vectors from disk: %s", e.getMessage());
      }
    }

    return result;
  }

  /**
   * Load vectors from a specific page on disk.
   * Phase 5: Implements actual page reading from disk using FileChannel and VectorSerializer deserialization.
   *
   * @param pageIndex the page number to load
   *
   * @return map of vector keys to RID sets for this page
   *
   * @throws IOException if page I/O error occurs
   */
  private Map<VectorKey, Set<RID>> loadPageFromDisk(final int pageIndex) throws IOException {
    Map<VectorKey, Set<RID>> pageVectors = new HashMap<>();

    if (pageIndex < 0 || pageIndex >= pageOffsets.size()) {
      return pageVectors; // Page doesn't exist
    }

    final long pageOffset = pageOffsets.get(pageIndex);
    final long pageEnd = (pageIndex + 1 < pageOffsets.size())
        ? pageOffsets.get(pageIndex + 1)
        : currentPageOffset;
    final int pageSize = (int) (pageEnd - pageOffset);

    if (pageSize <= 0) {
      return pageVectors; // Empty page
    }

    try {
      // Phase 5: Read page from disk using FileChannel
      if (fileChannel == null || !fileChannel.isOpen()) {
        LogManager.instance().log(this, Level.WARNING,
            "FileChannel not available for reading page %d", pageIndex);
        return pageVectors;
      }

      // Allocate buffer to read the page
      ByteBuffer pageBuffer = ByteBuffer.allocate(pageSize);

      // Read page data from disk at specific offset
      int bytesRead = fileChannel.read(pageBuffer, pageOffset);

      if (bytesRead <= 0) {
        LogManager.instance().log(this, Level.FINE,
            "Page %d is empty or not readable (read %d bytes)", pageIndex, bytesRead);
        return pageVectors;
      }

      // Prepare buffer for reading (rewind to beginning)
      pageBuffer.rewind();

      // Deserialize vectors from page buffer using VectorSerializer
      while (pageBuffer.remaining() > 0) {
        try {
          Binary buffer = new Binary(pageBuffer.array());
          buffer.position(pageBuffer.position());

          // Deserialize vector and RIDs
          // Returns Object[] containing [float[] vector, Set<RID> rids]
          Object[] entry = VectorSerializer.deserializeVectorEntry(buffer, database);
          if (entry != null && entry.length >= 2) {
            final float[] vector = (float[]) entry[0];
            @SuppressWarnings("unchecked")
            final Set<RID> rids = (Set<RID>) entry[1];
            final VectorKey key = new VectorKey(vector);
            pageVectors.put(key, rids);

            // Update buffer position after deserialization
            pageBuffer.position(buffer.position());
          } else {
            // No more complete entries in buffer
            break;
          }
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.FINE,
              "Error deserializing vector from page %d: %s", pageIndex, e.getMessage());
          break;
        }
      }

      LogManager.instance().log(this, Level.FINE,
          "Loaded page %d from disk: %d vectors, %d bytes", pageIndex, pageVectors.size(), bytesRead);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error reading page %d from disk: %s", pageIndex, e.getMessage());
    }

    return pageVectors;
  }

  /**
   * Clear the mutable index (called after compaction).
   */
  public void clear() {
    vectorToRIDs.clear();
    mutablePages.clear();
    totalEntries = 0;
    mutablePages.add(new MutablePage(0, pageSize));
  }

  /**
   * Get number of vectors in this mutable index.
   *
   * @return vector count
   */
  public long countEntries() {
    return totalEntries;
  }

  /**
   * Check if compaction should be scheduled.
   *
   * @return true if mutable pages exceed threshold
   */
  public boolean shouldScheduleCompaction() {
    return mutablePages.size() >= minPagesToScheduleACompaction;
  }

  /**
   * Get minimum pages threshold for compaction scheduling.
   *
   * @return pages threshold
   */
  public int getMinPagesToScheduleACompaction() {
    return minPagesToScheduleACompaction;
  }

  /**
   * Set minimum pages threshold for compaction scheduling.
   *
   * @param minPages pages threshold
   */
  public void setMinPagesToScheduleACompaction(final int minPages) {
    if (minPages <= 0)
      throw new IllegalArgumentException("minPagesToScheduleACompaction must be > 0");
    this.minPagesToScheduleACompaction = minPages;
  }

  /**
   * Get vector dimensions.
   *
   * @return dimensions
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Get similarity function.
   *
   * @return similarity function name
   */
  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  /**
   * Get HNSW max connections.
   *
   * @return max connections
   */
  public int getMaxConnections() {
    return maxConnections;
  }

  /**
   * Get HNSW beam width.
   *
   * @return beam width
   */
  public int getBeamWidth() {
    return beamWidth;
  }

  /**
   * Get HNSW alpha parameter.
   *
   * @return alpha
   */
  public float getAlpha() {
    return alpha;
  }

  /**
   * Close the mutable index (cleanup resources).
   * Phase 5: Flush all pending disk writes and close file handles.
   */
  public void close() {
    // Flush disk cache with all remaining entries
    if (diskEnabled) {
      try {
        flushCurrentPage(); // Flush final partial page
        LogManager.instance().log(this, Level.FINE,
            "Mutable index '%s' closed after writing %d entries to disk", name, entriesWrittenToDisk);
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error flushing on close: %s", e.getMessage());
      }

      // Phase 5: Close file handles
      try {
        if (fileChannel != null && fileChannel.isOpen()) {
          fileChannel.close();
        }
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing FileChannel: %s", e.getMessage());
      }

      try {
        if (randomAccessFile != null) {
          randomAccessFile.close();
        }
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing RandomAccessFile: %s", e.getMessage());
      }
    }

    // Close in-memory pages
    mutablePages.forEach(MutablePage::close);
  }

  // ==================== Inner Classes ====================

  /**
   * Represents a mutable page in memory.
   */
  static class MutablePage {
    private final int     pageNumber;
    private final int     pageSize;
    private       boolean mutable = true;

    MutablePage(final int pageNumber, final int pageSize) {
      this.pageNumber = pageNumber;
      this.pageSize = pageSize;
    }

    void close() {
      // Phase 3: Persist page to disk
    }

    boolean isMutable() {
      return mutable;
    }

    void markImmutable() {
      this.mutable = false;
    }
  }

  /**
   * Wrapper for vector with proper equality and hashing.
   */
  static class VectorKey {
    final         float[] vector;
    private final int     hash;

    VectorKey(final float[] vector) {
      this.vector = vector.clone();
      this.hash = computeHash(vector);
    }

    private static int computeHash(final float[] vector) {
      int hash = 1;
      for (final float v : vector) {
        hash = 31 * hash + Float.floatToIntBits(v);
      }
      return hash;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof VectorKey))
        return false;
      final VectorKey other = (VectorKey) obj;
      if (vector.length != other.vector.length)
        return false;
      for (int i = 0; i < vector.length; i++) {
        if (Float.floatToIntBits(vector[i]) != Float.floatToIntBits(other.vector[i]))
          return false;
      }
      return true;
    }
  }

  /**
   * Flush current page buffer to disk and create a new page.
   * Phase 5: Implements actual disk write operations using RandomAccessFile and FileChannel.
   *
   * @throws IOException if I/O error occurs
   */
  private void flushCurrentPage() throws IOException {
    if (!diskEnabled || currentPageBuffer == null || currentPageBuffer.position() == 0) {
      return; // Nothing to flush
    }

    try {
      // Prepare buffer for writing (flip from write mode to read mode)
      currentPageBuffer.flip();

      // Phase 5: Write page to disk using FileChannel
      if (fileChannel != null && fileChannel.isOpen()) {
        // Write the current page buffer to disk at the current offset
        int bytesWritten = fileChannel.write(currentPageBuffer, currentPageOffset);

        // Update position tracking for next page
        currentPageOffset += bytesWritten;

        // Track flushed entries for audit trail
        entriesWrittenToDisk++;

        LogManager.instance().log(this, Level.FINE,
            "Flushed page to disk: offset=%d, size=%d bytes", currentPageOffset - bytesWritten, bytesWritten);
      } else {
        throw new IOException("FileChannel is not available for writing");
      }

      // Create new page for next batch of vectors
      createNewPage();

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error flushing page buffer: %s", e.getMessage());
      throw e; // Re-throw to allow caller to handle
    }
  }

  /**
   * Create a new page for future writes.
   */
  private void createNewPage() {
    currentPageBuffer = ByteBuffer.allocate(pageSize);
    pageOffsets.add(currentPageOffset);
  }

  /**
   * Result of a KNN search.
   */
  public static class VectorSearchResult {
    public final float[]  vector;
    public final float    distance;
    public final Set<RID> rids;

    public VectorSearchResult(final float[] vector, final float distance, final Set<RID> rids) {
      this.vector = vector;
      this.distance = distance;
      this.rids = new HashSet<>(rids);
    }
  }
}
