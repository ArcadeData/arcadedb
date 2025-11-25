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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.BucketLSMVectorIndexBuilder;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.LockManager;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

/**
 * Vector index implementation using JVector library with page-based transactional storage.
 * This implementation stores vector data on disk using ArcadeDB's page system for transactional support.
 * Unlike HNSW which uses graph vertices/edges, this stores vectors directly in pages and maintains
 * the graph structure separately for better performance and transactional integrity.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndex implements com.arcadedb.index.Index, IndexInternal {
  public static final  String            FILE_EXT        = "lsmvecidx";
  public static final  int               CURRENT_VERSION = 0;
  public static final  int               DEF_PAGE_SIZE   = 262_144;
  private static final VectorTypeSupport vts             = VectorizationProvider.getInstance().getVectorTypeSupport();

  // Page header layout constants
  public static final int OFFSET_FREE_CONTENT = 0;  // 4 bytes
  public static final int OFFSET_NUM_ENTRIES  = 4;   // 4 bytes
  public static final int OFFSET_MUTABLE      = 8;       // 1 byte
  public static final int HEADER_BASE_SIZE    = 9;     // offsetFreeContent(4) + numberOfEntries(4) + mutable(1)

  private final String                 indexName;
  protected     LSMVectorIndexMutable  mutable;
  private final ReentrantReadWriteLock lock;
  private       LSMVectorIndexMetadata metadata;

  // In-memory JVector index (rebuilt from pages on load)
  private volatile ImmutableGraphIndex                     graphIndex;
  private volatile List<VectorEntry>                       graphIndexOrdinalMapping; // Maps graph ordinals to vector entries
  private final    ConcurrentHashMap<Integer, VectorEntry> vectorRegistry;
  private final    AtomicInteger                           nextId;
  private final    AtomicReference<INDEX_STATUS>           status;
  private final    AtomicBoolean                           graphIndexDirty;

  // Compaction support
  private final AtomicInteger           currentMutablePages;
  private final int                     minPagesToScheduleACompaction;
  private       LSMVectorIndexCompacted compactedSubIndex;
  private       boolean                 valid = true;

  /**
   * Represents a vector entry with its RID and vector data
   */
  private static class VectorEntry {
    final    int     id;
    final    RID     rid;
    final    float[] vector;
    volatile boolean deleted;

    VectorEntry(final int id, final RID rid, final float[] vector) {
      this.id = id;
      this.rid = rid;
      this.vector = vector;
      this.deleted = false;
    }
  }

  /**
   * Transaction-specific context for buffering vector operations
   */
  private static class TransactionVectorContext {
    final Map<RID, VectorOperation> operations = new ConcurrentHashMap<>();

    enum OperationType {
      ADD, REMOVE
    }

    static class VectorOperation {
      final OperationType type;
      final float[]       vector;

      VectorOperation(final OperationType type, final float[] vector) {
        this.type = type;
        this.vector = vector;
      }
    }
  }

  /**
   * Comparable wrapper for float[] to use in transaction tracking.
   * Vectors are compared by their hash code for uniqueness in the transaction map.
   */
  private static class ComparableVector implements Comparable<ComparableVector> {
    final float[] vector;
    final int     hashCode;

    ComparableVector(final float[] vector) {
      this.vector = vector;
      this.hashCode = java.util.Arrays.hashCode(vector);
    }

    @Override
    public int compareTo(final ComparableVector other) {
      // First compare by hash code for performance
      final int hashCompare = Integer.compare(this.hashCode, other.hashCode);
      if (hashCompare != 0)
        return hashCompare;

      // If hash codes are equal, perform lexicographical comparison of vector elements
      // to maintain the Comparable contract: compareTo == 0 iff equals == true
      final int minLength = Math.min(this.vector.length, other.vector.length);
      for (int i = 0; i < minLength; i++) {
        final int elementCompare = Float.compare(this.vector[i], other.vector[i]);
        if (elementCompare != 0)
          return elementCompare;
      }
      // If all compared elements are equal, compare by length
      return Integer.compare(this.vector.length, other.vector.length);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ComparableVector other))
        return false;
      return java.util.Arrays.equals(vector, other.vector);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder<? extends Index> builder) {
      final BucketLSMVectorIndexBuilder vectorBuilder = (BucketLSMVectorIndexBuilder) builder;

      return new LSMVectorIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getPageSize(),
          vectorBuilder.getTypeName(), vectorBuilder.getPropertyNames(),
          vectorBuilder.dimensions, vectorBuilder.similarityFunction, vectorBuilder.maxConnections, vectorBuilder.beamWidth,
          vectorBuilder.idPropertyName
      );
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      final LSMVectorIndex index = new LSMVectorIndex(database, name, filePath, id, mode, pageSize, version);
      return index.mutable;
    }
  }

  /**
   * Constructor for creating a new index
   */
  public LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final String typeName, final String[] propertyNames,
      final int dimensions, final VectorSimilarityFunction similarityFunction, final int maxConnections, final int beamWidth,
      final String idPropertyName) {
    try {
      this.indexName = name;

      this.metadata = new LSMVectorIndexMetadata(typeName, propertyNames, -1);
      this.metadata.dimensions = dimensions;
      this.metadata.similarityFunction = similarityFunction;
      this.metadata.maxConnections = maxConnections;
      this.metadata.beamWidth = beamWidth;
      this.metadata.idPropertyName = idPropertyName;

      this.lock = new ReentrantReadWriteLock();
      this.vectorRegistry = new ConcurrentHashMap<>();
      this.nextId = new AtomicInteger(0);
      this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);
      this.graphIndexDirty = new AtomicBoolean(false);

      // Initialize compaction fields
      this.currentMutablePages = new AtomicInteger(0); // No page0 - start with 0 pages
      this.minPagesToScheduleACompaction = database.getConfiguration()
          .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
      this.compactedSubIndex = null;

      // Create the component that handles page storage
      this.mutable = new LSMVectorIndexMutable(database, indexName, filePath, mode, pageSize);
      this.mutable.setMainIndex(this);

      initializeGraphIndex();
    } catch (final IOException e) {
      throw new IndexException("Error on creating index '" + name + "'", e);
    }
  }

  /**
   * Constructor for loading an existing index
   */
  protected LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    this.indexName = name;

    this.metadata = new LSMVectorIndexMetadata(null, new String[0], -1);
    this.lock = new ReentrantReadWriteLock();
    this.vectorRegistry = new ConcurrentHashMap<>();
    this.nextId = new AtomicInteger(0);
    this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);
    this.graphIndexDirty = new AtomicBoolean(false);

    // Create the component that handles page storage
    this.mutable = new LSMVectorIndexMutable(database, name, filePath, id, mode, pageSize, version);
    this.mutable.setMainIndex(this);

    // Initialize compaction fields
    this.currentMutablePages = new AtomicInteger(mutable.getTotalPages());
    this.minPagesToScheduleACompaction = database.getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    this.compactedSubIndex = null;

    // Load vectors from pages - only if this is an existing index file
    // During replication on replicas, the file may not exist yet and will be created/replicated later
    try {
      loadVectorsFromPages();

      // Rebuild graph index from loaded vectors
      initializeGraphIndex();
    } catch (final Exception e) {
      // If we can't load vectors (e.g., during initial replication), just use empty index
      // Metadata will be applied from schema and vectors will be populated as data arrives
      LogManager.instance().log(this, Level.FINE,
          "Could not load vectors from pages for index %s (may be a new replicated index): %s", name, e.getMessage());
      this.graphIndexDirty.set(true);
    }
  }

  private void initializeGraphIndex() {
    lock.writeLock().lock();
    try {
      // Build list of non-deleted vectors with index mapping
      final List<VectorEntry> nonDeletedVectors = vectorRegistry.values().stream()
          .filter(v -> !v.deleted)
          .sorted((a, b) -> Integer.compare(a.id, b.id)) // Sort by ID for consistent ordering
          .toList();

      // Create a RandomAccessVectorValues implementation from our vector registry
      final RandomAccessVectorValues vectors = new RandomAccessVectorValues() {
        @Override
        public int size() {
          return nonDeletedVectors.size();
        }

        @Override
        public int dimension() {
          return metadata.dimensions;
        }

        @Override
        public VectorFloat<?> getVector(int i) {
          if (i < 0 || i >= nonDeletedVectors.size())
            return null;
          final VectorEntry entry = nonDeletedVectors.get(i);
          return entry != null ? vts.createFloatVector(entry.vector) : null;
        }

        @Override
        public boolean isValueShared() {
          return false;
        }

        @Override
        public RandomAccessVectorValues copy() {
          return this;
        }
      };

      if (vectors.size() > 0) {
        // Build the graph index using JVector 4.0 API
        LogManager.instance().log(this, Level.INFO, "Building JVector graph index with " + vectors.size() + " vectors");

        // Create BuildScoreProvider for index construction
        final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(vectors, metadata.similarityFunction);

        // Build the graph index
        try (final GraphIndexBuilder builder = new GraphIndexBuilder(
            scoreProvider,
            metadata.dimensions,
            metadata.maxConnections,  // M parameter (graph degree)
            metadata.beamWidth,       // efConstruction (construction search depth)
            1.2f,            // neighbor overflow factor
            1.2f,            // alpha diversity relaxation
            false,           // no distance transform
            true)) {         // enable concurrent updates

          this.graphIndex = builder.build(vectors);
          // Store the mapping for search results (ordinal -> VectorEntry)
          this.graphIndexOrdinalMapping = new ArrayList<>(nonDeletedVectors);
          LogManager.instance().log(this, Level.INFO, "JVector graph index built successfully");
        }
      } else {
        this.graphIndex = null;
        this.graphIndexOrdinalMapping = null;
        LogManager.instance().log(this, Level.INFO, "No vectors to index, graph index is null");
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error initializing JVector graph index", e);
      throw new IndexException("Error initializing JVector graph index", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Load all vectors from LSM-style pages.
   * Reads from all pages, later entries override earlier ones (LSM merge-on-read).
   */
  private void loadVectorsFromPages() {
    try {
      // NOTE: All metadata (dimensions, similarityFunction, maxConnections, beamWidth) comes from schema JSON
      // via applyMetadataFromSchema(). Pages contain only vector data, no metadata.

      // Read all data pages (starting from page 0) in LSM style
      final int totalPages = getTotalPages();
      int entriesRead = 0;
      int maxVectorId = -1; // Track max ID to compute nextId

      for (int pageNum = 0; pageNum < totalPages; pageNum++) {
        final BasePage currentPage = getDatabase().getTransaction().getPage(
            new PageId(getDatabase(), getFileId(), pageNum), getPageSize());

        // Read page header
        final int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        final int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);
        final byte mutable = currentPage.readByte(OFFSET_MUTABLE); // Read mutable flag (but don't use it during loading)

        if (numberOfEntries == 0)
          continue; // Empty page

        // Read pointer table (starts at HEADER_BASE_SIZE offset)
        final int[] pointers = new int[numberOfEntries];
        for (int i = 0; i < numberOfEntries; i++)
          pointers[i] = currentPage.readInt(HEADER_BASE_SIZE + (i * 4));

        // Read entries using pointers
        for (int i = 0; i < numberOfEntries; i++) {
          int pos = pointers[i];

          final int id = currentPage.readInt(pos);
          pos += Binary.INT_SERIALIZED_SIZE;

          final long position = currentPage.readLong(pos);
          pos += Binary.LONG_SERIALIZED_SIZE;

          final int bucketId = currentPage.readInt(pos);
          pos += Binary.INT_SERIALIZED_SIZE;

          final RID rid = new RID(getDatabase(), bucketId, position);

          final float[] vector = new float[metadata.dimensions];
          for (int j = 0; j < metadata.dimensions; j++) {
            vector[j] = currentPage.readFloat(pos);
            pos += Binary.FLOAT_SERIALIZED_SIZE;
          }

          final boolean deleted = currentPage.readByte(pos) == 1;

          // Track max vector ID to compute nextId
          if (id > maxVectorId)
            maxVectorId = id;

          // Add/update in registry (LSM style: later entries override earlier ones)
          final VectorEntry entry = new VectorEntry(id, rid, vector);
          entry.deleted = deleted;
          vectorRegistry.put(id, entry);
          entriesRead++;
        }
      }

      // Compute nextId from the maximum vector ID found + 1
      nextId.set(maxVectorId + 1);

      LogManager.instance().log(this, Level.INFO,
          "Loaded " + vectorRegistry.size() + " unique vectors (" + entriesRead + " total entries) from " +
              totalPages + " pages for index: " + indexName + ", nextId=" + nextId.get());

      // Rebuild the graph index with loaded non-deleted vectors
      if (!vectorRegistry.isEmpty()) {
        initializeGraphIndex();
        graphIndexDirty.set(false);
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error loading vectors from pages", e);
      throw new IndexException("Error loading vectors from pages", e);
    }
  }

  public void markIndexDirty() {
    graphIndexDirty.set(true);
  }

  /**
   * Persist only changed vectors incrementally to pages in LSM style.
   * Pages grow from head (pointers) and tail (data), similar to LSMTreeIndexMutable.
   * This avoids rewriting the entire index on every commit.
   */
  private void persistVectorsDeltaIncremental(final List<Integer> changedVectorIds) {
    try {
      if (changedVectorIds.isEmpty())
        return;

      // Calculate entry size: id(4) + position(8) + bucketId(4) + vector(dimensions*4) + deleted(1)
      final int entrySize =
          Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + (metadata.dimensions
              * Binary.FLOAT_SERIALIZED_SIZE) + Binary.BYTE_SERIALIZED_SIZE;

      // Get or create the last mutable page (pages start from 0 now - no page0 metadata)
      int lastPageNum = getTotalPages() - 1;
      if (lastPageNum < 0) {
        lastPageNum = 0;
        createNewVectorDataPage(lastPageNum);
      }

      // Append changed vectors to pages
      for (final Integer vectorId : changedVectorIds) {
        final VectorEntry entry = vectorRegistry.get(vectorId);
        if (entry == null)
          continue;

        // Get current page
        MutablePage currentPage = getDatabase().getTransaction().getPageToModify(
            new PageId(getDatabase(), getFileId(), lastPageNum), getPageSize(), false);

        // Read page header
        int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

        // Calculate space needed
        final int headerSize = HEADER_BASE_SIZE + ((numberOfEntries + 1) * 4); // base header + pointers
        final int availableSpace = offsetFreeContent - headerSize;

        final TrackableBinary currentPageBuffer = currentPage.getTrackable();

        if (availableSpace < entrySize) {
          // Page is full, mark it as immutable before creating a new page
          currentPageBuffer.putByte(OFFSET_MUTABLE, (byte) 0); // mutable = 0 (page is no longer being written to)

          lastPageNum++;
          currentPage = createNewVectorDataPage(lastPageNum);
          offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
          numberOfEntries = 0;
        }

        // Write entry at tail (backwards from offsetFreeContent)
        final int entryOffset = offsetFreeContent - entrySize;

        currentPageBuffer.position(entryOffset);

        currentPageBuffer.putInt(entry.id);
        currentPageBuffer.putLong(entry.rid.getPosition());
        currentPageBuffer.putInt(entry.rid.getBucketId());
        for (int i = 0; i < metadata.dimensions; i++)
          currentPageBuffer.putFloat(entry.vector[i]);

        currentPageBuffer.putByte((byte) (entry.deleted ? 1 : 0));

        // Add pointer to entry in header (at HEADER_BASE_SIZE offset)
        currentPageBuffer.putInt(HEADER_BASE_SIZE + (numberOfEntries * 4), entryOffset);

        // Update page header
        numberOfEntries++;
        offsetFreeContent = entryOffset;
        currentPageBuffer.putInt(OFFSET_FREE_CONTENT, offsetFreeContent);
        currentPageBuffer.putInt(OFFSET_NUM_ENTRIES, numberOfEntries);
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error persisting vector delta", e);
      throw new IndexException("Error persisting vector delta", e);
    }
  }

  /**
   * Create a new vector data page with LSM-style header.
   * Page layout: [offsetFreeContent(4)][numberOfEntries(4)][mutable(1)][pointers...]...[entries from tail]
   */
  private MutablePage createNewVectorDataPage(final int pageNum) {
    final PageId pageId = new PageId(getDatabase(), getFileId(), pageNum);
    final MutablePage page = getDatabase().getTransaction().addPage(pageId, getPageSize());

    int pos = 0;
    page.writeInt(pos, page.getMaxContentSize()); // offsetFreeContent starts at end of page
    pos += TrackableBinary.INT_SERIALIZED_SIZE;

    page.writeInt(pos, 0);              // numberOfEntries = 0
    pos += TrackableBinary.INT_SERIALIZED_SIZE;

    page.writeInt(pos, (byte) 1);          // mutable = 1 (page is actively being written to)

    // Track mutable pages for compaction trigger
    currentMutablePages.incrementAndGet();

    return page;
  }

  /**
   * Search for k nearest neighbors to the given vector and return results with similarity scores.
   * This method is similar to HnswVectorIndex.findNeighborsFromVector and avoids the need to
   * recalculate distances after the search.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   *
   * @return List of pairs containing RID and similarity score
   */
  public List<com.arcadedb.utility.Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k) {
    if (queryVector == null)
      throw new IllegalArgumentException("Query vector cannot be null");

    if (queryVector.length != metadata.dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

    lock.readLock().lock();
    try {
      // Lazy rebuild: If graph index is dirty, rebuild it before searching
      if (graphIndexDirty.get()) {
        lock.readLock().unlock();
        lock.writeLock().lock();
        try {
          // Double-check after acquiring write lock
          if (graphIndexDirty.get()) {
            LogManager.instance().log(this, Level.INFO,
                "Graph index is dirty, rebuilding from " + vectorRegistry.size() + " vectors");
            initializeGraphIndex();
            graphIndexDirty.set(false);
          }
          // Downgrade to read lock
          lock.readLock().lock();
        } finally {
          lock.writeLock().unlock();
        }
      }

      if (graphIndex == null || vectorRegistry.isEmpty()) {
        return Collections.emptyList();
      }

      // Convert query vector to VectorFloat
      final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

      // Create RandomAccessVectorValues for scoring using the same ordinal mapping as the graph index
      final RandomAccessVectorValues vectors = new RandomAccessVectorValues() {
        @Override
        public int size() {
          return graphIndexOrdinalMapping != null ? graphIndexOrdinalMapping.size() : 0;
        }

        @Override
        public int dimension() {
          return metadata.dimensions;
        }

        @Override
        public VectorFloat<?> getVector(int i) {
          if (graphIndexOrdinalMapping == null || i < 0 || i >= graphIndexOrdinalMapping.size())
            return null;
          final VectorEntry entry = graphIndexOrdinalMapping.get(i);
          return entry != null ? vts.createFloatVector(entry.vector) : null;
        }

        @Override
        public boolean isValueShared() {
          return false;
        }

        @Override
        public RandomAccessVectorValues copy() {
          return this;
        }
      };

      // Perform search
      final SearchResult searchResult = GraphSearcher.search(
          queryVectorFloat,
          k,
          vectors,
          metadata.similarityFunction,
          graphIndex,
          Bits.ALL
      );

      // Extract RIDs and scores from search results using ordinal mapping
      final List<com.arcadedb.utility.Pair<RID, Float>> results = new ArrayList<>();
      for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
        final int ordinal = nodeScore.node;
        if (graphIndexOrdinalMapping != null && ordinal >= 0 && ordinal < graphIndexOrdinalMapping.size()) {
          final VectorEntry entry = graphIndexOrdinalMapping.get(ordinal);
          if (entry != null && !entry.deleted) {
            // JVector returns similarity scores - convert to distance based on similarity function
            final float score = nodeScore.score;
            final float distance = switch (metadata.similarityFunction) {
              case COSINE ->
                // For cosine, similarity is in [-1, 1], distance is 1 - similarity
                  1.0f - score;
              case EUCLIDEAN ->
                // For euclidean, the score is already the distance
                  score;
              case DOT_PRODUCT ->
                // For dot product, higher score is better (closer), so negate it
                  -score;
              default -> score;
            };
            results.add(new com.arcadedb.utility.Pair<>(entry.rid, distance));
          }
        }
      }

      LogManager.instance().log(this, Level.FINE, "Vector search returned " + results.size() + " results");
      return results;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error performing vector search", e);
      throw new IndexException("Error performing vector search", e);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (keys == null || keys.length == 0 || !(keys[0] instanceof float[] queryVector))
      throw new IllegalArgumentException("Expected float array as key for vector search");

    if (queryVector.length != metadata.dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

    final int k = limit > 0 ? limit : 10; // Default to top 10 results

    lock.readLock().lock();
    try {
      // Lazy rebuild: If graph index is dirty, rebuild it before searching
      if (graphIndexDirty.get()) {
        lock.readLock().unlock();
        lock.writeLock().lock();
        try {
          // Double-check after acquiring write lock
          if (graphIndexDirty.get()) {
            LogManager.instance().log(this, Level.INFO,
                "Graph index is dirty, rebuilding from " + vectorRegistry.size() + " vectors");
            initializeGraphIndex();
            graphIndexDirty.set(false);
          }
          // Downgrade to read lock
          lock.readLock().lock();
        } finally {
          lock.writeLock().unlock();
        }
      }

      if (graphIndex == null)
        return new IndexCursor() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public Identifiable next() {
            return null;
          }

          @Override
          public Identifiable getRecord() {
            return null;
          }

          @Override
          public Object[] getKeys() {
            return new Object[0];
          }

          @Override
          public byte[] getBinaryKeyTypes() {
            return new byte[0];
          }

          @Override
          public BinaryComparator getComparator() {
            return null;
          }

          @Override
          public long estimateSize() {
            return 0;
          }

          @Override
          public Iterator<Identifiable> iterator() {
            return Collections.emptyIterator();
          }
        };

      // Perform search using JVector 4.0 API
      final List<RID> resultRIDs = new ArrayList<>();

      if (vectorRegistry.isEmpty()) {
        LogManager.instance().log(this, Level.INFO, "No vectors in index, returning empty results");
      } else {
        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Create RandomAccessVectorValues for scoring using the same ordinal mapping as the graph index
        final RandomAccessVectorValues vectors = new RandomAccessVectorValues() {
          @Override
          public int size() {
            return graphIndexOrdinalMapping != null ? graphIndexOrdinalMapping.size() : 0;
          }

          @Override
          public int dimension() {
            return metadata.dimensions;
          }

          @Override
          public VectorFloat<?> getVector(int i) {
            if (graphIndexOrdinalMapping == null || i < 0 || i >= graphIndexOrdinalMapping.size())
              return null;
            final VectorEntry entry = graphIndexOrdinalMapping.get(i);
            return entry != null ? vts.createFloatVector(entry.vector) : null;
          }

          @Override
          public boolean isValueShared() {
            return false;
          }

          @Override
          public RandomAccessVectorValues copy() {
            return this;
          }
        };

        // Perform search
        final SearchResult searchResult = GraphSearcher.search(
            queryVectorFloat,
            k,
            vectors,
            metadata.similarityFunction,
            graphIndex,
            Bits.ALL
        );

        // Extract RIDs from search results using ordinal mapping
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (graphIndexOrdinalMapping != null && ordinal >= 0 && ordinal < graphIndexOrdinalMapping.size()) {
            final VectorEntry entry = graphIndexOrdinalMapping.get(ordinal);
            if (entry != null && !entry.deleted) {
              resultRIDs.add(entry.rid);
            }
          }
        }

        LogManager.instance().log(this, Level.FINE, "Vector search returned " + resultRIDs.size() + " results");
      }

      return new IndexCursor() {
        private int position = 0;

        @Override
        public boolean hasNext() {
          return position < resultRIDs.size();
        }

        @Override
        public Identifiable next() {
          if (!hasNext())
            return null;
          return resultRIDs.get(position++);
        }

        @Override
        public Identifiable getRecord() {
          if (position > 0 && position <= resultRIDs.size())
            return resultRIDs.get(position - 1);
          return null;
        }

        @Override
        public Object[] getKeys() {
          return new Object[] { queryVector };
        }

        @Override
        public byte[] getBinaryKeyTypes() {
          return new byte[0];
        }

        @Override
        public BinaryComparator getComparator() {
          return null;
        }

        @Override
        public long estimateSize() {
          return resultRIDs.size();
        }

        @Override
        public Iterator<Identifiable> iterator() {
          return (Iterator<Identifiable>) (Iterator<?>) resultRIDs.iterator();
        }
      };
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error performing vector search", e);
      throw new IndexException("Error performing vector search", e);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void put(final Object[] keys, final RID[] values) {

    if (keys == null || keys.length == 0)
      throw new IllegalArgumentException("Keys cannot be null or empty");

    if (values == null || values.length == 0)
      throw new IllegalArgumentException("Values cannot be null or empty");

    // Validate vector - can be either float[] or ComparableVector (from transaction replay)
    final float[] vector;
    if (keys[0] instanceof float[]) {
      vector = (float[]) keys[0];
    } else if (keys[0] instanceof ComparableVector) {
      vector = ((ComparableVector) keys[0]).vector;
    } else {
      throw new IllegalArgumentException(
          "Expected float array or ComparableVector as key for vector index, got " + keys[0].getClass());
    }

    if (vector.length != metadata.dimensions)
      throw new IllegalArgumentException(
          "Vector dimension " + vector.length + " does not match index dimension " + metadata.dimensions);

    final RID rid = values[0];
    final com.arcadedb.database.TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();

    if (txStatus == com.arcadedb.database.TransactionContext.STATUS.BEGUN) {
      // During BEGUN: Register with TransactionIndexContext for file locking and transaction tracking
      // Wrap vector in ComparableVector for TransactionIndexContext's TreeMap
      // TransactionIndexContext will replay this operation during commit, which will hit the else branch below
      getDatabase().getTransaction()
          .addIndexOperation(this, com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation.ADD,
              new Object[] { new ComparableVector(vector) }, rid);

    } else {
      // No transaction OR during commit replay: apply immediately
      // During commit phases, TransactionIndexContext.commit() calls this method directly
      lock.writeLock().lock();
      try {
        final int id = nextId.getAndIncrement();
        final VectorEntry vectorEntry = new VectorEntry(id, rid, vector);
        vectorRegistry.put(id, vectorEntry);

        // Persist incrementally
        persistVectorsDeltaIncremental(Collections.singletonList(id));

        // Mark graph as dirty - will rebuild on next query
        graphIndexDirty.set(true);
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  @Override
  public void remove(final Object[] keys) {
    // Not directly supported - use remove(keys, rid) instead
    throw new UnsupportedOperationException("Use remove(keys, rid) for vector index");
  }

  @Override
  public void remove(final Object[] keys, final Identifiable value) {
    final RID rid = value.getIdentity();
    final com.arcadedb.database.TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();

    if (txStatus == com.arcadedb.database.TransactionContext.STATUS.BEGUN) {
      // During BEGUN: Register with TransactionIndexContext for file locking and transaction tracking
      // Use a dummy ComparableVector since we don't have the vector value for removes
      // TransactionIndexContext will replay this operation during commit, which will hit the else branch below
      getDatabase().getTransaction()
          .addIndexOperation(this, com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
              new Object[] { new ComparableVector(new float[metadata.dimensions]) }, rid);

    } else {
      // No transaction OR during commit replay: apply immediately
      // During commit phases, TransactionIndexContext.commit() calls this method directly
      lock.writeLock().lock();
      try {
        final List<Integer> deletedIds = new ArrayList<>();
        for (final VectorEntry v : vectorRegistry.values()) {
          if (v.rid.equals(rid)) {
            v.deleted = true;
            deletedIds.add(v.id);
          }
        }

        // Persist incrementally
        persistVectorsDeltaIncremental(deletedIds);

        // Mark graph as dirty - will rebuild on next query
        graphIndexDirty.set(true);
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public void onAfterCommit() {
    // Check if compaction should be triggered after commit
    // Operations are applied immediately during TransactionIndexContext replay (not buffered here)
    if (minPagesToScheduleACompaction > 1 && currentMutablePages.get() >= minPagesToScheduleACompaction) {
      LogManager.instance()
          .log(this, Level.FINE, "Scheduled compaction of vector index '%s' (currentMutablePages=%d totalPages=%d)",
              null, getComponentName(), currentMutablePages.get(), getTotalPages());
      ((com.arcadedb.database.async.DatabaseAsyncExecutorImpl) getDatabase().async()).compact(this);
    }
  }

  public void onAfterRollback() {
    // Nothing to do - operations are not buffered locally
    // TransactionIndexContext handles rollback
  }

  @Override
  public long countEntries() {
    lock.writeLock().lock();
    try {
      // Check if we need to reload vectors from pages
      // This handles the case where pages were replicated but vectorRegistry wasn't updated
      final int totalPages = getTotalPages();

      if (totalPages > 0 && vectorRegistry.isEmpty()) {
        // Pages exist but registry is empty - reload from pages
        try {
          loadVectorsFromPages();
          initializeGraphIndex();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to reload vectors from pages: %s", e.getMessage());
        }
      }

      return vectorRegistry.values().stream().filter(v -> !v.deleted).count();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getName() {
    return indexName;
  }

  @Override
  public String getTypeName() {
    return metadata.typeName;
  }

  @Override
  public List<String> getPropertyNames() {
    return metadata.propertyNames;
  }

  @Override
  public List<Integer> getFileIds() {
    return Collections.singletonList(mutable.getFileId());
  }

  @Override
  public int getPageSize() {
    return mutable.getPageSize();
  }

  public int getTotalPages() {
    return mutable.getTotalPages();
  }

  public int getFileId() {
    return mutable.getFileId();
  }

  public DatabaseInternal getDatabase() {
    return mutable.getDatabase();
  }

  public String getComponentName() {
    return mutable.getName();
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  public void setAssociatedIndex(final IndexInternal index) {
    // Not applicable for this index type
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    // Not applicable for this index type
  }

  @Override
  public TypeIndex getTypeIndex() {
    return null;
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    // Type name is immutable for vector indexes
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return new byte[0]; // Vector indexes use float arrays, not binary key types
  }

  @Override
  public String getMostRecentFileName() {
    return indexName;
  }

  @Override
  public boolean scheduleCompaction() {
    checkIsValid();
    if (getDatabase().getPageManager().isPageFlushingSuspended(getDatabase()))
      return false;
    return status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.COMPACTION_SCHEDULED);
  }

  @Override
  public boolean isCompacting() {
    return status.get() == INDEX_STATUS.COMPACTION_IN_PROGRESS;
  }

  @Override
  public boolean isValid() {
    return true; // Index is always valid unless explicitly dropped
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.LSM_VECTOR;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    // Not applicable for vector index
  }

  @Override
  public int getAssociatedBucketId() {
    if (metadata.associatedBucketId == -1)
      LogManager.instance().log(this, Level.WARNING, "getAssociatedBucketId() returning -1, metadata not set!");
    return metadata.associatedBucketId;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {

    checkIsValid();
    final DatabaseInternal database = getDatabase();

    if (database.getMode() == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + getName() + "'");

    if (database.getPageManager().isPageFlushingSuspended(database))
      // POSTPONE COMPACTING (DATABASE BACKUP IN PROGRESS?)
      return false;

    if (!status.compareAndSet(INDEX_STATUS.COMPACTION_SCHEDULED, INDEX_STATUS.COMPACTION_IN_PROGRESS))
      // COMPACTION NOT SCHEDULED
      return false;

    try {
      return LSMVectorIndexCompactor.compact(this);
    } catch (final TimeoutException e) {
      // IGNORE IT, WILL RETRY LATER
      return false;
    } finally {
      status.set(INDEX_STATUS.AVAILABLE);
    }
  }

  @Override
  public JSONObject toJSON() {
    // Store complete vector index metadata in schema JSON for replication.
    // This single source of truth is used both for schema persistence and distributed replication.
    final JSONObject json = new JSONObject();

    // Add required fields for schema loading (matching LSMTreeIndex pattern)
    json.put("type", getType());
    json.put("bucket", getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());

    // Add vector-specific metadata
    json.put("indexName", indexName);
    json.put("typeName", metadata.typeName);
    json.put("properties", metadata.propertyNames);
    json.put("dimensions", metadata.dimensions);
    json.put("similarityFunction", metadata.similarityFunction.name());
    json.put("maxConnections", metadata.maxConnections);
    json.put("beamWidth", metadata.beamWidth);
    json.put("idPropertyName", metadata.idPropertyName);
    json.put("version", CURRENT_VERSION);
    return json;
  }

  /**
   * Applies metadata from the schema JSON to this vector index.
   * Called by LocalSchema.load() after the index is created to ensure metadata
   * from the central schema overrides any defaults or file-based values.
   * Particularly important during replication when metadata comes from the
   * replicated schema JSON rather than separate .metadata.json files.
   *
   * @param indexJSON The complete index JSON from the schema containing all configuration
   */
  @Override
  public void setMetadata(final JSONObject indexJSON) {
    if (indexJSON == null)
      return;

    final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy =
        LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(
            indexJSON.getString("nullStrategy", LSMTreeIndexAbstract.NULL_STRATEGY.ERROR.name())
        );

    setNullStrategy(nullStrategy);

    if (indexJSON.has("typeName"))
      this.metadata.typeName = indexJSON.getString("typeName");
    if (indexJSON.has("properties")) {
      final var jsonArray = indexJSON.getJSONArray("properties");
      this.metadata.propertyNames = new ArrayList<>();
      for (int i = 0; i < jsonArray.length(); i++)
        metadata.propertyNames.add(jsonArray.getString(i));
    }

    metadata.fromJSON(indexJSON);

    LogManager.instance().log(this, Level.FINE,
        "Applied metadata from schema to vector index: %s (dimensions=%d)", indexName, this.metadata.dimensions);
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      // NOTE: Metadata is now embedded in the schema JSON via toJSON() and is automatically
      // replicated with the schema. We don't write a separate .metadata.json file anymore
      // to avoid path transformation issues during replication.

      mutable.close();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void drop() {
    lock.writeLock().lock();
    try {
      // Clear all vectors and transaction contexts
      vectorRegistry.clear();

      // Delete index files
      final File indexFile = new File(mutable.getFilePath());
      if (indexFile.exists())
        indexFile.delete();

      // NOTE: Metadata is now embedded in the schema JSON via toJSON() and is automatically
      // deleted when the schema is updated. We no longer need to delete separate .metadata.json files.

      // Close the component
      close();
    } finally {
      lock.writeLock().unlock();
      valid = false;
    }
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("totalVectors", (long) vectorRegistry.size());
    stats.put("dimensions", (long) metadata.dimensions);
    stats.put("maxConnections", (long) metadata.maxConnections);
    stats.put("beamWidth", (long) metadata.beamWidth);
    return stats;
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    for (final INDEX_STATUS expectedStatus : expectedStatuses)
      if (this.status.compareAndSet(expectedStatus, newStatus))
        return true;
    return false;
  }

  @Override
  public LSMVectorIndexMetadata getMetadata() {
    return metadata;
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    checkIsValid();
    this.metadata = (LSMVectorIndexMetadata) metadata;
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    lock.writeLock().lock();
    try {
      if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {
        try {
          final AtomicInteger total = new AtomicInteger();
          final long LOG_INTERVAL = 10000; // Log every 10K records
          final long startTime = System.currentTimeMillis();

          if (metadata.propertyNames == null || metadata.propertyNames.isEmpty())
            throw new IndexException("Cannot rebuild vector index '" + indexName + "' because property names are missing");

          LogManager.instance().log(this, Level.INFO, "Building vector index '%s' on %d properties...", indexName,
              metadata.propertyNames.size());

          final DatabaseInternal db = getDatabase();

          // Check if we need to start a transaction
          final boolean startedTransaction =
              db.getTransaction().getStatus() != com.arcadedb.database.TransactionContext.STATUS.BEGUN;
          if (startedTransaction)
            db.getWrappedDatabaseInstance().begin();

          try {
            // Scan the bucket and index all documents
            db.scanBucket(db.getSchema().getBucketById(metadata.associatedBucketId).getName(), record -> {
              db.getIndexer().addToIndex(LSMVectorIndex.this, record.getIdentity(), (Document) record);
              total.incrementAndGet();

              // Periodic progress logging
              if (total.get() % LOG_INTERVAL == 0) {
                final long elapsed = System.currentTimeMillis() - startTime;
                final double rate = total.get() / (elapsed / 1000.0);
                LogManager.instance()
                    .log(this, Level.INFO, "Building vector index '%s': processed %d records (%.0f records/sec)...",
                        indexName, total.get(), rate);
              }

              if (total.get() % buildIndexBatchSize == 0) {
                // Commit in batches
                db.getWrappedDatabaseInstance().commit();
                db.getWrappedDatabaseInstance().begin();
              }

              if (callback != null)
                callback.onDocumentIndexed((Document) record, total.get());

              return true;
            });

            // Final commit if we started a transaction
            if (startedTransaction)
              db.getWrappedDatabaseInstance().commit();

            // Completion logging
            final long elapsed = System.currentTimeMillis() - startTime;
            LogManager.instance().log(this, Level.INFO, "Completed building vector index '%s': processed %d records in %dms",
                indexName, total.get(), elapsed);

            return total.get();
          } catch (final Exception e) {
            // Rollback if we started a transaction
            if (startedTransaction && db.getTransaction().getStatus() == com.arcadedb.database.TransactionContext.STATUS.BEGUN)
              db.getWrappedDatabaseInstance().rollback();
            throw e;
          }

        } finally {
          status.set(INDEX_STATUS.AVAILABLE);
        }
      } else
        throw new NeedRetryException("Error building vector index '" + indexName + "' because it is not available");
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public PaginatedComponent getComponent() {
    return mutable;
  }

  @Override
  public Type[] getKeyTypes() {
    return new Type[] { Type.ARRAY_OF_FLOATS };
  }

  public int getDimensions() {
    return metadata.dimensions;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return metadata.similarityFunction;
  }

  public int getMaxConnections() {
    return metadata.maxConnections;
  }

  public int getBeamWidth() {
    return metadata.beamWidth;
  }

  public String getIdPropertyName() {
    return metadata.idPropertyName;
  }

  /**
   * Gets the compacted sub-index, if any.
   */
  public LSMVectorIndexCompacted getSubIndex() {
    return compactedSubIndex;
  }

  /**
   * Sets the compacted sub-index.
   */
  public void setSubIndex(final LSMVectorIndexCompacted subIndex) {
    this.compactedSubIndex = subIndex;
  }

  /**
   * Gets the current number of mutable pages.
   */
  public int getCurrentMutablePages() {
    return currentMutablePages.get();
  }

  /**
   * Atomically replaces this index with a new one that has the compacted sub-index.
   * Copies remaining mutable pages from startingFromPage onwards to the new index.
   *
   * @param startingFromPage The first page to copy from current index
   * @param compactedIndex   The compacted sub-index to attach
   *
   * @return The new index file ID
   */
  protected LSMVectorIndexMutable splitIndex(final int startingFromPage, final LSMVectorIndexCompacted compactedIndex)
      throws IOException, InterruptedException {

    final DatabaseInternal database = getDatabase();
    if (database.isTransactionActive())
      throw new IllegalStateException("Cannot replace compacted index because a transaction is active");

    final int fileId = getFileId();
    final LockManager.LOCK_STATUS locked =
        getDatabase().getTransactionManager().tryLockFile(fileId, 0, Thread.currentThread());

    if (locked == LockManager.LOCK_STATUS.NO)
      throw new IllegalStateException("Cannot replace compacted index because cannot lock index file " + fileId);

    final AtomicInteger lockedNewFileId = new AtomicInteger(-1);

    try {
      lock.writeLock().lock();
      try {
        // Create new index file with compacted sub-index
        final int last_ = getComponentName().lastIndexOf('_');
        final String newName = getComponentName().substring(0, last_) + "_" + System.nanoTime();

        final LSMVectorIndexMutable newMutableIndex = new LSMVectorIndexMutable(database, newName,
            database.getDatabasePath() + File.separator + newName, mutable.getDatabase().getMode(), mutable.getPageSize(),
            PaginatedComponent.TEMP_EXT + LSMVectorIndexMutable.FILE_EXT);

        database.getSchema().getEmbedded().registerFile(newMutableIndex);

        // LOCK NEW FILE
        database.getTransactionManager().tryLockFile(newMutableIndex.getFileId(), 0, Thread.currentThread());
        lockedNewFileId.set(newMutableIndex.getFileId());

        final List<MutablePage> modifiedPages = new ArrayList<>();

        // Copy remaining mutable pages from old index to new index
        final int pagesToCopy = getTotalPages() - startingFromPage;
        for (int i = 0; i < pagesToCopy; i++) {
          final BasePage currentPage = getDatabase().getTransaction()
              .getPage(new PageId(getDatabase(), fileId, i + startingFromPage), getPageSize());

          // Copy the entire page content
          final MutablePage newPage =
              new MutablePage(new PageId(getDatabase(), newMutableIndex.getFileId(), i + 1), getPageSize());

          final ByteBuffer oldContent = currentPage.getContent();
          oldContent.rewind();
          newPage.getContent().put(oldContent);

          modifiedPages.add(getDatabase().getPageManager().updatePageVersion(newPage, true));
        }

        // Write all pages
        if (!modifiedPages.isEmpty())
          getDatabase().getPageManager().writePages(modifiedPages, false);

        // SWAP OLD WITH NEW INDEX IN EXCLUSIVE LOCK (NO READ/WRITE ARE POSSIBLE IN THE MEANTIME)
        newMutableIndex.removeTempSuffix();

        mutable = newMutableIndex;

        // Update schema with file migration
        ((LocalSchema) getDatabase().getSchema()).setMigratedFileId(fileId, newMutableIndex.getFileId());

        getDatabase().getSchema().getEmbedded().saveConfiguration();
        return newMutableIndex;

      } finally {
        lock.writeLock().unlock();
      }

    } finally {
      final int lockedFile = lockedNewFileId.get();
      if (lockedFile != -1)
        getDatabase().getTransactionManager().unlockFile(lockedFile, Thread.currentThread());

      if (locked == LockManager.LOCK_STATUS.YES)
        getDatabase().getTransactionManager().unlockFile(fileId, Thread.currentThread());
    }
  }

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Index '" + indexName + "' is not valid. Probably has been drop or rebuilt");
  }
}
