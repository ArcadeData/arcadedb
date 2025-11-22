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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.LSMVectorIndexBuilder;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
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
public class LSMVectorIndex extends PaginatedComponent implements com.arcadedb.index.Index, IndexInternal {
  public static final  String            FILE_EXT        = "lsmvecidx";
  public static final  int               CURRENT_VERSION = 0;
  public static final  int               DEF_PAGE_SIZE   = 262_144;
  private static final VectorTypeSupport vts             = VectorizationProvider.getInstance().getVectorTypeSupport();

  // Page header layout constants
  public static final int OFFSET_FREE_CONTENT = 0;  // 4 bytes
  public static final int OFFSET_NUM_ENTRIES = 4;   // 4 bytes
  public static final int OFFSET_MUTABLE = 8;       // 1 byte
  public static final int HEADER_BASE_SIZE = 9;     // offsetFreeContent(4) + numberOfEntries(4) + mutable(1)

  private final int                      dimensions;
  private final VectorSimilarityFunction similarityFunction;
  private final int                      maxConnections;
  private final int                      beamWidth;
  private final String                   indexName;
  private final String                   typeName;
  private final String[]                 propertyNames;
  private final ReentrantReadWriteLock   lock;
  private       int                      associatedBucketId;

  // Transaction support: pending operations are buffered per transaction
  private final ConcurrentHashMap<Long, TransactionVectorContext> transactionContexts;

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
    public IndexInternal create(final IndexBuilder builder) {
      if (!(builder instanceof LSMVectorIndexBuilder))
        throw new IndexException("Expected LSMVectorIndexBuilder but received " + builder);

      try {
        return new LSMVectorIndex((LSMVectorIndexBuilder) builder);
      } catch (final IOException e) {
        throw new IndexException("Error creating LSM vector index", e);
      }
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new LSMVectorIndex(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /**
   * Constructor for creating a new index
   */
  protected LSMVectorIndex(final LSMVectorIndexBuilder builder) throws IOException {
    super(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(), FILE_EXT, ComponentFile.MODE.READ_WRITE,
        DEF_PAGE_SIZE, CURRENT_VERSION);

    this.indexName = builder.getIndexName();
    this.typeName = builder.getTypeName();
    this.propertyNames = builder.getPropertyNames();
    this.dimensions = builder.getDimensions();
    this.similarityFunction = builder.getSimilarityFunction();
    this.maxConnections = builder.getMaxConnections();
    this.beamWidth = builder.getBeamWidth();

    this.lock = new ReentrantReadWriteLock();
    this.transactionContexts = new ConcurrentHashMap<>();
    this.vectorRegistry = new ConcurrentHashMap<>();
    this.nextId = new AtomicInteger(0);
    this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);
    this.graphIndexDirty = new AtomicBoolean(false);
    this.associatedBucketId = -1; // Will be set via setMetadata()

    // Initialize compaction fields
    this.currentMutablePages = new AtomicInteger(1); // Start with page 0
    this.minPagesToScheduleACompaction = builder.getDatabase().getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    this.compactedSubIndex = null;

    initializeGraphIndex();
  }

  /**
   * Constructor for loading an existing index
   */
  protected LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);

    this.lock = new ReentrantReadWriteLock();
    this.transactionContexts = new ConcurrentHashMap<>();
    this.vectorRegistry = new ConcurrentHashMap<>();
    this.nextId = new AtomicInteger(0);
    this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);
    this.graphIndexDirty = new AtomicBoolean(false);
    this.associatedBucketId = -1; // Will be set via setMetadata()

    // Initialize compaction fields
    this.currentMutablePages = new AtomicInteger(getTotalPages());
    this.minPagesToScheduleACompaction = database.getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    this.compactedSubIndex = null;

    // Load configuration from JSON metadata file
    final String metadataPath = filePath.replace("." + FILE_EXT, ".metadata.json");
    final File metadataFile = new File(metadataPath);

    if (!metadataFile.exists())
      throw new IndexException("Metadata file not found for index: " + metadataPath);

    final String fileContent = FileUtils.readFileAsString(metadataFile);
    final JSONObject json = new JSONObject(fileContent);

    this.indexName = json.getString("indexName", name);
    this.typeName = json.getString("typeName", "");
    this.dimensions = json.getInt("dimensions");
    this.similarityFunction = VectorSimilarityFunction.valueOf(json.getString("similarityFunction", "COSINE"));
    this.maxConnections = json.getInt("maxConnections", 16);
    this.beamWidth = json.getInt("beamWidth", 100);

    // Load property names
    final List<String> propList = new ArrayList<>();
    if (json.has("propertyNames")) {
      final var jsonArray = json.getJSONArray("propertyNames");
      for (int i = 0; i < jsonArray.length(); i++)
        propList.add(jsonArray.getString(i));
    }
    this.propertyNames = propList.toArray(new String[0]);

    // Load vectors from pages
    loadVectorsFromPages();

    // Rebuild graph index
    initializeGraphIndex();
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
          return dimensions;
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
        final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(vectors, similarityFunction);

        // Build the graph index
        try (final GraphIndexBuilder builder = new GraphIndexBuilder(
            scoreProvider,
            dimensions,
            maxConnections,  // M parameter (graph degree)
            beamWidth,       // efConstruction (construction search depth)
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
      // Read header from page 0
      final BasePage page0 = database.getTransaction().getPage(new PageId(database, getFileId(), 0), getPageSize());
      final ByteBuffer buffer0 = page0.getContent();
      buffer0.position(0);

      final int storedNextId = buffer0.getInt();
      if (storedNextId == 0) {
        LogManager.instance().log(this, Level.FINE, "No vectors to load - empty index: " + indexName);
        return;
      }

      // Read and validate metadata
      final int storedDimensions = buffer0.getInt();
      if (storedDimensions != dimensions) {
        throw new IndexException("Dimension mismatch: expected " + dimensions + " but found " + storedDimensions);
      }

      // Skip similarity, maxConnections, beamWidth - already set from constructor
      buffer0.getInt();
      buffer0.getInt();
      buffer0.getInt();

      nextId.set(storedNextId);

      // Read all data pages (1 onwards) in LSM style
      final int totalPages = getTotalPages();
      int entriesRead = 0;

      for (int pageNum = 1; pageNum < totalPages; pageNum++) {
        final BasePage currentPage = database.getTransaction().getPage(
            new PageId(database, getFileId(), pageNum), getPageSize());
        final ByteBuffer pageBuffer = currentPage.getContent();
        pageBuffer.position(0);

        // Read page header
        final int offsetFreeContent = pageBuffer.getInt(OFFSET_FREE_CONTENT);
        final int numberOfEntries = pageBuffer.getInt(OFFSET_NUM_ENTRIES);
        final byte mutable = pageBuffer.get(OFFSET_MUTABLE); // Read mutable flag (but don't use it during loading)

        if (numberOfEntries == 0)
          continue; // Empty page

        // Read pointer table (starts at HEADER_BASE_SIZE offset)
        final int[] pointers = new int[numberOfEntries];
        for (int i = 0; i < numberOfEntries; i++) {
          pointers[i] = pageBuffer.getInt(HEADER_BASE_SIZE + (i * 4));
        }

        // Read entries using pointers
        for (int i = 0; i < numberOfEntries; i++) {
          pageBuffer.position(pointers[i]);

          final int id = pageBuffer.getInt();
          final long position = pageBuffer.getLong();
          final int bucketId = pageBuffer.getInt();
          final RID rid = new RID(database, bucketId, position);

          final float[] vector = new float[dimensions];
          for (int j = 0; j < dimensions; j++) {
            vector[j] = pageBuffer.getFloat();
          }

          final boolean deleted = pageBuffer.get() == 1;

          // Add/update in registry (LSM style: later entries override earlier ones)
          final VectorEntry entry = new VectorEntry(id, rid, vector);
          entry.deleted = deleted;
          vectorRegistry.put(id, entry);
          entriesRead++;
        }
      }

      LogManager.instance().log(this, Level.INFO,
          "Loaded " + vectorRegistry.size() + " unique vectors (" + entriesRead + " total entries) from " +
              (totalPages - 1) + " pages for index: " + indexName);

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

  /**
   * Persist only changed vectors incrementally to pages in LSM style.
   * Pages grow from head (pointers) and tail (data), similar to LSMTreeIndexMutable.
   * This avoids rewriting the entire index on every commit.
   */
  private void persistVectorsDeltaIncremental(final List<Integer> changedVectorIds) {
    try {
      // Update metadata in page 0
      final BasePage page0 = database.getTransaction().getPageToModify(
          new PageId(database, getFileId(), 0), getPageSize(), false);
      final ByteBuffer buffer0 = page0.getContent();
      buffer0.position(0);
      buffer0.putInt(nextId.get()); // Update next ID
      buffer0.putInt(dimensions);
      buffer0.putInt(similarityFunction.ordinal());
      buffer0.putInt(maxConnections);
      buffer0.putInt(beamWidth);

      if (changedVectorIds.isEmpty())
        return;

      // Calculate entry size: id(4) + position(8) + bucketId(4) + vector(dimensions*4) + deleted(1)
      final int entrySize = 4 + 8 + 4 + (dimensions * 4) + 1;

      // Get or create the last mutable page
      int lastPageNum = getTotalPages() - 1;
      if (lastPageNum < 1) {
        lastPageNum = 1;
        createNewVectorDataPage(lastPageNum);
      }

      // Append changed vectors to pages
      for (final Integer vectorId : changedVectorIds) {
        final VectorEntry entry = vectorRegistry.get(vectorId);
        if (entry == null)
          continue;

        // Get current page
        BasePage currentPage = database.getTransaction().getPageToModify(
            new PageId(database, getFileId(), lastPageNum), getPageSize(), false);
        ByteBuffer pageBuffer = currentPage.getContent();

        // Read page header
        int offsetFreeContent = pageBuffer.getInt(OFFSET_FREE_CONTENT);
        int numberOfEntries = pageBuffer.getInt(OFFSET_NUM_ENTRIES);

        // Calculate space needed
        final int headerSize = HEADER_BASE_SIZE + ((numberOfEntries + 1) * 4); // base header + pointers
        final int availableSpace = offsetFreeContent - headerSize;

        if (availableSpace < entrySize) {
          // Page is full, mark it as immutable before creating a new page
          pageBuffer.put(OFFSET_MUTABLE, (byte) 0); // mutable = 0 (page is no longer being written to)
          
          lastPageNum++;
          currentPage = createNewVectorDataPage(lastPageNum);
          pageBuffer = currentPage.getContent();
          offsetFreeContent = pageBuffer.getInt(OFFSET_FREE_CONTENT);
          numberOfEntries = 0;
        }

        // Write entry at tail (backwards from offsetFreeContent)
        final int entryOffset = offsetFreeContent - entrySize;
        pageBuffer.position(entryOffset);

        pageBuffer.putInt(entry.id);
        pageBuffer.putLong(entry.rid.getPosition());
        pageBuffer.putInt(entry.rid.getBucketId());
        for (int i = 0; i < dimensions; i++) {
          pageBuffer.putFloat(entry.vector[i]);
        }
        pageBuffer.put((byte) (entry.deleted ? 1 : 0));

        // Add pointer to entry in header (at HEADER_BASE_SIZE offset)
        pageBuffer.putInt(HEADER_BASE_SIZE + (numberOfEntries * 4), entryOffset);

        // Update page header
        numberOfEntries++;
        offsetFreeContent = entryOffset;
        pageBuffer.putInt(OFFSET_FREE_CONTENT, offsetFreeContent);
        pageBuffer.putInt(OFFSET_NUM_ENTRIES, numberOfEntries);
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
  private BasePage createNewVectorDataPage(final int pageNum) {
    final PageId pageId = new PageId(database, getFileId(), pageNum);
    final BasePage page = database.getTransaction().addPage(pageId, getPageSize());
    final ByteBuffer buffer = page.getContent();

    buffer.position(0);
    buffer.putInt(getPageSize()); // offsetFreeContent starts at end of page
    buffer.putInt(0);              // numberOfEntries = 0
    buffer.put((byte) 1);          // mutable = 1 (page is actively being written to)

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
   * @param k The number of neighbors to return
   * @return List of pairs containing RID and similarity score
   */
  public List<com.arcadedb.utility.Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k) {
    if (queryVector == null)
      throw new IllegalArgumentException("Query vector cannot be null");

    if (queryVector.length != dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + dimensions);

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
          return dimensions;
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
          similarityFunction,
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
            final float distance;
            switch (similarityFunction) {
              case COSINE:
                // For cosine, similarity is in [-1, 1], distance is 1 - similarity
                distance = 1.0f - score;
                break;
              case EUCLIDEAN:
                // For euclidean, the score is already the distance
                distance = score;
                break;
              case DOT_PRODUCT:
                // For dot product, higher score is better (closer), so negate it
                distance = -score;
                break;
              default:
                distance = score;
            }
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

    if (queryVector.length != dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + dimensions);

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
            return dimensions;
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
            similarityFunction,
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

    if (vector.length != dimensions)
      throw new IllegalArgumentException("Vector dimension " + vector.length + " does not match index dimension " + dimensions);

    final RID rid = values[0];
    final com.arcadedb.database.TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();

    if (txStatus == com.arcadedb.database.TransactionContext.STATUS.BEGUN) {
      // During BEGUN: Register with TransactionIndexContext for file locking and transaction tracking
      // Wrap vector in ComparableVector for TransactionIndexContext's TreeMap
      getDatabase().getTransaction()
          .addIndexOperation(this, com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation.ADD,
              new Object[] { new ComparableVector(vector) }, rid);

      // Buffer the operation locally for actual processing in onAfterCommit()
      final long txId = Thread.currentThread().getId();
      final TransactionVectorContext context = transactionContexts.computeIfAbsent(txId, k -> new TransactionVectorContext());
      context.operations.put(rid, new TransactionVectorContext.VectorOperation(
          TransactionVectorContext.OperationType.ADD, vector));

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
      getDatabase().getTransaction()
          .addIndexOperation(this, com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
              new Object[] { new ComparableVector(new float[dimensions]) }, rid);

      // Buffer the operation locally (used only for cleanup tracking)
      final long txId = Thread.currentThread().getId();
      final TransactionVectorContext context = transactionContexts.computeIfAbsent(txId, k -> new TransactionVectorContext());
      context.operations.put(rid, new TransactionVectorContext.VectorOperation(
          TransactionVectorContext.OperationType.REMOVE, null));

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

  @Override
  public void onAfterCommit() {
    // Operations are already handled by TransactionIndexContext.commit()
    // which calls put()/remove() during commit phases
    // So we just need to clean up our local context and trigger compaction if needed
    final long txId = Thread.currentThread().getId();
    final TransactionVectorContext context = transactionContexts.remove(txId);
    if (context == null || context.operations.isEmpty())
      return;

    // Check if compaction should be triggered
    if (minPagesToScheduleACompaction > 1 && currentMutablePages.get() >= minPagesToScheduleACompaction) {
      LogManager.instance()
          .log(this, Level.FINE, "Scheduled compaction of vector index '%s' (currentMutablePages=%d totalPages=%d)",
              null, componentName, currentMutablePages.get(), getTotalPages());
      ((com.arcadedb.database.async.DatabaseAsyncExecutorImpl) database.async()).compact(this);
    }
  }

  public void onAfterRollback() {
    // Discard the transaction context
    final long txId = Thread.currentThread().getId();
    transactionContexts.remove(txId);
  }

  @Override
  public long countEntries() {
    lock.readLock().lock();
    try {
      return vectorRegistry.values().stream().filter(v -> !v.deleted).count();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getName() {
    return indexName;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public List<String> getPropertyNames() {
    return Arrays.asList(propertyNames);
  }

  @Override
  public List<Integer> getFileIds() {
    return Collections.singletonList(getFileId());
  }

  @Override
  public int getPageSize() {
    return DEF_PAGE_SIZE;
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
    return filePath;
  }

  @Override
  public boolean scheduleCompaction() {
    return status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.COMPACTION_SCHEDULED);
  }

  @Override
  public boolean isCompacting() {
    final INDEX_STATUS currentStatus = status.get();
    return currentStatus == INDEX_STATUS.COMPACTION_SCHEDULED || currentStatus == INDEX_STATUS.COMPACTION_IN_PROGRESS;
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
    if (associatedBucketId == -1)
      LogManager.instance().log(this, Level.WARNING, "getAssociatedBucketId() returning -1, metadata not set!");
    return associatedBucketId;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return LSMVectorIndexCompactor.compact(this);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("indexName", indexName);
    json.put("typeName", typeName);
    json.put("propertyNames", propertyNames);
    json.put("dimensions", dimensions);
    json.put("similarityFunction", similarityFunction.name());
    json.put("maxConnections", maxConnections);
    json.put("beamWidth", beamWidth);
    json.put("version", CURRENT_VERSION);
    return json;
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      // Save metadata
      final String metadataPath = filePath.replace("." + FILE_EXT, ".metadata.json");
      try (final FileWriter writer = new FileWriter(metadataPath)) {
        writer.write(toJSON().toString(2));
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error saving metadata for index: " + indexName, e);
      }

      super.close();
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
      transactionContexts.clear();

      // Delete index files
      final File indexFile = new File(filePath);
      if (indexFile.exists())
        indexFile.delete();

      final File metadataFile = new File(filePath.replace("." + FILE_EXT, ".metadata.json"));
      if (metadataFile.exists())
        metadataFile.delete();

      // Close the component
      close();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("totalVectors", (long) vectorRegistry.size());
    stats.put("dimensions", (long) dimensions);
    stats.put("maxConnections", (long) maxConnections);
    stats.put("beamWidth", (long) beamWidth);
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
  public void setMetadata(final String typeName, final String[] propertyNames, final int associatedBucketId) {
    // Store the associated bucket ID - needed for transaction file locking
    this.associatedBucketId = associatedBucketId;
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    // Vector index is built incrementally as documents are added
    // Return the current number of vectors in the index
    return vectorRegistry.size();
  }

  @Override
  public PaginatedComponent getComponent() {
    return this;
  }

  @Override
  public Type[] getKeyTypes() {
    return new Type[] { Type.ARRAY_OF_FLOATS };
  }

  public int getDimensions() {
    return dimensions;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getBeamWidth() {
    return beamWidth;
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
  protected int splitIndex(final int startingFromPage, final LSMVectorIndexCompacted compactedIndex)
      throws IOException, InterruptedException {

    if (database.isTransactionActive())
      throw new IllegalStateException("Cannot replace compacted index because a transaction is active");

    final int fileId = getFileId();
    final com.arcadedb.utility.LockManager.LOCK_STATUS locked =
        database.getTransactionManager().tryLockFile(fileId, 0, Thread.currentThread());

    if (locked == com.arcadedb.utility.LockManager.LOCK_STATUS.NO)
      throw new IllegalStateException("Cannot replace compacted index because cannot lock index file " + fileId);

    final AtomicInteger lockedNewFileId = new AtomicInteger(-1);

    try {
      lock.writeLock().lock();
      try {
        // Create new index file with compacted sub-index
        final String newName = componentName + "_" + System.nanoTime();
        final String newFilePath = database.getDatabasePath() + File.separator + newName;

        // Build metadata for new index
        final LSMVectorIndexBuilder builder = new LSMVectorIndexBuilder(database, typeName, propertyNames)
            .withIndexName(indexName)
            .withDimensions(dimensions)
            .withSimilarity(similarityFunction.name())
            .withMaxConnections(maxConnections)
            .withBeamWidth(beamWidth);

        // Create the new index with same configuration
        final LSMVectorIndex newIndex = new LSMVectorIndex(builder);
        newIndex.setSubIndex(compactedIndex);
        database.getSchema().getEmbedded().registerFile(newIndex);

        // Lock new file
        database.getTransactionManager().tryLockFile(newIndex.getFileId(), 0, Thread.currentThread());
        lockedNewFileId.set(newIndex.getFileId());

        final List<com.arcadedb.engine.MutablePage> modifiedPages = new ArrayList<>();

        // Copy remaining mutable pages from old index to new index
        final int pagesToCopy = getTotalPages() - startingFromPage;
        for (int i = 0; i < pagesToCopy; i++) {
          final BasePage currentPage = database.getTransaction()
              .getPage(new PageId(database, fileId, i + startingFromPage), pageSize);

          // Copy the entire page content
          final com.arcadedb.engine.MutablePage newPage =
              new com.arcadedb.engine.MutablePage(new PageId(database, newIndex.getFileId(), i + 1), pageSize);

          final ByteBuffer oldContent = currentPage.getContent();
          oldContent.rewind();
          newPage.getContent().put(oldContent);

          modifiedPages.add(database.getPageManager().updatePageVersion(newPage, true));
        }

        // Write all pages
        if (!modifiedPages.isEmpty()) {
          database.getPageManager().writePages(modifiedPages, false);
        }

        // Update schema with file migration
        ((com.arcadedb.schema.LocalSchema) database.getSchema()).setMigratedFileId(fileId, newIndex.getFileId());
        database.getSchema().getEmbedded().saveConfiguration();

        LogManager.instance().log(this, Level.INFO,
            "Successfully split vector index '%s': old fileId=%d, new fileId=%d, pages copied=%d",
            null, componentName, fileId, newIndex.getFileId(), pagesToCopy);

        return newIndex.getFileId();

      } finally {
        lock.writeLock().unlock();
      }

    } finally {
      final int lockedFile = lockedNewFileId.get();
      if (lockedFile != -1)
        database.getTransactionManager().unlockFile(lockedFile, Thread.currentThread());

      if (locked == com.arcadedb.utility.LockManager.LOCK_STATUS.YES)
        database.getTransactionManager().unlockFile(fileId, Thread.currentThread());
    }
  }
}
