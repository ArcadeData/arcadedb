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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
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
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
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
  public static final String FILE_EXT        = "lsmvectoridx";
  public static final int    CURRENT_VERSION = 0;
  public static final int    DEF_PAGE_SIZE   = 262_144;
  private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

  private final int                                     dimensions;
  private final VectorSimilarityFunction                similarityFunction;
  private final int                                     maxConnections;
  private final int                                     beamWidth;
  private final String                                  indexName;
  private final String                                  typeName;
  private final String[]                                propertyNames;
  private final ReentrantReadWriteLock                  lock;
  private int                                           associatedBucketId;

  // Transaction support: pending operations are buffered per transaction
  private final ConcurrentHashMap<Long, TransactionVectorContext> transactionContexts;

  // In-memory JVector index (rebuilt from pages on load)
  private volatile ImmutableGraphIndex                  graphIndex;
  private final ConcurrentHashMap<Integer, VectorEntry> vectorRegistry;
  private final AtomicInteger                           nextId;
  private final AtomicReference<INDEX_STATUS>           status;

  /**
   * Represents a vector entry with its RID and vector data
   */
  private static class VectorEntry {
    final int       id;
    final RID       rid;
    final float[]   vector;
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
      return Integer.compare(this.hashCode, other.hashCode);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (!(o instanceof ComparableVector other)) return false;
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
    this.associatedBucketId = -1; // Will be set via setMetadata()

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
    this.associatedBucketId = -1; // Will be set via setMetadata()

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
      // Create a RandomAccessVectorValues implementation from our vector registry
      final RandomAccessVectorValues vectors = new RandomAccessVectorValues() {
        @Override
        public int size() {
          return (int) vectorRegistry.values().stream().filter(v -> !v.deleted).count();
        }

        @Override
        public int dimension() {
          return dimensions;
        }

        @Override
        public VectorFloat<?> getVector(int i) {
          final VectorEntry entry = vectorRegistry.values().stream()
              .filter(v -> !v.deleted && v.id == i)
              .findFirst()
              .orElse(null);
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
          LogManager.instance().log(this, Level.INFO, "JVector graph index built successfully");
        }
      } else {
        this.graphIndex = null;
        LogManager.instance().log(this, Level.INFO, "No vectors to index, graph index is null");
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error initializing JVector graph index", e);
      throw new IndexException("Error initializing JVector graph index", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void loadVectorsFromPages() {
    try {
      // Read header from page 0
      final BasePage page0 = database.getTransaction().getPage(new PageId(database, getFileId(), 0), getPageSize());
      final ByteBuffer buffer0 = page0.getContent();
      buffer0.position(0);

      final int totalEntries = buffer0.getInt();
      if (totalEntries == 0) {
        LogManager.instance().log(this, Level.INFO, "No vectors to load for index: " + indexName);
        return;
      }

      // Read metadata (already set from constructor, but validate)
      final int storedDimensions = buffer0.getInt();
      final int storedSimilarity = buffer0.getInt();
      final int storedMaxConnections = buffer0.getInt();
      final int storedBeamWidth = buffer0.getInt();
      final int storedNextId = buffer0.getInt();

      if (storedDimensions != dimensions) {
        throw new IndexException("Dimension mismatch: expected " + dimensions + " but found " + storedDimensions);
      }

      nextId.set(storedNextId);

      // Read vector entries from subsequent pages
      final int entrySize = 4 + 8 + 4 + (dimensions * 4) + 1;
      final int entriesPerPage = (getPageSize() - 100) / entrySize;

      int pageNum = 1;
      int entriesRead = 0;

      while (entriesRead < totalEntries) {
        final BasePage currentPage = database.getTransaction().getPage(new PageId(database, getFileId(), pageNum++), getPageSize());
        final ByteBuffer pageBuffer = currentPage.getContent();
        pageBuffer.position(0);

        final int entriesInPage = pageBuffer.getInt();

        for (int i = 0; i < entriesInPage && entriesRead < totalEntries; i++) {
          final int id = pageBuffer.getInt();
          final long position = pageBuffer.getLong();
          final int bucketId = pageBuffer.getInt();
          final RID rid = new RID(database, bucketId, position);

          final float[] vector = new float[dimensions];
          for (int j = 0; j < dimensions; j++) {
            vector[j] = pageBuffer.getFloat();
          }

          final boolean deleted = pageBuffer.get() == 1;

          final VectorEntry entry = new VectorEntry(id, rid, vector);
          entry.deleted = deleted;
          vectorRegistry.put(id, entry);

          entriesRead++;
        }
      }

      LogManager.instance().log(this, Level.INFO, "Loaded " + entriesRead + " vectors for index: " + indexName);

      // Rebuild the graph index with loaded vectors
      if (!vectorRegistry.isEmpty()) {
        initializeGraphIndex();
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error loading vectors from pages", e);
      throw new IndexException("Error loading vectors from pages", e);
    }
  }

  private void persistVectorsToPages() {
    try {
      // Write header to page 0
      final BasePage page0 = database.getTransaction().getPage(new PageId(database, getFileId(), 0), getPageSize());
      final ByteBuffer buffer0 = page0.getContent();
      buffer0.clear();

      // Write metadata
      buffer0.putInt(vectorRegistry.size());  // Total entries (including deleted)
      buffer0.putInt(dimensions);
      buffer0.putInt(similarityFunction.ordinal());
      buffer0.putInt(maxConnections);
      buffer0.putInt(beamWidth);
      buffer0.putInt(nextId.get());

      // Write vector entries to subsequent pages
      // Calculate how many entries fit per page
      final int entrySize = 4 + 8 + 4 + (dimensions * 4) + 1; // id + RID + position + vector + deleted flag
      final int entriesPerPage = (getPageSize() - 100) / entrySize; // Reserve 100 bytes for page overhead

      int pageNum = 1;
      int entryInPage = 0;
      BasePage currentPage = null;
      ByteBuffer pageBuffer = null;
      int startPosition = 0;

      for (final VectorEntry entry : vectorRegistry.values()) {
        if (entryInPage == 0) {
          // Flush previous page if exists
          if (currentPage != null) {
            // Update entry count at the beginning of the page
            final int savedPosition = pageBuffer.position();
            pageBuffer.position(startPosition);
            pageBuffer.putInt(entryInPage);
            pageBuffer.position(savedPosition);
          }

          // Get new page
          currentPage = database.getTransaction().getPage(new PageId(database, getFileId(), pageNum++), getPageSize());
          pageBuffer = currentPage.getContent();
          pageBuffer.clear();
          startPosition = pageBuffer.position();
          pageBuffer.putInt(0); // Entry count placeholder
        }

        // Write entry
        pageBuffer.putInt(entry.id);
        pageBuffer.putLong(entry.rid.getPosition());
        pageBuffer.putInt(entry.rid.getBucketId());
        for (int i = 0; i < dimensions; i++) {
          pageBuffer.putFloat(entry.vector[i]);
        }
        pageBuffer.put((byte) (entry.deleted ? 1 : 0));

        entryInPage++;

        if (entryInPage >= entriesPerPage) {
          // Page is full, update entry count
          final int savedPosition = pageBuffer.position();
          pageBuffer.position(startPosition);
          pageBuffer.putInt(entryInPage);
          pageBuffer.position(savedPosition);

          entryInPage = 0;
          currentPage = null;
        }
      }

      // Flush final page if it has data
      if (currentPage != null && entryInPage > 0) {
        final int savedPosition = pageBuffer.position();
        pageBuffer.position(startPosition);
        pageBuffer.putInt(entryInPage);
        pageBuffer.position(savedPosition);
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error persisting vectors to pages", e);
      throw new IndexException("Error persisting vectors to pages", e);
    }
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (keys == null || keys.length == 0 || !(keys[0] instanceof float[]))
      throw new IllegalArgumentException("Expected float array as key for vector search");

    final float[] queryVector = (float[]) keys[0];
    if (queryVector.length != dimensions)
      throw new IllegalArgumentException("Query vector dimension " + queryVector.length + " does not match index dimension " + dimensions);

    final int k = limit > 0 ? limit : 10; // Default to top 10 results

    lock.readLock().lock();
    try {
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

        // Create RandomAccessVectorValues for scoring
        final RandomAccessVectorValues vectors = new RandomAccessVectorValues() {
          @Override
          public int size() {
            return (int) vectorRegistry.values().stream().filter(v -> !v.deleted).count();
          }

          @Override
          public int dimension() {
            return dimensions;
          }

          @Override
          public VectorFloat<?> getVector(int i) {
            final VectorEntry entry = vectorRegistry.values().stream()
                .filter(v -> !v.deleted && v.id == i)
                .findFirst()
                .orElse(null);
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

        // Extract RIDs from search results
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int nodeId = nodeScore.node;
          final VectorEntry entry = vectorRegistry.values().stream()
              .filter(v -> !v.deleted && v.id == nodeId)
              .findFirst()
              .orElse(null);
          if (entry != null) {
            resultRIDs.add(entry.rid);
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

    // Unwrap ComparableVector if needed (for transaction replay)
    final float[] vector;
    if (keys[0] instanceof ComparableVector) {
      vector = ((ComparableVector) keys[0]).vector;
    } else if (keys[0] instanceof float[]) {
      vector = (float[]) keys[0];
    } else {
      throw new IllegalArgumentException("Expected float array or ComparableVector as key for vector index, got " + keys[0].getClass());
    }

    if (vector.length != dimensions)
      throw new IllegalArgumentException("Vector dimension " + vector.length + " does not match index dimension " + dimensions);

    final RID rid = values[0];

    // Buffer operations during transactions for atomic commit
    final com.arcadedb.database.TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();
    if (txStatus == com.arcadedb.database.TransactionContext.STATUS.BEGUN ||
        txStatus == com.arcadedb.database.TransactionContext.STATUS.COMMIT_1ST_PHASE ||
        txStatus == com.arcadedb.database.TransactionContext.STATUS.COMMIT_2ND_PHASE) {

      // Only buffer if this is the original call, not a replay
      // Replay is detected by checking if the key is already wrapped in ComparableVector
      if (!(keys[0] instanceof ComparableVector)) {
        // Register the index with the transaction for file locking
        // Wrap in ComparableVector to mark it as tracked
        getDatabase().getTransaction()
            .addIndexOperation(this, com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation.ADD,
                new Object[]{new ComparableVector(vector)}, rid);

        // Buffer the operation in our local context for actual processing in onAfterCommit()
        final long txId = Thread.currentThread().getId();
        final TransactionVectorContext context = transactionContexts.computeIfAbsent(txId, k -> new TransactionVectorContext());
        context.operations.put(rid, new TransactionVectorContext.VectorOperation(
            TransactionVectorContext.OperationType.ADD, vector));
      }
      // If it's wrapped, this is a replay during commit - skip buffering since we already have it
    } else {
      // No transaction: apply immediately
      lock.writeLock().lock();
      try {
        final int id = nextId.getAndIncrement();
        final VectorEntry vectorEntry = new VectorEntry(id, rid, vector);
        vectorRegistry.put(id, vectorEntry);
        initializeGraphIndex();
        persistVectorsToPages();
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

    // Register with transaction system so onAfterCommit() gets called
    if (getDatabase().getTransaction().getStatus() == com.arcadedb.database.TransactionContext.STATUS.BEGUN) {
      // For remove, we don't have the vector value, so use a dummy comparable
      final ComparableVector comparableKey = new ComparableVector(new float[dimensions]);

      // Register with transaction index system
      getDatabase().getTransaction()
          .addIndexOperation(this, com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
              new Object[]{comparableKey}, rid);

      // Buffer the operation in our local context for actual processing in onAfterCommit()
      final long txId = Thread.currentThread().getId();
      final TransactionVectorContext context = transactionContexts.computeIfAbsent(txId, k -> new TransactionVectorContext());
      context.operations.put(rid, new TransactionVectorContext.VectorOperation(
          TransactionVectorContext.OperationType.REMOVE, null));
    } else{
      // No transaction: apply immediately
      lock.writeLock().lock();
      try {
        vectorRegistry.values().stream()
            .filter(v -> v.rid.equals(rid))
            .forEach(v -> v.deleted = true);
        initializeGraphIndex();
        persistVectorsToPages();
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  @Override
  public void onAfterCommit() {
    final long txId = Thread.currentThread().getId();
    final TransactionVectorContext context = transactionContexts.remove(txId);
    if (context == null || context.operations.isEmpty())
      return;

    lock.writeLock().lock();
    try {
      boolean needsRebuild = false;

      // Apply all buffered operations
      for (final Map.Entry<RID, TransactionVectorContext.VectorOperation> entry : context.operations.entrySet()) {
        final RID rid = entry.getKey();
        final TransactionVectorContext.VectorOperation op = entry.getValue();

        if (op.type == TransactionVectorContext.OperationType.ADD) {
          final int id = nextId.getAndIncrement();
          final VectorEntry vectorEntry = new VectorEntry(id, rid, op.vector);
          vectorRegistry.put(id, vectorEntry);
          needsRebuild = true;
        } else if (op.type == TransactionVectorContext.OperationType.REMOVE) {
          // Mark as deleted
          vectorRegistry.values().stream()
              .filter(v -> v.rid.equals(rid))
              .forEach(v -> v.deleted = true);
          needsRebuild = true;
        }
      }

      if (needsRebuild) {
        // Rebuild the graph index with the updated vectors
        initializeGraphIndex();

        // Persist to pages
        persistVectorsToPages();
      }
    } finally {
      lock.writeLock().unlock();
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
    // Compaction handled internally by JVector
    return false;
  }

  @Override
  public boolean isCompacting() {
    return false; // JVector handles compaction internally
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
    // Compaction handled internally by JVector
    return false;
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
}
