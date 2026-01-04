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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
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
import com.arcadedb.utility.Pair;
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
public class LSMVectorIndex implements Index, IndexInternal {
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

  // Graph lifecycle management (Phase 2: Disk-based graph storage)
  enum GraphState {
    LOADING,    // No graph available yet (initial state during startup)
    IMMUTABLE,  // OnDiskGraphIndex - lazy-loaded from disk, optimized for searches
    MUTABLE     // OnHeapGraphIndex - in memory, accepting incremental updates
  }

  private volatile GraphState                    graphState;
  private volatile ImmutableGraphIndex           graphIndex;        // Current graph (OnHeap or OnDisk)
  private volatile int[]                         ordinalToVectorId; // Maps graph ordinals to vector IDs
  private final    VectorLocationIndex           vectorIndex;       // Lightweight pointer index
  private final    AtomicInteger                 nextId;
  private final    AtomicReference<INDEX_STATUS> status;

  // Graph file for persistent storage of graph topology
  // Allows lazy-loading graph from disk and avoiding expensive rebuilds
  private final LSMVectorIndexGraphFile graphFile;
  private final AtomicInteger           mutationsSinceSerialize;

  // Thresholds for graph state transitions
  // Note: JVector's addGraphNode() is meant for pre-build additions, not post-build incremental updates
  // Therefore we use periodic rebuilds which amortize cost over many operations (10x better than rebuild-on-every-search)
  // Mutation threshold is now configurable via metadata.mutationsBeforeRebuild or GlobalConfiguration

  // Compaction support
  private final AtomicInteger           currentMutablePages;
  private final int                     minPagesToScheduleACompaction;
  private       LSMVectorIndexCompacted compactedSubIndex;
  private       boolean                 valid = true;

  public interface GraphBuildCallback {
    /**
     * Called periodically during graph index construction.
     *
     * @param phase          Current phase: "validating", "building", or "persisting"
     * @param processedNodes Number of unique nodes processed so far
     * @param totalNodes     Total number of nodes to process
     * @param vectorAccesses Total number of vector accesses (getVector calls)
     */
    void onGraphBuildProgress(String phase, int processedNodes, int totalNodes, long vectorAccesses);
  }

  /**
   * Custom Bits implementation for filtering vector search by RID.
   * Maps graph ordinals to vector IDs, then checks if the corresponding RID is in the allowed set.
   */
  private class RIDBitsFilter implements Bits {
    private final Set<RID>            allowedRIDs;
    private final int[]               ordinalToVectorIdSnapshot;
    private final VectorLocationIndex vectorIndexSnapshot;

    RIDBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
        final VectorLocationIndex vectorIndexSnapshot) {
      this.allowedRIDs = allowedRIDs;
      this.ordinalToVectorIdSnapshot = ordinalToVectorIdSnapshot;
      this.vectorIndexSnapshot = vectorIndexSnapshot;
    }

    @Override
    public boolean get(final int ordinal) {
      // Check if ordinal is within bounds
      if (ordinal < 0 || ordinal >= ordinalToVectorIdSnapshot.length)
        return false;

      // Map ordinal to vector ID
      final int vectorId = ordinalToVectorIdSnapshot[ordinal];

      // Get the RID for this vector ID
      final VectorLocationIndex.VectorLocation loc = vectorIndexSnapshot.getLocation(vectorId);
      if (loc == null || loc.deleted)
        return false;

      // Check if this RID is in the allowed set
      return allowedRIDs.contains(loc.rid);
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
      this.hashCode = Arrays.hashCode(vector);
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
      return Arrays.equals(vector, other.vector);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  /**
   * Helper class for collecting vector entries during graph build.
   * Used to avoid race conditions with concurrent VectorLocationIndex modifications.
   */
  private static class VectorEntryForGraphBuild {
    final int     vectorId;
    final RID     rid;
    final boolean isCompacted;
    final long    absoluteFileOffset;

    VectorEntryForGraphBuild(final int vectorId, final RID rid, final boolean isCompacted, final long absoluteFileOffset) {
      this.vectorId = vectorId;
      this.rid = rid;
      this.isCompacted = isCompacted;
      this.absoluteFileOffset = absoluteFileOffset;
    }
  }

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder<? extends Index> builder) {
      final BucketLSMVectorIndexBuilder vectorBuilder = (BucketLSMVectorIndexBuilder) builder;

      return new LSMVectorIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(), ComponentFile.MODE.READ_WRITE,
          builder.getPageSize(), vectorBuilder.getTypeName(), vectorBuilder.getPropertyNames(), vectorBuilder.dimensions,
          vectorBuilder.similarityFunction, vectorBuilder.maxConnections, vectorBuilder.beamWidth, vectorBuilder.idPropertyName,
          vectorBuilder.quantizationType);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      // Check if this is a compacted index file (created during compaction)
      if (filePath.endsWith(LSMVectorIndexCompacted.FILE_EXT))
        return new LSMVectorIndexCompacted(null, database, name, filePath, id, mode, pageSize, version);

      // Otherwise, load as main mutable index
      return new LSMVectorIndex(database, name, filePath, id, mode, pageSize, version).mutable;
    }
  }

  /**
   * Constructor for creating a new index
   */
  public LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath, final ComponentFile.MODE mode,
      final int pageSize, final String typeName, final String[] propertyNames, final int dimensions,
      final VectorSimilarityFunction similarityFunction, final int maxConnections, final int beamWidth, final String idPropertyName,
      final VectorQuantizationType quantizationType) {
    try {
      this.indexName = name;

      this.metadata = new LSMVectorIndexMetadata(typeName, propertyNames, -1);
      this.metadata.dimensions = dimensions;
      this.metadata.similarityFunction = similarityFunction;
      this.metadata.quantizationType = quantizationType;
      this.metadata.maxConnections = maxConnections;
      this.metadata.beamWidth = beamWidth;
      this.metadata.idPropertyName = idPropertyName;

      this.lock = new ReentrantReadWriteLock();
      this.vectorIndex = new VectorLocationIndex(getLocationCacheSize(database));
      this.ordinalToVectorId = new int[0];
      this.nextId = new AtomicInteger(0);
      this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);

      // Initialize graph lifecycle management
      this.graphState = GraphState.LOADING;
      this.mutationsSinceSerialize = new AtomicInteger(0);

      // Initialize compaction fields
      this.currentMutablePages = new AtomicInteger(0); // No page0 - start with 0 pages
      this.minPagesToScheduleACompaction = database.getConfiguration()
          .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
      this.compactedSubIndex = null;

      // Create the component that handles page storage
      this.mutable = new LSMVectorIndexMutable(database, indexName, filePath, mode, pageSize);
      this.mutable.setMainIndex(this);

      // Create graph file component (same timing as mutable - outside transaction)
      final String graphFileName = indexName + "_vecgraph";
      final String graphFilePath = filePath + "_vecgraph";
      this.graphFile = new LSMVectorIndexGraphFile(database, graphFileName, graphFilePath, mode, pageSize);
      this.graphFile.setMainIndex(this);
      database.getSchema().getEmbedded().registerFile(this.graphFile);

      LogManager.instance()
          .log(this, Level.FINE, "Created LSMVectorIndex: indexName=%s, vectorFileId=%d, graphFileId=%d", indexName,
              mutable.getFileId(), graphFile.getFileId());

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
    this.vectorIndex = new VectorLocationIndex(getLocationCacheSize(database));
    this.ordinalToVectorId = new int[0];
    this.nextId = new AtomicInteger(0);
    this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);

    // Initialize graph lifecycle management
    this.graphState = GraphState.LOADING;
    this.mutationsSinceSerialize = new AtomicInteger(0);

    // Create the component that handles page storage
    this.mutable = new LSMVectorIndexMutable(database, name, filePath, id, mode, pageSize, version);
    this.mutable.setMainIndex(this);

    // Discover and load graph file if it exists
    this.graphFile = discoverAndLoadGraphFile();
    if (this.graphFile != null) {
      this.graphFile.setMainIndex(this);
    }

    // Initialize compaction fields
    this.currentMutablePages = new AtomicInteger(mutable.getTotalPages());
    this.minPagesToScheduleACompaction = database.getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);

    // Discover and load compacted sub-index file if it exists (critical for replicas after compaction)
    LogManager.instance().log(this, Level.FINE, "Attempting to discover compacted sub-index for index: %s", null, name);
    this.compactedSubIndex = discoverAndLoadCompactedSubIndex();
    if (this.compactedSubIndex != null) {
      LogManager.instance()
          .log(this, Level.WARNING, "Successfully loaded compacted sub-index: %s (fileId=%d)", this.compactedSubIndex.getName(),
              this.compactedSubIndex.getFileId());
    } else {
      LogManager.instance().log(this, Level.FINE, "No compacted sub-index found for index: %s", null, name);
    }

    // DON'T load vectors here - metadata.dimensions is still -1 at this point!
    // Vector loading is deferred until after schema loads metadata via onAfterSchemaLoad() hook.
    // See loadVectorsAfterSchemaLoad() method which is called by LSMVectorIndexMutable.onAfterSchemaLoad()
  }

  /**
   * Load vectors from pages after schema has loaded metadata.
   * Called by LSMVectorIndexMutable.onAfterSchemaLoad() after dimensions are set from schema.json.
   */
  public void loadVectorsAfterSchemaLoad() {
    LogManager.instance()
        .log(this, Level.SEVERE, "loadVectorsAfterSchemaLoad called for index %s: dimensions=%d, mutablePages=%d, hasGraphFile=%s",
            indexName, metadata.dimensions, mutable.getTotalPages(), graphFile != null);

    // Only load vectors if we have valid metadata (dimensions > 0) and pages exist
    if (metadata.dimensions > 0 && mutable.getTotalPages() > 0) {
      try {
        LogManager.instance()
            .log(this, Level.SEVERE, "Loading vectors for index %s after schema load (dimensions=%d, pages=%d, fileId=%d)",
                indexName, metadata.dimensions, mutable.getTotalPages(), mutable.getFileId());

        loadVectorsFromPages();

        // Graph will be lazy-loaded on first search via ensureGraphAvailable()
        // Don't build it here - causes deadlock during database load when PageManager isn't fully ready
        LogManager.instance().log(this, Level.SEVERE,
            "Successfully loaded %d vector locations for index %s (graph will be lazy-loaded on first search)", vectorIndex.size(),
            indexName);
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Could not load vectors from pages for index %s: %s", indexName, e.getMessage());
        this.graphState = GraphState.LOADING;
      }
    } else {
      LogManager.instance()
          .log(this, Level.SEVERE, "Skipping vector load for index %s (dimensions=%d, pages=%d)", indexName, metadata.dimensions,
              mutable.getTotalPages());
    }
  }

  /**
   * Discovers and loads the compacted sub-index file if it exists.
   * This is critical for replicas after compaction, where VectorLocationIndex entries
   * may reference fileId of the compacted file, but the compacted component isn't loaded.
   *
   * @return The loaded compacted sub-index, or null if none found
   */
  private LSMVectorIndexCompacted discoverAndLoadCompactedSubIndex() {
    try {
      final DatabaseInternal database = getDatabase();
      final String componentName = mutable.getName();

      // Extract the index name prefix (everything up to the last '_')
      final int lastUnderscore = componentName.lastIndexOf('_');
      if (lastUnderscore == -1) {
        // No underscore in name - no compacted file expected
        return null;
      }

      final String namePrefix = componentName.substring(0, lastUnderscore);

      // First, check if compacted file is already loaded in schema (all files)
      // This handles the case where the file was already loaded by LocalSchema.load()
      for (int i = 0; i < 1000; i++) {  // Check up to 1000 file IDs
        try {
          final Component comp = database.getSchema().getFileByIdIfExists(i);
          if (comp instanceof LSMVectorIndexCompacted) {
            final String compName = comp.getName();
            if (compName.startsWith(namePrefix + "_") && !compName.equals(componentName)) {
              LogManager.instance()
                  .log(this, Level.SEVERE, "Found existing compacted sub-index in schema: %s (fileId=%d)", compName,
                      comp.getFileId());
              return (LSMVectorIndexCompacted) comp;
            }
          }
        } catch (final Exception e) {
          // File ID doesn't exist, continue
        }
      }

      // If not found in schema, look for ComponentFile in FileManager
      // FileManager tracks all files on disk with their fileIds
      ComponentFile compactedComponentFile = null;
      long highestTimestamp = -1;

      LogManager.instance().log(this, Level.FINE, "Searching FileManager for compacted files with prefix: %s", null, namePrefix);

      for (final ComponentFile file : database.getFileManager().getFiles()) {
        final String fileName = file.getComponentName();
        final String fileExt = file.getFileExtension();

        // Check if this is a compacted sub-index file matching our pattern
        if (LSMVectorIndexCompacted.FILE_EXT.equals(fileExt) && fileName.startsWith(namePrefix + "_") && !fileName.equals(
            componentName)) {

          // Extract timestamp from filename to find most recent
          final int lastUnder = fileName.lastIndexOf('_');
          if (lastUnder != -1) {
            try {
              final long timestamp = Long.parseLong(fileName.substring(lastUnder + 1));
              if (timestamp > highestTimestamp) {
                highestTimestamp = timestamp;
                compactedComponentFile = file;
              }
            } catch (final NumberFormatException e) {
              // Not a valid timestamp, skip
            }
          }
        }
      }

      if (compactedComponentFile == null) {
        // No compacted file found
        return null;
      }

      // Load the compacted sub-index using the ComponentFile's metadata (includes fileId)
      final String compactedName = compactedComponentFile.getComponentName();
      final int compactedFileId = compactedComponentFile.getFileId();
      final String compactedPath = compactedComponentFile.getFilePath();
      final int pageSize = compactedComponentFile instanceof PaginatedComponentFile ?
          ((PaginatedComponentFile) compactedComponentFile).getPageSize() :
          mutable.getPageSize();
      final int version = compactedComponentFile.getVersion();

      // Create the compacted index component from the ComponentFile
      final LSMVectorIndexCompacted compactedIndex = new LSMVectorIndexCompacted(this, database, compactedName, compactedPath,
          compactedFileId, database.getMode(), pageSize, version);

      // NOTE: Do NOT register with schema here - the file is already registered by LocalSchema.load()
      // when it scans the database directory. Registering twice causes "File with id already exists" error.

      LogManager.instance()
          .log(this, Level.WARNING, "Discovered and loaded compacted sub-index: %s (fileId=%d, pages=%d)", compactedName,
              compactedIndex.getFileId(), compactedIndex.getTotalPages());

      return compactedIndex;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error discovering compacted sub-index for %s: %s", indexName, e.getMessage());
      return null;
    }
  }

  /**
   * Discovers and loads the graph file if it exists.
   * Called during index loading to reconnect with persisted graph topology.
   *
   * @return The loaded graph file, or null if none found
   */
  private LSMVectorIndexGraphFile discoverAndLoadGraphFile() {
    try {
      final DatabaseInternal database = getDatabase();
      final String expectedGraphFileName = mutable.getName() + "_" + LSMVectorIndexGraphFile.FILE_EXT;

      LogManager.instance()
          .log(this, Level.FINE, "Discovering graph file for index %s, looking for: %s", indexName, expectedGraphFileName);

      // Look for ComponentFile in FileManager
      for (final ComponentFile file : database.getFileManager().getFiles()) {
        if (file != null && LSMVectorIndexGraphFile.FILE_EXT.equals(file.getFileExtension()) && file.getComponentName()
            .equals(expectedGraphFileName)) {

          final int pageSize = file instanceof com.arcadedb.engine.PaginatedComponentFile ?
              ((com.arcadedb.engine.PaginatedComponentFile) file).getPageSize() :
              mutable.getPageSize();

          final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(database, file.getComponentName(),
              file.getFilePath(), file.getFileId(), database.getMode(), pageSize, file.getVersion());

          database.getSchema().getEmbedded().registerFile(graphFile);

          LogManager.instance().log(this, Level.INFO, "Discovered and loaded graph file: %s (fileId=%d)", graphFile.getName(),
              graphFile.getFileId());

          return graphFile;
        }
      }

      LogManager.instance()
          .log(this, Level.FINE, "No graph file found in FileManager for index %s. Graph will be built on first search.",
              indexName);
      return null;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error discovering graph file for %s: %s", indexName, e.getMessage());
      return null;
    }
  }

  /**
   * Initialize graph index - called during index creation/loading.
   * For new indexes, builds graph immediately. For loaded indexes, graph is lazy-loaded on first search.
   */
  private void initializeGraphIndex() {
    // For newly created indexes (during constructor), build graph immediately
    // For loaded indexes, graph will be lazy-loaded on first search via ensureGraphAvailable()
    if (vectorIndex.size() > 0 && graphState == GraphState.LOADING) {
      // Check if we can lazy-load from persisted graph
      if (graphFile != null && graphFile.hasPersistedGraph()) {
        LogManager.instance().log(this, Level.INFO, "Graph will be lazy-loaded from disk for index: %s", indexName);
        return;
      }

      // No persisted graph - build now for new indexes
      LogManager.instance().log(this, Level.INFO, "Building graph from scratch for index: %s", indexName);

      // NOTE: buildGraphFromScratch() manages locking internally
      // Don't hold lock here - JVector uses parallel threads during graph build
      buildGraphFromScratch();
    }
  }

  /**
   * Ensure graph is available for searching. Lazy-loads from disk if needed.
   * This is the entry point for all search operations.
   */
  private void ensureGraphAvailable() {
    if (graphState != GraphState.LOADING)
      return; // Graph already available or being built

    lock.writeLock().lock();
    try {
      // Double-check after acquiring lock
      if (graphState != GraphState.LOADING)
        return; // Another thread already started building

      // Try to load persisted graph from disk
      if (graphFile != null && graphFile.hasPersistedGraph()) {
        try {
          this.graphIndex = graphFile.loadGraph();
          this.graphState = GraphState.IMMUTABLE;

          // Rebuild ordinalToVectorId from vectorIndex
          this.ordinalToVectorId = vectorIndex.getAllVectorIds().filter(id -> {
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(id);
            return loc != null && !loc.deleted;
          }).sorted().toArray();

          LogManager.instance().log(this, Level.INFO,
              "Loaded graph from disk for index: %s, graphSize=%d, ordinalToVectorIdLength=%d, vectorIndexSize=%d", indexName,
              graphIndex != null ? graphIndex.size() : 0, ordinalToVectorId.length, vectorIndex.size());
          return;
        } catch (final Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Failed to load graph for %s, will rebuild: %s", indexName, e.getMessage());
        }
      }

    } finally {
      lock.writeLock().unlock();
    }

    // No persisted graph or load failed - build from scratch
    buildGraphFromScratch();
  }

  /**
   * Build graph from scratch by reading all active vectors and constructing the graph index.
   * After building, persists the graph to disk and transitions to IMMUTABLE state.
   */
  private void buildGraphFromScratch() {
    buildGraphFromScratch(null);
  }

  /**
   * Build graph from scratch with optional progress callback.
   *
   * @param graphCallback Optional callback for graph build progress
   */
  private void buildGraphFromScratch(final GraphBuildCallback graphCallback) {
    // buildGraphFromScratchWithRetry() reads pages directly and rebuilds vectorIndex
    // No need to reload here - just call the retry logic directly
    buildGraphFromScratchWithRetry(graphCallback);
  }

  /**
   * Internal implementation of buildGraphFromScratch.
   * Always reads directly from pages to avoid race conditions with concurrent VectorLocationIndex modifications.
   *
   * @param graphCallback Optional callback for graph build progress
   */
  private void buildGraphFromScratchWithRetry(final GraphBuildCallback graphCallback) {
    // CRITICAL FIX: Collect vectors DIRECTLY from pages instead of from vectorIndex.
    // This avoids race conditions where concurrent replication adds entries to vectorIndex
    // that don't yet exist on disk pages. We iterate pages and read what's actually persisted.
    final java.util.Map<RID, VectorEntryForGraphBuild> ridToLatestVector = new java.util.HashMap<>();
    int totalEntriesRead = 0;
    int filteredZeroVectors = 0;
    int filteredDeletedVectors = 0;

    // First, read from compacted sub-index if it exists
    if (compactedSubIndex != null) {
      final int compactedTotalPages = compactedSubIndex.getTotalPages();
      for (int pageNum = 0; pageNum < compactedTotalPages; pageNum++) {
        try {
          final PageId pageId = new PageId(getDatabase(), compactedSubIndex.getFileId(), pageNum);
          final var page = getDatabase().getPageManager().getImmutablePage(pageId, getPageSize(), false, false);
          if (page == null)
            continue;

          final int numberOfEntries = page.readInt(OFFSET_NUM_ENTRIES);
          if (numberOfEntries == 0)
            continue;

          // Calculate header size (page 0 has extra metadata)
          final int headerSize;
          if (pageNum == 0) {
            // Compacted page 0: base header + dimensions + similarity + maxConn + beamWidth
            headerSize = HEADER_BASE_SIZE + (4 * 4); // 9 + 16 = 25 bytes
          } else {
            headerSize = HEADER_BASE_SIZE; // 9 bytes
          }

          // Calculate absolute file offset for this page
          final long pageStartOffset = (long) pageNum * getPageSize();

          // Parse variable-sized entries sequentially (no pointer table)
          int currentOffset = headerSize;
          for (int i = 0; i < numberOfEntries; i++) {
            // Record absolute file offset for this entry
            final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + currentOffset;

            // Read variable-sized vectorId
            final long[] vectorIdAndSize = page.readNumberAndSize(currentOffset);
            final int vectorId = (int) vectorIdAndSize[0];
            currentOffset += (int) vectorIdAndSize[1];

            // Read variable-sized bucketId
            final long[] bucketIdAndSize = page.readNumberAndSize(currentOffset);
            final int bucketId = (int) bucketIdAndSize[0];
            currentOffset += (int) bucketIdAndSize[1];

            // Read variable-sized position
            final long[] positionAndSize = page.readNumberAndSize(currentOffset);
            final long position = positionAndSize[0];
            currentOffset += (int) positionAndSize[1];

            final RID rid = new RID(getDatabase(), bucketId, position);

            // Read deleted flag (fixed 1 byte)
            final boolean deleted = page.readByte(currentOffset) == 1;
            currentOffset += 1;

            totalEntriesRead++;

            if (deleted) {
              filteredDeletedVectors++;
              continue;
            }

            // Note: Zero vector check will be done when loading from document during graph build
            // Keep latest (highest ID) vector for each RID
            final VectorEntryForGraphBuild existing = ridToLatestVector.get(rid);
            if (existing == null || vectorId > existing.vectorId) {
              ridToLatestVector.put(rid, new VectorEntryForGraphBuild(vectorId, rid, true, entryFileOffset));
            }
          }
        } catch (final Exception e) {
          // Skip problematic pages
          LogManager.instance()
              .log(this, Level.WARNING, "Error reading compacted page %d during graph build: %s", null, pageNum, e.getMessage());
        }
      }
    }

    // Then read from mutable index
    final int mutableTotalPages = getTotalPages();
    for (int pageNum = 0; pageNum < mutableTotalPages; pageNum++) {
      try {
        final PageId pageId = new PageId(getDatabase(), getFileId(), pageNum);
        final var page = getDatabase().getPageManager().getImmutablePage(pageId, getPageSize(), false, false);
        if (page == null)
          continue;

        final int numberOfEntries = page.readInt(OFFSET_NUM_ENTRIES);
        if (numberOfEntries == 0)
          continue;

        // Calculate absolute file offset for this page
        final long pageStartOffset = (long) pageNum * getPageSize();

        // Parse variable-sized entries sequentially (no pointer table)
        int currentOffset = HEADER_BASE_SIZE; // Mutable pages always use base header size
        for (int i = 0; i < numberOfEntries; i++) {
          // Record absolute file offset for this entry
          final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + currentOffset;

          // Read variable-sized vectorId
          final long[] vectorIdAndSize = page.readNumberAndSize(currentOffset);
          final int vectorId = (int) vectorIdAndSize[0];
          currentOffset += (int) vectorIdAndSize[1];

          // Read variable-sized bucketId
          final long[] bucketIdAndSize = page.readNumberAndSize(currentOffset);
          final int bucketId = (int) bucketIdAndSize[0];
          currentOffset += (int) bucketIdAndSize[1];

          // Read variable-sized position
          final long[] positionAndSize = page.readNumberAndSize(currentOffset);
          final long position = positionAndSize[0];
          currentOffset += (int) positionAndSize[1];

          final RID rid = new RID(getDatabase(), bucketId, position);

          // Read deleted flag (fixed 1 byte)
          final boolean deleted = page.readByte(currentOffset) == 1;
          currentOffset += 1;

          totalEntriesRead++;

          if (deleted) {
            filteredDeletedVectors++;
            continue;
          }

          // Note: Zero vector check will be done when loading from document during graph build
          // Keep latest (highest ID) vector for each RID (mutable entries override compacted)
          final VectorEntryForGraphBuild existing = ridToLatestVector.get(rid);
          if (existing == null || vectorId > existing.vectorId) {
            ridToLatestVector.put(rid, new VectorEntryForGraphBuild(vectorId, rid, false, entryFileOffset));
          }
        }
      } catch (final Exception e) {
        // Skip problematic pages
        LogManager.instance().log(this, Level.WARNING, "Error reading mutable page %d during graph build: %s - %s", null, pageNum,
            e.getClass().getSimpleName(), e.getMessage());
        if (LogManager.instance().isDebugEnabled())
          e.printStackTrace();
      }
    }

    // Build ordinal mapping from deduplicated vectors read directly from pages
    final int[] activeVectorIds = ridToLatestVector.values().stream().mapToInt(v -> v.vectorId).sorted().toArray();

    // Log statistics
    if (filteredZeroVectors > 0 || filteredDeletedVectors > 0) {
      LogManager.instance()
          .log(this, Level.INFO, "Graph build from pages: %d total entries, %d deleted, %d zero vectors, %d active for graph",
              totalEntriesRead, filteredDeletedVectors, filteredZeroVectors, activeVectorIds.length);
    }

    // Acquire write lock for updating vectorIndex and preparing build
    lock.writeLock().lock();
    final RandomAccessVectorValues vectors;
    final int[] finalActiveVectorIds;
    try {
      // CRITICAL: If we couldn't read any entries from pages (e.g., during database close),
      // DON'T clear vectorIndex - use what's already in memory!
      final int[] vectorIds;
      if (!ridToLatestVector.isEmpty()) {
        // Update vectorIndex to match what we found on pages (sync it with disk state)
        // This ensures vectorIndex is consistent with the graph we're about to build
        vectorIndex.clear();
        for (final VectorEntryForGraphBuild entry : ridToLatestVector.values()) {
          vectorIndex.addOrUpdate(entry.vectorId, entry.isCompacted, entry.absoluteFileOffset, entry.rid, false);
        }
        vectorIds = activeVectorIds; // Use vector IDs from pages
      } else {
        LogManager.instance().log(this, Level.SEVERE,
            "FALLBACK: Could not read vectors from pages (database closing), using existing vectorIndex with %d entries",
            vectorIndex.size());
        // Build vector IDs from existing vectorIndex
        vectorIds = vectorIndex.getAllVectorIds().filter(id -> {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(id);
          return loc != null && !loc.deleted;
        }).sorted().toArray();
        LogManager.instance()
            .log(this, Level.SEVERE, "FALLBACK: Built %d active vector IDs from in-memory vectorIndex", vectorIds.length);
      }

      // Create a SNAPSHOT of vectorIndex for JVector to use safely
      final String vectorProp =
          metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.get(0) : "vector";

      // CRITICAL FIX: Validate vectors before building graph to filter out deleted documents
      // When a document is deleted, getVector() returns null which breaks JVector index building
      final Map<Integer, VectorLocationIndex.VectorLocation> vectorLocationSnapshot = new HashMap<>();
      final List<Integer> validVectorIds = new ArrayList<>();
      int skippedDeletedDocs = 0;

      // Progress tracking for validation phase
      final int totalVectorsToValidate = vectorIds.length;
      int validatedCount = 0;
      final long VALIDATION_PROGRESS_INTERVAL = 1000;

      for (int vectorId : vectorIds) {
        final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
        if (loc != null && !loc.deleted) {
          // Validate that the document still exists and has a valid vector
          try {
            final com.arcadedb.database.Record record = getDatabase().lookupByRID(loc.rid, false);
            if (record != null) {
              final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) record;
              final Object vectorObj = doc.get(vectorProp);

              final float[] vector = VectorUtils.convertToFloatArray(vectorObj);

              if (vector != null && vector.length == metadata.dimensions) {
                // Validate vector is not all zeros (would cause NaN in cosine similarity)
                boolean hasNonZero = false;
                for (float v : vector) {
                  if (v != 0.0f) {
                    hasNonZero = true;
                    break;
                  }
                }
                if (hasNonZero) {
                  vectorLocationSnapshot.put(vectorId, loc);
                  validVectorIds.add(vectorId);
                }
              }
            }
          } catch (final RecordNotFoundException e) {
            // Document was deleted - skip this vector
            skippedDeletedDocs++;
          } catch (final Exception e) {
            // Other errors - skip this vector
            skippedDeletedDocs++;
          }
        }

        // Report validation progress
        validatedCount++;
        if (graphCallback != null && validatedCount % VALIDATION_PROGRESS_INTERVAL == 0) {
          graphCallback.onGraphBuildProgress("validating", validatedCount, totalVectorsToValidate, 0);
        }
      }

      // Final validation progress report
      if (graphCallback != null && validatedCount > 0) {
        graphCallback.onGraphBuildProgress("validating", validatedCount, totalVectorsToValidate, 0);
      }

      if (skippedDeletedDocs > 0) {
        LogManager.instance()
            .log(this, Level.INFO, "Filtered out %d vectors with deleted/invalid documents during graph build", skippedDeletedDocs);
      }

      // Use validated vector IDs instead of unfiltered ones
      final int[] filteredVectorIds = validVectorIds.stream().mapToInt(Integer::intValue).toArray();
      this.ordinalToVectorId = filteredVectorIds;
      finalActiveVectorIds = filteredVectorIds;

      if (filteredVectorIds.length == 0) {
        this.graphIndex = null;
        this.graphState = GraphState.IMMUTABLE;
        LogManager.instance().log(this, Level.INFO, "No vectors to index, graph is null for index: " + indexName);
        return;
      }

      final int graphBuildCacheSize = getGraphBuildCacheSize();
      LogManager.instance().log(this, Level.INFO, "Building graph with %d vectors using property '%s' (cache enabled: size=%d)",
          filteredVectorIds.length, vectorProp, graphBuildCacheSize);

      // Create lazy-loading vector values that reads vectors from documents or index pages (if quantized)
      vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions, vectorProp,
          vectorLocationSnapshot,  // Use immutable snapshot
          finalActiveVectorIds, this,  // Pass LSM index reference for quantization support
          graphBuildCacheSize  // Pass configurable cache size
      );

      // Mark that graph building is in progress to prevent new inserts
      this.graphState = GraphState.MUTABLE;
    } finally {
      lock.writeLock().unlock();
    }

    try {
      // Build the graph index using JVector 4.0 API (WITHOUT holding our lock - JVector uses parallel threads)
      LogManager.instance()
          .log(this, Level.INFO, "Building JVector graph index with " + vectors.size() + " vectors for index: " + indexName);

      // Create BuildScoreProvider for index construction
      final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(vectors, metadata.similarityFunction);

      // Build the graph index (parallel operation - no lock held)
      final ImmutableGraphIndex builtGraph;
      try (final GraphIndexBuilder builder = new GraphIndexBuilder(scoreProvider, metadata.dimensions,
          metadata.maxConnections,  // M parameter (graph degree)
          metadata.beamWidth,       // efConstruction (construction search depth)
          metadata.neighborOverflowFactor,    // neighbor overflow factor (default: 1.2)
          metadata.alphaDiversityRelaxation,  // alpha diversity relaxation (default: 1.2)
          false,           // no distance transform
          true)) {         // enable concurrent updates

        // Start progress monitoring thread if callback provided
        final Thread progressMonitor;
        final AtomicBoolean buildComplete = new AtomicBoolean(false);
        if (graphCallback != null) {
          final int totalNodes = vectors.size();
          progressMonitor = new Thread(() -> {
            try {
              while (!buildComplete.get()) {
                // Poll JVector's internal state
                final int nodesAdded = builder.getGraph().getIdUpperBound();
                final int insertsInProgress = builder.insertsInProgress();

                // Report progress
                graphCallback.onGraphBuildProgress("building", nodesAdded, totalNodes, nodesAdded + insertsInProgress);

                // Sleep briefly before next poll
                Thread.sleep(100); // Poll every 100ms
              }
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING, "Error in graph build progress monitor: " + e.getMessage());
            }
          }, "JVector-Progress-Monitor-" + indexName);
          progressMonitor.setDaemon(true);
          progressMonitor.start();
        } else {
          progressMonitor = null;
        }

        try {
          builtGraph = builder.build(vectors);
        } finally {
          // Stop progress monitoring
          buildComplete.set(true);
          if (progressMonitor != null) {
            try {
              progressMonitor.join(1000); // Wait up to 1 second for clean shutdown
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        }

        LogManager.instance().log(this, Level.INFO, "JVector graph index built successfully");
      } catch (final AssertionError e) {
        LogManager.instance().log(this, Level.SEVERE, "JVector assertion failed during graph build (dimensions=%d, vectors=%d): %s",
            metadata.dimensions, vectors.size(), e.getMessage());
        throw e;
      }

      // Reacquire write lock to update graph state
      lock.writeLock().lock();
      try {
        this.graphIndex = builtGraph;
        this.graphState = GraphState.IMMUTABLE;
      } finally {
        lock.writeLock().unlock();
      }

      // Persist graph to disk IMMEDIATELY in its own transaction
      // This ensures the graph is available on next database open (fast restart)
      if (graphFile != null) {
        final int totalNodes = graphIndex.getIdUpperBound();
        LogManager.instance().log(this, Level.FINE, "Writing vector graph to disk for index: %s (nodes=%d)", indexName, totalNodes);

        // Report persistence phase start
        if (graphCallback != null) {
          graphCallback.onGraphBuildProgress("persisting", 0, totalNodes, 0);
        }

        // Start a dedicated transaction for graph persistence
        final boolean startedTransaction = !getDatabase().isTransactionActive();
        if (startedTransaction)
          getDatabase().begin();

        try {
          graphFile.writeGraph(graphIndex, vectors);

          // Report persistence completion
          if (graphCallback != null) {
            graphCallback.onGraphBuildProgress("persisting", totalNodes, totalNodes, 0);
          }

          // Commit the transaction to persist graph pages
          if (startedTransaction) {
            getDatabase().commit();
            LogManager.instance().log(this, Level.FINE, "Vector graph persisted and committed for index: %s", indexName);
          } else {
            LogManager.instance()
                .log(this, Level.FINE, "Vector graph persisted (transaction managed by caller) for index: %s", indexName);
          }
        } catch (final Exception e) {
          // Rollback on error
          if (startedTransaction) {
            try {
              getDatabase().rollback();
            } catch (final Exception rollbackEx) {
              // Ignore rollback errors
            }
          }
          LogManager.instance()
              .log(this, Level.SEVERE, "PERSIST: Failed to persist graph for %s: %s - %s", indexName, e.getClass().getSimpleName(),
                  e.getMessage());
          e.printStackTrace();
          // Don't throw - allow the index to continue working, just won't have persisted graph
        }
      } else {
        LogManager.instance().log(this, Level.SEVERE, "PERSIST: graphFile is NULL, cannot persist graph for index: %s", indexName);
      }
      this.mutationsSinceSerialize.set(0);

      LogManager.instance().log(this, Level.INFO, "Built graph for index: " + indexName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error building graph from scratch", e);
      throw new IndexException("Error building graph from scratch", e);
    }
  }

  /**
   * Rebuild the graph if mutation threshold reached (Phase 5+: Periodic Rebuilds).
   * Rebuilds every N mutations to amortize cost over many operations.
   * Assumes write lock is already held by caller.
   */
  private void rebuildGraphIfNeeded() {
    if (graphState != GraphState.MUTABLE)
      return;

    if (mutationsSinceSerialize.get() < getMutationsBeforeRebuild())
      return; // Not enough mutations yet

    LogManager.instance().log(this, Level.INFO,
        "Rebuilding graph after " + mutationsSinceSerialize.get() + " mutations (threshold: " + getMutationsBeforeRebuild()
            + ", index: " + indexName + ")");

    try {
      // Rebuild graph from current vectorIndex state
      buildGraphFromScratch();
      // buildGraphFromScratch() resets state and counter
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error rebuilding graph after mutations", e);
      // Don't throw - allow operations to continue, will retry on next threshold
    }
  }

  /**
   * Load vector location metadata from LSM-style pages.
   * Only reads metadata (page location, RID, deleted flag), NOT the actual vector data.
   * This dramatically reduces memory usage and speeds up loading.
   * Reads from all pages, later entries override earlier ones (LSM merge-on-read).
   */
  private void loadVectorsFromPages() {
    try {
      // NOTE: All metadata (dimensions, similarityFunction, maxConnections, beamWidth) comes from schema JSON
      // via applyMetadataFromSchema(). Pages contain only vector data, no metadata.

      LogManager.instance()
          .log(this, Level.FINE, "loadVectorsFromPages START: index=%s, totalPages=%d, vectorIndexSizeBefore=%d", null, indexName,
              getTotalPages(), vectorIndex.size());

      int entriesRead = 0;
      int maxVectorId = -1;

      // Load from compacted sub-index first (if it exists)
      if (compactedSubIndex != null) {
        final int compactedEntries = loadVectorsFromFile(compactedSubIndex.getFileId(), compactedSubIndex.getTotalPages(), true);
        entriesRead += compactedEntries;
        LogManager.instance()
            .log(this, Level.INFO, "Loaded %d entries from compacted sub-index (fileId=%d)", null, compactedEntries,
                compactedSubIndex.getFileId());
      }

      // Load from mutable index (always present)
      final int mutableEntries = loadVectorsFromFile(getFileId(), getTotalPages(), false);
      entriesRead += mutableEntries;

      // Compute nextId from the maximum vector ID found across both files
      maxVectorId = vectorIndex.getAllVectorIds().max().orElse(-1);
      nextId.set(maxVectorId + 1);

      LogManager.instance().log(this, Level.FINE,
          "loadVectorsFromPages DONE: Loaded " + vectorIndex.size() + " vector locations (" + entriesRead
              + " total entries) for index: " + indexName + ", nextId=" + nextId.get() + ", fileId=" + getFileId() + ", totalPages="
              + getTotalPages() + (compactedSubIndex != null ?
              ", compactedFileId=" + compactedSubIndex.getFileId() + ", compactedPages=" + compactedSubIndex.getTotalPages() :
              ""));

      // NOTE: Do NOT call initializeGraphIndex() here - it would cause infinite recursion
      // because buildGraphFromScratch() calls loadVectorsFromPages()
      // Graph initialization is handled separately by the constructor and ensureGraphAvailable()

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error loading vectors from pages", e);
      throw new IndexException("Error loading vectors from pages", e);
    }
  }

  /**
   * Load vector location metadata from a specific file's pages.
   *
   * @param fileId      The file ID to load from
   * @param totalPages  The number of pages in that file
   * @param isCompacted True if loading from compacted file, false if from mutable file
   *
   * @return Number of entries read
   */
  private int loadVectorsFromFile(final int fileId, final int totalPages, final boolean isCompacted) {
    int entriesRead = 0;
    int pagesWithEntries = 0;

    LogManager.instance().log(this, Level.FINE,
        "loadVectorsFromFile: fileId=" + fileId + ", totalPages=" + totalPages + ", isCompacted=" + isCompacted);

    for (int pageNum = 0; pageNum < totalPages; pageNum++) {
      try {
        // Use getImmutablePage to read directly from disk, not from transaction cache
        final BasePage currentPage = getDatabase().getPageManager()
            .getImmutablePage(new PageId(getDatabase(), fileId, pageNum), getPageSize(), false, false);

        if (currentPage == null) {
          LogManager.instance().log(this, Level.FINE, "Page %d in file %d does not exist", null, pageNum, fileId);
          continue;
        }

        // Read page header
        final int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        final int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

        if (numberOfEntries == 0)
          continue; // Empty page

        pagesWithEntries++;

        // Calculate header size (page 0 of compacted index has extra metadata)
        final int headerSize;
        if (isCompacted && pageNum == 0) {
          // Compacted page 0: base header + dimensions + similarity + maxConn + beamWidth
          headerSize = HEADER_BASE_SIZE + (4 * 4); // 9 + 16 = 25 bytes (base + 4 ints)
        } else {
          headerSize = HEADER_BASE_SIZE; // 9 bytes
        }

        // Calculate absolute file offset for the start of this page's data
        final long pageStartOffset = (long) pageNum * getPageSize();

        // Parse variable-sized entries sequentially (no pointer table)
        int currentOffset = headerSize;
        for (int i = 0; i < numberOfEntries; i++) {
          // Record absolute file offset for this entry (before reading it)
          final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + currentOffset;

          // Read variable-sized vectorId
          final long[] vectorIdAndSize = currentPage.readNumberAndSize(currentOffset);
          final int id = (int) vectorIdAndSize[0];
          currentOffset += (int) vectorIdAndSize[1];

          // Read variable-sized bucketId
          final long[] bucketIdAndSize = currentPage.readNumberAndSize(currentOffset);
          final int bucketId = (int) bucketIdAndSize[0];
          currentOffset += (int) bucketIdAndSize[1];

          // Read variable-sized position
          final long[] positionAndSize = currentPage.readNumberAndSize(currentOffset);
          final long position = positionAndSize[0];
          currentOffset += (int) positionAndSize[1];

          final RID rid = new RID(getDatabase(), bucketId, position);

          // Read deleted flag (fixed 1 byte)
          final boolean deleted = currentPage.readByte(currentOffset) == 1;
          currentOffset += 1;

          // Skip quantized vector data if quantization is enabled
          // This data is not needed for location index, only for vector retrieval
          if (metadata.quantizationType != VectorQuantizationType.NONE) {
            // Read quantization type flag
            final byte quantTypeOrdinal = currentPage.readByte(currentOffset);
            currentOffset += 1;

            final VectorQuantizationType quantType = VectorQuantizationType.values()[quantTypeOrdinal];

            if (quantType == VectorQuantizationType.INT8) {
              // Skip: vector length (4 bytes) + quantized bytes + min (4 bytes) + max (4 bytes)
              final int vectorLength = currentPage.readInt(currentOffset);
              currentOffset += 4; // vector length
              currentOffset += vectorLength; // quantized bytes
              currentOffset += 8; // min + max (2 floats)

            } else if (quantType == VectorQuantizationType.BINARY) {
              // Skip: original length (4 bytes) + packed bytes + median (4 bytes)
              final int originalLength = currentPage.readInt(currentOffset);
              currentOffset += 4; // original length
              final int byteCount = (originalLength + 7) / 8; // packed bytes
              currentOffset += byteCount; // packed bytes
              currentOffset += 4; // median (float)
            }
          }

          // Store location metadata with absolute file offset
          vectorIndex.addOrUpdate(id, isCompacted, entryFileOffset, rid, deleted);
          entriesRead++;
        }
      } catch (final Exception e) {
        // Page might not exist, skip
        LogManager.instance().log(this, Level.SEVERE, "Skipping page %d in file %d: %s", null, pageNum, fileId, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.FINE,
        "loadVectorsFromFile DONE: fileId=" + fileId + ", entriesRead=" + entriesRead + ", pagesWithEntries=" + pagesWithEntries);

    return entriesRead;
  }

  /**
   * Persist a single vector and add its location to the vectorIndex.
   * Used during put() operations.
   */
  private void persistVectorWithLocation(final int id, final RID rid, final float[] vector) {
    try {
      // Quantize vector if quantization is enabled
      final VectorQuantizationMetadata qmeta = (VectorQuantizationMetadata) quantizeVector(vector);

      // Calculate variable entry size for this specific entry
      final int vectorIdSize = Binary.getNumberSpace(id);
      final int bucketIdSize = Binary.getNumberSpace(rid.getBucketId());
      final int positionSize = Binary.getNumberSpace(rid.getPosition());
      int entrySize = vectorIdSize + positionSize + bucketIdSize + 1; // +1 for deleted byte

      // Add size for quantized vector data if quantization is enabled
      if (qmeta != null) {
        entrySize += 1; // quantization type flag
        if (qmeta.getType() == VectorQuantizationType.INT8) {
          final VectorQuantizationMetadata.Int8QuantizationMetadata int8meta = (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta;
          entrySize += 4; // vector length (int)
          entrySize += int8meta.quantized.length; // quantized bytes
          entrySize += 8; // min + max (2 floats)
        } else if (qmeta.getType() == VectorQuantizationType.BINARY) {
          final VectorQuantizationMetadata.BinaryQuantizationMetadata binmeta = (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta;
          entrySize += 4; // original length (int)
          entrySize += binmeta.packed.length; // packed bytes
          entrySize += 4; // median (float)
        }
      }

      // Get or create the last mutable page
      int lastPageNum = getTotalPages() - 1;
      if (lastPageNum < 0) {
        lastPageNum = 0;
        createNewVectorDataPage(lastPageNum);
      }

      // Get current page
      MutablePage currentPage = getDatabase().getTransaction()
          .getPageToModify(new PageId(getDatabase(), getFileId(), lastPageNum), getPageSize(), false);

      // Read page header using MutablePage methods (accounts for PAGE_HEADER_SIZE automatically)
      int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
      int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

      // Validate offsetFreeContent is sane (detect old-format or corrupted pages)
      if (offsetFreeContent < HEADER_BASE_SIZE || offsetFreeContent > currentPage.getMaxContentSize()) {
        // Old format page or corrupted, create new page
        LogManager.instance()
            .log(this, Level.WARNING, "Invalid offsetFreeContent=%d in page %d (expected range: %d-%d), creating new page",
                offsetFreeContent, lastPageNum, HEADER_BASE_SIZE, currentPage.getMaxContentSize());
        currentPage.writeByte(OFFSET_MUTABLE, (byte) 0);
        lastPageNum++;
        currentPage = createNewVectorDataPage(lastPageNum);
        offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        numberOfEntries = 0;
      }

      // Calculate space needed (no pointer table - just header + sequential entries)
      final int availableSpace = currentPage.getMaxContentSize() - offsetFreeContent;

      if (availableSpace < entrySize) {
        // Page is full, mark it as immutable before creating a new page
        currentPage.writeByte(OFFSET_MUTABLE, (byte) 0); // mutable = 0

        lastPageNum++;
        currentPage = createNewVectorDataPage(lastPageNum);
        offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        numberOfEntries = 0;
      }

      // Calculate absolute file offset for this entry
      final long pageStartOffset = (long) lastPageNum * getPageSize();
      final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + offsetFreeContent;

      // Write entry sequentially using variable-sized encoding
      int bytesWritten = 0;
      bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, id);
      bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getBucketId());
      bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getPosition());
      bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) 0); // not deleted

      // Write quantized vector data if quantization is enabled
      if (qmeta != null) {
        // Write quantization type flag
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) qmeta.getType().ordinal());

        if (qmeta.getType() == VectorQuantizationType.INT8) {
          final VectorQuantizationMetadata.Int8QuantizationMetadata int8meta = (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta;

          // Write vector length
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, int8meta.quantized.length);

          // Write quantized bytes
          for (final byte b : int8meta.quantized) {
            bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, b);
          }

          // Write min and max
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(int8meta.min));
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(int8meta.max));

        } else if (qmeta.getType() == VectorQuantizationType.BINARY) {
          final VectorQuantizationMetadata.BinaryQuantizationMetadata binmeta = (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta;

          // Write original length
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, binmeta.originalLength);

          // Write packed bytes
          for (final byte b : binmeta.packed) {
            bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, b);
          }

          // Write median
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(binmeta.median));
        }
      }

      // Update page header
      numberOfEntries++;
      offsetFreeContent += bytesWritten;
      currentPage.writeInt(OFFSET_FREE_CONTENT, offsetFreeContent);
      currentPage.writeInt(OFFSET_NUM_ENTRIES, numberOfEntries);

      // Add location to vectorIndex with absolute file offset (isCompacted=false for mutable file)
      vectorIndex.addOrUpdate(id, false, entryFileOffset, rid, false);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error persisting vector with location", e);
      throw new IndexException("Error persisting vector with location", e);
    }
  }

  /**
   * Persist deletion tombstones for deleted vectors.
   * Writes deleted entries to pages so they persist across restarts (LSM style).
   */
  private void persistDeletionTombstones(final List<Integer> deletedIds) {
    try {
      if (deletedIds.isEmpty())
        return;

      // Get or create the last mutable page
      int lastPageNum = getTotalPages() - 1;
      if (lastPageNum < 0) {
        lastPageNum = 0;
        createNewVectorDataPage(lastPageNum);
      }

      // Append deletion tombstones to pages
      for (final Integer vectorId : deletedIds) {
        final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
        if (loc == null)
          continue;

        // Calculate variable entry size for this specific entry
        final int vectorIdSize = Binary.getNumberSpace(vectorId);
        final int bucketIdSize = Binary.getNumberSpace(loc.rid.getBucketId());
        final int positionSize = Binary.getNumberSpace(loc.rid.getPosition());
        final int entrySize = vectorIdSize + positionSize + bucketIdSize + 1; // +1 for deleted byte

        // Get current page
        MutablePage currentPage = getDatabase().getTransaction()
            .getPageToModify(new PageId(getDatabase(), getFileId(), lastPageNum), getPageSize(), false);

        // Read page header (accounts for PAGE_HEADER_SIZE automatically)
        int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

        // Validate offsetFreeContent is sane (detect old-format or corrupted pages)
        if (offsetFreeContent < HEADER_BASE_SIZE || offsetFreeContent > currentPage.getMaxContentSize()) {
          // Old format page or corrupted, create new page
          LogManager.instance()
              .log(this, Level.WARNING, "Invalid offsetFreeContent=%d in page %d (expected range: %d-%d), creating new page",
                  offsetFreeContent, lastPageNum, HEADER_BASE_SIZE, currentPage.getMaxContentSize());
          currentPage.writeByte(OFFSET_MUTABLE, (byte) 0);
          lastPageNum++;
          currentPage = createNewVectorDataPage(lastPageNum);
          offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
          numberOfEntries = 0;
        }

        // Calculate space needed (no pointer table - just header + sequential entries)
        final int availableSpace = currentPage.getMaxContentSize() - offsetFreeContent;

        if (availableSpace < entrySize) {
          // Page is full, mark it as immutable before creating a new page
          currentPage.writeByte(OFFSET_MUTABLE, (byte) 0);

          lastPageNum++;
          currentPage = createNewVectorDataPage(lastPageNum);
          offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
          numberOfEntries = 0;
        }

        // Write deletion tombstone sequentially using variable-sized encoding
        int bytesWritten = 0;
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, vectorId);
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, loc.rid.getBucketId());
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, loc.rid.getPosition());
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) 1); // Mark as deleted

        // Update page header
        numberOfEntries++;
        offsetFreeContent += bytesWritten;

        currentPage.writeInt(OFFSET_FREE_CONTENT, offsetFreeContent);
        currentPage.writeInt(OFFSET_NUM_ENTRIES, numberOfEntries);
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error persisting deletion tombstones", e);
      throw new IndexException("Error persisting deletion tombstones", e);
    }
  }

  // ========== QUANTIZATION HELPER METHODS ==========

  /**
   * Quantizes a float vector according to the index's quantization type.
   * Returns a QuantizationResult containing the quantized data and metadata needed for dequantization.
   *
   * @param vector The float vector to quantize
   *
   * @return Quantization result with quantized bytes and metadata, or null if quantization is NONE
   */
  private Object quantizeVector(final float[] vector) {
    if (metadata.quantizationType == VectorQuantizationType.NONE)
      return null; // No quantization

    if (metadata.quantizationType == VectorQuantizationType.INT8)
      return quantizeToInt8(vector);

    if (metadata.quantizationType == VectorQuantizationType.BINARY)
      return quantizeToBinary(vector);

    throw new IndexException("Unsupported quantization type: " + metadata.quantizationType);
  }

  /**
   * Quantizes a float vector to INT8 using min-max scaling.
   * Algorithm extracted from SQLFunctionVectorQuantizeInt8.
   *
   * @param vector The float vector to quantize
   *
   * @return Int8QuantizationMetadata containing quantized bytes and min/max values
   */
  private VectorQuantizationMetadata.Int8QuantizationMetadata quantizeToInt8(final float[] vector) {
    // Find min and max
    float min = vector[0];
    float max = vector[0];
    for (final float value : vector) {
      if (value < min)
        min = value;
      if (value > max)
        max = value;
    }

    // Quantize to int8 [-128, 127]
    final byte[] quantized = new byte[vector.length];
    if (min == max) {
      // All values are the same
      for (int i = 0; i < vector.length; i++) {
        quantized[i] = 0;
      }
    } else {
      final float range = max - min;
      for (int i = 0; i < vector.length; i++) {
        final float normalized = (vector[i] - min) / range; // [0, 1]
        final int scaled = Math.round(normalized * 255.0f); // [0, 255]
        final byte shifted = (byte) (scaled - 128); // [-128, 127]
        quantized[i] = shifted;
      }
    }

    return new VectorQuantizationMetadata.Int8QuantizationMetadata(quantized, min, max);
  }

  /**
   * Quantizes a float vector to BINARY using median threshold.
   * Algorithm extracted from SQLFunctionVectorQuantizeBinary.
   *
   * @param vector The float vector to quantize
   *
   * @return BinaryQuantizationMetadata containing packed bits and median value
   */
  private VectorQuantizationMetadata.BinaryQuantizationMetadata quantizeToBinary(final float[] vector) {
    // Calculate median
    final float median = calculateMedian(vector);

    // Quantize to binary
    final int byteCount = (vector.length + 7) / 8; // Round up to nearest byte
    final byte[] packed = new byte[byteCount];

    for (int i = 0; i < vector.length; i++) {
      if (vector[i] >= median) {
        // Set bit to 1
        final int byteIndex = i / 8;
        final int bitIndex = i % 8;
        packed[byteIndex] |= (1 << bitIndex);
      }
    }

    return new VectorQuantizationMetadata.BinaryQuantizationMetadata(packed, median, vector.length);
  }

  /**
   * Calculate median of array.
   * Helper method for binary quantization.
   */
  private float calculateMedian(final float[] values) {
    final float[] sorted = values.clone();
    java.util.Arrays.sort(sorted);
    if (sorted.length % 2 == 0) {
      return (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0f;
    } else {
      return sorted[sorted.length / 2];
    }
  }

  /**
   * Dequantizes a quantized vector back to float array.
   * Algorithm extracted from SQLFunctionVectorDequantizeInt8 and similar.
   *
   * @param quantized The quantized byte array
   * @param qmeta     The quantization metadata containing min/max or median
   *
   * @return The dequantized float vector
   */
  private float[] dequantizeVector(final byte[] quantized, final VectorQuantizationMetadata qmeta) {
    if (qmeta == null || qmeta.getType() == VectorQuantizationType.NONE)
      throw new IndexException("Cannot dequantize: no quantization metadata");

    if (qmeta.getType() == VectorQuantizationType.INT8)
      return dequantizeFromInt8(quantized, (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta);

    if (qmeta.getType() == VectorQuantizationType.BINARY)
      return dequantizeFromBinary(quantized, (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta);

    throw new IndexException("Unsupported quantization type: " + qmeta.getType());
  }

  /**
   * Dequantizes an INT8 quantized vector back to float array.
   * Algorithm extracted from SQLFunctionVectorDequantizeInt8.
   *
   * @param quantized The quantized byte array
   * @param qmeta     The INT8 quantization metadata with min/max
   *
   * @return The dequantized float vector
   */
  private float[] dequantizeFromInt8(final byte[] quantized, final VectorQuantizationMetadata.Int8QuantizationMetadata qmeta) {
    final float[] result = new float[quantized.length];
    final float range = qmeta.max - qmeta.min;

    if (range == 0.0f) {
      // All values were the same, return min value for all
      for (int i = 0; i < quantized.length; i++) {
        result[i] = qmeta.min;
      }
    } else {
      for (int i = 0; i < quantized.length; i++) {
        // Reverse quantization: value = (((quantized + 128) / 255) * range) + min
        // Convert signed byte [-128, 127] back to [0, 255] range by adding 128
        final int scaled = (int) quantized[i] + 128; // Convert to [0, 255]
        final float normalized = scaled / 255.0f; // [0, 1]
        result[i] = normalized * range + qmeta.min;
      }
    }

    return result;
  }

  /**
   * Dequantizes a BINARY quantized vector back to float array.
   * Unpacks bits and converts back to float values using the median threshold.
   *
   * @param packed The packed binary data
   * @param qmeta  The BINARY quantization metadata with median
   *
   * @return The dequantized float vector
   */
  private float[] dequantizeFromBinary(final byte[] packed, final VectorQuantizationMetadata.BinaryQuantizationMetadata qmeta) {
    final float[] result = new float[qmeta.originalLength];

    for (int i = 0; i < qmeta.originalLength; i++) {
      final int byteIndex = i / 8;
      final int bitIndex = i % 8;
      final boolean bitSet = (packed[byteIndex] & (1 << bitIndex)) != 0;

      // Reconstruct value based on bit: 1 -> above median, 0 -> below median
      // This is a lossy approximation - we just use median or 0 as the values
      result[i] = bitSet ? qmeta.median : 0.0f;
    }

    return result;
  }

  /**
   * Reads a quantized vector from a file offset and dequantizes it.
   * This method reads the quantized vector data stored in index pages and converts it back to float[].
   *
   * @param fileOffset  The absolute file offset where the vector entry starts
   * @param isCompacted Whether to read from compacted or mutable file
   *
   * @return The dequantized float vector, or null if quantization is disabled or vector not found
   */
  protected float[] readVectorFromOffset(final long fileOffset, final boolean isCompacted) {
    try {
      // If no quantization is enabled, return null (caller should fetch from document)
      if (metadata.quantizationType == VectorQuantizationType.NONE)
        return null;

      // Calculate page number and offset within page
      final int pageNum = (int) (fileOffset / getPageSize());
      final int offsetInPage = (int) (fileOffset % getPageSize()) - BasePage.PAGE_HEADER_SIZE;

      // Get the appropriate file ID
      final int fileId = isCompacted ? compactedSubIndex.getFileId() : getFileId();

      // Read the page
      final BasePage page = getDatabase().getPageManager()
          .getImmutablePage(new PageId(getDatabase(), fileId, pageNum), getPageSize(), false, false);

      try {
        // Skip over the entry header (vectorId, bucketId, position, deleted flag)
        // These are variable-sized, so we need to read and skip them
        int pos = offsetInPage;

        // Read and skip vectorId
        final long[] vectorIdAndSize = page.readNumberAndSize(pos);
        pos += (int) vectorIdAndSize[1];
        // Read and skip bucketId
        final long[] bucketIdAndSize = page.readNumberAndSize(pos);
        pos += (int) bucketIdAndSize[1];
        // Read and skip position
        final long[] positionAndSize = page.readNumberAndSize(pos);
        pos += (int) positionAndSize[1];
        // Skip deleted flag
        pos += 1;

        // Read quantization type flag
        final byte quantTypeOrdinal = page.readByte(pos);
        pos += 1;

        final VectorQuantizationType quantType = VectorQuantizationType.values()[quantTypeOrdinal];

        if (quantType == VectorQuantizationType.INT8) {
          // Read vector length
          final int vectorLength = page.readInt(pos);
          pos += 4;

          // Read quantized bytes
          final byte[] quantized = new byte[vectorLength];
          for (int i = 0; i < vectorLength; i++) {
            quantized[i] = page.readByte(pos);
            pos += 1;
          }

          // Read min and max
          final float min = Float.intBitsToFloat(page.readInt(pos));
          pos += 4;
          final float max = Float.intBitsToFloat(page.readInt(pos));

          // Dequantize
          final VectorQuantizationMetadata.Int8QuantizationMetadata qmeta = new VectorQuantizationMetadata.Int8QuantizationMetadata(
              quantized, min, max);
          return dequantizeFromInt8(quantized, qmeta);

        } else if (quantType == VectorQuantizationType.BINARY) {
          // Read original length
          final int originalLength = page.readInt(pos);
          pos += 4;

          // Read packed bytes
          final int byteCount = (originalLength + 7) / 8;
          final byte[] packed = new byte[byteCount];
          for (int i = 0; i < byteCount; i++) {
            packed[i] = page.readByte(pos);
            pos += 1;
          }

          // Read median
          final float median = Float.intBitsToFloat(page.readInt(pos));

          // Dequantize
          final VectorQuantizationMetadata.BinaryQuantizationMetadata qmeta = new VectorQuantizationMetadata.BinaryQuantizationMetadata(
              packed, median, originalLength);
          return dequantizeFromBinary(packed, qmeta);
        }

        return null;

      } finally {
        // BasePage is managed by PageManager, no explicit close needed
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error reading vector from offset %d: %s", fileOffset, e.getMessage());
      return null;
    }
  }

  // ========== END QUANTIZATION HELPER METHODS ==========

  /**
   * Create a new vector data page with LSM-style header.
   * Page layout: [offsetFreeContent(4)][numberOfEntries(4)][mutable(1)][entries grow forward sequentially]
   */
  private MutablePage createNewVectorDataPage(final int pageNum) {
    final PageId pageId = new PageId(getDatabase(), getFileId(), pageNum);
    final MutablePage page = getDatabase().getTransaction().addPage(pageId, getPageSize());

    int pos = 0;
    // offsetFreeContent starts right after header (entries grow forward sequentially)
    pos += page.writeInt(pos, HEADER_BASE_SIZE);
    pos += page.writeInt(pos, 0);              // numberOfEntries = 0
    page.writeByte(pos, (byte) 1);         // mutable = 1 (page is actively being written to)

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
  public List<Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k) {
    return findNeighborsFromVector(queryVector, k, null);
  }

  /**
   * Search for k nearest neighbors to the given vector within a filtered set of RIDs.
   * This method allows restricting the search space to specific records, useful for
   * filtering by user ID, category, or other criteria during graph traversal.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   * @param allowedRIDs Optional set of RIDs to restrict search to (null means no filtering)
   *
   * @return List of pairs containing RID and similarity score
   */
  public List<Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k, final Set<RID> allowedRIDs) {
    if (queryVector == null)
      throw new IllegalArgumentException("Query vector cannot be null");

    if (queryVector.length != metadata.dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

    // Ensure graph is available (lazy-load from disk if needed, or build if not persisted)
    ensureGraphAvailable();

    boolean readLockHeld = false;
    lock.readLock().lock();
    readLockHeld = true;
    try {
      // Phase 5+: Check if graph needs rebuilding due to pending mutations
      // With periodic rebuilds (threshold=1000), we may have some pending mutations
      if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
        // Graph is out of sync - need to rebuild before searching
        lock.readLock().unlock();
        readLockHeld = false;
        lock.writeLock().lock();
        try {
          // Double-check after acquiring write lock
          if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
            LogManager.instance().log(this, Level.FINE,
                "Rebuilding graph before search (accumulated " + mutationsSinceSerialize.get() + " mutations)");
            buildGraphFromScratch();
          }
          // Downgrade to read lock
          lock.readLock().lock();
          readLockHeld = true;
        } finally {
          lock.writeLock().unlock();
        }
      }

      if (graphIndex == null || vectorIndex.size() == 0)
        return Collections.emptyList();

      // Convert query vector to VectorFloat
      final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

      // Create lazy-loading RandomAccessVectorValues
      // Vector property name is the first property in the index
      final String vectorProp =
          metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.getFirst() : "vector";

      final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions, vectorProp,
          vectorIndex, ordinalToVectorId, this  // Pass LSM index reference for quantization support
      );

      // Perform search with optional RID filtering
      final Bits bitsFilter = (allowedRIDs != null && !allowedRIDs.isEmpty()) ?
          new RIDBitsFilter(allowedRIDs, ordinalToVectorId, vectorIndex) :
          Bits.ALL;

      final SearchResult searchResult = GraphSearcher.search(queryVectorFloat, k, vectors, metadata.similarityFunction, graphIndex,
          bitsFilter);

      LogManager.instance()
          .log(this, Level.INFO, "GraphSearcher returned %d nodes, graphSize=%d, vectorsSize=%d, ordinalToVectorIdLength=%d",
              searchResult.getNodes().length, graphIndex.size(), vectors.size(), ordinalToVectorId.length);

      // Extract RIDs and scores from search results using ordinal mapping
      final List<Pair<RID, Float>> results = new ArrayList<>();
      int skippedOutOfBounds = 0;
      int skippedDeletedOrNull = 0;
      for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
        final int ordinal = nodeScore.node;
        if (ordinal >= 0 && ordinal < ordinalToVectorId.length) {
          final int vectorId = ordinalToVectorId[ordinal];
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
          if (loc != null && !loc.deleted) {
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
            results.add(new com.arcadedb.utility.Pair<>(loc.rid, distance));
          } else {
            skippedDeletedOrNull++;
          }
        } else {
          skippedOutOfBounds++;
        }
      }

      LogManager.instance()
          .log(this, Level.INFO, "Vector search returned %d results (skipped: %d out of bounds, %d deleted/null)", results.size(),
              skippedOutOfBounds, skippedDeletedOrNull);
      return results;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error performing vector search", e);
      throw new IndexException("Error performing vector search", e);
    } finally {
      if (readLockHeld) {
        lock.readLock().unlock();
      }
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

    // Ensure graph is available (lazy-load from disk if needed)
    ensureGraphAvailable();

    boolean readLockHeld = false;
    lock.readLock().lock();
    readLockHeld = true;
    try {
      // Phase 5+: Check if graph needs rebuilding due to pending mutations
      // With periodic rebuilds (threshold=1000), we may have some pending mutations
      if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
        // Graph is out of sync - need to rebuild before searching
        lock.readLock().unlock();
        readLockHeld = false;
        lock.writeLock().lock();
        try {
          // Double-check after acquiring write lock
          if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
            LogManager.instance().log(this, Level.FINE,
                "Rebuilding graph before search (accumulated " + mutationsSinceSerialize.get() + " mutations)");
            buildGraphFromScratch();
          }
          // Downgrade to read lock
          lock.readLock().lock();
          readLockHeld = true;
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

      if (vectorIndex.size() == 0) {
        LogManager.instance().log(this, Level.INFO, "No vectors in index, returning empty results");
      } else {
        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Create lazy-loading RandomAccessVectorValues
        // Vector property name is the first property in the index
        final String vectorProp =
            metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.get(0) : "vector";

        final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions, vectorProp,
            vectorIndex, ordinalToVectorId, this  // Pass LSM index reference for quantization support
        );

        // Perform search
        final SearchResult searchResult = GraphSearcher.search(queryVectorFloat, k, vectors, metadata.similarityFunction,
            graphIndex, Bits.ALL);

        // Extract RIDs from search results using ordinal mapping
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal >= 0 && ordinal < ordinalToVectorId.length) {
            final int vectorId = ordinalToVectorId[ordinal];
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
            if (loc != null && !loc.deleted) {
              resultRIDs.add(loc.rid);
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
      if (readLockHeld) {
        lock.readLock().unlock();
      }
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
    if (keys[0] instanceof ComparableVector c)
      vector = c.vector;
    else
      vector = VectorUtils.convertToFloatArray(keys[0]);

    if (vector == null) {
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

        // Persist vector to page (will be added to vectorIndex inside persistVectorWithLocation)
        persistVectorWithLocation(id, rid, vector);

        // Phase 5+: Periodic rebuild strategy (amortizes cost over many operations)
        if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING) {
          // Transition to MUTABLE state to track ongoing mutations
          this.graphState = GraphState.MUTABLE;
        }

        // Increment mutation counter
        mutationsSinceSerialize.incrementAndGet();

        // DON'T trigger rebuild during transaction commit - defer until query time
        // rebuildGraphIfNeeded() calls buildGraphFromScratch() which clears vectorIndex and
        // tries to reload from pages, but pages aren't visible yet during commit phase
        // The graph will be rebuilt on the next query via ensureGraphAvailable() / get()
        // rebuildGraphIfNeeded();
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
        // Find all vectors with matching RID and mark as deleted
        final List<Integer> deletedIds = new ArrayList<>();
        for (int vectorId : vectorIndex.getAllVectorIds().toArray()) {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
          if (loc != null && loc.rid.equals(rid) && !loc.deleted) {
            vectorIndex.markDeleted(vectorId);
            deletedIds.add(vectorId);
          }
        }

        // Persist deletion tombstones
        if (!deletedIds.isEmpty()) {
          persistDeletionTombstones(deletedIds);

          // Phase 5+: Periodic rebuild strategy (amortizes cost over many operations)
          if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING) {
            // Transition to MUTABLE state to track ongoing mutations
            this.graphState = GraphState.MUTABLE;
          }

          // Increment mutation counter (count number of deletions)
          mutationsSinceSerialize.addAndGet(deletedIds.size());
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public void onAfterCommit() {
    // DISABLED: Compaction for vector indexes is currently disabled
    // Vector indexes don't benefit much from compaction since vectors are rarely updated
    // Re-enable once compaction properly handles uninitialized pages

    // Check if compaction should be triggered after commit
    // Operations are applied immediately during TransactionIndexContext replay (not buffered here)
    // if (minPagesToScheduleACompaction > 1 && currentMutablePages.get() >= minPagesToScheduleACompaction) {
    //   LogManager.instance()
    //       .log(this, Level.FINE, "Scheduled compaction of vector index '%s' (currentMutablePages=%d totalPages=%d)",
    //           null, getComponentName(), currentMutablePages.get(), getTotalPages());
    //   ((com.arcadedb.database.async.DatabaseAsyncExecutorImpl) getDatabase().async()).compact(this);
    // }
  }

  @Override
  public long countEntries() {
    // Use vectorIndex which already applies LSM merge-on-read semantics
    // (latest entry for each RID, filtering out deleted entries)
    return vectorIndex.getActiveCount();
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

    LogManager.instance().log(this, Level.INFO, "compact() called for index: %s", null, getName());
    checkIsValid();
    final DatabaseInternal database = getDatabase();

    if (database.getMode() == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + getName() + "'");

    if (database.getPageManager().isPageFlushingSuspended(database)) {
      LogManager.instance().log(this, Level.INFO, "compact() returning false: page flushing suspended");
      // POSTPONE COMPACTING (DATABASE BACKUP IN PROGRESS?)
      return false;
    }

    LogManager.instance().log(this, Level.INFO,
        "compact() current status: %s, attempting compareAndSet from COMPACTION_SCHEDULED to COMPACTION_IN_PROGRESS", status.get());
    if (!status.compareAndSet(INDEX_STATUS.COMPACTION_SCHEDULED, INDEX_STATUS.COMPACTION_IN_PROGRESS)) {
      LogManager.instance()
          .log(this, Level.INFO, "compact() returning false: status compareAndSet failed (current status: %s)", status.get());
      // COMPACTION NOT SCHEDULED
      return false;
    }

    try {
      LogManager.instance().log(this, Level.INFO, "compact() calling LSMVectorIndexCompactor.compact()");
      return LSMVectorIndexCompactor.compact(this);
    } catch (final TimeoutException e) {
      LogManager.instance().log(this, Level.INFO, "compact() caught TimeoutException: %s", e.getMessage());
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
    if (metadata.quantizationType != VectorQuantizationType.NONE)
      json.put("quantization", metadata.quantizationType.name());
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

    final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(
        indexJSON.getString("nullStrategy", LSMTreeIndexAbstract.NULL_STRATEGY.ERROR.name()));

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

    LogManager.instance().log(this, Level.FINE, "Applied metadata from schema to vector index: %s (dimensions=%d)", indexName,
        this.metadata.dimensions);
  }

  @Override
  public void flush() {
    if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {

      // Build and persist graph if it hasn't been built yet
      // This ensures the graph is available on next database open (fast restart)
      // Build graph if it's in LOADING (never built) or MUTABLE (has pending changes) state
      if (vectorIndex.size() > 0 && (graphState == GraphState.LOADING || graphState == GraphState.MUTABLE)) {
        try {
          LogManager.instance()
              .log(this, Level.FINE, "Building graph before close for index: %s (this may take 1-2 minutes for large datasets)",
                  indexName);
          final long startTime = System.currentTimeMillis();
          buildGraphFromScratch();
          final long elapsed = System.currentTimeMillis() - startTime;
          LogManager.instance().log(this, Level.FINE, "Graph building completed in %d seconds", elapsed / 1000);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Failed to build graph before close: " + e.getMessage(), e);
          // Don't fail close if graph building fails
        }
      } else {
        LogManager.instance()
            .log(this, Level.FINE, "Skipping graph build on close: vectorIndexSize=%d, graphState=%s", vectorIndex.size(),
                graphState);
      }
    }
  }

  @Override
  public void close() {
    flush();
  }

  @Override
  public void drop() {
    lock.writeLock().lock();
    try {
      // Clear all vector locations
      vectorIndex.clear();
      ordinalToVectorId = new int[0];

      final DatabaseInternal db = mutable != null ? mutable.getDatabase() : null;

      // Drop compacted sub-index if it exists
      if (compactedSubIndex != null) {
        try {
          final int compactedFileId = compactedSubIndex.getFileId();
          if (db != null && db.isOpen()) {
            db.getPageManager().deleteFile(db, compactedFileId);
            db.getFileManager().dropFile(compactedFileId);
            db.getSchema().getEmbedded().removeFile(compactedFileId);
          } else {
            final File compactedFile = compactedSubIndex.getOSFile();
            if (compactedFile != null && compactedFile.exists() && !compactedFile.delete()) {
              LogManager.instance().log(this, Level.WARNING, "Error deleting compacted index file '%s'", compactedFile.getPath());
            }
          }
        } catch (final Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error dropping compacted sub-index for '%s': %s", indexName, e.getMessage());
        }
      }

      // Drop the mutable component (this properly deletes the physical file)
      if (mutable != null) {
        try {
          final int mutableFileId = mutable.getFileId();
          if (db != null && db.isOpen()) {
            db.getPageManager().deleteFile(db, mutableFileId);
            db.getFileManager().dropFile(mutableFileId);
            db.getSchema().getEmbedded().removeFile(mutableFileId);
          } else {
            final File mutableFile = mutable.getOSFile();
            if (mutableFile != null && mutableFile.exists() && !mutableFile.delete()) {
              LogManager.instance().log(this, Level.WARNING, "Error deleting mutable index file '%s'", mutableFile.getPath());
            }
          }
        } catch (final Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error dropping mutable component for '%s': %s", indexName, e.getMessage());
        }
      }

      // Delete graph file if it exists
      if (graphFile != null) {
        final File graphIndexFile = graphFile.getOSFile();
        if (graphIndexFile.exists())
          graphIndexFile.delete();
      }

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
    stats.put("totalVectors", (long) vectorIndex.size());
    stats.put("activeVectors", vectorIndex.getActiveCount());
    stats.put("deletedVectors", (long) vectorIndex.size() - vectorIndex.getActiveCount());
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
    return build(buildIndexBatchSize, callback, null);
  }

  /**
   * Build the vector index with optional graph building progress callback.
   *
   * @param buildIndexBatchSize Batch size for committing during index build
   * @param callback            Callback for document indexing progress
   * @param graphCallback       Callback for graph building progress
   *
   * @return Total number of records indexed
   */
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback, final GraphBuildCallback graphCallback) {
    final long totalRecords;

    lock.writeLock().lock();
    try {
      if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {
        try {
          final AtomicInteger total = new AtomicInteger();
          final long LOG_INTERVAL = 10000; // Log every 10K records
          final long startTime = System.currentTimeMillis();

          if (metadata.propertyNames == null || metadata.propertyNames.isEmpty())
            throw new IndexException("Cannot rebuild vector index '" + indexName + "' because property names are missing");

          LogManager.instance()
              .log(this, Level.INFO, "Building vector index '%s' on %d properties...", indexName, metadata.propertyNames.size());

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
                    .log(this, Level.INFO, "Building vector index '%s': processed %d records (%.0f records/sec)...", indexName,
                        total.get(), rate);
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
            LogManager.instance()
                .log(this, Level.INFO, "Completed building vector index '%s': processed %d records in %dms", indexName, total.get(),
                    elapsed);

            totalRecords = total.get();
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

    // After index build completes, build and persist the graph
    // This ensures the graph is ready for searches and persisted for fast restart
    if (vectorIndex.size() > 0 && graphState == GraphState.LOADING) {
      LogManager.instance().log(this, Level.INFO, "Building graph after index build for: " + indexName);
      try {
        buildGraphFromScratch(graphCallback);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to build graph after index build: " + e.getMessage(), e);
        // Don't fail the whole index build if graph building fails
      }
    }

    return totalRecords;
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
    final LockManager.LOCK_STATUS locked = getDatabase().getTransactionManager().tryLockFile(fileId, 0, Thread.currentThread());

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
          final MutablePage newPage = new MutablePage(new PageId(getDatabase(), newMutableIndex.getFileId(), i + 1), getPageSize());

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

        // Set the compacted sub-index on the main index
        this.compactedSubIndex = compactedIndex;

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

  /**
   * Get the VectorLocationIndex (used by compactor to reload after compaction)
   */
  protected VectorLocationIndex getVectorIndex() {
    return vectorIndex;
  }

  /**
   * Apply a replicated page update to VectorLocationIndex.
   * Called by TransactionManager.applyChanges() during HA replication to keep
   * in-memory VectorLocationIndex synchronized with replicated pages.
   * <p>
   * This ensures replicas don't have stale VectorLocationIndex that causes offset mismatches.
   *
   * @param page The page that was just replicated and written
   */
  public void applyReplicatedPageUpdate(final com.arcadedb.engine.MutablePage page) {
    try {
      final int pageNum = page.getPageId().getPageNumber();
      final int fileId = page.getPageId().getFileId();

      // Determine if this page is in the compacted or mutable file
      final boolean isCompacted = (compactedSubIndex != null && fileId == compactedSubIndex.getFileId());

      // Read page header
      final int offsetFreeContent = page.readInt(OFFSET_FREE_CONTENT);
      final int numberOfEntries = page.readInt(OFFSET_NUM_ENTRIES);

      LogManager.instance().log(this, Level.FINE,
          "applyReplicatedPageUpdate: index=%s, pageNum=%d, fileId=%d, entries=%d, freeContent=%d, vectorIndexSizeBefore=%d",
          indexName, pageNum, fileId, numberOfEntries, offsetFreeContent, vectorIndex.size());

      if (numberOfEntries == 0)
        return; // Empty page, nothing to update

      // Calculate header size (compacted page 0 has extra metadata)
      final int headerSize;
      if (isCompacted && pageNum == 0) {
        // Compacted page 0: base header + dimensions + similarity + maxConn + beamWidth
        headerSize = HEADER_BASE_SIZE + (4 * 4); // 9 + 16 = 25 bytes
      } else {
        headerSize = HEADER_BASE_SIZE; // 9 bytes
      }

      // Calculate absolute file offset for this page
      final long pageStartOffset = (long) pageNum * getPageSize();

      // Parse variable-sized entries sequentially (no pointer table)
      int currentOffset = headerSize;
      for (int i = 0; i < numberOfEntries; i++) {
        // Record absolute file offset for this entry
        final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + currentOffset;

        // Read variable-sized vectorId
        final long[] vectorIdAndSize = page.readNumberAndSize(currentOffset);
        final int id = (int) vectorIdAndSize[0];
        currentOffset += (int) vectorIdAndSize[1];

        // Read variable-sized bucketId
        final long[] bucketIdAndSize = page.readNumberAndSize(currentOffset);
        final int bucketId = (int) bucketIdAndSize[0];
        currentOffset += (int) bucketIdAndSize[1];

        // Read variable-sized position
        final long[] positionAndSize = page.readNumberAndSize(currentOffset);
        final long position = positionAndSize[0];
        currentOffset += (int) positionAndSize[1];

        final RID rid = new RID(getDatabase(), bucketId, position);

        // Read deleted flag (fixed 1 byte)
        final boolean deleted = page.readByte(currentOffset) == 1;
        currentOffset += 1;

        // Update VectorLocationIndex with this entry's absolute file offset
        // LSM semantics: later entries override earlier ones
        vectorIndex.addOrUpdate(id, isCompacted, entryFileOffset, rid, deleted);
      }

      LogManager.instance()
          .log(this, Level.FINE, "Applied replicated page update: pageNum=%d, fileId=%d, isCompacted=%b, entries=%d", pageNum,
              fileId, isCompacted, numberOfEntries);

    } catch (final Exception e) {
      // Log but don't fail replication - VectorLocationIndex will be rebuilt if needed
      LogManager.instance()
          .log(this, Level.WARNING, "Error applying replicated page update for index %s: %s", indexName, e.getMessage());
    }
  }

  /**
   * Reload vectors from pages after compaction.
   * Called by compactor after splitIndex to refresh VectorLocationIndex with new file structure.
   */
  protected void loadVectorsFromPagesAfterCompaction() {
    loadVectorsFromPages();
  }

  /**
   * Rebuild graph index after compaction.
   * Called by compactor after reloading VectorLocationIndex.
   */
  protected void rebuildGraphAfterCompaction() {
    initializeGraphIndex();
  }

  /**
   * Get the location cache size from configuration (per-index metadata or global default).
   *
   * @return Maximum number of vector locations to cache, or -1 for unlimited
   */
  private int getLocationCacheSize() {
    if (metadata != null && metadata.locationCacheSize > -1) {
      return metadata.locationCacheSize;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE);
  }

  /**
   * Get the location cache size from configuration during initialization.
   * Used when mutable is not yet initialized.
   *
   * @param database The database instance
   *
   * @return Maximum number of vector locations to cache, or -1 for unlimited
   */
  private int getLocationCacheSize(final DatabaseInternal database) {
    if (metadata != null && metadata.locationCacheSize > -1) {
      return metadata.locationCacheSize;
    }
    return database.getConfiguration().getValueAsInteger(com.arcadedb.GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE);
  }

  /**
   * Get the graph build cache size from configuration (per-index metadata or global default).
   *
   * @return Maximum number of vectors to cache during graph building
   */
  private int getGraphBuildCacheSize() {
    if (metadata != null && metadata.graphBuildCacheSize > -1) {
      return metadata.graphBuildCacheSize;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE);
  }

  /**
   * Get the mutations before rebuild threshold from configuration (per-index metadata or global default).
   *
   * @return Number of mutations before rebuilding graph index
   */
  private int getMutationsBeforeRebuild() {
    if (metadata != null && metadata.mutationsBeforeRebuild > 0) {
      return metadata.mutationsBeforeRebuild;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD);
  }

  /**
   * Get a vector location by ID, with fallback to page scanning if evicted from cache.
   * This method provides transparent cache miss handling for bounded location caches.
   *
   * @param vectorId The vector ID to look up
   *
   * @return The vector location, or null if not found
   */
  private VectorLocationIndex.VectorLocation getVectorLocation(final int vectorId) {
    // Try cache first (O(1) lookup)
    VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
    if (loc != null) {
      return loc;
    }

    // Cache miss - reconstruct by scanning pages (expensive but rare for LRU cache)
    loc = reconstructLocationFromPages(vectorId);
    if (loc != null) {
      // Add back to cache for future access
      vectorIndex.addOrUpdate(vectorId, loc.isCompacted, loc.absoluteFileOffset, loc.rid, loc.deleted);
    }
    return loc;
  }

  /**
   * Reconstruct a vector location from pages when evicted from cache.
   * Scans compacted index first (more likely for old vectors), then mutable index.
   *
   * @param vectorId The vector ID to find
   *
   * @return The reconstructed location, or null if not found
   */
  private VectorLocationIndex.VectorLocation reconstructLocationFromPages(final int vectorId) {
    // Scan compacted index first (more likely to contain old vectors)
    if (compactedSubIndex != null) {
      final VectorLocationIndex.VectorLocation loc = scanPagesForVectorId(compactedSubIndex.getFileId(),
          compactedSubIndex.getTotalPages(), vectorId, true);
      if (loc != null)
        return loc;
    }

    // Scan mutable index
    return scanPagesForVectorId(mutable.getFileId(), mutable.getTotalPages(), vectorId, false);
  }

  /**
   * Scan pages in a specific file to find a vector by ID.
   * Similar to loadVectorsFromFile() but stops at first match.
   *
   * @param fileId      The file ID to scan
   * @param totalPages  Number of pages in the file
   * @param vectorId    The vector ID to find
   * @param isCompacted True if scanning compacted file
   *
   * @return The vector location if found, null otherwise
   */
  private VectorLocationIndex.VectorLocation scanPagesForVectorId(final int fileId, final int totalPages, final int vectorId,
      final boolean isCompacted) {
    for (int pageNum = 0; pageNum < totalPages; pageNum++) {
      try {
        final BasePage currentPage = mutable.getDatabase().getPageManager()
            .getImmutablePage(new PageId(mutable.getDatabase(), fileId, pageNum), getPageSize(), false, false);

        if (currentPage == null)
          continue;

        final int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        final int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

        if (numberOfEntries == 0)
          continue;

        // Calculate header size
        final int headerSize;
        if (isCompacted && pageNum == 0) {
          headerSize = HEADER_BASE_SIZE + (4 * 4); // base + 4 ints for metadata
        } else {
          headerSize = HEADER_BASE_SIZE;
        }

        // Calculate absolute file offset for this page's data
        final long pageBaseOffset = ((long) fileId << 32) | (pageNum * getPageSize());

        // Parse entries in this page
        int entryOffset = headerSize;
        for (int i = 0; i < numberOfEntries && entryOffset < offsetFreeContent; i++) {
          final long entryFileOffset = pageBaseOffset + entryOffset;
          final int id = currentPage.readInt(entryOffset);
          entryOffset += 4;

          // Check if this is the vector we're looking for
          if (id == vectorId) {
            // Found it! Read the rest of the entry
            final int bucketId = currentPage.readInt(entryOffset);
            entryOffset += 4;
            final long position = currentPage.readLong(entryOffset);
            entryOffset += 8;
            final RID rid = new RID(mutable.getDatabase(), bucketId, position);

            final byte flags = currentPage.readByte(entryOffset);
            entryOffset += 1;
            final boolean deleted = (flags & 0x01) != 0;

            // Skip vector data (if present)
            // Entry format: id(4) + bucketId(4) + position(8) + flags(1) + [quantized data]
            // We don't need to read the vector data, just return the location

            return new VectorLocationIndex.VectorLocation(isCompacted, entryFileOffset, rid, deleted);
          }

          // Skip to next entry: bucketId(4) + position(8) + flags(1) + vector data
          entryOffset += 4 + 8 + 1;

          // Skip quantized vector data if present
          if (metadata.quantizationType != VectorQuantizationType.NONE) {
            final int quantizedSize = calculateQuantizedSize(metadata.dimensions, metadata.quantizationType);
            entryOffset += quantizedSize;
          }
        }
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error scanning page %d in file %d for vectorId %d: %s", pageNum, fileId, vectorId,
                e.getMessage());
      }
    }

    return null; // Not found in this file
  }

  /**
   * Calculate the size of quantized vector data in bytes.
   *
   * @param dimensions       Number of vector dimensions
   * @param quantizationType Type of quantization
   *
   * @return Size in bytes
   */
  private int calculateQuantizedSize(final int dimensions, final VectorQuantizationType quantizationType) {
    switch (quantizationType) {
    case INT8:
      return dimensions; // 1 byte per dimension
    case BINARY:
      return (dimensions + 7) / 8; // 1 bit per dimension, rounded up to bytes
    case NONE:
    default:
      return 0;
    }
  }

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Index '" + indexName + "' is not valid. Probably has been drop or rebuilt");
  }
}
